package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jackc/pglogrepl"

	"strand/internal/backup"
	"strand/internal/reader/archive"
)

// MySQL continuous binlog archiving reuses Strand's binlog archiver (`strand archive binlog`: GTID
// auto-positioned, crash-safe, gap detection) into a local staging directory and uploads every finalized
// file to the repository, then removes it locally. With log_flush_seconds, FLUSH BINARY LOGS is issued
// that often so a file is finalized (and archived) at least that frequently (needs RELOAD).

var archivedName = regexp.MustCompile(`^([0-9]{8})-(.+)$`)

// pitrSourceIdentity reads the server's durable identifier. Missing identity
// still permits a standalone backup but cannot anchor a chain of archived logs.
func (e *Env) pitrSourceIdentity(ctx context.Context, job backup.Job) string {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if job.Engine() == backup.EnginePostgres {
		conn, err := connectBackupPG(ctx, e.Getenv, job.Conn, "", true)
		if err != nil {
			return ""
		}
		defer conn.Close(ctx)
		system, err := pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			return ""
		}
		return system.SystemID
	}
	db, err := openBackupMySQL(e.Getenv, job.Conn)
	if err != nil {
		return ""
	}
	defer db.Close()
	var id string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.server_uuid").Scan(&id); err != nil {
		return ""
	}
	return id
}

// ArchiveBinlog runs until ctx ends. stagingDir must be private to this job.
func (e *Env) ArchiveBinlog(ctx context.Context, job backup.Job, stagingDir string) error {
	if err := checkRuntimeTLS(job.Engine(), job.Conn); err != nil {
		return err
	}
	repo, err := e.OpenJobRepository(ctx, job.Repository, job.Bucket, job.Prefix)
	if err != nil {
		return err
	}
	enc, err := e.encodeOptions(job)
	if err != nil {
		return err
	}
	port := job.Conn.Port
	if port == 0 {
		port = 3306
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte("strand-backup-binlog:" + job.ID))
	scfg := replication.BinlogSyncerConfig{Flavor: "mysql", Host: job.Conn.Host, Port: uint16(port), User: job.Conn.User,
		Password: password(e.Getenv, job.Conn), ServerID: h.Sum32()%1_000_000_000 + 1_000_000_000}
	var errTLS error
	scfg.TLSConfig, errTLS = mysqlBackupTLSConfig(job.Conn)
	if errTLS != nil {
		return errTLS
	}
	db, err := openBackupMySQL(e.Getenv, job.Conn)
	if err != nil {
		return err
	}
	defer db.Close()
	var start, sourceID string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_purged, @@GLOBAL.server_uuid").Scan(&start, &sourceID); err != nil {
		return fmt.Errorf("read gtid_purged (binlog archiving needs gtid_mode=ON): %v", redactAll(err.Error(), []string{scfg.Password}))
	}
	last, err := e.latestOriginalLog(ctx, job.ID)
	if err != nil {
		return err
	}
	if last != nil && last.Details["source_identity"] != sourceID {
		return errors.New("binlog archive cannot resume: prior log does not identify the current MySQL server")
	}
	if err := ensureBinlogSource(stagingDir, sourceID); err != nil {
		return err
	}
	if last != nil {
		raw, err := os.ReadFile(filepath.Join(stagingDir, ".strand-archive.json"))
		if err != nil {
			return fmt.Errorf("binlog archive cannot resume without its local checkpoint: %w", err)
		}
		var state struct {
			LastSeq int `json:"last_seq"`
		}
		seq, err := strconv.Atoi(last.Details["seq"])
		if err != nil || json.Unmarshal(raw, &state) != nil || state.LastSeq < seq {
			return errors.New("binlog archive's local checkpoint precedes the last catalogued segment")
		}
	}
	a, err := archive.OpenArchiver(archive.ArchiverConfig{Dir: stagingDir, Syncer: scfg, StartGTID: start, Heartbeat: 10 * time.Second,
		Log: slog.New(slog.NewTextHandler(e.logWriter(), nil))})
	if err != nil {
		return err
	}
	actx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	var runErr error
	go func() {
		runErr = a.Run(actx)
		close(runDone)
	}()
	defer func() {
		cancel()
		<-runDone
		_ = a.Close()
	}()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	flushEvery := time.Duration(job.LogFlushSeconds) * time.Second
	lastFlush := time.Now()
	for {
		select {
		case <-runDone:
			if ctx.Err() != nil {
				return nil
			}
			return runErr
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		if err := e.uploadBinlogs(ctx, job, repo, enc, stagingDir, sourceID); err != nil {
			e.logf("binlog archive: upload: %v", err)
		}
		if flushEvery > 0 && time.Since(lastFlush) > flushEvery {
			lastFlush = time.Now()
			if _, err := db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
				e.logf("binlog archive: FLUSH BINARY LOGS: %v", err)
			}
		}
	}
}

func (e *Env) logWriter() io.Writer {
	if e.Log != nil {
		return e.Log
	}
	return os.Stderr
}

func ensureBinlogSource(dir, sourceID string) error {
	if sourceID == "" {
		return errors.New("binlog archive needs a MySQL server UUID")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, ".strand-source-identity")
	data, err := os.ReadFile(path)
	if err == nil {
		if string(data) != sourceID {
			return errors.New("binlog staging belongs to another MySQL server")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("binlog staging contains files with no verified source identity")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = file.WriteString(sourceID)
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

// uploadBinlogs uploads finalized archive files whose state is committed (seq <= last_seq).
func (e *Env) uploadBinlogs(ctx context.Context, job backup.Job, repo *Repository, enc backup.EncodeOptions, dir string, sourceID string) error {
	raw, err := os.ReadFile(filepath.Join(dir, ".strand-archive.json"))
	if err != nil {
		return nil // nothing finalized yet
	}
	var state struct {
		LastSeq int `json:"last_seq"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return err
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var names []string
	for _, en := range ents {
		if archivedName.MatchString(en.Name()) {
			names = append(names, en.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		m := archivedName.FindStringSubmatch(name)
		seq, _ := strconv.Atoi(m[1])
		if seq > state.LastSeq {
			break
		}
		path := filepath.Join(dir, name)
		meta, err := BinlogFileMeta(path)
		if err != nil {
			return fmt.Errorf("parse %s: %w", name, err)
		}
		meta.Details["server_file"] = m[2]
		meta.Details["source_identity"] = sourceID
		meta.Position.BinlogFile = m[2]
		meta.EndPosition.BinlogFile = m[2]
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		rec, err := e.storeLog(ctx, job, repo, enc, logSegment{ID: "binlog-" + safeSegment(job.ID) + "-" + m[1], Name: "binlog/" + name,
			Data: f, Method: backup.MethodBinlog, Seq: uint64(seq), Meta: meta})
		f.Close()
		if err != nil {
			return err
		}
		e.logf("binlog archive: %s archived (%s)", name, rec.ID)
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

// BinlogFileMeta reads first/last event times and the GTIDs of a binlog file.
func BinlogFileMeta(path string) (backup.Meta, error) {
	p := replication.NewBinlogParser()
	p.SetRawMode(true)
	var first, last uint32
	var coverageStart, coverageEnd int64
	var successor string
	var finalType replication.EventType
	gtids := map[string][2]int64{}
	var order []string
	err := p.ParseFile(path, 0, func(ev *replication.BinlogEvent) error {
		h := ev.Header
		finalType = h.EventType
		// Synthetic events have no source offset. A discontinuity means the
		// upstream GTID filter skipped part of this file, so only the final
		// contiguous suffix can prove coverage through the next rotation.
		if h.LogPos >= h.EventSize && h.EventSize > 0 {
			start, end := int64(h.LogPos-h.EventSize), int64(h.LogPos)
			if coverageEnd == 0 || coverageEnd != start {
				coverageStart = start
			}
			coverageEnd = end
		}
		if ev.Header.Timestamp != 0 {
			if first == 0 {
				first = ev.Header.Timestamp
			}
			last = ev.Header.Timestamp
		}
		if rotate, ok := ev.Event.(*replication.RotateEvent); ok && h.Timestamp != 0 {
			successor = string(rotate.NextLogName)
		}
		if ev.Header.EventType == replication.GTID_EVENT && len(ev.RawData) >= 19+1+16+8 {
			b := ev.RawData[19+1:]
			sid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
			gno := int64(0)
			for i := 7; i >= 0; i-- {
				gno = gno<<8 | int64(b[16+i])
			}
			r, ok := gtids[sid]
			if !ok {
				order = append(order, sid)
				r = [2]int64{gno, gno}
			}
			if gno < r[0] {
				r[0] = gno
			}
			if gno > r[1] {
				r[1] = gno
			}
			gtids[sid] = r
		}
		return nil
	})
	if err != nil {
		return backup.Meta{}, err
	}
	if first == 0 {
		return backup.Meta{}, errors.New("binlog file has no events")
	}
	var parts []string
	for _, sid := range order {
		r := gtids[sid]
		parts = append(parts, fmt.Sprintf("%s:%d-%d", sid, r[0], r[1]))
	}
	s, l := time.Unix(int64(first), 0).UTC(), time.Unix(int64(last), 0).UTC()
	details := map[string]string{"next_file": successor}
	if finalType == replication.STOP_EVENT {
		details["closed_with_stop"] = "true"
	}
	return backup.Meta{Position: backup.Position{GTIDSet: strings.Join(parts, ","), BinlogPos: coverageStart},
		EndPosition: backup.Position{BinlogPos: coverageEnd}, StartTime: &s, EndTime: &l,
		Details: details}, nil
}
