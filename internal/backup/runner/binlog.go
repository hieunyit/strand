package runner

import (
	"context"
	"database/sql"
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
	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/backup"
	"strand/internal/reader/archive"
)

// MySQL continuous binlog archiving reuses Strand's binlog archiver (`strand archive binlog`: GTID
// auto-positioned, crash-safe, gap detection) into a local staging directory and uploads every finalized
// file to the repository, then removes it locally. With log_flush_seconds, FLUSH BINARY LOGS is issued
// that often so a file is finalized (and archived) at least that frequently (needs RELOAD).

var archivedName = regexp.MustCompile(`^([0-9]{8})-(.+)$`)

func mysqlDSN(getenv Getenv, c backup.ConnSpec) string {
	cfg := drivermysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net = c.User, password(getenv, c), "tcp"
	port := c.Port
	if port == 0 {
		port = 3306
	}
	cfg.Addr = c.Host + ":" + strconv.Itoa(port)
	cfg.ParseTime = true
	return cfg.FormatDSN()
}

// ArchiveBinlog runs until ctx ends. stagingDir must be private to this job.
func (e *Env) ArchiveBinlog(ctx context.Context, job backup.Job, stagingDir string) error {
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
	db, err := sql.Open("mysql", mysqlDSN(e.Getenv, job.Conn))
	if err != nil {
		return err
	}
	defer db.Close()
	var start string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_purged").Scan(&start); err != nil {
		return fmt.Errorf("read gtid_purged (binlog archiving needs gtid_mode=ON): %v", redactAll(err.Error(), []string{scfg.Password}))
	}
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return err
	}
	a, err := archive.OpenArchiver(archive.ArchiverConfig{Dir: stagingDir, Syncer: scfg, StartGTID: start, Heartbeat: 10 * time.Second,
		Log: slog.New(slog.NewTextHandler(e.logWriter(), nil))})
	if err != nil {
		return err
	}
	defer a.Close()
	runErr := make(chan error, 1)
	actx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { runErr <- a.Run(actx) }()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	flushEvery := time.Duration(job.LogFlushSeconds) * time.Second
	lastFlush := time.Now()
	for {
		select {
		case err := <-runErr:
			if ctx.Err() != nil {
				return nil
			}
			return err
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		if err := e.uploadBinlogs(ctx, job, repo, enc, stagingDir); err != nil {
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

// uploadBinlogs uploads finalized archive files whose state is committed (seq <= last_seq).
func (e *Env) uploadBinlogs(ctx context.Context, job backup.Job, repo *Repository, enc backup.EncodeOptions, dir string) error {
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
	gtids := map[string][2]int64{}
	var order []string
	err := p.ParseFile(path, 0, func(ev *replication.BinlogEvent) error {
		if ev.Header.Timestamp != 0 {
			if first == 0 {
				first = ev.Header.Timestamp
			}
			last = ev.Header.Timestamp
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
	return backup.Meta{Position: backup.Position{GTIDSet: strings.Join(parts, ",")}, StartTime: &s, EndTime: &l,
		Details: map[string]string{}}, nil
}
