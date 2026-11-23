package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"strand/internal/backup"
)

// PostgreSQL continuous WAL archiving: Strand is a physical replication client (like pg_receivewal) on a
// replication slot, so no archive_command and no tool on the database host are needed. Every completed
// WAL segment is uploaded (compressed/encrypted per job) and cataloged as a log record [start, end) LSN.
// With log_flush_seconds set, pg_switch_wal() is called when a partially filled segment is older than
// that, bounding the RPO on quiet databases (the role needs EXECUTE on pg_switch_wal).

// PGConnString builds a keyword/value connection string for c.
func PGConnString(getenv Getenv, c backup.ConnSpec, database string, replication bool) string {
	q := url.Values{}
	mode := "prefer"
	if c.TLSMode == "disable" {
		mode = "disable"
	} else if c.TLSMode != "" {
		mode = "require" // connectBackupPG installs the verification policy without fallback.
	}
	q.Set("sslmode", mode)
	if replication {
		q.Set("replication", "true")
	}
	host := c.Host
	if c.Port > 0 {
		host += ":" + strconv.Itoa(c.Port)
	}
	u := url.URL{Scheme: "postgres", Host: host, Path: "/" + database, RawQuery: q.Encode()}
	if c.User != "" {
		if pw := password(getenv, c); pw != "" {
			u.User = url.UserPassword(c.User, pw)
		} else {
			u.User = url.User(c.User)
		}
	}
	return u.String()
}

// WALSegmentName returns the WAL file name of the segment containing lsn.
func WALSegmentName(timeline uint32, lsn, segSize uint64) string {
	segno := lsn / segSize
	perID := uint64(0x100000000) / segSize
	return fmt.Sprintf("%08X%08X%08X", timeline, segno/perID, segno%perID)
}

func parseSize(s string) (uint64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := uint64(1)
	for suf, m := range map[string]uint64{"KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30} {
		if strings.HasSuffix(s, suf) {
			mult, s = m, strings.TrimSuffix(s, suf)
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n * mult, err
}

// ArchiveWAL streams WAL until ctx ends. It resumes after the newest archived segment of the job.
func (e *Env) ArchiveWAL(ctx context.Context, job backup.Job) error {
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
	slot := "strand_" + strings.ReplaceAll(strings.ToLower(job.ID), "-", "_")
	conn, err := connectBackupPG(ctx, e.Getenv, job.Conn, "", true)
	if err != nil {
		return fmt.Errorf("replication connection: %v", redactAll(err.Error(), []string{password(e.Getenv, job.Conn)}))
	}
	defer conn.Close(context.Background())
	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return err
	}
	res, err := conn.Exec(ctx, "SHOW wal_segment_size").ReadAll()
	if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
		return fmt.Errorf("read wal_segment_size: %v", err)
	}
	segSize, err := parseSize(string(res[0].Rows[0][0]))
	if err != nil || segSize == 0 {
		return fmt.Errorf("invalid wal_segment_size")
	}
	start := uint64(sys.XLogPos) / segSize * segSize
	last, err := e.latestOriginalLog(ctx, job.ID)
	if err != nil {
		return err
	}
	if last != nil && last.Method == backup.MethodWAL {
		if last.Details["source_identity"] != sys.SystemID || last.Details["timeline"] != strconv.FormatUint(uint64(sys.Timeline), 10) {
			return errors.New("WAL archive cannot resume: prior log does not prove the current PostgreSQL system and timeline")
		}
		v, err := ParseLSN(last.EndPosition.LSN)
		if err != nil || v == 0 || v%segSize != 0 {
			return errors.New("WAL archive cannot resume from an invalid segment boundary")
		}
		start = v
	}
	if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, slot, "", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.PhysicalReplication, SnapshotAction: "RESERVE_WAL"}); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42710" { // duplicate_object: slot exists
			return fmt.Errorf("create replication slot %s: %w", slot, err)
		}
	}
	if err := pglogrepl.StartReplication(ctx, conn, slot, pglogrepl.LSN(start), pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.PhysicalReplication, Timeline: sys.Timeline}); err != nil {
		return fmt.Errorf("start replication at %s: %w", FormatLSN(start), err)
	}
	e.logf("wal archive %s: streaming from %s (timeline %d, segment %d bytes)", job.Source, FormatLSN(start), sys.Timeline, segSize)
	w := &walWriter{env: e, job: job, repo: repo, enc: enc, segSize: segSize, sourceID: sys.SystemID, timeline: uint32(sys.Timeline), segStart: start}
	w.buf = make([]byte, 0, segSize)
	flushEvery := time.Duration(job.LogFlushSeconds) * time.Second
	status := time.Now()
	for {
		if time.Since(status) > 10*time.Second {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(w.segStart + uint64(len(w.buf))),
				WALFlushPosition: pglogrepl.LSN(w.segStart), WALApplyPosition: pglogrepl.LSN(w.segStart)}); err != nil {
				return err
			}
			status = time.Now()
		}
		if flushEvery > 0 && len(w.buf) > 0 && time.Since(w.first) > flushEvery && time.Since(w.switched) > flushEvery {
			w.switched = time.Now()
			go e.switchWAL(job)
		}
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}
		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			if er, ok := msg.(*pgproto3.ErrorResponse); ok {
				return fmt.Errorf("replication error: %s", er.Message)
			}
			continue
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err == nil && pkm.ReplyRequested {
				status = time.Time{}
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return err
			}
			if err := w.add(ctx, uint64(xld.WALStart), xld.WALData); err != nil {
				return err
			}
			if w.flushed {
				w.flushed = false
				status = time.Time{} // acknowledge the archived segment so the slot releases it
			}
		}
	}
}

func (e *Env) switchWAL(job backup.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := "postgres"
	if len(job.Conn.Databases) > 0 {
		db = job.Conn.Databases[0]
	}
	conn, err := connectBackupPG(ctx, e.Getenv, job.Conn, db, false)
	if err != nil {
		e.logf("wal archive: pg_switch_wal connection failed")
		return
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "SELECT pg_switch_wal()").ReadAll(); err != nil {
		e.logf("wal archive: pg_switch_wal: %v", err)
	}
}

type walWriter struct {
	env      *Env
	job      backup.Job
	repo     *Repository
	enc      backup.EncodeOptions
	segSize  uint64
	sourceID string
	timeline uint32
	segStart uint64
	buf      []byte
	first    time.Time
	last     time.Time
	switched time.Time
	flushed  bool
}

func (w *walWriter) add(ctx context.Context, at uint64, data []byte) error {
	for len(data) > 0 {
		if at < w.segStart+uint64(len(w.buf)) {
			skip := w.segStart + uint64(len(w.buf)) - at
			if skip >= uint64(len(data)) {
				return nil
			}
			data, at = data[skip:], at+skip
		}
		if at != w.segStart+uint64(len(w.buf)) {
			return fmt.Errorf("WAL stream jumped from %s to %s", FormatLSN(w.segStart+uint64(len(w.buf))), FormatLSN(at))
		}
		if len(w.buf) == 0 {
			w.first = time.Now().UTC()
		}
		room := w.segSize - uint64(len(w.buf))
		n := uint64(len(data))
		if n > room {
			n = room
		}
		w.buf = append(w.buf, data[:n]...)
		w.last = time.Now().UTC()
		data, at = data[n:], at+n
		if uint64(len(w.buf)) == w.segSize {
			if err := w.upload(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *walWriter) upload(ctx context.Context) error {
	name := WALSegmentName(w.timeline, w.segStart, w.segSize)
	segno := w.segStart / w.segSize
	end := w.segStart + w.segSize
	first, last := w.first, w.last
	rec, err := w.env.storeLog(ctx, w.job, w.repo, w.enc, logSegment{
		ID: "wal-" + safeSegment(w.job.ID) + "-" + name, Name: "wal/" + name, Data: bytes.NewReader(w.buf),
		Method: backup.MethodWAL, Seq: segno,
		Meta: backup.Meta{Position: backup.Position{LSN: FormatLSN(w.segStart)}, EndPosition: backup.Position{LSN: FormatLSN(end)},
			StartTime: &first, EndTime: &last, Details: map[string]string{"timeline": strconv.Itoa(int(w.timeline)), "segment": name, "source_identity": w.sourceID}},
	})
	if err != nil {
		return fmt.Errorf("archive WAL segment %s: %w", name, err)
	}
	w.env.logf("wal archive: %s archived (%s)", name, rec.ID)
	w.segStart, w.buf, w.flushed = end, w.buf[:0], true
	return nil
}
