package web

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"strand/internal/web/store"
)

// Audit retention (DB mode). audit_log is append-only; old rows leave it only through an archive:
//
//  1. plan: rows with ts < cutoff (cutoff = now - audit_retention_days, or --before), audit.archive entries
//     excluded; nothing to do when there are none;
//  2. export them, ordered by id, to a gzip-compressed JSONL file (temporary name, fsync, rename), 0600;
//  3. verify the file: decompress, count lines, check the first/last id, recompute the sha256;
//  4. write <file>.sha256 (sha256sum format) and <file>.manifest.json next to it;
//  5. strand_audit_archive() (migration 009) deletes exactly those rows (all or nothing: the count must still
//     match) and records the manifest row (audit_archives) and an `audit.archive` entry with the count, the
//     range and the file's sha256, in one transaction.
//
// If step 5 fails, the file stays and the rows stay: running again exports them again (a new file). The archive
// files are never deleted by Strand: keep or move them according to your retention policy.

// AuditArchiveOptions configure ArchiveAuditRows.
type AuditArchiveOptions struct {
	Before time.Time // rows strictly older than this are archived
	Out    string    // output file (.jsonl.gz); "" = <Dir>/audit-<from>-<to>.jsonl.gz
	Dir    string    // directory when Out is empty
	DryRun bool      // plan only
	By     string    // who runs it (cli | web-auto), recorded in the audit.archive entry
	Log    *slog.Logger
}

// AuditArchiveResult reports an archive.
type AuditArchiveResult struct {
	Plan     store.AuditArchivePlan `json:"plan"`
	File     string                 `json:"file,omitempty"`
	SHA256   string                 `json:"sha256,omitempty"`
	Manifest string                 `json:"manifest,omitempty"`
	Bytes    int64                  `json:"bytes,omitempty"`
	Deleted  int64                  `json:"deleted"`
	DryRun   bool                   `json:"dry_run,omitempty"`
}

// auditArchiveLine is one line of the export (same columns as audit_log; data is the original JSON entry).
type auditArchiveLine struct {
	ID       int64           `json:"id"`
	Time     time.Time       `json:"ts"`
	IP       string          `json:"ip"`
	Action   string          `json:"action"`
	Pipeline string          `json:"pipeline"`
	Result   string          `json:"result"`
	Data     json.RawMessage `json:"data"`
}

// ArchiveAuditRows runs the archive (see above) under a cross-instance lock.
func ArchiveAuditRows(ctx context.Context, st store.Store, o AuditArchiveOptions) (*AuditArchiveResult, error) {
	if o.Before.IsZero() {
		return nil, errors.New("audit archive: no cutoff")
	}
	if o.Before.After(time.Now().Add(-store.MinAuditArchiveAge)) {
		return nil, fmt.Errorf("audit archive: the cutoff %s is less than one day old", o.Before.Format(time.RFC3339))
	}
	var res *AuditArchiveResult
	err := st.Locked(ctx, "audit-archive", func(ctx context.Context) error {
		var err error
		res, err = archiveLocked(ctx, st, o)
		return err
	})
	return res, err
}

func archiveLocked(ctx context.Context, st store.Store, o AuditArchiveOptions) (*AuditArchiveResult, error) {
	pl, err := st.PlanAuditArchive(ctx, o.Before)
	if err != nil {
		return nil, err
	}
	res := &AuditArchiveResult{Plan: pl, DryRun: o.DryRun}
	if pl.Count == 0 || o.DryRun {
		return res, nil
	}
	out := o.Out
	if out == "" {
		if o.Dir == "" {
			return nil, errors.New("audit archive: no output file or directory")
		}
		out = filepath.Join(o.Dir, fmt.Sprintf("audit-%s-%s.jsonl.gz", pl.MinTS.UTC().Format("20060102T150405Z"), pl.MaxTS.UTC().Format("20060102T150405Z")))
	}
	if !strings.HasSuffix(out, ".gz") {
		out += ".gz"
	}
	if _, err := os.Stat(out); err == nil {
		return nil, fmt.Errorf("audit archive: %s already exists", out)
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o750); err != nil {
		return nil, fmt.Errorf("audit archive: %w", err)
	}
	sum, size, err := writeAuditExport(ctx, st, pl, out)
	if err != nil {
		return nil, err
	}
	if err := verifyAuditExport(out, pl, sum); err != nil {
		return nil, fmt.Errorf("audit archive: verification of %s failed, nothing deleted: %w", out, err)
	}
	base := filepath.Base(out)
	if err := writeFileSync(out+".sha256", []byte(sum+"  "+base+"\n")); err != nil {
		return nil, err
	}
	man, _ := json.MarshalIndent(map[string]any{"file": base, "sha256": sum, "bytes": size, "rows": pl.Count, "min_id": pl.MinID,
		"max_id": pl.MaxID, "from": pl.MinTS.UTC(), "to": pl.MaxTS.UTC(), "before": pl.Before.UTC(), "created": time.Now().UTC(),
		"format": "gzip-compressed JSON lines, one audit_log row per line (id, ts, ip, action, pipeline, result, data)"}, "", "  ")
	if err := writeFileSync(out+".manifest.json", append(man, '\n')); err != nil {
		return nil, err
	}
	by := o.By
	if by == "" {
		by = "cli"
	}
	host, _ := os.Hostname()
	n, err := st.ArchiveAudit(ctx, store.AuditArchive{Before: pl.Before, MaxID: pl.MaxID, Count: pl.Count, File: out, SHA256: sum,
		Detail: map[string]any{"by": by, "host": host, "bytes": size}})
	if err != nil {
		return nil, fmt.Errorf("audit archive: rows exported to %s (verified) but not removed: %w", out, err)
	}
	res.File, res.SHA256, res.Manifest, res.Bytes, res.Deleted = out, sum, out+".manifest.json", size, n
	return res, nil
}

// writeAuditExport writes the rows of pl to path (via a temporary file) and returns the file's sha256 and size.
func writeAuditExport(ctx context.Context, st store.Store, pl store.AuditArchivePlan, path string) (string, int64, error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", 0, fmt.Errorf("audit archive: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	h := sha256.New()
	cw := &countWriter{w: io.MultiWriter(f, h)}
	bw := bufio.NewWriterSize(cw, 256<<10)
	zw := gzip.NewWriter(bw)
	enc := json.NewEncoder(zw)
	var n int64
	err = st.ScanAudit(ctx, pl.Before, pl.MaxID, func(r store.AuditRow) error {
		n++
		data := json.RawMessage(r.Data)
		if !json.Valid(data) {
			b, _ := json.Marshal(string(r.Data))
			data = b
		}
		return enc.Encode(auditArchiveLine{ID: r.ID, Time: r.Time.UTC(), IP: r.IP, Action: r.Action, Pipeline: r.Pipeline, Result: r.Result, Data: data})
	})
	if err != nil {
		return "", 0, fmt.Errorf("audit archive: export: %w", err)
	}
	if n != pl.Count {
		return "", 0, fmt.Errorf("audit archive: exported %d rows, planned %d (rows changed meanwhile); nothing deleted", n, pl.Count)
	}
	if err := zw.Close(); err != nil {
		return "", 0, err
	}
	if err := bw.Flush(); err != nil {
		return "", 0, err
	}
	if err := f.Sync(); err != nil {
		return "", 0, err
	}
	if err := f.Close(); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", 0, err
	}
	ok = true
	syncDir(filepath.Dir(path))
	return hex.EncodeToString(h.Sum(nil)), cw.n, nil
}

// verifyAuditExport re-reads the file: sha256, gzip integrity, line count and id range.
func verifyAuditExport(path string, pl store.AuditArchivePlan, sum string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	zr, err := gzip.NewReader(io.TeeReader(f, h))
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	var n, first, last int64
	for sc.Scan() {
		var l auditArchiveLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return fmt.Errorf("line %d: %w", n+1, err)
		}
		if n == 0 {
			first = l.ID
		}
		if l.ID <= last {
			return fmt.Errorf("line %d: ids not increasing", n+1)
		}
		last = l.ID
		n++
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if _, err := io.Copy(io.Discard, zr); err != nil {
		return err
	}
	if _, err := io.Copy(h, f); err != nil { // trailing bytes after the gzip stream, if any
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sum {
		return fmt.Errorf("sha256 %s, written %s", got, sum)
	}
	if n != pl.Count || first != pl.MinID || last != pl.MaxID {
		return fmt.Errorf("file holds %d rows (ids %d..%d), planned %d (ids %d..%d)", n, first, last, pl.Count, pl.MinID, pl.MaxID)
	}
	return nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func writeFileSync(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// ---- in the web: settings and the optional automatic archive ----

// auditArchiveDir: archive directory (setting audit_archive_dir, default <state_dir>/audit-archive).
func (s *Server) auditArchiveDir() string {
	if d := s.settingStr("audit_archive_dir"); d != "" {
		return d
	}
	return filepath.Join(s.cfg.StateDir, "audit-archive")
}

func (s *Server) settingInt(key string) int { v, _ := s.live.get(key).(int); return v }

// auditMaintenance: daily (and shortly after start). DB mode, active instance, audit_archive_auto on and a
// retention > 0: archive the rows older than the retention. Both modes: rotate the audit.log file copy by age.
func (s *Server) auditMaintenance(ctx context.Context) {
	if d := s.settingInt("audit_rotate_days"); d > 0 {
		if err := s.audit.rotateIfOlder(time.Duration(d) * 24 * time.Hour); err != nil {
			s.log.Warn("rotate audit.log failed", "err", err)
		}
	}
	if !s.isActive() || !s.settingBool("audit_archive_auto") {
		return
	}
	days := s.settingInt("audit_retention_days")
	if days <= 0 {
		return
	}
	actx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	res, err := ArchiveAuditRows(actx, s.db.st, AuditArchiveOptions{Before: time.Now().Add(-time.Duration(days) * 24 * time.Hour),
		Dir: s.auditArchiveDir(), By: "web-auto", Log: s.log})
	if err != nil {
		s.log.Error("automatic audit archive failed", "err", err)
		_ = s.audit.Log(AuditEntry{Action: "audit.archive", Result: "error", Detail: map[string]any{"error": err.Error(), "by": "web-auto"}})
		return
	}
	if res.Deleted > 0 {
		s.log.Info("archived old audit rows", "rows", res.Deleted, "file", res.File, "sha256", res.SHA256)
	}
}

var auditMaintEvery = 24 * time.Hour

func (s *Server) runAuditMaintenance(ctx context.Context) {
	first := time.NewTimer(time.Minute)
	defer first.Stop()
	t := time.NewTicker(auditMaintEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-first.C:
		case <-t.C:
		}
		s.auditMaintenance(ctx)
	}
}
