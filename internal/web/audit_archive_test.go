package web

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/web/store"
)

// Regression: audit_log grew forever (append-only, no retention). Old rows are now archived: exported to a
// verified .jsonl.gz with a sha256 manifest, removed, and an audit.archive entry records it.
func TestAuditArchiveExportsVerifiesAndRemoves(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		ctx := context.Background()
		st := mk()
		old := time.Now().Add(-400 * 24 * time.Hour)
		for i := 0; i < 50; i++ {
			d, _ := json.Marshal(AuditEntry{Time: old, Action: "start", Pipeline: "orders", Result: "ok", Detail: map[string]any{"i": i}})
			if err := st.AppendAudit(ctx, store.AuditRow{Time: old.Add(time.Duration(i) * time.Minute), Action: "start", Pipeline: "orders", Result: "ok", Data: d}); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.AppendAudit(ctx, store.AuditRow{Time: time.Now(), Action: "login", Result: "ok", Data: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		cfg := &Config{StateDir: dir}
		var out bytes.Buffer
		// Dry run: nothing written or removed.
		if _, err := auditArchiveWith(ctx, st, cfg, AuditArchiveCLIOptions{DryRun: true, W: &out}); err != nil || !strings.Contains(out.String(), "dry run: 50 audit rows") {
			t.Fatalf("dry run: %v %s", err, out.String())
		}
		res, err := auditArchiveWith(ctx, st, cfg, AuditArchiveCLIOptions{W: &out}) // default retention 365 days
		if err != nil || res.Deleted != 50 {
			t.Fatalf("archive: %+v %v", res, err)
		}
		if filepath.Dir(res.File) != filepath.Join(dir, "audit-archive") {
			t.Fatalf("file %s not in the default archive dir", res.File)
		}
		b, err := os.ReadFile(res.File)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != res.SHA256 {
			t.Fatal("sha256 of the file differs from the reported one")
		}
		if sf, _ := os.ReadFile(res.File + ".sha256"); !strings.HasPrefix(string(sf), res.SHA256+"  "+filepath.Base(res.File)) {
			t.Fatalf("sha256 sidecar = %q", sf)
		}
		if fi, _ := os.Stat(res.File); fi.Mode().Perm() != 0o600 {
			t.Fatalf("archive mode %v", fi.Mode().Perm())
		}
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(zr)
		n := 0
		for sc.Scan() {
			var l auditArchiveLine
			if err := json.Unmarshal(sc.Bytes(), &l); err != nil || l.Action != "start" || !strings.Contains(string(l.Data), `"i":`) {
				t.Fatalf("line %d: %s (%v)", n, sc.Text(), err)
			}
			n++
		}
		if n != 50 {
			t.Fatalf("file holds %d lines", n)
		}
		rows, _ := st.ListAudit(ctx, 10)
		if len(rows) != 2 || rows[0].Action != "audit.archive" || !strings.Contains(string(rows[0].Data), res.SHA256) {
			t.Fatalf("audit after archive: %+v", rows)
		}
		// Running again: nothing left.
		out.Reset()
		if _, err := auditArchiveWith(ctx, st, cfg, AuditArchiveCLIOptions{W: &out}); err != nil || !strings.Contains(out.String(), "nothing to archive") {
			t.Fatalf("second run: %v %s", err, out.String())
		}
	})
}

// The automatic archive of the web: off by default; on, the active instance archives with the retention.
func TestAuditArchiveAutomaticInWeb(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	ctx := context.Background()
	if err := st.AppendAudit(ctx, store.AuditRow{Time: time.Now().Add(-40 * 24 * time.Hour), Action: "start", Data: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	e.srv.auditMaintenance(ctx)
	if ar, _ := st.ListAuditArchives(ctx, 5); len(ar) != 0 {
		t.Fatal("archived although audit_archive_auto is off")
	}
	c, csrf := e.loginOK()
	resp, body := e.do(c, "POST", "/api/v1/settings", `{"audit_archive_auto": true, "audit_retention_days": 30}`, map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("settings: %d %s", resp.StatusCode, body)
	}
	e.srv.auditMaintenance(ctx)
	ar, _ := st.ListAuditArchives(ctx, 5)
	if len(ar) != 1 || ar[0].Count != 1 || !strings.HasPrefix(ar[0].File, filepath.Join(e.srv.cfg.StateDir, "audit-archive")) {
		t.Fatalf("archives = %+v", ar)
	}
}

// File mode: audit.log is rotated by size (and age) into audit-<ts>.log.gz + .sha256; the new file starts with
// an audit.rotate line naming the previous file and its sha256.
func TestAuditLogRotation(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a, err := OpenAuditLog(dir, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.rotateBytes = func() int64 { return 2048 }
	for i := 0; i < 40; i++ {
		if err := a.Log(AuditEntry{Action: "login", IP: strings.Repeat("x", 40)}); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	gz, _ := filepath.Glob(filepath.Join(dir, "audit-*.log.gz"))
	if len(gz) == 0 {
		t.Fatal("no rotated file")
	}
	b, _ := os.ReadFile(gz[0])
	sum := sha256.Sum256(b)
	if sf, _ := os.ReadFile(gz[0] + ".sha256"); !strings.HasPrefix(string(sf), hex.EncodeToString(sum[:])) {
		t.Fatalf("sidecar = %q", sf)
	}
	if raw, _ := filepath.Glob(filepath.Join(dir, "audit-*.log")); len(raw) != 0 {
		t.Fatalf("uncompressed copy left: %v", raw)
	}
	cur, _ := os.ReadFile(filepath.Join(dir, "audit.log"))
	if fi, _ := os.Stat(filepath.Join(dir, "audit.log")); fi.Size() >= 2048+200 {
		t.Fatalf("audit.log not bounded: %d", fi.Size())
	}
	_ = cur
	// Chain: some file starts with audit.rotate naming the first archive and its sha256.
	found := false
	for _, f := range append(gz[1:], filepath.Join(dir, "audit.log")) {
		var first []byte
		if strings.HasSuffix(f, ".gz") {
			fb, _ := os.ReadFile(f)
			zr, _ := gzip.NewReader(bytes.NewReader(fb))
			first, _ = bufio.NewReader(zr).ReadBytes('\n')
		} else {
			fb, _ := os.ReadFile(f)
			first, _, _ = bytes.Cut(fb, []byte("\n"))
		}
		if bytes.Contains(first, []byte(`"audit.rotate"`)) && bytes.Contains(first, []byte(filepath.Base(gz[0]))) && bytes.Contains(first, []byte(hex.EncodeToString(sum[:]))) {
			found = true
		}
	}
	if !found {
		t.Fatal("no audit.rotate line chaining to the first rotated file")
	}
	// Age: a file whose first line is older than the limit is rotated.
	before, _ := filepath.Glob(filepath.Join(dir, "audit-*.log.gz"))
	now = now.Add(48 * time.Hour)
	if err := a.rotateIfOlder(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if after, _ := filepath.Glob(filepath.Join(dir, "audit-*.log.gz")); len(after) != len(before)+1 {
		t.Fatalf("age rotation: %d -> %d files", len(before), len(after))
	}
}
