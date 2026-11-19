package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/exitcode"
	"strand/internal/objectstore"
	"strand/internal/testutil"
	"strand/internal/web"
	"strand/internal/web/store"
)

func runBackup(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := newBackupCmd()
	var so, se bytes.Buffer
	cmd.SetOut(&so)
	cmd.SetErr(&se)
	cmd.SetArgs(args)
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	err := cmd.ExecuteContext(context.Background())
	return so.String(), se.String(), err
}

func exitOf(err error) int {
	var e *exitcode.Error
	if errors.As(err, &e) {
		return e.ExitCode
	}
	if err != nil {
		return 1
	}
	return 0
}

// End to end against real PostgreSQL + MinIO: push (multipart, parallel) -> list -> fetch verified;
// crash recovery (stale "uploading" record + orphaned multipart upload); refusal to overwrite; tamper
// detection; audit rows for every operation; usage errors exit 2.
func TestBackupPushListFetchCLI(t *testing.T) {
	dsn := testutil.NewPostgresDSN(t)
	endpoint := testutil.MinIOEndpoint(t)
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = ""
	t.Setenv(web.EnvDatabaseDSN, dsn)
	t.Setenv("STRAND_BACKUP_S3_ENDPOINT", endpoint)
	t.Setenv("STRAND_BACKUP_S3_ACCESS_KEY", "minioadmin")
	t.Setenv("STRAND_BACKUP_S3_SECRET_KEY", "minioadmin")
	t.Setenv("STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL", "true")
	t.Setenv("STRAND_BACKUP_TMPDIR", t.TempDir())
	ctx := context.Background()
	obj, err := objectstore.New(objectstore.Config{Endpoint: endpoint, Bucket: "central", AccessKey: "minioadmin", SecretKey: "minioadmin", AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.CreateBucket(ctx); err != nil {
		t.Fatal(err)
	}

	d := t.TempDir()
	manifest := filepath.Join(d, "seed.json")
	os.WriteFile(manifest, []byte(`{"version":1,"engine":"mysql","kind":"mysqldump","tool_version":"8.4.3","source_identity":"mysql-prod-1",
		"backup_ref":"/local/dump.sql","start_position":{"gtid_set":"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77"},"tables":["app.t"],"created_at":"2026-11-19T00:00:00Z"}`), 0o600)
	artifact := filepath.Join(d, "dump.sql")
	data := bytes.Repeat([]byte("INSERT INTO t VALUES (1,'x');\n"), 400_000) // ~11.6 MiB: multipart with 5 MiB parts
	os.WriteFile(artifact, data, 0o600)
	sum := sha256.Sum256(data)

	// Usage errors exit 2 and never touch storage.
	if _, _, err := runBackup(t, "push", "--manifest", manifest); exitOf(err) != exitcode.InvalidSyntax {
		t.Fatalf("missing flags exit = %d (%v)", exitOf(err), err)
	}

	// Simulate an uploader killed mid-transfer: record stuck in "uploading" + an open multipart upload.
	st, err := store.Open(ctx, dsn, store.OpenOptions{MaxWait: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cat, _ := backup.NewCatalog(st)
	rec := backup.Record{ID: "bkp-1", Source: "mysql-prod-1", Engine: "mysql", Tool: "mysqldump", ToolVersion: "8.4.3",
		Bucket: "central", ObjectKey: "prod/mysql/bkp-1/artifact", ManifestURI: "s3://central/prod/mysql/bkp-1/manifest.json",
		Position: backup.Position{GTIDSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77"}, Status: backup.StatusQueued, CreatedAt: time.Now().UTC()}
	if _, _, err := cat.Create(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cat.UpdateStatus(ctx, "bkp-1", backup.StatusUploading, nil); err != nil {
		t.Fatal(err)
	}
	orphanSrc := &slowReader{n: 12 << 20}
	octx, ocancel := context.WithCancel(ctx)
	orphanSrc.after = ocancel
	small, _ := objectstore.New(objectstore.Config{Endpoint: endpoint, Bucket: "central", AccessKey: "minioadmin", SecretKey: "minioadmin", AllowInsecureLocal: true, PartSize: 5 << 20, Concurrency: 1})
	_, _ = small.Upload(octx, "prod/mysql/bkp-1/artifact", orphanSrc, -1, "") // cancelled; aborts itself
	// A truly killed process cannot abort: leave one open upload behind deliberately.
	if n, _ := small.AbortStaleUploads(ctx, "prod/mysql/bkp-1/artifact"); n != 0 {
		t.Fatalf("cancelled upload left %d open uploads", n)
	}
	leaveOpenUpload(t, endpoint, "prod/mysql/bkp-1/artifact")

	out, errOut, err := runBackup(t, "push", "--manifest", manifest, "--artifact", artifact, "--bucket", "central", "--prefix", "prod/mysql", "--id", "bkp-1", "--part-size-mib", "5", "--upload-concurrency", "3")
	if err != nil {
		t.Fatalf("push: %v\n%s%s", err, out, errOut)
	}
	if !strings.Contains(errOut, "aborted 1 unfinished multipart upload") || !strings.Contains(out, "backup bkp-1 complete") {
		t.Fatalf("push output: %s / %s", out, errOut)
	}
	if n, _ := small.AbortStaleUploads(ctx, "prod/mysql/bkp-1/artifact"); n != 0 {
		t.Fatalf("%d multipart uploads still open after push", n)
	}
	got, _, _ := cat.Get(ctx, "bkp-1")
	if got.Status != backup.StatusComplete || got.SHA256 != hex.EncodeToString(sum[:]) || got.Bytes != int64(len(data)) {
		t.Fatalf("catalog record = %+v", got)
	}
	// Idempotent re-run.
	if out, _, err := runBackup(t, "push", "--manifest", manifest, "--artifact", artifact, "--bucket", "central", "--prefix", "prod/mysql", "--id", "bkp-1"); err != nil || !strings.Contains(out, "already complete") {
		t.Fatalf("re-push: %v %s", err, out)
	}
	// Directory artifact (tar stream, unknown size) with a generated ID.
	dir := filepath.Join(d, "xb")
	os.MkdirAll(filepath.Join(dir, "sub"), 0o700)
	os.WriteFile(filepath.Join(dir, "sub", "ibdata1"), data[:1<<20], 0o600)
	if out, errOut, err := runBackup(t, "push", "--manifest", manifest, "--artifact", dir, "--bucket", "central"); err != nil {
		t.Fatalf("dir push: %v %s %s", err, out, errOut)
	}

	out, _, err = runBackup(t, "list", "--source", "mysql-prod-1", "--limit", "1")
	if err != nil || strings.Count(out, "mysql-prod-1") != 1 {
		t.Fatalf("list: %v\n%s", err, out)
	}
	out, _, err = runBackup(t, "list", "--json", "--status", "complete")
	if err != nil || !strings.Contains(out, `"bkp-1"`) {
		t.Fatalf("list json: %v %s", err, out)
	}

	dest := filepath.Join(d, "restore", "dump.sql")
	os.MkdirAll(filepath.Dir(dest), 0o700)
	if out, errOut, err := runBackup(t, "fetch", "--id", "bkp-1", "--out", dest, "--with-manifest"); err != nil {
		t.Fatalf("fetch: %v %s %s", err, out, errOut)
	}
	if b, _ := os.ReadFile(dest); !bytes.Equal(b, data) {
		t.Fatal("fetched artifact differs")
	}
	if m, _ := os.ReadFile(dest + ".manifest.json"); !strings.Contains(string(m), `"backup_ref": "s3://central/prod/mysql/bkp-1/artifact"`) {
		t.Fatalf("manifest sidecar = %s", m)
	}
	// Never overwrite.
	if _, _, err := runBackup(t, "fetch", "--id", "bkp-1", "--out", dest); exitOf(err) != exitcode.InvalidSyntax {
		t.Fatalf("overwrite exit = %d (%v)", exitOf(err), err)
	}
	// Tampered object (same size): fetch fails with a data error and leaves nothing behind.
	bad := append([]byte(nil), data...)
	bad[len(bad)/2] ^= 1
	if _, err := obj.Upload(ctx, "prod/mysql/bkp-1/artifact", bytes.NewReader(bad), int64(len(bad)), ""); err != nil {
		t.Fatal(err)
	}
	dest2 := filepath.Join(d, "restore", "again.sql")
	if _, _, err := runBackup(t, "fetch", "--id", "bkp-1", "--out", dest2); exitOf(err) != exitcode.DataError {
		t.Fatalf("tampered fetch exit = %d (%v)", exitOf(err), err)
	}
	if _, err := os.Stat(dest2); err == nil {
		t.Fatal("tampered artifact was published")
	}
	if ents, _ := filepath.Glob(filepath.Join(d, "restore", ".strand-fetch-*")); len(ents) != 0 {
		t.Fatalf("temporary download files left: %v", ents)
	}

	rows, err := st.ListAudit(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	var acts []string
	for _, r := range rows {
		acts = append(acts, r.Action+":"+r.Result)
		if strings.Contains(string(r.Data), "minioadmin") {
			t.Fatalf("audit row contains credentials: %s", r.Data)
		}
	}
	joined := strings.Join(acts, ",")
	for _, want := range []string{"backup.push:ok", "backup.fetch:ok", "backup.fetch:error"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("audit = %s, missing %s", joined, want)
		}
	}
}

type slowReader struct {
	n, read int
	after   func()
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.read >= r.n {
		if r.after != nil {
			r.after()
		}
		time.Sleep(100 * time.Millisecond)
		return 0, fmt.Errorf("stopped")
	}
	if len(p) > r.n-r.read {
		p = p[:r.n-r.read]
	}
	for i := range p {
		p[i] = 'x'
	}
	r.read += len(p)
	return len(p), nil
}

// leaveOpenUpload initiates a multipart upload and uploads nothing, like a process killed with SIGKILL.
func leaveOpenUpload(t *testing.T, endpoint, key string) {
	t.Helper()
	c, _ := objectstore.New(objectstore.Config{Endpoint: endpoint, Bucket: "central", AccessKey: "minioadmin", SecretKey: "minioadmin", AllowInsecureLocal: true})
	if err := c.InitiateForTest(context.Background(), key); err != nil {
		t.Fatal(err)
	}
}
