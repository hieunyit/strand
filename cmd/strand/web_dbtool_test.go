package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/testutil"
	"strand/internal/web"
)

func runWebDB(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := newWebDBCmd()
	var so bytes.Buffer
	cmd.SetOut(&so)
	cmd.SetErr(&so)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return so.String(), err
}

// legacy file-mode web.yaml -> `web db migrate --dry-run` (nothing written) -> migrate (verified) ->
// re-run -> `web db export`, on a real PostgreSQL; then `init --database-dsn` writes database.dsn.
func TestWebDBMigrateAndExportCLI(t *testing.T) {
	dsn := testutil.NewPostgresDSN(t)
	t.Setenv(web.EnvDatabaseDSN, "")
	t.Setenv(web.EnvSecretKey, "0123456789abcdef0123456789abcdef")
	d := t.TempDir()
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = filepath.Join(d, "web.yaml")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// web.yaml của một bản cài chế độ file cũ (trước khi web bắt buộc PostgreSQL).
	h, err := web.HashPassword("file-mode-pass-1")
	must(err)
	must(os.MkdirAll(filepath.Join(d, "st"), 0o750))
	must(os.WriteFile(cfgPath, []byte("listen: 127.0.0.1:8080\nstate_dir: "+filepath.Join(d, "st")+"\nscan:\n  - "+d+"/p/*.yaml\n"+
		"password_hash: \""+h+"\"\napi_token_hash: \""+web.HashAPIToken("strand_legacy")+"\"\n"), 0o600))
	must(os.MkdirAll(filepath.Join(d, "p"), 0o750))
	must(os.WriteFile(filepath.Join(d, "p", "scanned.yaml"), []byte("version: 1\n"), 0o640))
	must(os.WriteFile(filepath.Join(d, "p", "scanned.env"), []byte("PW=topsecret\n"), 0o600))
	must(os.WriteFile(filepath.Join(d, "st", "audit.log"), []byte(`{"time":"2026-01-01T00:00:00Z","ip":"1.1.1.1","action":"login","result":"ok"}`+"\n"), 0o600))

	out, err := runWebDB(t, "migrate", "--dsn", dsn, "--dry-run")
	if err != nil || !strings.Contains(out, "dry run: nothing was written") || !strings.Contains(out, "schema at version 0") {
		t.Fatalf("dry run: %v\n%s", err, out)
	}
	if strings.Contains(out, "file-mode-pass") || strings.Contains(out, "topsecret") {
		t.Fatal("secret in output")
	}
	out, err = runWebDB(t, "migrate", "--dsn", dsn)
	if err != nil || !strings.Contains(out, "verified:") {
		t.Fatalf("migrate: %v\n%s", err, out)
	}
	out, err = runWebDB(t, "migrate", "--dsn", dsn)
	if err != nil || !strings.Contains(out, "verified:") {
		t.Fatalf("re-run: %v\n%s", err, out)
	}
	st := openTestStore(t, dsn)
	ctx := context.Background()
	row, ok, _ := st.GetPipeline(ctx, "scanned")
	if !ok || row.Origin != "scan" || len(row.EnvEnc) == 0 {
		t.Fatalf("scanned pipeline not migrated: %+v", row)
	}
	if n, _ := st.CountAudit(ctx); n != 1 {
		t.Fatalf("audit rows = %d, want 1", n)
	}
	exp := filepath.Join(d, "export")
	out, err = runWebDB(t, "export", "--dsn", dsn, "--dir", exp)
	if err != nil || !strings.Contains(out, "exported to") {
		t.Fatalf("export: %v\n%s", err, out)
	}
	if b, _ := os.ReadFile(filepath.Join(exp, "pipelines", "scanned.env")); string(b) != "PW=topsecret\n" {
		t.Fatalf("exported env = %q", b)
	}
	wy, err := web.LoadConfig(filepath.Join(exp, "web.yaml"))
	if err != nil || wy.PasswordHash == "" || wy.APITokenHash == "" || wy.DBEnabled() {
		t.Fatalf("exported web.yaml: %v %+v", err, wy)
	}

	// init --database-dsn: database.dsn in web.yaml, admin in the DB.
	cfgPath = filepath.Join(d, "web2.yaml")
	if _, _, err := runWebInit(t, "db-mode-pass-12\n", "--password-stdin", "--quiet", "--state-dir", filepath.Join(d, "st2"), "--database-dsn", dsn); err != nil {
		t.Fatal(err)
	}
	wc, err := web.LoadConfig(cfgPath)
	if err != nil || wc.Database.DSN != dsn {
		t.Fatalf("database.dsn not written: %v", err)
	}
	if b, _ := os.ReadFile(cfgPath); strings.Contains(string(b), "\npassword_hash:") {
		t.Fatal("password hash written to web.yaml in DB mode")
	}
}
