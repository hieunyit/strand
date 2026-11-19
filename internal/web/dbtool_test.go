package web

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/web/store"
)

// File mode installation -> `strand web db migrate` (dry run, real, re-run) -> web in DB mode sees everything
// -> `strand web db export` gives back a file-mode tree with the same content.
func TestDBMigrateVerifyAndExport(t *testing.T) {
	cfg, cmd := legacyFileState(t)
	st := store.NewMem()
	box, _ := store.NewBox(dbKey)
	ctx := context.Background()
	var out strings.Builder

	rep, err := migrateWith(ctx, st, cfg, box, MigrateOptions{DryRun: true, Out: &out}, func() (bool, string, error) { return false, "", nil })
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, it := range rep.Items {
		if it.Status != "new" {
			t.Errorf("dry run: %s %s is %s", it.Kind, it.Name, it.Status)
		}
		kinds[it.Kind]++
	}
	if kinds["admin"] != 1 || kinds["pipeline"] != 2 || kinds["job"] != 1 || kinds["job_output"] != 1 || kinds["audit"] != 1 {
		t.Fatalf("plan kinds = %v\n%s", kinds, out.String())
	}
	if n, _ := st.CountAudit(ctx); n != 0 {
		t.Fatal("dry run wrote to the database")
	}

	out.Reset()
	rep, err = migrateWith(ctx, st, cfg, box, MigrateOptions{Out: &out}, nil)
	if err != nil || !rep.Verified {
		t.Fatalf("migrate: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "verified:") {
		t.Fatalf("report:\n%s", out.String())
	}
	auditLines, _ := countAuditLines(filepath.Join(cfg.StateDir, "audit.log"))
	if n, _ := st.CountAudit(ctx); int(n) != auditLines {
		t.Fatalf("audit rows %d, file lines %d", n, auditLines)
	}
	row, _, _ := st.GetPipeline(ctx, "filepipe")
	if row.Origin != store.OriginCreated || len(row.EnvEnc) == 0 {
		t.Fatalf("created pipeline row: %+v", row)
	}
	if row, _, _ := st.GetPipeline(ctx, "orders"); row.Origin != store.OriginFile {
		t.Fatalf("declared pipeline origin %q", row.Origin)
	}
	// Re-run: nothing new, still verified, no duplicate audit rows.
	out.Reset()
	if rep, err = migrateWith(ctx, st, cfg, box, MigrateOptions{Out: &out}, nil); err != nil || !rep.Verified {
		t.Fatalf("re-run: %v\n%s", err, out.String())
	}
	for _, it := range rep.Items {
		if it.Status != "present" {
			t.Errorf("after re-run: %s %s is %s", it.Kind, it.Name, it.Status)
		}
	}
	if n, _ := st.CountAudit(ctx); int(n) != auditLines {
		t.Fatalf("re-run duplicated audit rows: %d", n)
	}

	// Web in DB mode on the migrated database: no second import, pipelines and job output there.
	c2 := *cfg
	srv, err := New(Options{Config: &c2, Version: "test", Commander: cmd, Store: st, SecretKey: dbKey})
	if err != nil {
		t.Fatal(err)
	}
	if srv.reg.Get("filepipe") == nil || srv.reg.Get("orders") == nil {
		t.Fatal("pipelines missing in DB mode")
	}
	jobs := srv.jobs.List("orders")
	if len(jobs) != 1 {
		t.Fatalf("jobs in DB mode: %d", len(jobs))
	}
	b, _ := st.ReadJobOutput(ctx, jobs[0].ID, 0, 1<<20)
	if !strings.Contains(string(b), "line two") {
		t.Fatalf("job output not migrated: %q", b)
	}

	// Export back to files.
	dir := filepath.Join(t.TempDir(), "export")
	out.Reset()
	if err := exportWith(ctx, st, cfg, box, ExportOptions{Dir: dir, Out: &out}); err != nil {
		t.Fatal(err)
	}
	wy, err := LoadConfig(filepath.Join(dir, "web.yaml"))
	if err != nil || wy.PasswordHash != cfg.PasswordHash || wy.DBEnabled() {
		t.Fatalf("exported web.yaml: %v %+v", err, wy)
	}
	env, _ := os.ReadFile(filepath.Join(dir, "pipelines", "filepipe.env"))
	if !strings.Contains(string(env), "SrcSecretPw") {
		t.Fatal(".env not exported decrypted")
	}
	var doc createdFileDoc
	pj, _ := os.ReadFile(filepath.Join(dir, "state", CreatedFile))
	if json.Unmarshal(pj, &doc) != nil || len(doc.Pipelines) != 1 || doc.Pipelines[0].Name != "filepipe" {
		t.Fatalf("pipelines.json: %s", pj)
	}
	lb, _ := os.ReadFile(filepath.Join(dir, "state", "jobs", jobs[0].ID+".log"))
	if string(lb) != string(b) {
		t.Fatal("job log differs")
	}
	f, _ := os.Open(filepath.Join(dir, "state", "audit.log"))
	defer f.Close()
	n := 0
	for sc := bufio.NewScanner(f); sc.Scan(); n++ {
		var a AuditEntry
		if json.Unmarshal(sc.Bytes(), &a) != nil || a.Action == "" {
			t.Fatalf("audit line %d not an entry: %s", n, sc.Text())
		}
	}
	if total, _ := st.CountAudit(ctx); int64(n) != total {
		t.Fatalf("exported %d audit lines, database has %d", n, total)
	}
	if err := exportWith(ctx, st, cfg, box, ExportOptions{Dir: dir}); err == nil {
		t.Fatal("export into a non-empty directory accepted")
	}
}

// legacyFileState dựng trạng thái của một bản cài chế độ file cũ (trước khi web bắt buộc PostgreSQL): web.yaml
// với password_hash và pipeline khai báo, <state_dir>/pipelines.json với một pipeline tạo từ web (.env có bí
// mật), một job verify xong cùng output, audit.log. Dùng cho `strand web db migrate` / nhập lần khởi động đầu.
func legacyFileState(t *testing.T) (*Config, *fakeCmd) {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(state, "jobs"), 0o750))
	write := func(path, data string, perm os.FileMode) { t.Helper(); must(os.WriteFile(path, []byte(data), perm)) }
	spool := filepath.Join(dir, "spool")
	must(os.MkdirAll(spool, 0o750))
	orders := filepath.Join(dir, "orders.yaml")
	write(orders, fmt.Sprintf(pipelineYAML, spool, filepath.Join(dir, "a.sock")), 0o640)
	cfgDir := filepath.Join(dir, "pipelines")
	must(os.MkdirAll(cfgDir, 0o750))
	fp := filepath.Join(cfgDir, "filepipe.yaml")
	write(fp, "version: 1\nname: filepipe\nsource: { kind: mysql, host: 10.0.0.1, user: u, password_env: SRC_PW }\n"+
		"target: { kind: postgres, host: 10.0.0.2, user: u, password_env: DST_PW, database: app }\ntables: { include: [\"app.*\"] }\n", 0o640)
	write(filepath.Join(cfgDir, "filepipe.env"), "SRC_PW=SrcSecretPw\nDST_PW=DstSecretPw\n", 0o600)
	b, _ := json.Marshal(createdFileDoc{Version: 1, Pipelines: []PipelineEntry{{Name: "filepipe", Config: fp, EnvFile: filepath.Join(cfgDir, "filepipe.env")}}})
	write(filepath.Join(state, CreatedFile), string(b), 0o600)
	id := "20261115T100000Z-b5290e1c"
	zero := 0
	end := time.Date(2026, 11, 15, 10, 0, 5, 0, time.UTC)
	jb, _ := json.Marshal(JobStatus{ID: id, Pipeline: "orders", Kind: "verify", Args: []string{"verify", "--mode", "full"},
		Started: end.Add(-5 * time.Second), Ended: &end, ExitCode: &zero})
	write(filepath.Join(state, "jobs", id+".json"), string(jb), 0o600)
	write(filepath.Join(state, "jobs", id+".log"), "line one\nline two\n", 0o600)
	write(filepath.Join(state, "audit.log"), `{"time":"2026-11-15T10:00:00Z","ip":"127.0.0.1","action":"login","result":"ok"}`+"\n"+
		`{"time":"2026-11-15T10:00:01Z","ip":"127.0.0.1","action":"verify","pipeline":"orders","result":"started"}`+"\n", 0o600)
	h, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	must(err)
	return &Config{StrandBinary: "x", Listen: "127.0.0.1:0", StateDir: state, ConfigDir: cfgDir, PasswordHash: string(h),
		Pipelines: []PipelineEntry{{Name: "orders", Config: orders}}}, &fakeCmd{out: map[string]string{}}
}
