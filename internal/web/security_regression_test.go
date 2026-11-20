package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strand/internal/web/store"
	"strings"
	"testing"
	"time"
)

func TestAuditUnmaskReordered(t *testing.T) {
	orig := []byte("source:\n  password: source-secret\ntarget:\n  password: target-secret\n")
	edited := "target:\n  password: \"********\"\nsource:\n  password: \"********\"\n"
	got, err := UnmaskConfigText(edited, orig)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "target:\n  password: target-secret") {
		t.Errorf("passwords assigned by occurrence rather than YAML path: %s", got)
	}
}
func TestAuditEnvSavePersistsRegistration(t *testing.T) {
	e := newEnv(t, nil)
	p := e.srv.reg.Get("orders")
	if _, err := e.srv.saveEnvContent(context.Background(), p, map[string]string{"SRC_PW": "test-secret"}, "test"); err != nil {
		t.Fatal(err)
	}
	if e.srv.reg.Get("orders").EnvFile == "" {
		t.Error("saveEnvContent returns success but Refresh loses new env_file")
	}
}
func TestAuditSecretsInAudit(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	code, _ := e.act(c, csrf, "orders", "env-save", map[string]string{"entries": `{"SRC_PW":"audit-canary-secret"}`})
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	raw, err := os.ReadFile(e.srv.audit.Path())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "audit-canary-secret") {
		t.Error("plaintext password saved into audit log")
	}
}
func TestAuditConfigDiffLeaksSecret(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	raw, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte("\n# comment\npassword: diff-canary-secret\n")...)
	e.writePipelineFile("orders", p.ConfigPath, string(raw), 0o600)
	masked, err := MaskConfigText(raw)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.srv.validateConfigContent(context.Background(), p, masked+"# next\n")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Diff, "diff-canary-secret") {
		t.Error("config diff discloses originally masked password")
	}
}
func TestAuditLimiterMemory(t *testing.T) {
	now := time.Now()
	l := NewLoginLimiter(func() time.Time { return now })
	l.Fail("192.0.2.1")
	now = now.Add(24 * time.Hour)
	l.Fail("192.0.2.2")
	if _, ok := l.m["192.0.2.1"]; ok {
		t.Error("expired failed IP is never removed")
	}
}

func TestConfigDiffBoundsWork(t *testing.T) {
	got := unifiedDiff("old", "new", strings.Repeat("old\n", 10000), strings.Repeat("new\n", 10000))
	if !strings.Contains(got, "diff omitted") {
		t.Fatal("large diff must use bounded fallback")
	}
}

func TestEnvDynamicArgsRedacted(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	code, _ := e.act(c, csrf, "orders", "env-save", map[string]string{"CUSTOM": "dynamic-canary-secret"})
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	raw, err := os.ReadFile(e.srv.audit.Path())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "dynamic-canary-secret") {
		t.Fatal("dynamic env value leaked")
	}
}

type rejectPipelineWrites struct{ store.Store }

func (s rejectPipelineWrites) UpsertPipeline(context.Context, store.Pipeline) error {
	return errors.New("test database unavailable")
}

func TestEditPersistenceFailureRollsBack(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	orig, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMem()
	if err := st.UpsertPipeline(t.Context(), store.Pipeline{Name: p.Name, ConfigPath: p.ConfigPath, ConfigYAML: string(orig), HasConfig: true}); err != nil {
		t.Fatal(err)
	}
	db, err := newMetaDB(rejectPipelineWrites{st}, "0123456789abcdef0123456789abcdef", e.srv.log)
	if err != nil {
		t.Fatal(err)
	}
	e.srv.db = db
	if _, err := e.srv.saveConfigContent(t.Context(), p, string(orig)+"\n# updated\n", false, false, "test"); err == nil {
		t.Fatal("config save succeeded despite database failure")
	}
	got, _ := os.ReadFile(p.ConfigPath)
	if string(got) != string(orig) {
		t.Fatal("config not rolled back")
	}
	envPath := e.srv.envPathFor(p)
	if err := os.Remove(envPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"CUSTOM": "value"}, "test"); err == nil {
		t.Fatal("env save succeeded despite database failure")
	}
	if _, err := os.Stat(envPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new env was not removed: %v", err)
	}
}

func TestEnvRegistrationSurvivesRegistryRestart(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"CUSTOM": "value"}, "test"); err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry(e.srv.cfg, e.cmd)
	reg.db = e.srv.db
	reg.Refresh(t.Context())
	if got := reg.Get(p.Name); got == nil || got.EnvFile != e.srv.envPathFor(p) {
		t.Fatalf("env path lost after restart: %+v", got)
	}
}

func TestEnvSavePersistsNewDBPath(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	orig, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMem()
	if err := st.UpsertPipeline(t.Context(), store.Pipeline{Name: p.Name, ConfigPath: p.ConfigPath, ConfigYAML: string(orig), HasConfig: true}); err != nil {
		t.Fatal(err)
	}
	db, err := newMetaDB(st, "0123456789abcdef0123456789abcdef", e.srv.log)
	if err != nil {
		t.Fatal(err)
	}
	e.srv.db = db
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"CUSTOM": "value"}, "test"); err != nil {
		t.Fatal(err)
	}
	row, _, err := st.GetPipeline(t.Context(), p.Name)
	if err != nil {
		t.Fatal(err)
	}
	if row.EnvPath == "" || len(row.EnvEnc) == 0 {
		t.Fatal("new env path/content not stored")
	}
}

func TestEnvRejectsInvalidNames(t *testing.T) {
	for _, key := range []string{"BAD\nINJECTED", "BAD=KEY", "$BAD", "1BAD", "", "A-B"} {
		if _, err := formatEnvLine(key, "value"); err == nil {
			t.Errorf("accepted invalid key %q", key)
		}
	}
	for _, key := range []string{"GOOD", "_GOOD", "GOOD_1"} {
		if _, err := formatEnvLine(key, "value"); err != nil {
			t.Errorf("rejected valid key %q: %v", key, err)
		}
	}
}

// A stored backup-connection secret exists in plaintext only inside the Web process while it is handed to a
// backup process through that process's environment: it must not appear in any API response, HTML page,
// metrics output, audit entry, log line or process argv, and never in the clear in the database.
func TestStoredConnectionSecretAppearsNowhere(t *testing.T) {
	e := newCPEnv(t)
	var logs strings.Builder
	e.srv.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a := e.admin()
	var seen []string
	rec := func(s string) string { seen = append(seen, s); return s }
	id := connID(t, rec(a.must("POST", "/api/v1/backup-connections", storedConn("secret-conn"), 201)))
	rec(a.must("GET", "/api/v1/backup-connections", nil, 200))
	rec(a.must("GET", "/api/v1/backup-connections/"+id, nil, 200))
	rec(a.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 200))
	rec(a.must("POST", "/api/v1/backup-connections/test", storedConn("secret-conn"), 200))
	upd := storedConn("secret-conn")
	upd["password"] = canary + "-2"
	rec(a.must("PUT", "/api/v1/backup-connections/"+id, upd, 200))
	b := mysqlJobBody("secret-job", id)
	b["enabled"] = true
	rec(a.must("POST", "/api/v1/backup-jobs", b, 201))
	rec(a.must("GET", "/api/v1/backup-jobs", nil, 200))
	rec(a.must("GET", "/api/v1/backup-policies", nil, 200))
	for _, page := range []string{"/backups", "/audit", "/", "/jobs"} {
		_, body := a.e.testEnv.do(a.c, "GET", page, "", nil)
		rec(body)
	}
	_, metrics := a.e.testEnv.do(a.c, "GET", "/metrics", "", nil)
	rec(metrics)
	sealed, _, _ := e.st.GetBackupConnection(context.Background(), id)
	b64 := base64.StdEncoding.EncodeToString(sealed.PasswordSealed)
	for i, s := range seen {
		if strings.Contains(s, canary) || strings.Contains(s, b64) || strings.Contains(s, base64.RawStdEncoding.EncodeToString(sealed.PasswordSealed)) {
			t.Fatalf("surface %d leaks the connection secret", i)
		}
	}
	for _, sp := range e.rec.take() {
		if strings.Contains(strings.Join(sp.Args, "\x00"), canary) || strings.Contains(strings.Join(sp.DisplayArgs, "\x00"), canary) {
			t.Fatalf("secret in argv of %s", sp.Kind)
		}
	}
	if strings.Contains(e.auditText(), canary) {
		t.Fatal("secret in the audit log")
	}
	if strings.Contains(logs.String(), canary) {
		t.Fatalf("secret in the log: %s", logs.String())
	}
	row, _, _ := e.st.GetBackupConnection(context.Background(), id)
	if strings.Contains(string(row.PasswordSealed), canary) || len(row.PasswordSealed) == 0 {
		t.Fatal("secret not sealed in the database row")
	}
	// The Go value serializes without any secret field, so no future handler can leak it by accident.
	raw, _ := json.Marshal(row)
	if strings.Contains(string(raw), "ealed") || strings.Contains(string(raw), canary) || !strings.Contains(string(raw), `"password_set":true`) {
		t.Fatalf("connection JSON = %s", raw)
	}
}
