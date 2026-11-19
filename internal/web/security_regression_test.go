package web

import (
	"context"
	"errors"
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
	if err = os.WriteFile(p.ConfigPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
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
