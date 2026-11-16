package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/web/store"
)

// flakyStore wraps a Store and fails selected calls on demand (partial outages the Mem store cannot model).
type flakyStore struct {
	store.Store
	failUpsertPipeline atomic.Bool
	failAuditBatchFrom atomic.Int32 // >0: the n-th AppendAuditBatch call (1-based) and later fail
	auditBatchCalls    atomic.Int32
}

var errFlaky = errors.New("flaky: database write failed")

func (f *flakyStore) UpsertPipeline(ctx context.Context, p store.Pipeline) error {
	if f.failUpsertPipeline.Load() {
		return errFlaky
	}
	return f.Store.UpsertPipeline(ctx, p)
}

const dbKey = "0123456789abcdef0123456789abcdef"

// Config edit while the database rejects the write: the old code kept the new file and only logged a
// warning, so the next start/validate re-materialised the stale DB copy and silently reverted the edit.
func TestConfigEditFailsClosedWhenDatabaseWriteFails(t *testing.T) {
	fs := &flakyStore{Store: store.NewMem()}
	cfgDir := t.TempDir()
	e := dbEnv(t, fs, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir; c.Scan = nil },
		func(o *Options) { o.SecretKey = dbKey })
	c, csrf := e.loginOK()
	e.setExit(0)
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("flaky", "SrcSecretPw", "DstSecretPw")); code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	yamlPath := filepath.Join(cfgDir, "flaky.yaml")
	orig, _ := os.ReadFile(yamlPath)
	fs.failUpsertPipeline.Store(true)
	body, _ := json.Marshal(map[string]any{"yaml": string(orig) + "\n# edit during outage\n"})
	resp, out := e.do(c, "POST", "/api/v1/pipelines/flaky/config", string(body), map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 503 {
		t.Fatalf("config save with DB write failing: %d %s (want 503)", resp.StatusCode, out)
	}
	if b, _ := os.ReadFile(yamlPath); string(b) != string(orig) {
		t.Fatalf("file changed although the database copy was not updated: %q", b)
	}
	// .env edit: same rule.
	envPath := filepath.Join(cfgDir, "flaky.env")
	origEnv, _ := os.ReadFile(envPath)
	eb, _ := json.Marshal(map[string]any{"entries": map[string]string{"EXTRA": "1"}})
	resp, out = e.do(c, "POST", "/api/v1/pipelines/flaky/env", string(eb), map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 503 {
		t.Fatalf("env save with DB write failing: %d %s (want 503)", resp.StatusCode, out)
	}
	if b, _ := os.ReadFile(envPath); string(b) != string(origEnv) {
		t.Fatalf(".env changed although the database copy was not updated")
	}
	// Database back: the edit goes through and lands in both.
	fs.failUpsertPipeline.Store(false)
	resp, out = e.do(c, "POST", "/api/v1/pipelines/flaky/config", string(body), map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("config save: %d %s", resp.StatusCode, out)
	}
	row, _, _ := fs.GetPipeline(context.Background(), "flaky")
	if !strings.Contains(row.ConfigYAML, "# edit during outage") {
		t.Fatal("edit not stored in the database")
	}
}

// .env edited while STRAND_WEB_SECRET_KEY is unset: the sealed copy from when the key was set is stale and
// must be dropped, otherwise setting the key again would materialise the old .env over the newer one.
func TestEnvEditWithoutKeyDropsStaleSealedCopy(t *testing.T) {
	st := store.NewMem()
	cfgDir := t.TempDir()
	e := dbEnv(t, st, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir; c.Scan = nil },
		func(o *Options) { o.SecretKey = dbKey })
	c, csrf := e.loginOK()
	e.setExit(0)
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("rekey", "SrcSecretPw", "DstSecretPw")); code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	if row, _, _ := st.GetPipeline(context.Background(), "rekey"); len(row.EnvEnc) == 0 {
		t.Fatal("precondition: env not sealed")
	}
	t.Setenv(EnvSecretKey, "")
	e.srv.db.secret = ""
	e2 := e.restart() // same store, no key
	e2.srv.db.box = nil
	c2, csrf2 := e2.loginOK()
	eb, _ := json.Marshal(map[string]any{"entries": map[string]string{"NEWVAR": "fresh"}})
	resp, out := e2.do(c2, "POST", "/api/v1/pipelines/rekey/env", string(eb), map[string]string{csrfHeader: csrf2, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("env save: %d %s", resp.StatusCode, out)
	}
	if row, _, _ := st.GetPipeline(context.Background(), "rekey"); len(row.EnvEnc) != 0 {
		t.Fatal("stale sealed .env kept in the database after an edit without the key")
	}
}

func (f *flakyStore) AppendAuditBatch(ctx context.Context, rows []store.AuditRow, prog *store.MetaKV) error {
	n := f.auditBatchCalls.Add(1)
	if from := f.failAuditBatchFrom.Load(); from > 0 && n >= from {
		return errFlaky
	}
	return f.Store.AppendAuditBatch(ctx, rows, prog)
}

// First start with a database imports audit.log in batches. If it is interrupted after some batches were
// committed, the next start must resume, not re-insert the committed rows (audit_log is append-only).
func TestAuditImportResumesWithoutDuplicates(t *testing.T) {
	mem := store.NewMem()
	fs := &flakyStore{Store: mem}
	var sb strings.Builder
	const lines = 2500
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&sb, `{"time":"2026-01-01T00:00:00Z","ip":"1.2.3.4","action":"old.%d","result":"ok"}`+"\n", i)
	}
	h, _ := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	must0(t, mem.SetPasswordHash(context.Background(), string(h)))
	fs.failAuditBatchFrom.Store(2) // first batch (1000 rows) commits, second fails
	var stateDir string
	e := newEnvWith(t, func(e *testEnv, c *Config) {
		stateDir = c.StateDir
		must0(t, os.MkdirAll(stateDir, 0o750))
		must0(t, os.WriteFile(filepath.Join(stateDir, "audit.log"), []byte(sb.String()), 0o600))
	}, func(o *Options) { o.Store = store.NewMem() }) // throwaway env, only to get a valid Config
	c2 := *e.srv.cfg
	c2.StateDir = stateDir
	if _, err := New(Options{Config: &c2, Store: fs, Version: "test", Commander: e.cmd}); err == nil {
		t.Fatal("start succeeded although the audit import failed")
	}
	fs.failAuditBatchFrom.Store(0)
	if _, err := New(Options{Config: &c2, Store: fs, Version: "test", Commander: e.cmd}); err != nil {
		t.Fatal(err)
	}
	rows, _ := mem.ListAudit(context.Background(), 100000)
	seen := map[string]int{}
	for _, r := range rows {
		if strings.HasPrefix(r.Action, "old.") {
			seen[r.Action]++
		}
	}
	if len(seen) != lines {
		t.Fatalf("imported %d distinct lines, want %d", len(seen), lines)
	}
	for a, n := range seen {
		if n != 1 {
			t.Fatalf("%s imported %d times", a, n)
		}
	}
}

// Changing the admin password in DB mode must sign out every session: sessions survive web restarts in the
// database, so (unlike file mode, where a restart drops them) a stolen cookie would otherwise stay valid.
func TestPasswordChangeRevokesDatabaseSessions(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, _ := e.loginOK()
	if resp, _ := e.do(c, "GET", "/api/v1/session", "", nil); resp.StatusCode != 200 {
		t.Fatalf("session: %d", resp.StatusCode)
	}
	h, _ := bcrypt.GenerateFromPassword([]byte("another-password"), bcrypt.MinCost)
	must0(t, SetStorePassword(context.Background(), st, string(h)))
	if resp, _ := e.do(c, "GET", "/api/v1/session", "", nil); resp.StatusCode != 401 {
		t.Fatalf("old session still valid after password change: %d", resp.StatusCode)
	}
	// init in DB mode (password reset) goes through the same path.
	var buf strings.Builder
	cfgPath := filepath.Join(t.TempDir(), "web.yaml")
	if _, err := RunInit(InitOptions{ConfigPath: cfgPath, Listen: "127.0.0.1:8080", StateDir: t.TempDir(), Password: "third-password-x",
		Store: st, Stdout: &buf, Stderr: &buf, Quiet: true}); err != nil {
		t.Fatal(err)
	}
	must0(t, st.PutSession(context.Background(), store.Session{IDHash: "x", CSRF: "c", Created: time.Now(), LastSeen: time.Now()}))
	if _, err := RunInit(InitOptions{ConfigPath: cfgPath, Password: "fourth-password-x", Store: st, Stdout: &buf, Stderr: &buf, Quiet: true}); err != nil {
		t.Fatal(err)
	}
	if n, _ := st.CountSessions(context.Background()); n != 0 {
		t.Fatalf("%d sessions left after password reset via init", n)
	}
}

// A changed STRAND_WEB_SECRET_KEY used to start fine and only log "decrypt failed" on every start/validate,
// while the next .env edit re-sealed that pipeline with the other key (mixed keys). Now: a clear startup
// error, and STRAND_WEB_SECRET_KEY_PREVIOUS rotates every sealed copy to the new key.
func TestSecretKeyMismatchAndRotation(t *testing.T) {
	st := store.NewMem()
	cfgDir := t.TempDir()
	e := dbEnv(t, st, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir; c.Scan = nil },
		func(o *Options) { o.SecretKey = dbKey })
	c, csrf := e.loginOK()
	e.setExit(0)
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("kp", "SrcSecretPw", "DstSecretPw")); code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	cfg := *e.srv.cfg
	const newKey = "fedcba9876543210fedcba9876543210"
	_, err := New(Options{Config: &cfg, Version: "test", Commander: e.cmd, Store: st, SecretKey: newKey})
	if err == nil || !strings.Contains(err.Error(), EnvSecretKeyPrevious) {
		t.Fatalf("start with a different key: err = %v, want a clear mismatch error", err)
	}
	if _, err := New(Options{Config: &cfg, Version: "test", Commander: e.cmd, Store: st, SecretKey: newKey, PreviousSecretKey: dbKey}); err != nil {
		t.Fatalf("rotation start: %v", err)
	}
	row, _, _ := st.GetPipeline(context.Background(), "kp")
	nb, _ := store.NewBox(newKey)
	if pt, err := nb.Open(row.EnvEnc, "kp"); err != nil || !strings.Contains(string(pt), "SrcSecretPw") {
		t.Fatalf(".env not re-encrypted with the new key: %v", err)
	}
	// After rotation the new key alone starts; the old one is now rejected.
	if _, err := New(Options{Config: &cfg, Version: "test", Commander: e.cmd, Store: st, SecretKey: newKey}); err != nil {
		t.Fatalf("start with the new key after rotation: %v", err)
	}
	if _, err := New(Options{Config: &cfg, Version: "test", Commander: e.cmd, Store: st, SecretKey: dbKey}); err == nil {
		t.Fatal("old key accepted after rotation")
	}
}

type pingErrStore struct{ store.Store }

func (pingErrStore) Ping(context.Context) error {
	return errors.New(`failed to connect to host=db-internal.corp.example user=strand database=strand: dial tcp 10.1.2.3:5432: connection refused`)
}

// /healthz is unauthenticated: it must not reveal the database host, address, user or name.
func TestHealthzDoesNotLeakDatabaseDetails(t *testing.T) {
	mem := store.NewMem()
	e := dbEnv(t, mem, nil, nil)
	e.srv.db.st = pingErrStore{mem}
	resp, body := e.do(e.client(), "GET", "/healthz", "", nil)
	if resp.StatusCode != 503 {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	for _, leak := range []string{"db-internal", "10.1.2.3", "user=", "database="} {
		if strings.Contains(body, leak) {
			t.Fatalf("healthz leaks %q: %s", leak, body)
		}
	}
}
