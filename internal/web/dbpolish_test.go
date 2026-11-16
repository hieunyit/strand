package web

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"strand/internal/web/store"
)

// flakyStore wraps a Store and fails selected calls on demand (partial outages the Mem store cannot model).
type flakyStore struct {
	store.Store
	failUpsertPipeline atomic.Bool
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
