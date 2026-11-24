package web

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/web/store"
)

type materializeReadGate struct {
	store.Store
	entered chan struct{}
	release chan struct{}
	paused  atomic.Bool
}

func (g *materializeReadGate) GetPipeline(ctx context.Context, name string) (store.Pipeline, bool, error) {
	row, ok, err := g.Store.GetPipeline(ctx, name)
	if g.paused.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
	}
	return row, ok, err
}

func TestMaterializeCannotOverwriteConfigSaveInFlight(t *testing.T) {
	e := newEnv(t, nil)
	p := e.srv.reg.Get("orders")
	old := e.srv.getOriginalConfig(p)
	if len(old) == 0 {
		t.Fatal("expected seeded config")
	}
	newConfig := append(append([]byte(nil), old...), []byte("# saved while lifecycle materializes\n")...)
	ctx := context.Background()
	row, ok, err := e.srv.db.st.GetPipeline(ctx, p.Name)
	if err != nil || !ok {
		t.Fatalf("load pipeline metadata: found=%v err=%v", ok, err)
	}
	row.MissingSince = time.Now().UTC()
	row.Conflicts = `[{"file":"prior"}]`
	if err := e.srv.db.st.UpsertPipeline(ctx, row); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.db.st.SetDesired(ctx, p.Name, store.DesiredRunning); err != nil {
		t.Fatal(err)
	}

	// Model the save's commit section. Pause materialize after it captures the
	// stale row, write the new file and DB snapshot, then let it contend for the
	// lock. A correct materializer reloads the row after acquiring the lock.
	unlock, err := lockPipelineFiles(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	originalStore := e.srv.db.st
	gate := &materializeReadGate{Store: originalStore, entered: make(chan struct{}), release: make(chan struct{})}
	materializer := &Server{
		db:  &metaDB{st: gate, box: e.srv.db.box, log: e.srv.log},
		log: e.srv.log, audit: e.srv.audit, now: e.srv.now,
	}
	done := make(chan struct{})
	go func() {
		materializer.materializeOpt(p.Name, true)
		close(done)
	}()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		close(gate.release)
		unlock()
		t.Fatal("materialize did not read its initial metadata row")
	}
	if err := os.WriteFile(p.ConfigPath, newConfig, 0o640); err != nil {
		close(gate.release)
		unlock()
		t.Fatal(err)
	}
	if err := e.srv.persistPipelineConfigSnapshot(p, old, newConfig); err != nil {
		close(gate.release)
		unlock()
		t.Fatalf("persist config snapshot: %v", err)
	}
	close(gate.release)
	unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("materialize did not finish after save released the lock")
	}

	disk, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	db := e.srv.getOriginalConfig(p)
	if string(disk) != string(newConfig) || string(db) != string(newConfig) {
		t.Fatalf("config diverged after materialize/save race: disk new=%v, DB new=%v", string(disk) == string(newConfig), string(db) == string(newConfig))
	}
	if err := e.srv.checkPipelineConfigSnapshot(p, old); err == nil {
		t.Fatal("stale editor version was allowed after another save committed")
	}
	row, ok, err = e.srv.db.st.GetPipeline(ctx, p.Name)
	if err != nil || !ok {
		t.Fatalf("reload pipeline metadata: found=%v err=%v", ok, err)
	}
	if row.MissingSince.IsZero() || row.Conflicts != `[{"file":"prior"}]` {
		t.Fatalf("config save clobbered unrelated pipeline metadata: missing=%v conflicts=%q", !row.MissingSince.IsZero(), row.Conflicts)
	}
	desired, err := e.srv.db.st.Desired(ctx)
	if err != nil || desired[p.Name] != store.DesiredRunning {
		t.Fatalf("config save clobbered desired state: desired=%v err=%v", desired[p.Name], err)
	}
}

func TestConfigSaveCopiesDiscoveredConfigIntoMetadata(t *testing.T) {
	e := opsEnv(t, false, nil)
	e.setExit(0)
	p := e.srv.reg.Get("orders")
	ctx := context.Background()
	row, ok, err := e.srv.db.st.GetPipeline(ctx, p.Name)
	if err != nil || !ok {
		t.Fatalf("load pipeline metadata: found=%v err=%v", ok, err)
	}
	row.HasConfig, row.ConfigYAML = false, ""
	if err := e.srv.db.st.UpsertPipeline(ctx, row); err != nil {
		t.Fatal(err)
	}
	initial, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	updated := string(initial) + "\n# discovered pipeline edit\n"
	if _, err := e.srv.saveConfigContent(ctx, p, updated, false, false, ""); err != nil {
		t.Fatal(err)
	}
	row, ok, err = e.srv.db.st.GetPipeline(ctx, p.Name)
	if err != nil || !ok || !row.HasConfig || row.ConfigYAML != updated {
		t.Fatalf("discovered pipeline edit did not persist: found=%v configured=%v same=%v err=%v", ok, row.HasConfig, row.ConfigYAML == updated, err)
	}
}

// concurrentConfigSave commits a newer config from "another Web instance"
// right before this instance's next metadata write.
type concurrentConfigSave struct {
	store.Store
	armed  atomic.Bool
	name   string
	suffix string
}

func (c *concurrentConfigSave) interleave(ctx context.Context) {
	if !c.armed.CompareAndSwap(true, false) {
		return
	}
	row, _, _ := c.Store.GetPipeline(ctx, c.name)
	_, _ = c.Store.UpdatePipelineConfigCompareAndSwap(ctx, c.name, row.HasConfig, row.ConfigYAML, row.ConfigYAML+c.suffix)
}

func (c *concurrentConfigSave) UpsertPipeline(ctx context.Context, p store.Pipeline) error {
	c.interleave(ctx)
	return c.Store.UpsertPipeline(ctx, p)
}

func (c *concurrentConfigSave) UpdatePipelineEnv(ctx context.Context, name, envPath string, envEnc []byte) (bool, error) {
	c.interleave(ctx)
	return c.Store.UpdatePipelineEnv(ctx, name, envPath, envEnc)
}

// Another Web instance saves a config after this instance synced its disk but
// before it stores the .env: the .env save must not copy the older disk config
// back over the newer database version.
func TestEnvSaveKeepsNewerDatabaseConfig(t *testing.T) {
	e := newEnv(t, nil)
	p := e.srv.reg.Get("orders")
	ctx := context.Background()
	const suffix = "# saved on another Web instance\n"
	other := &concurrentConfigSave{Store: e.srv.db.st, name: p.Name, suffix: suffix}
	e.srv.db.st = other
	other.armed.Store(true)
	if _, err := e.srv.saveEnvContent(ctx, p, map[string]string{"EXTRA": "1"}, "test"); err != nil {
		t.Fatal(err)
	}
	if other.armed.Load() {
		t.Fatal("the concurrent save was not interleaved")
	}
	after, _, err := other.Store.GetPipeline(ctx, p.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(after.ConfigYAML, suffix) {
		t.Fatalf("env save replaced the newer database config with the local file:\n%s", after.ConfigYAML)
	}
	if len(after.EnvEnc) == 0 {
		t.Fatal("env save did not store the .env")
	}
}

// Credentials kept in the encrypted metadata database (no .env file): another
// Web instance saves a config between this instance's read and its write. The
// credential save must not put the older config_yaml back.
func TestDatabaseCredentialSaveKeepsConcurrentConfig(t *testing.T) {
	e, c, csrf := npEnv(t)
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("dbcred", "a", "b")); code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	p := e.srv.reg.Get("dbcred")
	if p == nil || p.dbEnv == nil {
		t.Fatal("expected a pipeline with database-stored credentials")
	}
	const suffix = "# saved on another Web instance\n"
	other := &concurrentConfigSave{Store: e.srv.db.st, name: p.Name, suffix: suffix}
	e.srv.db.st = other
	other.armed.Store(true)
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"EXTRA": "1"}, "test"); err != nil {
		t.Fatal(err)
	}
	if other.armed.Load() {
		t.Fatal("the concurrent save was not interleaved")
	}
	row, _, err := other.Store.GetPipeline(t.Context(), p.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(row.ConfigYAML, suffix) {
		t.Fatalf("credential save replaced the newer config:\n%s", row.ConfigYAML)
	}
	kv, err := e.srv.db.pipelineEnv(p.Name)
	if err != nil || !strings.Contains(strings.Join(kv, "|"), "EXTRA=1") {
		t.Fatalf("credentials not stored: %v %v", kv, err)
	}
}
