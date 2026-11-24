package web

import (
	"context"
	"os"
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
