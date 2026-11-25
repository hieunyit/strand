package mongocdc

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"strand/internal/admin"
)

// Several databases: each gets its patterns, a share of the copy workers and its own
// checkpoint key; one database keeps the pipeline name as its key.
func TestDatabaseConfigsSplit(t *testing.T) {
	base := Config{Pipeline: "p", Include: []string{"app.*", "billing.invoices", "*.shared"}, Exclude: []string{"app.tmp", "*.old"}, SnapshotParallel: 5}
	cfgs, err := DatabaseConfigs(context.Background(), base, []Database{{"app", "app"}, {"billing", "billing_copy"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfgs) != 2 {
		t.Fatalf("%d configs", len(cfgs))
	}
	a, b := cfgs[0], cfgs[1]
	if a.Pipeline != "p/app" || b.Pipeline != "p/billing" || b.TargetDB != "billing_copy" {
		t.Fatalf("keys/targets: %q %q %q", a.Pipeline, b.Pipeline, b.TargetDB)
	}
	if !reflect.DeepEqual(a.Include, []string{"app.*", "*.shared"}) || !reflect.DeepEqual(b.Include, []string{"billing.invoices", "*.shared"}) {
		t.Fatalf("include: %v %v", a.Include, b.Include)
	}
	if !reflect.DeepEqual(a.Exclude, []string{"app.tmp", "*.old"}) || !reflect.DeepEqual(b.Exclude, []string{"*.old"}) {
		t.Fatalf("exclude: %v %v", a.Exclude, b.Exclude)
	}
	if a.SnapshotParallel+b.SnapshotParallel != 5 || a.SnapshotParallel != 3 {
		t.Fatalf("parallel %d + %d", a.SnapshotParallel, b.SnapshotParallel)
	}
	for _, c := range cfgs {
		if err := validateConfig(Config{Pipeline: c.Pipeline, SourceURI: "x", TargetURI: "y", SourceDB: c.SourceDB, TargetDB: c.TargetDB, Include: c.Include, Exclude: c.Exclude}); err != nil {
			t.Fatalf("%s: %v", c.SourceDB, err)
		}
	}
	one, err := DatabaseConfigs(context.Background(), base, []Database{{"app", "app"}})
	if err != nil || len(one) != 1 || one[0].Pipeline != "p" || !reflect.DeepEqual(one[0].Include, base.Include) || one[0].SnapshotParallel != 5 {
		t.Fatalf("one database: %v %+v", err, one)
	}
}

// RunAll stops every database when one stops with an error and names that database.
func TestRunAllStopsTogether(t *testing.T) {
	old := runDatabase
	t.Cleanup(func() { runDatabase = old })
	boom := errors.New("boom")
	var mu sync.Mutex
	stopped := map[string]bool{}
	runDatabase = func(ctx context.Context, c Config) error {
		if c.SourceDB == "bad" {
			time.Sleep(50 * time.Millisecond)
			return boom
		}
		<-ctx.Done()
		mu.Lock()
		stopped[c.SourceDB] = true
		mu.Unlock()
		return ctx.Err()
	}
	err := RunAll(context.Background(), []Config{{SourceDB: "a"}, {SourceDB: "bad"}, {SourceDB: "c"}})
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "database bad") {
		t.Fatalf("err %v", err)
	}
	if !stopped["a"] || !stopped["c"] {
		t.Fatalf("not stopped: %v", stopped)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	runDatabase = func(ctx context.Context, c Config) error { <-ctx.Done(); return ctx.Err() }
	if err := RunAll(ctx, []Config{{SourceDB: "a"}, {SourceDB: "b"}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	runDatabase = func(ctx context.Context, c Config) error {
		if c.SourceDB == "a" {
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	}
	if err := RunAll(context.Background(), []Config{{SourceDB: "a"}, {SourceDB: "b"}}); err == nil || !strings.Contains(err.Error(), "database a: stopped") {
		t.Fatalf("one database returning: %v", err)
	}
}

// The admin socket shows several databases as one pipeline.
func TestAdminHandlerSeveralDatabases(t *testing.T) {
	ctx := context.Background()
	a, b := NewObserver("p/app"), NewObserver("p/billing")
	a.setPhase(PhaseStreaming)
	b.setPhase(PhaseSnapshot)
	a.setCollection("users", CollDone, progressSnapshot, 10, 10)
	b.setCollection("users", CollRunning, progressSnapshot, 5, 20)
	a.applied(3, map[string]int{"insert": 3}, 3, 0, bson.Timestamp{T: uint32(time.Now().Unix())}, "tokA")
	b.applied(2, map[string]int{"insert": 2}, 2, 0, bson.Timestamp{T: uint32(time.Now().Unix())}, "tokB")
	h := &AdminHandler{Pipeline: "p", Parts: []AdminPart{
		{SourceDB: "app", Obs: a, ListSource: func(context.Context) ([]string, error) { return []string{"users", "extra"}, nil }},
		{SourceDB: "billing", Obs: b, ListSource: func(context.Context) ([]string, error) { return []string{"users"}, nil }},
	}}
	top, err := h.Top(ctx)
	if err != nil || top.PipelineID != "p" || top.State != "bootstrapping" || top.TablesTotal != 2 || top.TablesDone != 1 || top.SnapshotRows != 15 {
		t.Fatalf("top %v %+v", err, top)
	}
	st, _ := h.Status(ctx)
	if st.AppliedRows != 5 || st.AppliedTxns != 2 {
		t.Fatalf("status %+v", st)
	}
	tables, _ := h.Tables(ctx)
	if len(tables) != 2 || tables[0].Table != "app.users" || tables[1].Table != "billing.users" || tables[0].ProgressPct != 100 {
		t.Fatalf("tables %+v", tables)
	}
	src, err := h.SourceTables(ctx)
	if err != nil || len(src) != 3 || src[0] != (admin.SourceTable{Table: "app.users", InPipeline: true}) || src[1].InPipeline || src[2].Table != "billing.users" {
		t.Fatalf("source tables %v %+v", err, src)
	}
	cp, _ := h.CheckpointShow(ctx)
	if !strings.Contains(cp.Applied, "app: resume_token=tokA") || !strings.Contains(cp.Applied, "billing: resume_token=tokB") {
		t.Fatalf("checkpoint %+v", cp)
	}
	if err := h.SnapshotAdd(ctx, "users"); err == nil || !strings.Contains(err.Error(), "database.collection") {
		t.Fatalf("bare name: %v", err)
	}
	if err := h.SnapshotAdd(ctx, "other.users"); err == nil || !strings.Contains(err.Error(), "not part of this pipeline") {
		t.Fatalf("unknown database: %v", err)
	}
	if err := h.SnapshotAdd(ctx, "billing.users"); !errors.Is(err, ErrNotStreaming) {
		t.Fatalf("billing is still copying: %v", err)
	}
	if _, err := h.Pause(ctx); err != nil {
		t.Fatal(err)
	}
	if !a.Snapshot().Paused || !b.Snapshot().Paused {
		t.Fatal("pause must reach every database")
	}
	if _, err := h.Resume(ctx, false); err != nil || a.Snapshot().Paused || b.Snapshot().Paused {
		t.Fatalf("resume: %v", err)
	}
	b.setPhase(PhaseStreaming)
	if top, _ = h.Top(ctx); top.State != "synced" {
		t.Fatalf("both streaming: %+v", top)
	}
	b.setFailed("lost")
	b.setPhase(PhaseStopped)
	if top, _ = h.Top(ctx); top.State != "failed" {
		t.Fatalf("one failed: %+v", top)
	}

	// One database: names and behaviour as before.
	one := &AdminHandler{Obs: a, SourceDB: "app"}
	if tables, _ := one.Tables(ctx); len(tables) != 1 || tables[0].Table != "app.users" {
		t.Fatalf("one database tables %+v", tables)
	}
}
