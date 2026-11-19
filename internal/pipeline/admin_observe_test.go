package pipeline

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"strand/internal/admin"
	"strand/internal/config"
	"strand/internal/model"
	"strand/internal/snapshot"
)

func observeTestPipeline(t *testing.T) *Pipeline {
	t.Helper()
	st, err := openLocalState(t.TempDir(), "p")
	if err != nil {
		t.Fatal(err)
	}
	a := model.TableID{Schema: "app", Table: "a"} // done
	b := model.TableID{Schema: "app", Table: "b"} // running
	c := model.TableID{Schema: "app", Table: "c"} // pending
	if err := st.SetDone(a.String(), true); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPlan(b.String(), snapshot.SnapshotPlan{MinPK: 1, MaxPK: 1000, ChunkRows: 100, NextPK: 251}); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Name: "p", Source: config.SourceConfig{Kind: "mysql"}, Target: config.TargetConfig{Kind: "mysql"}}
	p := &Pipeline{id: "p", cfg: cfg, strict: true, epoch: 9, state: st, startedAt: time.Now().Add(-time.Minute),
		govAlgo: "static", srcEngine: model.EngineMySQL, dstEngine: model.EngineMySQL,
		snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions()),
		tables: map[model.TableID]*tableMeta{
			a: {ID: a, PK: []string{"id"}, PKNumeric: true},
			b: {ID: b, PK: []string{"id"}, PKNumeric: true},
			c: {ID: c, PK: []string{"k"}},
		},
		opts: Options{StartAt: "seed", Events: admin.NewEventRing(5)}}
	p.snap.Manager.RegisterTable(b, "id", []*snapshot.Chunk{
		{ID: 1, Table: b, PKCol: "id", StartPK: 1, EndPK: 500, State: snapshot.StateDone, BufferedChanges: map[snapshot.Key]*model.RowChange{}},
		{ID: 2, Table: b, PKCol: "id", StartPK: 501, EndPK: 1000, State: snapshot.StatePending, BufferedChanges: map[snapshot.Key]*model.RowChange{}},
	}, 1000)
	p.startSnapRun(b, 0)
	p.tableRowsEst.Store(b, int64(1000))
	p.tableRowsEst.Store(c, int64(50))
	p.running.Store(true)
	return p
}

func TestAdminStatus(t *testing.T) {
	p := observeTestPipeline(t)
	h := &adminHandler{p: p}
	st, err := h.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.PipelineID != "p" || st.Epoch != 9 || st.CommitOrder != "strict" || st.StartAt != "seed" ||
		st.SourceKind != "mysql" || st.Governor.Algorithm != "static" || st.UptimeSeconds < 59 {
		t.Fatalf("unexpected status %+v", st)
	}
	p.paused.Store(true)
	if st, _ = h.Status(context.Background()); !st.Paused || st.State != "paused" {
		t.Fatalf("paused flag not reflected: %+v", st)
	}
	// Chưa sẵn sàng: vẫn trả lời, không panic.
	p2 := &Pipeline{id: "x", cfg: p.cfg, startedAt: time.Now()}
	if st, err = (&adminHandler{p: p2}).Status(context.Background()); err != nil || st.State != "starting" {
		t.Fatalf("status while starting = %+v, %v", st, err)
	}
}

func TestAdminTables(t *testing.T) {
	p := observeTestPipeline(t)
	h := &adminHandler{p: p}
	ts, err := h.Tables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ts) != 3 {
		t.Fatalf("want 3 tables, got %+v", ts)
	}
	by := map[string]admin.TableInfo{}
	for _, ti := range ts {
		by[ti.Table] = ti
	}
	if a := by["app.a"]; a.State != "done" || a.ProgressPct != 100 {
		t.Fatalf("a: %+v", a)
	}
	b := by["app.b"]
	if b.State != "running" || b.ProgressPct < 24.9 || b.ProgressPct > 25.1 || b.RowsEstimate != 1000 ||
		b.RowsCopied < 249 || b.RowsCopied > 251 || b.Ranges["Done"] != 1 || b.Ranges["Pending"] != 1 || b.GenericPK {
		t.Fatalf("b: %+v", b)
	}
	if c := by["app.c"]; c.State != "pending" || !c.GenericPK || c.RowsEstimate != 50 || c.ProgressPct != 0 {
		t.Fatalf("c: %+v", c)
	}
	// Thứ tự ổn định theo tên.
	if ts[0].Table != "app.a" || ts[2].Table != "app.c" {
		t.Fatalf("not sorted: %+v", ts)
	}
	// Chưa sẵn sàng: lỗi rõ ràng.
	if _, err := (&adminHandler{p: &Pipeline{}}).Tables(context.Background()); err != errNotReady {
		t.Fatalf("want errNotReady, got %v", err)
	}
}

func TestSnapshotProgressDoesNotMoveBackWhenGenericWalkAddsRanges(t *testing.T) {
	p := &Pipeline{}
	for i, sample := range []float64{30, 20, 41, 41, 100} {
		got := p.monotonicSnapshotProgress("app.keyed", sample)
		want := []float64{30, 30, 41, 41, 100}[i]
		if got != want {
			t.Fatalf("sample %.1f: progress = %.1f, want %.1f", sample, got, want)
		}
	}
	if got := p.monotonicSnapshotProgress("app.keyed", 20); got != 100 {
		t.Fatalf("progress must stay monotonic, got %.1f after reaching 100", got)
	}
}

func TestAdminEvents(t *testing.T) {
	p := observeTestPipeline(t)
	log := slog.New(p.opts.Events.Wrap(slog.NewTextHandler(discard{}, nil)))
	log.Info("info is not kept")
	log.Warn("careful", "table", "app.b")
	log.Error("bad")
	ev, err := (&adminHandler{p: p}).Events(context.Background())
	if err != nil || len(ev) != 2 || ev[0].Message != "careful" || ev[1].Level != "ERROR" {
		t.Fatalf("events = %+v, %v", ev, err)
	}
	// Không cấu hình ring: rỗng, không panic.
	if ev, err = (&adminHandler{p: &Pipeline{}}).Events(context.Background()); err != nil || len(ev) != 0 {
		t.Fatalf("nil ring: %+v, %v", ev, err)
	}
}

type discard struct{}

func (discard) Write(b []byte) (int, error) { return len(b), nil }
