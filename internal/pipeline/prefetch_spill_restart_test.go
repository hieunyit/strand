package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"strand/internal/assemble"
	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/spool"
)

// CP-1/2, SD-1: prefetch phải replay được sau khi route nhưng trước commit target.
func TestPrefetchSpilledTxnSurvivesRouteAndRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sim := newCSim(t, 41)
	sim.build(ctx, cancel)
	sim.p.srcEngine = model.EngineMySQL
	sim.p.applied0 = checkpoint.NewMySQLAppliedSet()
	sim.p.seen = sim.p.applied0.Clone()
	sim.p.lsns = nil
	sim.p.xids = nil
	sim.p.laneOn = false
	dir := t.TempDir()
	spoolDir := filepath.Join(dir, "prefetch")
	durableDir := filepath.Join(spoolDir, "spill")
	runtimeDir := filepath.Join(dir, "runtime", "spill")
	if err := os.MkdirAll(runtimeDir, 0750); err != nil {
		t.Fatal(err)
	}
	sim.p.cfg.Resources.Spool.Path = filepath.Dir(runtimeDir)
	if err := os.MkdirAll(durableDir, 0750); err != nil {
		t.Fatal(err)
	}
	sb := assemble.NewSpillBuffer(1, durableDir)
	rows := []model.RowChange{
		{Table: cItems, Op: model.OpInsert, Key: []model.Value{int64(201)}, Cols: []model.ColValue{{Name: "id", Value: int64(201)}, {Name: "ver", Value: int64(1)}, {Name: "qty", Value: int64(1)}, {Name: "doc", Value: "first"}}},
		{Table: cItems, Op: model.OpInsert, Key: []model.Value{int64(202)}, Cols: []model.ColValue{{Name: "id", Value: int64(202)}, {Name: "ver", Value: int64(2)}, {Name: "qty", Value: int64(2)}, {Name: "doc", Value: "second"}}},
	}
	for _, row := range rows {
		if err := sb.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	txn := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}}
	if err := sb.Finish(txn); err != nil {
		t.Fatal(err)
	}
	if !txn.IsSpilled() {
		t.Fatal("fixture did not spill")
	}
	sp, err := spool.New(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = sp.Append(ctx, txn); err != nil {
		t.Fatal(err)
	}
	if err = sp.Close(); err != nil {
		t.Fatal(err)
	}
	sp, err = spool.New(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	it, err := sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := it.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = it.Close(); err != nil {
		t.Fatal(err)
	}
	// Không có worker: route đã Admit nhưng chưa có transaction target hoặc checkpoint commit.
	if err = sim.p.route(ctx, replay); err != nil {
		t.Fatal(err)
	}
	var routed *model.Txn
	select {
	case routed = <-sim.p.sched.Ready():
	case <-time.After(time.Second):
		t.Fatal("route did not admit transaction")
	}
	defer os.Remove(routed.SpillPath)
	if sim.p.sched.Lwd() != 0 {
		t.Fatal("unexpected target completion")
	}
	if err = sp.Close(); err != nil {
		t.Fatal(err)
	}
	sp, err = spool.New(spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	it, err = sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	replay, err = it.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []model.RowChange
	if err = replay.ForEachRow(func(row *model.RowChange) error { got = append(got, *row); return nil }); err != nil {
		t.Fatalf("prefetched payload missing after restart: %v", err)
	}
	// Dùng reflection để cùng regression biên dịch trên API cũ trước khi có trường ownership.
	owned := reflect.ValueOf(routed).Elem().FieldByName("SpillOwned")
	borrowed := reflect.ValueOf(replay).Elem().FieldByName("SpillOwned")
	if !owned.IsValid() || !owned.Bool() || !borrowed.IsValid() || borrowed.Bool() {
		t.Fatal("incorrect ownership for routed or decoded spill")
	}
	if filepath.Dir(routed.SpillPath) != runtimeDir {
		t.Fatalf("routed spill directory %s, want runtime directory %s", filepath.Dir(routed.SpillPath), runtimeDir)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("replayed rows differ: %+v", got)
	}
	// Nếu scheduler từ chối, chỉ file output tạm bị xóa; dữ liệu spool vẫn giữ.
	if err := os.Remove(routed.SpillPath); err != nil {
		t.Fatal(err)
	}
	sim.p.sched.Close()
	if err := sim.p.route(ctx, replay); err == nil {
		t.Fatal("expected scheduler failure")
	}
	matches, err := filepath.Glob(filepath.Join(runtimeDir, "strand-routed-*.spill"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary spill remains after failed admission: %v %v", matches, err)
	}
	if err := replay.ForEachRow(func(*model.RowChange) error { return nil }); err != nil {
		t.Fatalf("failed admission removed durable input: %v", err)
	}
}
