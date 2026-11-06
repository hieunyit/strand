package pipeline

// Regression test review N14: đổi tables.rename cho bảng đã bootstrap (Done) phải bị từ chối lúc khởi
// động; bảng chưa Done chỉ ghi ánh xạ mới (bỏ plan dở); state cũ chưa có ánh xạ thì ghi ánh xạ hiện tại.
// Unit test thuần: state trên thư mục tạm, không cần DB.

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/snapshot"
)

func TestN14PlanTargetMapping(t *testing.T) {
	done := func(t string) bool { return t == "app.a" }
	persisted := map[string]string{"app.a": "dst.a", "app.b": "dst.b"}
	current := map[string]string{"app.a": "dst.a2", "app.b": "dst.b2", "app.c": "app.c"}
	conflicts, record, replan := planTargetMapping(done, persisted, current)
	if len(conflicts) != 1 || conflicts[0] != (targetChange{Table: "app.a", Was: "dst.a", Now: "dst.a2"}) {
		t.Fatalf("conflicts = %+v, want app.a dst.a→dst.a2", conflicts)
	}
	if record["app.b"] != "dst.b2" || record["app.c"] != "app.c" || len(record) != 2 {
		t.Fatalf("record = %v", record)
	}
	if len(replan) != 1 || replan[0] != "app.b" {
		t.Fatalf("replan = %v, want [app.b]", replan)
	}
	if c, r, _ := planTargetMapping(done, persisted, map[string]string{"app.a": "dst.a"}); len(c) != 0 || len(r) != 0 {
		t.Fatalf("unchanged mapping: conflicts=%v record=%v", c, r)
	}
}

func newTargetTestPipeline(t *testing.T, dir string, rename map[string]string) *Pipeline {
	t.Helper()
	f, err := newTableFilter([]string{"app.*"}, nil, rename)
	if err != nil {
		t.Fatal(err)
	}
	st, err := openLocalState(dir, "p1")
	if err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{filter: f, state: st, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		tables: map[model.TableID]*tableMeta{{Schema: "app", Table: "a"}: {}, {Schema: "app", Table: "b"}: {}}}
	st.targetOf = func(name string) string {
		sc, tb, _ := strings.Cut(name, ".")
		return p.filter.Target(model.TableID{Schema: sc, Table: tb}).String()
	}
	return p
}

func TestN14RenameOfDoneTableRejected(t *testing.T) {
	dir := t.TempDir()
	p := newTargetTestPipeline(t, dir, map[string]string{"app.a": "dst.a"})
	if err := p.checkTableTargets(); err != nil {
		t.Fatalf("first start: %v", err)
	}
	if err := p.state.SetDone("app.a", true); err != nil {
		t.Fatal(err)
	}
	if err := p.state.SetPlan("app.b", snapshot.SnapshotPlan{NextPK: 100}); err != nil {
		t.Fatal(err)
	}

	// Khởi động lại, rename đổi đích của app.a (Done) → từ chối, exit 3.
	p2 := newTargetTestPipeline(t, dir, map[string]string{"app.a": "dst.a_new", "app.b": "dst.b_new"})
	err := p2.checkTableTargets()
	var xe *exitcode.Error
	if err == nil || !errors.As(err, &xe) || xe.ExitCode != exitcode.Rejected || xe.Code != exitcode.ErrTableTargetChanged {
		t.Fatalf("rename of a done table: err = %v, want table_target_changed (exit 3)", err)
	}
	if !strings.Contains(err.Error(), "app.a was bootstrapped into dst.a but tables.rename now maps it to dst.a_new") {
		t.Fatalf("message: %v", err)
	}
	// Từ chối thì không ghi gì: plan dở của app.b còn nguyên.
	if _, ok := p2.state.Plan("app.b"); !ok {
		t.Fatalf("rejected start dropped app.b plan")
	}

	// Chỉ đổi đích của bảng chưa Done → nhận, ghi ánh xạ mới, bỏ plan dở (chunk đã chép ở đích cũ).
	p3 := newTargetTestPipeline(t, dir, map[string]string{"app.a": "dst.a", "app.b": "dst.b_new"})
	if err := p3.checkTableTargets(); err != nil {
		t.Fatalf("rename of a pending table: %v", err)
	}
	if _, ok := p3.state.Plan("app.b"); ok {
		t.Fatalf("pending table renamed: stale snapshot plan kept")
	}
	p4 := newTargetTestPipeline(t, dir, map[string]string{"app.a": "dst.a", "app.b": "dst.b_new"})
	if got := p4.state.data.Targets["app.b"]; got != "dst.b_new" {
		t.Fatalf("persisted mapping of app.b = %q, want dst.b_new", got)
	}
}

func TestN14LegacyStateRecordsMapping(t *testing.T) {
	dir := t.TempDir()
	// File state cũ: có bảng Done, không có table_targets.
	raw := `{"pipeline_id":"p1","snapshot_done":{"app.a":true}}`
	if err := os.WriteFile(filepath.Join(dir, "pipeline-state.json"), []byte(raw), 0o640); err != nil {
		t.Fatal(err)
	}
	p := newTargetTestPipeline(t, dir, map[string]string{"app.a": "dst.a"})
	if err := p.checkTableTargets(); err != nil {
		t.Fatalf("legacy state rejected: %v", err)
	}
	// Lần sau đổi rename của app.a (Done) thì bị từ chối.
	p2 := newTargetTestPipeline(t, dir, map[string]string{"app.a": "dst.other"})
	if err := p2.checkTableTargets(); exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Fatalf("rename after legacy upgrade: err = %v, want exit 3", err)
	}
}
