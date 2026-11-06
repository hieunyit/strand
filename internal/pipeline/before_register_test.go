package pipeline

import (
	"fmt"
	"math"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// review N10: source MySQL → target có FK. Bảng con đang chờ snapshot (bảng cha còn đang snapshot) mà
// range chưa đăng ký: event của nó KHÔNG được apply thẳng (INSERT con trỏ tới row cha chưa có ở target
// → vi phạm FK, pipeline dừng). Bảng mới (chưa có plan): bỏ cả bảng — snapshot sau đó đọc lại toàn bộ.
func TestBeforeRegister_MySQLSourceDropsPendingTable(t *testing.T) {
	st, err := openLocalState(t.TempDir(), "p")
	if err != nil {
		t.Fatal(err)
	}
	child := model.TableID{Schema: "app", Table: "children"}
	parent := model.TableID{Schema: "app", Table: "items"}
	done := model.TableID{Schema: "app", Table: "done"}
	if err := st.SetDone(done.String(), true); err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{state: st, srcEngine: model.EngineMySQL, dstEngine: model.EnginePG,
		snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())}
	p.snap.Manager.RegisterTable(parent, "id", []*snapshot.Chunk{{ID: 1, Table: parent, PKCol: "id", StartPK: 1, EndPK: 100,
		State: snapshot.StatePending, BufferedChanges: map[snapshot.Key]*model.RowChange{}}}, 100)

	if !p.beforeRegister(child) {
		t.Fatalf("pending, unregistered table of a MySQL source must go through keepBeforeRegister")
	}
	if p.beforeRegister(parent) {
		t.Fatalf("registered table must go through the range-state")
	}
	if p.beforeRegister(done) {
		t.Fatalf("done table must be applied as is")
	}
	ins := &model.RowChange{Table: child, Op: model.OpInsert, Key: []model.Value{int64(1)},
		Cols:        []model.ColValue{{Name: "id", Value: int64(1)}, {Name: "pid", Value: int64(50)}},
		ForeignKeys: []model.ForeignKey{{ParentTable: parent}}}
	parts, err := p.keepBeforeRegister(ins)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 0 {
		t.Fatalf("insert into a not-yet-snapshotted child table must be dropped (the snapshot reads it), got %d part(s)", len(parts))
	}
}

// review N10: plan dở từ lần chạy trước (source MySQL): [1,99] đã flush, [100,200] sẽ đọc lại, > 200 là
// đuôi. Khóa từ reader MySQL có thể là int8/int16/kiểu không dấu: phải được đọc đúng, nếu không event ở
// phần đã flush bị coi là "sẽ đọc lại" và bị bỏ (mất dữ liệu).
func TestBeforeRegister_MySQLResumedPlanKeyTypes(t *testing.T) {
	st, err := openLocalState(t.TempDir(), "p")
	if err != nil {
		t.Fatal(err)
	}
	tbl := model.TableID{Schema: "app", Table: "t"}
	if err := st.SetPlan(tbl.String(), snapshot.SnapshotPlan{MinPK: 1, MaxPK: 200, ChunkRows: 10, NextPK: 100}); err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{state: st, srcEngine: model.EngineMySQL, snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())}
	if !p.beforeRegister(tbl) {
		t.Fatal("table with a resumed plan is pending and unregistered")
	}
	cases := []struct {
		key  model.Value
		keep bool
	}{
		{int64(50), true}, {int8(50), true}, {int16(50), true}, {uint8(50), true}, {uint16(50), true},
		{uint32(50), true}, {uint64(50), true}, {uint(50), true}, // phần đã flush
		{int64(150), false}, {uint32(150), false}, // phần sẽ đọc lại
		{uint64(1000), true},            // đuôi sau MaxPK
		{uint64(math.MaxUint64), false}, // không biểu diễn được: coi như sẽ đọc lại (như trước)
	}
	for _, c := range cases {
		rc := &model.RowChange{Table: tbl, Op: model.OpInsert, Key: []model.Value{c.key},
			Cols: []model.ColValue{{Name: "id", Value: c.key}}}
		parts, err := p.keepBeforeRegister(rc)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(parts) == 1; got != c.keep {
			t.Fatalf("key %s: kept=%v, want %v", fmt.Sprintf("%T(%v)", c.key, c.key), got, c.keep)
		}
	}
}
