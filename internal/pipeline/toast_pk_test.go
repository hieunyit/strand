package pipeline

import (
	"fmt"
	"path/filepath"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// spec: 02 §8 — UPDATE đổi PK mà cột TOAST không đổi khi bảng (source PG) còn chờ tới lượt snapshot
// (range chưa đăng ký, plan dở từ lần chạy trước: [1,99] đã flush, [100,200] sẽ đọc lại).
func TestToastPK_BeforeRegisterPG(t *testing.T) {
	st, err := openLocalState(t.TempDir(), "p")
	if err != nil {
		t.Fatal(err)
	}
	tbl := model.TableID{Schema: "app", Table: "t"}
	if err := st.SetPlan(tbl.String(), snapshot.SnapshotPlan{MinPK: 1, MaxPK: 200, ChunkRows: 10, NextPK: 100}); err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{state: st, snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())}
	p.snap.LoadPlan = func(t model.TableID) (snapshot.SnapshotPlan, bool) { return st.Plan(t.String()) }
	p.snap.SavePlan = func(t model.TableID, pl snapshot.SnapshotPlan) error { return st.SetPlan(t.String(), pl) }

	move := func(from, to int64) *model.RowChange {
		return &model.RowChange{Table: tbl, Op: model.OpUpdate, Key: []model.Value{to}, OldKey: []model.Value{from},
			Cols: []model.ColValue{{Name: "id", Value: to}, {Name: "doc", Unchanged: true}}}
	}
	desc := func(parts []*model.RowChange) string {
		s := ""
		for _, r := range parts {
			s += fmt.Sprintf("%s%v/%v ", r.Op, r.Key, r.OldKey)
		}
		return s
	}
	cases := []struct {
		from, to int64
		want     string
	}{
		{5, 50, "update[50]/[5] "},     // cả hai đã ở target: SQL điều kiện của writer
		{5, 150, "delete[5]/[] "},      // khóa mới sẽ được đọc: chỉ xóa khóa cũ
		{150, 160, ""},                 // cả hai sẽ được đọc
		{150, 60, ""},                  // khóa cũ sẽ được đọc, khóa mới thì không: đọc lại 60
		{60, 7, ""},                    // 60 đang chờ đọc lại (coi như sẽ đọc): đọc lại 7
		{8, 60, "delete[8]/[] "},       // khóa mới đang chờ đọc lại
		{9, 1000, "update[1000]/[9] "}, // sau MaxPK: đã ở target
	}
	for _, c := range cases {
		parts, err := p.keepBeforeRegisterPG(move(c.from, c.to))
		if err != nil {
			t.Fatal(err)
		}
		if got := desc(parts); got != c.want {
			t.Fatalf("move %d -> %d: %q, want %q", c.from, c.to, got, c.want)
		}
	}
	pl, _ := st.Plan(tbl.String())
	if fmt.Sprint(pl.Reread) != "[60 7]" {
		t.Fatalf("plan reread %v, want [60 7] (persisted before the txn is admitted)", pl.Reread)
	}
	// Event thường trên khóa chờ đọc lại bị bỏ (sẽ được đọc lại).
	parts, _ := p.keepBeforeRegisterPG(&model.RowChange{Table: tbl, Op: model.OpUpdate, Key: []model.Value{int64(7)},
		Cols: []model.ColValue{{Name: "id", Value: int64(7)}, {Name: "doc", Value: "x"}}})
	if len(parts) != 0 {
		t.Fatalf("event on a key waiting for re-read must be dropped, got %s", desc(parts))
	}
	// Restart đọc lại plan từ đĩa.
	st2, err := openLocalState(filepath.Dir(st.path), "p")
	if err != nil {
		t.Fatal(err)
	}
	if pl2, _ := st2.Plan(tbl.String()); fmt.Sprint(pl2.Reread) != "[60 7]" {
		t.Fatalf("plan on disk %v", pl2.Reread)
	}
}
