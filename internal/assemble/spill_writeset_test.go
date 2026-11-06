package assemble_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
	"strand/internal/sched"
)

// assembleBig lắp một Txn 15.001 row (~45.000 khóa ở bảng con: PK + 2 UNIQUE mỗi row, dưới ngưỡng 50.000) (bảng có UNIQUE thường, UNIQUE prefix, FK tới PK và tới UNIQUE của
// bảng cha) với trần RAM maxInMem.
func assembleBig(t *testing.T, maxInMem int) *model.Txn {
	t.Helper()
	child := model.TableID{Schema: "app", Table: "child"}
	parent := model.TableID{Schema: "app", Table: "parent"}
	asm := assemble.New(assemble.WithMaxInMemRows(maxInMem), assemble.WithSpillDir(t.TempDir()),
		assemble.WithKeyMetadata(map[model.TableID]assemble.KeyMeta{child: {
			Unique: []assemble.UniqueIndex{
				{Name: "uk_code", Columns: []string{"code"}},
				{Name: "uk_pfx", Columns: []string{"note"}, Prefix: true},
			},
			ForeignKeys: []assemble.ForeignKeyMeta{
				{Columns: []string{"pid"}, ParentTable: parent},
				{Columns: []string{"pref"}, ParentTable: parent, ParentIndex: "uk_ref"},
			},
		}}))
	asm.Registry().RegisterTable("app", "child", []string{"id"}, []string{"id", "code", "note", "pid", "pref"})
	var ins [][]any
	for i := 0; i < 15_000; i++ {
		ins = append(ins, []any{int64(i), "c" + string(rune('a'+i%26)) + string(rune(i)), "n", int64(i % 97), "r" + string(rune(i%13+'a'))})
	}
	mustPush(t, asm, gtid("7"))
	mustPush(t, asm, reader.Event{Type: reader.EventQuery, Query: "BEGIN"})
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "child", Op: model.OpInsert, Rows: ins})
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "child", Op: model.OpUpdate,
		Rows: [][]any{{int64(1), "old", "n", int64(1), "ra"}, {int64(500_000), "new", "m", int64(2), "rb"}}})
	txn, err := asm.Push(reader.Event{Type: reader.EventXID})
	if err != nil || txn == nil {
		t.Fatalf("txn not finished: %v", err)
	}
	return txn
}

// invariant: CO-1 (Gate 2 G2-8). Txn > MaxInMemRows (10.000) của assembler tràn đĩa nhưng ≤
// table_level_threshold: writeset gom khi duyệt file tràn (như router) phải bằng đúng writeset của cùng
// Txn trong RAM — mọi loại khóa sống qua codec spill — và scheduler giữ cấp row cho nó.
func TestCO1_SpilledAssembledTxnWritesetMatchesInMemory(t *testing.T) {
	inMem := assembleBig(t, 1_000_000)
	if inMem.IsSpilled() {
		t.Fatal("reference txn must stay in RAM")
	}
	spilled := assembleBig(t, 10_000)
	if !spilled.IsSpilled() {
		t.Fatal("expected a spilled txn")
	}
	cfg := sched.DefaultConfig()
	ws := model.NewWritesetCollector(cfg.TableLevelThreshold, cfg.WritesetBudget)
	if err := spilled.ForEachRow(func(rc *model.RowChange) error { ws.Add(rc); return nil }); err != nil {
		t.Fatal(err)
	}
	spilled.SpillWriteset = ws.Writeset()
	if got, want := spilled.WritesetByTable(), inMem.WritesetByTable(); !reflect.DeepEqual(got, want) {
		for k, v := range want {
			t.Logf("want %s: %d, got %d", k, len(v), len(got[k]))
		}
		t.Fatalf("spilled writeset differs from in-memory writeset (%d vs %d tables)", len(got), len(want))
	}

	s := sched.NewScheduler(cfg)
	defer s.Close()
	ctx := context.Background()
	if err := s.Admit(ctx, spilled); err != nil {
		t.Fatal(err)
	}
	<-s.Ready()
	if n := s.TableWritesetEntries("app.child"); n == 0 {
		t.Fatalf("spilled txn under table_level_threshold must keep row-level entries")
	}
	free := &model.Txn{Rows: []model.RowChange{{Table: model.TableID{Schema: "app", Table: "child"}, Key: []model.Value{int64(900_000)}}}}
	if err := s.Admit(ctx, free); err != nil {
		t.Fatal(err)
	}
	select {
	case x := <-s.Ready():
		if x != free {
			t.Fatalf("unexpected ready txn")
		}
	case <-time.After(time.Second):
		t.Fatalf("txn on an untouched key must run in parallel with the spilled txn (row-level)")
	}
}
