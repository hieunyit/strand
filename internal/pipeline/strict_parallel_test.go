package pipeline

import (
	"testing"
	"time"

	"strand/internal/model"
)

func spTxn(rows ...model.RowChange) *model.Txn { return &model.Txn{Rows: rows} }

func spRow(tbl string, key int64) model.RowChange {
	return model.RowChange{Table: model.TableID{Schema: "app", Table: tbl}, Op: model.OpUpdate, Key: []model.Value{key}}
}

func TestStrictBatchKeysConflicts(t *testing.T) {
	a := keysOfBatch([]*model.Txn{spTxn(spRow("t", 1), spRow("t", 2))}, 0)
	b := keysOfBatch([]*model.Txn{spTxn(spRow("t", 3))}, 0)
	c := keysOfBatch([]*model.Txn{spTxn(spRow("t", 2))}, 0)
	other := keysOfBatch([]*model.Txn{spTxn(spRow("u", 1))}, 0)
	if a.conflicts(b) || b.conflicts(a) || a.conflicts(other) {
		t.Fatal("disjoint keys must not conflict")
	}
	if !a.conflicts(c) || !c.conflicts(a) {
		t.Fatal("shared key app.t/2 must conflict")
	}
	// Unique key và khóa cha FK cùng namespace với khóa bị tham chiếu.
	uk := spRow("t", 9)
	uk.UniqueKeys = []model.UniqueKey{{Name: "email", Key: []model.Value{"x@y"}}}
	uk2 := spRow("t", 10)
	uk2.UniqueKeys = []model.UniqueKey{{Name: "email", OldKey: []model.Value{"x@y"}}}
	if !keysOfBatch([]*model.Txn{spTxn(uk)}, 0).conflicts(keysOfBatch([]*model.Txn{spTxn(uk2)}, 0)) {
		t.Fatal("rows sharing a unique key value (old or new) must conflict")
	}
	child := model.RowChange{Table: model.TableID{Schema: "app", Table: "child"}, Op: model.OpInsert, Key: []model.Value{int64(5)},
		ForeignKeys: []model.ForeignKey{{ParentTable: model.TableID{Schema: "app", Table: "t"}, ParentKey: []model.Value{int64(2)}}}}
	if !keysOfBatch([]*model.Txn{spTxn(child)}, 0).conflicts(c) {
		t.Fatal("a child row must conflict with its parent row")
	}
	// Keyless, vượt ngưỡng cấp bảng, DDL.
	keyless := model.RowChange{Table: model.TableID{Schema: "app", Table: "t"}, Op: model.OpInsert}
	if !keysOfBatch([]*model.Txn{spTxn(keyless)}, 0).conflicts(b) {
		t.Fatal("a keyless row locks the whole table")
	}
	if big := keysOfBatch([]*model.Txn{spTxn(spRow("t", 100), spRow("t", 101), spRow("t", 102))}, 2); !big.conflicts(b) {
		t.Fatal("more keys than table_level_threshold lock the whole table")
	}
	if ddl := keysOfBatch([]*model.Txn{{DDL: "ALTER TABLE app.z ADD c INT"}}, 0); !ddl.conflicts(other) || !other.conflicts(ddl) {
		t.Fatal("a DDL batch conflicts with everything")
	}
	empty := keysOfBatch([]*model.Txn{spTxn()}, 0)
	if empty.conflicts(a) {
		t.Fatal("an empty transaction (watermark, heartbeat) conflicts with nothing")
	}
}

// Batch mới chỉ chờ batch CÒN ĐANG GHI chung khóa; batch đã ghi xong (chỉ chờ lượt commit) không chặn.
func TestInflightSetWaitsOnlyForApplyingBatches(t *testing.T) {
	s := newInflightSet()
	a := keysOfBatch([]*model.Txn{spTxn(spRow("t", 1))}, 0)
	b := keysOfBatch([]*model.Txn{spTxn(spRow("t", 1))}, 0)
	free := keysOfBatch([]*model.Txn{spTxn(spRow("t", 2))}, 0)
	if !s.acquire(a) || !s.acquire(free) {
		t.Fatal("acquire")
	}
	got := make(chan bool, 1)
	go func() { got <- s.acquire(b) }()
	select {
	case <-got:
		t.Fatal("b shares a key with a batch still applying: must wait")
	case <-time.After(50 * time.Millisecond):
	}
	s.applied(a)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("acquire after applied returned false")
		}
	case <-time.After(time.Second):
		t.Fatal("b must start once a only waits for its commit turn")
	}
	s.release(a)
	s.release(b)
	// Dừng: acquire đang chờ trả false.
	c := keysOfBatch([]*model.Txn{spTxn(spRow("t", 2))}, 0)
	go func() { got <- s.acquire(c) }()
	time.Sleep(20 * time.Millisecond)
	s.stop()
	if ok := <-got; ok {
		t.Fatal("acquire after stop must return false")
	}
}
