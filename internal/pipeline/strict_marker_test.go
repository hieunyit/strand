package pipeline

import (
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Promote bước 3 (PG source): marker fence là logical decoding message không transactional, reader
// phát thành Txn rỗng XID = 0. Ở strict nó phải đi qua scheduler (row toàn cục chỉ ghi cùng batch),
// ở relaxed thì không cần (compactor dời mốc qua lsnTracker).
func TestSkipEmptyPGTxn(t *testing.T) {
	pg := func(lsn, xid uint64) *model.Txn {
		return &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: lsn}, XID: xid}
	}
	if !skipEmptyPGTxn(false, pg(100, 0)) {
		t.Fatalf("relaxed: empty watermark txn must be skipped")
	}
	if skipEmptyPGTxn(true, pg(100, 0)) {
		t.Fatalf("strict: empty watermark txn must go through the scheduler so the durable lwd reaches it")
	}
	if !skipEmptyPGTxn(true, pg(100, 42)) {
		t.Fatalf("strict: empty txn with an xid must still be skipped")
	}
	rows := pg(100, 0)
	rows.Rows = []model.RowChange{{}}
	if skipEmptyPGTxn(true, rows) || skipEmptyPGTxn(false, rows) {
		t.Fatalf("txn with rows is never skipped")
	}
}

// Strict: batch commit theo seq nên mốc bền vững tới được LSN của Txn cuối batch kể cả khi router
// chưa kịp ghi nhận Txn đó vào lsnTracker (noteRouted chạy sau Admit).
func TestStrictGlobalVouchesLastTxnOfBatch(t *testing.T) {
	p := &Pipeline{strictApplied: checkpoint.NewPGAppliedSet(0), lsns: &lsnTracker{}}
	t1 := &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: 100}, XID: 7}
	t1.SetSeq(1)
	p.lsns.routed(100, 1)
	marker := &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: 200}}
	marker.SetSeq(2) // router chưa gọi noteRouted cho seq 2
	p.strictGlobal([]*model.Txn{t1})
	p.strictGlobal([]*model.Txn{marker})
	if got := p.strictApplied.Lwd().LSN; got != 200 {
		t.Fatalf("strict lwd = %d, want 200 (last txn of the committed batch)", got)
	}
}
