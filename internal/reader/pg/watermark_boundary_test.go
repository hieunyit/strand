package pg

import (
	"context"
	"testing"

	"strand/internal/assemble"
	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/reader"
)

// PostgreSQL logicalmsg_decode truyền EndRecPtr vào message; commit kế tiếp có thể bắt đầu
// ngay tại đó. Watermark chỉ chứng minh các commit NHỎ HƠN EndRecPtr đã được phát (CP-1/2,
// CO-1/2). RS-1/2 vẫn đạt high đọc trước khi ghi message vì high <= đầu record < EndRecPtr.
func TestWatermarkDoesNotAcknowledgeNextCommit(t *testing.T) {
	const nextCommit uint64 = 1008
	events := make(chan reader.Event, 64)
	st := &streamState{r: &Reader{}, ctx: context.Background(), out: events}
	if err := st.handle(pgoBuf{'M'}.u8(0).u64(nextCommit).str(WatermarkPrefix).u32(0)); err != nil {
		t.Fatal(err)
	}
	asm := assemble.New()
	var marker *model.Txn
	for len(events) > 0 {
		tx, err := asm.Push(<-events)
		if err != nil {
			t.Fatal(err)
		}
		if tx != nil {
			marker = tx
		}
	}
	if marker == nil {
		t.Fatal("missing watermark")
	}
	applied := checkpoint.NewPGAppliedSet(0)
	applied.Vouch(marker.SourcePos.LSN, nil)
	if applied.Contains(model.Pos{Engine: model.EnginePG, LSN: nextCommit}) {
		t.Errorf("watermark checkpoint %s acknowledges unprocessed next commit %d; restart will skip entire transaction", applied.String(), nextCommit)
	}
	if marker.SourcePos.LSN < 1000 {
		t.Fatalf("watermark no longer reaches high before its record: %d", marker.SourcePos.LSN)
	}
	// Mô phỏng crash sau khi lưu checkpoint watermark nhưng trước khi router nhận Txn kế tiếp.
	// Reader mới phải phát lại đủ row của Txn đó, không được coi nó là đã apply (CP-2).
	restarted := &streamState{r: &Reader{}, ctx: context.Background(), out: events, applied: applied.Clone(),
		rels: map[uint32]*relState{}, announced: map[uint32]uint64{}}
	change := simChange{rel: simRels[0], op: model.OpInsert, key: 7, v: "must replay", doc: "payload"}
	for _, msg := range [][]byte{
		pgoBuf{'B'}.u64(nextCommit).u64(1).u32(42),
		msgRelation(0, simRels[0]), change.msg(0),
		pgoBuf{'C'}.u8(0).u64(nextCommit).u64(nextCommit + 32).u64(1),
	} {
		if err := restarted.handle(msg); err != nil {
			t.Fatal(err)
		}
	}
	var replayed *model.Txn
	for len(events) > 0 {
		tx, err := asm.Push(<-events)
		if err != nil {
			t.Fatal(err)
		}
		if tx != nil {
			replayed = tx
		}
	}
	if replayed == nil || replayed.XID != 42 || len(replayed.Rows) != 1 {
		t.Fatalf("restart lost entire next transaction: %+v", replayed)
	}
	if err := matchRow(&change, replayed.Rows[0]); err != nil {
		t.Fatal(err)
	}
}
