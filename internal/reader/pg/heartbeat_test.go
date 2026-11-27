package pg

import (
	"context"
	"testing"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
)

// Heartbeat (message không transactional prefix strand_heartbeat) thành một Txn rỗng ở vị trí message − 1
// và đẩy progress của reader (slot tiến khi các bảng của pipeline không có ghi). Message transactional
// hoặc prefix lạ, hay heartbeat nằm giữa một Txn đang mở, không phát gì.
func TestHeartbeatMessageBecomesEmptyTxn(t *testing.T) {
	const at uint64 = 5000
	events := make(chan reader.Event, 64)
	r := &Reader{}
	st := &streamState{r: r, ctx: context.Background(), out: events}
	for _, m := range [][]byte{
		pgoBuf{'M'}.u8(1).u64(at - 100).str(HeartbeatPrefix).u32(0), // transactional: bỏ qua
		pgoBuf{'M'}.u8(0).u64(at - 50).str("app_message").u32(0),    // prefix của ứng dụng khác
		pgoBuf{'M'}.u8(0).u64(at).str(HeartbeatPrefix).u32(0),
	} {
		if err := st.handle(m); err != nil {
			t.Fatal(err)
		}
	}
	asm := assemble.New()
	var txns []*model.Txn
	for len(events) > 0 {
		tx, err := asm.Push(<-events)
		if err != nil {
			t.Fatal(err)
		}
		if tx != nil {
			txns = append(txns, tx)
		}
	}
	if len(txns) != 1 || len(txns[0].Rows) != 0 || txns[0].SourcePos.LSN != at-1 {
		t.Fatalf("heartbeat txns = %+v, want one empty txn at %d", txns, at-1)
	}
	if got := r.progress.Load(); got != at-1 {
		t.Fatalf("reader progress = %d, want %d", got, at-1)
	}
}
