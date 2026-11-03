package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"strand/internal/model"
)

func btxn(seq uint64, rows int, valBytes int) *model.Txn {
	t := &model.Txn{}
	t.SetSeq(seq)
	for i := 0; i < rows; i++ {
		t.Rows = append(t.Rows, model.RowChange{Op: model.OpInsert, Key: []model.Value{int64(i)},
			Cols: []model.ColValue{{Name: "b", Value: strings.Repeat("x", valBytes)}}})
	}
	return t
}

func feed(txns ...*model.Txn) chan *model.Txn {
	ch := make(chan *model.Txn, len(txns)+1)
	for _, t := range txns {
		ch <- t
	}
	return ch
}

func TestGatherBatchLimits(t *testing.T) {
	ctx := context.Background()
	// max_rows
	b, rows, _ := gatherBatch(ctx, btxn(1, 3, 1), feed(btxn(2, 3, 1), btxn(3, 3, 1)), batchLimits{maxRows: 5, maxBytes: 1 << 20})
	if len(b) != 2 || rows != 6 {
		t.Fatalf("max_rows: got %d txns %d rows, want 2 txns", len(b), rows)
	}
	// max_bytes: mỗi Txn ~1000 byte, ngưỡng 2500 -> đóng sau Txn thứ 3
	b, _, _ = gatherBatch(ctx, btxn(1, 1, 1000), feed(btxn(2, 1, 1000), btxn(3, 1, 1000), btxn(4, 1, 1000)), batchLimits{maxRows: 1000, maxBytes: 2500})
	if len(b) != 3 {
		t.Fatalf("max_bytes: got %d txns, want 3", len(b))
	}
	// Txn tràn đĩa đứng một mình
	big := &model.Txn{SpillPath: "/x", RowCount: 10}
	big.SetSeq(1)
	b, _, _ = gatherBatch(ctx, big, feed(btxn(2, 1, 1)), batchLimits{maxRows: 1000, maxBytes: 1 << 20})
	if len(b) != 1 {
		t.Fatalf("spilled txn must close the batch, got %d", len(b))
	}
	// strict: Txn không liền seq thành stash
	b, _, stash := gatherBatch(ctx, btxn(1, 1, 1), feed(btxn(2, 1, 1), btxn(4, 1, 1)), batchLimits{maxRows: 1000, maxBytes: 1 << 20, strict: true})
	if len(b) != 2 || stash == nil || stash.Seq() != 4 {
		t.Fatalf("strict: batch %d stash %v", len(b), stash)
	}
}

func TestGatherBatchMaxWait(t *testing.T) {
	ctx := context.Background()
	ch := make(chan *model.Txn, 4)
	go func() {
		time.Sleep(20 * time.Millisecond)
		ch <- btxn(2, 1, 1)
	}()
	// không có max_wait: không chờ Txn đến sau
	b, _, _ := gatherBatch(ctx, btxn(1, 1, 1), ch, batchLimits{maxRows: 1000, maxBytes: 1 << 20})
	if len(b) != 1 {
		t.Fatalf("no max_wait must not wait, got %d", len(b))
	}
	<-ch
	go func() {
		time.Sleep(20 * time.Millisecond)
		ch <- btxn(2, 1, 1)
	}()
	start := time.Now()
	b, _, _ = gatherBatch(ctx, btxn(1, 1, 1), ch, batchLimits{maxRows: 1000, maxBytes: 1 << 20, maxWait: 200 * time.Millisecond})
	if len(b) != 2 {
		t.Fatalf("max_wait must collect a txn arriving within the window, got %d", len(b))
	}
	if el := time.Since(start); el < 150*time.Millisecond || el > 2*time.Second {
		t.Fatalf("max_wait window not honoured: %v", el)
	}
}
