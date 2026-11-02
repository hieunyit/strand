package pipeline

import (
	"context"
	"testing"
	"time"

	"strand/internal/model"
)

// invariant: CP-5 (liveness)
// Strict: Txn phải được giao cho worker theo đúng seq, nếu không mọi worker có thể giữ seq
// lớn và chờ lượt trong khi seq nhỏ nhất còn nằm trong hàng đợi (deadlock).
func TestCP5_StrictDispatcherReleasesInSeqOrder(t *testing.T) {
	in := make(chan *model.Txn, 10)
	out := make(chan *model.Txn, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &Pipeline{}
	go p.strictDispatcher(ctx, in, out)
	for _, s := range []uint64{3, 5, 1, 2, 4} {
		tx := &model.Txn{}
		tx.SetSeq(s)
		in <- tx
	}
	for want := uint64(1); want <= 5; want++ {
		select {
		case got := <-out:
			if got.Seq() != want {
				t.Fatalf("dispatched seq %d, want %d", got.Seq(), want)
			}
		case <-time.After(time.Second):
			t.Fatalf("seq %d never dispatched", want)
		}
	}
}
