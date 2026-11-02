package pipeline

import (
	"container/heap"
	"context"

	"strand/internal/model"
)

// Ở chế độ strict, commit phải theo đúng seq (CP-5). Scheduler phát Txn sẵn sàng theo thứ tự
// DAG, không theo seq; nếu mọi worker đang giữ seq lớn và chờ lượt, Txn seq nhỏ nhất nằm
// trong hàng đợi không ai lấy: deadlock. Bộ điều phối này sắp lại Txn sẵn sàng theo seq và chỉ
// giao Txn có seq kế tiếp, nên worker đang chờ lượt luôn chờ Txn đã được giao cho worker khác.

type seqHeap []*model.Txn

func (h seqHeap) Len() int            { return len(h) }
func (h seqHeap) Less(i, j int) bool  { return h[i].Seq() < h[j].Seq() }
func (h seqHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *seqHeap) Push(x interface{}) { *h = append(*h, x.(*model.Txn)) }
func (h *seqHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (p *Pipeline) strictDispatcher(ctx context.Context, in <-chan *model.Txn, out chan<- *model.Txn) {
	h := &seqHeap{}
	next := uint64(1)
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-in:
			if t == nil {
				return
			}
			heap.Push(h, t)
		}
		for h.Len() > 0 && (*h)[0].Seq() == next {
			t := heap.Pop(h).(*model.Txn)
			select {
			case out <- t:
			case <-ctx.Done():
				return
			}
			next++
		}
	}
}
