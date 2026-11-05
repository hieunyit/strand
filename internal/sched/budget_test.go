package sched

import (
	"context"
	"testing"
	"time"

	"strand/internal/model"
)

// invariant: CO-2 — review R09. Một Txn đầu kẹt (chưa Done) trên khóa nóng: các Txn sau cùng khóa chờ
// trong DAG. Admit phải dừng nhận khi phần giữ trong RAM (entry khóa theo seq, số / byte Txn chờ) chạm
// trần, và nhận tiếp khi Txn đầu Done — không tăng vô hạn, không khóa chết.
func TestCO2_BlockedDAGIsBoundedAndResumes(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  func(*Config)
		txn  func() *model.Txn
	}{
		{"retained_keys", func(c *Config) { c.WritesetBudget = 8 }, func() *model.Txn { return hotTxn(0) }},
		{"pending_count", func(c *Config) { c.MaxPending = 8 }, func() *model.Txn { return hotTxn(0) }},
		{"pending_bytes", func(c *Config) { c.MaxPendingBytes = 8 << 10 }, func() *model.Txn { return hotTxn(1 << 10) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.cfg(&cfg)
			s := NewScheduler(cfg)
			defer s.Close()
			ctx := context.Background()
			admitted := make(chan struct{}, 100)
			go func() {
				for i := 0; i < 100; i++ {
					if err := s.Admit(ctx, tc.txn()); err != nil {
						return
					}
					admitted <- struct{}{}
				}
			}()
			first := <-s.Ready()
			n := countWithin(admitted, 300*time.Millisecond)
			if n == 0 || n > 10 {
				t.Fatalf("admitted %d txns behind a stuck head, want 1..10 (bounded)", n)
			}
			s.mu.Lock()
			pending, keys := len(s.pendingTxns), s.retainedKeys
			s.mu.Unlock()
			if pending > 9 || keys > 10 {
				t.Fatalf("pending=%d retained keys=%d: not bounded", pending, keys)
			}
			// Txn đầu Done → các Txn còn lại lần lượt đi qua.
			s.Done(first.Seq(), nil)
			total := n
			deadline := time.After(10 * time.Second)
			for total < 100 {
				select {
				case r := <-s.Ready():
					s.Done(r.Seq(), nil)
				case <-admitted:
					total++
				case <-deadline:
					t.Fatalf("only %d/100 admitted after the head completed", total)
				}
			}
		})
	}
}

func hotTxn(bytes int64) *model.Txn {
	return &model.Txn{Bytes: bytes, Rows: []model.RowChange{{Table: model.TableID{Schema: "app", Table: "hot"}, Key: []model.Value{int64(1)}}}}
}

func countWithin(ch <-chan struct{}, d time.Duration) int {
	n := 0
	timeout := time.After(d)
	for {
		select {
		case <-ch:
			n++
		case <-timeout:
			return n
		}
	}
}
