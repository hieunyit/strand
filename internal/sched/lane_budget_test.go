package sched

import (
	"context"
	"testing"
	"time"

	"strand/internal/model"
)

func laneTxn(k int, bytes int64) *model.Txn {
	return &model.Txn{Lane: true, Bytes: bytes, Rows: []model.RowChange{{Table: model.TableID{Schema: "app", Table: "lane"}, Key: []model.Value{int64(k)}}}}
}

// invariant: CO-2 — review N02. Luồng toàn Txn làn, worker làn kẹt ở target (đã lấy batch, chưa Done):
// Txn làn không vào pendingTxns nên trước đây Admit nhận vô hạn (laneQ/laneReady tăng). Txn làn đang
// giữ phải tính vào MaxPending / MaxPendingBytes; nhận tiếp khi làn rút (NextLaneBatch + Done).
func TestN02_LaneOnlyWorkloadIsBounded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cfg   func(*Config)
		bytes int64
	}{
		{"pending_count", func(c *Config) { c.MaxPending = 8 }, 0},
		{"pending_bytes", func(c *Config) { c.MaxPendingBytes = 8 << 10 }, 1 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.cfg(&cfg)
			s := NewScheduler(cfg)
			defer s.Close()
			ctx := context.Background()
			const total = 100
			admitted := make(chan struct{}, total)
			go func() {
				for i := 0; i < total; i++ {
					if err := s.Admit(ctx, laneTxn(i, tc.bytes)); err != nil {
						return
					}
					admitted <- struct{}{}
				}
			}()
			// Worker làn lấy một batch rồi kẹt (chưa Done).
			stuck := s.NextLaneBatch(ctx, 1, 0)
			if len(stuck) != 1 {
				t.Fatalf("first lane batch = %d txns", len(stuck))
			}
			n := countWithin(admitted, 300*time.Millisecond)
			if n == 0 || n > 10 {
				t.Fatalf("admitted %d lane txns behind a stuck lane worker, want 1..10 (bounded)", n)
			}
			if lp := s.LanePending(); lp > 9 {
				t.Fatalf("lane queue = %d: not bounded", lp)
			}
			s.Done(stuck[0].Seq(), nil)
			got := n
			done := 1
			deadline := time.After(10 * time.Second)
			for done < total {
				select {
				case <-admitted:
					got++
				case <-deadline:
					t.Fatalf("stalled: admitted=%d done=%d", got, done)
				default:
					tctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
					b := s.NextLaneBatch(tctx, 256, 0)
					cancel()
					for _, x := range b {
						s.Done(x.Seq(), nil)
						done++
					}
				}
			}
		})
	}
}

// review N02: một Txn làn lớn hơn MaxPendingBytes vẫn được nhận khi làn trống (không khóa chết); Txn
// kế tiếp chờ tới khi nó Done.
func TestN02_OversizedLaneTxnNoDeadlock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPendingBytes = 1 << 10
	s := NewScheduler(cfg)
	defer s.Close()
	ctx := context.Background()
	big := laneTxn(1, 10<<10)
	actx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.Admit(actx, big); err != nil {
		t.Fatalf("oversized lane txn must be admitted when nothing is held: %v", err)
	}
	second := make(chan error, 1)
	go func() { second <- s.Admit(ctx, laneTxn(2, 10<<10)) }()
	select {
	case err := <-second:
		t.Fatalf("second oversized txn admitted while first is held: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	b := s.NextLaneBatch(ctx, 256, 0)
	if len(b) != 1 || b[0] != big {
		t.Fatalf("lane batch = %d", len(b))
	}
	s.Done(big.Seq(), nil)
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Admit not woken after lane Done")
	}
}
