package sched

import (
	"context"
	"testing"

	"strand/internal/model"
)

// Repeated mutations in a transaction need only one cleanup entry per hash.
// Completing an older owner must preserve the dependency on the newer owner.
func TestRepeatedKeyRetainedOncePerTransaction(t *testing.T) {
	for _, strict := range []bool{false, true} {
		cfg := DefaultConfig()
		cfg.Serial = strict
		s := NewScheduler(cfg)
		txs := make([]*model.Txn, 3)
		for i := range txs {
			tx := hotTxn(32)
			row := tx.Rows[0]
			for range 99 {
				tx.Rows = append(tx.Rows, row)
			}
			txs[i] = tx
			if err := s.Admit(context.Background(), tx); err != nil {
				t.Fatal(err)
			}
			want := i + 1
			if strict {
				want = 0
			}
			if s.retainedKeys != want {
				t.Fatalf("strict=%t after admit %d: retained %d, want %d", strict, i, s.retainedKeys, want)
			}
		}
		for _, tx := range txs {
			select {
			case next := <-s.Ready():
				if next != tx {
					t.Fatalf("strict=%t emitted seq %d, want %d", strict, next.Seq(), tx.Seq())
				}
			default:
				t.Fatalf("strict=%t missing seq %d", strict, tx.Seq())
			}
			if !strict {
				select {
				case next := <-s.Ready():
					t.Fatalf("seq %d ran before owner %d finished", next.Seq(), tx.Seq())
				default:
				}
			}
			s.Done(tx.Seq(), nil)
		}
		if s.retainedKeys != 0 || s.TotalWritesetEntries() != 0 {
			t.Fatalf("strict=%t leaked completed writeset", strict)
		}
		s.Close()
	}
}
