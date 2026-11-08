package spool_test

import (
	"context"
	"path/filepath"
	"testing"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/spool"
)

func TestSpoolTakesSpillOwnership(t *testing.T) {
	for _, tc := range []struct {
		name     string
		options  func() []spool.Option
		accepted bool
	}{
		{"local", func() []spool.Option { return nil }, true},
		{"buffered_bucket", func() []spool.Option {
			return []spool.Option{spool.WithLocalHardLimit(1), spool.WithBucket(spool.NewMemoryBucketBackend(), "test/")}
		}, true},
		{"capacity_rejected", func() []spool.Option { return []spool.Option{spool.WithTotalHardLimit(1)} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sb := assemble.NewSpillBuffer(1, dir)
			for i := 0; i < 2; i++ {
				if err := sb.Append(model.RowChange{Op: model.OpInsert, Key: []model.Value{int64(i)}}); err != nil {
					t.Fatal(err)
				}
			}
			txn := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}}
			if err := sb.Finish(txn); err != nil {
				t.Fatal(err)
			}
			if !txn.SpillOwned {
				t.Fatal("assembler did not mark temporary spill owned")
			}
			sp, err := spool.New(filepath.Join(dir, "spool"), tc.options()...)
			if err != nil {
				t.Fatal(err)
			}
			defer sp.Close()
			err = sp.Append(context.Background(), txn)
			if tc.accepted && err != nil {
				t.Fatal(err)
			}
			if !tc.accepted && err == nil {
				t.Fatal("expected capacity failure")
			}
			if txn.SpillOwned == tc.accepted {
				t.Fatalf("after append accepted=%v spill ownership=%v", tc.accepted, txn.SpillOwned)
			}
			if tc.accepted {
				if err := sp.Flush(context.Background()); err != nil {
					t.Fatal(err)
				}
				it, err := sp.Iter(context.Background(), model.Pos{})
				if err != nil {
					t.Fatal(err)
				}
				got, err := it.Next(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if got.SpillOwned {
					t.Fatal("decoded spool reference must be borrowed")
				}
				if err := it.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
