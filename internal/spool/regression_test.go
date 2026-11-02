package spool_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"errors"
	"io"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/spool"
)

type crashingBucket struct {
	*spool.MemoryBucketBackend
	crashOnDelete bool
}

func (b *crashingBucket) Delete(ctx context.Context, key string) error {
	err := b.MemoryBucketBackend.Delete(ctx, key)
	if b.crashOnDelete {
		panic("simulated crash right after a bucket object was deleted")
	}
	return err
}

func (b *crashingBucket) Close() error { return nil }

// invariant: CP-1 (spool durability)
// Truncate deleted overflow objects from the bucket BEFORE committing the Pebble
// batch that removes their references. A crash in between left references to
// deleted objects, and every later Iter failed.
func TestSpoolTruncateCrashNeverLeavesDanglingBucketRefs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket := &crashingBucket{MemoryBucketBackend: spool.NewMemoryBucketBackend()}

	s, err := spool.New(dir, spool.WithLocalHardLimit(1), spool.WithBucket(bucket, "p/"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		txn := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "u:" + string(rune('0'+i))},
			Rows: []model.RowChange{{Table: model.TableID{Schema: "a", Table: "t"}, Op: model.OpInsert, Key: []model.Value{int64(i)}}}}
		if err := s.Append(ctx, txn); err != nil {
			t.Fatal(err)
		}
	}

	applied, _ := checkpoint.ParseAppliedSet(model.EngineMySQL, "u:1-2")
	bucket.crashOnDelete = true
	func() {
		defer func() { _ = recover() }()
		_ = s.Truncate(ctx, applied)
	}()
	bucket.crashOnDelete = false
	_ = s.Close()

	s2, err := spool.New(dir, spool.WithLocalHardLimit(1), spool.WithBucket(bucket, "p/"))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	it, err := s2.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	for {
		txn, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("spool unreadable after crash in Truncate: %v", err)
		}
		_ = txn
	}
}

// invariant: CO-1
// The old spool codec dropped UniqueKeys/ForeignKeys (writeset), DDL, SourceTS and
// SpilledTables: a Txn replayed from the spool could run unordered or skip a DDL.
func TestCO1_SpoolCodecKeepsWritesetFields(t *testing.T) {
	orig := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "u:7"},
		SourceTS:  1762000000, DDL: "", SpilledTables: []model.TableID{{Schema: "a", Table: "t"}},
		Rows: []model.RowChange{{
			Table: model.TableID{Schema: "a", Table: "t"}, Op: model.OpUpdate, Key: []model.Value{int64(1)},
			UniqueKeys:  []model.UniqueKey{{Name: "uk", Key: []model.Value{"x"}, OldKey: []model.Value{"y"}}},
			ForeignKeys: []model.ForeignKey{{ParentTable: model.TableID{Schema: "a", Table: "p"}, ParentKey: []model.Value{int64(9)}}},
			Cols:        []model.ColValue{{Name: "c", Value: int16(3)}},
		}},
	}
	ddl := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "u:8"}, DDL: "ALTER TABLE a.t ADD x INT"}
	for _, in := range []*model.Txn{orig, ddl} {
		raw, err := spool.EncodeTxnForTest(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := spool.DecodeTxnForTest(raw)
		if err != nil {
			t.Fatal(err)
		}
		if out.DDL != in.DDL || out.SourceTS != in.SourceTS || len(out.SpilledTables) != len(in.SpilledTables) {
			t.Fatalf("txn fields lost: %+v", out)
		}
		if a, b := in.WritesetByTable(), out.WritesetByTable(); len(a) != len(b) || len(a["a.t"]) != len(b["a.t"]) || len(a["a.p"]) != len(b["a.p"]) {
			t.Fatalf("writeset changed through spool: %v vs %v", a, b)
		}
	}
}
