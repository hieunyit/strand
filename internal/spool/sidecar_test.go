package spool

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

const spillSID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func spillTxn(path string, seq string) *model.Txn {
	return &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: spillSID + ":" + seq}, SpillPath: path, RowCount: 1, SpillOwned: true}
}
func writeSidecar(t *testing.T, dir string, n int) string {
	t.Helper()
	p := filepath.Join(dir, "spill")
	if err := os.WriteFile(p, []byte(strings.Repeat("x", n)), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}
func appliedGTIDs(t *testing.T, s string) checkpoint.AppliedSet {
	t.Helper()
	a, err := checkpoint.ParseMySQLGTIDSet(spillSID + ":" + s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func TestSidecarQuotaRejectsPayload(t *testing.T) {
	dir := t.TempDir()
	path := writeSidecar(t, dir, 4096)
	s, err := New(filepath.Join(dir, "db"), WithTotalHardLimit(1024))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	tx := spillTxn(path, "1")
	err = s.Append(context.Background(), tx)
	if exitcode.GetExitCode(err) != exitcode.DataError || !tx.SpillOwned {
		t.Fatalf("err=%v owned=%v", err, tx.SpillOwned)
	}
	if s.Usage().LocalBytes != 0 {
		t.Fatalf("rejected file charged: %+v", s.Usage())
	}
}
func TestSidecarSharedRetainedAcrossTruncateRestart(t *testing.T) {
	dir := t.TempDir()
	path := writeSidecar(t, dir, 4096)
	dbdir := filepath.Join(dir, "db")
	ctx := context.Background()
	s, err := New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []string{"1", "2"} {
		if err = s.Append(ctx, spillTxn(path, i)); err != nil {
			t.Fatal(err)
		}
	}
	both := s.Usage().LocalBytes
	if both < 4096 || both >= 8192 {
		t.Fatalf("shared file should be charged once: %d", both)
	}
	if err = s.Truncate(ctx, appliedGTIDs(t, "1")); err != nil {
		t.Fatal(err)
	}
	one := s.Usage().LocalBytes
	if one < 4096 || one >= both {
		t.Fatalf("shared file lost charge: before=%d after=%d", both, one)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	if s.Usage().LocalBytes != one {
		t.Fatalf("usage after restart=%d want %d", s.Usage().LocalBytes, one)
	}
	if err = s.Truncate(ctx, appliedGTIDs(t, "1-2")); err != nil {
		t.Fatal(err)
	}
	if s.Usage().LocalBytes != 4096 {
		t.Fatalf("retained file not charged: %+v", s.Usage())
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("borrowed sidecar was removed: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Usage().LocalBytes != 4096 {
		t.Fatalf("orphan retained file lost on restart: %+v", s.Usage())
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = s.Append(ctx, &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: spillSID + ":3"}}); err != nil {
		t.Fatal(err)
	}
	if s.Usage().LocalBytes >= 4096 {
		t.Fatalf("deleted file not released: %+v", s.Usage())
	}
}
func TestSidecarOverflowAndMigration(t *testing.T) {
	dir := t.TempDir()
	path := writeSidecar(t, dir, 512)
	dbdir := filepath.Join(dir, "db")
	bucket := NewMemoryBucketBackend()
	opts := []Option{WithLocalHardLimit(513), WithBucket(bucket, "s/")}
	ctx := context.Background()
	s, err := New(dbdir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append(ctx, spillTxn(path, "1")); err != nil {
		t.Fatal(err)
	}
	if err = s.Append(ctx, spillTxn(path, "2")); err != nil {
		t.Fatal(err)
	}
	if u := s.Usage(); u.LocalBytes != 512 || u.BucketBytes == 0 {
		t.Fatalf("buffer accounting: %+v", u)
	}
	if err = s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// Recreate an older database with no sidecar index: the migration must fetch bucket txns.
	batch := s.db.NewBatch()
	if err = batch.Delete(sidecarVersionKey, nil); err != nil {
		t.Fatal(err)
	}
	if err = batch.Delete(sidecarKey(path), nil); err != nil {
		t.Fatal(err)
	}
	if err = batch.Delete(sidecarRefKey(1), nil); err != nil {
		t.Fatal(err)
	}
	if err = batch.Delete(sidecarRefKey(2), nil); err != nil {
		t.Fatal(err)
	}
	if err = batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	bucket.ResetCounters()
	s, err = New(dbdir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if bucket.GetCalls() != 1 {
		t.Fatalf("migration fetched one segment %d times", bucket.GetCalls())
	}
	if u := s.Usage(); u.LocalBytes != 512 || u.BucketBytes == 0 {
		t.Fatalf("migration accounting: %+v", u)
	}
	if err = s.Truncate(ctx, appliedGTIDs(t, "1-2")); err != nil {
		t.Fatal(err)
	}
	if u := s.Usage(); u.LocalBytes != 512 || u.BucketBytes != 0 {
		t.Fatalf("after truncate: %+v", u)
	}
}
func TestSidecarMissingReferencedFailsClosed(t *testing.T) {
	dir := t.TempDir()
	path := writeSidecar(t, dir, 512)
	dbdir := filepath.Join(dir, "db")
	s, err := New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append(context.Background(), spillTxn(path, "1")); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = s.Append(context.Background(), spillTxn(path, "2")); err == nil {
		t.Fatal("missing referenced file allowed")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = New(dbdir); err == nil {
		s.Close()
		t.Fatal("reopened missing referenced file")
	}
}
func TestSidecarOverflowFailureRetainsCharge(t *testing.T) {
	dir := t.TempDir()
	path := writeSidecar(t, dir, 512)
	bucket := NewMemoryBucketBackend()
	s, err := New(filepath.Join(dir, "db"), WithLocalHardLimit(513), WithBucket(bucket, "s/"), WithBeforeBatchCommit(func() error { return os.ErrPermission }))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append(context.Background(), spillTxn(path, "1")); err != nil {
		t.Fatal(err)
	}
	if err = s.Flush(context.Background()); err == nil {
		t.Fatal("expected flush failure")
	}
	if u := s.Usage(); u.LocalBytes != 512 {
		t.Fatalf("retained failed sidecar uncharged: %+v", u)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSidecarLegacyOrphanIsCharged(t *testing.T) {
	dir := t.TempDir()
	dbdir := filepath.Join(dir, "db")
	s, err := New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	// Model the directory layout left by pre-index prefetch after truncating a txn.
	if err = os.MkdirAll(filepath.Join(dbdir, "spill"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dbdir, "spill", "old.bin")
	if err = os.WriteFile(path, make([]byte, 1234), 0600); err != nil {
		t.Fatal(err)
	}
	if err = s.db.Delete(sidecarVersionKey, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.Usage().LocalBytes; got != 1234 {
		t.Fatalf("legacy orphan usage=%d", got)
	}
}

func TestSidecarLegacyBucketMigrationRequiresBackend(t *testing.T) {
	dir := t.TempDir()
	path := writeSidecar(t, dir, 512)
	dbdir := filepath.Join(dir, "db")
	bucket := NewMemoryBucketBackend()
	s, err := New(dbdir, WithLocalHardLimit(513), WithBucket(bucket, "s/"))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Append(context.Background(), spillTxn(path, "1")); err != nil {
		t.Fatal(err)
	}
	if err = s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := s.db.NewBatch()
	_ = b.Delete(sidecarVersionKey, nil)
	_ = b.Delete(sidecarKey(path), nil)
	_ = b.Delete(sidecarRefKey(1), nil)
	if err = b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = New(dbdir); err == nil {
		s.Close()
		t.Fatal("legacy bucket refs accepted without migration backend")
	}
}
