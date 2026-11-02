package spool_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/spool"
)

// AC1: Append/Iter/Truncate đúng sau restart
func TestAC1_RestartAppendIterTruncate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	uuid := "3e11fa47-71ca-11e1-9e33-c80aa9429562"

	// 1. Khởi tạo Spool lần 1 và ghi 3 transaction
	sp1, err := spool.New(dir)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}

	txn1 := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":1"},
		SchemaVer: 1,
		Bytes:     120,
		Rows: []model.RowChange{
			{
				Table: model.TableID{Schema: "app", Table: "users"},
				Op:    model.OpInsert,
				Key:   []model.Value{1},
				Cols: []model.ColValue{
					{Name: "id", Value: 1},
					{Name: "name", Value: "Alice"},
				},
			},
		},
	}

	txn2 := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":2"},
		SchemaVer: 1,
		Bytes:     250,
		Rows: []model.RowChange{
			{
				Table:  model.TableID{Schema: "app", Table: "users"},
				Op:     model.OpUpdate,
				Key:    []model.Value{20},
				OldKey: []model.Value{10},
				Cols: []model.ColValue{
					{Name: "id", Value: 20},
					{Name: "name", Value: "Bob"},
				},
			},
		},
	}

	txn3 := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":3"},
		SchemaVer: 1,
		Bytes:     300,
		Rows: []model.RowChange{
			{
				Table: model.TableID{Schema: "app", Table: "orders"},
				Op:    model.OpInsert,
				Key:   []model.Value{101},
				Cols: []model.ColValue{
					{Name: "id", Value: 101},
					{Name: "amount", Value: 99.5},
				},
			},
		},
	}

	if err := sp1.Append(ctx, txn1); err != nil {
		t.Fatalf("Append txn1 failed: %v", err)
	}
	if err := sp1.Append(ctx, txn2); err != nil {
		t.Fatalf("Append txn2 failed: %v", err)
	}
	if err := sp1.Append(ctx, txn3); err != nil {
		t.Fatalf("Append txn3 failed: %v", err)
	}

	// Đóng spool (mô phỏng crash/restart)
	if err := sp1.Close(); err != nil {
		t.Fatalf("Close sp1 failed: %v", err)
	}

	// 2. Mở lại Spool lần 2 từ cùng thư mục (RESTART)
	sp2, err := spool.New(dir)
	if err != nil {
		t.Fatalf("spool.New (restart 1) failed: %v", err)
	}

	// Kiểm tra Iter từ đầu: phải đọc được cả 3 txn theo đúng thứ tự
	iter, err := sp2.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter failed: %v", err)
	}

	r1, err := iter.Next(ctx)
	if err != nil || r1.SourcePos.GTID != uuid+":1" {
		t.Fatalf("expected txn1, got %v, err=%v", r1, err)
	}
	if r1.Rows[0].Op != model.OpInsert {
		t.Fatalf("expected r1 OpInsert, got %v", r1.Rows[0].Op)
	}

	r2, err := iter.Next(ctx)
	if err != nil || r2.SourcePos.GTID != uuid+":2" {
		t.Fatalf("expected txn2, got %v, err=%v", r2, err)
	}
	if r2.Rows[0].Op != model.OpUpdate {
		t.Fatalf("expected r2 OpUpdate, got %v", r2.Rows[0].Op)
	}
	// Kiểm tra OldKey của UPDATE đổi PK được bảo toàn
	if len(r2.Rows[0].OldKey) == 0 || r2.Rows[0].OldKey[0] != 10 {
		t.Fatalf("expected r2 OldKey [10], got %v", r2.Rows[0].OldKey)
	}

	r3, err := iter.Next(ctx)
	if err != nil || r3.SourcePos.GTID != uuid+":3" {
		t.Fatalf("expected txn3, got %v, err=%v", r3, err)
	}
	if r3.Rows[0].Op != model.OpInsert {
		t.Fatalf("expected r3 OpInsert, got %v", r3.Rows[0].Op)
	}
	_, err = iter.Next(ctx)
	if err != io.EOF {
		t.Fatalf("expected io.EOF at end of iter, got %v", err)
	}
	_ = iter.Close()

	// Kiểm tra Iter có `from`: đọc từ txn2
	iterFrom, err := sp2.Iter(ctx, model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":2"})
	if err != nil {
		t.Fatalf("Iter from txn2 failed: %v", err)
	}
	rf2, err := iterFrom.Next(ctx)
	if err != nil || rf2.SourcePos.GTID != uuid+":2" {
		t.Fatalf("expected rf2 (txn2), got %v, err=%v", rf2, err)
	}
	rf3, err := iterFrom.Next(ctx)
	if err != nil || rf3.SourcePos.GTID != uuid+":3" {
		t.Fatalf("expected rf3 (txn3), got %v, err=%v", rf3, err)
	}
	_ = iterFrom.Close()

	// Kiểm tra Iter với `from` không tồn tại phải trả về lỗi
	_, errNotFound := sp2.Iter(ctx, model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":999"})
	if errNotFound == nil {
		t.Fatalf("expected error when Iter with non-existent position")
	}

	// Truncate: đánh dấu txn1 và txn2 đã được apply
	applied := checkpoint.NewMySQLAppliedSet(uuid)
	applied.Add(model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":1"})
	applied.Add(model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":2"})

	if err := sp2.Truncate(ctx, applied); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Đóng spool (mô phỏng restart lần 2)
	if err := sp2.Close(); err != nil {
		t.Fatalf("Close sp2 failed: %v", err)
	}

	// 3. Mở lại Spool lần 3 (RESTART 2)
	sp3, err := spool.New(dir)
	if err != nil {
		t.Fatalf("spool.New (restart 2) failed: %v", err)
	}
	defer sp3.Close()

	// Iter từ đầu: chỉ còn lại txn3, txn1 và txn2 đã bị loại bỏ vĩnh viễn
	iterPostTruncate, err := sp3.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter post-truncate failed: %v", err)
	}
	defer iterPostTruncate.Close()

	pt1, err := iterPostTruncate.Next(ctx)
	if err != nil || pt1.SourcePos.GTID != uuid+":3" {
		t.Fatalf("expected only txn3 after truncate and restart, got %v, err=%v", pt1, err)
	}
	_, err = iterPostTruncate.Next(ctx)
	if err != io.EOF {
		t.Fatalf("expected EOF after txn3, got %v", err)
	}

	// Ghi tiếp txn4 vào sp3
	txn4 := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":4"},
		SchemaVer: 1,
		Bytes:     180,
	}
	if err := sp3.Append(ctx, txn4); err != nil {
		t.Fatalf("Append txn4 failed: %v", err)
	}

	t.Logf("AC1 PASS: Append/Iter/Truncate đúng đắn qua các lần restart")
}

// AC2: Usage báo đúng số byte
func TestAC2_UsageBytesAccuracy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	uuid := "550e8400-e29b-41d4-a716-446655440000"

	// 1. Mở Spool mới: ban đầu 0 byte
	sp1, err := spool.New(dir)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}
	if sp1.Usage().LocalBytes != 0 {
		t.Fatalf("expected initial LocalBytes = 0, got %d", sp1.Usage().LocalBytes)
	}

	// 2. Ghi Txn 1
	txn1 := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":1"},
		SchemaVer: 1,
		Rows: []model.RowChange{
			{
				Table: model.TableID{Schema: "db", Table: "t1"},
				Op:    model.OpInsert,
				Key:   []model.Value{100},
				Cols:  []model.ColValue{{Name: "c1", Value: "hello world"}},
			},
		},
	}
	if err := sp1.Append(ctx, txn1); err != nil {
		t.Fatalf("Append txn1 failed: %v", err)
	}

	u1 := sp1.Usage().LocalBytes
	if u1 <= 0 {
		t.Fatalf("expected positive LocalBytes after Append txn1, got %d", u1)
	}
	t.Logf("Sau Append txn1: LocalBytes = %d", u1)

	// 3. Ghi Txn 2
	txn2 := &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":2"},
		SchemaVer: 1,
		Rows: []model.RowChange{
			{
				Table: model.TableID{Schema: "db", Table: "t1"},
				Op:    model.OpInsert,
				Key:   []model.Value{200},
				Cols:  []model.ColValue{{Name: "c1", Value: "second transaction payload"}},
			},
		},
	}
	if err := sp1.Append(ctx, txn2); err != nil {
		t.Fatalf("Append txn2 failed: %v", err)
	}

	u2 := sp1.Usage().LocalBytes
	if u2 <= u1 {
		t.Fatalf("expected LocalBytes to increase after txn2 (got u1=%d, u2=%d)", u1, u2)
	}
	txn2Size := u2 - u1
	t.Logf("Sau Append txn2: LocalBytes = %d (tăng %d bytes)", u2, txn2Size)

	// Đóng Spool
	if err := sp1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 4. Mở lại Spool (RESTART 1): Usage phải khớp chính xác u2
	sp2, err := spool.New(dir)
	if err != nil {
		t.Fatalf("spool.New restart failed: %v", err)
	}

	restartedUsage := sp2.Usage().LocalBytes
	if restartedUsage != u2 {
		t.Fatalf("AC2 FAIL: LocalBytes sau restart = %d, không khớp trước restart = %d", restartedUsage, u2)
	}
	t.Logf("LocalBytes sau restart 1 khớp hoàn hảo: %d bytes", restartedUsage)

	// 5. Truncate Txn 1: LocalBytes phải giảm đúng kích thước của Txn 1 (còn lại txn2Size)
	applied := checkpoint.NewMySQLAppliedSet(uuid)
	applied.Add(model.Pos{Engine: model.EngineMySQL, GTID: uuid + ":1"})
	if err := sp2.Truncate(ctx, applied); err != nil {
		t.Fatalf("Truncate txn1 failed: %v", err)
	}

	postTruncateUsage := sp2.Usage().LocalBytes
	if postTruncateUsage != txn2Size {
		t.Fatalf("AC2 FAIL: expected post-truncate LocalBytes = %d, got %d", txn2Size, postTruncateUsage)
	}
	t.Logf("Sau Truncate txn1: LocalBytes = %d (giảm đúng %d bytes của txn1)", postTruncateUsage, u1)

	// Đóng Spool lần 2
	if err := sp2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 6. Mở lại Spool (RESTART 2): Usage sau truncate phải tiếp tục bảo toàn
	sp3, err := spool.New(dir)
	if err != nil {
		t.Fatalf("spool.New restart 2 failed: %v", err)
	}
	defer sp3.Close()

	if sp3.Usage().LocalBytes != txn2Size {
		t.Fatalf("AC2 FAIL: LocalBytes sau restart 2 = %d, expected %d", sp3.Usage().LocalBytes, txn2Size)
	}

	t.Logf("AC2 PASS: Usage báo chính xác số byte lưu trữ qua mọi thao tác và qua các lần restart")
}

// Kiểm tra mã hóa và giải mã với nhiều kiểu dữ liệu Go phong phú
func TestCodec_Roundtrip(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	orig := &model.Txn{
		SourcePos: model.Pos{
			Engine: model.EnginePG,
			LSN:    987654321,
		},
		SchemaVer: 42,
		Bytes:     1024,
		RowCount:  2,
		SpillPath: "/tmp/spill.tmp",
		Rows: []model.RowChange{
			{
				Table: model.TableID{Schema: "public", Table: "orders"},
				Op:    model.OpInsert,
				Key:   []model.Value{123, "order-abc"},
				Cols: []model.ColValue{
					{Name: "id", Value: 123},
					{Name: "code", Value: "order-abc"},
					{Name: "raw", Value: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
					{Name: "active", Value: true},
					{Name: "ratio", Value: 3.14159},
					{Name: "created_at", Value: now},
					{Name: "nullable", Value: nil},
				},
			},
			{
				Table:  model.TableID{Schema: "public", Table: "orders"},
				Op:     model.OpUpdate,
				Key:    []model.Value{456},
				OldKey: []model.Value{123},
				Cols: []model.ColValue{
					{Name: "id", Value: 456},
					{Name: "unchanged_col", Value: nil, Unchanged: true},
				},
			},
			{
				Table: model.TableID{Schema: "public", Table: "orders"},
				Op:    model.OpDelete,
				Key:   []model.Value{789},
				Cols:  []model.ColValue{{Name: "id", Value: 789}},
			},
		},
	}

	data, err := spool.EncodeTxnForTest(orig)
	if err != nil {
		t.Fatalf("EncodeTxn failed: %v", err)
	}

	decoded, err := spool.DecodeTxnForTest(data)
	if err != nil {
		t.Fatalf("DecodeTxn failed: %v", err)
	}

	if decoded.SourcePos.Engine != orig.SourcePos.Engine || decoded.SourcePos.LSN != orig.SourcePos.LSN {
		t.Fatalf("SourcePos mismatch: got %+v, want %+v", decoded.SourcePos, orig.SourcePos)
	}
	if decoded.SchemaVer != orig.SchemaVer || decoded.SpillPath != orig.SpillPath {
		t.Fatalf("Metadata mismatch: %+v vs %+v", decoded, orig)
	}
	if len(decoded.Rows) != len(orig.Rows) {
		t.Fatalf("Rows count mismatch: %d vs %d", len(decoded.Rows), len(orig.Rows))
	}

	r0 := decoded.Rows[0]
	if r0.Op != model.OpInsert {
		t.Fatalf("r0 Op mismatch: got %v, want OpInsert", r0.Op)
	}
	if r0.Key[0] != 123 || r0.Key[1] != "order-abc" {
		t.Fatalf("Key mismatch: %v", r0.Key)
	}
	if r0.Cols[3].Value != true {
		t.Fatalf("Bool col mismatch: %v", r0.Cols[3].Value)
	}

	r1 := decoded.Rows[1]
	if r1.Op != model.OpUpdate {
		t.Fatalf("r1 Op mismatch: got %v, want OpUpdate", r1.Op)
	}
	if len(r1.OldKey) == 0 || r1.OldKey[0] != 123 {
		t.Fatalf("OldKey mismatch: %v", r1.OldKey)
	}
	if !r1.Cols[1].Unchanged {
		t.Fatalf("Unchanged col mismatch: %v", r1.Cols[1])
	}

	r2 := decoded.Rows[2]
	if r2.Op != model.OpDelete {
		t.Fatalf("r2 Op mismatch: got %v, want OpDelete", r2.Op)
	}
	if r2.Key[0] != 789 {
		t.Fatalf("r2 Key mismatch: %v", r2.Key)
	}
}

func makeOverflowTxn(uuid string, seq int) *model.Txn {
	return &model.Txn{
		SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", uuid, seq)},
		SchemaVer: 1,
		Bytes:     128,
		RowCount:  1,
		SourceTS:  1700000000 + int64(seq),
		Rows: []model.RowChange{
			{
				Table: model.TableID{Schema: "app", Table: "events"},
				Op:    model.OpInsert,
				Key:   []model.Value{seq},
				Cols: []model.ColValue{
					{Name: "id", Value: seq},
					{Name: "payload", Value: fmt.Sprintf("payload-data-%06d", seq)},
				},
			},
		},
	}
}

// 1. 5000 overflow txns -> Put calls <= 5000/2000+2; iterate returns all 5000 in order,
// byte-identical to input (compare encoded form); Get calls <= number of segments.
func TestSegment_Batch5000OverflowTxns(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	uuid := "11111111-2222-3333-4444-555555555555"

	memBucket := spool.NewMemoryBucketBackend()
	sp, err := spool.New(dir,
		spool.WithLocalHardLimit(1),
		spool.WithBucket(memBucket, "pref/"),
		spool.WithSegmentMaxAge(time.Hour), // chỉ đo gom theo kích thước/số Txn, không phụ thuộc tốc độ máy (-race)
	)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}
	defer sp.Close()

	const totalTxns = 5000
	wantEncoded := make([][]byte, totalTxns)
	for i := 1; i <= totalTxns; i++ {
		tx := makeOverflowTxn(uuid, i)
		enc, err := spool.EncodeTxnForTest(tx)
		if err != nil {
			t.Fatalf("EncodeTxnForTest(%d) failed: %v", i, err)
		}
		wantEncoded[i-1] = enc
		if err := sp.Append(ctx, tx); err != nil {
			t.Fatalf("Append(%d) failed: %v", i, err)
		}
	}

	if err := sp.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	putCalls := memBucket.PutCalls()
	maxPutCalls := totalTxns/2000 + 2
	if putCalls <= 0 || putCalls > maxPutCalls {
		t.Fatalf("PutCalls = %d, want in [1, %d]", putCalls, maxPutCalls)
	}

	keys, err := memBucket.List(ctx, "pref/seg/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) != putCalls {
		t.Fatalf("segment count = %d, want %d (keys=%v)", len(keys), putCalls, keys)
	}
	for _, k := range keys {
		raw, err := memBucket.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", k, err)
		}
		if len(raw) < 5 || string(raw[:5]) != "STSG1" {
			t.Fatalf("segment %s missing STSG1 magic", k)
		}
	}
	memBucket.ResetCounters()

	iter, err := sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter failed: %v", err)
	}
	defer iter.Close()

	for i := 1; i <= totalTxns; i++ {
		got, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("iter.Next at %d failed: %v", i, err)
		}
		gotEnc, err := spool.EncodeTxnForTest(got)
		if err != nil {
			t.Fatalf("EncodeTxnForTest(got %d) failed: %v", i, err)
		}
		if !bytes.Equal(gotEnc, wantEncoded[i-1]) {
			t.Fatalf("txn %d encoded bytes mismatch", i)
		}
	}
	if _, err := iter.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after %d txns, got %v", totalTxns, err)
	}

	getCalls := memBucket.GetCalls()
	if getCalls <= 0 || getCalls > len(keys) {
		t.Fatalf("GetCalls = %d, want <= %d (number of segments)", getCalls, len(keys))
	}
}

// 2. age flush: one txn, wait 600 ms, second Append triggers a PUT of the first segment.
func TestSegment_AgeFlush(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	uuid := "22222222-3333-4444-5555-666666666666"

	memBucket := spool.NewMemoryBucketBackend()
	sp, err := spool.New(dir,
		spool.WithLocalHardLimit(1),
		spool.WithBucket(memBucket, "pref/"),
	)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}
	defer sp.Close()

	if err := sp.Append(ctx, makeOverflowTxn(uuid, 1)); err != nil {
		t.Fatalf("first Append failed: %v", err)
	}
	if got := memBucket.PutCalls(); got != 0 {
		t.Fatalf("expected 0 PutCalls right after first Append, got %d", got)
	}

	time.Sleep(600 * time.Millisecond)

	if err := sp.Append(ctx, makeOverflowTxn(uuid, 2)); err != nil {
		t.Fatalf("second Append failed: %v", err)
	}
	if got := memBucket.PutCalls(); got != 1 {
		t.Fatalf("expected second Append after 600ms to trigger 1 PutCall, got %d", got)
	}

	iter, err := sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter failed: %v", err)
	}
	defer iter.Close()

	for i := 1; i <= 2; i++ {
		tx, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("iter.Next(%d) failed: %v", i, err)
		}
		wantGTID := fmt.Sprintf("%s:%d", uuid, i)
		if tx.SourcePos.GTID != wantGTID {
			t.Fatalf("got GTID %s, want %s", tx.SourcePos.GTID, wantGTID)
		}
	}
	if _, err := iter.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// 3. crash safety: Put succeeds then simulate failure before batch commit -> reopen spool:
// no refs, iterate returns nothing, no error.
func TestSegment_CrashSafetyBeforeBatchCommit(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	uuid := "33333333-4444-5555-6666-777777777777"

	memBucket := spool.NewMemoryBucketBackend()
	simErr := errors.New("simulated crash before pebble batch commit")

	sp1, err := spool.New(dir,
		spool.WithLocalHardLimit(1),
		spool.WithBucket(memBucket, "pref/"),
	)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}
	spool.SetBeforeBatchCommitForTest(sp1, func() error {
		return simErr
	})

	if err := sp1.Append(ctx, makeOverflowTxn(uuid, 1)); err != nil {
		t.Fatalf("Append(1) failed: %v", err)
	}
	if err := sp1.Append(ctx, makeOverflowTxn(uuid, 2)); err != nil {
		t.Fatalf("Append(2) failed: %v", err)
	}

	flushErr := sp1.Flush(ctx)
	if !errors.Is(flushErr, simErr) {
		t.Fatalf("expected Flush error %v, got %v", simErr, flushErr)
	}
	// Put đã thành công trước khi batch commit bị lỗi
	if got := memBucket.PutCalls(); got != 1 {
		t.Fatalf("expected 1 PutCall before simulated crash, got %d", got)
	}

	if err := sp1.Close(); err != nil {
		t.Fatalf("Close sp1 failed: %v", err)
	}

	// Mở lại spool trên cùng thư mục
	sp2, err := spool.New(dir,
		spool.WithLocalHardLimit(1),
		spool.WithBucket(memBucket, "pref/"),
	)
	if err != nil {
		t.Fatalf("spool.New reopen failed: %v", err)
	}
	defer sp2.Close()

	if u := sp2.Usage(); u.LocalBytes != 0 || u.BucketBytes != 0 {
		t.Fatalf("expected 0 usage after reopen, got %+v", u)
	}

	iter, err := sp2.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter after reopen failed: %v", err)
	}
	defer iter.Close()

	tx, err := iter.Next(ctx)
	if !errors.Is(err, io.EOF) || tx != nil {
		t.Fatalf("expected empty iter (io.EOF) after crash before batch commit, got tx=%v, err=%v", tx, err)
	}
}

// 4. Truncate of half the txns keeps the segment object; truncate of the rest deletes it;
// usage returns to 0.
func TestSegment_TruncateHalfThenRest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	uuid := "44444444-5555-6666-7777-888888888888"

	memBucket := spool.NewMemoryBucketBackend()
	sp, err := spool.New(dir,
		spool.WithLocalHardLimit(1),
		spool.WithBucket(memBucket, "pref/"),
	)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}
	defer sp.Close()

	const count = 10
	for i := 1; i <= count; i++ {
		if err := sp.Append(ctx, makeOverflowTxn(uuid, i)); err != nil {
			t.Fatalf("Append(%d) failed: %v", i, err)
		}
	}
	if err := sp.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	keys, err := memBucket.List(ctx, "pref/seg/")
	if err != nil || len(keys) != 1 {
		t.Fatalf("expected 1 segment object in bucket, got %v (err=%v)", keys, err)
	}
	fullUsage := sp.Usage().BucketBytes
	if fullUsage <= 0 {
		t.Fatalf("expected positive BucketBytes, got %d", fullUsage)
	}

	// Truncate nửa đầu (1..5): segment object vẫn phải được giữ lại
	appliedHalf := checkpoint.NewMySQLAppliedSet(uuid)
	for i := 1; i <= count/2; i++ {
		appliedHalf.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", uuid, i)})
	}
	if err := sp.Truncate(ctx, appliedHalf); err != nil {
		t.Fatalf("Truncate first half failed: %v", err)
	}

	keysAfterHalf, err := memBucket.List(ctx, "pref/seg/")
	if err != nil || len(keysAfterHalf) != 1 {
		t.Fatalf("expected segment object to be kept after half truncate, got %v (err=%v)", keysAfterHalf, err)
	}
	halfUsage := sp.Usage().BucketBytes
	if halfUsage <= 0 || halfUsage >= fullUsage {
		t.Fatalf("expected BucketBytes after half truncate in (0, %d), got %d", fullUsage, halfUsage)
	}

	// Kiểm tra Iter chỉ còn lại 6..10
	iter, err := sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter after half truncate failed: %v", err)
	}
	for i := count/2 + 1; i <= count; i++ {
		tx, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("iter.Next(%d) failed: %v", i, err)
		}
		wantGTID := fmt.Sprintf("%s:%d", uuid, i)
		if tx.SourcePos.GTID != wantGTID {
			t.Fatalf("got GTID %s, want %s", tx.SourcePos.GTID, wantGTID)
		}
	}
	if _, err := iter.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = iter.Close()

	// Truncate phần còn lại (6..10): segment object phải bị xóa và usage về 0
	appliedAll := checkpoint.NewMySQLAppliedSet(uuid)
	for i := 1; i <= count; i++ {
		appliedAll.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", uuid, i)})
	}
	if err := sp.Truncate(ctx, appliedAll); err != nil {
		t.Fatalf("Truncate rest failed: %v", err)
	}

	keysAfterAll, err := memBucket.List(ctx, "pref/seg/")
	if err != nil || len(keysAfterAll) != 0 {
		t.Fatalf("expected segment object to be deleted after full truncate, got %v (err=%v)", keysAfterAll, err)
	}
	if u := sp.Usage(); u.BucketBytes != 0 || u.LocalBytes != 0 {
		t.Fatalf("expected usage to return to 0 after full truncate, got %+v", u)
	}
}

// 5. old-format BUCKET: refs written by the previous code path still iterate and truncate.
func TestSegment_BackwardCompatLegacyBucketRefs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	uuid := "55555555-6666-7777-8888-999999999999"

	memBucket := spool.NewMemoryBucketBackend()
	sp, err := spool.New(dir,
		spool.WithLocalHardLimit(1),
		spool.WithBucket(memBucket, "pref/"),
	)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}
	defer sp.Close()

	// Ghi 2 txn theo định dạng cũ BUCKET:<prefix>txn/<seq>
	if err := spool.AppendLegacyBucketForTest(ctx, sp, makeOverflowTxn(uuid, 1)); err != nil {
		t.Fatalf("AppendLegacyBucketForTest(1) failed: %v", err)
	}
	if err := spool.AppendLegacyBucketForTest(ctx, sp, makeOverflowTxn(uuid, 2)); err != nil {
		t.Fatalf("AppendLegacyBucketForTest(2) failed: %v", err)
	}
	// Ghi tiếp 2 txn theo định dạng mới BUCKETSEG:
	if err := sp.Append(ctx, makeOverflowTxn(uuid, 3)); err != nil {
		t.Fatalf("Append(3) failed: %v", err)
	}
	if err := sp.Append(ctx, makeOverflowTxn(uuid, 4)); err != nil {
		t.Fatalf("Append(4) failed: %v", err)
	}
	if err := sp.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	legacyKeys, _ := memBucket.List(ctx, "pref/txn/")
	segKeys, _ := memBucket.List(ctx, "pref/seg/")
	if len(legacyKeys) != 2 || len(segKeys) != 1 {
		t.Fatalf("expected 2 legacy keys and 1 seg key, got legacy=%v seg=%v", legacyKeys, segKeys)
	}

	// Iterate phải đọc được cả 4 txn theo đúng thứ tự
	iter, err := sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter failed: %v", err)
	}
	for i := 1; i <= 4; i++ {
		tx, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("iter.Next(%d) failed: %v", i, err)
		}
		wantGTID := fmt.Sprintf("%s:%d", uuid, i)
		if tx.SourcePos.GTID != wantGTID {
			t.Fatalf("got GTID %s, want %s", tx.SourcePos.GTID, wantGTID)
		}
	}
	if _, err := iter.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = iter.Close()

	// Truncate cả 4 txn
	applied := checkpoint.NewMySQLAppliedSet(uuid)
	for i := 1; i <= 4; i++ {
		applied.Add(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", uuid, i)})
	}
	if err := sp.Truncate(ctx, applied); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	allKeys, _ := memBucket.List(ctx, "pref/")
	if len(allKeys) != 0 {
		t.Fatalf("expected all legacy and segment objects deleted, got %v", allKeys)
	}
	if u := sp.Usage(); u.BucketBytes != 0 || u.LocalBytes != 0 {
		t.Fatalf("expected 0 usage after truncate, got %+v", u)
	}
}

// 6. Put failure -> Append/Flush returns the error, nothing is referenced;
// also verify bucket Get errors in Iter are returned and never swallowed.
func TestSegment_PutFailureAndIterGetError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	uuid := "66666666-7777-8888-9999-000000000000"

	memBucket := spool.NewMemoryBucketBackend()
	sp, err := spool.New(dir,
		spool.WithLocalHardLimit(1),
		spool.WithBucket(memBucket, "pref/"),
	)
	if err != nil {
		t.Fatalf("spool.New failed: %v", err)
	}
	defer sp.Close()

	putFailErr := errors.New("simulated bucket Put error")
	memBucket.SetPutError(putFailErr)

	// Case A: Append 1 txn rồi Flush -> Flush trả lỗi, không có gì được tham chiếu
	if err := sp.Append(ctx, makeOverflowTxn(uuid, 1)); err != nil {
		t.Fatalf("Append(1) failed unexpectedly before flush: %v", err)
	}
	if err := sp.Flush(ctx); !errors.Is(err, putFailErr) {
		t.Fatalf("expected Flush error wrapping %v, got %v", putFailErr, err)
	}

	// Case B: Append đủ 2000 txns để kích hoạt flush ngay trong Append -> Append trả lỗi
	var appendErr error
	for i := 1; i <= 2000; i++ {
		if err := sp.Append(ctx, makeOverflowTxn(uuid, i)); err != nil {
			appendErr = err
			break
		}
	}
	if !errors.Is(appendErr, putFailErr) {
		t.Fatalf("expected Append error wrapping %v on 2000th txn, got %v", putFailErr, appendErr)
	}

	memBucket.SetPutError(nil)

	if u := sp.Usage(); u.BucketBytes != 0 || u.LocalBytes != 0 {
		t.Fatalf("expected 0 usage after failed Put, got %+v", u)
	}
	iter, err := sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter failed: %v", err)
	}
	if tx, err := iter.Next(ctx); !errors.Is(err, io.EOF) || tx != nil {
		t.Fatalf("expected nothing referenced (io.EOF), got tx=%v, err=%v", tx, err)
	}
	_ = iter.Close()

	// Case C: Khi Put thành công nhưng Get trong Iter lỗi -> Iter.Next phải trả về lỗi (không nuốt lỗi)
	if err := sp.Append(ctx, makeOverflowTxn(uuid, 10)); err != nil {
		t.Fatalf("Append(10) failed: %v", err)
	}
	if err := sp.Flush(ctx); err != nil {
		t.Fatalf("Flush(10) failed: %v", err)
	}

	getFailErr := errors.New("simulated bucket Get error")
	memBucket.SetGetError(getFailErr)

	iter2, err := sp.Iter(ctx, model.Pos{})
	if err != nil {
		t.Fatalf("Iter2 failed: %v", err)
	}
	defer iter2.Close()

	_, nextErr := iter2.Next(ctx)
	if !errors.Is(nextErr, getFailErr) || !strings.Contains(nextErr.Error(), "pref/seg/") {
		t.Fatalf("expected Iter.Next to return Get error %v, got %v", getFailErr, nextErr)
	}
}
