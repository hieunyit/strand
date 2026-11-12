package mysql

import (
	"context"
	"testing"
	"time"

	"strand/internal/checkpoint"
)

// Hồi quy: dưới tải liên tục, pipeline binlog-position không bao giờ báo "synced" vì TxnFreeToHead quét tới
// EOF hiện tại (gồm cả Txn ghi sau mốc head) và seen được lấy trước head. Kiểm tra: Txn ghi SAU khi đã lấy
// head không làm kết quả thành false; Txn nằm giữa from và head vẫn làm kết quả false.
func TestTxnFreeToHead_Container_IgnoresTxnAfterSampledHead(t *testing.T) {
	db, _, _ := startLegacyMySQL(t, "mysql:5.6")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE app; CREATE TABLE app.t (id INT PRIMARY KEY AUTO_INCREMENT, v INT) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	headKey := func() (string, uint64) {
		f, p, err := MasterStatus(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		base, seq, err := checkpoint.SplitBinlogName(f)
		if err != nil {
			t.Fatal(err)
		}
		k, err := checkpoint.BinlogKey(seq, p)
		if err != nil {
			t.Fatal(err)
		}
		return base, k
	}
	insert := func() {
		if _, err := db.ExecContext(ctx, "INSERT INTO app.t (v) VALUES (1)"); err != nil {
			t.Fatal(err)
		}
	}

	base, k0 := headKey()
	if ok, err := TxnFreeToHead(ctx, db, base, k0); err != nil || !ok {
		t.Fatalf("idle source at head: ok=%v err=%v, want true", ok, err)
	}

	// Txn trong (from, head]: chưa kịp.
	insert()
	if ok, err := TxnFreeToHead(ctx, db, base, k0); err != nil || ok {
		t.Fatalf("Txn between from and head: ok=%v err=%v, want false", ok, err)
	}

	// Txn commit sau khi head đã được lấy (tải liên tục): reader đã thấy tới head → kịp. FLUSH để from < head
	// với chỉ event không thuộc Txn ở giữa (Rotate, Format_desc...), buộc phải quét tới head.
	_, k1 := headKey()
	if _, err := db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
		t.Fatal(err)
	}
	ok, err := TxnFreeToHeadFunc(ctx, db, base, func() uint64 {
		insert()
		insert()
		return k1
	})
	if err != nil || !ok {
		t.Fatalf("Txn committed after the sampled head: ok=%v err=%v, want true", ok, err)
	}
	// Đối chứng: các Txn đó nằm sau k1, nên lấy head mới thì không còn kịp.
	if ok, err := TxnFreeToHead(ctx, db, base, k1); err != nil || ok {
		t.Fatalf("from=k1 after new Txn: ok=%v err=%v, want false", ok, err)
	}
}
