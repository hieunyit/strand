package verify

import (
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// ApplyMark chế độ vị trí binlog: target theo kịp khi Lwd của tập đã apply ≥ vị trí đầu binlog lúc đọc S1.
func TestApplyMarkBinlogPosMissing(t *testing.T) {
	mark := func(seq uint32, off uint64) ApplyMark {
		k, _ := checkpoint.BinlogKey(seq, off)
		return ApplyMark{pos: k, posBase: "mysql-bin", text: "mysql-bin:x"}
	}
	key := func(seq uint32, off uint64) uint64 { k, _ := checkpoint.BinlogKey(seq, off); return k }
	set := checkpoint.NewMySQLPosAppliedSet("mysql-bin", key(2, 500))
	if miss, err := mark(2, 500).missing(model.EngineMySQL, set); err != nil || miss != "" {
		t.Fatalf("covered: %q %v", miss, err)
	}
	if miss, err := mark(2, 900).missing(model.EngineMySQL, set); err != nil || miss == "" {
		t.Fatalf("not covered: %q %v", miss, err)
	}
	// extra trên Lwd không làm mốc "đã phủ" (còn Txn chưa apply giữa chừng).
	set.Add(checkpoint.PosOfKey("mysql-bin", key(2, 1000)))
	if miss, _ := mark(2, 900).missing(model.EngineMySQL, set); miss == "" {
		t.Fatal("extra above lwd must not satisfy the mark")
	}
	if _, err := mark(2, 500).missing(model.EngineMySQL, checkpoint.NewMySQLPosAppliedSet("other-bin", key(2, 500))); err == nil {
		t.Fatal("different base name accepted")
	}
	if _, err := mark(2, 500).missing(model.EngineMySQL, checkpoint.NewMySQLAppliedSet()); err == nil {
		// GTID set rỗng không phải tập vị trí: lỗi rõ ràng, không im lặng coi là đã phủ.
		t.Fatal("GTID set accepted for a position mark")
	}
	if miss, _ := mark(2, 500).missing(model.EngineMySQL, nil); miss == "" {
		t.Fatal("nil applied set covers a mark")
	}
}
