package e2e

import "testing"

// Kiểm thử thuần (không container) cho quyết định của checkPGInvariants / checkPGInvariantsSnapshotKilled:
// ghi lại phiên bản bởi snapshot sau kill -9 (range đã flush mà plan chưa lưu) được chấp nhận chỉ ở chế độ
// snapshotRewrites; writer apply hai lần, hay apply phiên bản mà snapshot đã ghi trước, luôn là lỗi.
func TestApplyLogDupDecision(t *testing.T) {
	cases := []struct {
		name           string
		cdc, snap      int
		firstCDC       bool
		strict, killed bool // kết quả mong đợi: true = vi phạm
	}{
		{"once by writer", 1, 0, true, false, false},
		{"once by snapshot", 0, 1, false, false, false},
		{"snapshot re-read after kill", 0, 2, false, true, false},
		{"snapshot re-read three times", 0, 3, false, true, false},
		{"writer then snapshot re-read", 1, 1, true, true, false},
		{"writer then two re-reads", 1, 2, true, true, false},
		{"writer twice", 2, 0, true, true, true},
		{"writer twice plus re-read", 2, 1, true, true, true},
		{"snapshot then writer (Txn ≤ high re-applied)", 1, 1, false, true, true},
	}
	for _, c := range cases {
		if got := applyLogDup(c.cdc, c.snap, c.firstCDC, false); got != c.strict {
			t.Errorf("%s strict: dup = %v, want %v", c.name, got, c.strict)
		}
		if got := applyLogDup(c.cdc, c.snap, c.firstCDC, true); got != c.killed {
			t.Errorf("%s snapshotRewrites: dup = %v, want %v", c.name, got, c.killed)
		}
	}
}
