package pipeline

import (
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// lsnTracker: mốc xác nhận không bao giờ vượt Txn chưa commit; Txn bị bỏ (đã apply / rỗng) không
// giữ mốc lại.
func TestLSNTrackerPoint(t *testing.T) {
	var tr lsnTracker
	if p := tr.point(0); p != 0 {
		t.Fatalf("empty tracker point = %d", p)
	}
	tr.routed(100, 1)
	tr.routed(150, 0) // bỏ qua (đã apply)
	tr.routed(200, 2)
	tr.routed(300, 3)
	if p := tr.point(0); p != 99 {
		t.Fatalf("nothing done: point = %d, want 99", p)
	}
	if p := tr.point(1); p != 199 {
		t.Fatalf("seq 1 done: point = %d, want 199", p)
	}
	if p := tr.point(3); p != 300 {
		t.Fatalf("all done: point = %d, want 300", p)
	}
	tr.routed(400, 0) // Txn rỗng sau khi mọi thứ đã xong
	if p := tr.point(3); p != 400 {
		t.Fatalf("idle skipped txn: point = %d, want 400", p)
	}
	// Seq được Done trước khi router kịp ghi nhận: mốc không vượt LSN đã thấy.
	if p := tr.point(5); p != 400 {
		t.Fatalf("unknown seq done: point = %d, want 400", p)
	}
	tr.routed(500, 5)
	tr.routed(600, 6)
	if p := tr.point(5); p != 599 {
		t.Fatalf("seq 5 done, 6 pending: point = %d, want 599", p)
	}

	// Kết hợp với PGAppliedSet: Txn chưa commit (600) không bị coi là đã apply.
	s := checkpoint.NewPGAppliedSet(0)
	s.Add(model.Pos{Engine: model.EnginePG, LSN: 700})
	s.Vouch(tr.point(5), nil)
	if s.Contains(model.Pos{Engine: model.EnginePG, LSN: 600}) || !s.Contains(model.Pos{Engine: model.EnginePG, LSN: 500}) ||
		!s.Contains(model.Pos{Engine: model.EnginePG, LSN: 700}) {
		t.Fatalf("vouched set wrong: %s", s.String())
	}
}
