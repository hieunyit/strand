package pipeline

import (
	"context"
	"testing"

	"strand/internal/admin"
)

// review R12: admin socket mở trước khi sched/snap/governor được dựng. Lệnh cần chúng phải trả lỗi
// rõ ràng (không panic); Top/Pause/Resume vẫn chạy.
func TestAdminHandlerNotReady(t *testing.T) {
	p := &Pipeline{id: "p1"} // sched, snap, gov, comp đều nil — như lúc vừa mở socket
	h := &adminHandler{p: p}
	ctx := context.Background()

	top, err := h.Top(ctx)
	if err != nil || top.State != "starting" || top.LagSeconds != 0 {
		t.Fatalf("Top while starting = %+v, %v; want state starting, lag 0", top, err)
	}
	if _, err := h.ThrottleSet(ctx, admin.Request{SourceIOPS: 100}); err != errNotReady {
		t.Fatalf("ThrottleSet while starting: err = %v, want errNotReady", err)
	}
	if err := h.SnapshotAdd(ctx, "db.t"); err != errNotReady {
		t.Fatalf("SnapshotAdd while starting: err = %v, want errNotReady", err)
	}
	if _, err := h.VerifyDelta(ctx); err != errNotReady {
		t.Fatalf("VerifyDelta while starting: err = %v, want errNotReady", err)
	}
	if _, err := h.CheckpointRewind(ctx, "x"); err != errNotReady {
		t.Fatalf("CheckpointRewind while starting: err = %v, want errNotReady", err)
	}
}
