package pipeline

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// review R14: hai `snapshot add` đồng thời cho cùng bảng — chỉ đúng một lệnh giành được quyền.
func TestClaimSnapshotAddExclusive(t *testing.T) {
	p := &Pipeline{}
	id := model.TableID{Schema: "db", Table: "t"}
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if p.claimSnapshotAdd(id) {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if n := wins.Load(); n != 1 {
		t.Fatalf("concurrent claims won = %d, want 1", n)
	}
	if !p.claimSnapshotAdd(model.TableID{Schema: "db", Table: "other"}) {
		t.Fatalf("claim of a different table must succeed")
	}
	p.releaseSnapshotAdd(id)
	if !p.claimSnapshotAdd(id) {
		t.Fatalf("claim after release must succeed")
	}
}

// SnapshotAdd từ chối bảng đang được một lệnh add khác xử lý (chưa đăng ký range), và nhả claim
// khi trả lỗi sớm.
func TestSnapshotAddRejectsInFlightTable(t *testing.T) {
	p := &Pipeline{snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())}
	id := model.TableID{Schema: "db", Table: "t"}
	if !p.claimSnapshotAdd(id) { // lệnh thứ nhất đang chạy (đang tải metadata, tạo bảng đích...)
		t.Fatal("first claim failed")
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("second SnapshotAdd went past the claim check: panic %v", r)
			}
		}()
		err := p.SnapshotAdd(context.Background(), "db.t")
		if err == nil || !strings.Contains(err.Error(), "already being snapshotted") {
			t.Fatalf("second SnapshotAdd err = %v, want already being snapshotted", err)
		}
	}()
	p.releaseSnapshotAdd(id)

	// Lỗi sớm sau khi giành claim (bảng đã đăng ký range) phải nhả claim.
	p.snap.Manager.RegisterTable(id, "id", nil)
	if err := p.SnapshotAdd(context.Background(), "db.t"); err == nil {
		t.Fatal("SnapshotAdd of a registered table must fail")
	}
	if !p.claimSnapshotAdd(id) {
		t.Fatal("claim leaked after an early error return")
	}
}
