package pipeline

import (
	"strings"
	"testing"

	"strand/internal/model"
)

// Nghiệm thu lại (P1): hai bảng nguồn ngoài phạm vi ban đầu ánh xạ tới cùng bảng đích (app.x rename sang
// app.y, app.y giữ tên mặc định). Bảng đang vào pipeline phải giữ tên đích để lệnh thứ hai bị từ chối cả
// khi bảng thứ nhất chưa vào p.tables.
func TestReserveTargetsRejectsAConcurrentJoinToTheSameTarget(t *testing.T) {
	f, err := newTableFilter([]string{"app.base"}, nil, map[string]string{"app.x": "app.y"})
	if err != nil {
		t.Fatal(err)
	}
	base, x, y := model.TableID{Schema: "app", Table: "base"}, model.TableID{Schema: "app", Table: "x"}, model.TableID{Schema: "app", Table: "y"}
	p := &Pipeline{filter: f, tables: map[model.TableID]*tableMeta{base: {}}}

	releaseX, err := p.reserveTargets([]model.TableID{x})
	if err != nil {
		t.Fatalf("reserve app.x: %v", err)
	}
	if _, err := p.reserveTargets([]model.TableID{y}); err == nil || !strings.Contains(err.Error(), "both map to target table app.y") {
		t.Fatalf("reserve app.y while app.x joins as app.y: err %v", err)
	}
	// Cùng bảng nguồn (snapshot add và DDL tạo bảng cho cùng app.x) không phải trùng.
	releaseX2, err := p.reserveTargets([]model.TableID{x})
	if err != nil {
		t.Fatalf("reserve app.x twice: %v", err)
	}
	releaseX2()
	if _, err := p.reserveTargets([]model.TableID{y}); err == nil {
		t.Fatal("one release of app.x dropped the other holder's reservation")
	}
	releaseX()
	releaseX() // gọi lại an toàn
	releaseY, err := p.reserveTargets([]model.TableID{y})
	if err != nil {
		t.Fatalf("reserve app.y after app.x gave up: %v", err)
	}
	releaseY()

	// Bảng đã vào p.tables vẫn chặn như trước.
	p.tables[x] = &tableMeta{}
	if _, err := p.reserveTargets([]model.TableID{y}); err == nil {
		t.Fatal("reserve app.y while app.x is in the pipeline as app.y")
	}
}
