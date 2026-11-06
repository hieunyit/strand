package pipeline

import (
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// review R10: hai bảng nguồn rename về cùng một bảng đích phải bị từ chối khi dựng bộ lọc.
func TestR10_NewTableFilterRejectsDuplicateRenameTargets(t *testing.T) {
	_, err := newTableFilter([]string{"a.*"}, nil, map[string]string{"a.t1": "b.t", "a.t2": "b.t"})
	if err == nil || !strings.Contains(err.Error(), "b.t") {
		t.Fatalf("want duplicate rename target error, got %v", err)
	}
	if _, err := newTableFilter([]string{"a.*"}, nil, map[string]string{"a.t1": "b.t1", "a.t2": "b.t2"}); err != nil {
		t.Fatalf("distinct targets must be accepted: %v", err)
	}
}

// review R10: tên đích trùng giữa một bảng rename và một bảng nguồn không rename (a.t1→a.t2 trong khi
// a.t2 cũng được include) chỉ thấy được trên danh sách bảng cuối cùng.
func TestR10_TargetCollisionInFinalTableList(t *testing.T) {
	f, err := newTableFilter([]string{"a.*"}, nil, map[string]string{"a.t1": "a.t2"})
	if err != nil {
		t.Fatal(err)
	}
	t1 := model.TableID{Schema: "a", Table: "t1"}
	t2 := model.TableID{Schema: "a", Table: "t2"}
	t3 := model.TableID{Schema: "a", Table: "t3"}

	err = checkTargetCollisions(f, map[model.TableID]*tableMeta{t1: {}, t2: {}, t3: {}}, nil)
	var se *exitcode.Error
	if !errors.As(err, &se) || se.ExitCode != exitcode.InvalidSyntax || !strings.Contains(err.Error(), "a.t2") {
		t.Fatalf("want exit-2 collision error naming a.t2, got %v", err)
	}
	// Bảng mới tham gia (DDL CREATE) va chạm với bảng đã thuộc pipeline.
	if err := checkTargetCollisions(f, map[model.TableID]*tableMeta{t2: nil}, map[model.TableID]*tableMeta{t1: {}}); err == nil {
		t.Fatalf("joining table colliding with existing renamed table must be rejected")
	}
	// Không va chạm: a.t1→a.t2, a.t3 giữ tên; và bảng đã có trong existing không tự va chạm với chính nó.
	if err := checkTargetCollisions(f, map[model.TableID]*tableMeta{t1: {}, t3: {}}, map[model.TableID]*tableMeta{t1: {}}); err != nil {
		t.Fatalf("unexpected collision: %v", err)
	}
}
