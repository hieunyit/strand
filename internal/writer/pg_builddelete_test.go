package writer

import (
	"testing"

	"strand/internal/model"
)

// Review 1.5: buildDelete với danh sách khóa rỗng từng panic (keys[len(keys)-1] với len = 0).
func TestBuildDeleteRejectsEmptyOrShortKeys(t *testing.T) {
	tbl := model.TableID{Schema: "public", Table: "t"}
	call := func(pk []string, keys [][]model.Value) (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("buildDelete panicked: %v", r)
			}
		}()
		_, _, err = buildDelete(tbl, pk, keys)
		return err
	}
	if err := call([]string{"id"}, nil); err == nil {
		t.Fatalf("expected error for empty key list")
	}
	if err := call([]string{"a", "b"}, [][]model.Value{{int64(1)}}); err == nil {
		t.Fatalf("expected error for key shorter than primary key")
	}
	q, args, err := buildDelete(tbl, []string{"id"}, [][]model.Value{{int64(1)}, {int64(2)}, {int64(3)}})
	if err != nil || len(args) != 4 || q == "" {
		t.Fatalf("normal delete: q=%q args=%v err=%v", q, args, err)
	}
}
