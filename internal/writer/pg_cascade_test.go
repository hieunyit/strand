package writer_test

import (
	"context"
	"testing"

	"strand/internal/model"
	"strand/internal/writer"
)

// Regression (review việc B, 2026-11-04): bảng đích PG có FK ON DELETE CASCADE (schema-init tạo FK
// giữ nguyên ON DELETE của source). Flush snapshot của bảng CHA (range hoặc tập khóa re-read) từng
// DELETE cả range rồi upsert lại: PG cascade xoá luôn row CON của các khóa vẫn còn ở source → mất dữ
// liệu âm thầm khi bảng cha được snapshot lại trong lúc bảng con đã có dữ liệu ở target.
// Khóa thật sự vắng trong snapshot (đã xoá ở source) vẫn phải bị xoá (và cascade như source).
func TestPGReplace_DoesNotCascadeDeleteChildrenOfSurvivingKeys(t *testing.T) {
	e := newPGEnv(t, "pg-replace-cascade")
	parent := pgTable(t, e.db, "casc_parent", "id bigint PRIMARY KEY, v text")
	pgExec(t, e.db, `CREATE TABLE public.casc_child (id bigint PRIMARY KEY, pid bigint REFERENCES public.casc_parent(id) ON DELETE CASCADE)`)
	pgExec(t, e.db, `INSERT INTO public.casc_parent SELECT g, 'old' FROM generate_series(1, 6) g`)
	pgExec(t, e.db, `INSERT INTO public.casc_child SELECT g, g FROM generate_series(1, 6) g`)
	row := func(id int64, v string) *model.RowChange {
		return &model.RowChange{Table: parent, Op: model.OpInsert, Key: []model.Value{id}, Cols: []model.ColValue{cv("id", id), cv("v", v)}}
	}
	ctx := context.Background()
	// Range [1,3]: 1 và 3 còn ở source (giá trị mới), 2 đã bị xoá ở source.
	if err := writer.PGReplaceRange(ctx, e.db, parent, "id", []string{"id", "v"}, []*model.RowChange{row(1, "n1"), row(3, "n3")}, 1, 3); err != nil {
		t.Fatal(err)
	}
	// Tập khóa {4,5}: 4 còn (giá trị mới), 5 đã bị xoá.
	if err := writer.PGReplaceKeys(ctx, e.db, parent, "id", []string{"id", "v"}, []*model.RowChange{row(4, "n4")}, []int64{4, 5}); err != nil {
		t.Fatal(err)
	}
	var children []int64
	rs, err := e.db.Query(`SELECT id FROM public.casc_child ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	for rs.Next() {
		var id int64
		_ = rs.Scan(&id)
		children = append(children, id)
	}
	want := []int64{1, 3, 4, 6}
	if len(children) != len(want) {
		t.Fatalf("children on target = %v, want %v (children of surviving parents must not be cascade-deleted)", children, want)
	}
	for i := range want {
		if children[i] != want[i] {
			t.Fatalf("children on target = %v, want %v", children, want)
		}
	}
	var v1, v4 string
	_ = e.db.QueryRow(`SELECT v FROM public.casc_parent WHERE id = 1`).Scan(&v1)
	_ = e.db.QueryRow(`SELECT v FROM public.casc_parent WHERE id = 4`).Scan(&v4)
	if v1 != "n1" || v4 != "n4" {
		t.Fatalf("parent values not replaced: 1=%q 4=%q", v1, v4)
	}
}
