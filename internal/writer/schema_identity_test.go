package writer_test

import (
	"context"
	"strand/internal/model"
	"strand/internal/writer"
	"testing"
)

// invariant: CP-1, CP-2 — ID từ source phải được giữ nguyên, không gọi nextval thay ID.
func TestCP1_PGAlwaysIdentityExplicitSourceValues(t *testing.T) {
	for _, copyMin := range []int{0, 100} {
		name := "identity_merge"
		if copyMin == 100 {
			name = "identity_insert"
		}
		t.Run(name, func(t *testing.T) {
			e := newPGEnv(t, name)
			tbl := pgTable(t, e.db, name, "id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text")
			w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
			w.SetCopyMinRows(copyMin)
			rows := []model.RowChange{ins(tbl, []model.Value{int64(101)}, cv("id", int64(101)), cv("v", "source"))}
			if err := w.ApplyBatch(context.Background(), []*model.Txn{{Rows: rows}}, e.delta("identity")); err != nil {
				t.Fatal(err)
			}
			var id int64
			if err := e.db.QueryRow("SELECT id FROM public." + name).Scan(&id); err != nil || id != 101 {
				t.Fatalf("id=%d err=%v", id, err)
			}
			if err := writer.PGReplaceRange(context.Background(), e.db, tbl, "id", []string{"id", "v"}, []*model.RowChange{&rows[0]}, 101, 101); err != nil {
				t.Fatal(err)
			}
		})
	}
}
