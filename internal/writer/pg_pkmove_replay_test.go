package writer_test

import (
	"context"
	"testing"

	"strand/internal/model"
)

// Replaying an already-applied primary-key move on a referenced parent: the
// target already holds the new key (copied by a later snapshot or catch-up)
// next to the old one. The writer must keep the new row and remove the old key
// instead of failing on a duplicate key on every retry.
func TestPGReferencedPKMoveReplayWhenNewKeyExists(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "pkmove-replay")
	parent := pgTable(t, e.db, "move_parent", "id bigint PRIMARY KEY, v text")
	pgExec(t, e.db, `CREATE TABLE public.move_child (id bigint PRIMARY KEY, pid bigint REFERENCES public.move_parent(id) ON UPDATE CASCADE ON DELETE CASCADE)`)
	pgExec(t, e.db, `INSERT INTO public.move_parent VALUES (1, 'stale'), (2, 'later')`)
	pgExec(t, e.db, `INSERT INTO public.move_child VALUES (20, 2)`)
	w := e.writer(t, map[model.TableID][]string{parent: {"id"}})
	move := model.RowChange{Table: parent, Op: model.OpUpdate, OldKey: []model.Value{int64(1)}, Key: []model.Value{int64(2)},
		Cols: []model.ColValue{cv("id", int64(2)), cv("v", "moved")}}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{move}}}, e.delta("replay")); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	var n int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM public.move_parent WHERE id = 1`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("old key rows = %d err=%v, want 0", n, err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM public.move_child WHERE pid = 2`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("child of the kept new row = %d err=%v, want 1", n, err)
	}
	if applied, _ := e.workerRow(t); applied != "replay" {
		t.Fatalf("applied = %q", applied)
	}
}
