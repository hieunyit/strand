package writer_test

import (
	"context"
	"strings"
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

// Review P1: with ON UPDATE CASCADE + ON DELETE CASCADE, children still on the
// old key must follow it to the new key (as the source UPDATE did), not be
// deleted by removing the old row. SET NULL children get NULL; NO ACTION
// children make the writer stop without deleting anything.
func TestPGReferencedPKMoveReplayKeepsChildrenOfOldKey(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "pkmove-children")
	parent := pgTable(t, e.db, "mv_parent", "id bigint PRIMARY KEY, v text")
	pgExec(t, e.db, `CREATE TABLE public.mv_cascade (id bigint PRIMARY KEY, pid bigint REFERENCES public.mv_parent(id) ON UPDATE CASCADE ON DELETE CASCADE)`)
	pgExec(t, e.db, `CREATE TABLE public.mv_setnull (id bigint PRIMARY KEY, pid bigint REFERENCES public.mv_parent(id) ON UPDATE SET NULL ON DELETE CASCADE)`)
	pgExec(t, e.db, `INSERT INTO public.mv_parent VALUES (1, 'stale'), (2, 'later')`)
	pgExec(t, e.db, `INSERT INTO public.mv_cascade VALUES (10, 1), (20, 2)`)
	pgExec(t, e.db, `INSERT INTO public.mv_setnull VALUES (11, 1)`)
	w := e.writer(t, map[model.TableID][]string{parent: {"id"}})
	move := model.RowChange{Table: parent, Op: model.OpUpdate, OldKey: []model.Value{int64(1)}, Key: []model.Value{int64(2)},
		Cols: []model.ColValue{cv("id", int64(2)), cv("v", "moved")}}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{move}}}, e.delta("children")); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	var n int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM public.mv_parent WHERE id = 1`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("old key rows = %d err=%v, want 0", n, err)
	}
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM public.mv_cascade WHERE pid = 2`).Scan(&n); err != nil || n != 2 {
		t.Fatalf("ON UPDATE CASCADE children on the new key = %d err=%v, want 2 (child of the old key was deleted?)", n, err)
	}
	var pid *int64
	if err := e.db.QueryRow(`SELECT pid FROM public.mv_setnull WHERE id = 11`).Scan(&pid); err != nil || pid != nil {
		t.Fatalf("ON UPDATE SET NULL child: pid=%v err=%v, want NULL", pid, err)
	}
}

func TestPGReferencedPKMoveReplayRefusesNoActionChildren(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "pkmove-noaction")
	parent := pgTable(t, e.db, "na_parent", "id bigint PRIMARY KEY, v text")
	pgExec(t, e.db, `CREATE TABLE public.na_child (id bigint PRIMARY KEY, pid bigint REFERENCES public.na_parent(id) ON UPDATE NO ACTION ON DELETE CASCADE)`)
	pgExec(t, e.db, `INSERT INTO public.na_parent VALUES (1, 'stale'), (2, 'later')`)
	pgExec(t, e.db, `INSERT INTO public.na_child VALUES (10, 1)`)
	w := e.writer(t, map[model.TableID][]string{parent: {"id"}})
	move := model.RowChange{Table: parent, Op: model.OpUpdate, OldKey: []model.Value{int64(1)}, Key: []model.Value{int64(2)},
		Cols: []model.ColValue{cv("id", int64(2)), cv("v", "moved")}}
	err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{move}}}, e.delta("noaction"))
	if err == nil || !strings.Contains(err.Error(), "still references the old key") {
		t.Fatalf("ApplyBatch error = %v, want a refusal naming the referencing table", err)
	}
	var n int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM public.na_child WHERE id = 10 AND pid = 1`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("child changed after refusal: %d err=%v", n, err)
	}
}

func TestPGReferencedCaseInsensitiveKeyUpdate(t *testing.T) {
	e := newPGEnv(t, "casefold-pkmove")
	pgExec(t, e.db, "CREATE EXTENSION IF NOT EXISTS citext")
	parent := pgTable(t, e.db, "casefold_parent", "id citext PRIMARY KEY")
	pgExec(t, e.db, "CREATE TABLE public.casefold_child (id bigint PRIMARY KEY, pid citext REFERENCES public.casefold_parent(id) ON UPDATE CASCADE ON DELETE CASCADE)")
	pgExec(t, e.db, "INSERT INTO public.casefold_parent VALUES ('alice')")
	pgExec(t, e.db, "INSERT INTO public.casefold_child VALUES (1, 'alice')")
	w := e.writer(t, map[model.TableID][]string{parent: {"id"}})
	move := model.RowChange{Table: parent, Op: model.OpUpdate, OldKey: []model.Value{"alice"}, Key: []model.Value{"ALICE"}, Cols: []model.ColValue{cv("id", "ALICE")}}
	if err := w.ApplyBatch(context.Background(), []*model.Txn{{Rows: []model.RowChange{move}}}, e.delta("casefold")); err != nil {
		t.Fatal(err)
	}
	var parents, children int
	var key string
	if err := e.db.QueryRow("SELECT (SELECT count(*) FROM public.casefold_parent), (SELECT count(*) FROM public.casefold_child), COALESCE((SELECT id::text FROM public.casefold_parent), '')").Scan(&parents, &children, &key); err != nil || parents != 1 || children != 1 || key != "ALICE" {
		t.Fatalf("parents=%d children=%d key=%q err=%v; expected both retained with updated parent key", parents, children, key, err)
	}
}

// A key change that keeps a TOAST value goes through the partial-update path.
// Under citext the new key equals the old one, so the row must be updated in
// place; deleting the old key would delete the moved row.
func TestPGCaseInsensitivePartialKeyUpdate(t *testing.T) {
	e := newPGEnv(t, "casefold-partial")
	pgExec(t, e.db, "CREATE EXTENSION IF NOT EXISTS citext")
	tbl := pgTable(t, e.db, "casefold_toast", "id citext PRIMARY KEY, n int, doc text")
	pgExec(t, e.db, "INSERT INTO public.casefold_toast VALUES ('alice', 1, repeat('x', 10000))")
	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
	move := model.RowChange{Table: tbl, Op: model.OpUpdate, OldKey: []model.Value{"alice"}, Key: []model.Value{"ALICE"},
		Cols: []model.ColValue{cv("id", "ALICE"), cv("n", int64(2)), unchanged("doc")}}
	if err := w.ApplyBatch(context.Background(), []*model.Txn{{Rows: []model.RowChange{move}}}, e.delta("casefold")); err != nil {
		t.Fatal(err)
	}
	var rows, n, docLen int
	var key string
	if err := e.db.QueryRow("SELECT count(*), COALESCE(max(id::text), ''), COALESCE(max(n), 0), COALESCE(max(length(doc)), 0) FROM public.casefold_toast").Scan(&rows, &key, &n, &docLen); err != nil || rows != 1 || key != "ALICE" || n != 2 || docLen != 10000 {
		t.Fatalf("rows=%d key=%q n=%d doc=%d err=%v; want the row kept with the new key and its TOAST value", rows, key, n, docLen, err)
	}
}
