package writer_test

import (
	"context"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/writer"
)

func TestNK05_PostgresKeylessPartitionPhysicalIdentity(t *testing.T) {
	e := newPGEnv(t, "keyless-partition")
	ctx := context.Background()
	for _, q := range []string{
		`CREATE TABLE public.nk_partition (k int, v text) PARTITION BY RANGE(k)`,
		`CREATE TABLE public.nk_partition_a PARTITION OF public.nk_partition FOR VALUES FROM (0) TO (10)`,
		`CREATE TABLE public.nk_partition_b PARTITION OF public.nk_partition FOR VALUES FROM (10) TO (20)`,
		`CREATE INDEX ON public.nk_partition (k)`,
		`INSERT INTO public.nk_partition VALUES (1, 'one'), (11, 'other')`,
	} {
		pgExec(t, e.db, q)
	}
	var sameCTID bool
	if err := e.db.QueryRow(`SELECT a.ctid=b.ctid FROM public.nk_partition_a a CROSS JOIN public.nk_partition_b b`).Scan(&sameCTID); err != nil {
		t.Fatal(err)
	}
	if !sameCTID {
		t.Fatal("fixture must exercise ctid collision across partitions")
	}
	table := model.TableID{Schema: "public", Table: "nk_partition"}
	w := e.writer(t, map[model.TableID][]string{table: {"k", "v"}})
	w.SetKeyless(table, true)
	change := model.RowChange{Table: table, Op: model.OpUpdate, OldKey: []model.Value{int64(1), "one"}, Key: []model.Value{int64(1), "updated"}, Cols: []model.ColValue{{Name: "k", Value: int64(1)}, {Name: "v", Value: "updated"}}}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{change}}}, e.delta("partition-update")); err != nil {
		t.Fatal(err)
	}
	var other string
	if err := e.db.QueryRow(`SELECT v FROM public.nk_partition WHERE k=11`).Scan(&other); err != nil || other != "other" {
		t.Fatalf("other partition modified: %q %v", other, err)
	}
	change = model.RowChange{Table: table, Op: model.OpDelete, Key: []model.Value{int64(1), "updated"}}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{change}}}, e.delta("partition-delete")); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.db.QueryRow(`SELECT count(*) FROM public.nk_partition`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("delete affected another partition: count=%d err=%v", n, err)
	}

	pgExec(t, e.db, `INSERT INTO public.nk_partition VALUES (7, NULL), (7, NULL), (7, 'decoy')`)
	deleteNull := model.RowChange{Table: table, Op: model.OpDelete, Key: []model.Value{int64(7), nil}}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{deleteNull}}}, e.delta("delete-one-duplicate")); err != nil {
		t.Fatal(err)
	}
	checkDuplicates := func() {
		t.Helper()
		var nulls, decoys int
		err := e.db.QueryRow(`SELECT count(*) FILTER (WHERE v IS NULL), count(*) FILTER (WHERE v = 'decoy')
			FROM public.nk_partition WHERE k = 7`).Scan(&nulls, &decoys)
		if err != nil || nulls != 1 || decoys != 1 {
			t.Fatalf("full-image match changed the wrong multiplicities: null=%d decoy=%d err=%v", nulls, decoys, err)
		}
	}
	checkDuplicates()

	applied, batch := e.workerRow(t)
	missing := model.RowChange{Table: table, Op: model.OpDelete, Key: []model.Value{int64(7), "missing"}}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{deleteNull, missing}}}, e.delta("must-roll-back")); exitcode.GetExitCode(err) != exitcode.DataError {
		t.Fatalf("expected missing before-image to fail closed: %v", err)
	}
	checkDuplicates()
	if after, afterBatch := e.workerRow(t); after != applied || afterBatch != batch {
		t.Fatalf("failed batch advanced checkpoint: (%q,%d) -> (%q,%d)", applied, batch, after, afterBatch)
	}

	// Even an unchanged PG UPDATE needs a matching row at the target. Here
	// the first change temporarily removes that row within the same batch.
	unchanged := model.RowChange{Table: table, Op: model.OpUpdate, Key: deleteNull.Key}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{deleteNull, unchanged}}}, e.delta("must-roll-back-unchanged")); exitcode.GetExitCode(err) != exitcode.DataError {
		t.Fatalf("expected unchanged update of absent row to fail closed: %v", err)
	}
	checkDuplicates()
	if after, afterBatch := e.workerRow(t); after != applied || afterBatch != batch {
		t.Fatalf("unchanged update advanced checkpoint: (%q,%d) -> (%q,%d)", applied, batch, after, afterBatch)
	}

	toast := model.TableID{Schema: "public", Table: "nk_toast_only"}
	payload := strings.Repeat("toast", 3000)
	pgExec(t, e.db, `CREATE TABLE public.nk_toast_only(v text)`)
	pgExec(t, e.db, `ALTER TABLE public.nk_toast_only ALTER COLUMN v SET STORAGE EXTERNAL`)
	if _, err := e.db.Exec(`INSERT INTO public.nk_toast_only(v) VALUES ($1)`, payload); err != nil {
		t.Fatal(err)
	}
	w.SetPrimaryKey(toast, []string{"v"})
	w.SetKeyless(toast, true)
	unchangedToast := model.RowChange{Table: toast, Op: model.OpUpdate, OldKey: []model.Value{payload},
		Key: []model.Value{nil}, Cols: []model.ColValue{{Name: "v", Unchanged: true}}}
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: []model.RowChange{unchangedToast}}}, e.delta("all-toast-unchanged")); err != nil {
		t.Fatalf("update whose only column is unchanged TOAST: %v", err)
	}
	if err := e.db.QueryRow(`SELECT v FROM public.nk_toast_only`).Scan(&other); err != nil || other != payload {
		t.Fatalf("unchanged TOAST value differs: %v", err)
	}
}

func TestS03_PostgresStaleWorkerCreationAfterTakeover(t *testing.T) {
	e := newPGEnv(t, "stale-create")
	old := writer.NewPGWriter(e.db, e.pid, 999, e.epoch)
	if _, err := e.store.AdvanceEpoch(context.Background(), e.pid, "new-owner", "hash"); err != nil {
		t.Fatal(err)
	}
	if err := old.EnsureWorkerRow(context.Background()); exitcode.GetExitCode(err) != exitcode.Fenced {
		t.Fatalf("stale worker creation must be fenced: %v", err)
	}
	var n int
	if err := e.db.QueryRow(`SELECT count(*) FROM _strand_worker_delta WHERE pipeline_id=$1 AND worker_id=999`, e.pid).Scan(&n); err != nil || n != 0 {
		t.Fatalf("stale worker row persisted: %d %v", n, err)
	}
}
