package writer_test

import (
	"context"
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
