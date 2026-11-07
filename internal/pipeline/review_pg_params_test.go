package pipeline

import (
	"context"
	"fmt"
	"strand/internal/model"
	"strand/internal/snapshot"
	"strings"
	"testing"
)

// RS-1/SG-1: snapshot PK rộng giữ row sống và con FK; không vượt giới hạn bind, kể cả re-read tập khóa.
func TestReviewPGWideKeySnapshotParameterLimit(t *testing.T) {
	db, _ := startTestDB(t, "pg")
	ctx := context.Background()
	id := model.TableID{Schema: "public", Table: "wide"}
	var defs, pk, cols []string
	codec := &snapshot.PKCodec{}
	for j := 0; j < 14; j++ {
		n := fmt.Sprintf("k%d", j)
		defs = append(defs, n+" integer NOT NULL")
		pk = append(pk, n)
		cols = append(cols, n)
		codec.Cols = append(codec.Cols, snapshot.PKColumn{Name: n, Kind: snapshot.PKInt, SQLType: "integer"})
	}
	defs = append(defs, "v integer", "PRIMARY KEY ("+strings.Join(pk, ",")+")", "UNIQUE(k0)")
	cols = append(cols, "v")
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE public.wide (" + strings.Join(defs, ",") + ")")
	exec("CREATE TABLE public.child(id int PRIMARY KEY REFERENCES public.wide(k0) ON DELETE CASCADE)")
	vals := func(i int) []model.Value {
		v := make([]model.Value, 14)
		for j := range v {
			v[j] = int64(0)
		}
		v[0] = int64(i)
		return v
	}
	f, _ := newTableFilter([]string{"public.wide"}, nil, nil)
	p := &Pipeline{dst: db, filter: f, tables: map[model.TableID]*tableMeta{id: {PKCodec: codec, TargetPKCodec: codec}}}
	var images []*model.RowChange
	var keys []snapshot.Bound
	for i := 1; i <= 5000; i++ {
		k := vals(i)
		r := &model.RowChange{Table: id, Key: k, Op: model.OpInsert}
		for j, n := range pk {
			r.Cols = append(r.Cols, model.ColValue{Name: n, Value: k[j]})
		}
		r.Cols = append(r.Cols, model.ColValue{Name: "v", Value: int64(i)})
		images = append(images, r)
		b, err := codec.BoundOf(k)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, b)
	}
	lo, _ := codec.BoundOf(vals(0))
	hi, _ := codec.BoundOf(vals(6000))
	for _, mode := range []string{"range", "keys"} {
		t.Run(mode, func(t *testing.T) {
			exec("TRUNCATE public.child, public.wide")
			zeros := strings.Repeat(",0", 13)
			exec("INSERT INTO public.wide SELECT g" + zeros + ",-1 FROM generate_series(1,5001) g")
			exec("INSERT INTO public.child VALUES (1),(5001)")
			var err error
			if mode == "range" {
				err = p.pgFlushRange(ctx, id, cols, images, lo, hi)
			} else {
				extra, _ := codec.BoundOf(vals(5001))
				err = p.pgFlushKeySet(ctx, id, cols, images, append(append([]snapshot.Bound{}, keys...), extra))
			}
			if err != nil {
				t.Fatal(err)
			}
			var n, bad, children int
			if err := db.QueryRow("SELECT count(*),count(*) FILTER(WHERE v<>k0) FROM public.wide").Scan(&n, &bad); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT count(*) FROM public.child WHERE id=1").Scan(&children); err != nil {
				t.Fatal(err)
			}
			if n != 5000 || bad != 0 || children != 1 {
				t.Fatalf("rows=%d bad=%d surviving children=%d", n, bad, children)
			}
		})
	}
	t.Run("rollback_and_retry", func(t *testing.T) {
		exec("ALTER TABLE public.wide ADD CONSTRAINT positive_v CHECK (v>=0)")
		exec("INSERT INTO public.wide SELECT 5001" + strings.Repeat(",0", 13) + ",5001")
		exec("INSERT INTO public.child VALUES (5001)")
		bad := append([]*model.RowChange{}, images...)
		last := *images[len(images)-1]
		last.Cols = append([]model.ColValue{}, last.Cols...)
		last.Cols[len(last.Cols)-1].Value = int64(-1)
		bad[len(bad)-1] = &last
		if err := p.pgFlushRange(ctx, id, cols, bad, lo, hi); err == nil {
			t.Fatal("expected CHECK failure")
		}
		var n int
		if err := db.QueryRow("SELECT count(*) FROM public.child WHERE id=5001").Scan(&n); err != nil || n != 1 {
			t.Fatalf("failed snapshot committed its cascade: n=%d err=%v", n, err)
		}
		// Rollback phải bỏ bảng tạm; lần flush kế tiếp không được lỗi relation already exists.
		if err := p.pgFlushRange(ctx, id, cols, images, lo, hi); err != nil {
			t.Fatal(err)
		}
	})

}
