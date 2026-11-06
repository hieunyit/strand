package verify_test

// Kiểm lại row lệch ở đường so chuỗi chính tắc (PG→PG, MySQL→PG; Gate 2 04 §5): row bị UPDATE liên tục
// ở source (không có pipeline, target không đổi) phải được báo unverifiable_hot, không phải lệch; row
// lệch thật (ổn định) vẫn là lệch. Trước khi có kiểm lại, full verify của hai cặp này so row thẳng nên
// row nóng luôn bị báo lệch giả.

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"strand/internal/model"
	"strand/internal/verify"
)

// hammer UPDATE row id liên tục ở source (workers goroutine) cho tới khi gọi hàm trả về.
func hammer(t *testing.T, db *sql.DB, stmt string, workers int) func() int64 {
	t.Helper()
	var stop atomic.Bool
	var n atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if _, err := db.Exec(stmt); err == nil {
					n.Add(1)
				}
			}
		}()
	}
	return func() int64 { stop.Store(true); wg.Wait(); return n.Load() }
}

// runHot chạy full verify tối đa 3 lượt: lệch đúng một row (planted), row nóng không bao giờ lệch, và
// được báo unverifiable_hot ở ít nhất một lượt.
func runHot(t *testing.T, mv *verify.MerkleVerifier, tbl model.TableID, planted string) {
	t.Helper()
	for attempt := 1; attempt <= 3; attempt++ {
		rep, err := mv.VerifyTableFull(context.Background(), tbl, "id")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rep.Mismatches, []string{planted}) {
			t.Fatalf("attempt %d: mismatches = %v, want [%s] (hot row 1 must not be a mismatch); report %+v", attempt, rep.Mismatches, planted, *rep)
		}
		if len(rep.UnverifiableHot) > 1 || (len(rep.UnverifiableHot) == 1 && rep.UnverifiableHot[0] != "1") {
			t.Fatalf("attempt %d: unverifiable_hot = %v, want [1]", attempt, rep.UnverifiableHot)
		}
		if len(rep.UnverifiableHot) == 1 {
			return
		}
	}
	t.Fatal("hot row 1 was never reported unverifiable_hot in 3 runs")
}

func TestRecheckDB_PGToPGHotRowIsUnverifiableNotMismatch(t *testing.T) {
	pg := getPostgres(t)
	mustExec(t, pg, "CREATE SCHEMA IF NOT EXISTS h1; CREATE SCHEMA IF NOT EXISTS h2")
	for _, s := range []string{"h1", "h2"} {
		mustExec(t, pg, fmt.Sprintf("DROP TABLE IF EXISTS %s.hot", s))
		mustExec(t, pg, fmt.Sprintf("CREATE TABLE %s.hot (id bigint PRIMARY KEY, n bigint NOT NULL, note text)", s))
		mustExec(t, pg, fmt.Sprintf("INSERT INTO %s.hot SELECT g, g, 'r' || g FROM generate_series(1, 2000) g", s))
	}
	mustExec(t, pg, "UPDATE h2.hot SET note = 'PLANTED' WHERE id = 777")
	stop := hammer(t, pg, "UPDATE h1.hot SET n = n + 1 WHERE id = 1", 4)
	defer stop()
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: pg, Target: pg, PipelineID: "pipe_hot_pp",
		SourceEngine: model.EnginePG, TargetEngine: model.EnginePG, ChunkRows: 500,
		TargetName: func(t model.TableID) model.TableID { return model.TableID{Schema: "h2", Table: t.Table} }})
	runHot(t, mv, model.TableID{Schema: "h1", Table: "hot"}, "777")
	t.Logf("%d updates of the hot row", stop())
}

func TestRecheckDB_MySQLToPGHotRowIsUnverifiableNotMismatch(t *testing.T) {
	src, _ := getMySQL(t)
	pg := getPostgres(t)
	mustExec(t, src, "DROP TABLE IF EXISTS verify_src.hot_mp")
	mustExec(t, src, "CREATE TABLE verify_src.hot_mp (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, note VARCHAR(32))")
	mustExec(t, pg, "DROP TABLE IF EXISTS public.hot_mp")
	mustExec(t, pg, "CREATE TABLE public.hot_mp (id bigint PRIMARY KEY, n bigint NOT NULL, note varchar(32))")
	for i := 1; i <= 2000; i++ {
		mustExec(t, src, "INSERT INTO verify_src.hot_mp VALUES (?, ?, ?)", i, i, fmt.Sprintf("r%d", i))
	}
	mustExec(t, pg, "INSERT INTO public.hot_mp SELECT g, g, 'r' || g FROM generate_series(1, 2000) g")
	mustExec(t, pg, "UPDATE public.hot_mp SET note = 'PLANTED' WHERE id = 777")
	stop := hammer(t, src, "UPDATE verify_src.hot_mp SET n = n + 1 WHERE id = 1", 4)
	defer stop()
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: src, Target: pg, PipelineID: "pipe_hot_mp",
		SourceEngine: model.EngineMySQL, TargetEngine: model.EnginePG, ChunkRows: 500,
		TargetName: func(model.TableID) model.TableID { return model.TableID{Schema: "public", Table: "hot_mp"} }})
	runHot(t, mv, model.TableID{Schema: "verify_src", Table: "hot_mp"}, "777")
	t.Logf("%d updates of the hot row", stop())
}
