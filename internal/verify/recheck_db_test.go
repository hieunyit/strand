package verify_test

// Kiểm lại row lệch ở đường so chuỗi chính tắc (PG→PG, MySQL→PG; Gate 2 04 §5): row bị UPDATE liên tục
// ở source (không có pipeline, target không đổi) phải được báo unverifiable_hot, không phải lệch; row
// lệch thật (ổn định) vẫn là lệch. Trước khi có kiểm lại, full verify của hai cặp này so row thẳng nên
// row nóng luôn bị báo lệch giả.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"strand/internal/model"
	"strand/internal/verify"
)

// hotConnector bọc driver thật: TRƯỚC mỗi SELECT đọc bảng nóng (sel khớp), chạy một UPDATE row nóng
// bằng pool khác (bump) rồi mới cho SELECT chạy. Nhờ vậy mọi cặp đọc S1/S2 của verify đều thấy row đổi,
// bất kể máy chậm hay nhanh. (Trước đây dùng vòng UPDATE tự do: trên máy tải nặng commit MySQL chỉ
// ~12 lần/giây, cửa sổ S1..S2 cỡ ms nên ~1/8 lượt không có commit nào lọt vào, row ổn định S1==S2
// mà target khác thì đúng là "lệch" — verify không sai, test thiếu tải nóng thật.)
type hotConnector struct {
	drv  driver.Driver
	dsn  string
	sel  func(query string) bool
	bump func()
}

func (c *hotConnector) Connect(context.Context) (driver.Conn, error) {
	in, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &hotConn{Conn: in, c: c}, nil
}
func (c *hotConnector) Driver() driver.Driver { return c.drv }

type hotConn struct {
	driver.Conn
	c *hotConnector
}

func (h *hotConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if h.c.sel(q) {
		h.c.bump()
	}
	if qc, ok := h.Conn.(driver.QueryerContext); ok {
		return qc.QueryContext(ctx, q, args)
	}
	return nil, driver.ErrSkip
}
func (h *hotConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if ec, ok := h.Conn.(driver.ExecerContext); ok {
		return ec.ExecContext(ctx, q, args)
	}
	return nil, driver.ErrSkip
}
func (h *hotConn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	if h.c.sel(q) { // driver không có QueryerContext thì database/sql đi đường Prepare
		h.c.bump()
	}
	if pc, ok := h.Conn.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, q)
	}
	return h.Conn.Prepare(q)
}
func (h *hotConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	if bt, ok := h.Conn.(driver.ConnBeginTx); ok {
		return bt.BeginTx(ctx, o)
	}
	return h.Conn.Begin()
}
func (h *hotConn) Ping(ctx context.Context) error {
	if p, ok := h.Conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}
func (h *hotConn) ResetSession(ctx context.Context) error {
	if r, ok := h.Conn.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}
func (h *hotConn) IsValid() bool {
	if v, ok := h.Conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}
func (h *hotConn) CheckNamedValue(nv *driver.NamedValue) error {
	if ck, ok := h.Conn.(driver.NamedValueChecker); ok {
		return ck.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

// hotSource mở pool source mà mọi SELECT chứa table đều đi sau một lần UPDATE stmt (bằng plain).
// Trả pool và số lần bump (để test khẳng định tải nóng thật sự chạy).
func hotSource(t *testing.T, plain *sql.DB, dsn, table, stmt string) (*sql.DB, *atomic.Int64) {
	t.Helper()
	var n atomic.Int64
	db := sql.OpenDB(&hotConnector{drv: plain.Driver(), dsn: dsn,
		sel: func(q string) bool {
			return strings.Contains(q, table) && strings.HasPrefix(strings.TrimSpace(q), "SELECT")
		},
		bump: func() {
			if _, err := plain.Exec(stmt); err != nil {
				t.Errorf("hot bump: %v", err)
				return
			}
			n.Add(1)
		}})
	t.Cleanup(func() { _ = db.Close() })
	return db, &n
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
	hotSrc, bumps := hotSource(t, pg, pgDSN, "hot", "UPDATE h1.hot SET n = n + 1 WHERE id = 1")
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: hotSrc, Target: pg, PipelineID: "pipe_hot_pp",
		SourceEngine: model.EnginePG, TargetEngine: model.EnginePG, ChunkRows: 500,
		TargetName: func(t model.TableID) model.TableID { return model.TableID{Schema: "h2", Table: t.Table} }})
	runHot(t, mv, model.TableID{Schema: "h1", Table: "hot"}, "777")
	t.Logf("%d updates of the hot row", bumps.Load())
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
	hotSrc, bumps := hotSource(t, src, mysqlSrcDSN, "hot_mp", "UPDATE verify_src.hot_mp SET n = n + 1 WHERE id = 1")
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: hotSrc, Target: pg, PipelineID: "pipe_hot_mp",
		SourceEngine: model.EngineMySQL, TargetEngine: model.EnginePG, ChunkRows: 500,
		TargetName: func(model.TableID) model.TableID { return model.TableID{Schema: "public", Table: "hot_mp"} }})
	runHot(t, mv, model.TableID{Schema: "verify_src", Table: "hot_mp"}, "777")
	t.Logf("%d updates of the hot row", bumps.Load())
}

// Đối chứng: row đã lệch mà KHÔNG đổi trong cửa sổ S1..S2 (không tải nóng) phải là lệch thật — chứng minh
// kết quả "hot" ở trên đến từ việc row đổi, không phải verify bỏ qua mọi row lệch.
func TestRecheckDB_MySQLToPGStableDiffIsMismatch(t *testing.T) {
	src, _ := getMySQL(t)
	pg := getPostgres(t)
	mustExec(t, src, "DROP TABLE IF EXISTS verify_src.cold_mp")
	mustExec(t, src, "CREATE TABLE verify_src.cold_mp (id BIGINT PRIMARY KEY, n BIGINT NOT NULL)")
	mustExec(t, pg, "DROP TABLE IF EXISTS public.cold_mp")
	mustExec(t, pg, "CREATE TABLE public.cold_mp (id bigint PRIMARY KEY, n bigint NOT NULL)")
	mustExec(t, src, "INSERT INTO verify_src.cold_mp VALUES (1, 1), (2, 2)")
	mustExec(t, pg, "INSERT INTO public.cold_mp VALUES (1, 1), (2, 99)")
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: src, Target: pg, PipelineID: "pipe_cold_mp",
		SourceEngine: model.EngineMySQL, TargetEngine: model.EnginePG, ChunkRows: 500,
		TargetName: func(model.TableID) model.TableID { return model.TableID{Schema: "public", Table: "cold_mp"} }})
	rep, err := mv.VerifyTableFull(context.Background(), model.TableID{Schema: "verify_src", Table: "cold_mp"}, "id")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rep.Mismatches, []string{"2"}) || len(rep.UnverifiableHot) != 0 {
		t.Fatalf("mismatches = %v hot = %v, want [2] and none", rep.Mismatches, rep.UnverifiableHot)
	}
}
