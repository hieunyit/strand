package promote_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/promote"
)

// recDriver: driver database/sql giả, ghi lại mọi câu SQL; bảng _strand_promote chỉ "tồn tại" sau
// khi có CREATE TABLE. Không cần DB thật (review R31).
type recDriver struct {
	mu      sync.Mutex
	queries []string
	created bool
	missing error // lỗi trả về khi SELECT trên bảng chưa tồn tại
}

func (d *recDriver) Open(string) (driver.Conn, error) { return &recConn{d: d}, nil }

func (d *recDriver) record(q string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries = append(d.queries, q)
	if strings.Contains(strings.ToUpper(q), "CREATE TABLE") {
		d.created = true
	}
}

func (d *recDriver) writes() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, q := range d.queries {
		u := strings.ToUpper(strings.TrimSpace(q))
		for _, w := range []string{"CREATE", "INSERT", "REPLACE", "UPDATE", "DELETE", "ALTER", "DROP"} {
			if strings.HasPrefix(u, w) {
				out = append(out, q)
				break
			}
		}
	}
	return out
}

type recConn struct{ d *recDriver }

func (c *recConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}
func (c *recConn) Close() error              { return nil }
func (c *recConn) Begin() (driver.Tx, error) { return nil, errors.New("tx not supported") }

func (c *recConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.d.record(q)
	return driver.RowsAffected(0), nil
}

func (c *recConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	c.d.record(q)
	c.d.mu.Lock()
	created := c.d.created
	c.d.mu.Unlock()
	if strings.Contains(q, "_strand_promote") && !created {
		return nil, c.d.missing
	}
	return emptyRows{}, nil
}

type emptyRows struct{}

func (emptyRows) Columns() []string {
	return []string{"pipeline_id", "state", "current_step", "completed_steps", "sequences", "suggested_sequences", "updated_at"}
}
func (emptyRows) Close() error              { return nil }
func (emptyRows) Next([]driver.Value) error { return io.EOF }

var recDriverSeq atomic.Int64

func openRecDB(t *testing.T, missing error) (*sql.DB, *recDriver) {
	t.Helper()
	d := &recDriver{missing: missing}
	name := fmt.Sprintf("promote-rec-%d", recDriverSeq.Add(1))
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, d
}

// review R31: `promote --dry-run` từng gọi DBStateStore.LoadProgress → EnsureTable → CREATE TABLE
// _strand_promote trên target. Dry-run không được ghi gì; bảng chưa có = chưa có tiến trình.
func TestR31_PromoteDryRunDoesNotWriteTarget(t *testing.T) {
	cases := map[string]struct {
		missing error
		store   func(*sql.DB) promote.StateStore
	}{
		"mysql": {&mysql.MySQLError{Number: 1146, Message: "Table '_strand_promote' doesn't exist"},
			func(db *sql.DB) promote.StateStore { return promote.NewDBStateStore(db) }},
		"pg": {&pgconn.PgError{Code: "42P01", Message: `relation "_strand_promote" does not exist`},
			func(db *sql.DB) promote.StateStore { return promote.NewPGStateStore(db) }},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			db, d := openRecDB(t, c.missing)
			cfg := promote.Config{PipelineID: "p1", DryRun: true}
			progress, err := promote.NewPromoter(cfg, c.store(db), nil, nil, nil, nil).Run(context.Background())
			if err != nil {
				t.Fatalf("dry-run on a target without _strand_promote must succeed: %v", err)
			}
			if progress == nil || progress.PipelineID != "p1" || len(progress.CompletedSteps) != 0 {
				t.Fatalf("dry-run must plan from scratch, got %+v", progress)
			}
			if w := d.writes(); len(w) != 0 {
				t.Fatalf("dry-run wrote to the target: %q", w)
			}
		})
	}
}

// review R31: nạp chỉ-đọc vẫn báo lỗi thật (không phải "bảng không tồn tại"), còn LoadProgress
// thường vẫn tạo bảng như trước.
func TestR31_ReadOnlyLoadKeepsOtherErrorsAndNormalLoadCreatesTable(t *testing.T) {
	db, _ := openRecDB(t, &mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})
	if _, err := promote.NewDBStateStore(db).LoadProgressReadOnly(context.Background(), "p1"); err == nil {
		t.Fatal("permission error must not be treated as missing table")
	}

	db2, d2 := openRecDB(t, &mysql.MySQLError{Number: 1146})
	p, err := promote.NewDBStateStore(db2).LoadProgress(context.Background(), "p1")
	if err != nil || p != nil {
		t.Fatalf("LoadProgress on new table: p=%v err=%v", p, err)
	}
	if w := d2.writes(); len(w) != 1 || !strings.Contains(w[0], "CREATE TABLE") {
		t.Fatalf("non-dry-run LoadProgress must still ensure the table, writes=%q", w)
	}
}
