package promote_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strand/internal/promote"
	"strings"
	"testing"
)

// PR-1: reload bất đồng bộ phải được quan sát; phiên quản trị vẫn ghi được và không rò SET qua pool.
func TestReviewPGFenceReloadAndWritableAdmin(t *testing.T) {
	state := &fenceState{}
	db := sql.OpenDB(fenceConnector{state})
	defer db.Close()
	if err := promote.NewPGSourceDriver(db, "app_role").Fence(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !state.observed {
		t.Fatal("Fence returned before confirming reload")
	}
	if !state.closed {
		t.Fatal("writable admin session returned to pool")
	}
}

type fenceState struct{ writable, reload, observed, closed bool }
type fenceConnector struct{ *fenceState }

func (c fenceConnector) Connect(context.Context) (driver.Conn, error) {
	return &fenceConn{c.fenceState}, nil
}
func (c fenceConnector) Driver() driver.Driver { return fenceDriver{} }

type fenceDriver struct{}

func (fenceDriver) Open(string) (driver.Conn, error) { return nil, fmt.Errorf("use connector") }

type fenceConn struct{ *fenceState }

func (c *fenceConn) Prepare(string) (driver.Stmt, error) { return nil, fmt.Errorf("prepare") }
func (c *fenceConn) Close() error                        { c.closed = true; return nil }
func (c *fenceConn) Begin() (driver.Tx, error)           { return nil, fmt.Errorf("begin") }
func (c *fenceConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(q, "SET default_transaction_read_only = off") {
		c.writable = true
	}
	if strings.Contains(q, "pg_reload_conf") {
		c.reload = true
	}
	if strings.HasPrefix(q, "ALTER ROLE") && c.reload && !c.writable {
		return nil, fmt.Errorf("25006: cannot execute ALTER ROLE in a read-only transaction")
	}
	return driver.RowsAffected(1), nil
}
func (c *fenceConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(q, "pg_conf_load_time() >"):
		c.observed = c.reload
		return &fenceRows{v: c.observed, has: true}, nil
	case strings.Contains(q, "pg_conf_load_time()"):
		return &fenceRows{v: "2026-11-07 00:00:00+00", has: true}, nil
	case strings.Contains(q, "pg_stat_activity"):
		return &fenceRows{}, nil
	default:
		return nil, fmt.Errorf("query: %s", q)
	}
}

type fenceRows struct {
	v   driver.Value
	has bool
}

func (*fenceRows) Columns() []string { return []string{"value"} }
func (*fenceRows) Close() error      { return nil }
func (r *fenceRows) Next(out []driver.Value) error {
	if !r.has {
		return io.EOF
	}
	r.has = false
	out[0] = r.v
	return nil
}
