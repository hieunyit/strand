package snapshot

// Regression test review N05: truy vấn WEIGHT_STRING phải có timeout (trước đây QueryRow không
// context: source treo thì luồng CDC treo vô hạn). Driver giả chặn tới khi ctx bị hủy; không cần DB.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

type n05BlockDriver struct{}

func (n05BlockDriver) Open(string) (driver.Conn, error) { return n05BlockConn{}, nil }

type n05BlockConn struct{}

func (n05BlockConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (n05BlockConn) Close() error                        { return nil }
func (n05BlockConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (n05BlockConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func init() { sql.Register("n05block", n05BlockDriver{}) }

func TestN05WeigherQueryHasTimeout(t *testing.T) {
	db, err := sql.Open("n05block", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old := weighQueryTimeout
	weighQueryTimeout = 50 * time.Millisecond
	defer func() { weighQueryTimeout = old }()

	weigh := newMySQLWeigher(db)
	done := make(chan error, 1)
	go func() {
		_, err := weigh(PKColumn{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, "a")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("weigher error = %v, want context deadline exceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("weigher query blocked without a deadline")
	}
}
