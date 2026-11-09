package e2e

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/admin"
	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Mô hình async commit: write=150, insert=250; A(commit=200) chưa apply nhưng marker(300) đã apply.
// LWD=199 hợp lệ ở relaxed. Helper chỉ được trả sau lần đọc thứ hai khi A đã apply (LWD=300).
func TestReviewF11CatchupWaitsForInsertedWAL(t *testing.T) {
	t.Setenv("STRAND_E2E_F11_WITNESS", "1")
	t.Setenv("STRAND_E2E_F11_UNSAFE_BARRIER", "")
	dir := t.TempDir()
	var polls atomic.Int32
	src := sql.OpenDB(f11Connector{source: true, polls: &polls})
	defer src.Close()
	dst := sql.OpenDB(f11Connector{polls: &polls})
	defer dst.Close()
	socket := filepath.Join(t.TempDir(), "admin.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	defer func() { ln.Close(); <-done }()
	go func() {
		defer close(done)
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			var req admin.Request
			_ = json.NewDecoder(c).Decode(&req)
			_ = json.NewEncoder(c).Encode(admin.Response{OK: true, Top: &admin.TopInfo{State: "synced"}})
			c.Close()
		}
	}()
	waitPGCaughtUp(t, &pgNode{DB: src}, &pgNode{DB: dst}, &strandCfg{Name: "f11", Socket: socket, Dir: dir}, nil, time.Second)
	if polls.Load() < 2 {
		t.Fatal("barrier returned while A was still missing; marker alone is not a prefix")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "f11-barrier.json"))
	if err != nil {
		t.Fatal(err)
	}
	var witness struct {
		Cutoff  string `json:"cutoff"`
		Write   string `json:"write"`
		Insert  string `json:"insert"`
		Applied string `json:"applied"`
	}
	if err := json.Unmarshal(raw, &witness); err != nil {
		t.Fatal(err)
	}
	if witness.Cutoff != "0/FA" || witness.Write != "0/96" || witness.Insert != "0/FA" {
		t.Fatalf("wrong witness: %+v", witness)
	}
	applied, err := checkpoint.ParseAppliedSet(model.EnginePG, witness.Applied)
	if err != nil || applied.Lwd().LSN != 300 {
		t.Fatalf("wrong durable witness LWD: %q, err=%v", witness.Applied, err)
	}
}

type f11Connector struct {
	source bool
	polls  *atomic.Int32
}

func (c f11Connector) Connect(context.Context) (driver.Conn, error) { return &f11Conn{c}, nil }
func (c f11Connector) Driver() driver.Driver                        { return f11Driver{} }

type f11Driver struct{}

func (f11Driver) Open(string) (driver.Conn, error) { return nil, fmt.Errorf("use connector") }

type f11Conn struct{ f11Connector }

func (c *f11Conn) Prepare(string) (driver.Stmt, error) { return nil, fmt.Errorf("unexpected prepare") }
func (c *f11Conn) Close() error                        { return nil }
func (c *f11Conn) Begin() (driver.Tx, error)           { return f11Tx{}, nil }
func (c *f11Conn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (c *f11Conn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	var v driver.Value
	switch {
	case strings.Contains(q, "pg_prepared_xacts"), strings.Contains(q, "_strand_worker_delta"):
		return &f11Rows{}, nil
	case strings.Contains(q, "pg_current_wal_lsn") && strings.Contains(q, "pg_current_wal_insert_lsn"):
		return &f11Rows{vs: []driver.Value{"0/96", "0/FA"}, has: true}, nil
	case strings.Contains(q, "pg_current_wal_insert_lsn"):
		v = "0/FA"
	case strings.Contains(q, "pg_current_wal_lsn"):
		v = "0/96"
	case strings.Contains(q, "SELECT epoch"):
		v = int64(1)
	case strings.Contains(q, "SELECT applied"):
		lsn := "0/C7"
		holes := `["0/C8"]`
		if c.polls.Add(1) > 1 {
			lsn = "0/12C"
			holes = "[]"
		}
		v = fmt.Sprintf(`{"lwd":%q,"high":"0/12C","holes":%s}`, lsn, holes)
	case strings.Contains(q, "count(*)"):
		v = int64(1)
	default:
		return nil, fmt.Errorf("unexpected query: %s", q)
	}
	return &f11Rows{v: v, has: true}, nil
}

type f11Tx struct{}

func (f11Tx) Commit() error   { return nil }
func (f11Tx) Rollback() error { return nil }

type f11Rows struct {
	v   driver.Value
	vs  []driver.Value
	has bool
}

func (r *f11Rows) Columns() []string {
	if r.vs != nil {
		return []string{"write", "insert"}
	}
	return []string{"value"}
}
func (*f11Rows) Close() error { return nil }
func (r *f11Rows) Next(v []driver.Value) error {
	if !r.has {
		return io.EOF
	}
	r.has = false
	if r.vs != nil {
		copy(v, r.vs)
	} else {
		v[0] = r.v
	}
	return nil
}
