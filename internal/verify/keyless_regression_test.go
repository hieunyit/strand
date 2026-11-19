package verify

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strand/internal/model"
	"strings"
	"testing"
)

type keylessHashConnector struct{ hash func() string }

func (c keylessHashConnector) Connect(context.Context) (driver.Conn, error) {
	return keylessHashConn{c.hash}, nil
}
func (c keylessHashConnector) Driver() driver.Driver { return keylessHashDriver{c} }

type keylessHashDriver struct{ c keylessHashConnector }

func (d keylessHashDriver) Open(string) (driver.Conn, error) {
	return d.c.Connect(context.Background())
}

type keylessHashConn struct{ hash func() string }

func (c keylessHashConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c keylessHashConn) Close() error              { return nil }
func (c keylessHashConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c keylessHashConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "information_schema.columns") {
		return &keylessHashRows{cols: []string{"column", "type"}, data: [][]driver.Value{{"v", "text"}}}, nil
	}
	if strings.Contains(q, "count(*)::text") {
		return &keylessHashRows{cols: []string{"count", "hash"}, data: [][]driver.Value{{"1", c.hash()}}}, nil
	}
	return nil, errors.New("unexpected query: " + q)
}

type keylessHashRows struct {
	cols []string
	data [][]driver.Value
}

func (r *keylessHashRows) Columns() []string { return r.cols }
func (r *keylessHashRows) Close() error      { return nil }
func (r *keylessHashRows) Next(v []driver.Value) error {
	if len(r.data) == 0 {
		return io.EOF
	}
	copy(v, r.data[0])
	r.data = r.data[1:]
	return nil
}

func TestKeylessVerifyRequiresStableSource(t *testing.T) {
	cases := []struct {
		name          string
		src, dst      []string
		attempts      int
		match, stable bool
	}{
		{"matching_first_read_is_unstable", []string{"a", "b", "b", "b"}, []string{"a", "b"}, 2, true, true},
		{"exhaustion_inconclusive", []string{"a", "b", "b", "c", "c", "d", "d", "e", "e", "f"}, []string{"a", "b", "c", "d", "e"}, 5, false, false},
		{"stable_mismatch", []string{"a", "a"}, []string{"b"}, 1, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, wait := range []bool{false, true} {
				sn, dn := 0, 0
				src := sql.OpenDB(keylessHashConnector{func() string {
					if sn >= len(tc.src) {
						t.Fatal("unexpected extra source hash")
					}
					v := tc.src[sn]
					sn++
					return v
				}})
				dst := sql.OpenDB(keylessHashConnector{func() string {
					if dn >= len(tc.dst) {
						t.Fatal("unexpected extra target hash")
					}
					v := tc.dst[dn]
					dn++
					return v
				}})
				cfg := FullVerifyConfig{Source: src, Target: dst, SourceEngine: model.EnginePG, TargetEngine: model.EnginePG}
				waiter := &fakeWaiter{}
				if wait {
					cfg.ApplyWaiter = waiter
				}
				rep, err := NewMerkleVerifier(cfg).VerifyTableKeyless(context.Background(), model.TableID{Schema: "public", Table: "t"})
				src.Close()
				dst.Close()
				if err != nil {
					t.Fatal(err)
				}
				if rep.Match != tc.match || rep.StableOnSource != tc.stable || rep.Attempts != tc.attempts {
					t.Fatalf("wait=%v got %+v", wait, rep)
				}
				if sn != 2*tc.attempts || dn != tc.attempts {
					t.Fatalf("hash counts src=%d dst=%d", sn, dn)
				}
				if wait && (waiter.marks != tc.attempts || waiter.waits != tc.attempts) {
					t.Fatalf("waiter %+v", waiter)
				}
			}
		})
	}
}
