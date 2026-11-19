package validate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strand/internal/model"
	"strand/internal/rowkey"
	"strings"
	"testing"
)

type targetKeyConnector struct{ indexes [][]driver.Value }

func (c targetKeyConnector) Connect(context.Context) (driver.Conn, error) {
	return targetKeyConn{c.indexes}, nil
}
func (c targetKeyConnector) Driver() driver.Driver { return targetKeyDriver{c} }

type targetKeyDriver struct{ c targetKeyConnector }

func (d targetKeyDriver) Open(string) (driver.Conn, error) { return d.c.Connect(context.Background()) }

type targetKeyConn struct{ indexes [][]driver.Value }

func (c targetKeyConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c targetKeyConn) Close() error              { return nil }
func (c targetKeyConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c targetKeyConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "COUNT(*)") {
		return &targetKeyRows{cols: []string{"count"}, data: [][]driver.Value{{int64(1)}}}, nil
	}
	if strings.Contains(q, "information_schema.STATISTICS") {
		return &targetKeyRows{cols: []string{"index", "column", "prefix"}, data: append([][]driver.Value(nil), c.indexes...)}, nil
	}
	return nil, errors.New("unexpected metadata query: " + q)
}

type targetKeyRows struct {
	cols []string
	data [][]driver.Value
}

func (r *targetKeyRows) Columns() []string { return r.cols }
func (r *targetKeyRows) Close() error      { return nil }
func (r *targetKeyRows) Next(v []driver.Value) error {
	if len(r.data) == 0 {
		return io.EOF
	}
	copy(v, r.data[0])
	r.data = r.data[1:]
	return nil
}

func TestCheckTargetKeyRejectsConflictingUniqueConstraints(t *testing.T) {
	exact := [][]driver.Value{{"uk_ab", "a", nil}, {"uk_ab", "b", nil}}
	cases := []struct {
		name string
		rows [][]driver.Value
		ok   bool
	}{
		{"exact", exact, true},
		{"subset_only", [][]driver.Value{{"uk_a", "a", nil}}, false},
		{"exact_and_subset", append(append([][]driver.Value{}, exact...), []driver.Value{"uk_a", "a", nil}), false},
		{"exact_and_unrelated", append(append([][]driver.Value{}, exact...), []driver.Value{"uk_c", "c", nil}), true},
		{"exact_and_functional", append(append([][]driver.Value{}, exact...), []driver.Value{"uk_fn", nil, nil}), true},
		{"exact_and_prefix", append(append([][]driver.Value{}, exact...), []driver.Value{"uk_prefix", "a", int64(3)}), true},
		{"exact_and_superset", append(append([][]driver.Value{}, exact...), []driver.Value{"uk_abc", "a", nil}, []driver.Value{"uk_abc", "b", nil}, []driver.Value{"uk_abc", "c", nil}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(targetKeyConnector{tc.rows})
			defer db.Close()
			id := model.TableID{Schema: "app", Table: "t"}
			err := CheckTargetKey(context.Background(), db, model.EngineMySQL, id, id, rowkey.Key{Name: "source_ab", Columns: []string{"a", "b"}})
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v want %v, err %v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestCheckTargetKeyMirroredPrimaryAndEmailUnique(t *testing.T) {
	db := sql.OpenDB(targetKeyConnector{[][]driver.Value{{"PRIMARY", "id", nil}, {"uk_email", "email", nil}}})
	defer db.Close()
	id := model.TableID{Schema: "app", Table: "users"}
	if err := CheckTargetKey(context.Background(), db, model.EngineMySQL, id, id, rowkey.Key{Name: rowkey.PrimaryName, Columns: []string{"id"}}); err != nil {
		t.Fatal(err)
	}
	// A selected fallback identity must also coexist with unrelated mirrored unique constraints.
	if err := CheckTargetKey(context.Background(), db, model.EngineMySQL, id, id, rowkey.Key{Name: "uk_email", Columns: []string{"email"}}); err != nil {
		t.Fatal(err)
	}
}
