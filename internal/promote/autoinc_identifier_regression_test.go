package promote

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

type autoIncIdentConnector struct {
	column  string
	queries *[]string
}

func (c autoIncIdentConnector) Connect(context.Context) (driver.Conn, error) {
	return autoIncIdentConn{c}, nil
}
func (autoIncIdentConnector) Driver() driver.Driver { return autoIncIdentDriver{} }

type autoIncIdentDriver struct{}

func (autoIncIdentDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type autoIncIdentConn struct{ autoIncIdentConnector }

func (autoIncIdentConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (autoIncIdentConn) Close() error              { return nil }
func (autoIncIdentConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c autoIncIdentConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	*c.queries = append(*c.queries, q)
	return driver.RowsAffected(1), nil
}
func (c autoIncIdentConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	*c.queries = append(*c.queries, q)
	if strings.Contains(q, "information_schema.COLUMNS") {
		return &autoIncIdentRows{cols: []string{"COLUMN_NAME", "AUTO_INCREMENT"}, values: []driver.Value{c.column, int64(11)}}, nil
	}
	return &autoIncIdentRows{cols: []string{"MAX"}, values: []driver.Value{int64(10)}}, nil
}

type autoIncIdentRows struct {
	cols   []string
	values []driver.Value
}

func (r *autoIncIdentRows) Columns() []string { return r.cols }
func (r *autoIncIdentRows) Close() error      { return nil }
func (r *autoIncIdentRows) Next(dest []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(dest, r.values)
	r.values = nil
	return nil
}

func TestApplyAutoIncrementPreservesCatalogIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		name, table, column, wantSelect, wantAlter string
	}{
		{"leading space", "app.items", " id", "SELECT MAX(` id`) FROM `app`.`items`", "ALTER TABLE `app`.`items` AUTO_INCREMENT = 21"},
		{"leading backtick", "app.items", "`id", "SELECT MAX(```id`) FROM `app`.`items`", "ALTER TABLE `app`.`items` AUTO_INCREMENT = 21"},
		{"trailing backtick", "app.items`", "id`", "SELECT MAX(`id```) FROM `app`.`items```", "ALTER TABLE `app`.`items``` AUTO_INCREMENT = 21"},
		{"schema space and backtick", " app`.items", "id", "SELECT MAX(`id`) FROM ` app```.`items`", "ALTER TABLE ` app```.`items` AUTO_INCREMENT = 21"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var queries []string
			db := sql.OpenDB(autoIncIdentConnector{tc.column, &queries})
			defer db.Close()
			err := NewMySQLTargetDriver(db).ApplyAutoIncrement(context.Background(), []AutoIncCounter{{DstTable: tc.table, Next: 21}})
			if err != nil {
				t.Fatal(err)
			}
			if len(queries) != 4 || queries[2] != tc.wantSelect || queries[3] != tc.wantAlter {
				t.Fatalf("catalog identifiers were changed: queries=%q, want SELECT %q and ALTER %q", queries, tc.wantSelect, tc.wantAlter)
			}
		})
	}
}

func TestMySQLSequenceOperationsPreserveRawIdentifiers(t *testing.T) {
	var queries []string
	db := sql.OpenDB(autoIncIdentConnector{queries: &queries})
	defer db.Close()
	d := NewMySQLTargetDriver(db)
	if max, err := d.FindColumnMax(context.Background(), "app.`items`", "`id`"); err != nil || max != 10 {
		t.Fatalf("FindColumnMax: max=%d err=%v", max, err)
	}
	if err := d.Setval(context.Background(), SequenceInfo{Table: "app.`items`"}, 21); err != nil {
		t.Fatal(err)
	}
	wantSelect := "SELECT MAX(```id```) FROM `app`.```items```;"
	wantAlter := "ALTER TABLE `app`.```items``` AUTO_INCREMENT = 21;"
	if len(queries) != 2 || queries[0] != wantSelect || queries[1] != wantAlter {
		t.Fatalf("raw names changed: queries=%q, want %q and %q", queries, wantSelect, wantAlter)
	}
}

func TestMySQLSequenceExplicitSchema(t *testing.T) {
	var queries []string
	db := sql.OpenDB(autoIncIdentConnector{queries: &queries})
	defer db.Close()
	d := NewMySQLTargetDriver(db)
	// Table with dot in table name when schema is explicitly provided
	if err := d.Setval(context.Background(), SequenceInfo{Schema: "my_db", Table: "order.details"}, 50); err != nil {
		t.Fatal(err)
	}
	wantAlter := "ALTER TABLE `my_db`.`order.details` AUTO_INCREMENT = 50;"
	if len(queries) != 1 || queries[0] != wantAlter {
		t.Fatalf("explicit schema: queries=%q, want %q", queries, wantAlter)
	}
}
