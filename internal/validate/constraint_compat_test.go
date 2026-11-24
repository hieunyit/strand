package validate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// mockQueryConn implements driver.Conn with simulated table metadata.
type mockQueryConn struct {
	responses map[string]*mockTableData
}

type mockTableData struct {
	cols []string
	rows [][]driver.Value
	err  error
}

func (c *mockQueryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not implemented")
}
func (c *mockQueryConn) Close() error              { return nil }
func (c *mockQueryConn) Begin() (driver.Tx, error) { return nil, errors.New("tx not implemented") }

func (c *mockQueryConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	// Check most specific pattern first
	if strings.Contains(q, "LEFT JOIN information_schema.COLUMNS") {
		if data, ok := c.responses["LEFT JOIN information_schema.COLUMNS"]; ok {
			return &mockRows{cols: data.cols, data: append([][]driver.Value(nil), data.rows...)}, nil
		}
	}
	for pattern, data := range c.responses {
		if strings.Contains(q, pattern) {
			if data.err != nil {
				return nil, data.err
			}
			return &mockRows{
				cols: data.cols,
				data: append([][]driver.Value(nil), data.rows...),
			}, nil
		}
	}
	return nil, fmt.Errorf("unhandled mock query: %s", q)
}

type mockRows struct {
	cols []string
	data [][]driver.Value
}

func (r *mockRows) Columns() []string { return r.cols }
func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Next(dest []driver.Value) error {
	if len(r.data) == 0 {
		return io.EOF
	}
	copy(dest, r.data[0])
	r.data = r.data[1:]
	return nil
}

type mockConnector struct {
	responses map[string]*mockTableData
}

func (c mockConnector) Connect(context.Context) (driver.Conn, error) {
	return &mockQueryConn{responses: c.responses}, nil
}
func (c mockConnector) Driver() driver.Driver { return mockDriver{c} }

type mockDriver struct{ c mockConnector }

func (d mockDriver) Open(string) (driver.Conn, error) {
	return d.c.Connect(context.Background())
}

// helpers to build mock databases
func buildMockMySQLDB(exists bool, cols []ColumnDescriptor, ucs []UniqueConstraint) *sql.DB {
	resp := make(map[string]*mockTableData)
	count := int64(0)
	if exists {
		count = 1
	}
	resp["information_schema.TABLES"] = &mockTableData{
		cols: []string{"count"},
		rows: [][]driver.Value{{count}},
	}

	var colRows [][]driver.Value
	for _, c := range cols {
		isNull := "NO"
		if c.Nullable {
			isNull = "YES"
		}
		dt := c.DataType
		if dt == "" {
			dt = "varchar"
		}
		colRows = append(colRows, []driver.Value{c.Name, isNull, c.Collation, dt})
	}
	resp["information_schema.COLUMNS"] = &mockTableData{
		cols: []string{"COLUMN_NAME", "IS_NULLABLE", "COLLATION_NAME", "DATA_TYPE"},
		rows: colRows,
	}

	var statRows [][]driver.Value
	for _, uc := range ucs {
		idxName := uc.Name
		if uc.IsPrimary {
			idxName = "PRIMARY"
		}
		if uc.HasExpr && len(uc.Columns) == 0 {
			statRows = append(statRows, []driver.Value{idxName, nil, nil, nil})
		}
		for _, col := range uc.Columns {
			var subPart driver.Value
			if uc.HasPrefix {
				subPart = int64(10)
			}
			var colVal driver.Value = col
			if uc.HasExpr {
				colVal = nil
			}
			statRows = append(statRows, []driver.Value{idxName, colVal, subPart, "NO"})
		}
	}
	resp["information_schema.STATISTICS"] = &mockTableData{
		cols: []string{"INDEX_NAME", "COLUMN_NAME", "SUB_PART", "NULLABLE"},
		rows: statRows,
	}

	var statJoinRows [][]driver.Value
	for _, uc := range ucs {
		idxName := uc.Name
		if uc.IsPrimary {
			idxName = "PRIMARY"
		}
		if uc.HasExpr && len(uc.Columns) == 0 {
			statJoinRows = append(statJoinRows, []driver.Value{idxName, nil, nil, "", ""})
		}
		for _, col := range uc.Columns {
			var subPart driver.Value
			if uc.HasPrefix {
				subPart = int64(10)
			}
			var colVal driver.Value = col
			if uc.HasExpr {
				colVal = nil
			}
			isNullable := "NO"
			for _, c := range cols {
				if c.Name == col && c.Nullable {
					isNullable = "YES"
					break
				}
			}
			statJoinRows = append(statJoinRows, []driver.Value{idxName, colVal, subPart, isNullable, ""})
		}
	}
	resp["LEFT JOIN information_schema.COLUMNS"] = &mockTableData{
		cols: []string{"INDEX_NAME", "COLUMN_NAME", "SUB_PART", "IS_NULLABLE", "EXTRA"},
		rows: statJoinRows,
	}

	return sql.OpenDB(mockConnector{responses: resp})
}

func buildMockPGDB(exists bool, cols []ColumnDescriptor, ucs []UniqueConstraint) *sql.DB {
	resp := make(map[string]*mockTableData)
	count := int64(0)
	if exists {
		count = 1
	}
	resp["information_schema.tables"] = &mockTableData{
		cols: []string{"count"},
		rows: [][]driver.Value{{count}},
	}

	var colRows [][]driver.Value
	for _, c := range cols {
		isNull := "NO"
		if c.Nullable {
			isNull = "YES"
		}
		dt := c.DataType
		if dt == "" {
			dt = "text"
		}
		colRows = append(colRows, []driver.Value{c.Name, isNull, c.Collation, dt})
	}
	resp["information_schema.columns"] = &mockTableData{
		cols: []string{"column_name", "is_nullable", "collation_name", "data_type"},
		rows: colRows,
	}

	var pgIndexRows [][]driver.Value
	for _, uc := range ucs {
		relname := uc.Name
		if uc.HasExpr && len(uc.Columns) == 0 {
			pgIndexRows = append(pgIndexRows, []driver.Value{relname, uc.IsPrimary, uc.IsPartial, true, uc.NullsNotDistinct, 1, "", false, ""})
		}
		for ord, col := range uc.Columns {
			attName := col
			hasExpr := uc.HasExpr
			if hasExpr {
				attName = ""
			}
			isNotNull := true
			collName := ""
			for _, cd := range cols {
				if cd.Name == col {
					isNotNull = !cd.Nullable
					collName = cd.Collation
					break
				}
			}
			pgIndexRows = append(pgIndexRows, []driver.Value{relname, uc.IsPrimary, uc.IsPartial, hasExpr, uc.NullsNotDistinct, ord + 1, attName, isNotNull, collName})
		}
	}
	resp["pg_class"] = &mockTableData{
		cols: []string{"relname", "indisprimary", "is_partial", "has_expr", "nulls_not_distinct", "ord", "attname", "attnotnull", "collname"},
		rows: pgIndexRows,
	}

	return sql.OpenDB(mockConnector{responses: resp})
}

// Regression 1: primary id + mirrored email accepted
func TestRegression_PrimaryIDAndMirroredEmailAccepted(t *testing.T) {
	ctx := context.Background()
	srcCols := []ColumnDescriptor{
		{Name: "id", Nullable: false, DataType: "int"},
		{Name: "email", Nullable: false, DataType: "varchar"},
	}
	srcIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		{Name: "uk_email", Columns: []string{"email"}},
	}

	dstCols := []ColumnDescriptor{
		{Name: "id", Nullable: false, DataType: "int"},
		{Name: "email", Nullable: false, DataType: "varchar"},
	}
	dstIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		{Name: "uk_email", Columns: []string{"email"}},
	}

	srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
	defer srcDB.Close()
	dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
	defer dstDB.Close()

	table := model.TableID{Schema: "app", Table: "users"}
	err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
	if err != nil {
		t.Fatalf("expected accepted for mirrored primary+email, got error: %v", err)
	}

	// Also verify PG target
	dstPGDB := buildMockPGDB(true, dstCols, dstIdx)
	defer dstPGDB.Close()
	err = CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstPGDB, model.EnginePG, table, table, ConstraintCheckOptions{})
	if err != nil {
		t.Fatalf("expected accepted for MySQL->PG mirrored primary+email, got error: %v", err)
	}
}

// Regression 2: target-only email rejected
func TestRegression_TargetOnlyEmailRejected(t *testing.T) {
	ctx := context.Background()
	srcCols := []ColumnDescriptor{
		{Name: "id", Nullable: false, DataType: "int"},
		{Name: "email", Nullable: false, DataType: "varchar"},
	}
	// Source ONLY has primary key id, no unique constraint on email
	srcIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
	}

	dstCols := []ColumnDescriptor{
		{Name: "id", Nullable: false, DataType: "int"},
		{Name: "email", Nullable: false, DataType: "varchar"},
	}
	// Target has primary key id AND target-only unique constraint on email
	dstIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		{Name: "uk_email", Columns: []string{"email"}},
	}

	table := model.TableID{Schema: "app", Table: "users"}

	// Target MySQL: verify silent merge warning
	t.Run("Target_MySQL_Silent_Merge", func(t *testing.T) {
		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected target-only email to be rejected, got nil error")
		}
		var ee *exitcode.Error
		if !errors.As(err, &ee) || ee.Code != exitcode.ErrMissingPK || ee.ExitCode != exitcode.Rejected {
			t.Fatalf("expected ErrMissingPK with Rejected exitcode, got: %v", err)
		}
		if !strings.Contains(err.Error(), "MySQL ON DUPLICATE KEY UPDATE will match this constraint and silently overwrite distinct source rows") {
			t.Fatalf("expected MySQL silent merge warning, got: %v", err)
		}
		if !strings.Contains(err.Error(), "remediation:") {
			t.Fatalf("expected remediation in error message, got: %v", err)
		}
	})

	// Target PostgreSQL: verify ON CONFLICT failure error
	t.Run("Target_Postgres_OnConflict_Failure", func(t *testing.T) {
		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstPGDB := buildMockPGDB(true, dstCols, dstIdx)
		defer dstPGDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstPGDB, model.EnginePG, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected target-only email to be rejected on PG target, got nil error")
		}
		var ee *exitcode.Error
		if !errors.As(err, &ee) || ee.Code != exitcode.ErrMissingPK || ee.ExitCode != exitcode.Rejected {
			t.Fatalf("expected ErrMissingPK with Rejected exitcode, got: %v", err)
		}
		if !strings.Contains(err.Error(), "PostgreSQL ON CONFLICT does not arbitrate on this constraint and will abort replication") {
			t.Fatalf("expected PG ON CONFLICT failure explanation, got: %v", err)
		}
		if !strings.Contains(err.Error(), "remediation:") {
			t.Fatalf("expected remediation in error message, got: %v", err)
		}
	})
}

// Regression 3: nullable UNIQUE
func TestRegression_NullableUnique(t *testing.T) {
	ctx := context.Background()
	table := model.TableID{Schema: "app", Table: "users"}

	// Case 3a: Source email is nullable, target requires NOT NULL
	t.Run("Source_Nullable_Target_NotNull_Rejected", func(t *testing.T) {
		srcCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: true},
		}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		dstCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: false}, // Target NOT NULL
		}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected rejection when source column is nullable but target is NOT NULL")
		}
		if !strings.Contains(err.Error(), "allows NULL but target column \"email\" is NOT NULL") {
			t.Fatalf("expected nullable mismatch message, got: %v", err)
		}
	})

	// Case 3b: Source has nullable UNIQUE(a), target has UNIQUE(a, b) superset
	t.Run("Source_Nullable_Subset_Cannot_Imply_Superset", func(t *testing.T) {
		srcCols := []ColumnDescriptor{
			{Name: "a", Nullable: true},
			{Name: "b", Nullable: false},
		}
		srcIdx := []UniqueConstraint{
			{Name: "uk_a", Columns: []string{"a"}},
		}

		dstCols := []ColumnDescriptor{
			{Name: "a", Nullable: true},
			{Name: "b", Nullable: false},
		}
		dstIdx := []UniqueConstraint{
			{Name: "uk_ab", Columns: []string{"a", "b"}},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected rejection when nullable source column attempts to imply superset")
		}
		if !strings.Contains(err.Error(), "allows NULL; cannot safely imply target superset unique index") {
			t.Fatalf("expected superset nullability error, got: %v", err)
		}
	})

	// Case 3c: Mirrored nullable unique with matching nullability accepted
	t.Run("Mirrored_Nullable_Accepted", func(t *testing.T) {
		srcCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: true},
		}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		dstCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: true},
		}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err != nil {
			t.Fatalf("expected mirrored nullable unique to be accepted, got: %v", err)
		}
	})

	// Case 3d: Target NullsNotDistinct with source nullable rejected
	t.Run("Target_NullsNotDistinct_Rejected", func(t *testing.T) {
		srcTC := &TableConstraints{
			Table:  table,
			Engine: model.EnginePG,
			Columns: map[string]ColumnDescriptor{
				"email": {Name: "email", Nullable: true},
			},
			Uniques: []UniqueConstraint{
				{Name: "uk_email", Columns: []string{"email"}},
			},
		}
		dstTC := &TableConstraints{
			Table:  table,
			Engine: model.EnginePG,
			Columns: map[string]ColumnDescriptor{
				"email": {Name: "email", Nullable: true},
			},
			Uniques: []UniqueConstraint{
				{Name: "uk_email", Columns: []string{"email"}, NullsNotDistinct: true},
			},
		}

		err := VerifyConstraintsCompatible(srcTC, dstTC, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected rejection when target enforces NULLS NOT DISTINCT")
		}
		if !strings.Contains(err.Error(), "enforces NULLS NOT DISTINCT") {
			t.Fatalf("expected NULLS NOT DISTINCT error, got: %v", err)
		}
	})
}

// Regression 4: strict subset vs superset
func TestRegression_StrictSubsetVsSuperset(t *testing.T) {
	ctx := context.Background()
	table := model.TableID{Schema: "app", Table: "t"}

	// Case 4a: Superset on target is accepted
	// Source has PRIMARY(id) NOT NULL. Target has UNIQUE(id, tenant_id).
	t.Run("Superset_Accepted", func(t *testing.T) {
		srcCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "tenant_id", Nullable: false},
		}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		}

		dstCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "tenant_id", Nullable: false},
		}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_id_tenant", Columns: []string{"id", "tenant_id"}},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err != nil {
			t.Fatalf("expected target superset to be accepted, got: %v", err)
		}
	})

	// Case 4b: Strict subset on target is rejected
	// Source composite PRIMARY(tenant_id, id). Target has UNIQUE(id).
	t.Run("Strict_Subset_Rejected", func(t *testing.T) {
		srcCols := []ColumnDescriptor{
			{Name: "tenant_id", Nullable: false},
			{Name: "id", Nullable: false},
		}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"tenant_id", "id"}},
		}

		dstCols := []ColumnDescriptor{
			{Name: "tenant_id", Nullable: false},
			{Name: "id", Nullable: false},
		}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"tenant_id", "id"}},
			{Name: "uk_id_only", Columns: []string{"id"}}, // strict subset
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected target strict subset to be rejected")
		}
		if !strings.Contains(err.Error(), "strict subset") {
			t.Fatalf("expected strict subset explanation, got: %v", err)
		}
	})
}

// Regression 5: functional/prefix descriptors
func TestRegression_FunctionalAndPrefixDescriptors(t *testing.T) {
	ctx := context.Background()
	table := model.TableID{Schema: "app", Table: "users"}

	// Case 5a: Target functional / expression index rejected with remediation
	t.Run("Target_Functional_Index_Rejected", func(t *testing.T) {
		srcCols := []ColumnDescriptor{{Name: "id", Nullable: false}, {Name: "email", Nullable: false}}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		dstCols := []ColumnDescriptor{{Name: "id", Nullable: false}, {Name: "email", Nullable: false}}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_lower_email", Columns: []string{"email"}, HasExpr: true},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected target functional index to be rejected")
		}
		if !strings.Contains(err.Error(), "functional/expression index not provably equivalent") {
			t.Fatalf("expected functional index rejection message, got: %v", err)
		}
		if !strings.Contains(err.Error(), "remediation:") {
			t.Fatalf("expected remediation, got: %v", err)
		}
	})

	// Case 5b: Target prefix index (SUB_PART) rejected with remediation
	t.Run("Target_Prefix_Index_Rejected", func(t *testing.T) {
		srcCols := []ColumnDescriptor{{Name: "id", Nullable: false}, {Name: "email", Nullable: false}}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		dstCols := []ColumnDescriptor{{Name: "id", Nullable: false}, {Name: "email", Nullable: false}}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_prefix_email", Columns: []string{"email"}, HasPrefix: true},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected target prefix index to be rejected")
		}
		if !strings.Contains(err.Error(), "prefix columns (SUB_PART)") {
			t.Fatalf("expected prefix index rejection message, got: %v", err)
		}
	})

	// Case 5c: Source prefix index cannot imply full target unique
	t.Run("Source_Prefix_Index_Cannot_Imply_Target", func(t *testing.T) {
		srcCols := []ColumnDescriptor{{Name: "id", Nullable: false}, {Name: "email", Nullable: false}}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email_prefix", Columns: []string{"email"}, HasPrefix: true},
		}

		dstCols := []ColumnDescriptor{{Name: "id", Nullable: false}, {Name: "email", Nullable: false}}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected source prefix index to not imply full target unique")
		}
		if !strings.Contains(err.Error(), "source unique index \"uk_email_prefix\" uses prefix columns which does not guarantee full value uniqueness") {
			t.Fatalf("expected source prefix warning, got: %v", err)
		}
	})

	// Case 5d: Mirrored functional index with matching descriptors accepted
	t.Run("Mirrored_Functional_Index_Accepted", func(t *testing.T) {
		srcTC := &TableConstraints{
			Table:  table,
			Engine: model.EnginePG,
			Columns: map[string]ColumnDescriptor{
				"id":    {Name: "id", Nullable: false},
				"email": {Name: "email", Nullable: false},
			},
			PrimaryKey: &UniqueConstraint{Name: "pk_id", IsPrimary: true, Columns: []string{"id"}},
			Uniques: []UniqueConstraint{
				{Name: "uk_fn", Columns: []string{"email"}, HasExpr: true},
			},
		}
		dstTC := &TableConstraints{
			Table:  table,
			Engine: model.EnginePG,
			Columns: map[string]ColumnDescriptor{
				"id":    {Name: "id", Nullable: false},
				"email": {Name: "email", Nullable: false},
			},
			PrimaryKey: &UniqueConstraint{Name: "pk_id", IsPrimary: true, Columns: []string{"id"}},
			Uniques: []UniqueConstraint{
				{Name: "uk_fn", Columns: []string{"email"}, HasExpr: true},
			},
		}

		err := VerifyConstraintsCompatible(srcTC, dstTC, ConstraintCheckOptions{})
		if err == nil {
			t.Fatal("expected unverifiable functional index to be rejected")
		}
	})
}

// Regression 6: target missing table
func TestRegression_TargetMissingTable(t *testing.T) {
	ctx := context.Background()
	srcCols := []ColumnDescriptor{
		{Name: "id", Nullable: false},
	}
	srcIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
	}

	srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
	defer srcDB.Close()
	// Target table does NOT exist (exists = false)
	dstDB := buildMockMySQLDB(false, nil, nil)
	defer dstDB.Close()

	table := model.TableID{Schema: "app", Table: "missing_orders"}
	err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
	if err != nil {
		t.Fatalf("expected target missing table to be safely skipped (nil error), got: %v", err)
	}
}

// Regression 7: renamed table and column contracts
func TestRegression_RenamedTableAndColumns(t *testing.T) {
	ctx := context.Background()
	srcTable := model.TableID{Schema: "app", Table: "users"}
	dstTable := model.TableID{Schema: "app", Table: "app_users"}

	srcCols := []ColumnDescriptor{
		{Name: "id", Nullable: false},
		{Name: "email", Nullable: false},
	}
	srcIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		{Name: "uk_email", Columns: []string{"email"}},
	}

	// Target table has renamed name app_users and column user_email
	dstCols := []ColumnDescriptor{
		{Name: "id", Nullable: false},
		{Name: "user_email", Nullable: false},
	}
	dstIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		{Name: "uk_user_email", Columns: []string{"user_email"}},
	}

	srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
	defer srcDB.Close()
	dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
	defer dstDB.Close()

	opts := ConstraintCheckOptions{
		TableRename: map[string]string{
			"app.users": "app.app_users",
		},
		ColumnRename: map[string]map[string]string{
			"app.users": {"email": "user_email"},
		},
	}

	err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, srcTable, dstTable, opts)
	if err != nil {
		t.Fatalf("expected renamed table and column contracts to be accepted, got: %v", err)
	}

	// Also check list function CheckConstraintCompatibility
	err = CheckConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, []model.TableID{srcTable}, opts)
	if err != nil {
		t.Fatalf("expected CheckConstraintCompatibility to apply rename contracts and pass, got: %v", err)
	}
}

// Collation looseness verification
func TestRegression_CollationLooser(t *testing.T) {
	ctx := context.Background()
	table := model.TableID{Schema: "app", Table: "users"}

	srcCols := []ColumnDescriptor{
		{Name: "id", Nullable: false},
		{Name: "code", Nullable: false, Collation: "utf8mb4_bin"},
	}
	srcIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		{Name: "uk_code", Columns: []string{"code"}},
	}

	dstCols := []ColumnDescriptor{
		{Name: "id", Nullable: false},
		{Name: "code", Nullable: false, Collation: "utf8mb4_general_ci"}, // looser collation
	}
	dstIdx := []UniqueConstraint{
		{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		{Name: "uk_code", Columns: []string{"code"}},
	}

	srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
	defer srcDB.Close()
	dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
	defer dstDB.Close()

	err := CheckTableConstraintCompatibility(ctx, srcDB, model.EngineMySQL, dstDB, model.EngineMySQL, table, table, ConstraintCheckOptions{})
	if err == nil {
		t.Fatal("expected looser target collation to be rejected")
	}
	var ee *exitcode.Error
	if !errors.As(err, &ee) || ee.Code != exitcode.ErrCollationLooser || ee.ExitCode != exitcode.Rejected {
		t.Fatalf("expected ErrCollationLooser exit 3, got: %v", err)
	}
	if !strings.Contains(err.Error(), "looser than source collation") {
		t.Fatalf("expected looser collation message, got: %v", err)
	}
}

// Integration with CheckRowKeys / CheckKeys
func TestRegression_CheckRowKeysIntegration(t *testing.T) {
	ctx := context.Background()
	table := model.TableID{Schema: "app", Table: "users"}

	// 1. Mirrored primary + email passes
	t.Run("CheckRowKeys_Passes_On_Mirrored", func(t *testing.T) {
		srcCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: false},
		}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		dstCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: false},
		}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		lines, err := CheckRowKeys(ctx, srcDB, "mysql", dstDB, "mysql", []model.TableID{table}, nil, KeylessOpts{})
		if err != nil {
			t.Fatalf("expected CheckRowKeys to pass, got: %v (lines: %v)", err, lines)
		}
	})

	// 2. Target-only unique index causes CheckRowKeys to fail with ErrMissingPK
	t.Run("CheckRowKeys_Fails_On_TargetOnly_Unique", func(t *testing.T) {
		srcCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: false},
		}
		srcIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
		}

		dstCols := []ColumnDescriptor{
			{Name: "id", Nullable: false},
			{Name: "email", Nullable: false},
		}
		dstIdx := []UniqueConstraint{
			{Name: "PRIMARY", IsPrimary: true, Columns: []string{"id"}},
			{Name: "uk_email", Columns: []string{"email"}},
		}

		srcDB := buildMockMySQLDB(true, srcCols, srcIdx)
		defer srcDB.Close()
		dstDB := buildMockMySQLDB(true, dstCols, dstIdx)
		defer dstDB.Close()

		_, err := CheckRowKeys(ctx, srcDB, "mysql", dstDB, "mysql", []model.TableID{table}, nil, KeylessOpts{})
		if err == nil {
			t.Fatal("expected CheckRowKeys to fail on target-only unique constraint")
		}
		var ee *exitcode.Error
		if !errors.As(err, &ee) || ee.Code != exitcode.ErrMissingPK || ee.ExitCode != exitcode.Rejected {
			t.Fatalf("expected ErrMissingPK exit 3, got: %v", err)
		}
	})
}

// TestIsolatedLiveMySQLCheck performs an isolated live verification against a real MySQL 8.4 instance.
func TestIsolatedLiveMySQLCheck(t *testing.T) {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.4",
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
			Cmd:          []string{"--server-id=1", "--binlog-format=ROW", "--binlog-row-image=FULL", "--innodb-use-native-aio=OFF"},
			WaitingFor:   wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "3306")
	addr := net.JoinHostPort(host, port.Port())
	db, err := sql.Open("mysql", fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", addr))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 1. Create schemas and tables:
	_, err = db.ExecContext(ctx, `
		CREATE DATABASE src_db;
		CREATE DATABASE dst_db;
		CREATE TABLE src_db.users (
			id INT PRIMARY KEY,
			email VARCHAR(100) NOT NULL,
			UNIQUE KEY uk_email (email)
		) ENGINE=InnoDB;
		CREATE TABLE dst_db.users (
			id INT PRIMARY KEY,
			email VARCHAR(100) NOT NULL,
			UNIQUE KEY uk_email (email)
		) ENGINE=InnoDB;
	`)
	if err != nil {
		t.Fatalf("create tables: %v", err)
	}

	srcTable := model.TableID{Schema: "src_db", Table: "users"}
	dstTable := model.TableID{Schema: "dst_db", Table: "users"}

	// 2. Mirrored primary id + uk_email passes
	if err := CheckTableConstraintCompatibility(ctx, db, model.EngineMySQL, db, model.EngineMySQL, srcTable, dstTable, ConstraintCheckOptions{}); err != nil {
		t.Fatalf("expected live mirrored primary + uk_email to pass, got: %v", err)
	}

	// 3. Add target-only unique constraint: must fail with ErrMissingPK
	_, err = db.ExecContext(ctx, `
		ALTER TABLE dst_db.users ADD COLUMN phone VARCHAR(20) NOT NULL;
		ALTER TABLE dst_db.users ADD UNIQUE KEY uk_phone (phone);
	`)
	if err != nil {
		t.Fatalf("add target-only unique: %v", err)
	}

	err = CheckTableConstraintCompatibility(ctx, db, model.EngineMySQL, db, model.EngineMySQL, srcTable, dstTable, ConstraintCheckOptions{})
	if err == nil {
		t.Fatal("expected live check to reject target-only uk_phone")
	}
	var ee *exitcode.Error
	if !errors.As(err, &ee) || ee.Code != exitcode.ErrMissingPK || ee.ExitCode != exitcode.Rejected {
		t.Fatalf("expected ErrMissingPK exit 3, got: %v", err)
	}
	if !strings.Contains(err.Error(), "MySQL ON DUPLICATE KEY UPDATE will match this constraint and silently overwrite distinct source rows") {
		t.Fatalf("expected silent merge warning, got: %v", err)
	}

	// 4. Drop target-only unique constraint and add superset unique constraint: must pass
	_, err = db.ExecContext(ctx, `
		ALTER TABLE dst_db.users DROP INDEX uk_phone;
		ALTER TABLE dst_db.users ADD UNIQUE KEY uk_id_email (id, email);
	`)
	if err != nil {
		t.Fatalf("alter to superset: %v", err)
	}

	if err := CheckTableConstraintCompatibility(ctx, db, model.EngineMySQL, db, model.EngineMySQL, srcTable, dstTable, ConstraintCheckOptions{}); err != nil {
		t.Fatalf("expected live check with superset to pass, got: %v", err)
	}
}

func TestRegressionExpressionSemanticsUnknown(t *testing.T) {
	cols := []ColumnDescriptor{{Name: "email"}, {Name: "phone"}}
	src := buildMockMySQLDB(true, cols, []UniqueConstraint{{Name: "lower_email", HasExpr: true}})
	dst := buildMockMySQLDB(true, cols, []UniqueConstraint{{Name: "lower_phone", HasExpr: true}})
	defer src.Close()
	defer dst.Close()
	table := model.TableID{Schema: "app", Table: "t"}
	if err := CheckTableConstraintCompatibility(context.Background(), src, model.EngineMySQL, dst, model.EngineMySQL, table, table, ConstraintCheckOptions{}); err == nil {
		t.Fatal("different expressions must not be accepted")
	}
}
func TestRegressionPGNullsNotDistinctCatalog(t *testing.T) {
	cols := []ColumnDescriptor{{Name: "email", Nullable: true}}
	table := model.TableID{Schema: "app", Table: "t"}
	for _, tc := range []struct {
		name                   string
		source, target, reject bool
	}{
		{"ordinary to not distinct", false, true, true},
		{"ordinary to ordinary", false, false, false},
		{"not distinct to not distinct", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := buildMockPGDB(true, cols, []UniqueConstraint{{Name: "src", Columns: []string{"email"}, NullsNotDistinct: tc.source}})
			dst := buildMockPGDB(true, cols, []UniqueConstraint{{Name: "dst", Columns: []string{"email"}, NullsNotDistinct: tc.target}})
			defer src.Close()
			defer dst.Close()
			err := CheckTableConstraintCompatibility(context.Background(), src, model.EnginePG, dst, model.EnginePG, table, table, ConstraintCheckOptions{})
			if (err != nil) != tc.reject {
				t.Fatalf("reject=%v: %v", tc.reject, err)
			}
		})
	}
}

func TestRegressionPGColumnCatalogFailureIsNotIgnored(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "t"}
	key := []UniqueConstraint{{Name: "email_key", Columns: []string{"email"}}}
	src := buildMockPGDB(true, []ColumnDescriptor{{Name: "email", Nullable: true}}, key)
	dst := buildMockPGDB(true, []ColumnDescriptor{{Name: "email", Nullable: false}}, key)
	defer src.Close()
	defer dst.Close()

	// A permission/catalog failure must not erase the target NOT NULL metadata and
	// make the source nullable unique key appear compatible.
	dstDB := dst.Driver().(mockDriver).c.responses
	dstDB["information_schema.columns"].err = errors.New("permission denied reading columns")
	err := CheckTableConstraintCompatibility(context.Background(), src, model.EnginePG, dst, model.EnginePG, table, table, ConstraintCheckOptions{})
	if err == nil || !strings.Contains(err.Error(), "permission denied reading columns") {
		t.Fatalf("column catalog error was ignored: %v", err)
	}
}
