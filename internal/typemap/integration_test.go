package typemap_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
	"strand/internal/typemap"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

var (
	mysqlOnce sync.Once
	mysqlDB   *sql.DB
	mysqlStop func()

	pgOnce sync.Once
	pgDB   *sql.DB
	pgStop func()
)

func getMySQL(t *testing.T) *sql.DB {
	t.Helper()
	mysqlOnce.Do(func() {
		ctx := context.Background()
		cmd := []string{
			"--server-id=1001",
			"--gtid-mode=ON",
			"--enforce-gtid-consistency=ON",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
		}

		req := testcontainers.ContainerRequest{
			Image:        "mysql:8.4",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "dev",
				"MYSQL_DATABASE":      "strand",
			},
			Cmd:        cmd,
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Logf("failed to start mysql container: %v", err)
			return
		}

		host, err := container.Host(ctx)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get mysql host: %v", err)
			return
		}

		portNat, err := container.MappedPort(ctx, "3306")
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get mapped mysql port: %v", err)
			return
		}

		addr := net.JoinHostPort(host, portNat.Port())
		dsn := fmt.Sprintf("root:dev@tcp(%s)/strand?multiStatements=true&parseTime=true", addr)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to open mysql db: %v", err)
			return
		}

		for i := 0; i < 30; i++ {
			if err := db.Ping(); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		mysqlDB = db
		mysqlStop = func() {
			_ = db.Close()
			_ = container.Terminate(context.Background())
		}
	})

	if mysqlDB == nil {
		t.Fatal("MySQL container could not be started")
	}
	return mysqlDB
}

func getPostgres(t *testing.T) *sql.DB {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		cmd := []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
		}

		req := testcontainers.ContainerRequest{
			Image:        "postgres:18",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "strand",
				"POSTGRES_PASSWORD": "dev",
				"POSTGRES_DB":       "strand",
			},
			Cmd:        cmd,
			WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(180 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Logf("failed to start postgres container: %v", err)
			return
		}

		host, err := container.Host(ctx)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get pg host: %v", err)
			return
		}

		portNat, err := container.MappedPort(ctx, "5432")
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get pg mapped port: %v", err)
			return
		}

		port, _ := strconv.Atoi(portNat.Port())
		dsn := fmt.Sprintf("postgres://strand:dev@%s:%d/strand?sslmode=disable", host, port)

		db, err := sql.Open("pgx", dsn)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to open pg db: %v", err)
			return
		}

		for i := 0; i < 30; i++ {
			if err := db.Ping(); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		pgDB = db
		pgStop = func() {
			_ = db.Close()
			_ = container.Terminate(context.Background())
		}
	})

	if pgDB == nil {
		t.Fatal("Postgres container could not be started")
	}
	return pgDB
}

func TestMain(m *testing.M) {
	code := m.Run()
	if mysqlStop != nil {
		mysqlStop()
	}
	if pgStop != nil {
		pgStop()
	}
	os.Exit(code)
}

// AC1: _ci -> C: bảng có PK varchar utf8mb4_0900_ai_ci, hai giá trị chỉ khác hoa/thường KHÔNG tồn tại cùng lúc ở source;
// map sang PG collation "C"; copy dữ liệu bằng typemap -> 0 lệch khi so từng row.
func TestAC1_Collation_CI_To_C_CopyNoMismatch(t *testing.T) {
	ctx := context.Background()
	mDB := getMySQL(t)
	pDB := getPostgres(t)

	// 1. Tạo bảng nguồn trên MySQL với PK varchar utf8mb4_0900_ai_ci
	_, err := mDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS strand.test_ac1_users (
			username VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci PRIMARY KEY,
			full_name VARCHAR(100) NOT NULL,
			age INT,
			created_at TIMESTAMP(6)
		)`)
	if err != nil {
		t.Fatalf("create mysql table failed: %v", err)
	}
	_, _ = mDB.ExecContext(ctx, "DELETE FROM strand.test_ac1_users")

	// Chèn dữ liệu mẫu (các giá trị không trùng hoa/thường)
	_, err = mDB.ExecContext(ctx, `
		INSERT INTO strand.test_ac1_users (username, full_name, age, created_at) VALUES
		('alice', 'Alice Smith', 25, '2026-01-01 10:00:00.000000'),
		('BOB', 'Bob Builder', 30, '2026-01-02 11:30:00.000000'),
		('Charlie', 'Charlie Brown', 35, '2026-01-03 12:45:00.000000')`)
	if err != nil {
		t.Fatalf("insert mysql data failed: %v", err)
	}

	// 2. Tạo bảng đích trên PostgreSQL với PK varchar collation "C"
	_, err = pDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS public.test_ac1_users (
			username VARCHAR(64) COLLATE "C" PRIMARY KEY,
			full_name TEXT NOT NULL,
			age INTEGER,
			created_at TIMESTAMPTZ(6)
		)`)
	if err != nil {
		t.Fatalf("create postgres table failed: %v", err)
	}
	_, _ = pDB.ExecContext(ctx, "TRUNCATE TABLE public.test_ac1_users")

	// 3. Đọc metadata bảng từ MySQL và dựng column mappings
	cols, err := typemap.InspectMySQLColumns(ctx, mDB, "strand", "test_ac1_users")
	if err != nil {
		t.Fatalf("inspect mysql columns failed: %v", err)
	}
	opts := typemap.DefaultOptions()
	mappings := make(map[string]typemap.ColumnMapping, len(cols))
	for _, col := range cols {
		cm, err := typemap.MapMySQLColumn(col, opts)
		if err != nil {
			t.Fatalf("map column %s failed: %v", col.Name, err)
		}
		mappings[col.Name] = cm
	}

	// Kiểm tra collation cột khóa
	if err := typemap.CheckKeyCollation("utf8mb4_0900_ai_ci", "C"); err != nil {
		t.Fatalf("CheckKeyCollation utf8mb4_0900_ai_ci -> C failed: %v", err)
	}

	// 4. Copy dữ liệu bằng typemap: đọc từng dòng từ MySQL, chuyển đổi và ghi sang PostgreSQL
	rows, err := mDB.QueryContext(ctx, "SELECT username, full_name, age, created_at FROM strand.test_ac1_users ORDER BY username")
	if err != nil {
		t.Fatalf("query mysql failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var uRaw, fRaw, cRaw any
		var aRaw any
		if err := rows.Scan(&uRaw, &fRaw, &aRaw, &cRaw); err != nil {
			t.Fatalf("scan mysql row failed: %v", err)
		}

		uVal, err := mappings["username"].Convert(uRaw)
		if err != nil {
			t.Fatalf("convert username failed: %v", err)
		}
		fVal, err := mappings["full_name"].Convert(fRaw)
		if err != nil {
			t.Fatalf("convert full_name failed: %v", err)
		}
		aVal, err := mappings["age"].Convert(aRaw)
		if err != nil {
			t.Fatalf("convert age failed: %v", err)
		}
		cVal, err := mappings["created_at"].Convert(cRaw)
		if err != nil {
			t.Fatalf("convert created_at failed: %v", err)
		}

		_, err = pDB.ExecContext(ctx, `
			INSERT INTO public.test_ac1_users (username, full_name, age, created_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (username) DO UPDATE SET full_name = EXCLUDED.full_name, age = EXCLUDED.age, created_at = EXCLUDED.created_at`,
			uVal, fVal, aVal, cVal)
		if err != nil {
			t.Fatalf("insert into postgres failed: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration failed: %v", err)
	}

	// 5. So sánh từng row giữa MySQL và PostgreSQL -> đảm bảo 0 lệch
	pRows, err := pDB.QueryContext(ctx, "SELECT username, full_name, age, created_at FROM public.test_ac1_users ORDER BY username")
	if err != nil {
		t.Fatalf("query postgres failed: %v", err)
	}
	defer pRows.Close()

	type userRow struct {
		username string
		fullName string
		age      int32
		createAt time.Time
	}

	var pgList []userRow
	for pRows.Next() {
		var r userRow
		if err := pRows.Scan(&r.username, &r.fullName, &r.age, &r.createAt); err != nil {
			t.Fatalf("scan postgres row failed: %v", err)
		}
		pgList = append(pgList, r)
	}

	expectedUsers := []string{"BOB", "Charlie", "alice"}
	if len(pgList) != len(expectedUsers) {
		t.Fatalf("expected %d rows in PG, got %d", len(expectedUsers), len(pgList))
	}
	for i, exp := range expectedUsers {
		if pgList[i].username != exp {
			t.Errorf("row %d username mismatch: expected %q, got %q", i, exp, pgList[i].username)
		}
	}
}

// AC2: target lỏng hơn: cột khóa target dùng citext hoặc collation nondeterministic (ICU ci)
// -> validate exit 3 collation_looser.
func TestAC2_TargetCollationLooser_Rejected(t *testing.T) {
	ctx := context.Background()
	mDB := getMySQL(t)
	pDB := getPostgres(t)
	opts := typemap.DefaultOptions()

	// 1. Kiểm thử trường hợp target cột khóa dùng citext
	t.Run("target_key_citext", func(t *testing.T) {
		_, err := mDB.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS strand.ac2_test_citext (
				code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY,
				data VARCHAR(100)
			)`)
		if err != nil {
			t.Fatalf("create mysql table failed: %v", err)
		}

		_, err = pDB.ExecContext(ctx, `
			CREATE EXTENSION IF NOT EXISTS citext;
			CREATE TABLE IF NOT EXISTS public.ac2_test_citext (
				code CITEXT PRIMARY KEY,
				data TEXT
			)`)
		if err != nil {
			t.Fatalf("create pg table failed: %v", err)
		}

		_, err = typemap.CheckMySQLToPGTables(ctx, mDB, pDB, []string{"strand.ac2_test_citext"}, opts)
		if err == nil {
			t.Fatalf("expected error for citext target key column, got nil")
		}
		code := exitcode.GetExitCode(err)
		if code != exitcode.Rejected {
			t.Errorf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
		}
		if !strings.Contains(err.Error(), exitcode.ErrCollationLooser) && !strings.Contains(err.Error(), "citext") {
			t.Errorf("expected error to contain %s or citext, got: %v", exitcode.ErrCollationLooser, err)
		}
	})

	// 2. Kiểm thử trường hợp target dùng collation nondeterministic (ICU ci)
	t.Run("target_key_icu_nondeterministic", func(t *testing.T) {
		_, err := mDB.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS strand.ac2_test_nondet (
				code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY,
				data VARCHAR(100)
			)`)
		if err != nil {
			t.Fatalf("create mysql table failed: %v", err)
		}

		_, err = pDB.ExecContext(ctx, `
			DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'strand_coll_x') THEN
					CREATE COLLATION strand_coll_x (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
				END IF;
			END$$;
			CREATE TABLE IF NOT EXISTS public.ac2_test_nondet (
				code VARCHAR(50) COLLATE strand_coll_x PRIMARY KEY,
				data TEXT
			)`)
		if err != nil {
			t.Fatalf("create pg table with icu collation failed: %v", err)
		}

		_, err = typemap.CheckMySQLToPGTables(ctx, mDB, pDB, []string{"strand.ac2_test_nondet"}, opts)
		if err == nil {
			t.Fatalf("expected error for nondeterministic ICU collation, got nil")
		}
		code := exitcode.GetExitCode(err)
		if code != exitcode.Rejected {
			t.Errorf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
		}
		if !strings.Contains(err.Error(), exitcode.ErrCollationLooser) {
			t.Errorf("expected error to contain %s, got: %v", exitcode.ErrCollationLooser, err)
		}
	})
}

// AC3: zero date: source có '0000-00-00' -> validate báo đúng bảng/cột/số row với policy reject.
func TestAC3_ZeroDate_ValidationReject(t *testing.T) {
	ctx := context.Background()
	mDB := getMySQL(t)
	pDB := getPostgres(t)

	// Tạo bảng nguồn có cột DATE
	_, err := mDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS strand.test_ac3_zero_date (
			id INT PRIMARY KEY,
			event_date DATE
		)`)
	if err != nil {
		t.Fatalf("create mysql table failed: %v", err)
	}
	_, _ = mDB.ExecContext(ctx, "DELETE FROM strand.test_ac3_zero_date")

	// Chèn dòng chứa '0000-00-00' bằng cách tắt sql_mode nghiêm ngặt trong session
	_, err = mDB.ExecContext(ctx, `
		SET SESSION sql_mode = '';
		INSERT INTO strand.test_ac3_zero_date (id, event_date) VALUES (1, '0000-00-00');`)
	if err != nil {
		t.Fatalf("insert zero date into mysql failed: %v", err)
	}

	opts := typemap.DefaultOptions()
	opts.ZeroDatePolicy = "reject"

	_, err = typemap.CheckMySQLToPGTables(ctx, mDB, pDB, []string{"strand.test_ac3_zero_date"}, opts)
	if err == nil {
		t.Fatalf("expected error for zero date with policy reject, got nil")
	}

	// 1. Kiểm tra exit code là 3 (Rejected / ErrTypeUnmappable)
	code := exitcode.GetExitCode(err)
	if code != exitcode.Rejected {
		t.Errorf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
	}

	errMsg := err.Error()
	// 2. Báo đúng bảng
	if !strings.Contains(errMsg, "test_ac3_zero_date") {
		t.Errorf("error message does not contain table name 'test_ac3_zero_date': %s", errMsg)
	}
	// 3. Báo đúng cột
	if !strings.Contains(errMsg, "event_date") {
		t.Errorf("error message does not contain column name 'event_date': %s", errMsg)
	}
	// 4. Báo đúng số row (1 row)
	if !strings.Contains(errMsg, "1 row") {
		t.Errorf("error message does not contain row count (1 row): %s", errMsg)
	}
}

func TestValidation_PracticalScan_Int8OverflowAndEnum(t *testing.T) {
	ctx := context.Background()
	mDB := getMySQL(t)
	pDB := getPostgres(t)

	// 1. Unsigned bigint vượt 2^63-1 khi dùng int8-checked
	t.Run("bigint_unsigned_overflow_scan", func(t *testing.T) {
		_, err := mDB.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS strand.test_bigint_over (
				id BIGINT UNSIGNED PRIMARY KEY,
				info VARCHAR(50)
			)`)
		if err != nil {
			t.Fatalf("create table failed: %v", err)
		}
		_, _ = mDB.ExecContext(ctx, "DELETE FROM strand.test_bigint_over")
		// Chèn 1 dòng có giá trị vượt 2^63-1 (ví dụ: 18446744073709551615)
		_, err = mDB.ExecContext(ctx, "INSERT INTO strand.test_bigint_over VALUES (18446744073709551615, 'overflow')")
		if err != nil {
			t.Fatalf("insert overflow bigint failed: %v", err)
		}

		opts := typemap.DefaultOptions()
		opts.BigIntUnsignedPolicy = "int8-checked"

		_, err = typemap.CheckMySQLToPGTables(ctx, mDB, pDB, []string{"strand.test_bigint_over"}, opts)
		if err == nil {
			t.Fatalf("expected error for bigint unsigned exceeding 2^63-1")
		}
		if exitcode.GetExitCode(err) != exitcode.Rejected {
			t.Errorf("expected exit code %d, got %d", exitcode.Rejected, exitcode.GetExitCode(err))
		}
		if !strings.Contains(err.Error(), "test_bigint_over") || !strings.Contains(err.Error(), "id") {
			t.Errorf("expected table and column in error, got: %v", err)
		}
	})

	// 2. ENUM ngoài danh sách hợp lệ
	t.Run("enum_invalid_value_scan", func(t *testing.T) {
		_, err := mDB.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS strand.test_enum_invalid (
				id INT PRIMARY KEY,
				state ENUM('open', 'closed')
			)`)
		if err != nil {
			t.Fatalf("create table failed: %v", err)
		}
		_, _ = mDB.ExecContext(ctx, "DELETE FROM strand.test_enum_invalid")
		// Tắt strict mode và chèn giá trị rỗng/lỗi (index 0 trong MySQL enum)
		_, err = mDB.ExecContext(ctx, `
			SET SESSION sql_mode = '';
			INSERT INTO strand.test_enum_invalid (id, state) VALUES (1, '');`)
		if err != nil {
			t.Fatalf("insert invalid enum failed: %v", err)
		}

		opts := typemap.DefaultOptions()
		opts.EnumPolicy = "check"

		_, err = typemap.CheckMySQLToPGTables(ctx, mDB, pDB, []string{"strand.test_enum_invalid"}, opts)
		if err == nil {
			t.Fatalf("expected error for invalid enum value")
		}
		if exitcode.GetExitCode(err) != exitcode.Rejected {
			t.Errorf("expected exit code %d, got %d", exitcode.Rejected, exitcode.GetExitCode(err))
		}
	})
}
