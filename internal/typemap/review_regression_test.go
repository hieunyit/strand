package typemap_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/typemap"
)

// Review 3.1: dưới sql_mode mặc định của MySQL 8.4 (strict + NO_ZERO_DATE), `col = '0000-00-00'` trả
// ERROR 1525; trước đây lỗi bị nuốt và validate PASS dù bảng có zero date. Kết nối validate ở đây là pool
// MỚI (sql_mode mặc định của server), dữ liệu zero được chèn trên một conn riêng với sql_mode=”.
func TestReview31_ZeroDate_RejectedUnderDefaultSQLMode(t *testing.T) {
	ctx := context.Background()
	_ = getMySQL(t)
	pDB := getPostgres(t)

	ins, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open insert pool: %v", err)
	}
	defer ins.Close()
	conn, err := ins.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = ''"); err != nil {
		t.Fatalf("set sql_mode: %v", err)
	}

	// Pool validate: sql_mode mặc định của server (không ai SET SESSION trên pool này).
	vdb, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		t.Fatalf("open validate pool: %v", err)
	}
	defer vdb.Close()
	var mode string
	if err := vdb.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&mode); err != nil {
		t.Fatalf("read sql_mode: %v", err)
	}
	if !strings.Contains(mode, "NO_ZERO_DATE") || !strings.Contains(mode, "STRICT_TRANS_TABLES") {
		t.Fatalf("expected default strict sql_mode with NO_ZERO_DATE, got %q", mode)
	}

	cases := []struct {
		name, colType, zero string
	}{
		{"timestamp", "TIMESTAMP NULL", "0000-00-00 00:00:00"},
		{"date", "DATE", "0000-00-00"},
		{"datetime", "DATETIME", "0000-00-00 00:00:00"},
		{"date_zero_day", "DATE", "2024-05-00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := "rv31_zero_" + tc.name
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS strand.%s", tbl)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE strand.%s (id INT PRIMARY KEY, v %s)", tbl, tc.colType)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO strand.%s VALUES (1, '2024-01-02'), (2, '%s')", tbl, tc.zero)); err != nil {
				t.Fatalf("insert zero value: %v", err)
			}
			opts := typemap.DefaultOptions()
			opts.ZeroDatePolicy = "reject"
			_, err := typemap.CheckMySQLToPGTables(ctx, vdb, pDB, []string{"strand." + tbl}, opts)
			if err == nil {
				t.Fatalf("expected zero date in %s (%s) to be rejected under default sql_mode, validate passed", tbl, tc.colType)
			}
			if exitcode.GetExitCode(err) != exitcode.Rejected || !strings.Contains(err.Error(), "zero date") {
				t.Fatalf("expected Rejected zero-date error, got exit %d: %v", exitcode.GetExitCode(err), err)
			}
		})
	}

	// Bảng sạch (không zero date) vẫn phải PASS: predicate mới không được báo sai.
	t.Run("clean_table_passes", func(t *testing.T) {
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS strand.rv31_clean"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "CREATE TABLE strand.rv31_clean (id INT PRIMARY KEY, d DATE, ts TIMESTAMP NULL, dt DATETIME(6))"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO strand.rv31_clean VALUES (1, '2000-10-10', '2020-01-01 00:00:00', '2010-10-10 10:00:00.000100'), (2, NULL, NULL, NULL)"); err != nil {
			t.Fatal(err)
		}
		if _, err := typemap.CheckMySQLToPGTables(ctx, vdb, pDB, []string{"strand.rv31_clean"}, typemap.DefaultOptions()); err != nil {
			t.Fatalf("clean table must pass: %v", err)
		}
	})
}

// Review 3.3: hai collation cùng tên ở hai schema khác nhau, một deterministic, một nondeterministic.
// Tra theo tên + LIMIT 1 có thể chọn nhầm bản deterministic; phải resolve theo attcollation của cột.
func TestReview33_PGCollation_SameNameDifferentSchemas(t *testing.T) {
	ctx := context.Background()
	mDB := getMySQL(t)
	pDB := getPostgres(t)

	// Tạo bản deterministic TRƯỚC (thường nằm trước trong heap => LIMIT 1 cũ chọn nó).
	_, err := pDB.ExecContext(ctx, `
		DROP SCHEMA IF EXISTS rv33_a CASCADE;
		DROP SCHEMA IF EXISTS rv33_b CASCADE;
		CREATE SCHEMA rv33_a;
		CREATE SCHEMA rv33_b;
		CREATE COLLATION rv33_a.rv33_dupcoll (provider = icu, locale = 'und', deterministic = true);
		CREATE COLLATION rv33_b.rv33_dupcoll (provider = icu, locale = 'und', deterministic = false);
		CREATE TABLE public.rv33_nondet (code VARCHAR(50) COLLATE rv33_b.rv33_dupcoll PRIMARY KEY, data TEXT);
		CREATE TABLE public.rv33_det (code VARCHAR(50) COLLATE rv33_a.rv33_dupcoll PRIMARY KEY, data TEXT);
		CREATE DOMAIN public.rv33_dom AS VARCHAR(50) COLLATE rv33_b.rv33_dupcoll;
		CREATE TABLE public.rv33_domain (code public.rv33_dom PRIMARY KEY, data TEXT);`)
	if err != nil {
		t.Fatalf("setup pg: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pDB.ExecContext(context.Background(), `
			DROP TABLE IF EXISTS public.rv33_nondet, public.rv33_det, public.rv33_domain;
			DROP DOMAIN IF EXISTS public.rv33_dom;
			DROP SCHEMA IF EXISTS rv33_a CASCADE;
			DROP SCHEMA IF EXISTS rv33_b CASCADE;`)
	})
	for _, tbl := range []string{"rv33_nondet", "rv33_det", "rv33_domain"} {
		if _, err := mDB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS strand.%s (
			code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, data VARCHAR(100))`, tbl)); err != nil {
			t.Fatal(err)
		}
	}
	opts := typemap.DefaultOptions()

	for _, tbl := range []string{"rv33_nondet", "rv33_domain"} {
		t.Run(tbl+"_rejected", func(t *testing.T) {
			_, err := typemap.CheckMySQLToPGTables(ctx, mDB, pDB, []string{"strand." + tbl}, opts)
			if err == nil {
				t.Fatalf("expected key column on nondeterministic rv33_b.rv33_dupcoll to be rejected")
			}
			if exitcode.GetExitCode(err) != exitcode.Rejected || !strings.Contains(err.Error(), exitcode.ErrCollationLooser) {
				t.Fatalf("expected collation_looser reject, got exit %d: %v", exitcode.GetExitCode(err), err)
			}
		})
	}
	t.Run("deterministic_passes", func(t *testing.T) {
		if _, err := typemap.CheckMySQLToPGTables(ctx, mDB, pDB, []string{"strand.rv33_det"}, opts); err != nil {
			t.Fatalf("key column on deterministic rv33_a.rv33_dupcoll must pass: %v", err)
		}
	})
}
