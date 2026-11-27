package e2e

import (
	"database/sql"

	"strings"
	"testing"
	"time"
)

// spec: 02 §8 (zero date). MySQL → PG với tables.zero_date: epoch và zero_date_tables (set_null cho một
// bảng): zero date ở dữ liệu có sẵn (snapshot logic) và ở thay đổi mới (CDC) được ghi thành
// 1970-01-01 / NULL, pipeline không dừng, `strand verify --mode full` exit 0. Cột NOT NULL của bảng
// set_null giữ chính sách đó nhưng không có zero date nên không chặn.
func TestZD_E2E_MySQLToPGZeroDatePolicies(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t,
		"SET GLOBAL time_zone = '+00:00'",
		"CREATE DATABASE zd",
		"CREATE TABLE zd.ev (id INT PRIMARY KEY, d DATE NULL, dt DATETIME(6) NOT NULL, ts TIMESTAMP(6) NULL, v INT)",
		"CREATE TABLE zd.au (id INT PRIMARY KEY, d DATE NULL, created DATETIME NOT NULL, v INT)",
	)
	dst.exec(t,
		"CREATE SCHEMA zd",
		"CREATE TABLE zd.ev (id int PRIMARY KEY, d date, dt timestamp(6) NOT NULL, ts timestamptz(6), v int)",
		"CREATE TABLE zd.au (id int PRIMARY KEY, d date, created timestamp NOT NULL, v int)",
	)
	zeroExec := func(q string) {
		t.Helper()
		// multiStatements: SET và INSERT chạy trên cùng một kết nối.
		src.exec(t, "SET SESSION sql_mode = ''; SET SESSION time_zone = '+00:00'; "+q)
	}
	zeroExec(`INSERT INTO zd.ev VALUES
		(1, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', 1),
		(2, '2024-02-29', '2024-02-29 10:11:12.5', '2024-02-29 10:11:12.5', 2),
		(3, '2024-05-00', '2024-00-10 00:00:00', NULL, 3)`)
	zeroExec(`INSERT INTO zd.au VALUES (1, '0000-00-00', '2024-01-01 00:00:00', 1), (2, '2023-12-31', '2024-01-02 00:00:00', 2)`)

	cfg := strandCfg{Name: "e2e-zero-date", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: []string{"zd.*"}, Dir: testDir(t),
		TablesExtra: "  zero_date: epoch\n  zero_date_tables:\n    zd.au: set_null\n"}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p, 3*time.Minute)

	// CDC: insert và update mang zero date, cùng một hàng thường để chắc stream đã tới.
	zeroExec(`INSERT INTO zd.ev VALUES (10, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', 10)`)
	zeroExec(`UPDATE zd.ev SET d = '0000-00-00', v = 22 WHERE id = 2`)
	zeroExec(`INSERT INTO zd.au VALUES (10, '0000-00-00', '2024-03-03 00:00:00', 10)`)
	src.exec(t, "INSERT INTO zd.au VALUES (11, '2024-04-04', '2024-04-04 00:00:00', 11)")

	waitPGRow(t, dst.DB, p, "SELECT count(*) FROM zd.au WHERE id = 11", "1")
	type want struct{ q, v string }
	for _, w := range []want{
		{"SELECT d::text || '|' || dt::text || '|' || (ts AT TIME ZONE 'UTC')::text FROM zd.ev WHERE id = 1", "1970-01-01|1970-01-01 00:00:00|1970-01-01 00:00:00"},
		{"SELECT d::text || '|' || dt::text || '|' || coalesce(ts::text, 'N') FROM zd.ev WHERE id = 3", "1970-01-01|1970-01-01 00:00:00|N"},
		{"SELECT d::text || '|' || dt::text || '|' || (ts AT TIME ZONE 'UTC')::text FROM zd.ev WHERE id = 10", "1970-01-01|1970-01-01 00:00:00|1970-01-01 00:00:00"},
		{"SELECT d::text || '|' || v::text FROM zd.ev WHERE id = 2", "1970-01-01|22"},
		{"SELECT coalesce(d::text, 'N') || '|' || created::text FROM zd.au WHERE id = 1", "N|2024-01-01 00:00:00"},
		{"SELECT coalesce(d::text, 'N') || '|' || created::text FROM zd.au WHERE id = 10", "N|2024-03-03 00:00:00"},
		{"SELECT d::text FROM zd.au WHERE id = 2", "2023-12-31"},
	} {
		var got string
		if err := dst.DB.QueryRow(w.q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", w.q, err)
		}
		if got != w.v {
			t.Fatalf("%s = %q, want %q", w.q, got, w.v)
		}
	}
	out := p.output.String()
	if !strings.Contains(out, "zero date written as a replacement value") {
		t.Fatalf("missing zero date replacement log\n%s", p.tail())
	}
	for _, col := range []string{`"column":"d"`, `"column":"dt"`, `"column":"ts"`, `"written_as":"NULL"`, `"written_as":"1970-01-01 00:00:00"`} {
		if !strings.Contains(out, col) {
			t.Fatalf("replacement log lacks %s\n%s", col, p.tail())
		}
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	for _, tbl := range []string{"zd.ev", "zd.au"} {
		vout, code := runStrandCLI(t, 3*time.Minute, "verify", "-c", cfgPath, "--mode", "full", "--table", tbl)
		if code != 0 {
			t.Fatalf("verify --mode full %s exit %d:\n%s", tbl, code, vout)
		}
	}
	// Lệch thật vẫn bị bắt ở hàng có giá trị thay.
	dst.exec(t, "UPDATE zd.ev SET v = 99 WHERE id = 1")
	vout, code := runStrandCLI(t, 3*time.Minute, "verify", "-c", cfgPath, "--mode", "full", "--table", "zd.ev")
	if code != 5 {
		t.Fatalf("verify after a real change: exit %d, want 5\n%s", code, vout)
	}
}

// waitPGRow chờ truy vấn q trên target trả want (pipeline phải còn chạy).
func waitPGRow(t *testing.T, db *sql.DB, p *strandProc, q, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var got string
		err := db.QueryRow(q).Scan(&got)
		if err == nil && got == want {
			return
		}
		if p.exited() {
			t.Fatalf("strand exited (code %d) while waiting for %s\n%s", p.exitCode(), q, p.tail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s = %q (err %v), want %q\n%s", q, got, err, want, p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
}
