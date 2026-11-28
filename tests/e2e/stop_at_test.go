package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// `sync --stop-at` / `--stop-at-time` (như CdcStopPosition của AWS DMS): Strand apply mọi thay đổi tới điểm
// dừng, không apply thay đổi sau đó, thoát exit 0; lần chạy sau (không có điểm dừng) tiếp tục từ đó.

// waitExit chờ Strand tự thoát và trả exit code.
func waitExit(t *testing.T, p *strandProc, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !p.exited() {
		if time.Now().After(deadline) {
			t.Fatalf("strand did not stop at the stop position within %s\n%s", timeout, p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	return p.exitCode()
}

func pgCount(t *testing.T, n *pgNode, q string) int {
	t.Helper()
	var c int
	if err := n.DB.QueryRow(q).Scan(&c); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return c
}

func TestSA_E2E_PGStopAtLSNAndTime(t *testing.T) {
	slot, pub := "strand_e2e_stop", "strand_e2e_stop_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	cfg := strandCfg{Name: "e2e-pg-stop", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	cfgPath := cfg.write(t)

	// Lô A (10 Txn), điểm dừng, lô B (10 Txn).
	for i := 1; i <= 10; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.marker VALUES (%d, 'A')", 9000+i))
	}
	var stop string
	if err := src.DB.QueryRow("SELECT pg_current_wal_insert_lsn()::text").Scan(&stop); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.marker VALUES (%d, 'B')", 9100+i))
	}

	p := startStrand(t, cfgPath, "--start-at", lsn, "--stop-at", stop)
	if code := waitExit(t, p, 2*time.Minute); code != 0 {
		t.Fatalf("stop at LSN: exit %d\n%s", code, p.tail())
	}
	if !strings.Contains(p.output.String(), "stop position reached") {
		t.Fatalf("missing stop log\n%s", p.tail())
	}
	if a, b := pgCount(t, dst, "SELECT count(*) FROM app.marker WHERE tag = 'A'"), pgCount(t, dst, "SELECT count(*) FROM app.marker WHERE tag = 'B'"); a != 10 || b != 0 {
		t.Fatalf("after --stop-at %s: target has %d A rows and %d B rows, want 10 and 0", stop, a, b)
	}

	// Mốc thời gian giữa lô C và lô D.
	for i := 1; i <= 5; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.marker VALUES (%d, 'C')", 9200+i))
	}
	time.Sleep(2 * time.Second)
	at := time.Now().Add(-1 * time.Second)
	time.Sleep(time.Second)
	for i := 1; i <= 5; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.marker VALUES (%d, 'D')", 9300+i))
	}
	p = startStrand(t, cfgPath, "--stop-at-time", at.Format(time.RFC3339))
	if code := waitExit(t, p, 2*time.Minute); code != 0 {
		t.Fatalf("stop at time: exit %d\n%s", code, p.tail())
	}
	if b, c, d := pgCount(t, dst, "SELECT count(*) FROM app.marker WHERE tag = 'B'"), pgCount(t, dst, "SELECT count(*) FROM app.marker WHERE tag = 'C'"), pgCount(t, dst, "SELECT count(*) FROM app.marker WHERE tag = 'D'"); b != 10 || c != 5 || d != 0 {
		t.Fatalf("after --stop-at-time: B=%d C=%d D=%d, want 10 5 0", b, c, d)
	}

	// Đã ở sau điểm dừng: thoát ngay, không apply gì.
	p = startStrand(t, cfgPath, "--stop-at", stop)
	if code := waitExit(t, p, time.Minute); code != 0 || !strings.Contains(p.output.String(), "already covers the stop position") {
		t.Fatalf("stop position already applied: exit %d\n%s", code, p.tail())
	}
	if d := pgCount(t, dst, "SELECT count(*) FROM app.marker WHERE tag = 'D'"); d != 0 {
		t.Fatalf("a run whose stop position is already applied applied D rows: %d", d)
	}

	// Chạy tiếp không có điểm dừng: lô D tới.
	p = startStrand(t, cfgPath)
	waitPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	comparePG(t, src, dst, pgAppTables...)
}

func TestSA_E2E_MySQLStopAtGTID(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, "CREATE DATABASE sa", "CREATE TABLE sa.m (id INT PRIMARY KEY, tag VARCHAR(4))")
	dst.exec(t, "CREATE SCHEMA sa", "CREATE TABLE sa.m (id int PRIMARY KEY, tag varchar(4))")
	cfg := strandCfg{Name: "e2e-mysql-stop", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: []string{"sa.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	for i := 1; i <= 10; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO sa.m VALUES (%d, 'A')", i))
	}
	stop := src.gtidExecuted(t)
	for i := 1; i <= 10; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO sa.m VALUES (%d, 'B')", 100+i))
	}
	p = startStrand(t, cfgPath, "--stop-at", stop)
	if code := waitExit(t, p, 2*time.Minute); code != 0 {
		t.Fatalf("stop at GTID: exit %d\n%s", code, p.tail())
	}
	if a, b := pgCount(t, dst, "SELECT count(*) FROM sa.m WHERE tag = 'A'"), pgCount(t, dst, "SELECT count(*) FROM sa.m WHERE tag = 'B'"); a != 10 || b != 0 {
		t.Fatalf("after --stop-at %s: A=%d B=%d, want 10 0", stop, a, b)
	}
	p = startStrand(t, cfgPath)
	waitPGRow(t, dst.DB, p, "SELECT count(*)::text FROM sa.m WHERE tag = 'B'", "10")
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
}

// --stop-at-time khi source đứng yên sau mốc nhưng router còn hàng nghìn Txn trước mốc chưa xử lý: trước
// đây cờ "đuổi kịp" của monitor (PG: sent_lsn cách đầu WAL ≤ 16 MiB) bật ngay, pipeline dừng và router bỏ
// các Txn trước mốc còn trong hàng đợi — báo "đã apply tới mốc" khi target còn thiếu.
func TestSA_E2E_PGStopAtTimeWaitsForQueuedTxns(t *testing.T) {
	slot, pub := "strand_e2e_stopq", "strand_e2e_stopq_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	cfg := strandCfg{Name: "e2e-pg-stopq", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	cfgPath := cfg.write(t)

	const n = 30000
	src.exec(t, fmt.Sprintf(`DO $$ BEGIN FOR i IN 1..%d LOOP INSERT INTO app.marker VALUES (100000 + i, 'Q'); COMMIT; END LOOP; END $$`, n))
	time.Sleep(1100 * time.Millisecond)
	at := time.Now()
	time.Sleep(3 * time.Second) // source đứng yên, đồng hồ đã qua mốc

	p := startStrand(t, cfgPath, "--start-at", lsn, "--stop-at-time", at.Format(time.RFC3339))
	if code := waitExit(t, p, 3*time.Minute); code != 0 {
		t.Fatalf("stop at time: exit %d\n%s", code, p.tail())
	}
	if !strings.Contains(p.output.String(), "stop position reached") {
		t.Fatalf("missing stop log\n%s", p.tail())
	}
	if got := pgCount(t, dst, "SELECT count(*) FROM app.marker WHERE tag = 'Q'"); got != n {
		t.Fatalf("--stop-at-time %s reported the stop position as applied with %d of the %d transactions committed before it", at.Format(time.RFC3339), got, n)
	}
}
