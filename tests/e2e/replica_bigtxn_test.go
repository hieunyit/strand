package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/network"
)

func makeReplica(t *testing.T, r *mysqlNode, primaryAlias string) {
	t.Helper()
	r.exec(t,
		fmt.Sprintf("CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=3306, SOURCE_USER='root', SOURCE_PASSWORD='dev', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1", primaryAlias),
		"START REPLICA")
}

func waitReplicaCaughtUp(t *testing.T, primary, r *mysqlNode) {
	t.Helper()
	want := primary.gtidExecuted(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var res int
	if err := r.DB.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 300)", want).Scan(&res); err != nil || res != 0 {
		t.Fatalf("replica %s did not catch up (res=%d err=%v)", r.Alias, res, err)
	}
}

// Gate 1: đổi replica (nguồn đọc) giữa các lần restart vẫn resume đúng theo GTID (CP-2, CP-3).
func TestG1_E2E_ReplicaSwitchBetweenRestarts(t *testing.T) {
	seed := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	primary := startMySQL(t, mysqlImage("source"), "primary", 1, nw)
	r1 := startMySQL(t, mysqlImage("source"), "replica1", 3, nw)
	r2 := startMySQL(t, mysqlImage("source"), "replica2", 4, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	makeReplica(t, r1, "primary")
	makeReplica(t, r2, "primary")
	createSchema(t, primary)
	createSchema(t, dst)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	waitReplicaCaughtUp(t, primary, r1)
	waitReplicaCaughtUp(t, primary, r2)

	wl := newWorkload(primary.DB, seed)
	wl.Preload(t, 5000)
	wl.Start(6, 5000)

	cfg := strandCfg{Name: "e2e-replica", SourceDSN: r1.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t)}
	sources := []*mysqlNode{r1, r2, r1, r2}
	var p *strandProc
	for i, s := range sources {
		cfg.SourceDSN = s.RootDSN
		p = startStrand(t, cfg.write(t))
		waitSynced(t, &cfg, p, 3*time.Minute)
		time.Sleep(4 * time.Second)
		if i < len(sources)-1 {
			// Xen kẽ dừng an toàn và kill -9 khi đổi nguồn đọc.
			if i%2 == 0 {
				p.kill9()
			} else if code := p.stop(t); code != 0 {
				t.Fatalf("exit %d\n%s", code, p.tail())
			}
			t.Logf("run %d read from %s", i, s.Alias)
		}
	}
	wl.Stop()
	waitReplicaCaughtUp(t, primary, r2)
	waitCaughtUp(t, primary, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, primary, dst, appTables...)
}

// Gate 1: transaction rất lớn (mặc định 50 triệu row, STRAND_E2E_BIGTXN_ROWS) trộn với Txn
// nhỏ đang chạy: Txn lớn không bị tách, không giữ hết trong RAM, 0 lệch (CO-1, P1-03).
func TestG1_E2E_LargeTransaction(t *testing.T) {
	seed := seedFromEnv(t)
	rows := envInt("STRAND_E2E_BIGTXN_ROWS", 50_000_000)
	src, dst := newPair(t)
	for _, n := range []*mysqlNode{src, dst} {
		n.exec(t, "CREATE TABLE app.big (id BIGINT PRIMARY KEY, ver BIGINT UNSIGNED NOT NULL, v INT NOT NULL)")
	}
	src.exec(t, "CREATE TABLE app.nums (n INT PRIMARY KEY)")
	var sb strings.Builder
	sb.WriteString("INSERT INTO app.nums VALUES ")
	for i := 0; i < 10000; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(%d)", i)
	}
	src.exec(t, sb.String())

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 2000)

	cfg := strandCfg{Name: "e2e-bigtxn", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored", "app.nums"}, Dir: testDir(t)}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)
	wl.Start(4, 2000)

	start := time.Now()
	outer := (rows + 9999) / 10000
	q := fmt.Sprintf(`INSERT INTO app.big (id, ver, v)
		SELECT a.n * 10000 + b.n, UUID_SHORT(), a.n FROM app.nums a JOIN app.nums b
		 WHERE a.n < %d AND a.n * 10000 + b.n < %d`, outer, rows)
	if _, err := src.DB.Exec(q); err != nil {
		t.Fatalf("big txn: %v", err)
	}
	t.Logf("source committed one transaction of %d rows in %s", rows, time.Since(start).Round(time.Second))
	// Thêm vài Txn nhỏ chạm cùng bảng ngay sau Txn lớn (phải chờ Txn lớn ở target).
	for i := 0; i < 50; i++ {
		src.exec(t, fmt.Sprintf("UPDATE app.big SET ver = UUID_SHORT(), v = v + 1 WHERE id = %d", i*7))
	}
	wl.Stop()

	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Hour)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	var cs, cd, ss, sd int64
	_ = src.DB.QueryRow("SELECT COUNT(*), COALESCE(SUM(v),0) FROM app.big").Scan(&cs, &ss)
	_ = dst.DB.QueryRow("SELECT COUNT(*), COALESCE(SUM(v),0) FROM app.big").Scan(&cd, &sd)
	if cs != cd || ss != sd {
		t.Fatalf("app.big mismatch: source count=%d sum=%d, target count=%d sum=%d", cs, ss, cd, sd)
	}
	var chkS, chkD string
	_ = src.DB.QueryRow("CHECKSUM TABLE app.big").Scan(new(string), &chkS)
	_ = dst.DB.QueryRow("CHECKSUM TABLE app.big").Scan(new(string), &chkD)
	if chkS != chkD {
		t.Fatalf("app.big CHECKSUM differs: %s vs %s", chkS, chkD)
	}
	compareFinal(t, src, dst, appTables...)
	t.Logf("large transaction of %d rows applied: count, sum and CHECKSUM TABLE identical; total %s", rows, time.Since(start).Round(time.Second))
}
