package e2e

import (
	"strings"
	"testing"
	"time"
)

// 02 §12: DDL trên bảng thuộc pipeline (kể cả bảng MỚI khớp bộ lọc) chỉ đi tiếp khi schema ở target
// đã khớp; nếu chưa khớp thì dừng schema_change_pending (exit 10), và chạy lại sau khi DBA áp DDL ở
// target thì đi tiếp đúng từ DDL đó, không mất row.
func TestP1_E2E_DDLStopsUntilTargetSchemaMatches(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	createSchema(t, src, dst)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 2000)
	wl.Start(4, 2000)
	defer wl.Stop()

	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-ddl", CreateSchema: "off", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t)}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)

	// 1. CREATE TABLE khớp bộ lọc, target đã có bảng giống hệt: tham gia pipeline, mọi row tới target.
	const newDDL = "CREATE TABLE app.newt (id BIGINT PRIMARY KEY, v VARCHAR(20) NOT NULL)"
	dst.exec(t, newDDL)
	src.exec(t, newDDL)
	for i := 1; i <= 200; i++ {
		src.exec(t, "INSERT INTO app.newt VALUES ("+itoa(i)+", 'a')")
	}
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	compareFinal(t, src, dst, "app.newt")

	// 2. ALTER trên bảng của pipeline khi target CHƯA áp: dừng exit 10 schema_change_pending.
	src.exec(t, "ALTER TABLE app.newt ADD COLUMN w INT NULL")
	src.exec(t, "INSERT INTO app.newt VALUES (1001, 'b', 7)")
	deadline := time.Now().Add(2 * time.Minute)
	for !p.exited() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !p.exited() || p.exitCode() != 10 || !strings.Contains(p.output.String(), "schema_change_pending") {
		t.Fatalf("ALTER without target change: want exit 10 schema_change_pending\n%s", p.tail())
	}
	// Chạy lại khi target vẫn chưa khớp: vẫn dừng (không bỏ qua DDL).
	p2 := startStrand(t, cfg.write(t))
	if c := p2.exitCode(); c != 10 {
		t.Fatalf("restart before target change: exit %d, want 10\n%s", c, p2.tail())
	}

	// 3. DBA áp DDL ở target rồi chạy lại: đi tiếp từ DDL, row sau DDL tới đủ.
	dst.exec(t, "ALTER TABLE app.newt ADD COLUMN w INT NULL")
	p3 := startStrand(t, cfg.write(t))
	waitCaughtUp(t, src, dst, &cfg, p3, 3*time.Minute)

	// 4. CREATE TABLE khớp bộ lọc mà target chưa có: dừng (trước đây row của bảng này bị bỏ không báo).
	src.exec(t, "CREATE TABLE app.newu (id BIGINT PRIMARY KEY)")
	src.exec(t, "INSERT INTO app.newu VALUES (1), (2)")
	deadline = time.Now().Add(2 * time.Minute)
	for !p3.exited() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !p3.exited() || p3.exitCode() != 10 || !strings.Contains(p3.output.String(), "app.newu does not exist on target") {
		t.Fatalf("CREATE TABLE missing on target: want exit 10 naming app.newu\n%s", p3.tail())
	}
	dst.exec(t, "CREATE TABLE app.newu (id BIGINT PRIMARY KEY)")
	p4 := startStrand(t, cfg.write(t))
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p4, 3*time.Minute)
	if code := p4.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p4.tail())
	}
	compareFinal(t, src, dst, append(appTables, "app.newt", "app.newu")...)
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{digits[i%10]}, b...)
		i /= 10
	}
	return string(b)
}
