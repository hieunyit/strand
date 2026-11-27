package e2e

import (
	"strings"
	"testing"
	"time"
)

// Review DBA (P1/P2): một bảng vào pipeline lúc chạy (`snapshot add`) phải qua cùng kiểm tra như lúc khởi
// động: exclude thắng, unique phụ ở bảng đích bị từ chối (upsert MySQL gộp hai row nguồn thành một), bảng
// đích không được là chính một bảng nguồn trên cùng server. doctor/validate và lần khởi động sau thấy cả bảng
// đã thêm lúc chạy.
func TestSC_E2E_SnapshotAddRunsStartupScopeChecks(t *testing.T) {
	src, dst := newPair(t)
	for _, n := range []*mysqlNode{src, dst} {
		n.exec(t,
			"CREATE TABLE app.base (id BIGINT PRIMARY KEY, v INT NOT NULL)",
			"CREATE TABLE app.hidden (id BIGINT PRIMARY KEY, v INT NOT NULL)",
			"CREATE TABLE app.dup (id BIGINT PRIMARY KEY, v INT NOT NULL)",
			"CREATE TABLE app.later (id BIGINT PRIMARY KEY, v INT NOT NULL)")
	}
	src.exec(t,
		"INSERT INTO app.base VALUES (1, 1)",
		"INSERT INTO app.hidden VALUES (1, 1)",
		"INSERT INTO app.dup VALUES (1, 7), (2, 7)", // hai row khác PK, cùng v
		"INSERT INTO app.later VALUES (1, 5), (2, 5)")
	dst.exec(t, "ALTER TABLE app.dup ADD UNIQUE KEY uq_v (v)") // unique chỉ có ở target

	cfg := strandCfg{Name: "e2e-scope", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.base"}, Exclude: []string{"app.hidden"}, Dir: testDir(t)}
	path := cfg.write(t)
	p := startStrand(t, path)
	waitSynced(t, &cfg, p, 3*time.Minute)

	add := func(table string) (string, int) {
		t.Helper()
		return runStrandCLI(t, time.Minute, "snapshot", "add", "--table", table, "--socket", cfg.Socket)
	}
	if out, code := add("app.hidden"); code == 0 || !strings.Contains(out, "tables.exclude") {
		t.Fatalf("snapshot add of an excluded table: exit %d\n%s", code, out)
	}
	if out, code := add("app.dup"); code == 0 || !strings.Contains(out, "uq_v") {
		t.Fatalf("snapshot add with an extra target unique key: exit %d\n%s", code, out)
	}
	if out, code := add("app.later"); code != 0 {
		t.Fatalf("snapshot add app.later: exit %d\n%s", code, out)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for scRows(t, dst, "app.later") != 2 {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("app.later not copied (exited=%v)\n%s", p.exited(), p.tail())
		}
		time.Sleep(300 * time.Millisecond)
	}
	if n := scRows(t, dst, "app.hidden"); n != 0 {
		t.Fatalf("excluded table copied to the target: %d rows", n)
	}
	if n := scRows(t, dst, "app.dup"); n != 0 {
		t.Fatalf("rejected table written to the target: %d rows", n)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("stop: exit %d\n%s", code, p.tail())
	}

	// Bảng đã thêm lúc chạy (app.later) giờ có unique phụ ở target: doctor, validate và lần khởi động sau
	// đều phải thấy nó dù nó không có trong tables.include.
	dst.exec(t, "DELETE FROM app.later", "ALTER TABLE app.later ADD UNIQUE KEY uq_later_v (v)")
	if out, code := runStrandCLI(t, time.Minute, "doctor", "-c", path); code == 0 || !strings.Contains(out, "uq_later_v") {
		t.Fatalf("doctor must check the table added during sync: exit %d\n%s", code, out)
	}
	if out, code := runStrandCLI(t, time.Minute, "validate", "-c", path); code == 0 || !strings.Contains(out, "uq_later_v") {
		t.Fatalf("validate must check the table added during sync: exit %d\n%s", code, out)
	}
	p2 := startStrand(t, path)
	select {
	case <-p2.done:
	case <-time.After(2 * time.Minute):
		p2.stop(t)
		t.Fatalf("sync started with an incompatible target unique key on a table added during sync\n%s", p2.tail())
	}
	if code := p2.exitCode(); code == 0 || !strings.Contains(p2.output.String(), "uq_later_v") {
		t.Fatalf("restart: exit %d\n%s", code, p2.tail())
	}
}

// Cùng server làm source và target (bảng gốc được rename sang schema khác): `snapshot add` một bảng mà đích
// chính là bảng nguồn đó phải bị từ chối như lúc khởi động (CheckTableOverlap).
func TestSC_E2E_SnapshotAddRejectsSourceAsTarget(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	src.exec(t,
		"CREATE DATABASE app", "CREATE DATABASE mirror", "CREATE DATABASE strand",
		"CREATE TABLE app.a (id BIGINT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE mirror.a (id BIGINT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE app.self (id BIGINT PRIMARY KEY, v INT NOT NULL)",
		"INSERT INTO app.a VALUES (1, 1)",
		"INSERT INTO app.self VALUES (1, 1)")
	cfg := strandCfg{Name: "e2e-self", SourceDSN: src.RootDSN, TargetDSN: src.RootDSN + "strand",
		Include: []string{"app.a"}, Dir: testDir(t), TablesExtra: "  rename: {app.a: mirror.a}\n"}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)
	if out, code := runStrandCLI(t, time.Minute, "snapshot", "add", "--table", "app.self", "--socket", cfg.Socket); code == 0 || !strings.Contains(out, "same database") {
		t.Fatalf("snapshot add app.self -> app.self on the same server: exit %d\n%s", code, out)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("stop: exit %d\n%s", code, p.tail())
	}
}

func scRows(t *testing.T, n *mysqlNode, table string) int {
	t.Helper()
	var c int
	if err := n.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&c); err != nil {
		t.Fatal(err)
	}
	return c
}
