package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// startGroupReplicationTarget starts a one-member Group Replication group whose
// transaction size limit is limitBytes.
func startGroupReplicationTarget(t *testing.T, alias string, serverID int, limitBytes int, extra ...string) *mysqlNode {
	t.Helper()
	n := startMySQL(t, "mysql:8.4", alias, serverID, nil, append([]string{
		"--log-bin=mysql-bin",
		"--plugin-load-add=group_replication.so",
		"--loose-group-replication-group-name=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		"--loose-group-replication-start-on-boot=OFF",
		"--loose-group-replication-local-address=127.0.0.1:33061",
		"--loose-group-replication-group-seeds=127.0.0.1:33061"}, extra...)...)
	n.exec(t, "SET GLOBAL group_replication_bootstrap_group=ON", "START GROUP_REPLICATION",
		"SET GLOBAL group_replication_bootstrap_group=OFF",
		fmt.Sprintf("SET GLOBAL group_replication_transaction_size_limit=%d", limitBytes))
	var state string
	if err := n.DB.QueryRow("SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&state); err != nil || state != "ONLINE" {
		t.Fatalf("group replication member state %q: %v", state, err)
	}
	return n
}

// Group Replication rejects a transaction above its size limit at commit
// (error 3100 in before_commit). Auto-seed must commit such a copy in parts,
// and an interrupted attempt must empty those parts before copying again.
func TestMDB_E2E_AutoSeedGroupReplicationTargetCommitsInParts(t *testing.T) {
	src := startMariaDBImage(t, mdbImage(), "src", 1)
	dst := startGroupReplicationTarget(t, "dst", 2, 1<<20)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	dst.exec(t, "GRANT REFERENCES ON app.* TO 'strand'@'%'")
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.parent (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app.child (id BIGINT PRIMARY KEY, parent_id BIGINT, payload MEDIUMBLOB NOT NULL,"+
			" CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES app.parent(id) ON DELETE CASCADE) ENGINE=InnoDB",
		"INSERT INTO app.parent VALUES (1), (2), (3)")
	for i := 1; i <= 24; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.child VALUES (%d, %d, REPEAT('%c', 262144))", i, i%3+1, 'a'+i%26))
	}
	cfg := strandCfg{
		Name: "mdb-auto-seed-gr", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
		Include:   []string{"app.*"}, Dir: testDir(t), AutoSeed: true, TablesExtra: klKeylessCfg,
		SourceIOPS: 20, // 320 KiB/s: the 6 MiB copy takes about 20 s
	}
	path := cfg.write(t)
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitStrandLog(t, p, "auto-seed part committed", time.Minute)
	p.kill9()
	var visible int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.child").Scan(&visible); err != nil || visible == 0 {
		t.Fatalf("interrupted chunked copy: %d visible child rows, err=%v; want the committed parts", visible, err)
	}
	var applied string
	if err := dst.DB.QueryRow("SELECT applied FROM strand._strand_checkpoint WHERE pipeline_id=?", cfg.Name).Scan(&applied); err != nil || applied != "" {
		t.Fatalf("interrupted copy left a checkpoint: %q err=%v", applied, err)
	}

	// Rows deleted at the source while the pipeline is down must not survive
	// from the interrupted attempt.
	src.exec(t, "DELETE FROM app.child WHERE id=1")
	p = startStrand(t, path)
	waitStrandLog(t, p, "emptied target tables partly copied by an interrupted attempt", time.Minute)
	waitStrandLog(t, p, "auto-seed committed", 2*time.Minute)
	waitSynced(t, &cfg, p, time.Minute)
	for _, query := range []string{"SELECT * FROM app.parent ORDER BY id", "SELECT id, parent_id, MD5(payload) FROM app.child ORDER BY id"} {
		ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, query) })
	}
	src.exec(t, "DELETE FROM app.parent WHERE id=2", "UPDATE app.child SET payload='cdc' WHERE id=3")
	for _, query := range []string{"SELECT * FROM app.parent ORDER BY id", "SELECT id, parent_id, MD5(payload) FROM app.child ORDER BY id"} {
		ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, query) })
	}
	if strings.Contains(p.output.String(), "before_commit") {
		t.Fatalf("a commit hit the Group Replication size limit\n%s", p.tail())
	}
}

// Group Replication rejects rows of tables without a primary key (error 3098).
// Keyless source tables need the target to generate an invisible primary key;
// without it the pipeline must refuse before copying anything.
func TestMDB_E2E_AutoSeedGroupReplicationKeylessTables(t *testing.T) {
	src := startMariaDBImage(t, mdbImage(), "src", 1)
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.keyed (id BIGINT PRIMARY KEY, v VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.keyless (k VARCHAR(20), v VARCHAR(20)) ENGINE=InnoDB",
		"INSERT INTO app.keyed VALUES (1, 'a')", "INSERT INTO app.keyless VALUES ('x', '1'), ('x', '1'), ('y', '2')")
	mdbSourceUsers(t, src)
	newCfg := func(dst *mysqlNode, name string) strandCfg {
		return strandCfg{
			Name: name, SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
			TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
			Include:   []string{"app.*"}, Dir: testDir(t), AutoSeed: true, TablesExtra: klKeylessCfg,
		}
	}

	plain := startGroupReplicationTarget(t, "dst", 2, 1<<20)
	mdbTargetSetup(t, plain)
	cfg := newCfg(plain, "mdb-gr-keyless-refused")
	out, code := runStrandCLI(t, time.Minute, "sync", "-c", cfg.write(t))
	if code == 0 || !strings.Contains(out, "Group Replication") || !strings.Contains(out, "app.keyless") || !strings.Contains(out, "sql_generate_invisible_primary_key") {
		t.Fatalf("keyless table on a Group Replication target: exit %d, want an early refusal naming the fix\n%s", code, out)
	}
	var tables int
	if err := plain.DB.QueryRow("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='app'").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("refused start created %d target tables (err=%v)", tables, err)
	}

	gipk := startGroupReplicationTarget(t, "dst2", 3, 1<<20, "--sql-generate-invisible-primary-key=ON")
	mdbTargetSetup(t, gipk)
	cfg = newCfg(gipk, "mdb-gr-keyless-gipk")
	path := cfg.write(t)
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 2*time.Minute)
	src.exec(t, "INSERT INTO app.keyless VALUES ('z', '3')", "UPDATE app.keyless SET v='9' WHERE k='y'",
		"DELETE FROM app.keyless WHERE k='x' LIMIT 1", "UPDATE app.keyed SET v='b' WHERE id=1")
	for _, q := range []string{"SELECT k, v FROM app.keyless ORDER BY k, v", "SELECT * FROM app.keyed ORDER BY id"} {
		ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, gipk.DB, q) })
	}
}
