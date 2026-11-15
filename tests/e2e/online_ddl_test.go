package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// P3-02 — online DDL của gh-ost / pt-osc trên bảng đang nhận ghi (Gate 3): Strand không replicate dữ liệu copy
// sang bảng tạm, đích nhận đúng MỘT ALTER (tại barrier cut-over), không có bảng tạm ở đích, 0 lệch.
//
// Chế độ chạy tool:
//   - Binary thật: nếu `gh-ost` / `pt-online-schema-change` có trong PATH (hoặc STRAND_E2E_GHOST_BIN /
//     STRAND_E2E_PTOSC_BIN) thì chạy tool thật vào source qua cổng map của container.
//   - Mô phỏng: nếu không có binary, test phát ĐÚNG chuỗi SQL mà tool phát (tên bảng/trigger/RENAME theo
//     gh-ost 1.1 và pt-osc 3.x), kể cả trigger thật của pt-osc. Với gh-ost, việc áp binlog vào _gho được thay
//     bằng bước đồng bộ dưới LOCK TABLES ngay trước cut-over. Log test ghi rõ chế độ ("online DDL mode=...").
//
// Cả hai chế độ dùng cùng phần kiểm: đếm ALTER ở đích, không bảng tạm, log cut-over đúng 1 lần, 0 lệch.

const onlineAlter = "ADD COLUMN extra INT NULL"

func lookTool(envName string, names ...string) string {
	if p := os.Getenv(envName); p != "" {
		return p
	}
	for _, n := range names {
		if p, err := exec.LookPath(n); err == nil {
			return p
		}
	}
	return ""
}

func mustExecDB(t *testing.T, db *sql.DB, qs ...string) {
	t.Helper()
	for _, q := range qs {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// runOnlineDDLTool chạy gh-ost (tool="gh-ost") hoặc pt-osc (tool="pt-osc") trên schema.table của src.
func runOnlineDDLTool(t *testing.T, src *mysqlNode, tool, schema, table string) {
	t.Helper()
	host, port, _ := strings.Cut(src.Addr, ":")
	if tool == "gh-ost" {
		if bin := lookTool("STRAND_E2E_GHOST_BIN", "gh-ost"); bin != "" {
			t.Logf("online DDL mode=real binary=%s", bin)
			out, err := exec.Command(bin, "--host", host, "--port", port, "--user", "root", "--password", "dev",
				"--database", schema, "--table", table, "--alter", onlineAlter, "--allow-on-master", "--assume-rbr",
				"--initially-drop-ghost-table", "--initially-drop-old-table", "--ok-to-drop-table", "--cut-over=atomic",
				"--chunk-size=200", "--execute", "--verbose").CombinedOutput()
			if err != nil {
				t.Fatalf("gh-ost: %v\n%s", err, out)
			}
			return
		}
		t.Log("online DDL mode=simulated (gh-ost binary not available)")
		simulateGhost(t, src.DB, schema, table)
		return
	}
	if bin := lookTool("STRAND_E2E_PTOSC_BIN", "pt-online-schema-change"); bin != "" {
		t.Logf("online DDL mode=real binary=%s", bin)
		out, err := exec.Command(bin, "--alter", onlineAlter, fmt.Sprintf("h=%s,P=%s,u=root,p=dev,D=%s,t=%s", host, port, schema, table),
			"--execute", "--no-check-replication-filters", "--chunk-size=200", "--sleep=0.01", "--set-vars", "innodb_lock_wait_timeout=10").CombinedOutput()
		if err != nil {
			t.Fatalf("pt-online-schema-change: %v\n%s", err, out)
		}
		return
	}
	t.Log("online DDL mode=simulated (pt-online-schema-change binary not available)")
	simulatePtOSC(t, src.DB, schema, table)
}

// simulateGhost phát chuỗi SQL của gh-ost 1.1 (atomic cut-over).
func simulateGhost(t *testing.T, db *sql.DB, schema, table string) {
	q := func(n string) string { return fmt.Sprintf("`%s`.`%s`", schema, n) }
	orig, gho, ghc, del := q(table), q("_"+table+"_gho"), q("_"+table+"_ghc"), q("_"+table+"_del")
	mustExecDB(t, db,
		"create /* gh-ost */ table "+ghc+" (id bigint auto_increment, last_update timestamp not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, hint varchar(64) charset ascii not null, value varchar(4096) charset ascii not null, primary key(id), unique key hint_uidx(hint)) auto_increment=256 engine=InnoDB",
		"create /* gh-ost */ table "+gho+" like "+orig,
		"alter /* gh-ost */ table "+gho+" "+onlineAlter,
	)
	cols := "id, ver, v, cnt, junk"
	// Copy theo chunk (gh-ost: insert ignore ... select ... lock in share mode).
	for lo := 0; lo < 100000; lo += 200 {
		res, err := db.Exec(fmt.Sprintf("insert /* gh-ost */ ignore into %s (%s) (select %s from %s force index (`PRIMARY`) where id >= %d and id < %d lock in share mode)",
			gho, cols, cols, orig, lo, lo+200))
		if err != nil {
			t.Fatal(err)
		}
		if lo > 20000 {
			if n, _ := res.RowsAffected(); n == 0 {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Cut-over atomic: sentinel _del, LOCK TABLES, bắt kịp (thay cho áp binlog), RENAME ở phiên khác, drop sentinel, unlock.
	ctx := context.Background()
	c1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	ex := func(c *sql.Conn, s string) {
		if _, err := c.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	ex(c1, "create /* gh-ost */ table "+del+" (id int primary key) engine=InnoDB comment='magic-cut-over'")
	ex(c1, fmt.Sprintf("lock tables %s write, %s write, %s write", orig, gho, del))
	ex(c1, fmt.Sprintf("replace into %s (%s) select %s from %s", gho, cols, cols, orig))
	ex(c1, fmt.Sprintf("delete from %s where id not in (select id from %s)", gho, orig))
	done := make(chan error, 1)
	go func() {
		_, err := c2.ExecContext(ctx, fmt.Sprintf("rename /* gh-ost */ table %s to %s, %s to %s", orig, del, gho, orig))
		done <- err
	}()
	time.Sleep(500 * time.Millisecond)
	ex(c1, "drop /* gh-ost */ table "+del)
	ex(c1, "unlock tables")
	if err := <-done; err != nil {
		t.Fatalf("cut-over rename: %v", err)
	}
	mustExecDB(t, db, "drop /* gh-ost */ table if exists "+ghc, "drop /* gh-ost */ table if exists "+q("_"+table+"_del"))
}

// simulatePtOSC phát chuỗi SQL của pt-online-schema-change 3.x: _new + 3 trigger thật + copy theo chunk + RENAME.
func simulatePtOSC(t *testing.T, db *sql.DB, schema, table string) {
	q := func(n string) string { return fmt.Sprintf("`%s`.`%s`", schema, n) }
	orig, neu, old := q(table), q("_"+table+"_new"), q("_"+table+"_old")
	cols := "`id`, `ver`, `v`, `cnt`, `junk`"
	newVals := "NEW.`id`, NEW.`ver`, NEW.`v`, NEW.`cnt`, NEW.`junk`"
	tn := func(k string) string { return fmt.Sprintf("`%s`.`pt_osc_%s_%s_%s`", schema, schema, table, k) }
	mustExecDB(t, db,
		"CREATE TABLE "+neu+" LIKE "+orig,
		"ALTER TABLE "+neu+" "+onlineAlter,
		"CREATE TRIGGER "+tn("del")+" AFTER DELETE ON "+orig+" FOR EACH ROW DELETE IGNORE FROM "+neu+" WHERE "+neu+".`id` <=> OLD.`id`",
		"CREATE TRIGGER "+tn("upd")+" AFTER UPDATE ON "+orig+" FOR EACH ROW BEGIN DELETE IGNORE FROM "+neu+" WHERE !(OLD.`id` <=> NEW.`id`) AND "+neu+".`id` <=> OLD.`id`; REPLACE INTO "+neu+" ("+cols+") VALUES ("+newVals+"); END",
		"CREATE TRIGGER "+tn("ins")+" AFTER INSERT ON "+orig+" FOR EACH ROW REPLACE INTO "+neu+" ("+cols+") VALUES ("+newVals+")",
	)
	for lo := 0; lo < 100000; lo += 200 {
		res, err := db.Exec(fmt.Sprintf("INSERT LOW_PRIORITY IGNORE INTO %s (%s) SELECT %s FROM %s FORCE INDEX(`PRIMARY`) WHERE ((`id` >= %d)) AND ((`id` < %d)) LOCK IN SHARE MODE",
			neu, cols, cols, orig, lo, lo+200))
		if err != nil {
			t.Fatal(err)
		}
		if lo > 20000 {
			if n, _ := res.RowsAffected(); n == 0 {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	mustExecDB(t, db,
		"RENAME TABLE "+orig+" TO "+old+", "+neu+" TO "+orig,
		"DROP TABLE IF EXISTS "+old,
		"DROP TRIGGER IF EXISTS "+tn("del"), "DROP TRIGGER IF EXISTS "+tn("upd"), "DROP TRIGGER IF EXISTS "+tn("ins"),
	)
}

func logCount(p *strandProc, substr string) int { return strings.Count(p.output.String(), substr) }

func assertOnlineDDLLog(t *testing.T, p *strandProc) {
	t.Helper()
	if n := logCount(p, "online DDL cut-over detected"); n != 1 {
		t.Fatalf("cut-over detected %d times, want 1\n%s", n, p.tail())
	}
	if p.exited() {
		t.Fatalf("strand exited (code %d) during online DDL\n%s", p.exitCode(), p.tail())
	}
}

func startOnlineDDLTarget(t *testing.T, e *evoMySQLEnv) {
	t.Helper()
	e.dst.exec(t, "SET GLOBAL log_output='TABLE'", "TRUNCATE mysql.general_log", "SET GLOBAL general_log='ON'")
	t.Cleanup(func() { _, _ = e.dst.DB.Exec("SET GLOBAL general_log='OFF'") })
}

func countMySQLTargetAlters(t *testing.T, db *sql.DB, table string) (alters, tempMentions int) {
	t.Helper()
	pat := "%" + table + "%"
	if err := db.QueryRow("SELECT COUNT(*) FROM mysql.general_log WHERE command_type='Query' AND argument LIKE 'ALTER TABLE%' AND argument LIKE ?", pat).Scan(&alters); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM mysql.general_log WHERE command_type='Query' AND (argument LIKE ? OR argument LIKE ?) AND argument NOT LIKE 'SELECT%'",
		"%\\_"+table+"\\_%", "%pt\\_osc\\_%").Scan(&tempMentions); err != nil {
		t.Fatal(err)
	}
	return
}

func assertNoTempTables(t *testing.T, db *sql.DB, schema, table string) {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME LIKE ?", schema, "\\_"+table+"\\_%").Scan(&n); err != nil || n != 0 {
		t.Fatalf("temporary tables on target: n=%d err=%v", n, err)
	}
}

func onlineDDLMySQL(t *testing.T, tool string) {
	e := evoStartMySQL(t, "e2e-oddl-"+strings.ReplaceAll(tool, "-", ""), "")
	startOnlineDDLTarget(t, e)
	runOnlineDDLTool(t, e.src, tool, "app", "evo")
	deadline := time.Now().Add(2 * time.Minute)
	for mysqlColType(t, e.dst.DB, "app", "evo", "extra") == "" {
		mustStayRunning(t, e.p, tool)
		if time.Now().After(deadline) {
			t.Fatalf("ALTER not applied on target\n%s", e.p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	assertOnlineDDLLog(t, e.p)
	e.finish(t)
	alters, temps := countMySQLTargetAlters(t, e.dst.DB, "evo")
	if alters != 1 || temps != 0 {
		t.Fatalf("target received %d ALTER on evo (want 1) and %d statements touching temp tables (want 0)", alters, temps)
	}
	assertNoTempTables(t, e.dst.DB, "app", "evo")
}

func TestP3_E2E_OnlineDDL_GhOst_MySQL(t *testing.T) { onlineDDLMySQL(t, "gh-ost") }
func TestP3_E2E_OnlineDDL_PtOSC_MySQL(t *testing.T) { onlineDDLMySQL(t, "pt-osc") }

// Tắt nhận diện (tools: []): giữ hành vi cũ — pipeline không được lẳng lặng đi tiếp qua bảng tạm.
// Quan sát: không có dòng "online DDL cut-over detected".
func TestP3_E2E_OnlineDDL_Disabled_MySQL(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	createSchema(t, src, dst)
	src.exec(t, evoMySQLTable)
	evoPreload(t, src.DB, "app.evo", 200)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-oddl-off", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand", Include: []string{"app.evo"},
		Dir: testDir(t), SchemaOnlineDDL: "tools: []"}
	_ = seed
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)
	simulatePtOSC(t, src.DB, "app", "evo")
	time.Sleep(3 * time.Second)
	if logCount(p, "online DDL cut-over detected") != 0 {
		t.Fatalf("recognition must be off\n%s", p.tail())
	}
	p.stop(t)
}

// Chuỗi không khớp mẫu: gh-ost bị hủy giữa chừng (bảng _gho/_ghc bỏ dở, không có cut-over) — bảng gốc không đổi,
// bảng tạm không sang đích, 0 lệch; pipeline không dừng.
func TestP3_E2E_OnlineDDL_AbandonedGhost_MySQL(t *testing.T) {
	e := evoStartMySQL(t, "e2e-oddl-abandon", "")
	startOnlineDDLTarget(t, e)
	e.src.exec(t,
		"create /* gh-ost */ table app._evo_ghc (id bigint auto_increment, hint varchar(64) not null, primary key(id)) engine=InnoDB",
		"create /* gh-ost */ table app._evo_gho like app.evo",
		"alter /* gh-ost */ table app._evo_gho "+onlineAlter,
		"insert /* gh-ost */ ignore into app._evo_gho (id, ver, v, cnt, junk) (select id, ver, v, cnt, junk from app.evo where id < 400 lock in share mode)",
	)
	time.Sleep(3 * time.Second)
	mustStayRunning(t, e.p, "abandoned gh-ost")
	e.finish(t)
	if alters, temps := countMySQLTargetAlters(t, e.dst.DB, "evo"); alters != 0 || temps != 0 {
		t.Fatalf("target received %d ALTER, %d temp statements; want 0/0", alters, temps)
	}
	assertNoTempTables(t, e.dst.DB, "app", "evo")
	if logCount(e.p, "online DDL cut-over detected") != 0 {
		t.Fatal("no cut-over happened")
	}
}

// --------------------------------------------------------------------------
// MySQL → PostgreSQL
// --------------------------------------------------------------------------

func onlineDDLPG(t *testing.T, tool string) {
	e := evoStartPG(t, "e2e-oddl-pg-"+strings.ReplaceAll(tool, "-", ""))
	mustExecDB(t, e.dst.DB,
		"CREATE TABLE public.ddl_audit(tag text, obj text)",
		`CREATE FUNCTION public.ddl_audit_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP INSERT INTO public.ddl_audit VALUES (r.command_tag, r.object_identity); END LOOP; END $$`,
		"CREATE EVENT TRIGGER ddl_audit_trg ON ddl_command_end EXECUTE FUNCTION public.ddl_audit_fn()",
	)
	runOnlineDDLTool(t, e.src, tool, "evo", "t")
	deadline := time.Now().Add(2 * time.Minute)
	for pgColType(t, e.dst.DB, "evo", "t", "extra") == "" {
		mustStayRunning(t, e.p, tool)
		if time.Now().After(deadline) {
			t.Fatalf("ALTER not applied on PG target\n%s", e.p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	assertOnlineDDLLog(t, e.p)
	e.finish(t, "id", "ver", "v", "cnt", "extra")
	var alters, other int
	if err := e.dst.DB.QueryRow("SELECT count(*) FROM public.ddl_audit WHERE tag='ALTER TABLE' AND obj LIKE 'evo.t%'").Scan(&alters); err != nil {
		t.Fatal(err)
	}
	if err := e.dst.DB.QueryRow(`SELECT count(*) FROM public.ddl_audit WHERE obj LIKE '%\_t\_%'`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	if alters != 1 || other != 0 {
		t.Fatalf("PG target received %d ALTER TABLE on evo.t (want 1), %d DDL on temp objects (want 0)", alters, other)
	}
	var n int
	if err := e.dst.DB.QueryRow(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='evo' AND c.relname LIKE '\_t\_%'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("temporary tables on PG target: n=%d err=%v", n, err)
	}
}

func TestP3_E2E_OnlineDDL_GhOst_MySQLToPG(t *testing.T) { onlineDDLPG(t, "gh-ost") }
func TestP3_E2E_OnlineDDL_PtOSC_MySQLToPG(t *testing.T) { onlineDDLPG(t, "pt-osc") }
