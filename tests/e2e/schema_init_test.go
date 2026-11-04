package e2e

import (
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// invariant: CP-1/2, RS-1/2, CO-1 — bootstrap dưới tải vào schema đích chưa có;
// CREATE trong stream, restart, đối chiếu từng row và verify full đều phải qua.
func TestSchemaInit_E2E_EmptyTarget(t *testing.T) {
	seedFromEnv(t)
	for _, direction := range []string{"mysql-mysql", "mysql-postgres", "postgres-postgres"} {
		t.Run(direction, func(t *testing.T) {
			kinds := strings.Split(direction, "-")
			var srcDB, dstDB *sql.DB
			var srcMy, dstMy *mysqlNode
			var srcPG, dstPG *pgNode
			cfg := strandCfg{Name: "schema-init", SourceKind: kinds[0], TargetKind: kinds[1], Include: []string{"app.*"}, Dir: testDir(t), SourceIOPS: envInt("STRAND_SCHEMA_TEST_IOPS", 1000), LagSLO: "10m"}
			if kinds[0] == "mysql" {
				srcMy = startMySQL(t, mysqlImage("source"), "src", 1, nil)
				srcDB = srcMy.DB
				cfg.SourceDSN = srcMy.RootDSN
			} else {
				srcPG = startPG(t, "src")
				srcDB = srcPG.DB
				cfg.SourceDSN = srcPG.DSN
				cfg.SourceExtra = ", slot: schema_init_slot, publication: schema_init_pub, create_slot: true"
			}
			if kinds[1] == "mysql" {
				dstMy = startMySQL(t, mysqlImage("target"), "dst", 2, nil)
				dstMy.exec(t, "CREATE DATABASE strand")
				dstDB = dstMy.DB
				cfg.TargetDSN = dstMy.RootDSN + "strand"
			} else {
				dstPG = startPG(t, "dst")
				dstDB = dstPG.DB
				cfg.TargetDSN = dstPG.DSN
			}
			run := func(db *sql.DB, q string) {
				t.Helper()
				if _, err := db.Exec(q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			if kinds[0] == "mysql" {
				run(srcDB, "CREATE DATABASE app")
			} else {
				run(srcDB, "CREATE SCHEMA app")
			}
			run(srcDB, "CREATE TABLE app.items (id bigint PRIMARY KEY, v varchar(100) NOT NULL, ver bigint NOT NULL)")
			for batch := 0; batch < 8; batch++ {
				var values []string
				for j := 1; j <= 500; j++ {
					values = append(values, fmt.Sprintf("(%d,'initial',1)", batch*500+j))
				}
				run(srcDB, "INSERT INTO app.items VALUES "+strings.Join(values, ","))
			}
			run(srcDB, "CREATE TABLE app.children (id bigint PRIMARY KEY, v varchar(100) NOT NULL, ver bigint NOT NULL, pid bigint, FOREIGN KEY (pid) REFERENCES app.items(id))")
			run(srcDB, "INSERT INTO app.children VALUES (1,'child',1,1)")
			if srcPG != nil {
				run(srcDB, "CREATE TABLE app.marker (id bigint PRIMARY KEY, note text)")
				run(srcDB, "CREATE PUBLICATION schema_init_pub FOR ALL TABLES")
			}
			path := cfg.write(t)
			out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
			if code != 0 || !strings.Contains(out, "table(s) will be created") {
				t.Fatalf("validate code=%d\n%s", code, out)
			}
			out, code = runStrandCLI(t, time.Minute, "schema", "init", "--dry-run", "-c", path)
			if code != 0 || !strings.Contains(out, "CREATE TABLE") {
				t.Fatalf("dry-run code=%d\n%s", code, out)
			}
			var exists int
			q := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='app' AND table_name='items'"
			if err := dstDB.QueryRow(q).Scan(&exists); err != nil || exists != 0 {
				t.Fatalf("dry-run mutated target: exists=%d err=%v", exists, err)
			}
			var stopped atomic.Bool
			var ops atomic.Int64
			loadErr := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for !stopped.Load() {
					<-ticker.C
					if _, err := srcDB.Exec("UPDATE app.items SET ver=ver+1, v='changed' WHERE id=1"); err != nil {
						loadErr <- err
						return
					}
					ops.Add(1)
				}
			}()
			t.Cleanup(func() { stopped.Store(true); <-done })
			p := startStrand(t, path)
			t.Cleanup(func() {
				if t.Failed() && !p.exited() {
					if err := p.cmd.Process.Signal(syscall.SIGQUIT); err != nil {
						t.Logf("goroutine dump: %v", err)
					}
					select {
					case <-p.done:
					case <-time.After(2 * time.Second):
					}
					t.Logf("goroutine dump in %s", p.log)
				}
				if !p.exited() {
					p.kill9()
				}
			})
			// Tiền đề: tải đã commit khi bootstrap còn đang chạy, không suy từ sleep cố định.
			deadline := time.Now().Add(2 * time.Minute)
			overlapBase := int64(-1)
			for !(overlapBase >= 0 && ops.Load() > overlapBase && !strings.Contains(p.output.String(), "bootstrap finished")) {
				if overlapBase < 0 && strings.Contains(p.output.String(), "snapshot start") {
					overlapBase = ops.Load()
				}
				if p.exited() || pipelineSynced(cfg.Socket) || time.Now().After(deadline) {
					t.Fatalf("no load overlapping snapshot: ops=%d\n%s", ops.Load(), p.tail())
				}
				time.Sleep(10 * time.Millisecond)
			}
			waitSynced(t, &cfg, p, 4*time.Minute)
			stopped.Store(true)
			<-done
			select {
			case err := <-loadErr:
				t.Fatal(err)
			default:
			}
			t.Logf("load commits=%d", ops.Load())
			if srcMy != nil {
				run(srcDB, "CREATE TABLE app.new_table (id bigint PRIMARY KEY, v varchar(100) NOT NULL, ver bigint NOT NULL)")
				run(srcDB, "INSERT INTO app.new_table VALUES (1,'new',1)")
			}
			switch direction {
			case "mysql-mysql":
				waitCaughtUp(t, srcMy, dstMy, &cfg, p, 2*time.Minute)
			case "mysql-postgres":
				waitMySQLPGCaughtUp(t, srcMy, dstPG, &cfg, p, 2*time.Minute)
			default:
				waitPGCaughtUp(t, srcPG, dstPG, &cfg, p, 2*time.Minute)
			}
			tables := []string{"app.items", "app.children"}
			if srcMy != nil {
				tables = append(tables, "app.new_table")
			}
			for _, table := range tables {
				read := func(db *sql.DB) []string {
					t.Helper()
					rows, err := db.Query("SELECT id,v,ver FROM " + table + " ORDER BY id")
					if err != nil {
						t.Fatal(err)
					}
					defer rows.Close()
					var got []string
					for rows.Next() {
						var id, ver int64
						var v string
						if err := rows.Scan(&id, &v, &ver); err != nil {
							t.Fatal(err)
						}
						got = append(got, fmt.Sprintf("%d:%s:%d", id, v, ver))
					}
					if err := rows.Err(); err != nil {
						t.Fatal(err)
					}
					return got
				}
				a, b := read(srcDB), read(dstDB)
				if strings.Join(a, "\n") != strings.Join(b, "\n") {
					t.Fatalf("%s differs: source=%d target=%d", table, len(a), len(b))
				}
				out, code := runStrandCLI(t, 2*time.Minute, "verify", "--mode", "full", "--table", table, "-c", path)
				if code != 0 {
					t.Fatalf("verify code=%d\n%s", code, out)
				}
				t.Logf("%s rows=%d verify=%s", table, len(a), out)
			}
			if code := p.stop(t); code != 0 {
				t.Fatalf("stop code=%d\n%s", code, p.tail())
			}
			p = startStrand(t, path)
			waitSynced(t, &cfg, p, 2*time.Minute)
			if srcMy != nil {
				// Khoá process sau khi synced để CREATE và ALTER đều có trong binlog trước
				// khi router xử lý CREATE; không dựa vào race hay sleep.
				if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
					t.Fatal(err)
				}
				run(srcDB, "CREATE TABLE app.future_table (id bigint PRIMARY KEY)")
				run(srcDB, "ALTER TABLE app.future_table ADD COLUMN future_value int")
				if err := p.cmd.Process.Signal(syscall.SIGCONT); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(time.Minute)
				for !p.exited() {
					if time.Now().After(deadline) {
						t.Fatalf("future DDL was not rejected\n%s", p.tail())
					}
					time.Sleep(20 * time.Millisecond)
				}
				if code := p.exitCode(); code != 10 || !strings.Contains(p.output.String(), "later DDL mentions app.future_table") {
					t.Fatalf("future DDL code=%d\n%s", code, p.tail())
				}
				if err := dstDB.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='app' AND table_name='future_table'").Scan(&exists); err != nil || exists != 0 {
					t.Fatalf("future schema was created: exists=%d err=%v", exists, err)
				}
			} else if code := p.stop(t); code != 0 {
				t.Fatalf("restart code=%d\n%s", code, p.tail())
			}
		})
	}
}
