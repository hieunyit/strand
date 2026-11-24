package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

// The admin socket must answer while auto-seed copies: the copy can take hours
// before the rest of the pipeline is built, and a silent socket makes the web
// console report the running process as unreachable.
func TestMDB_E2E_AutoSeedAdminSocketReportsProgress(t *testing.T) {
	src := startMariaDBImage(t, mdbImage(), "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.payload (payload MEDIUMBLOB NOT NULL) ENGINE=InnoDB")
	for i := 0; i < 8; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.payload VALUES (REPEAT('%c', 524288))", 'a'+i))
	}
	cfg := strandCfg{
		Name: "mdb-auto-seed-admin", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
		Include:   []string{"app.*"}, Dir: testDir(t), AutoSeed: true, TablesExtra: klKeylessCfg,
		SourceIOPS: 10, // 160 KiB/s: the 4 MiB copy takes about 25 s
	}
	path := cfg.write(t)
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()

	var answered, sawRows bool
	deadline := time.Now().Add(90 * time.Second)
	for !strings.Contains(p.output.String(), "auto-seed committed") {
		if time.Now().After(deadline) || p.exited() {
			t.Fatalf("auto-seed did not commit\n%s", p.tail())
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c := admin.NewClient(cfg.socket(t))
		top, err := c.Top(ctx)
		if err == nil && top.State == "bootstrapping" && top.SnapshotTable == "app.payload" {
			if top.TablesTotal != 1 || top.TablesDone != 0 {
				cancel()
				t.Fatalf("auto-seed progress: %d/%d tables, want 0/1", top.TablesDone, top.TablesTotal)
			}
			status, err := c.Status(ctx)
			if err != nil || status.State != "bootstrapping" {
				cancel()
				t.Fatalf("status during auto-seed: %+v err=%v", status, err)
			}
			answered = true
			sawRows = sawRows || top.SnapshotRows > 0
		}
		cancel()
		time.Sleep(200 * time.Millisecond)
	}
	if !answered || !sawRows {
		t.Fatalf("admin socket during auto-seed: answered=%v rows reported=%v\n%s", answered, sawRows, p.tail())
	}
	waitSynced(t, &cfg, p, time.Minute)
	ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, "SELECT * FROM app.payload") })
}
