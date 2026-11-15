package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Hồi quy: bảng thêm bằng `snapshot add` (không nằm trong tables.include) phải còn trong pipeline sau khi
// restart `strand sync`. Trước đây nó âm thầm rời pipeline: source_tables báo in_pipeline=false và INSERT mới
// ở source không bao giờ tới target.
func TestSnapshotAddPersist_E2E_MySQLSurvivesRestart(t *testing.T) {
	src, dst := newPair(t)
	for i := 1; i <= 500; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.events VALUES (%d, UUID_SHORT(), 'pre')", i))
	}
	cfg := strandCfg{Name: "e2e-snapadd-persist", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.accounts", "app.orders"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p, 3*time.Minute)

	out, code := runStrandCLI(t, 30*time.Second, "snapshot", "add", "--table", "app.events", "--socket", cfg.Socket)
	if code != 0 {
		t.Fatalf("snapshot add: exit %d: %s", code, out)
	}
	waitStrandLog(t, p, `"snapshot done"`, 2*time.Minute)
	waitSynced(t, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}

	p2 := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p2, 3*time.Minute)
	for i := 1001; i <= 1100; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.events VALUES (%d, UUID_SHORT(), 'post')", i))
	}
	waitCaughtUp(t, src, dst, &cfg, p2, 3*time.Minute)
	if code := p2.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p2.tail())
	}
	if strings.Contains(p2.output.String(), "snapshot start") {
		t.Fatalf("restart re-snapshotted a table that was already done\n%s", p2.tail())
	}
	compareFinal(t, src, dst, "app.events")
}

// Như trên cho PG→PG (bảng đã nằm trong publication).
func TestSnapshotAddPersist_E2E_PGSurvivesRestart(t *testing.T) {
	slot, pub := "strand_e2e_sapersist", "strand_e2e_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	src.exec(t, "INSERT INTO app.events SELECT g, nextval('app.ver_seq'), 'pre' FROM generate_series(3001, 4000) g")
	cfg := strandCfg{Name: "e2e-pg-sapersist", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.items", "app.accounts", "app.marker"}, Dir: testDir(t), Bootstrap: "start_at", SourceIOPS: 20000}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath, "--start-at", lsn)
	waitSynced(t, &cfg, p, time.Minute)

	out, code := runStrandCLI(t, 30*time.Second, "snapshot", "add", "--table", "app.events", "--socket", cfg.Socket)
	if code != 0 {
		t.Fatalf("snapshot add: exit %d: %s", code, out)
	}
	waitStrandLog(t, p, `"snapshot done"`, 2*time.Minute)
	waitSynced(t, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}

	p2 := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p2, time.Minute)
	src.exec(t, "INSERT INTO app.events SELECT g, nextval('app.ver_seq'), 'post' FROM generate_series(6001, 6100) g")
	waitPGCaughtUp(t, src, dst, &cfg, p2, 3*time.Minute)
	if code := p2.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p2.tail())
	}
	if strings.Contains(p2.output.String(), "snapshot start") {
		t.Fatalf("restart re-snapshotted a table that was already done\n%s", p2.tail())
	}
	comparePG(t, src, dst, pgAppTables...)
}
