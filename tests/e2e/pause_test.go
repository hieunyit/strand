package e2e

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"strand/internal/checkpoint"
)

// TestP1_E2E_PauseSnapshotKeepsStream kiểm tra `strand pause --snapshot` chỉ dừng phần
// snapshot logic trong khi stream CDC vẫn tiếp tục apply, và `strand resume --snapshot-only`
// cho phép snapshot chạy tiếp tới khi đồng bộ hoàn tất với 0 lệch.
func TestP1_E2E_PauseSnapshotKeepsStream(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 60000)
	wl.Start(4, 60000)
	defer wl.Stop()

	cfg := strandCfg{
		Name:       "e2e-pause-snap",
		SourceDSN:  src.RootDSN,
		TargetDSN:  dst.RootDSN + "strand",
		Include:    []string{"app.*"},
		Exclude:    []string{"app.ignored"},
		Dir:        testDir(t),
		SourceIOPS: 50,
	}
	p := startStrand(t, cfg.write(t))
	time.Sleep(3 * time.Second)

	if out, code := runStrandCLI(t, 10*time.Second, "pause", "--snapshot", "--socket", cfg.Socket); code != 0 {
		t.Fatalf("pause --snapshot: exit %d: %s", code, out)
	}
	// Chunk đang đọc dở lúc ra lệnh vẫn được flush xong rồi mới dừng ở pace(ctx): chờ tới khi
	// tiến độ đứng yên qua hai lần đo liên tiếp rồi mới lấy mốc.
	prev := snapshotProgress(t, cfg.Dir)
	for stable, deadline := 0, time.Now().Add(60*time.Second); stable < 2; {
		if time.Now().After(deadline) {
			t.Fatalf("snapshot did not stop after pause --snapshot")
		}
		time.Sleep(2 * time.Second)
		cur := snapshotProgress(t, cfg.Dir)
		if cur == prev {
			stable++
		} else {
			stable, prev = 0, cur
		}
	}

	meta, err := sql.Open("mysql", dst.RootDSN+"strand")
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()

	beforeProg := snapshotProgress(t, cfg.Dir)
	beforeSet, err := checkpoint.LoadAppliedSnapshotMySQL(context.Background(), meta, cfg.Name, "mysql")
	if err != nil {
		t.Fatalf("load applied before wait: %v", err)
	}
	beforeApplied := beforeSet.(*checkpoint.MySQLAppliedSet)

	time.Sleep(10 * time.Second)

	afterProg := snapshotProgress(t, cfg.Dir)
	if afterProg != beforeProg {
		t.Fatalf("snapshot progressed (%d -> %d) while paused with --snapshot", beforeProg, afterProg)
	}
	afterSet, err := checkpoint.LoadAppliedSnapshotMySQL(context.Background(), meta, cfg.Name, "mysql")
	if err != nil {
		t.Fatalf("load applied after wait: %v", err)
	}
	afterApplied := afterSet.(*checkpoint.MySQLAppliedSet)
	diff := afterApplied.Clone().(*checkpoint.MySQLAppliedSet)
	diff.Subtract(beforeApplied)
	if diff.String() == "" {
		t.Fatalf("stream applied set did not advance while snapshot was paused (before=%s, after=%s)",
			beforeApplied.String(), afterApplied.String())
	}
	t.Logf("snapshot held at progress %d while stream advanced by %s", beforeProg, diff.String())

	if out, code := runStrandCLI(t, 10*time.Second, "resume", "--snapshot-only", "--socket", cfg.Socket); code != 0 {
		t.Fatalf("resume --snapshot-only: exit %d: %s", code, out)
	}
	if out, code := runStrandCLI(t, 10*time.Second, "throttle", "set", "--source-iops", "100000", "--socket", cfg.Socket); code != 0 {
		t.Fatalf("throttle set: exit %d: %s", code, out)
	}

	waitSynced(t, &cfg, p, 5*time.Minute)
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)
}
