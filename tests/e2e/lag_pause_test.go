package e2e

import (
	"context"
	"testing"
	"time"

	"strand/internal/admin"
)

// topLag đọc state + lag_seconds của `strand top` qua admin socket.
func topLag(t *testing.T, socket string) (string, float64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	top, err := admin.NewClient(socket).Top(ctx)
	if err != nil {
		t.Fatalf("top: %v", err)
	}
	return top.State, top.LagSeconds
}

// TestE2E_LagGrowsWhilePausedUnderSourceLoad: lag phải đo cả phần source mà Strand chưa đọc. Khi `pause`
// (reader/router dừng) dưới tải ghi ở source, hàng đợi nội bộ rỗng nhưng source vẫn tiến: top phải báo
// lag > vài giây (trước đây luôn 0), rồi về ~0 sau resume + đuổi kịp.
func TestE2E_LagGrowsWhilePausedUnderSourceLoad(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 5000)
	wl.Start(4, 5000)
	defer wl.Stop()

	cfg := strandCfg{
		Name:      "e2e-lag-pause",
		SourceDSN: src.RootDSN,
		TargetDSN: dst.RootDSN + "strand",
		Include:   []string{"app.*"},
		Exclude:   []string{"app.ignored"},
		Dir:       testDir(t),
	}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 5*time.Minute)

	if out, code := runStrandCLI(t, 10*time.Second, "pause", "--socket", cfg.Socket); code != 0 {
		t.Fatalf("pause: exit %d: %s", code, out)
	}
	// Chờ Txn đang xử lý dở apply xong; sau đó nguồn vẫn ghi nên lag phải tăng dần theo thời gian pause.
	var lag float64
	deadline := time.Now().Add(60 * time.Second)
	for {
		state, l := topLag(t, cfg.Socket)
		lag = l
		if state != "paused" {
			t.Fatalf("state = %q, want paused", state)
		}
		if lag > 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lag_seconds = %.1f after 60s paused under source load, want > 5", lag)
		}
		time.Sleep(time.Second)
	}
	t.Logf("lag while paused: %.1fs", lag)

	if out, code := runStrandCLI(t, 10*time.Second, "resume", "--socket", cfg.Socket); code != 0 {
		t.Fatalf("resume: exit %d: %s", code, out)
	}
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)

	// Đã đuổi kịp và source không còn ghi: lag về ~0 (monitor thăm dò mỗi giây).
	deadline = time.Now().Add(30 * time.Second)
	for {
		state, l := topLag(t, cfg.Socket)
		if state == "synced" && l < 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after resume and catch-up: state=%s lag_seconds=%.1f, want synced and < 1", state, l)
		}
		time.Sleep(time.Second)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)
}
