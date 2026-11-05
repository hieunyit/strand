package e2e

import (
	"strings"
	"testing"
	"time"
)

// P1-15: verify sample/delta trên pipeline đang chạy dưới tải có hot row: không báo lệch giả;
// lệch cài vào target bị phát hiện (exit 5 verify_mismatch); --repair fix sửa được.
func TestP1_E2E_VerifySampleDeltaRepair(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 5000)
	src.exec(t, "INSERT INTO app.accounts VALUES (9000000001, 'COLD1', UUID_SHORT(), 1, 'cold', NULL, NOW(6))")
	wl.Start(6, 5000)

	cfg := strandCfg{Name: "e2e-verify", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t), VerifyDelta: "on"}
	path := cfg.write(t)
	p := startStrand(t, path)
	waitSynced(t, &cfg, p, 3*time.Minute)
	time.Sleep(3 * time.Second)

	// 1. Dưới tải (khóa nóng 1..10 cập nhật liên tục): không báo lệch giả.
	out, code := runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--table", "app.accounts", "--mode", "sample", "--rate", "1.0")
	if code != 0 {
		t.Fatalf("verify under load reported a mismatch (false positive?): exit %d\n%s", code, out)
	}
	t.Logf("under load: %s", strings.TrimSpace(out))
	out, code = runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--mode", "delta", "--socket", cfg.Socket)
	if code != 0 {
		t.Fatalf("verify delta under load: exit %d\n%s", code, out)
	}
	t.Logf("delta: %s", strings.TrimSpace(out))

	// 2. Cài lệch thật vào target (row không ai ghi): phải phát hiện.
	dst.exec(t, "UPDATE app.accounts SET note = 'CORRUPTED' WHERE id = 9000000001")
	out, code = runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--table", "app.accounts", "--mode", "sample", "--rate", "1.0")
	if code != 5 || !strings.Contains(out, "9000000001") {
		t.Fatalf("injected mismatch not detected: exit %d\n%s", code, out)
	}
	// 3. Sửa. --repair fix bị từ chối khi pipeline đang chạy (review R04: repair song song với stream
	// có thể ghi đè ảnh mới đã checkpoint bằng ảnh cũ). Quy trình: target bắt kịp → dừng pipeline → sửa →
	// chạy lại. Dừng tải trước: khi source vẫn ghi mà pipeline dừng, "lệch" chủ yếu là target tụt lại
	// (pipeline sẽ phát lại), và ảnh source đổi giữa lúc so và lúc sửa có thể va UNIQUE.
	out, code = runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--table", "app.accounts", "--mode", "sample", "--rate", "1.0", "--repair", "fix")
	if code != 4 || !strings.Contains(out, "pipeline_locked") {
		t.Fatalf("repair while the pipeline runs: exit %d, want 4 pipeline_locked\n%s", code, out)
	}
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("stop before repair: exit %d\n%s", code, p.tail())
	}
	out, code = runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--table", "app.accounts", "--mode", "sample", "--rate", "1.0", "--repair", "fix")
	if code != 0 {
		t.Fatalf("repair: exit %d\n%s", code, out)
	}
	p = startStrand(t, path)
	waitSynced(t, &cfg, p, 3*time.Minute)
	time.Sleep(3 * time.Second)
	out, code = runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--table", "app.accounts", "--mode", "sample", "--rate", "1.0")
	if code != 0 {
		t.Fatalf("after repair still mismatching: exit %d\n%s", code, out)
	}
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	p.stop(t)
	compareFinal(t, src, dst, appTables...)
}

// 02 §9 + chế độ C: --start-at <gtid> trên target có dữ liệu sai thì verify sau seed phát hiện,
// pipeline không chuyển sang synced mà dừng với exit 5 verify_mismatch.
func TestP1_E2E_StartAtVerifyRejectsBadTarget(t *testing.T) {
	src, dst := newPair(t)
	wl := newWorkload(src.DB, 1)
	wl.Preload(t, 3000)
	// Target "đã có dữ liệu" nhưng sai: chỉ có 1 row (thiếu gần hết) và row đó khác source.
	dst.exec(t, "INSERT INTO app.accounts SELECT * FROM (SELECT 1 AS id, 'X' AS code, 1 AS ver, 0 AS balance, NULL AS note, NULL AS payload, NOW(6) AS updated_at) z")
	g := strings.ReplaceAll(src.gtidExecuted(t), "\n", "")
	cfg := strandCfg{Name: "e2e-startat", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.accounts"}, Dir: testDir(t), Bootstrap: "start_at"}
	p := startStrand(t, cfg.write(t), "--start-at", g)
	select {
	case <-p.done:
	case <-time.After(5 * time.Minute):
		p.kill9()
		t.Fatalf("pipeline did not stop on verify mismatch")
	}
	if code := p.exitCode(); code != 5 || !strings.Contains(p.output.String(), "verify_mismatch") {
		t.Fatalf("exit %d, want 5 verify_mismatch\n%s", code, p.tail())
	}
	// Restart vẫn phải verify (trạng thái chờ verify giữ qua restart), không tự thành synced.
	p2 := startStrand(t, cfg.write(t))
	select {
	case <-p2.done:
	case <-time.After(5 * time.Minute):
		p2.kill9()
		t.Fatalf("restarted pipeline skipped post-seed verify")
	}
	if code := p2.exitCode(); code != 5 {
		t.Fatalf("restart: exit %d, want 5\n%s", code, p2.tail())
	}
}
