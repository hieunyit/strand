package e2e

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// Phase 2 (01 P2): PG→PG chạy end-to-end. Target bắt đầu bằng đúng dữ liệu của source tại
// consistent_point của slot; Strand stream từ đó (--start-at <LSN>) trong khi source chịu tải:
// insert/update/delete, cột TOAST > 2 KB với UPDATE không chạm nó, UNIQUE ngoài PK hoán đổi trong
// một Txn, đổi PK, Txn lớn bị stream (> logical_decoding_work_mem, kể cả tràn đĩa ở assembler),
// PREPARE TRANSACTION + COMMIT/ROLLBACK PREPARED. Kết quả: từng row giống hệt, cột TOAST không
// bao giờ NULL, không phiên bản row nào bị apply hai lần, slot tiến theo lwd bền vững.
func TestP2_E2E_PGToPGStreamUnderLoad(t *testing.T) {
	for _, order := range []string{"relaxed", "strict"} {
		t.Run(order, func(t *testing.T) {
			seed := seedFromEnv(t)
			slot, pub := "strand_e2e_"+order, "strand_e2e_pub"
			src, dst, lsn := setupPGPair(t, slot, pub)

			wl := newPGWorkload(src.DB, seed)
			wl.Start(4, 3000)
			time.Sleep(2 * time.Second) // có tải trước khi Strand bắt đầu: phần đầu stream là backlog

			cfg := strandCfg{Name: "e2e-pg-" + order, SourceKind: "postgres", TargetKind: "postgres",
				SourceDSN: src.DSN, TargetDSN: dst.DSN, CommitOrder: order,
				SourceExtra: fmt.Sprintf(", slot: %s, publication: %s, sequence_sync_interval: 1s", slot, pub),
				Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
			cfgPath := cfg.write(t)
			vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
			p := startStrand(t, cfgPath, "--start-at", lsn)

			load := time.Duration(envInt("STRAND_E2E_PG_LOAD_SECONDS", 30)) * time.Second
			time.Sleep(load / 2)
			// Txn rất lớn giữa chừng: server stream nó, assembler tràn đĩa (> 10.000 row).
			if err := wl.BigTxn(15000); err != nil {
				t.Fatalf("big txn: %v", err)
			}
			time.Sleep(load / 2)
			wl.Stop()
			t.Logf("workload: %d ops, %d client-side errors, %d two-phase txns, %d big txns",
				wl.ops.Load(), wl.errs.Load(), wl.prepared.Load(), wl.bigTxns.Load())
			if wl.prepared.Load() == 0 || wl.bigTxns.Load() < 2 {
				t.Fatalf("workload did not exercise two-phase / big transactions")
			}

			waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
			// Slot tiến theo lwd bền vững (không đứng yên ở consistent_point, không vượt lwd).
			deadline := time.Now().Add(30 * time.Second)
			start, _ := parseLSNTest(lsn)
			for slotConfirmed(t, src, slot) <= start {
				if time.Now().After(deadline) {
					t.Fatalf("slot confirmed_flush_lsn did not advance past %s", lsn)
				}
				time.Sleep(500 * time.Millisecond)
			}
			if code := p.stop(t); code != 0 {
				t.Fatalf("strand exit code %d\n%s", code, p.tail())
			}
			vs.check(t)
			comparePG(t, src, dst, pgAppTables...)
			checkPGInvariants(t, dst)

			// Sequence độc lập được đồng bộ định kỳ vào _strand_promote (02 §8, Δt_sync).
			var synced int64
			if err := dst.DB.QueryRow(`SELECT coalesce(max((s->>'last_synced_value')::bigint), 0)
				FROM _strand_promote, jsonb_array_elements(sequences) s WHERE pipeline_id = $1`, cfg.Name).Scan(&synced); err != nil {
				t.Fatalf("read _strand_promote: %v", err)
			}
			if synced < 1000000 {
				t.Fatalf("independent sequence last_value not synced during sync (got %d)", synced)
			}
			t.Logf("sequence app.ver_seq last synced value %d", synced)
		})
	}
}

// waitPGInitialPositionDurable chờ checkpoint khởi tạo được ghi bền vững vào _strand_checkpoint (02 §4).
func waitPGInitialPositionDurable(t *testing.T, dst *pgNode, pipelineID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var applied string
		err := dst.DB.QueryRow("SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1", pipelineID).Scan(&applied)
		if err == nil && applied != "" {
			return applied
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial position not durable in _strand_checkpoint after %s (err=%v, applied=%q)", timeout, err, applied)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitPGPipelineReady kiểm tra readiness chuẩn: initial position durable trong _strand_checkpoint VÀ trạng thái
// pipeline running được ghi nhận (thay cho blind sleep).
func waitPGPipelineReady(t *testing.T, dst *pgNode, p *strandProc, pipelineID string, timeout time.Duration) string {
	t.Helper()
	waitStrandLog(t, p, `"msg":"pipeline running"`, timeout)
	return waitPGInitialPositionDurable(t, dst, pipelineID, timeout)
}

// Phase 2: kill -9 Strand N lần (mặc định 20, STRAND_E2E_PG_KILL9_ITERATIONS) khi source PG chịu
// tải liên tục → 0 lệch, không phiên bản row nào bị apply hai lần (CP-1, CP-2), ver không giảm.
// Kiểm tra rõ ranh giới bền vững (durable-start milestone): kill trước và sau mốc ghi checkpoint ban đầu
// để xác nhận hành vi resume theo tài liệu hoặc từ chối dứt khoát (explicit refusal).
func TestP2_E2E_PGKill9(t *testing.T) {
	seed := seedFromEnv(t)
	iterations := envInt("STRAND_E2E_PG_KILL9_ITERATIONS", 20)
	rng := rand.New(rand.NewSource(seed))
	slot, pub := "strand_e2e_kill9", "strand_e2e_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)

	wl := newPGWorkload(src.DB, seed)
	wl.Start(4, 3000)

	cfg := strandCfg{Name: "e2e-pg-kill9", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	cfgPath := cfg.write(t)
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")

	// --- Milestone 1: Fault inject kill TRƯỚC mốc durable start ---
	// Pipeline mới với bootstrap=start_at: nếu bị kill trước khi checkpoint bền, lần khởi động sau
	// không có --start-at phải bị TỪ CHỐI DỨT KHOÁT (explicit refusal) vì pipeline chưa có vị trí bền.
	pPre := startStrand(t, cfgPath, "--start-at", lsn)
	pPre.kill9()                        // kill ngay lập tức trước khi pipeline running / durable checkpoint hoàn tất
	pRefused := startStrand(t, cfgPath) // khởi động thiếu --start-at khi chưa có checkpoint bền
	select {
	case <-pRefused.done:
		if pRefused.exitCode() == 0 {
			t.Fatalf("expected refusal when restarting without --start-at before durable checkpoint, but exited 0\n%s", pRefused.tail())
		}
		if !strings.Contains(pRefused.tail(), "requires --start-at") && !strings.Contains(pRefused.tail(), "bootstrap.default=start_at") {
			t.Logf("refusal logged: %s", pRefused.tail())
		}
	case <-time.After(30 * time.Second):
		pRefused.kill9()
		t.Fatalf("strand did not exit with refusal within 30s\n%s", pRefused.tail())
	}

	// --- Milestone 2: Khởi động với --start-at và chờ đạt mốc durable start ---
	// Readiness thực thụ: durable checkpoint trong _strand_checkpoint + trạng thái pipeline running.
	pInit := startStrand(t, cfgPath, "--start-at", lsn)
	durableLSN := waitPGPipelineReady(t, dst, pInit, cfg.Name, time.Minute)
	t.Logf("durable start milestone reached: applied=%s", durableLSN)

	// Fault inject kill SAU mốc durable start:
	pInit.kill9()

	// Khởi động lại với --start-at khi checkpoint ĐÃ TỒN TẠI phải bị TỪ CHỐI DỨT KHOÁT (02 §10):
	pRefusedReStartAt := startStrand(t, cfgPath, "--start-at", lsn)
	select {
	case <-pRefusedReStartAt.done:
		if pRefusedReStartAt.exitCode() == 0 {
			t.Fatalf("expected refusal when passing --start-at to an existing checkpoint, but exited 0\n%s", pRefusedReStartAt.tail())
		}
		if !strings.Contains(pRefusedReStartAt.tail(), "checkpoint already exists") && !strings.Contains(pRefusedReStartAt.tail(), "only valid for a new pipeline") {
			t.Logf("re-start-at refusal logged: %s", pRefusedReStartAt.tail())
		}
	case <-time.After(30 * time.Second):
		pRefusedReStartAt.kill9()
		t.Fatalf("strand did not exit with refusal for existing checkpoint within 30s\n%s", pRefusedReStartAt.tail())
	}

	// Documented resume: Khởi động lại không cần --start-at -> tiếp tục thành công từ durable checkpoint.
	pResume := startStrand(t, cfgPath)
	waitPGPipelineReady(t, dst, pResume, cfg.Name, time.Minute)
	pResume.kill9()

	// --- Vòng lặp N lần kill -9 dưới tải liên tục ---
	start := time.Now()
	for i := 1; i <= iterations; i++ {
		p := startStrand(t, cfgPath)
		// Readiness chuẩn: không dùng sleep mù quáng; chờ checkpoint bền + pipeline running
		waitPGPipelineReady(t, dst, p, cfg.Name, time.Minute)

		wait := time.Duration(100+rng.Intn(1000)) * time.Millisecond
		select {
		case <-p.done:
			t.Fatalf("iteration %d: strand exited by itself with code %d before kill\n%s", i, p.exitCode(), p.tail())
		case <-time.After(wait):
		}
		p.kill9()
		if i%5 == 0 {
			t.Logf("kill -9 iteration %d/%d (elapsed %s, workload ops %d)", i, iterations, time.Since(start).Round(time.Second), wl.ops.Load())
		}
		if i == iterations/2 {
			if err := wl.BigTxn(12000); err != nil {
				t.Fatalf("big txn: %v", err)
			}
		}
	}
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors, %d two-phase txns, %d big txns",
		wl.ops.Load(), wl.errs.Load(), wl.prepared.Load(), wl.bigTxns.Load())

	p := startStrand(t, cfgPath)
	waitPGCaughtUp(t, src, dst, &cfg, p, 10*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("final run exit code %d\n%s", code, p.tail())
	}
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)
	t.Logf("PASS: %d kill -9, 0 deviation, total %s", iterations, time.Since(start).Round(time.Second))
}

func parseLSNTest(s string) (uint64, error) {
	var hi, lo uint64
	if _, err := fmt.Sscanf(s, "%X/%X", &hi, &lo); err != nil {
		return 0, err
	}
	return hi<<32 | lo, nil
}

// Phase 2 (02 §8): partial UPDATE (cột TOAST không đổi) mà row không có ở target → Strand không
// dựng lại được row nên dừng với verify_mismatch (exit 5), log chỉ ra bảng và khóa cần verify/sửa;
// không ghi gì sai (doc không bao giờ thành NULL).
func TestP2_E2E_PGToastRowMissingStopsWithVerifyMismatch(t *testing.T) {
	slot, pub := "strand_e2e_toast", "strand_e2e_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	cfg := strandCfg{Name: "e2e-pg-toast", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	p := startStrand(t, cfg.write(t), "--start-at", lsn)
	waitSynced(t, &cfg, p, time.Minute)

	dst.exec(t, "DELETE FROM app.items WHERE id = 42")
	src.exec(t, "UPDATE app.items SET qty = qty + 1, ver = nextval('app.ver_seq') WHERE id = 42") // doc không đổi → 'u'
	select {
	case <-p.done:
	case <-time.After(2 * time.Minute):
		p.kill9()
		t.Fatalf("strand did not stop on a partial update of a missing row\n%s", p.tail())
	}
	if code := p.exitCode(); code != 5 {
		t.Fatalf("exit code %d, want 5 (verify_mismatch)\n%s", code, p.tail())
	}
	out := p.output.String()
	if !strings.Contains(out, "toast row missing") || !strings.Contains(out, "app.items") || !strings.Contains(out, "42") {
		t.Fatalf("log does not name the table/key to verify:\n%s", p.tail())
	}
	var n int
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.items WHERE id = 42 OR doc IS NULL").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("writer must not write a partial row (found %d)", n)
	}
}
