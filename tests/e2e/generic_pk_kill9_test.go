package e2e

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Việc C, binary thật: kill -9 lặp lại (50 ms – 2 s sau mỗi lần khởi động) trong lúc snapshot bảng PK
// (VARCHAR utf8mb4_bin, INT) có UNIQUE phụ `code` mà tải liên tục đổi giá trị — kịch bản 1062 khi chạy
// tiếp plan dở (row cũ ở phần chưa đọc giữ giá trị unique). Resume từ plan khóa tổng quát phải xóa phần
// chưa đọc theo tuple; cuối: verify full 0 lệch, dump giống hệt.
func TestC_E2E_GenericPKKill9DuringSnapshot(t *testing.T) {
	seed := seedFromEnv(t)
	iterations := envInt("STRAND_E2E_GPK_KILL9_ITERATIONS", 60)
	rng := rand.New(rand.NewSource(seed))
	src, dst := newPair(t)
	src.exec(t, "CREATE TABLE app.gk_u (k VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, n INT NOT NULL, code VARCHAR(12) NOT NULL, v INT, PRIMARY KEY (k, n), UNIQUE KEY uk_code (code))")
	key := func(r *rand.Rand) (string, int) { return fmt.Sprintf("k%04d", r.Intn(3000)), r.Intn(3) }
	for i := 0; i < 6000; i++ {
		k, n := key(rng)
		_, _ = src.DB.Exec("INSERT IGNORE INTO app.gk_u VALUES (?, ?, ?, ?)", k, n, fmt.Sprintf("c%06d", i), i)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops atomic.Int64
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(s int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(s))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k, n := key(r)
				var err error
				switch r.Intn(5) {
				case 0:
					_, err = src.DB.Exec("INSERT IGNORE INTO app.gk_u VALUES (?, ?, ?, 1)", k, n, fmt.Sprintf("c%06d", 6000+r.Intn(60000)))
				case 1:
					_, err = src.DB.Exec("DELETE FROM app.gk_u WHERE k = ? AND n = ?", k, n)
				case 2: // đổi giá trị UNIQUE (giải phóng code cũ)
					_, err = src.DB.Exec("UPDATE IGNORE app.gk_u SET code = ? WHERE k = ? AND n = ?", fmt.Sprintf("c%06d", r.Intn(66000)), k, n)
				case 3: // đổi PK
					k2, n2 := key(r)
					_, err = src.DB.Exec("UPDATE IGNORE app.gk_u SET k = ?, n = ? WHERE k = ? AND n = ?", k2, n2, k, n)
				default:
					_, err = src.DB.Exec("UPDATE app.gk_u SET v = v + 1 WHERE k = ? AND n = ?", k, n)
				}
				if err == nil {
					ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}(seed + int64(w))
	}
	cfg := strandCfg{Name: "e2e-gpk-kill9", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.gk_u"}, Dir: testDir(t), CreateSchema: "auto", ChunkRows: "40"}
	path := cfg.write(t)
	for i := 1; i <= iterations; i++ {
		p := startStrand(t, path)
		wait := time.Duration(50+rng.Intn(1950)) * time.Millisecond
		select {
		case <-p.done:
			t.Fatalf("iteration %d: strand exited by itself with code %d before kill\n%s", i, p.exitCode(), p.tail())
		case <-time.After(wait):
		}
		p.kill9()
	}
	close(stop)
	wg.Wait()
	p := startStrand(t, path)
	waitSynced(t, &cfg, p, 10*time.Minute)
	waitCaughtUp(t, src, dst, &cfg, p, 10*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("final run exit %d\n%s", code, p.tail())
	}
	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", "app.gk_u", "--mode", "full")
	if code != 0 {
		t.Fatalf("verify --mode full: exit %d\n%s", code, out)
	}
	if a, b := dumpE2E(t, src, "app.gk_u"), dumpE2E(t, dst, "app.gk_u"); a != b {
		t.Fatalf("app.gk_u differs between source and target (%d vs %d rows)", strings.Count(a, "\n")+1, strings.Count(b, "\n")+1)
	}
	t.Logf("PASS: %d kill -9 during the generic-key snapshot, %d ops, %s", iterations, ops.Load(), strings.TrimSpace(out))
}
