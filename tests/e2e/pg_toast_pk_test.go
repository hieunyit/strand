package e2e

// Phase 2 (02 §8, P2-03): đổi PK khi cột TOAST không đổi trong lúc snapshot logic PG chạy song song
// với stream.

import (
	"database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pgToastPKMover đổi PK của app.items (cột doc > 2 KB lưu ngoài dòng, KHÔNG đổi → pgoutput gửi 'u'):
// khóa đích ngẫu nhiên trong [1, 2·span] (≥ span là sau b_last), nên PK cũ và PK mới thường nằm ở
// hai range khác trạng thái ("Xong", "Đang đọc", "Chưa đọc"). Thỉnh thoảng chuyển qua rồi chuyển lại
// trong hai Txn liền nhau.
func pgToastPKMover(db *sql.DB, seed, span int64, stop *atomic.Bool, wg *sync.WaitGroup, moves *atomic.Int64) {
	defer wg.Done()
	r := rand.New(rand.NewSource(seed))
	move := func(from, to int64) bool {
		res, err := db.Exec(`UPDATE app.items SET id = $1, ver = nextval('app.ver_seq'), qty = qty + 1 WHERE id = $2`, to, from)
		if err != nil {
			return false
		}
		n, _ := res.RowsAffected()
		if n == 1 {
			moves.Add(1)
		}
		return n == 1
	}
	for !stop.Load() {
		from, to := 1+r.Int63n(span*2), 1+r.Int63n(span*2)
		if move(from, to) && r.Intn(4) == 0 {
			move(to, from) // đổi qua đổi lại
		}
	}
}

var rereadKeysRe = regexp.MustCompile(`"msg":"snapshot done"[^\n]*"table":"app.items"[^\n]*"reread_keys":(\d+)`)

// Phase 2 (02 §8): bootstrap logic PG→PG dưới tải; app.items (doc > 2 KB, STORAGE EXTERNAL) bị đổi PK
// liên tục mà doc không đổi, xuyên các range đang ở mọi trạng thái. Lặp STRAND_E2E_PG_TOAST_PK_ITERATIONS
// lần (mặc định 3), mỗi lần cặp container mới: trạng thái cuối giống hệt (gồm doc, không bao giờ NULL),
// mọi phiên bản row được ghi đúng một lần, ver không giảm; Strand không dừng.
func TestP2_E2E_PGLogicalSnapshotToastPKMove(t *testing.T) {
	seed := seedFromEnv(t)
	n := envInt("STRAND_E2E_PG_TOAST_PK_ITERATIONS", 3)
	rereads := 0
	for i := 1; i <= n; i++ {
		t.Run(fmt.Sprintf("run%d", i), func(t *testing.T) { rereads += runPGToastPKMove(t, seed+int64(i)*104729) })
	}
	if rereads == 0 {
		t.Fatal("no key was ever re-read: the PK-change-with-unchanged-TOAST path across ranges was not exercised")
	}
}

func runPGToastPKMove(t *testing.T, seed int64) int {
	const span = 12000
	slot, pub := "strand_e2e_toastpk", "strand_e2e_pub"
	src, dst := setupPGLogical(t, pub, span, span/2, span)

	wl := newPGWorkload(src.DB, seed)
	wl.Start(2, span)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var moves, evMoves atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go pgToastPKMover(src.DB, seed+int64(i), span, &stop, &wg, &moves)
	}
	wg.Add(1)
	go pgPKMover(src.DB, seed+99, span/2, &stop, &wg, &evMoves)
	time.Sleep(time.Second)

	cfg := strandCfg{Name: "e2e-pg-toastpk", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s, create_slot: true", slot, pub),
		// IOPS thấp: giãn nhịp giữa các chunk (~2,4 s mỗi chunk: bảng PG không có AVG_ROW_LENGTH nên ước
		// lượng 200 B/row) để các lần đổi PK chắc chắn rơi vào lúc items có range ở cả ba trạng thái. lag_slo lớn: bootstrap_retention_risk (lag > lag_slo) bỏ
		// giãn nhịp và snapshot xong trong vài giây, khi router còn đang trễ — mọi event router xử lý trong
		// lúc đó đều cũ hơn biên của bảng nên không lần đổi PK nào cần đọc lại (đúng, nhưng không kiểm tra
		// được gì).
		Include: []string{"app.*"}, Dir: testDir(t), Bootstrap: "logical", SourceIOPS: 25, LagSLO: "10m"}
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
	p := startStrand(t, cfg.write(t))
	// Tải mở (không chờ target): ở strict một worker apply theo đúng thứ tự có thể chậm hơn tốc độ ghi của
	// source, khi đó lag tăng mãi và điểm hẹn của chunk không bao giờ tới. Tải chỉ chạy trong một cửa sổ
	// (mặc định 60 s, phủ toàn bộ phần đầu snapshot); sau đó pipeline PHẢI đuổi kịp và xong snapshot —
	// nếu treo thì là lỗi thật (khóa chết), không phải quá tải.
	loadFor := time.Duration(envInt("STRAND_E2E_PG_TOAST_PK_LOAD_SECONDS", 60)) * time.Second
	stopLoad := func() {
		stop.Store(true)
		wg.Wait()
		wl.Stop()
	}
	loadTimer := time.AfterFunc(loadFor, stopLoad)
	waitSynced(t, &cfg, p, 10*time.Minute)
	// Trạng thái "synced" của admin socket có lúc báo trước khi bootstrap logic xong (thấy được ở e2e này:
	// tải dừng trước khi items bắt đầu snapshot): chờ thêm dòng log kết thúc bootstrap.
	for deadline := time.Now().Add(10 * time.Minute); !strings.Contains(p.output.String(), "bootstrap finished: pipeline synced"); {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("logical bootstrap did not finish\n%s", p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(3 * time.Second) // stream tiếp (đổi PK trên range "Xong": SQL điều kiện của writer)
	loadTimer.Stop()
	stopLoad()
	t.Logf("workload: %d ops, %d client-side errors, %d items PK moves with unchanged TOAST, %d events PK moves",
		wl.ops.Load(), wl.errs.Load(), moves.Load(), evMoves.Load())
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	out := p.output.String()
	if strings.Contains(out, "not supported yet") {
		t.Fatalf("PK change with unchanged TOAST was rejected\n%s", p.tail())
	}
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, `"snapshot done"`) || strings.Contains(l, "bootstrap_retention_risk") || strings.Contains(l, "pipeline synced") {
			t.Log(l)
		}
	}
	reread := 0
	if mm := rereadKeysRe.FindStringSubmatch(out); mm != nil {
		reread, _ = strconv.Atoi(mm[1])
	} else {
		t.Fatalf("no \"snapshot done\" log line for app.items\n%s", p.tail())
	}
	t.Logf("app.items: %d keys re-read during the snapshot", reread)
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)
	return reread
}
