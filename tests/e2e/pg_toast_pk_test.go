package e2e

// Phase 2 (02 §8, P2-03): đổi PK khi cột TOAST không đổi trong lúc snapshot logic PG chạy song song
// với stream.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
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

// rereadQueryLike khớp SELECT của chunk re-read app.items trong pg_stat_activity của source (PGChunkSource:
// tập khóa rời rạc `t."id" IN (...)`; chunk chính dùng điều kiện range, không có IN).
const rereadQueryLike = `SELECT %FROM "app"."items" AS t WHERE % IN (%`

// itemsRereadKeys đọc pipeline-state.json: số khóa app.items đang chờ đọc lại (plan.reread_keys, lưu bền
// trước khi Txn đổi PK được Admit) và bảng còn chưa snapshot xong không (chưa có state = chưa xong).
func itemsRereadKeys(dir string) (n int, unfinished bool) {
	raw, err := os.ReadFile(filepath.Join(dir, "state", "pipeline-state.json"))
	if err != nil {
		return -1, true
	}
	var st struct {
		Plans map[string]struct {
			Finished bool    `json:"finished"`
			Reread   []int64 `json:"reread_keys"`
		} `json:"snapshot_plans"`
		Done map[string]bool `json:"snapshot_done"`
	}
	if json.Unmarshal(raw, &st) != nil {
		return -1, true
	}
	pl, ok := st.Plans["app.items"]
	if !ok {
		return 0, !st.Done["app.items"]
	}
	return len(pl.Reread), !st.Done["app.items"]
}

// Gate 2 (G2-7, 02 §8): đổi PK với partial TOAST trong lúc snapshot logic PG, kill -9 Strand ĐÚNG LÚC một
// chunk re-read đang chạy. Một goroutine dò pg_stat_activity của source mỗi 5 ms; thấy SELECT của chunk
// re-read app.items mới (query_start mới hơn lần trước) thì kill -9 ngay — lúc đó chunk vừa đọc xong
// và đang watermark/điểm hẹn/flush. Kill "trúng" khi state sau kill vẫn còn khóa chờ đọc lại (nghĩa vụ
// chưa được gỡ khỏi plan). Khởi động lại (resume plan: phần chưa đọc và khóa chờ đọc lại bị xóa ở target
// rồi đọc lại), lặp tới 3 lần kill trúng. Cuối: không mất row, không row nào sống lại (so từng row hai
// phía), doc không NULL, mọi phiên bản row ghi đúng một lần, ver không giảm qua mọi lần kill.
func TestG2_E2E_PGToastPKKillDuringReread(t *testing.T) {
	seed := seedFromEnv(t)
	const span = 12000
	slot, pub := "strand_e2e_toastpk_kill", "strand_e2e_pub"
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
	var loadOnce sync.Once
	stopLoad := func() {
		loadOnce.Do(func() {
			stop.Store(true)
			wg.Wait()
			wl.Stop()
		})
	}
	defer stopLoad()
	// Tải chỉ chạy trong một cửa sổ (như TestP2_E2E_PGLogicalSnapshotToastPKMove): ở strict, apply có thể
	// chậm hơn source. Hết tải, khóa còn trong plan vẫn được đọc lại (sau 30 s hoặc cuối walk) nên vẫn
	// còn chunk re-read để kill.
	loadTimer := time.AfterFunc(time.Duration(envInt("STRAND_E2E_PG_TOAST_PK_LOAD_SECONDS", 120))*time.Second, stopLoad)
	defer loadTimer.Stop()
	time.Sleep(time.Second)

	// Như TestP2_E2E_PGLogicalSnapshotToastPKMove: IOPS thấp để snapshot kéo dài (khóa được đánh dấu đọc
	// lại khi range ở nhiều trạng thái), lag_slo lớn để bootstrap_retention_risk không bỏ giãn nhịp.
	cfg := strandCfg{Name: "e2e-pg-toastpk-kill", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s, create_slot: true", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "logical", SourceIOPS: 25, LagSLO: "10m"}
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)

	var lastSeen time.Time
	if err := src.DB.QueryRow("SELECT now()").Scan(&lastSeen); err != nil {
		t.Fatal(err)
	}
	// nextReread chờ SELECT của chunk re-read mới; false khi hết hạn hoặc bảng đã snapshot xong.
	nextReread := func(deadline time.Time) bool {
		for time.Now().Before(deadline) {
			if p.exited() {
				t.Fatalf("strand exited (code %d) while waiting for a re-read chunk\n%s", p.exitCode(), p.tail())
			}
			var qs time.Time
			err := src.DB.QueryRow(`SELECT max(query_start) FROM pg_stat_activity
				WHERE pid <> pg_backend_pid() AND query LIKE $1 AND query_start > $2`, rereadQueryLike, lastSeen).Scan(&qs)
			if err == nil && !qs.IsZero() {
				lastSeen = qs
				return true
			}
			if _, unfinished := itemsRereadKeys(cfg.Dir); !unfinished {
				return false
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}

	landed, attempts := 0, 0
	for deadline := time.Now().Add(4 * time.Minute); landed < 3 && attempts < 6; {
		if !nextReread(deadline) {
			break
		}
		p.kill9()
		attempts++
		n, unfinished := itemsRereadKeys(cfg.Dir)
		if n > 0 {
			landed++
		}
		t.Logf("kill -9 #%d right after a re-read SELECT on the source: plan still holds %d re-read keys (items snapshot unfinished=%v)",
			attempts, n, unfinished)
		p = startStrand(t, cfgPath)
	}
	if landed == 0 {
		t.Fatalf("no kill -9 landed while a re-read chunk was in progress (%d kills, %d PK moves with unchanged TOAST)\n%s",
			attempts, moves.Load(), p.tail())
	}
	stopLoad()
	t.Logf("%d of %d kills landed during a re-read; workload: %d ops, %d client-side errors, %d items PK moves with unchanged TOAST, %d events PK moves",
		landed, attempts, wl.ops.Load(), wl.errs.Load(), moves.Load(), evMoves.Load())

	waitSynced(t, &cfg, p, 10*time.Minute)
	for deadline := time.Now().Add(10 * time.Minute); !strings.Contains(p.output.String(), "bootstrap finished: pipeline synced"); {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("logical bootstrap did not finish after the last restart\n%s", p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	out := p.output.String()
	if strings.Contains(out, "not supported yet") {
		t.Fatalf("PK change with unchanged TOAST was rejected\n%s", p.tail())
	}
	if mm := rereadKeysRe.FindStringSubmatch(out); mm != nil {
		t.Logf("app.items: %s keys re-read after the last restart", mm[1])
	}
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	// kill -9 trong lúc snapshot (kể cả giữa flush chunk re-read và lưu plan): snapshot có thể ghi lại
	// phiên bản đã có.
	checkPGInvariantsSnapshotKilled(t, dst)
}

// Gate 2 (G2-7, 02 §6/§8, toast_pk.go "Ngân sách"): ưu tiên 30 s của chunk re-read. Khóa chờ đọc lại sớm
// nhất đã chờ ≥ 30 s (DefaultPendingMaxWait) thì chunk re-read được đọc ngay ở ranh giới chunk kế tiếp,
// không đợi walk chính xong. Tải thưa (một goroutine ~10 lần đổi PK/s mà doc không đổi) nên số khóa chờ
// luôn xa ngưỡng max_pending_cross_range = 1024: chỉ luật 30 s nhả được khóa giữa chừng. Walk chính dài
// (10.000 row, chunk 100 row, IOPS 20 ≈ 1 s/chunk ≈ 100+ s). Đọc metric mỗi 0,5 s:
//   - strand_pending_cross_range_oldest_seconds trong lúc walk chính chưa xong không bao giờ vượt
//     30 s + 15 s (một chunk chính + chunk re-read + nhịp metric 1 s, dư nhiều) — không có luật 30 s thì
//     khóa đánh dấu sớm phải chờ tới cuối walk (≫ 45 s);
//   - luật thực sự được dùng: có lần tuổi ≥ 25 s rồi giảm hẳn (khóa được đọc lại) khi walk chính còn
//     range "Chưa đọc" (strand_snapshot_ranges{state="Pending"} > 0);
//   - cuối: dữ liệu giống hệt, doc không NULL, mọi phiên bản row ghi đúng một lần.
func TestG2_E2E_PGToastPKReread30sPriority(t *testing.T) {
	seed := seedFromEnv(t)
	const span = 10000
	slot, pub := "strand_e2e_toastpk_prio", "strand_e2e_pub"
	src, dst := setupPGLogical(t, pub, span, 10, 10)

	var stop atomic.Bool
	var moves atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(seed))
		for !stop.Load() {
			from, to := 1+r.Int63n(span*2), 1+r.Int63n(span*2)
			res, err := src.DB.Exec(`UPDATE app.items SET id = $1, ver = nextval('app.ver_seq'), qty = qty + 1 WHERE id = $2`, to, from)
			if err == nil {
				if n, _ := res.RowsAffected(); n == 1 {
					moves.Add(1)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	stopLoad := func() { stop.Store(true); wg.Wait() }
	defer stopLoad()

	cfg := strandCfg{Name: "e2e-pg-toastpk-prio", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s, create_slot: true", slot, pub),
		Include:     []string{"app.items", "app.marker"}, Dir: testDir(t), Bootstrap: "logical",
		SourceIOPS: 20, LagSLO: "10m", ChunkRows: "100", MetricsAddr: freeAddr(t)}
	p := startStrand(t, cfg.write(t))
	label := `pipeline="` + cfg.Name + `"`
	const bound = 45.0
	type obs struct {
		at                    time.Duration
		pending, oldest, rest float64
	}
	var hist []obs
	start := time.Now()
	maxAge, peak := 0.0, 0.0
	releasedMidWalk := 0
	maxPending := 0.0
	var walkEnd time.Duration
	seenWalk := false
	history := func() string {
		var b []string
		for _, h := range hist[max(0, len(hist)-40):] {
			b = append(b, fmt.Sprintf("t=%.1fs pending=%v oldest=%.1fs unread_ranges=%v", h.at.Seconds(), h.pending, h.oldest, h.rest))
		}
		return strings.Join(b, "\n")
	}
	for deadline := time.Now().Add(6 * time.Minute); walkEnd == 0; time.Sleep(500 * time.Millisecond) {
		if p.exited() {
			t.Fatalf("strand exited (code %d)\n%s", p.exitCode(), p.tail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("items main walk did not finish in 6 minutes\n%s\n%s", history(), p.tail())
		}
		o := obs{at: time.Since(start)}
		var ok1, ok2, ok3 bool
		o.pending, ok1, _ = scrapeMetric(cfg.MetricsAddr, "strand_pending_cross_range", label)
		o.oldest, ok2, _ = scrapeMetric(cfg.MetricsAddr, "strand_pending_cross_range_oldest_seconds", label)
		o.rest, ok3, _ = scrapeMetric(cfg.MetricsAddr, "strand_snapshot_ranges", label, `table="app.items"`, `state="Pending"`)
		itemsDone := rereadKeysRe.MatchString(p.output.String())
		if !ok1 || !ok2 {
			continue // metric server chưa lên
		}
		hist = append(hist, o)
		switch {
		case ok3 && o.rest > 0 && !itemsDone: // walk chính còn range "Chưa đọc"
			seenWalk = true
			maxAge = math.Max(maxAge, o.oldest)
			maxPending = math.Max(maxPending, o.pending)
			if o.oldest > bound {
				t.Fatalf("a re-read key waited %.1f s (> %.0f s) while the main walk was still running: the 30 s priority was not applied\n%s\n%s",
					o.oldest, bound, history(), p.tail())
			}
			if o.oldest >= 25 {
				peak = math.Max(peak, o.oldest)
			} else if peak >= 25 && o.oldest < peak-10 {
				releasedMidWalk++
				t.Logf("t=%.1fs: re-read released mid-walk (oldest wait %.1f s → %.1f s, %v keys pending)", o.at.Seconds(), peak, o.oldest, o.pending)
				peak = 0
			}
		case seenWalk || itemsDone:
			if !seenWalk {
				t.Fatalf("app.items finished before its main walk was observed\n%s", p.tail())
			}
			walkEnd = o.at
		}
	}
	stopLoad()
	t.Logf("items main walk ran ~%.0f s; max re-read wait during it %.1f s (bound %.0f s), max pending keys %v, %d mid-walk releases, %d PK moves",
		walkEnd.Seconds(), maxAge, bound, maxPending, releasedMidWalk, moves.Load())
	if releasedMidWalk == 0 {
		t.Fatalf("the 30 s rule was never exercised: no key waited ≥ 25 s and was then re-read before the main walk ended "+
			"(walk %.0f s, max wait %.1f s, %d moves) — the walk is too short or no key was marked\n%s", walkEnd.Seconds(), maxAge, moves.Load(), p.tail())
	}
	if maxPending >= 1024 {
		t.Fatalf("pending re-read keys reached max_pending_cross_range (%v): releases may come from the count limit, not the 30 s rule", maxPending)
	}

	waitSynced(t, &cfg, p, 5*time.Minute)
	for deadline := time.Now().Add(5 * time.Minute); !strings.Contains(p.output.String(), "bootstrap finished: pipeline synced"); {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("logical bootstrap did not finish\n%s", p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	comparePG(t, src, dst, "app.items", "app.marker")
	checkPGInvariants(t, dst)
}
