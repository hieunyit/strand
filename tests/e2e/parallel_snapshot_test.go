package e2e

// Snapshot song song nhiều bảng (bootstrap.logical.max_parallel). Mọi kịch bản chạy tải liên tục trên
// TẤT CẢ bảng trong lúc snapshot: INSERT/UPDATE/DELETE, UPDATE đổi PK (sang range khác), đổi chéo unique
// key, Txn chạm nhiều bảng — rồi so toàn bộ dữ liệu nguồn/đích.
//
// Chạy: STRAND_E2E_COMMIT_ORDER=strict|relaxed go test ./tests/e2e -run 'TestPS_E2E' -count=1
// Đo: go test ./tests/e2e -run '^$' -bench BenchmarkPS_ParallelSnapshot -benchtime 1x

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/network"

	"strand/internal/admin"
)

// psTable: một bảng của kịch bản. str: PK VARCHAR (khóa tổng quát, WEIGHT_STRING); uk: có unique key code.
type psTable struct {
	name    string
	rows    int
	str, uk bool
}

// Kích cỡ khác nhau để thứ tự lớn-trước có ý nghĩa; t5 dành cho `snapshot add`.
var psTablesMySQL = []psTable{
	{name: "ps.t1", rows: 40000},
	{name: "ps.t2", rows: 30000, uk: true},
	{name: "ps.t3", rows: 20000, str: true},
	{name: "ps.t4", rows: 12000},
}

// MySQL→PG: PK không phải một cột số nguyên chỉ được nhận khi bảng rỗng — chỉ dùng bảng PK số.
var psTablesPG = []psTable{
	{name: "ps.t1", rows: 30000},
	{name: "ps.t2", rows: 24000, uk: true},
	{name: "ps.t4", rows: 16000},
	{name: "ps.t5", rows: 9000},
}

var psExtraTable = psTable{name: "ps.t5", rows: 8000}

func psCreateSQL(tb psTable, collation string) string {
	idType := "BIGINT"
	if tb.str {
		idType = "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE " + collation
	}
	uk := ""
	if tb.uk {
		uk = ", code VARCHAR(32) NOT NULL, UNIQUE KEY uk_code (code)"
	}
	return fmt.Sprintf("CREATE TABLE %s (id %s NOT NULL PRIMARY KEY, ver BIGINT UNSIGNED NOT NULL, v INT NOT NULL, pad VARCHAR(200)%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		tb.name, idType, uk)
}

func psSetup(t testing.TB, src *mysqlNode, tables []psTable, collation string) {
	t.Helper()
	src.exec(t, "CREATE DATABASE IF NOT EXISTS ps")
	for _, tb := range tables {
		src.exec(t, psCreateSQL(tb, collation))
	}
}

func psKey(tb psTable, n int64) any {
	if tb.str {
		return fmt.Sprintf("k%09d", n)
	}
	return n
}

// psPreload: rows hàng mỗi bảng (khóa 1..rows), theo lô nhiều-row.
func psPreload(t testing.TB, db *sql.DB, tables []psTable) {
	t.Helper()
	for _, tb := range tables {
		for base := 1; base <= tb.rows; base += 1000 {
			var sb strings.Builder
			var args []any
			fmt.Fprintf(&sb, "INSERT INTO %s (id, ver, v, pad%s) VALUES ", tb.name, map[bool]string{true: ", code", false: ""}[tb.uk])
			for i := base; i < base+1000 && i <= tb.rows; i++ {
				if i > base {
					sb.WriteString(",")
				}
				if tb.uk {
					sb.WriteString("(?, UUID_SHORT(), ?, ?, ?)")
					args = append(args, psKey(tb, int64(i)), i, strings.Repeat("p", 40+i%120), fmt.Sprintf("c%09d", i))
				} else {
					sb.WriteString("(?, UUID_SHORT(), ?, ?)")
					args = append(args, psKey(tb, int64(i)), i, strings.Repeat("p", 40+i%120))
				}
			}
			if _, err := db.Exec(sb.String(), args...); err != nil {
				t.Fatalf("preload %s: %v", tb.name, err)
			}
		}
	}
}

// psWorkload: tải trên mọi bảng; khóa mới lấy từ một bộ đếm chung (không trùng khóa cũ).
type psWorkload struct {
	db      *sql.DB
	tables  []psTable
	seed    int64
	nextID  atomic.Int64
	ops     atomic.Int64
	errs    atomic.Int64
	stopped atomic.Bool
	wg      sync.WaitGroup
}

func newPSWorkload(db *sql.DB, tables []psTable, seed int64) *psWorkload {
	w := &psWorkload{db: db, tables: tables, seed: seed}
	w.nextID.Store(10_000_000)
	return w
}

func (w *psWorkload) Start(clients int) {
	for c := 0; c < clients; c++ {
		w.wg.Add(1)
		r := rand.New(rand.NewSource(w.seed + int64(c)*7919))
		go func() {
			defer w.wg.Done()
			for !w.stopped.Load() {
				if err := w.one(r); err != nil {
					w.errs.Add(1)
				}
				w.ops.Add(1)
			}
		}()
	}
}

func (w *psWorkload) Stop() {
	w.stopped.Store(true)
	w.wg.Wait()
}

func (w *psWorkload) one(r *rand.Rand) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n := 1
	if r.Intn(10) == 0 {
		n = len(w.tables) // Txn chạm mọi bảng
	}
	for i := 0; i < n; i++ {
		tb := w.tables[r.Intn(len(w.tables))]
		if n > 1 {
			tb = w.tables[i]
		}
		if err := w.op(tx, r, tb); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (w *psWorkload) op(tx *sql.Tx, r *rand.Rand, tb psTable) error {
	span := int64(tb.rows)
	key := func(n int64) any { return psKey(tb, n) }
	pad := strings.Repeat("u", 10+r.Intn(150))
	switch k := r.Intn(100); {
	case k < 35: // UPDATE khóa bất kỳ (nóng: nửa vào 50 khóa đầu)
		id := 1 + r.Int63n(span)
		if r.Intn(2) == 0 {
			id = 1 + r.Int63n(50)
		}
		_, err := tx.Exec("UPDATE "+tb.name+" SET ver = UUID_SHORT(), v = v + 1, pad = ? WHERE id = ?", pad, key(id))
		return err
	case k < 55: // INSERT/UPSERT rải khắp không gian khóa (cả range chưa đọc, đang đọc, đã xong)
		id := 1 + r.Int63n(span*2)
		if tb.uk {
			_, err := tx.Exec("INSERT INTO "+tb.name+" (id, ver, v, pad, code) VALUES (?, UUID_SHORT(), 0, ?, ?) ON DUPLICATE KEY UPDATE ver = UUID_SHORT(), pad = VALUES(pad)",
				key(id), pad, fmt.Sprintf("n%d", w.nextID.Add(1)))
			return err
		}
		_, err := tx.Exec("INSERT INTO "+tb.name+" (id, ver, v, pad) VALUES (?, UUID_SHORT(), 0, ?) ON DUPLICATE KEY UPDATE ver = UUID_SHORT(), pad = VALUES(pad)", key(id), pad)
		return err
	case k < 70: // DELETE
		_, err := tx.Exec("DELETE FROM "+tb.name+" WHERE id = ?", key(1+r.Int63n(span*2)))
		return err
	case k < 88: // UPDATE đổi PK: sang khóa mới ở range bất kỳ (thường là range khác)
		old := 1 + r.Int63n(span*2)
		var nw int64
		if r.Intn(2) == 0 {
			nw = w.nextID.Add(1) // sau b_last
		} else {
			nw = span*2 + 1 + r.Int63n(span*4) // giữa/ngoài các range đã đăng ký
			if r.Intn(2) == 0 {
				nw = 1 + r.Int63n(span) // vào giữa bảng
			}
		}
		_, err := tx.Exec("UPDATE IGNORE "+tb.name+" SET id = ?, ver = UUID_SHORT() WHERE id = ?", key(nw), key(old))
		return err
	default: // đổi chéo unique key giữa hai row (bảng có uk), hoặc UPDATE thêm
		if !tb.uk {
			_, err := tx.Exec("UPDATE "+tb.name+" SET ver = UUID_SHORT(), v = v - 1 WHERE id = ?", key(1+r.Int63n(span)))
			return err
		}
		a, b := 1+r.Int63n(span), 1+r.Int63n(span)
		if a == b {
			return nil
		}
		var ca, cb string
		if err := tx.QueryRow("SELECT code FROM "+tb.name+" WHERE id = ? FOR UPDATE", a).Scan(&ca); err != nil {
			return nil
		}
		if err := tx.QueryRow("SELECT code FROM "+tb.name+" WHERE id = ? FOR UPDATE", b).Scan(&cb); err != nil {
			return nil
		}
		for _, q := range []struct {
			code string
			id   int64
		}{{"tmp-" + ca, a}, {ca, b}, {cb, a}} {
			if _, err := tx.Exec("UPDATE "+tb.name+" SET code = ?, ver = UUID_SHORT() WHERE id = ?", q.code, q.id); err != nil {
				return err
			}
		}
		return nil
	}
}

func psNames(tables []psTable) []string {
	out := make([]string, len(tables))
	for i, tb := range tables {
		out[i] = tb.name
	}
	return out
}

// psWatch lấy mẫu `strand top` trong lúc bootstrap: số bảng chạy đồng thời lớn nhất.
type psWatch struct {
	stop    atomic.Bool
	wg      sync.WaitGroup
	maxRun  atomic.Int32
	seenAll sync.Map
}

func watchParallel(socket string) *psWatch {
	w := &psWatch{}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for !w.stop.Load() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			top, err := admin.NewClient(socket).Top(ctx)
			cancel()
			if err == nil {
				if n := int32(len(top.SnapshotTables)); n > w.maxRun.Load() {
					w.maxRun.Store(n)
				}
				for _, tb := range top.SnapshotTables {
					w.seenAll.Store(tb, true)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return w
}

func (w *psWatch) done() int32 {
	w.stop.Store(true)
	w.wg.Wait()
	return w.maxRun.Load()
}

// psPlans đọc state cục bộ: bảng → (đang dở: plan đã flush một phần), số bảng Done.
func psPlans(dir string) (partial map[string]bool, done map[string]bool) {
	partial, done = map[string]bool{}, map[string]bool{}
	raw, err := os.ReadFile(filepath.Join(dir, "state", "pipeline-state.json"))
	if err != nil {
		return
	}
	var st struct {
		Plans map[string]struct {
			MinPK   int64             `json:"min_pk"`
			NextPK  int64             `json:"next_pk"`
			LastKey []json.RawMessage `json:"last_key"`
		} `json:"snapshot_plans"`
		Done map[string]bool `json:"snapshot_done"`
	}
	_ = json.Unmarshal(raw, &st)
	for k, v := range st.Done {
		if v {
			done[k] = true
		}
	}
	for name, pl := range st.Plans {
		if !done[name] && (pl.NextPK > pl.MinPK || len(pl.LastKey) > 0) {
			partial[name] = true
		}
	}
	return
}

// psProgress: chữ ký tiến độ snapshot (plan từng bảng + Done) để phát hiện snapshot có tiến hay không.
func psProgress(dir string) string {
	raw, err := os.ReadFile(filepath.Join(dir, "state", "pipeline-state.json"))
	if err != nil {
		return ""
	}
	var st struct {
		Plans json.RawMessage `json:"snapshot_plans"`
		Done  json.RawMessage `json:"snapshot_done"`
	}
	_ = json.Unmarshal(raw, &st)
	return string(st.Plans) + "|" + string(st.Done)
}

var psMaxParallelLog = regexp.MustCompile(`"msg":"bootstrap snapshot"[^\n]*"max_parallel":(\d+)`)

func checkMaxParallelLogged(t *testing.T, p *strandProc, want int) {
	t.Helper()
	m := psMaxParallelLog.FindStringSubmatch(p.output.String())
	if m == nil || m[1] != fmt.Sprint(want) {
		t.Fatalf("bootstrap log lacks max_parallel %d (got %v)\n%s", want, m, p.tail())
	}
}

// waitMySQLEqual: hội tụ khi không có GTID (MariaDB, chế độ vị trí binlog): synced và dữ liệu bằng nhau.
func waitMySQLEqual(t *testing.T, src, dst *mysqlNode, cfg *strandCfg, p *strandProc, tables []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if p.exited() {
			t.Fatalf("strand exited (code %d) before converging\n%s", p.exitCode(), p.tail())
		}
		if pipelineSynced(cfg.Socket) && psEqual(src.DB, dst.DB, tables) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("source and target did not converge after %s\n%s", timeout, p.tail())
		}
		time.Sleep(time.Second)
	}
}

func psEqual(a, b *sql.DB, tables []string) bool {
	for _, tb := range tables {
		q := "SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT_WS('|', id, ver, v, pad))), 0) FROM " + tb
		var n1, n2 int64
		var s1, s2 string
		if a.QueryRow(q).Scan(&n1, &s1) != nil || b.QueryRow(q).Scan(&n2, &s2) != nil || n1 != n2 || s1 != s2 {
			return false
		}
	}
	return true
}

// checksumTables: CHECKSUM TABLE nguồn = đích cho từng bảng (cùng engine MySQL/MariaDB→MySQL thì định
// dạng row có thể khác nhau; ngoài compareFinal còn so một checksum theo nội dung).
func checksumTables(t *testing.T, src, dst *mysqlNode, tables []string) {
	t.Helper()
	for _, tb := range tables {
		q := "SELECT COUNT(*), COALESCE(SUM(CRC32(CONCAT_WS('|', id, ver, v, IFNULL(pad, '<n>')))), 0), COALESCE(BIT_XOR(CRC32(CONCAT_WS('|', id, ver, v, IFNULL(pad, '<n>')))), 0) FROM " + tb
		var n1, n2 int64
		var s1, s2, x1, x2 string
		if err := src.DB.QueryRow(q).Scan(&n1, &s1, &x1); err != nil {
			t.Fatal(err)
		}
		if err := dst.DB.QueryRow(q).Scan(&n2, &s2, &x2); err != nil {
			t.Fatal(err)
		}
		if n1 != n2 || s1 != s2 || x1 != x2 {
			t.Fatalf("checksum %s differs: source rows=%d sum=%s xor=%s, target rows=%d sum=%s xor=%s", tb, n1, s1, x1, n2, s2, x2)
		}
		t.Logf("checksum %s: %d rows, crc sum %s", tb, n1, s1)
	}
}

func psCfg(t testing.TB, name, srcDSN, dstDSN string, tables []string, n int) strandCfg {
	return strandCfg{Name: name, SourceDSN: srcDSN, TargetDSN: dstDSN, Include: tables, Dir: testDir(t),
		ChunkRows: "1000", MaxParallel: n, SourceIOPS: 100000}
}

// runPSUnderLoad: snapshot song song (n bảng) dưới tải trên mọi bảng → dữ liệu giống hệt.
func runPSUnderLoad(t *testing.T, src, dst *mysqlNode, cfg strandCfg, tables []psTable, gtid bool, wantParallel int32) {
	seed := seedFromEnv(t)
	names := psNames(tables)
	wl := newPSWorkload(src.DB, tables, seed)
	wl.Start(6)
	time.Sleep(2 * time.Second)
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	w := watchParallel(cfg.Socket)
	var vs *verSampler
	if gtid {
		vs = startVerSampler(dst, names...)
	}
	waitSynced(t, &cfg, p, 10*time.Minute)
	maxRun := w.done()
	time.Sleep(3 * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors", wl.ops.Load(), wl.errs.Load())
	if wl.ops.Load() < 500 {
		t.Fatalf("workload too small: %d ops", wl.ops.Load())
	}
	if gtid {
		waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	} else {
		waitMySQLEqual(t, src, dst, &cfg, p, names, 5*time.Minute)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	if vs != nil {
		vs.check(t)
	}
	checkMaxParallelLogged(t, p, cfg.MaxParallel)
	if maxRun < wantParallel {
		t.Fatalf("at most %d tables were snapshotted at the same time, want >= %d", maxRun, wantParallel)
	}
	if cfg.MaxParallel == 1 && maxRun > 1 {
		t.Fatalf("max_parallel=1 ran %d tables at the same time", maxRun)
	}
	t.Logf("tables snapshotted concurrently: up to %d (max_parallel %d)", maxRun, cfg.MaxParallel)
	compareFinal(t, src, dst, names...)
	checksumTables(t, src, dst, names)
}

// MySQL → MySQL, 4 bảng (PK số, PK số + unique key, PK VARCHAR collation ai_ci, PK số), max_parallel 3.
func TestPS_E2E_ParallelSnapshotUnderLoadMySQL(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	psSetup(t, src, psTablesMySQL, "utf8mb4_0900_ai_ci")
	psPreload(t, src.DB, psTablesMySQL)
	cfg := psCfg(t, "e2e-ps-my", src.RootDSN, dst.RootDSN+"strand", psNames(psTablesMySQL), 3)
	runPSUnderLoad(t, src, dst, cfg, psTablesMySQL, true, 2)
}

// MariaDB 10.6 → MySQL (chế độ vị trí binlog: low = LOCK TABLES t READ + SHOW MASTER STATUS mỗi chunk, trên
// kết nối riêng của từng bảng), max_parallel 3.
func TestPS_E2E_ParallelSnapshotUnderLoadMDB(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	psSetup(t, src, psTablesMySQL, "utf8mb4_general_ci")
	psPreload(t, src.DB, psTablesMySQL)
	cfg := psCfg(t, "e2e-ps-mdb", src.RootDSN, dst.RootDSN+"strand", psNames(psTablesMySQL), 3)
	runPSUnderLoad(t, src, dst, cfg, psTablesMySQL, false, 2)
}

// max_parallel = 1 (khai báo tường minh): hành vi cũ — một bảng một lúc, cùng kết quả.
func TestPS_E2E_MaxParallelOneRegression(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	psSetup(t, src, psTablesMySQL, "utf8mb4_0900_ai_ci")
	psPreload(t, src.DB, psTablesMySQL)
	cfg := psCfg(t, "e2e-ps-one", src.RootDSN, dst.RootDSN+"strand", psNames(psTablesMySQL), 1)
	runPSUnderLoad(t, src, dst, cfg, psTablesMySQL, true, 1)
}

// kill -9 khi ít nhất hai bảng đang dở (plan đã flush một phần) — nhiều lần — rồi chạy tiếp: mỗi bảng
// tiếp tục từ plan riêng, dữ liệu giống hệt.
func TestPS_E2E_ParallelSnapshotKill9Resume(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	tables := psTablesMySQL
	psSetup(t, src, tables, "utf8mb4_0900_ai_ci")
	psPreload(t, src.DB, tables)
	names := psNames(tables)
	cfg := psCfg(t, "e2e-ps-k9", src.RootDSN, dst.RootDSN+"strand", names, 3)
	cfg.SourceIOPS = 3000 // chậm vừa đủ để có nhiều bảng dở dang lúc kill
	cfgPath := cfg.write(t)
	wl := newPSWorkload(src.DB, tables, seed)
	wl.Start(6)
	vs := startVerSampler(dst, names...)
	rng := rand.New(rand.NewSource(seed))
	resumedTables := map[string]bool{}
	kills := 0
	for round := 0; round < 4; round++ {
		p := startStrand(t, cfgPath)
		// Cho lần chạy này snapshot được một lúc (đăng ký range, resume plan, vài chunk) rồi mới kill.
		waitStrandLog(t, p, `"msg":"bootstrap snapshot"`, 3*time.Minute)
		time.Sleep(time.Duration(500+rng.Intn(2500)) * time.Millisecond)
		deadline := time.Now().Add(3 * time.Minute)
		killed := false
		for !killed {
			if p.exited() {
				t.Fatalf("round %d: strand exited (code %d)\n%s", round, p.exitCode(), p.tail())
			}
			partial, done := psPlans(cfg.Dir)
			if len(done) == len(tables) {
				break // snapshot xong trước khi kịp kill
			}
			if len(partial) >= 2 {
				p.kill9()
				kills++
				killed = true
				t.Logf("round %d: kill -9 with %d tables half done (%v), %d done", round, len(partial), keys(partial), len(done))
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: no two tables half done within 3m\n%s", round, p.tail())
			}
			time.Sleep(50 * time.Millisecond)
		}
		if round > 0 {
			for _, m := range regexp.MustCompile(`"msg":"snapshot resumes from saved plan"[^\n]*"table":"([^"]+)"`).FindAllStringSubmatch(p.output.String(), -1) {
				resumedTables[m[1]] = true
			}
		}
		if !killed {
			p.kill9()
			break
		}
	}
	if kills < 2 {
		t.Fatalf("only %d kills happened while tables were half done", kills)
	}
	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 10*time.Minute)
	for _, m := range regexp.MustCompile(`"msg":"snapshot resumes from saved plan"[^\n]*"table":"([^"]+)"`).FindAllStringSubmatch(p.output.String(), -1) {
		resumedTables[m[1]] = true
	}
	if len(resumedTables) < 2 {
		t.Fatalf("restarts resumed %d half-done tables from their plans (%v), want >= 2", len(resumedTables), keys(resumedTables))
	}
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors; %d kills; resumed %v", wl.ops.Load(), wl.errs.Load(), kills, keys(resumedTables))
	waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	vs.check(t)
	compareFinal(t, src, dst, names...)
	checksumTables(t, src, dst, names)
}

func keys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// `snapshot add` một bảng mới trong lúc snapshot song song đang chạy (bảng add chạy cùng các worker và chia
// chung ngân sách đọc); `snapshot add` một bảng còn trong hàng đợi của bootstrap bị từ chối.
func TestPS_E2E_SnapshotAddDuringParallelSnapshot(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	all := append(append([]psTable{}, psTablesMySQL...), psExtraTable)
	psSetup(t, src, all, "utf8mb4_0900_ai_ci")
	psPreload(t, src.DB, all)
	cfg := psCfg(t, "e2e-ps-add", src.RootDSN, dst.RootDSN+"strand", psNames(psTablesMySQL), 3)
	cfg.SourceIOPS = 5000
	wl := newPSWorkload(src.DB, all, seed)
	wl.Start(6)
	p := startStrand(t, cfg.write(t))
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitStrandLog(t, p, `"msg":"bootstrap snapshot"`, 3*time.Minute)
	// t4 (nhỏ nhất) đứng cuối hàng: đang chờ (hoặc vừa chạy) — không được snapshot lần thứ hai.
	out, code := runStrandCLI(t, time.Minute, "snapshot", "add", "--table", "ps.t4", "--socket", cfg.Socket)
	if code == 0 || !strings.Contains(out, "already being snapshotted") {
		t.Fatalf("snapshot add of a table queued in the bootstrap: exit %d\n%s", code, out)
	}
	w := watchParallel(cfg.Socket)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		partial, done := psPlans(cfg.Dir)
		if len(partial) >= 2 && len(done) < len(psTablesMySQL)-1 {
			break
		}
		if time.Now().After(deadline) || p.exited() {
			t.Fatalf("parallel snapshot did not get going\n%s", p.tail())
		}
		time.Sleep(50 * time.Millisecond)
	}
	out, code = runStrandCLI(t, time.Minute, "snapshot", "add", "--table", "ps.t5", "--socket", cfg.Socket)
	if code != 0 {
		t.Fatalf("snapshot add ps.t5: exit %d\n%s", code, out)
	}
	if pipelineSynced(cfg.Socket) {
		t.Fatal("pipeline reported synced right after snapshot add")
	}
	waitSynced(t, &cfg, p, 10*time.Minute)
	maxRun := w.done()
	if _, ok := w.seenAll.Load("ps.t5"); !ok {
		t.Fatalf("ps.t5 was never reported running")
	}
	time.Sleep(2 * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops; up to %d tables at once", wl.ops.Load(), maxRun)
	waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	if maxRun < 3 {
		t.Fatalf("snapshot add did not run alongside the bootstrap workers (max %d tables at once)", maxRun)
	}
	compareFinal(t, src, dst, psNames(all)...)
	checksumTables(t, src, dst, psNames(all))
}

// Breaker mở (replica lag) giữa lúc nhiều bảng đang snapshot: MỌI worker dừng (không bảng nào tiến, không
// bảng mới nào bắt đầu); hết lag thì tất cả chạy tiếp và dữ liệu giống hệt.
func TestPS_E2E_BreakerPausesAllParallelWorkers(t *testing.T) {
	seed := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	primary := startMySQL(t, mysqlImage("source"), "primary", 1, nw)
	r1 := startMySQL(t, mysqlImage("source"), "replica1", 3, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	makeReplica(t, r1, "primary")
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	tables := []psTable{{name: "ps.t1", rows: 60000}, {name: "ps.t2", rows: 50000, uk: true}, {name: "ps.t3", rows: 40000, str: true}, {name: "ps.t4", rows: 30000}}
	names := psNames(tables)
	psSetup(t, primary, tables, "utf8mb4_0900_ai_ci")
	psPreload(t, primary.DB, tables)
	waitReplicaCaughtUp(t, primary, r1)
	wl := newPSWorkload(primary.DB, tables, seed)
	wl.Start(4)

	cfg := psCfg(t, "e2e-ps-breaker", r1.RootDSN, dst.RootDSN+"strand", names, 3)
	cfg.SourceIOPS, cfg.ChunkRows, cfg.BreakerLag = 400, "500", "5s"
	p := startStrand(t, cfg.write(t))
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		partial, _ := psPlans(cfg.Dir)
		if len(partial) >= 2 {
			break
		}
		if time.Now().After(deadline) || p.exited() {
			t.Fatalf("parallel snapshot did not start\n%s", p.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	r1.exec(t, "STOP REPLICA", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 60", "START REPLICA")
	deadline = time.Now().Add(2 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		top, err := admin.NewClient(cfg.Socket).Top(ctx)
		cancel()
		if err == nil && top.BreakerOpen {
			t.Logf("breaker open: %s; running %v", top.BreakerReason, top.SnapshotTables)
			break
		}
		if time.Now().After(deadline) || p.exited() {
			t.Fatalf("breaker did not open\n%s", p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, done := psPlans(cfg.Dir); len(done) == len(tables) {
		t.Fatal("snapshot finished before the breaker opened; the test needs a slower snapshot")
	}
	time.Sleep(5 * time.Second) // chunk đang dở (đã qua pace) được phép xong
	before := psProgress(cfg.Dir)
	time.Sleep(20 * time.Second)
	after := psProgress(cfg.Dir)
	if before != after {
		t.Fatalf("snapshot progressed while the breaker was open:\nbefore %s\nafter  %s", before, after)
	}
	t.Logf("breaker open: every worker held for 20s")
	r1.exec(t, "STOP REPLICA", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0", "START REPLICA")
	wl.Stop()
	waitReplicaCaughtUp(t, primary, r1)
	waitSynced(t, &cfg, p, 10*time.Minute)
	waitCaughtUp(t, primary, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	compareFinal(t, primary, dst, names...)
	checksumTables(t, primary, dst, names)
}

// MySQL → PostgreSQL, 4 bảng PK số (một có unique key), max_parallel 3, dưới tải.
func TestPS_E2E_ParallelSnapshotMySQLToPG(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	tables := psTablesPG
	names := psNames(tables)
	psSetup(t, src, tables, "utf8mb4_0900_ai_ci")
	pgSQL := []string{"CREATE SCHEMA ps"}
	for _, tb := range tables {
		uk := ""
		if tb.uk {
			uk = `, code varchar(32) COLLATE "C" NOT NULL UNIQUE`
		}
		pgSQL = append(pgSQL, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, ver numeric(20,0) NOT NULL, v integer NOT NULL, pad varchar(200)%s)", tb.name, uk))
	}
	dst.exec(t, pgSQL...)
	psPreload(t, src.DB, tables)
	cfg := strandCfg{Name: "e2e-ps-pg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: names, Dir: testDir(t), ChunkRows: "1000", MaxParallel: 3, SourceIOPS: 100000}
	wl := newPSWorkload(src.DB, tables, seed)
	wl.Start(6)
	time.Sleep(2 * time.Second)
	p := startStrand(t, cfg.write(t))
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	w := watchParallel(cfg.Socket)
	waitSynced(t, &cfg, p, 10*time.Minute)
	maxRun := w.done()
	time.Sleep(3 * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors; up to %d tables at once", wl.ops.Load(), wl.errs.Load(), maxRun)
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	checkMaxParallelLogged(t, p, 3)
	if maxRun < 2 {
		t.Fatalf("at most %d tables at once, want >= 2", maxRun)
	}
	var bad []string
	total := 0
	for _, tb := range tables {
		cols := "id, ver, v, pad"
		if tb.uk {
			cols += ", code"
		}
		q := "SELECT " + cols + " FROM " + tb.name + " ORDER BY id"
		s := dumpCanon(t, src.DB, q, -1)
		d := dumpCanon(t, dst.DB, q, -1)
		total += len(s)
		if len(s) != len(d) {
			bad = append(bad, fmt.Sprintf("%s: %d rows on source, %d on target", tb.name, len(s), len(d)))
		}
		for k, sv := range s {
			if dv, ok := d[k]; !ok || strings.Join(sv, "|") != strings.Join(dv, "|") {
				bad = append(bad, fmt.Sprintf("%s %s: src %v dst %v", tb.name, k, sv, dv))
			}
			if len(bad) > 15 {
				break
			}
		}
	}
	if len(bad) > 0 {
		t.Fatalf("MySQL→PG mismatch:\n%s", strings.Join(bad, "\n"))
	}
	t.Logf("final state: %d rows identical in %d tables", total, len(tables))
}

// BenchmarkPS_ParallelSnapshot: 4 bảng × STRAND_E2E_PS_BENCH_ROWS (mặc định 200k) hàng, giới hạn đọc nguồn
// cao (không chi phối), không tải; đo thời gian bootstrap với max_parallel 1 và 4.
func BenchmarkPS_ParallelSnapshot(b *testing.B) {
	rows := envInt("STRAND_E2E_PS_BENCH_ROWS", 200000)
	tables := []psTable{{name: "ps.t1", rows: rows}, {name: "ps.t2", rows: rows, uk: true}, {name: "ps.t3", rows: rows}, {name: "ps.t4", rows: rows}}
	for _, n := range []int{1, 4} {
		b.Run(fmt.Sprintf("max_parallel=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				el := psBenchOnce(b, tables, n)
				b.ReportMetric(el.Seconds(), "bootstrap_s")
			}
		})
	}
}

// psBenchOnce: một lần bootstrap trên cặp MySQL mới; trả thời gian từ "bootstrap snapshot" tới "synced".
func psBenchOnce(b *testing.B, tables []psTable, n int) time.Duration {
	b.StopTimer()
	src := startMySQL(b, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(b, mysqlImage("target"), "dst", 2, nil)
	dst.exec(b, "CREATE DATABASE IF NOT EXISTS strand")
	psSetup(b, src, tables, "utf8mb4_0900_ai_ci")
	psPreload(b, src.DB, tables)
	cfg := psCfg(b, fmt.Sprintf("bench-ps-%d", n), src.RootDSN, dst.RootDSN+"strand", psNames(tables), n)
	cfg.ChunkRows = os.Getenv("STRAND_E2E_PS_BENCH_CHUNK") // rỗng = auto
	cfg.SourceIOPS = envInt("STRAND_E2E_PS_BENCH_IOPS", 1000000)
	b.StartTimer()
	p := startStrand(b, cfg.write(b))
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitStrandLog(b, p, `"msg":"bootstrap snapshot"`, 3*time.Minute)
	start := time.Now()
	waitSynced(b, &cfg, p, 60*time.Minute)
	el := time.Since(start)
	b.StopTimer()
	if code := p.stop(b); code != 0 {
		b.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	if !psEqual(src.DB, dst.DB, psNames(tables)) {
		b.Fatal("source and target differ after the benchmark run")
	}
	b.Logf("max_parallel=%d: 4 tables x %d rows bootstrapped in %s", n, tables[0].rows, el.Round(time.Millisecond))
	return el
}
