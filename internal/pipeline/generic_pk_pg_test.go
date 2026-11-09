package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/config"
	"strand/internal/model"
	"strand/internal/snapshot"
)

type gpkPGShape struct {
	name, cols string
	pk         []string
	key        func(r *rand.Rand) []string // literal SQL của các cột PK
}

func gpkPGShapes() []gpkPGShape {
	q := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	words := []string{"a", "A", "b", "ab", "abc", "é", "e", "z", "中", "😀", "0", "10", "9", "aa"}
	return []gpkPGShape{
		{"comp_text_c", `a int4 NOT NULL, b text COLLATE "C" NOT NULL`, []string{"a", "b"},
			func(r *rand.Rand) []string {
				return []string{fmt.Sprint(r.Intn(20) - 10), q(words[r.Intn(len(words))] + fmt.Sprint(r.Intn(9)))}
			}},
		{"uuid_pk", `u uuid NOT NULL`, []string{"u"},
			func(r *rand.Rand) []string {
				return []string{fmt.Sprintf("'00000000-0000-0000-0000-%012x'", r.Intn(4000))}
			}},
		{"numeric_pk", `x numeric(14,4) NOT NULL`, []string{"x"},
			func(r *rand.Rand) []string { return []string{fmt.Sprintf("%d.%04d", r.Intn(400)-200, r.Intn(4)*2500)} }},
		{"ts_int", `t timestamptz NOT NULL, n int4 NOT NULL`, []string{"t", "n"},
			func(r *rand.Rand) []string {
				return []string{q(time.Unix(1700000000+r.Int63n(80)*3600, r.Int63n(4)*250e6).UTC().Format("2006-01-02 15:04:05.000Z07:00")), fmt.Sprint(r.Intn(12))}
			}},
		{"bpchar_c", `c char(8) COLLATE "C" NOT NULL`, []string{"c"},
			func(r *rand.Rand) []string { return []string{q(words[r.Intn(len(words))] + fmt.Sprint(r.Intn(60)))} }},
	}
}

func dumpPG(ctx context.Context, db *sql.DB, table string) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT t::text FROM "+table+" t ORDER BY 1")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return "", err
		}
		out = append(out, s)
	}
	return strings.Join(out, "\n"), rows.Err()
}

// invariant: RS-1/RS-2, CP-2 — việc C mức 1, PG→PG: snapshot logic các bảng PK ghép (int4, text "C") /
// uuid / numeric / (timestamptz, int4) / bpchar "C" dưới tải ghi đồng thời, gồm UPDATE đổi PK mà cột TOAST
// (doc > 2 KB) không đổi (đường đọc lại khóa); chunk 30 row. Target cuối giống hệt source.
func TestGPK_PGToPGSnapshotUnderLoadWithGenericKeys(t *testing.T) {
	root, rootDSN := startTestDB(t, "pg")
	ctx := context.Background()
	dsn := func(db string) string { return strings.Replace(rootDSN, "/app?", "/"+db+"?", 1) }
	for _, q := range []string{"CREATE DATABASE gsrc", "CREATE DATABASE gdst"} {
		if _, err := root.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	src, _ := sql.Open("pgx", dsn("gsrc"))
	dst, _ := sql.Open("pgx", dsn("gdst"))
	defer src.Close()
	defer dst.Close()
	exec := func(db *sql.DB, q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	shapes := gpkPGShapes()
	r := rand.New(rand.NewSource(5))
	doc := strings.Repeat("toast-", 600) // > 2 KB: lưu ngoài dòng, UPDATE không đổi doc thì pgoutput không gửi
	var include []string
	for _, sh := range shapes {
		ddl := fmt.Sprintf("CREATE TABLE public.%s (%s, v int, doc text, PRIMARY KEY (%s))", sh.name, sh.cols, strings.Join(sh.pk, ","))
		exec(src, ddl)
		exec(dst, ddl)
		exec(src, fmt.Sprintf("ALTER TABLE public.%s ALTER COLUMN doc SET STORAGE EXTERNAL", sh.name)) // không nén: doc > 2 KB nằm ngoài dòng
		for i := 0; i < 300; i++ {
			exec(src, fmt.Sprintf("INSERT INTO public.%s (%s, v, doc) VALUES (%s, %d, '%s%d') ON CONFLICT DO NOTHING",
				sh.name, strings.Join(sh.pk, ","), strings.Join(sh.key(r), ","), i, doc, i))
		}
		include = append(include, "public."+sh.name)
	}
	// Bảng điều khiển riêng giữ năm bảng random đều là snapshot mới.
	exec(src, `CREATE TABLE public.uuid_reread (u uuid PRIMARY KEY, v int, doc text)`)
	exec(dst, `CREATE TABLE public.uuid_reread (u uuid PRIMARY KEY, v int, doc text)`)
	exec(src, `ALTER TABLE public.uuid_reread ALTER COLUMN doc SET STORAGE EXTERNAL`)
	include = append(include, "public.uuid_reread")
	// Khóa dự trữ ngoài tập random: giữ TOAST ngoài dòng, old thuộc plan và new ở đuôi đã xong.
	exec(src, fmt.Sprintf("INSERT INTO public.uuid_reread (u,v,doc) VALUES ('88888888-8888-8888-8888-888888888888',1,'%s')", doc))
	exec(src, "CREATE PUBLICATION gpub FOR ALL TABLES")
	// Mô phỏng bootstrap bị ngắt: checkpoint P−1 trước stream, mọi bảng vẫn chờ snapshot.
	var slotLSN string
	if err := src.QueryRowContext(ctx, "SELECT lsn::text FROM pg_create_logical_replication_slot('gpk_slot','pgoutput')").Scan(&slotLSN); err != nil {
		t.Fatal(err)
	}
	lsn, err := checkpoint.ParseLSN(slotLSN)
	if err != nil || lsn == 0 {
		t.Fatalf("invalid slot LSN %q: %v", slotLSN, err)
	}
	store := checkpoint.NewPGStore(dst, "gpkpg", model.EnginePG)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	epoch, err := store.AdvanceEpoch(ctx, "gpkpg", "fixture", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteGlobal(ctx, checkpoint.Global{PipelineID: "gpkpg", Epoch: epoch, Applied: checkpoint.NewPGAppliedSet(lsn - 1).String()}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var stopOnce sync.Once
	stopWorkload := func() { stopOnce.Do(func() { close(stop) }); wg.Wait() }
	defer stopWorkload()
	var ops atomic.Int64
	for _, sh := range shapes {
		sh := sh
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr := rand.New(rand.NewSource(int64(len(sh.name)) * 31))
			where := func(k []string) string {
				w := make([]string, len(k))
				for i, c := range sh.pk {
					w[i] = c + " = " + k[i]
				}
				return strings.Join(w, " AND ")
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := sh.key(rr)
				var q string
				switch rr.Intn(5) {
				case 0:
					q = fmt.Sprintf("INSERT INTO public.%s (%s, v, doc) VALUES (%s, 1, 'new') ON CONFLICT DO NOTHING", sh.name, strings.Join(sh.pk, ","), strings.Join(k, ","))
				case 1:
					q = fmt.Sprintf("UPDATE public.%s SET v = v + 1 WHERE %s", sh.name, where(k))
				case 2:
					q = fmt.Sprintf("DELETE FROM public.%s WHERE %s", sh.name, where(k))
				default: // đổi PK, doc (TOAST) không đổi
					nk := sh.key(rr)
					set := make([]string, len(sh.pk))
					for i, c := range sh.pk {
						set[i] = c + " = " + nk[i]
					}
					q = fmt.Sprintf("UPDATE public.%s SET %s WHERE %s AND NOT EXISTS (SELECT 1 FROM public.%s WHERE %s)",
						sh.name, strings.Join(set, ", "), where(k), sh.name, strings.ReplaceAll(where(nk), "= ", "= "))
				}
				if _, err := src.ExecContext(ctx, q); err == nil {
					ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	dir := t.TempDir()
	// Bảng thứ sáu mô phỏng plan resume chưa đọc khóa nào để ép event trước register thành nghĩa vụ re-read.
	state, err := openLocalState(dir, "gpkpg")
	if err != nil {
		t.Fatal(err)
	}
	minKey, maxKey := "00000000-0000-0000-0000-000000000000", "88888888-8888-8888-8888-888888888888"
	if err := state.SetPlan("public.uuid_reread", snapshot.SnapshotPlan{GenericKey: true, ChunkRows: 30,
		MinKey: []snapshot.KeyVal{{S: &minKey}}, MaxKey: []snapshot.KeyVal{{S: &maxKey}}}); err != nil {
		t.Fatal(err)
	}
	for _, sh := range shapes {
		name := "public." + sh.name
		if state.IsDone(name) {
			t.Fatalf("original table %s already marked done", name)
		}
		if _, ok := state.Plan(name); ok {
			t.Fatalf("original table %s has a resume plan", name)
		}
	}
	cfg := &config.Config{Name: "gpkpg",
		Source: config.SourceConfig{Kind: "postgres", DSN: dsn("gsrc"), Slot: "gpk_slot", Publication: "gpub", CreateSlot: false},
		Target: config.TargetConfig{Kind: "postgres", DSN: dsn("gdst")},
		Tables: config.TablesConfig{Include: include}}
	cfg.Resources.Spool.Path = dir
	cfg.Resources.Workers.Max = 4
	cfg.Governor.Algorithm = "static"
	cfg.Resources.Source.IOPS.Soft = 100000
	cfg.Bootstrap.Logical.ChunkRows = "30"
	var logBuf syncBuf
	gate := &gpkSnapshotGate{base: slog.NewJSONHandler(&logBuf, nil), started: make(chan struct{}), release: make(chan struct{})}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	stopped := false
	go func() {
		done <- Run(rctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: slog.New(gate)})
	}()
	defer func() {
		gate.open()
		cancel()
		if !stopped {
			<-done
		}
	}()
	select {
	case <-gate.started:
	case <-time.After(2 * time.Minute):
		t.Fatal("uuid reread snapshot did not reach test gate")
	}
	res, err := src.ExecContext(ctx, "UPDATE public.uuid_reread SET u='ffffffff-ffff-ffff-ffff-ffffffffffff' WHERE u='88888888-8888-8888-8888-888888888888'")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("controlled TOAST move affected %d rows: %v", n, err)
	}
	// Router phải ghi nghĩa vụ trước khi cho snapshot đăng ký lại plan; timeout chỉ phát hiện lỗi.
	deadlineReread := time.Now().Add(time.Minute)
	for {
		raw, err := os.ReadFile(filepath.Join(dir, "pipeline-state.json"))
		var st stateFile
		found := false
		if err == nil && json.Unmarshal(raw, &st) == nil {
			for _, key := range st.Plans["public.uuid_reread"].RereadKeyVals {
				if len(key) == 1 && key[0].S != nil && *key[0].S == "ffffffff-ffff-ffff-ffff-ffffffffffff" {
					found = true
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadlineReread) {
			t.Fatalf("controlled move was not persisted as re-read: %s", raw)
		}
		time.Sleep(10 * time.Millisecond)
	}
	gate.open()
	deadline := time.Now().Add(4 * time.Minute)
	for {
		raw, _ := os.ReadFile(filepath.Join(dir, "pipeline-state.json"))
		var st stateFile
		_ = json.Unmarshal(raw, &st)
		all := true
		for _, n := range include {
			all = all && st.Done[n]
		}
		if all {
			break
		}
		select {
		case err := <-done:
			stopped = true
			t.Fatalf("pipeline exited during snapshot: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot did not finish: %+v", st.Done)
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(time.Second)
	stopWorkload()
	for _, sh := range shapes {
		var a, b string
		var err error
		ok := false
		for i := 0; i < 120 && !ok; i++ {
			if a, err = dumpPG(ctx, src, "public."+sh.name); err != nil {
				t.Fatal(err)
			}
			if b, err = dumpPG(ctx, dst, "public."+sh.name); err != nil {
				t.Fatal(err)
			}
			if ok = a == b; !ok {
				time.Sleep(500 * time.Millisecond)
			}
		}
		if !ok {
			t.Fatalf("%s: target differs from source (%d vs %d rows)", sh.name, strings.Count(b, "\n")+1, strings.Count(a, "\n")+1)
		}
		t.Logf("%s: %d rows identical", sh.name, strings.Count(a, "\n")+1)
	}
	controlSource, err := dumpPG(ctx, src, "public.uuid_reread")
	if err != nil {
		t.Fatal(err)
	}
	controlTarget, err := dumpPG(ctx, dst, "public.uuid_reread")
	if err != nil {
		t.Fatal(err)
	}
	if controlSource != controlTarget {
		t.Fatal("uuid_reread: target differs from source")
	}
	t.Log("uuid_reread: controlled row identical")
	rereads, uuidRereads := 0, 0
	for _, line := range strings.Split(logBuf.String(), "\n") {
		var e struct {
			Msg     string `json:"msg"`
			Table   string `json:"table"`
			Rereads int    `json:"reread_keys"`
		}
		if json.Unmarshal([]byte(line), &e) == nil && e.Msg == "snapshot done" {
			rereads += e.Rereads
			if e.Table == "public.uuid_reread" {
				uuidRereads += e.Rereads
			}
		}
	}
	t.Logf("workload: %d ops, %d keys re-read (TOAST PK moves), %d control-table re-reads", ops.Load(), rereads, uuidRereads)
	if rereads == 0 || uuidRereads == 0 {
		t.Fatalf("TOAST PK-move re-read not exercised on uuid: total=%d uuid=%d", rereads, uuidRereads)
	}
}

// syncBuf: buffer log an toàn đa luồng.
type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

// Test gate chỉ chặn snapshot table đã chọn; router và reader vẫn chạy thật.
type gpkSnapshotGate struct {
	base             slog.Handler
	started, release chan struct{}
	once             sync.Once
}

func (g *gpkSnapshotGate) Enabled(ctx context.Context, level slog.Level) bool {
	return g.base.Enabled(ctx, level)
}
func (g *gpkSnapshotGate) Handle(ctx context.Context, rec slog.Record) error {
	err := g.base.Handle(ctx, rec)
	if rec.Message != "snapshot start" {
		return err
	}
	var table string
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "table" {
			table = a.Value.String()
		}
		return true
	})
	if table == "public.uuid_reread" {
		close(g.started)
		<-g.release
	}
	return err
}
func (g *gpkSnapshotGate) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &gpkSnapshotGate{base: g.base.WithAttrs(attrs), started: g.started, release: g.release}
}
func (g *gpkSnapshotGate) WithGroup(name string) slog.Handler {
	return &gpkSnapshotGate{base: g.base.WithGroup(name), started: g.started, release: g.release}
}
func (g *gpkSnapshotGate) open() { g.once.Do(func() { close(g.release) }) }
