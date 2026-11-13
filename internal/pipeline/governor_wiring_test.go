package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"strand/internal/config"
	"strand/internal/governor"
	"strand/internal/model"
	"strand/internal/sched"
	"strand/internal/snapshot"
)

// Backlog #31 / review N11: nối đầu ra của governor vào actuator thật của pipeline và cho source PG
// chạy onoff / grace-v2. Toàn bộ test ở đây không cần DB (driver SQL giả).

// --- driver SQL giả cho các câu tín hiệu PG ---

type wiringConnector struct {
	answer func(q string) ([]string, []driver.Value, error)
}

func (c wiringConnector) Connect(context.Context) (driver.Conn, error) { return wiringConn(c), nil }
func (c wiringConnector) Driver() driver.Driver                        { return wiringDriver(c) }

type wiringDriver wiringConnector

func (d wiringDriver) Open(string) (driver.Conn, error) { return wiringConn(d), nil }

type wiringConn wiringConnector

func (c wiringConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c wiringConn) Close() error                        { return nil }
func (c wiringConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c wiringConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	cols, row, err := c.answer(q)
	if err != nil {
		return nil, err
	}
	r := &wiringRows{cols: cols}
	if row != nil {
		r.rows = [][]driver.Value{row}
	}
	return r, nil
}

type wiringRows struct {
	cols []string
	rows [][]driver.Value
}

func (r *wiringRows) Columns() []string { return r.cols }
func (r *wiringRows) Close() error      { return nil }
func (r *wiringRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

// fakePGSource: source PG đọc được mọi view; slot (nếu có) cách head sentLag byte, giữ retained byte WAL.
func fakePGSource(fail error, keepMB string, retained, sentLag float64) *sql.DB {
	return sql.OpenDB(wiringConnector{answer: func(q string) ([]string, []driver.Value, error) {
		if fail != nil {
			return nil, nil, fail
		}
		switch {
		case strings.Contains(q, "pg_has_role"):
			return []string{"m"}, []driver.Value{true}, nil
		case strings.Contains(q, "FROM pg_replication_slots s WHERE s.slot_name"):
			return []string{"a", "b", "c"}, []driver.Value{"reserved", retained, sentLag}, nil
		case strings.Contains(q, "r.replay_lsn IS NOT NULL"):
			return []string{"replays", "caught_up", "lag"}, nil, nil // không có standby vật lý
		case strings.Contains(q, "pg_settings"):
			return []string{"keep"}, []driver.Value{keepMB}, nil
		case strings.Contains(q, "pg_is_in_recovery()"):
			return []string{"a", "b"}, []driver.Value{false, 0.0}, nil
		}
		return nil, nil, errors.New("unexpected query")
	}})
}

func wiringPipeline(src model.Engine, algo string) *Pipeline {
	p := &Pipeline{cfg: &config.Config{}, srcEngine: src, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		govTick: 10 * time.Millisecond}
	p.cfg.Governor.Algorithm = algo
	p.cfg.Resources.Source.IOPS.Soft = 600
	p.cfg.Resources.Source.IOPS.Hard = 1200
	return p
}

// --- chọn governor ---

func TestSelectGovernorAlgo(t *testing.T) {
	for in, want := range map[string]string{"": "onoff", "ONOFF": "onoff", " static ": "static", "grace-v2": "grace-v2"} {
		if got, err := selectGovernorAlgo(in); err != nil || got != want {
			t.Fatalf("selectGovernorAlgo(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := selectGovernorAlgo("pid"); err == nil {
		t.Fatal("unknown algorithm must be rejected")
	}
}

// Regression backlog #31: source PG không còn bị ép về static khi đọc được tín hiệu.
func TestSetupGovernor_PGSourceRunsRequestedAlgorithm(t *testing.T) {
	for _, algo := range []string{"grace-v2", "onoff"} {
		p := wiringPipeline(model.EnginePG, algo)
		p.src = fakePGSource(nil, "-1", 0, 0)
		if err := p.setupGovernor(context.Background()); err != nil {
			t.Fatal(err)
		}
		if p.govAlgo != algo {
			t.Fatalf("PG source with governor.algorithm=%s runs %q", algo, p.govAlgo)
		}
		if _, isGrace := p.gov.(*governor.GraceGovernor); isGrace != (algo == "grace-v2") {
			t.Fatalf("%s: governor type %T", algo, p.gov)
		}
		if p.pgSignals == nil {
			t.Fatalf("%s: PG signal reader not set", algo)
		}
		if err := p.gov.Tick(context.Background()); err != nil {
			t.Fatalf("%s: tick on PG signals: %v", algo, err)
		}
		if !p.adaptiveChunks {
			t.Fatalf("%s with chunk_rows auto must size chunks adaptively", algo)
		}
	}
}

// Thiếu quyền trên view thống kê: cảnh báo và static, không chạy mù.
func TestSetupGovernor_PGSourceWithoutSignalAccessFallsBackToStatic(t *testing.T) {
	p := wiringPipeline(model.EnginePG, "grace-v2")
	p.src = fakePGSource(errors.New("permission denied for view pg_stat_replication (SQLSTATE 42501)"), "-1", 0, 0)
	if err := p.setupGovernor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.govAlgo != "static" || p.adaptiveChunks {
		t.Fatalf("unreadable PG signals: algo=%q adaptive=%v, want static and fixed chunks", p.govAlgo, p.adaptiveChunks)
	}
}

func TestSetupGovernor_StaticAndFixedChunkRowsKeepFixedChunks(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "static")
	if err := p.setupGovernor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.adaptiveChunks {
		t.Fatal("static must keep the configured chunk size (benchmark baseline)")
	}
	p = wiringPipeline(model.EngineMySQL, "grace-v2")
	p.cfg.Bootstrap.Logical.ChunkRows = "500"
	if err := p.setupGovernor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.adaptiveChunks || p.govAlgo != "grace-v2" {
		t.Fatalf("explicit chunk_rows must stay fixed: adaptive=%v algo=%q", p.adaptiveChunks, p.govAlgo)
	}
}

type fakeProber struct {
	called bool
	warns  []string
	err    error
}

func (f *fakeProber) ReadSignals(context.Context) (governor.DBSignals, error) {
	return governor.DBSignals{}, nil
}
func (f *fakeProber) Probe(context.Context) ([]string, error) { f.called = true; return f.warns, f.err }

func TestProbeSignals(t *testing.T) {
	p := wiringPipeline(model.EnginePG, "grace-v2")
	fp := &fakeProber{}
	if got := p.probeSignals(context.Background(), "static", fp); got != "static" || fp.called {
		t.Fatalf("static must not probe: got %q called=%v", got, fp.called)
	}
	if got := p.probeSignals(context.Background(), "onoff", &fakeProber{warns: []string{"hidden"}}); got != "onoff" {
		t.Fatalf("warnings must not change the algorithm: %q", got)
	}
	if got := p.probeSignals(context.Background(), "grace-v2", &fakeProber{err: errors.New("denied")}); got != "static" {
		t.Fatalf("probe error must fall back to static: %q", got)
	}
}

// --- P_lag số hạng WAL và "reader đã đuổi kịp" của source PG ---

func TestPGSignals_WALPressureAndCaughtUp(t *testing.T) {
	p := wiringPipeline(model.EnginePG, "grace-v2")
	p.src = fakePGSource(nil, "1024", float64(900<<20), 4096) // giữ 900 MiB / 1 GiB; walsender cách head 4 KiB
	if err := p.setupGovernor(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := p.walRetainedPressure(); got < 0.87 || got > 0.88 {
		t.Fatalf("WAL term of P_lag = %v, want 900/1024", got)
	}
	// Regression: trước đây caughtUpWithSource luôn false với PG → source rảnh bị coi là lag tăng mãi.
	if !p.caughtUpWithSource(context.Background()) {
		t.Fatal("walsender at head must count as caught up")
	}
	p.src = fakePGSource(nil, "1024", 0, float64(64<<20))
	p.pgSignals = governor.NewPGDBSignalReader(p.src, "s", nil)
	if p.caughtUpWithSource(context.Background()) {
		t.Fatal("walsender 64 MiB behind head is not caught up")
	}
	if (&Pipeline{srcEngine: model.EngineMySQL}).walRetainedPressure() != 0 {
		t.Fatal("MySQL source has no WAL term")
	}
}

// --- cửa sổ L → SetWindowSaturated ---

func TestFeedGrace_WindowSaturatedFollowsSnapshotReaders(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "grace-v2")
	p.sched = sched.NewScheduler(sched.DefaultConfig())
	gg := governor.NewGraceGovernor(governor.GraceConfig{SoftRate: 600})
	p.gov = gg
	p.feedGrace(context.Background(), gg)
	if gg.WindowSaturated() {
		t.Fatal("no snapshot reader: window is not used, Green must not grow R")
	}
	p.snapReaders.Store(1)
	p.feedGrace(context.Background(), gg)
	if !gg.WindowSaturated() {
		t.Fatal("one running reader fills the single-reader window")
	}
	p.snapGated.Store(1) // reader bị giữ ở cổng (pause / windows / breaker)
	p.feedGrace(context.Background(), gg)
	if gg.WindowSaturated() {
		t.Fatal("a gated reader does not use the window")
	}
}

func TestPaceGate_CountsGatedReaders(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "static")
	p.govAlgo = "static"
	p.snapPaused.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.pace(ctx, 0) }()
	deadline := time.Now().Add(2 * time.Second)
	for p.snapGated.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("paused reader not counted as gated")
		}
		time.Sleep(time.Millisecond)
	}
	p.snapPaused.Store(false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if g := p.snapGated.Load(); g != 0 {
		t.Fatalf("gated count after resume = %d, want 0", g)
	}
}

// --- giãn nhịp: bootstrap_retention_risk, hard limit, token bucket chung ---

// Regression review N11: nhánh retentionRisk trả về TRƯỚC bước giãn nhịp nên snapshot chạy không giới
// hạn (vượt hard limit). Nay retention risk chỉ bỏ qua windows / background dừng, vẫn giãn theo R.
func TestPace_RetentionRiskStillRateLimited(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "static")
	p.govAlgo = "static"
	p.cfg.Resources.Source.IOPS.Hard = 0
	p.setSoftRate(10) // 10 trang/giây → chunk 1 trang chờ 100 ms
	p.retentionRisk.Store(true)
	p.cfg.Bootstrap.Logical.Windows = []string{"00:00-00:00"} // không bao giờ mở: retention risk bỏ qua
	start := time.Now()
	if err := p.pace(context.Background(), 16384); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 80*time.Millisecond {
		t.Fatalf("pace under retention risk returned after %v: snapshot is not rate limited", el)
	}
}

func TestBackgroundRate(t *testing.T) {
	// static: soft hiện hành, kể cả khi retention risk.
	p := wiringPipeline(model.EngineMySQL, "static")
	p.govAlgo = "static"
	p.setSoftRate(600)
	p.retentionRisk.Store(true)
	if got := p.backgroundRate(); got != 600 {
		t.Fatalf("static rate = %v, want soft 600", got)
	}
	// onoff dừng background (P ≥ 1) nhưng retention risk: tăng ưu tiên tới hard limit.
	p = wiringPipeline(model.EngineMySQL, "onoff")
	p.govAlgo = "onoff"
	sim := governor.NewSimulatedSignalReader(governor.DBSignals{ReplicaLagSeconds: 100})
	og := governor.NewOnoffGovernor(governor.OnoffConfig{SoftRate: 600, HardRate: 1200, LagSLO: 5 * time.Second, SignalReader: sim})
	p.gov = og
	p.setSoftRate(600)
	for i := 0; i < 5; i++ {
		_ = og.Tick(context.Background())
	}
	if og.BackgroundRunning() {
		t.Fatal("test setup: onoff must have stopped the background")
	}
	if got := p.backgroundRate(); got != 600 {
		t.Fatalf("onoff stopped, no retention risk: rate = %v, want soft fallback 600", got)
	}
	p.retentionRisk.Store(true)
	if got := p.backgroundRate(); got != 1200 {
		t.Fatalf("onoff under retention risk: rate = %v, want hard 1200", got)
	}
	// Mọi governor bị chặn bởi hard limit ở token bucket.
	p = wiringPipeline(model.EngineMySQL, "grace-v2")
	p.govAlgo = "grace-v2"
	p.gov = governor.NewGraceGovernor(governor.GraceConfig{SoftRate: 600, HardRate: 1200})
	p.cfg.Resources.Source.IOPS.Hard = 300
	if got := p.backgroundRate(); got != 300 {
		t.Fatalf("rate above hard limit = %v, want clamped to 300", got)
	}
}

func TestTokenBucket_SharedAcrossReaders(t *testing.T) {
	var b tokenBucket
	t0 := time.Unix(1000, 0)
	d := 100 * time.Millisecond
	// Một reader: chờ đúng d sau mỗi chunk (hành vi cũ).
	if w := b.reserve(t0, d); w != d {
		t.Fatalf("first wait = %v, want %v", w, d)
	}
	if w := b.reserve(t0.Add(d+50*time.Millisecond), d); w != d {
		t.Fatalf("single reader second wait = %v, want %v", w, d)
	}
	// Hai reader xong chunk cùng lúc: khoản chờ cộng dồn → tổng tốc độ ≤ R.
	var c tokenBucket
	if w1, w2 := c.reserve(t0, d), c.reserve(t0, d); w1 != d || w2 != 2*d {
		t.Fatalf("concurrent waits = %v, %v; want %v, %v", w1, w2, d, 2*d)
	}
}

// --- cỡ chunk: luật 250 ms và trần chi phí R · 0,25 s ---

func TestNextChunkRows_RuleAndCostCap(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "static")
	p.govAlgo = "static"
	p.setSoftRate(600) // trần chi phí 150 trang/chunk
	id := model.TableID{Schema: "s", Table: "t"}
	// Chunk chậm (500 ms) → giảm một nửa.
	if got := p.nextChunkRows(id, snapshot.ChunkStat{Rows: 1000, ReadRows: 1000, Bytes: 100_000, Elapsed: 500 * time.Millisecond}); got != 500 {
		t.Fatalf("slow chunk: next rows = %d, want 500", got)
	}
	if p.snapChunkRows.Load() != 500 {
		t.Fatal("effective chunk rows (cost fallback of pace) not updated")
	}
	// Chunk nhanh (50 ms) → gấp đôi.
	if got := p.nextChunkRows(id, snapshot.ChunkStat{Rows: 1000, ReadRows: 1000, Bytes: 100_000, Elapsed: 50 * time.Millisecond}); got != 2000 {
		t.Fatalf("fast chunk: next rows = %d, want 2000", got)
	}
	// Row 16 KiB = 1 trang: chi phí ≤ 150 trang → ≤ 150 row.
	if got := p.nextChunkRows(id, snapshot.ChunkStat{Rows: 1000, ReadRows: 1000, Bytes: 1000 * 16384, Elapsed: 50 * time.Millisecond}); got != 150 {
		t.Fatalf("cost cap: next rows = %d, want 150", got)
	}
	// Không có byte thực: AVG_ROW_LENGTH.
	p.curAvgRow.Store(16384)
	if got := p.nextChunkRows(id, snapshot.ChunkStat{Rows: 1000, Elapsed: 250 * time.Millisecond}); got != 150 {
		t.Fatalf("avg-row cost cap: next rows = %d, want 150", got)
	}
}
