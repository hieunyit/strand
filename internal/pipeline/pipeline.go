// Package pipeline nối các tầng của Strand thành một pipeline MySQL→MySQL chạy được
// (design §1, spec 02 §3–§6): Reader → Txn Assembler → Router (lọc, rename, bỏ Txn đã
// apply, range-state của snapshot) → Scheduler → Writer pool → Coordinator (compaction).
package pipeline

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"

	"strand/internal/admin"
	"strand/internal/assemble"
	"strand/internal/checkpoint"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/governor"
	"strand/internal/metrics"
	"strand/internal/model"
	mysqlreader "strand/internal/reader/mysql"
	"strand/internal/sched"
	"strand/internal/seed"
	"strand/internal/snapshot"
	"strand/internal/spool"
	"strand/internal/writer"
)

// Options là tham số chạy một pipeline.
type Options struct {
	Config       *config.Config
	StartAt      string // "" | "seed" | GTID set (chế độ C)
	SeedManifest string // đường dẫn seed.json khi StartAt = "seed"
	Logger       *slog.Logger
	// ShutdownTimeout: thời gian tối đa chờ writer xong khi nhận SIGTERM (03 §4).
	ShutdownTimeout time.Duration
}

// Pipeline là một lần chạy `strand sync`.
type Pipeline struct {
	opts   Options
	cfg    *config.Config
	log    *slog.Logger
	id     string
	strict bool

	src, dst *sql.DB
	srcDSN   string

	store   *checkpoint.MySQLStore
	release func() error
	epoch   int64

	filter   *tableFilter
	tablesMu sync.RWMutex
	tables   map[model.TableID]*tableMeta
	state    *localState
	applied0 *checkpoint.MySQLAppliedSet // tập đã apply lúc khởi động (CP-2)

	sched   *sched.Scheduler
	comp    *checkpoint.Compactor
	orderer *writer.CommitOrderer
	writers []*writer.MySQLWriter
	snap    *snapshot.MySQLSnapshotter

	metrics *metrics.Metrics
	health  *metrics.HealthChecker

	routeMu  sync.Mutex // giữ trong lúc route một Txn; barrier của snapshot lấy khóa này
	seenMu   sync.Mutex
	seenCond *sync.Cond
	seen     *checkpoint.MySQLAppliedSet // applied0 ∪ mọi Txn router đã xử lý

	admitted      atomic.Uint64 // seq lớn nhất đã Admit
	appliedTxns   atomic.Int64
	appliedRows   atomic.Int64
	lastSourceTS  atomic.Int64 // SourceTS của Txn mới nhất đã commit ở target
	lastRoutedTS  atomic.Int64
	paused        atomic.Bool
	pauseMu       sync.Mutex
	pauseCond     *sync.Cond
	bootstrap     atomic.Bool
	retentionRisk atomic.Bool
	verifying     atomic.Bool // chờ verify sau seed (02 §9)
	prefetchDir   string
	prefetch      *spool.PebbleSpool
	gov           *governor.OnoffGovernor
	govAlgo       string
	govTick       time.Duration
	curAvgRow     atomic.Int64
	touched       *touchedKeys

	runCtx context.Context

	failMu  sync.Mutex
	failErr error
	cancel  context.CancelFunc
}

// Run chạy pipeline đến khi ctx bị hủy (dừng an toàn) hoặc gặp lỗi không tự phục hồi.
func Run(ctx context.Context, opts Options) error {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 100 * time.Second
	}
	cfg := opts.Config
	if !strings.EqualFold(cfg.Source.Kind, "mysql") || !strings.EqualFold(cfg.Target.Kind, "mysql") {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			"this build supports MySQL → MySQL pipelines only (PostgreSQL is Phase 2)", nil)
	}
	p := &Pipeline{
		opts:   opts,
		cfg:    cfg,
		log:    opts.Logger.With("pipeline", cfg.Name),
		id:     cfg.Name,
		strict: !strings.EqualFold(cfg.Target.CommitOrder, "relaxed"),
	}
	p.seenCond = sync.NewCond(&p.seenMu)
	p.pauseCond = sync.NewCond(&p.pauseMu)
	p.touched = newTouchedKeys(200000)
	return p.run(ctx)
}

func (p *Pipeline) fail(err error) {
	if err == nil {
		return
	}
	p.failMu.Lock()
	first := p.failErr == nil
	if first {
		p.failErr = err
	}
	p.failMu.Unlock()
	if first {
		p.log.Error("pipeline failed", "error", err)
		if p.health != nil {
			p.health.SetFailed(err.Error())
		}
		if exitcode.GetExitCode(err) == exitcode.Fenced && p.metrics != nil {
			p.metrics.FencedTotal.WithLabelValues(p.id).Inc()
		}
		if p.cancel != nil {
			p.cancel()
		}
	}
	p.seenMu.Lock()
	p.seenCond.Broadcast()
	p.seenMu.Unlock()
	p.pauseMu.Lock()
	p.pauseCond.Broadcast()
	p.pauseMu.Unlock()
}

func (p *Pipeline) failed() error {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	return p.failErr
}

func targetDSN(dsn string) (string, error) {
	c, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	if c.Params == nil {
		c.Params = map[string]string{}
	}
	// Giá trị thời gian đến từ binlog dưới dạng chuỗi UTC (reader đặt TimestampStringLocation=UTC).
	c.Params["time_zone"] = "'+00:00'"
	// Trong bootstrap, row con có thể tới trước row cha (range của bảng cha "Chưa đọc"):
	// target chưa nhất quán về FK cho tới khi snapshot xong. Khi stream, thứ tự cha–con do
	// writeset (FK) bảo đảm. Vì vậy phiên ghi của Strand không kiểm FK (như applier sao chép).
	c.Params["foreign_key_checks"] = "0"
	return c.FormatDSN(), nil
}

func durationOr(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(strings.TrimSpace(s)); err == nil && d > 0 {
		return d
	}
	return def
}

func (p *Pipeline) run(parent context.Context) (retErr error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	p.cancel = cancel
	p.runCtx = ctx

	var err error
	if p.filter, err = newTableFilter(p.cfg.Tables.Include, p.cfg.Tables.Exclude, p.cfg.Tables.Rename); err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
	}

	// --- Kết nối ---
	p.srcDSN = p.cfg.Source.DSN
	if p.src, err = sql.Open("mysql", p.srcDSN); err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid source DSN: %v", err), err)
	}
	defer p.src.Close()
	dstDSN, err := targetDSN(p.cfg.Target.DSN)
	if err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid target DSN: %v", err), err)
	}
	if p.dst, err = sql.Open("mysql", dstDSN); err != nil {
		return err
	}
	defer p.dst.Close()
	p.dst.SetMaxOpenConns(64)

	// --- Khóa pipeline, epoch (02 §4 quy tắc 1–2) ---
	p.store = checkpoint.NewMySQLStore(p.dst, p.id, model.EngineMySQL)
	if err := p.store.EnsureTables(ctx); err != nil {
		return fmt.Errorf("ensure metadata tables: %w", err)
	}
	if p.release, err = p.store.Lock(ctx, p.id); err != nil {
		return err
	}
	defer func() { _ = p.release() }()
	host, _ := os.Hostname()
	if p.epoch, err = p.store.AdvanceEpoch(ctx, p.id, fmt.Sprintf("%s/%d", host, os.Getpid()), configHash(p.cfg)); err != nil {
		return fmt.Errorf("advance epoch: %w", err)
	}
	p.log = p.log.With("epoch", p.epoch)
	p.log.Info("pipeline lock acquired")

	// --- Metadata source ---
	if p.tables, err = loadSourceTables(ctx, p.src, p.filter); err != nil {
		return fmt.Errorf("load source schema: %w", err)
	}
	if len(p.tables) == 0 {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "no source table matches tables.include", nil)
	}
	for id, m := range p.tables {
		if len(m.PK) == 0 {
			return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf("table %s has no primary key", id), nil)
		}
	}
	if p.state, err = openLocalState(p.cfg.Resources.Spool.Path, p.id); err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
	}

	// --- Tập đã apply và điểm bắt đầu ---
	loaded, err := p.store.LoadApplied(ctx)
	if err != nil {
		return fmt.Errorf("load applied set: %w", err)
	}
	applied := loaded.(*checkpoint.MySQLAppliedSet)
	if applied, err = p.initialPosition(ctx, applied); err != nil {
		return err
	}
	p.applied0 = applied.Clone().(*checkpoint.MySQLAppliedSet)
	p.seen = applied.Clone().(*checkpoint.MySQLAppliedSet)
	// Spool prefetch của `seed apply` (02 §9): phủ phần log từ P mà source/archive có thể đã xóa.
	p.prefetchDir = filepath.Join(p.cfg.Resources.Spool.Path, "prefetch")
	streamFrom := applied.Clone().(*checkpoint.MySQLAppliedSet)
	if fi, err := os.Stat(p.prefetchDir); err == nil && fi.IsDir() {
		if p.prefetch, err = spool.New(p.prefetchDir); err != nil {
			return fmt.Errorf("open prefetch spool: %w", err)
		}
		defer p.prefetch.Close()
		it, err := p.prefetch.Iter(ctx, model.Pos{})
		if err != nil {
			return err
		}
		n := 0
		for {
			t, err := it.Next(ctx)
			if err != nil {
				break
			}
			streamFrom.Add(t.SourcePos)
			n++
		}
		it.Close()
		p.log.Info("prefetch spool found", "transactions", n)
	}
	if err := p.checkLogAvailable(ctx, streamFrom); err != nil {
		return err
	}

	// Bảng không còn trong bộ lọc: quên trạng thái "đã snapshot" (thêm lại sau này phải snapshot lại,
	// vì event của nó đã bị bỏ qua trong thời gian bị loại).
	for _, name := range p.state.DoneTables() {
		s, t, _ := strings.Cut(name, ".")
		if _, ok := p.tables[model.TableID{Schema: s, Table: t}]; !ok {
			if err := p.state.SetDone(name, false); err != nil {
				return err
			}
		}
	}
	var pending []model.TableID
	for id := range p.tables {
		if !p.state.IsDone(id.String()) {
			pending = append(pending, id)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].String() < pending[j].String() })
	for _, id := range pending {
		if !p.tables[id].PKNumeric {
			return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected,
				fmt.Sprintf("table %s needs a logical snapshot but its primary key is not a single integer column (not supported yet)", id), nil)
		}
	}

	// --- Observability ---
	p.metrics = metrics.NewMetrics(prometheus.NewRegistry())
	p.health = metrics.NewHealthChecker(2 * time.Minute)
	if addr := p.cfg.Observability.Metrics; addr != "" {
		ms := metrics.NewServer(addr, p.metrics, p.health, p.log)
		if err := ms.Start(); err != nil {
			return fmt.Errorf("start metrics server: %w", err)
		}
		defer ms.Close()
	}
	if sock := p.cfg.Observability.AdminSocket; sock != "" {
		as := admin.NewServer(sock, &adminHandler{p: p}, p.log)
		if err := as.Start(); err != nil {
			return fmt.Errorf("start admin socket: %w", err)
		}
		defer as.Close()
	}

	// --- Scheduler, writer, coordinator ---
	p.sched = sched.NewScheduler(sched.DefaultConfig())
	nWorkers := p.cfg.Resources.Workers.Max
	if nWorkers <= 0 {
		nWorkers = 8
	}
	if p.strict {
		p.orderer = writer.NewCommitOrderer("strict", 1)
	}
	for i := 1; i <= nWorkers; i++ {
		w := writer.NewMySQLWriter(p.dst, p.id, i, p.epoch)
		for id, m := range p.tables {
			w.SetPrimaryKey(p.filter.Target(id), m.PK)
		}
		if p.orderer != nil {
			w.SetCommitOrderer(p.orderer)
		}
		if err := w.EnsureWorkerRow(ctx); err != nil {
			return err
		}
		p.writers = append(p.writers, w)
	}
	ccfg := checkpoint.DefaultCompactorConfig(p.id, p.epoch)
	ccfg.Interval = durationOr(p.cfg.Checkpoint.FlushEvery, 500*time.Millisecond)
	p.comp = checkpoint.NewCompactor(p.dst, p.store, p.id, model.EngineMySQL, model.EngineMySQL, ccfg)
	// Gộp row delta còn lại từ lần chạy trước vào row toàn cục TRƯỚC khi worker chạy.
	// Worker cắt delta theo LastGlobal, nên LastGlobal chỉ được là row toàn cục đã commit,
	// không bao giờ là "toàn cục ∪ delta" (nếu không, GTID chỉ có trong delta cũ bị cắt mất
	// khi worker ghi đè row delta của mình: vi phạm CP-1, Txn bị apply lại sau restart).
	if err := p.comp.Compact(ctx); err != nil {
		return fmt.Errorf("startup compaction: %w", err)
	}
	p.comp.Start(ctx)
	defer p.comp.Stop()

	// --- Governor ---
	if err := p.setupGovernor(); err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
	}
	// Tick đầu tiên trước khi snapshot chạy: breaker phải có hiệu lực ngay từ chunk đầu.
	if p.govAlgo != "static" {
		if err := p.gov.Tick(ctx); err != nil {
			p.log.Warn("initial governor tick failed", "error", err)
		}
	}

	// --- Snapshot ---
	opts := snapshot.DefaultChunkOptions()
	p.snap = snapshot.NewMySQLSnapshotter(opts)
	p.snap.Barrier = p.barrier
	p.snap.Pace = p.pace
	p.snap.TargetName = p.filter.Target
	p.snap.LoadPlan = func(t model.TableID) (snapshot.SnapshotPlan, bool) { return p.state.Plan(t.String()) }
	p.snap.SavePlan = func(t model.TableID, pl snapshot.SnapshotPlan) error { return p.state.SetPlan(t.String(), pl) }

	// --- Reader + Assembler ---
	keyMeta := func(id model.TableID) (assemble.KeyMeta, bool) {
		m := p.table(id)
		if m == nil {
			return assemble.KeyMeta{}, false
		}
		return m.Keys, true
	}
	spillDir := filepath.Join(p.cfg.Resources.Spool.Path, "spill")
	if err := os.MkdirAll(spillDir, 0o750); err != nil {
		return err
	}
	rd, err := mysqlreader.New(mysqlreader.Config{DSN: p.srcDSN, ServerID: p.cfg.Source.ServerID})
	if err != nil {
		return err
	}
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	defer rd.Close()
	asm := assemble.New(assemble.WithKeyMetadataFunc(keyMeta), assemble.WithSpillDir(spillDir))
	for id, m := range p.tables {
		asm.Registry().RegisterTable(id.Schema, id.Table, m.PK, m.Columns)
	}
	// Nguồn Txn cho router: spool prefetch (nếu có) trước, rồi stream trực tiếp từ
	// applied ∪ spool (server gửi đúng phần còn thiếu theo GTID).
	txns := make(chan *model.Txn, 128)
	asmErr := make(chan error, 1)
	go func() {
		defer close(txns)
		if p.prefetch != nil {
			if err := p.drainPrefetch(rctx, txns); err != nil {
				asmErr <- err
				return
			}
		}
		events, err := rd.Start(rctx, streamFrom)
		if err != nil {
			asmErr <- err
			return
		}
		live, liveErr := asm.Process(rctx, events)
		for {
			select {
			case <-rctx.Done():
				return
			case err, ok := <-liveErr:
				if ok && err != nil {
					asmErr <- err
					return
				}
				liveErr = nil
			case t, ok := <-live:
				if !ok {
					return
				}
				select {
				case txns <- t:
				case <-rctx.Done():
					return
				}
			}
		}
	}()

	// --- Goroutine ---
	var workersWG, bgWG sync.WaitGroup
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	for i, w := range p.writers {
		workersWG.Add(1)
		go func(i int, w *writer.MySQLWriter) {
			defer workersWG.Done()
			p.worker(wctx, i+1, w)
		}(i, w)
	}
	routerDone := make(chan struct{})
	go func() {
		defer close(routerDone)
		p.router(ctx, txns, asmErr)
	}()
	bgWG.Add(3)
	go func() { defer bgWG.Done(); p.governorLoop(ctx) }()
	go func() { defer bgWG.Done(); p.monitor(ctx) }()
	go func() { defer bgWG.Done(); p.watchCompactor(ctx) }()
	if strings.EqualFold(p.cfg.Verify.Delta, "on") {
		p.touched.enable(true)
	}
	if p.state.VerifyPending() {
		p.verifying.Store(true)
		bgWG.Add(1)
		go func() { defer bgWG.Done(); p.postSeedVerify(ctx) }()
	}
	if len(pending) > 0 {
		p.bootstrap.Store(true)
		bgWG.Add(1)
		go func() { defer bgWG.Done(); p.runSnapshots(ctx, pending) }()
	}
	p.log.Info("pipeline running", "workers", nWorkers, "strict", p.strict, "snapshot_pending", len(pending),
		"start", applied.String())

	// --- Chờ dừng ---
	select {
	case <-parent.Done():
		p.log.Info("stop requested: draining")
		rcancel()
		<-routerDone
		p.drain(p.opts.ShutdownTimeout)
	case <-ctx.Done():
	}
	if err := p.failed(); err != nil {
		cancel()
		wcancel()
		workersWG.Wait()
		return err
	}
	wcancel()
	workersWG.Wait()
	cancel()
	bgWG.Wait()

	// Ghi checkpoint lần cuối trước khi thoát (01 §3: dừng an toàn).
	fctx, fcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer fcancel()
	p.comp.Stop()
	if err := p.comp.Compact(fctx); err != nil {
		return fmt.Errorf("final checkpoint flush: %w", err)
	}
	p.log.Info("pipeline stopped cleanly")
	return nil
}

func configHash(cfg *config.Config) string {
	raw, _ := json.Marshal(cfg)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:32]
}

// initialPosition quyết định điểm bắt đầu cho pipeline mới (chế độ A/B/C, 02 §1, §10).
func (p *Pipeline) initialPosition(ctx context.Context, applied *checkpoint.MySQLAppliedSet) (*checkpoint.MySQLAppliedSet, error) {
	empty := applied.String() == ""
	if p.opts.StartAt != "" {
		if !empty {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
				"--start-at is only valid for a new pipeline; a checkpoint already exists (use strand checkpoint rewind)", nil)
		}
		var setStr string
		if p.opts.StartAt == "seed" {
			path := p.opts.SeedManifest
			if path == "" {
				path = "seed.json"
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, fmt.Sprintf("read seed manifest: %v", err), err)
			}
			var m seed.SeedManifest
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, fmt.Sprintf("parse seed manifest: %v", err), err)
			}
			if m.LogCoverage != nil && !m.LogCoverage.OK {
				return nil, exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "seed manifest reports log coverage not OK", nil)
			}
			setStr = m.StartPosition.GTIDSet
		} else {
			setStr = p.opts.StartAt
		}
		set, err := checkpoint.ParseMySQLGTIDSet(setStr)
		if err != nil || set.String() == "" {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid start position %q: %v", setStr, err), err)
		}
		// Dữ liệu đã có sẵn ở target (seed hoặc --start-at): không snapshot, nhưng phải verify
		// trước khi chuyển sang synced (02 §9; chế độ C: verify bắt buộc).
		if err := p.state.SetVerifyPending(true); err != nil {
			return nil, err
		}
		for id := range p.tables {
			if err := p.state.SetDone(id.String(), true); err != nil {
				return nil, err
			}
		}
		return set, p.writeGlobal(ctx, set)
	}
	if !empty {
		return applied, nil
	}
	mode := strings.ToLower(p.cfg.Bootstrap.Default)
	switch mode {
	case "", "logical":
	case "seed", "start_at":
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
			fmt.Sprintf("bootstrap.default=%s requires --start-at for a new pipeline", mode), nil)
	default:
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, "unknown bootstrap.default "+mode, nil)
	}
	// Chế độ A: stream từ vị trí hiện tại, mọi bảng được snapshot logic.
	var executed string
	if err := p.src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return nil, fmt.Errorf("read source gtid_executed: %w", err)
	}
	set, err := checkpoint.ParseMySQLGTIDSet(executed)
	if err != nil {
		return nil, err
	}
	for id := range p.tables {
		if err := p.state.SetDone(id.String(), false); err != nil {
			return nil, err
		}
	}
	if set.String() == "" {
		return set, nil // source chưa có transaction nào
	}
	return set, p.writeGlobal(ctx, set)
}

func (p *Pipeline) writeGlobal(ctx context.Context, set *checkpoint.MySQLAppliedSet) error {
	err := p.store.WriteGlobal(ctx, checkpoint.Global{PipelineID: p.id, Epoch: p.epoch, Applied: set.String()})
	if err != nil {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("write initial checkpoint: %v", err), err)
	}
	return nil
}

// checkLogAvailable: mọi GTID source đã purge phải nằm trong tập đã apply (SD-2, log_gap).
func (p *Pipeline) checkLogAvailable(ctx context.Context, applied *checkpoint.MySQLAppliedSet) error {
	var purged string
	if err := p.src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_purged").Scan(&purged); err != nil {
		return fmt.Errorf("read source gtid_purged: %w", err)
	}
	ps, err := checkpoint.ParseMySQLGTIDSet(purged)
	if err != nil {
		return err
	}
	ps.Subtract(applied)
	if missing := ps.String(); missing != "" {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("log gap: source already purged %s which was never applied", missing), nil)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

func (p *Pipeline) router(ctx context.Context, txns <-chan *model.Txn, asmErr <-chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-asmErr:
			if ok && err != nil {
				// Lỗi đọc binlog/lắp Txn: dừng, supervisor khởi động lại và resume theo GTID.
				p.fail(fmt.Errorf("binlog stream: %w", err))
				return
			}
			asmErr = nil
		case t, ok := <-txns:
			if !ok {
				return
			}
			p.waitIfPaused(ctx)
			if ctx.Err() != nil {
				return
			}
			p.routeMu.Lock()
			err := p.route(ctx, t)
			p.routeMu.Unlock()
			if err != nil {
				if ctx.Err() == nil {
					p.fail(err)
				}
				return
			}
		}
	}
}

func (p *Pipeline) markSeen(pos model.Pos) {
	p.seenMu.Lock()
	p.seen.Add(pos)
	p.seenCond.Broadcast()
	p.seenMu.Unlock()
}

func (p *Pipeline) table(id model.TableID) *tableMeta {
	p.tablesMu.RLock()
	defer p.tablesMu.RUnlock()
	return p.tables[id]
}

func (p *Pipeline) ddlTouchesIncluded(t *model.Txn) bool {
	q := strings.ToLower(t.DDL)
	p.tablesMu.RLock()
	defer p.tablesMu.RUnlock()
	for id := range p.tables {
		if strings.Contains(q, strings.ToLower(id.Table)) {
			return true
		}
	}
	return false
}

func (p *Pipeline) route(ctx context.Context, t *model.Txn) error {
	pos := t.SourcePos
	if t.TotalRows() == 0 && t.DDL == "" && pos.GTID == "" {
		return nil
	}
	if p.applied0.Contains(pos) {
		p.markSeen(pos) // CP-2: đã apply trước đó
		return nil
	}
	if t.DDL != "" && p.ddlTouchesIncluded(t) {
		// P1–P2: DDL trên bảng thuộc pipeline thì dừng chờ (02 §12).
		return exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
			fmt.Sprintf("DDL at %s touches a pipeline table: %q", pos.GTID, t.DDL), nil)
	}

	out := &model.Txn{SourcePos: pos, SourceTS: t.SourceTS}
	keep := func(rc *model.RowChange, emit func(model.RowChange) error) error {
		if p.table(rc.Table) == nil {
			return nil // ngoài pipeline (bộ lọc, hoặc chưa snapshot add)
		}
		parts := []*model.RowChange{rc}
		if p.snap.Manager.IsRegistered(rc.Table) {
			parts = parts[:0]
			for _, r := range p.snap.Manager.HandleStreamEventWithPKSplit(rc, pos) {
				switch r.Action {
				case snapshot.ActionApply:
					parts = append(parts, r.Event)
				case snapshot.ActionReject:
					return fmt.Errorf("snapshot range-state cannot place key %v of %s", r.Event.Key, rc.Table)
				}
			}
		}
		for _, part := range parts {
			p.touched.add(rc.Table, part.Key)
			if len(part.OldKey) > 0 {
				p.touched.add(rc.Table, part.OldKey)
			}
			c := *part
			c.Table = p.filter.Target(c.Table)
			for i := range c.ForeignKeys {
				c.ForeignKeys[i].ParentTable = p.filter.Target(c.ForeignKeys[i].ParentTable)
			}
			if err := emit(c); err != nil {
				return err
			}
		}
		return nil
	}

	if t.IsSpilled() {
		f, err := os.CreateTemp(filepath.Dir(t.SpillPath), "strand-routed-*.spill")
		if err != nil {
			return err
		}
		sw := model.NewSpillWriter(f)
		tables := map[model.TableID]bool{}
		var n int64
		err = t.ForEachRow(func(rc *model.RowChange) error {
			return keep(rc, func(c model.RowChange) error {
				n++
				tables[c.Table] = true
				for _, fk := range c.ForeignKeys {
					tables[fk.ParentTable] = true
				}
				return sw.Write(&c)
			})
		})
		if err == nil {
			err = sw.Flush()
		}
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		_ = os.Remove(t.SpillPath)
		if err != nil {
			_ = os.Remove(f.Name())
			return err
		}
		if n == 0 {
			_ = os.Remove(f.Name())
		} else {
			out.SpillPath, out.RowCount = f.Name(), n
			for tid := range tables {
				out.SpilledTables = append(out.SpilledTables, tid)
			}
		}
	} else {
		for i := range t.Rows {
			if err := keep(&t.Rows[i], func(c model.RowChange) error {
				out.Rows = append(out.Rows, c)
				return nil
			}); err != nil {
				return err
			}
		}
	}
	out.Bytes = t.Bytes

	// Txn rỗng sau khi lọc (bảng ngoài pipeline, DDL ngoài pipeline, event bị range-state bỏ)
	// vẫn đi qua scheduler để GTID của nó được ghi vào tập đã apply: không sinh holes vĩnh viễn.
	if err := p.sched.Admit(ctx, out); err != nil {
		return err
	}
	p.admitted.Store(out.Seq())
	if t.SourceTS > 0 {
		p.lastRoutedTS.Store(t.SourceTS)
	}
	p.markSeen(pos)
	return nil
}

func (p *Pipeline) waitIfPaused(ctx context.Context) {
	p.pauseMu.Lock()
	for p.paused.Load() && ctx.Err() == nil && p.failed() == nil {
		p.pauseCond.Wait()
	}
	p.pauseMu.Unlock()
}

// drainPrefetch đưa các Txn trong spool prefetch vào router; khi chúng đã apply hết ở target,
// spool được xóa.
func (p *Pipeline) drainPrefetch(ctx context.Context, out chan<- *model.Txn) error {
	it, err := p.prefetch.Iter(ctx, model.Pos{})
	if err != nil {
		return err
	}
	n := 0
	for {
		t, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			it.Close()
			return fmt.Errorf("read prefetch spool: %w", err)
		}
		select {
		case out <- t:
			n++
		case <-ctx.Done():
			it.Close()
			return nil
		}
	}
	it.Close()
	p.log.Info("prefetch spool replayed; switching to live stream", "transactions", n)
	go func() {
		for p.sched.Lwd() < p.admitted.Load() && ctx.Err() == nil && p.failed() == nil {
			time.Sleep(100 * time.Millisecond)
		}
		if ctx.Err() != nil || p.failed() != nil {
			return
		}
		_ = p.prefetch.Close()
		if err := os.RemoveAll(p.prefetchDir); err == nil {
			p.log.Info("prefetch spool applied and removed")
		}
	}()
	return nil
}

// drain chờ mọi Txn đã Admit được commit ở target (hoặc hết timeout / lỗi).
func (p *Pipeline) drain(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if p.sched.Err() != nil || p.failed() != nil {
			return false
		}
		if p.sched.Lwd() >= p.admitted.Load() {
			return true
		}
		if time.Now().After(deadline) {
			p.log.Warn("drain timed out", "lwd", p.sched.Lwd(), "admitted", p.admitted.Load())
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

func (p *Pipeline) worker(ctx context.Context, id int, w *writer.MySQLWriter) {
	maxRows := p.cfg.Target.Batch.MaxRows
	if maxRows <= 0 {
		maxRows = 2000
	}
	mine := checkpoint.NewMySQLAppliedSet()
	for {
		var first *model.Txn
		select {
		case <-ctx.Done():
			return
		case first = <-p.sched.Ready():
		}
		if first == nil {
			return
		}
		batch := []*model.Txn{first}
		rows := first.TotalRows()
		if !p.strict {
		gather:
			for rows < int64(maxRows) && len(batch) < 256 {
				select {
				case t := <-p.sched.Ready():
					if t == nil {
						break gather
					}
					batch = append(batch, t)
					rows += t.TotalRows()
				default:
					break gather
				}
			}
		}
		for _, t := range batch {
			mine.Add(t.SourcePos)
		}
		delta, err := p.deltaFor(ctx, mine)
		if err == nil {
			// Deadlock / lock wait timeout ở target: cả transaction (dữ liệu + row delta) đã
			// rollback, nên apply lại là an toàn (04 §4 "Khóa phía target: retry").
			for attempt := 1; ; attempt++ {
				err = w.ApplyBatch(ctx, batch, writer.DeltaRow{Applied: delta})
				if err == nil || !isRetryable(err) || attempt >= 20 || ctx.Err() != nil {
					break
				}
				time.Sleep(time.Duration(attempt*attempt) * 5 * time.Millisecond)
			}
		}
		if err != nil {
			for _, t := range batch {
				p.sched.Done(t.Seq(), err)
			}
			if ctx.Err() == nil {
				p.fail(fmt.Errorf("worker %d: %w", id, err))
			}
			return
		}
		for _, t := range batch {
			p.sched.Done(t.Seq(), nil)
			if t.IsSpilled() {
				_ = os.Remove(t.SpillPath)
			}
			if t.SourceTS > 0 {
				p.lastSourceTS.Store(t.SourceTS)
			}
		}
		p.appliedTxns.Add(int64(len(batch)))
		p.appliedRows.Add(rows)
		p.metrics.ApplyTxnTotal.WithLabelValues(p.id).Add(float64(len(batch)))
		p.metrics.ApplyRowsTotal.WithLabelValues(p.id).Add(float64(rows))
		p.health.RecordProgress()
	}
}

// deltaFor cắt khỏi tập của worker phần đã có trong row toàn cục (design §1, bước 3 của
// compaction) và bảo đảm row delta ≤ 64 KB (CP-4).
func (p *Pipeline) deltaFor(ctx context.Context, mine *checkpoint.MySQLAppliedSet) (string, error) {
	trim := func() {
		if g, ok := p.comp.LastGlobal().(*checkpoint.MySQLAppliedSet); ok && g != nil {
			mine.Subtract(g)
		}
	}
	trim()
	s := mine.String()
	if len(s) <= checkpoint.MaxDeltaBytes {
		return s, nil
	}
	if err := p.comp.Compact(ctx); err != nil {
		return "", fmt.Errorf("sync compaction: %w", err)
	}
	trim()
	s = mine.String()
	if len(s) > checkpoint.MaxDeltaBytes {
		return "", checkpoint.ErrDeltaExceededLimit
	}
	return s, nil
}

// ---------------------------------------------------------------------------
// Snapshot (chế độ A và snapshot add)
// ---------------------------------------------------------------------------

// barrier dừng router, chờ mọi Txn đã nhận được commit, rồi chạy fn (02 §5).
func (p *Pipeline) barrier(ctx context.Context, fn func() error) error {
	p.routeMu.Lock()
	defer p.routeMu.Unlock()
	if !p.drain(10 * time.Minute) {
		if err := p.failed(); err != nil {
			return err
		}
		return fmt.Errorf("snapshot barrier: pipeline did not drain")
	}
	return fn()
}

// rendezvous chờ router đã xử lý mọi GTID trong high watermark.
func (p *Pipeline) rendezvous(ctx context.Context, high checkpoint.AppliedSet) error {
	h, ok := high.(*checkpoint.MySQLAppliedSet)
	if !ok {
		return fmt.Errorf("unexpected high watermark type %T", high)
	}
	stop := context.AfterFunc(ctx, func() {
		p.seenMu.Lock()
		p.seenCond.Broadcast()
		p.seenMu.Unlock()
	})
	defer stop()
	p.seenMu.Lock()
	defer p.seenMu.Unlock()
	for !h.IsSubsetOf(p.seen) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := p.failed(); err != nil {
			return err
		}
		p.seenCond.Wait()
	}
	return nil
}

func (p *Pipeline) pace(ctx context.Context) error {
	for {
		breaker := p.govAlgo != "static" && p.gov.BreakerOpen()
		// Breaker luôn thắng: background dừng (02 §7). bootstrap_retention_risk chỉ ghi đè việc
		// onoff dừng vì áp suất lag (P_lag theo pha), để snapshot không tự khóa chết.
		if p.retentionRisk.Load() && !breaker {
			return nil
		}
		windowOK := inWindows(p.cfg.Bootstrap.Logical.Windows, time.Now())
		running := p.govAlgo == "static" || p.gov.BackgroundRunning()
		if windowOK && running && !breaker {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.govTick):
		}
	}
	// Chi phí chunk ước lượng (trang 16 KiB) chia cho tốc độ background R (token/giây).
	_, rate := p.gov.CurrentRate()
	if p.govAlgo == "static" || rate <= 0 {
		rate = float64(p.cfg.Resources.Source.IOPS.Soft)
	}
	if rate <= 0 {
		return nil
	}
	avg := p.curAvgRow.Load()
	if avg <= 0 {
		avg = 200
	}
	pages := float64(avg*snapshot.DefaultChunkOptions().DefaultChunkRows) / 16384
	d := time.Duration(pages / rate * float64(time.Second))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func (p *Pipeline) runSnapshots(ctx context.Context, tables []model.TableID) {
	for _, id := range tables {
		if err := p.snapshotTable(ctx, id); err != nil {
			if ctx.Err() == nil {
				p.fail(fmt.Errorf("snapshot %s: %w", id, err))
			}
			return
		}
	}
	p.bootstrap.Store(false)
	p.retentionRisk.Store(false)
	p.log.Info("bootstrap finished: pipeline synced")
}

func (p *Pipeline) snapshotTable(ctx context.Context, id model.TableID) error {
	m := p.table(id)
	p.curAvgRow.Store(m.AvgRow)
	start := time.Now()
	p.log.Info("snapshot start", "table", id.String())
	if err := p.snap.SnapshotAdd(ctx, p.src, p.dst, id, m.PK[0], m.Columns, p.rendezvous); err != nil {
		return err
	}
	if err := p.state.SetDone(id.String(), true); err != nil {
		return err
	}
	p.routeMu.Lock()
	p.snap.Manager.RemoveTable(id)
	p.routeMu.Unlock()
	p.log.Info("snapshot done", "table", id.String(), "duration", time.Since(start).String())
	return nil
}

// SnapshotAdd thêm một bảng khi pipeline đang chạy (strand snapshot add, 02 §10).
// Bảng chưa có trong pipeline được thêm vào ngay (metadata đọc từ source); để giữ bảng sau
// khi restart, người vận hành phải thêm nó vào tables.include.
func (p *Pipeline) SnapshotAdd(ctx context.Context, table string) error {
	s, t, ok := strings.Cut(table, ".")
	if !ok {
		return fmt.Errorf("table must be schema.table")
	}
	id := model.TableID{Schema: s, Table: t}
	if p.snap.Manager.IsRegistered(id) {
		return fmt.Errorf("table %s is already being snapshotted", id)
	}
	m := p.table(id)
	if m == nil {
		one, err := newTableFilter([]string{id.String()}, nil, p.cfg.Tables.Rename)
		if err != nil {
			return err
		}
		metas, err := loadSourceTables(ctx, p.src, one)
		if err != nil {
			return err
		}
		if m = metas[id]; m == nil {
			return fmt.Errorf("table %s does not exist on source", id)
		}
		if len(m.PK) == 0 {
			return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf("table %s has no primary key", id), nil)
		}
	}
	if !m.PKNumeric {
		return fmt.Errorf("table %s: logical snapshot needs a single integer primary key", id)
	}
	if err := p.state.SetDone(id.String(), false); err != nil {
		return err
	}
	// Đăng ký PK cho writer trước khi router bắt đầu đưa event của bảng vào pipeline.
	for _, w := range p.writers {
		w.SetPrimaryKey(p.filter.Target(id), m.PK)
	}
	p.routeMu.Lock()
	p.tablesMu.Lock()
	added := p.tables[id] == nil
	p.tables[id] = m
	p.tablesMu.Unlock()
	p.routeMu.Unlock()
	p.log.Info("snapshot add requested", "table", id.String(), "new_table", added)
	p.bootstrap.Store(true)
	go func() {
		if err := p.snapshotTable(ctx, id); err != nil && ctx.Err() == nil {
			p.fail(fmt.Errorf("snapshot add %s: %w", id, err))
			return
		}
		if p.snap.Manager.TableCount() == 0 {
			p.bootstrap.Store(false)
		}
	}()
	return nil
}

// ---------------------------------------------------------------------------
// Coordinator: metric, lag, retention
// ---------------------------------------------------------------------------

func (p *Pipeline) watchCompactor(ctx context.Context) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.comp.Err(); err != nil && exitcode.GetExitCode(err) == exitcode.Fenced {
				p.fail(err)
				return
			}
		}
	}
}

func (p *Pipeline) lagSeconds() float64 {
	if p.sched.Lwd() >= p.admitted.Load() {
		return 0
	}
	ts := p.lastSourceTS.Load()
	if ts == 0 {
		ts = p.lastRoutedTS.Load()
	}
	if ts == 0 {
		return 0
	}
	return time.Since(time.Unix(ts, 0)).Seconds()
}

func (p *Pipeline) monitor(ctx context.Context) {
	lagSLO := durationOr(p.cfg.Governor.LagSLO, 5*time.Second)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		phase := "synced"
		if p.bootstrap.Load() {
			phase = "bootstrap"
		}
		lag := p.lagSeconds()
		// Bootstrap: lag = khoảng cách reader → head (router so với commit mới nhất ở source).
		if phase == "bootstrap" {
			if ts := p.lastRoutedTS.Load(); ts > 0 && !p.caughtUpWithSource(ctx) {
				lag = time.Since(time.Unix(ts, 0)).Seconds()
			}
		}
		p.metrics.LagSeconds.WithLabelValues(p.id, phase).Set(lag)
		p.metrics.Holes.WithLabelValues(p.id).Set(float64(p.sched.Holes()))
		p.updateMetrics()
		if ls := p.comp.LastSuccess(); !ls.IsZero() {
			p.metrics.CheckpointCompactionLagSeconds.WithLabelValues(p.id).Set(time.Since(ls).Seconds())
		}
		if p.sched.Lwd() >= p.admitted.Load() {
			p.health.RecordProgress() // không có việc tồn đọng: vẫn coi là tiến
		}
		// P_lag theo pha (02 §7): ở bootstrap, P_lag ≥ 1 thì báo bootstrap_retention_risk và
		// TĂNG ưu tiên snapshot (không giãn chunk) thay vì dừng, để không tự khóa chết.
		if phase == "bootstrap" {
			pLag := lag / lagSLO.Seconds()
			if pLag >= 1 && !p.retentionRisk.Swap(true) {
				p.log.Warn("bootstrap_retention_risk: reader lag exceeds lag_slo during bootstrap; prioritising snapshot",
					"code", "bootstrap_retention_risk", "lag_seconds", lag, "lag_slo", lagSLO.String())
			}
			if headroom, ok := p.retentionHeadroom(ctx); ok {
				p.metrics.RetentionHeadroomSeconds.WithLabelValues(p.id).Set(headroom)
			}
		}
	}
}

func parseBytes(s string) float64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := 1.0
	for _, u := range []struct {
		suf string
		m   float64
	}{{"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10}, {"TIB", 1 << 40}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}} {
		if strings.HasSuffix(s, u.suf) {
			s, mult = strings.TrimSuffix(s, u.suf), u.m
			break
		}
	}
	var v float64
	fmt.Sscanf(s, "%f", &v)
	return v * mult
}

// updateMetrics cập nhật các gauge theo 02 §11 mà không có nơi phát sinh tự nhiên.
func (p *Pipeline) updateMetrics() {
	m := p.metrics
	p.tablesMu.RLock()
	for id := range p.tables {
		t := p.filter.Target(id).String()
		m.WritesetEntries.WithLabelValues(p.id, t).Set(float64(p.sched.TableWritesetEntries(t)))
	}
	p.tablesMu.RUnlock()
	active := p.snap.Manager.StateCounts()
	p.tablesMu.RLock()
	for id := range p.tables {
		name := id.String()
		if _, ok := active[name]; !ok && p.state.IsDone(name) {
			m.SnapshotRanges.WithLabelValues(p.id, name, "Done").Set(1)
			m.SeedProgressRatio.WithLabelValues(p.id, name).Set(1)
		}
	}
	p.tablesMu.RUnlock()
	for table, counts := range active {
		for _, st := range []string{"Pending", "Reading", "Rendezvous", "Flushing", "Done"} {
			m.SnapshotRanges.WithLabelValues(p.id, table, st).Set(float64(counts[st]))
		}
		if pl, ok := p.state.Plan(table); ok && pl.MaxPK > pl.MinPK {
			m.SeedProgressRatio.WithLabelValues(p.id, table).Set(float64(pl.NextPK-pl.MinPK) / float64(pl.MaxPK-pl.MinPK+1))
		}
	}
	// MySQL→MySQL không có thao tác đổi PK phải chờ (chỉ PG + TOAST, 02 §8).
	m.PendingCrossRange.WithLabelValues(p.id).Set(0)
	m.PendingCrossRangeOldestSeconds.WithLabelValues(p.id).Set(0)
	// onoff không ước lượng độ nhạy (GRACE v2 ở P2).
	m.SensitivityUnknown.WithLabelValues(p.id).Set(0)
	if p.prefetch != nil {
		u := p.prefetch.Usage()
		m.SpoolBytes.WithLabelValues(p.id, "local").Set(float64(u.LocalBytes))
		m.SpoolBytes.WithLabelValues(p.id, "bucket").Set(float64(u.BucketBytes))
	} else {
		m.SpoolBytes.WithLabelValues(p.id, "local").Set(0)
		m.SpoolBytes.WithLabelValues(p.id, "bucket").Set(0)
	}
	m.SpoolCapacityBytes.WithLabelValues(p.id, "local").Set(parseBytes(p.cfg.Resources.Spool.Hard))
	m.SpoolCapacityBytes.WithLabelValues(p.id, "bucket").Set(0)
	if p.gov != nil && p.govAlgo == "static" {
		m.GovernorRateTokens.WithLabelValues(p.id, "source_iops").Set(float64(p.cfg.Resources.Source.IOPS.Soft))
	}
	m.VerifyMismatchTotal.WithLabelValues(p.id, "").Add(0)
	m.VerifyUnverifiableHotTotal.WithLabelValues(p.id, "").Add(0)
	m.FencedTotal.WithLabelValues(p.id).Add(0)
	m.ApplyTxnTotal.WithLabelValues(p.id).Add(0)
	m.ApplyRowsTotal.WithLabelValues(p.id).Add(0)
	m.RetentionHeadroomSeconds.WithLabelValues(p.id)
	m.GovernorZone.WithLabelValues(p.id)
	m.GovernorPressure.WithLabelValues(p.id, "replica_lag")
	m.BreakerOpen.WithLabelValues(p.id)
	m.CheckpointCompactionLagSeconds.WithLabelValues(p.id)
	m.LagSeconds.WithLabelValues(p.id, "synced")
}

func (p *Pipeline) caughtUpWithSource(ctx context.Context) bool {
	var executed string
	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := p.src.QueryRowContext(qctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return false
	}
	set, err := checkpoint.ParseMySQLGTIDSet(executed)
	if err != nil {
		return false
	}
	p.seenMu.Lock()
	defer p.seenMu.Unlock()
	return set.IsSubsetOf(p.seen)
}

// retentionHeadroom ước lượng số giây còn lại trước khi source xóa binlog mà reader chưa đọc.
func (p *Pipeline) retentionHeadroom(ctx context.Context) (float64, bool) {
	var expire int64
	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := p.src.QueryRowContext(qctx, "SELECT @@GLOBAL.binlog_expire_logs_seconds").Scan(&expire); err != nil || expire <= 0 {
		return 0, false
	}
	ts := p.lastRoutedTS.Load()
	if ts == 0 {
		return float64(expire), true
	}
	return float64(expire) - time.Since(time.Unix(ts, 0)).Seconds(), true
}

// isRetryable: lỗi khóa của InnoDB (1213 deadlock, 1205 lock wait timeout).
func isRetryable(err error) bool {
	var me *drivermysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1213 || me.Number == 1205
	}
	return false
}
