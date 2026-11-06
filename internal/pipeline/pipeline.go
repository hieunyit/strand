// Package pipeline nối các tầng của Strand thành một pipeline chạy được (design §1, spec 02
// §3–§6): Reader → Txn Assembler → Router (lọc, rename, bỏ Txn đã apply, range-state của
// snapshot) → Scheduler → Writer pool → Coordinator (compaction). Cặp engine hỗ trợ:
// MySQL→MySQL, MySQL→PostgreSQL, PostgreSQL→PostgreSQL (phần riêng của PG ở engine.go).
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
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	"strand/internal/reader"
	mysqlreader "strand/internal/reader/mysql"
	pgreader "strand/internal/reader/pg"
	"strand/internal/sched"
	"strand/internal/seed"
	"strand/internal/snapshot"
	"strand/internal/spool"
	"strand/internal/writer"
)

// Options là tham số chạy một pipeline.
type Options struct {
	Config       *config.Config
	StartAt      string // "" | "seed" | GTID set (chế độ C, source MySQL) | LSN X/Y (source PG)
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

	srcEngine, dstEngine model.Engine

	store   checkpoint.CheckpointStore
	release func() error
	epoch   int64

	filter   *tableFilter
	tablesMu sync.RWMutex
	// softRateBits: soft limit IOPS source hiện hành (math.Float64bits), dùng cho nhịp static.
	softRateBits atomic.Uint64
	throughput   rateMeter
	// partRoots: cache partition lá → bảng gốc (source PG, pg_partition.go).
	partRoots pgPartitionRoots
	tables    map[model.TableID]*tableMeta
	state     *localState
	applied0  checkpoint.AppliedSet // tập đã apply lúc khởi động (CP-2)

	sched   *sched.Scheduler
	comp    *checkpoint.Compactor
	orderer *writer.CommitOrderer
	writers []batchWriter
	// laneOn: writer cuối của writers chạy làn tuần tự (laneWorker, model.Txn.Lane).
	laneOn bool
	snap   *snapshot.MySQLSnapshotter

	// Source PG: reader (để báo lwd bền vững), bộ theo dõi LSN cho Vouch, lwd bền vững ở strict.
	pgReader      *pgreader.Reader
	lsns          *lsnTracker
	strictDurable atomic.Uint64
	reorderRetry  atomic.Int64 // số batch phải apply lại theo seq vì unique_violation (applyInSeqOrder)
	// MySQL→PG: chuyển giá trị theo typemap (nil = giá trị đi thẳng).
	conv *valueConverter
	// Source PG: xid các Txn đã route chưa xác nhận visible, và phần đọc source của snapshot logic.
	xids     *snapshot.XIDTracker
	pgChunks *snapshot.PGChunkSource

	metrics *metrics.Metrics
	health  *metrics.HealthChecker

	routeMu  sync.Mutex // giữ trong lúc route một Txn; barrier của snapshot lấy khóa này
	seenMu   sync.Mutex
	seenCond *sync.Cond
	seen     checkpoint.AppliedSet // applied0 ∪ mọi Txn router đã xử lý (chỉ source MySQL)

	admitted     atomic.Uint64 // seq lớn nhất đã Admit
	routed       atomic.Uint64 // số Txn route xong (Admit hoặc bỏ vì đã apply); xem drainPrefetch (review R03)
	appliedTxns  atomic.Int64
	appliedRows  atomic.Int64
	lastSourceTS atomic.Int64 // SourceTS của Txn mới nhất đã commit ở target
	lastRoutedTS atomic.Int64
	paused       atomic.Bool
	pauseMu      sync.Mutex
	pauseCond    *sync.Cond
	snapPaused   atomic.Bool // tạm dừng riêng phần snapshot logic (strand pause --snapshot)
	bootstrap    atomic.Bool
	// running: khởi động xong (đã quyết định snapshot/verify sau seed). Trước đó admin báo "starting",
	// không bao giờ "synced" — admin socket mở sớm hơn lúc biết có bootstrap hay không.
	running       atomic.Bool
	retentionRisk atomic.Bool
	verifying     atomic.Bool // chờ verify sau seed (02 §9)
	strictMu      sync.Mutex
	strictApplied checkpoint.AppliedSet // row toàn cục ở chế độ strict (02 §4 quy tắc 5)
	readyCh       <-chan *model.Txn     // nguồn Txn cho worker (strict: đã sắp theo seq)
	prefetchDir   string
	prefetch      *spool.PebbleSpool
	gov           governor.Governor
	govAlgo       string
	govTick       time.Duration
	curAvgRow     atomic.Int64
	snapChunkRows atomic.Int64 // cỡ chunk hiệu lực gần nhất (ước lượng chi phí chunk khi không biết byte)
	// adaptiveChunks: cỡ chunk theo luật 250 ms của governor (chunk_rows = auto, governor ≠ static).
	adaptiveChunks bool
	// snapBucket: token bucket chung của mọi reader snapshot (bootstrap và `snapshot add` chạy song song
	// cùng chia một R, không phải mỗi reader một R).
	snapBucket tokenBucket
	// snapReaders / snapGated: số reader snapshot đang chạy / đang bị giữ ở cổng của pace (cửa sổ L).
	snapReaders atomic.Int32
	snapGated   atomic.Int32
	// pgSignals: bộ đọc tín hiệu governor của source PG (nil với source MySQL).
	pgSignals *governor.PGDBSignalReader
	touched   *touchedKeys

	runCtx   context.Context
	instance string

	// snapAdding: bảng đang được `snapshot add` (từ lúc nhận lệnh tới khi snapshot xong). Admin xử lý
	// kết nối song song; IsRegistered chỉ đúng sau khi snapshot đã đăng ký range (review R14).
	snapAddMu  sync.Mutex
	snapAdding map[model.TableID]bool

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
	srcEngine, dstEngine := engineOf(cfg.Source.Kind), engineOf(cfg.Target.Kind)
	switch {
	case srcEngine == model.EngineMySQL && dstEngine == model.EngineMySQL,
		srcEngine == model.EngineMySQL && dstEngine == model.EnginePG,
		srcEngine == model.EnginePG && dstEngine == model.EnginePG:
	default:
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("unsupported pipeline %s → %s (supported: mysql→mysql, mysql→postgres, postgres→postgres)",
				cfg.Source.Kind, cfg.Target.Kind), nil)
	}
	p := &Pipeline{
		opts:      opts,
		cfg:       cfg,
		log:       opts.Logger.With("pipeline", cfg.Name),
		id:        cfg.Name,
		strict:    !strings.EqualFold(cfg.Target.CommitOrder, "relaxed"),
		srcEngine: srcEngine,
		dstEngine: dstEngine,
	}
	if srcEngine == model.EnginePG {
		p.lsns = &lsnTracker{}
		p.xids = snapshot.NewXIDTracker()
	}
	if srcEngine == model.EngineMySQL && dstEngine == model.EnginePG {
		p.conv = newValueConverter()
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

func targetDSN(dsn, pipelineID, instance string) (string, error) {
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
	// Writer gửi câu lệnh dữ liệu theo lô multi-statement (một round-trip cho nhiều câu lệnh);
	// tham số được driver escape phía client.
	c.MultiStatements = true
	c.InterpolateParams = true
	// Nhãn kết nối (performance_schema.session_connect_attrs): instance mới nhận pipeline có thể
	// tìm và ngắt kết nối của instance cũ cùng pipeline_id (fencing ở mức DB, SG-1).
	// FormatDSN của go-sql-driver v1.8.1 bỏ mất ConnectionAttributes, nên gắn thẳng vào DSN.
	attrs := "strand_pipeline:" + pipelineID + ",strand_instance:" + instance
	out := c.FormatDSN()
	sep := "?"
	if strings.Contains(out, "?") {
		sep = "&"
	}
	return out + sep + "connectionAttributes=" + url.QueryEscape(attrs), nil
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
	// review G3: target MySQL so tên bảng đích không phân biệt hoa thường (lower_case_table_names).
	if p.filter, err = newTargetTableFilter(p.cfg.Tables.Include, p.cfg.Tables.Exclude, p.cfg.Tables.Rename, p.dstEngine != model.EnginePG); err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
	}

	// --- Kết nối ---
	p.srcDSN = p.cfg.Source.DSN
	srcDriver, srcSQLDSN := "mysql", p.srcDSN
	// Source MySQL: phiên SQL luôn ở UTC, như reader binlog (TimestampStringLocation=UTC) và phiên ghi
	// target. Trước đây chỉ ép khi target là PG; MySQL→MySQL với source ở múi giờ khác thì snapshot đọc
	// TIMESTAMP theo giờ địa phương rồi ghi vào target UTC — lệch đúng bằng độ lệch múi giờ (review R05).
	switch {
	case p.srcEngine == model.EnginePG:
		srcDriver = "pgx"
	default:
		if srcSQLDSN, err = mysqlSourceUTC(p.srcDSN); err != nil {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid source DSN: %v", err), err)
		}
	}
	if p.src, err = sql.Open(srcDriver, srcSQLDSN); err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid source DSN: %v", err), err)
	}
	defer p.src.Close()
	host, _ := os.Hostname()
	p.instance = fmt.Sprintf("%s/%d/%d", host, os.Getpid(), time.Now().UnixNano())
	if p.dstEngine == model.EnginePG {
		if p.dst, err = openPGTarget(p.cfg.Target.DSN, pgAppName(p.id, p.instance)); err != nil {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid target DSN: %v", err), err)
		}
	} else {
		dstDSN, err := targetDSN(p.cfg.Target.DSN, p.id, p.instance)
		if err != nil {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid target DSN: %v", err), err)
		}
		if p.dst, err = sql.Open("mysql", dstDSN); err != nil {
			return err
		}
	}
	defer p.dst.Close()
	p.dst.SetMaxOpenConns(64)

	// --- Khóa pipeline, epoch (02 §4 quy tắc 1–2) ---
	if p.dstEngine == model.EnginePG {
		p.store = checkpoint.NewPGStore(p.dst, p.id, p.srcEngine)
	} else {
		p.store = checkpoint.NewMySQLStore(p.dst, p.id, model.EngineMySQL)
	}
	if err := p.store.EnsureTables(ctx); err != nil {
		return fmt.Errorf("ensure metadata tables: %w", err)
	}
	if p.release, err = p.store.Lock(ctx, p.id); err != nil {
		return err
	}
	defer func() { _ = p.release() }()
	// Đã giữ khóa pipeline: mọi instance khác cùng pipeline_id là instance cũ. Ngắt kết nối
	// target của chúng TRƯỚC khi tăng epoch, vì transaction dở của chúng giữ row lock trên
	// chính row delta mà bước tăng epoch phải cập nhật.
	p.killStaleInstances(ctx)
	if p.epoch, err = p.store.AdvanceEpoch(ctx, p.id, fmt.Sprintf("%s/%d", host, os.Getpid()), configHash(p.cfg)); err != nil {
		return fmt.Errorf("advance epoch: %w", err)
	}
	p.log = p.log.With("epoch", p.epoch)
	p.log.Info("pipeline lock acquired")
	// Mọi ghi snapshot dùng ctx này kiểm epoch trong transaction của chính nó (SG-1, review R07).
	ctx = checkpoint.WithFence(ctx, checkpoint.Fence{PipelineID: p.id, Epoch: p.epoch})
	p.runCtx = ctx

	// --- Metadata source ---
	if p.tables, err = p.loadTables(ctx, p.filter); err != nil {
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
	{
		ids := make([]model.TableID, 0, len(p.tables))
		for id := range p.tables {
			ids = append(ids, id)
		}
		if err := p.checkPGPartitionPublication(ctx, ids); err != nil {
			return err
		}
	}
	if p.state, err = openLocalState(p.cfg.Resources.Spool.Path, p.id); err != nil {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
	}
	p.state.targetOf = func(name string) string {
		sc, t, _ := strings.Cut(name, ".")
		return p.filter.Target(model.TableID{Schema: sc, Table: t}).String()
	}
	if p.conv != nil {
		for id := range p.tables {
			if err := p.conv.load(ctx, p.src, id); err != nil {
				return err
			}
		}
	}

	// --- Tập đã apply và điểm bắt đầu ---
	loaded, err := p.store.LoadApplied(ctx)
	if err != nil {
		return fmt.Errorf("load applied set: %w", err)
	}
	var applied checkpoint.AppliedSet
	if p.srcEngine == model.EnginePG {
		if applied, err = p.initialPositionPG(ctx, loaded); err != nil {
			return err
		}
	} else {
		ma, err := p.initialPosition(ctx, loaded.(*checkpoint.MySQLAppliedSet))
		if err != nil {
			return err
		}
		applied = ma
	}
	p.applied0 = applied.Clone()
	p.strictApplied = applied.Clone()
	p.seen = applied.Clone()
	p.strictDurable.Store(applied.Lwd().LSN)
	// Spool prefetch của `seed apply` (02 §9): phủ phần log từ P mà source/archive có thể đã xóa.
	p.prefetchDir = filepath.Join(p.cfg.Resources.Spool.Path, "prefetch")
	streamFrom := applied.Clone()
	if fi, err := os.Stat(p.prefetchDir); err == nil && fi.IsDir() && p.srcEngine == model.EngineMySQL {
		var opts []spool.Option
		bucket, prefix, berr := spool.BucketFromURI(p.cfg.Resources.Spool.Overflow)
		if berr != nil {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, berr.Error(), berr)
		}
		if bucket != nil {
			opts = append(opts, spool.WithBucket(bucket, prefix))
		}
		if p.prefetch, err = spool.New(p.prefetchDir, opts...); err != nil {
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
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				it.Close()
				// Không được nuốt lỗi: đếm thiếu ở đây biến thành log_gap giả (hoặc bỏ sót Txn).
				return fmt.Errorf("read prefetch spool: %w", err)
			}
			streamFrom.Add(t.SourcePos)
			n++
		}
		it.Close()
		p.log.Info("prefetch spool found", "transactions", n)
	}
	if p.srcEngine == model.EngineMySQL {
		// Source PG: slot quyết định log còn đọc được (reader.Start trả log_gap khi slot đã vượt lwd).
		if err := p.checkLogAvailable(ctx, streamFrom.(*checkpoint.MySQLAppliedSet)); err != nil {
			return err
		}
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
	if err := p.checkTableTargets(); err != nil {
		return err
	}
	var pending []model.TableID
	for id := range p.tables {
		if !p.state.IsDone(id.String()) {
			pending = append(pending, id)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].String() < pending[j].String() })
	if p.dstEngine == model.EnginePG && p.cfg.Bootstrap.CreateSchemaAuto() {
		pending, err = orderSnapshotTables(pending, p.tables)
		if err != nil {
			return err
		}
	}

	// invariant: SD-1..4 — chỉ bảng pending được tạo từ schema hiện tại. Bảng Done
	// (seed/restore hoặc --start-at) mà thiếu không được tạo rỗng để che lỗi restore.
	// invariant: CP-1..5, RS-1/2, CO-1/2, SG-1 — đang giữ khóa pipeline, chưa chạy
	// snapshot/worker; tạo schema không thay tập applied, watermark hay thứ tự commit.
	if p.cfg.Bootstrap.CreateSchemaAuto() {
		if err := p.createTargetTables(ctx, pending); err != nil {
			return err
		}
	}
	// Cột generated ở target không nhận giá trị: đọc sau khi bảng đích chắc chắn đã có.
	if err := p.loadTargetGenerated(ctx, p.tables); err != nil {
		return err
	}
	// review R26: bảng đích và _strand_* trên target MySQL phải là InnoDB.
	if err := p.checkTargetEngines(ctx); err != nil {
		return err
	}

	if p.srcEngine == model.EngineMySQL && p.dstEngine == model.EnginePG {
		// MySQL→PG: bảng có PK không phải một cột số nguyên chỉ được nhận khi RỖNG ở source tại thời
		// điểm này (sau khi vị trí stream đã chốt): mọi row của nó sau đó đều đi qua stream, nên
		// "snapshot" là xóa sạch bảng đích. Bảng còn dữ liệu thì từ chối như trước.
		kept := pending[:0]
		for _, id := range pending {
			if p.tables[id].PKNumeric {
				kept = append(kept, id)
				continue
			}
			empty, err := p.emptyOnSource(ctx, id)
			if err != nil {
				return fmt.Errorf("check %s is empty: %w", id, err)
			}
			if !empty {
				kept = append(kept, id)
				continue
			}
			if err := writer.PGClearTable(ctx, p.dst, p.filter.Target(id)); err != nil {
				return fmt.Errorf("clear target table of %s: %w", id, err)
			}
			if err := p.state.SetDone(id.String(), true); err != nil {
				return err
			}
			p.log.Info("empty source table with a non-integer primary key joins the pipeline without snapshot", "table", id.String())
		}
		pending = kept
	}
	for _, id := range pending {
		if err := p.checkSnapshotKey(ctx, id, p.tables[id]); err != nil {
			return err
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
	scfg := sched.DefaultConfig()
	scfg.Serial = p.strict // strict: một writer, apply tuần tự theo seq (xem sched.Config.Serial)
	p.sched = sched.NewScheduler(scfg)
	nWorkers := p.cfg.Resources.Workers.Max
	if nWorkers <= 0 {
		nWorkers = 8
	}
	if p.strict {
		p.orderer = writer.NewCommitOrderer("strict", 1)
		// strict apply tuần tự (chờ lượt trước khi mở transaction), nên nhiều worker không thêm
		// song song mà chỉ chia nhỏ batch (mỗi worker một seq → một commit mỗi Txn). Một worker
		// gom tối đa các Txn liên tiếp vào một commit (group commit).
		nWorkers = 1
	}
	// Làn tuần tự (relaxed, source PG): thêm một writer riêng ở cuối p.writers (laneWorker).
	p.laneOn = p.srcEngine == model.EnginePG && !p.strict
	total := nWorkers
	if p.laneOn {
		total++
	}
	for i := 1; i <= total; i++ {
		var w batchWriter
		if p.dstEngine == model.EnginePG {
			w = writer.NewPGWriter(p.dst, p.id, i, p.epoch)
		} else {
			mw := writer.NewMySQLWriter(p.dst, p.id, i, p.epoch)
			mw.SetStatementPipelining(true)
			w = mw
		}
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
	p.comp = checkpoint.NewCompactor(p.dst, p.store, p.id, p.srcEngine, p.dstEngine, ccfg)
	if p.srcEngine == model.EnginePG && !p.strict {
		// Relaxed: row delta chỉ chứa LSN tường minh; coordinator dời mốc của row toàn cục tới
		// phần scheduler đã commit, để lwd (và slot) tiến (02 §4 quy tắc 4).
		p.comp.SetVouch(p.vouchGlobal)
	}
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
	if err := p.setupGovernor(ctx); err != nil {
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
	if cr := strings.TrimSpace(p.cfg.Bootstrap.Logical.ChunkRows); cr != "" && !strings.EqualFold(cr, "auto") {
		n, err := strconv.ParseInt(cr, 10, 64)
		if err != nil || n <= 0 {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
				fmt.Sprintf("bootstrap.logical.chunk_rows must be \"auto\" or a positive number, got %q", cr), nil)
		}
		opts.DefaultChunkRows = n
	}
	p.snapChunkRows.Store(opts.DefaultChunkRows)
	p.snap = snapshot.NewMySQLSnapshotter(opts)
	p.snap.Barrier = p.barrier
	p.snap.Pace = p.pace
	if p.adaptiveChunks {
		p.snap.NextChunkRows = p.nextChunkRows
	}
	p.snap.TargetName = p.filter.Target
	p.snap.LoadPlan = func(t model.TableID) (snapshot.SnapshotPlan, bool) { return p.state.Plan(t.String()) }
	p.snap.SavePlan = func(t model.TableID, pl snapshot.SnapshotPlan) error { return p.state.SetPlan(t.String(), pl) }
	if p.dstEngine == model.EnginePG {
		p.snap.FlushRows = p.pgFlushRows
		p.snap.FlushKeys = p.pgFlushKeys
		p.snap.ClearTarget = func(ctx context.Context, t model.TableID) error { return writer.PGClearTable(ctx, p.dst, t) }
		p.snap.ClearUnread = func(ctx context.Context, t model.TableID, pk string, lo, hi int64, hasRange bool, keys []int64) error {
			return writer.PGDeleteUnread(ctx, p.dst, t, pk, lo, hi, hasRange, keys)
		}
		p.snap.FlushRange = p.pgFlushRange
		p.snap.FlushKeySet = p.pgFlushKeySet
		p.snap.ClearUnreadRange = p.pgClearUnreadRange
		p.snap.ExactKeys = p.exactKeys
		p.snap.FlushExact = p.pgFlushExact
	}
	if p.srcEngine == model.EnginePG {
		snapDB, err := openPGSnapshotSource(p.srcDSN)
		if err != nil {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid source DSN: %v", err), err)
		}
		defer snapDB.Close()
		p.pgChunks = &snapshot.PGChunkSource{DB: snapDB, Tracker: p.xids, Log: p.log}
		p.snap.Chunks = p.pgChunks
	}

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
	var rd reader.Reader
	if p.srcEngine == model.EnginePG {
		slot, pub := p.pgSlotNames()
		prd, err := pgreader.New(pgreader.Config{DSN: p.srcDSN, Slot: slot, Publication: pub,
			// Phần thân Txn streaming / two-phase vượt trần RAM tràn vào spool của pipeline (review R08).
			SpillDir:   filepath.Join(p.cfg.Resources.Spool.Path, "pg-reader-"+p.id),
			CreateSlot: p.cfg.Source.CreateSlot, Failover: p.cfg.Source.FailoverSlot,
			ExportOnCreate: len(pending) > 0 && p.cfg.Source.CreateSlot})
		if err != nil {
			return err
		}
		p.pgReader, rd = prd, prd
		// Cổng visibility của snapshot logic (xem snapshot.PGChunkSource.Gate).
		p.setupPGSnapshotGate(ctx, prd)
		if len(pending) > 0 && p.pgChunks.GateErr != nil {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("logical snapshot from this PostgreSQL source is not possible: %v", p.pgChunks.GateErr), p.pgChunks.GateErr)
		}
	} else {
		charsets, err := mysqlCollationCharsets(ctx, p.src)
		if err != nil {
			return err
		}
		mrd, err := mysqlreader.New(mysqlreader.Config{DSN: p.srcDSN, ServerID: p.cfg.Source.ServerID, CollationCharsets: charsets})
		if err != nil {
			return err
		}
		rd = mrd
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
	p.readyCh = p.sched.Ready()
	if p.strict {
		ordered := make(chan *model.Txn, 1024)
		go p.strictDispatcher(wctx, p.sched.Ready(), ordered)
		p.readyCh = ordered
	}
	for i, w := range p.writers {
		workersWG.Add(1)
		go func(i int, w batchWriter) {
			defer workersWG.Done()
			if p.laneOn && i == len(p.writers)-1 {
				p.laneWorker(wctx, i+1, w)
				return
			}
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
		p.touched.enable(touchDelta, true)
	}
	if p.pgReader != nil {
		bgWG.Add(2)
		go func() { defer bgWG.Done(); p.pgConfirmLoop(ctx) }()
		go func() { defer bgWG.Done(); p.pgPruneXIDs(ctx) }()
		if p.dstEngine == model.EnginePG {
			stopSeq := p.startSequenceSync(ctx)
			defer stopSeq()
		}
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
	p.running.Store(true)
	p.log.Info("pipeline running", "workers", nWorkers, "strict", p.strict, "snapshot_pending", len(pending),
		"start", applied.(fmt.Stringer).String())

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
	p.log.Info("pipeline stopped cleanly", "applied_txns", p.appliedTxns.Load(), "reapplied_in_seq_order", p.reorderRetry.Load())
	return nil
}

// killStaleInstances ngắt mọi kết nối target của instance khác cùng pipeline_id (sau khi đã
// giữ khóa pipeline, trước khi tăng epoch). Instance cũ bị đóng băng hoặc bị phân vùng có thể đang giữ transaction mở và
// row lock trên target: epoch chặn nó commit, nhưng lock chỉ được nhả khi kết nối bị ngắt.
func (p *Pipeline) killStaleInstances(ctx context.Context) {
	if p.dstEngine == model.EnginePG {
		p.killStalePG(ctx)
		return
	}
	rows, err := p.dst.QueryContext(ctx, `
		SELECT a.PROCESSLIST_ID FROM performance_schema.session_connect_attrs a
		  JOIN performance_schema.session_connect_attrs b ON b.PROCESSLIST_ID = a.PROCESSLIST_ID
		 WHERE a.ATTR_NAME = 'strand_pipeline' AND a.ATTR_VALUE = ?
		   AND b.ATTR_NAME = 'strand_instance' AND b.ATTR_VALUE <> ?`, p.id, p.instance)
	if err != nil {
		p.log.Warn("cannot list stale instance connections", "error", err)
		return
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		if _, err := p.dst.ExecContext(ctx, fmt.Sprintf("KILL %d", id)); err != nil {
			p.log.Warn("cannot kill stale instance connection", "id", id, "error", err)
		}
	}
	if len(ids) > 0 {
		p.log.Warn("killed target connections of a previous instance of this pipeline", "connections", len(ids))
	}
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
		var setStr, seedIdentity string
		fromSeed := p.opts.StartAt == "seed"
		if fromSeed {
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
			setStr, seedIdentity = m.StartPosition.GTIDSet, m.SourceIdentity
		} else {
			setStr = p.opts.StartAt
		}
		set, err := checkpoint.ParseMySQLGTIDSet(setStr)
		if err != nil || set.String() == "" {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid start position %q: %v", setStr, err), err)
		}
		// SD-1: seed phải lấy từ đúng topology của source đang nối (như review N09 phía PG), kiểm trước
		// khi ghi bất kỳ trạng thái nào.
		if fromSeed {
			if err := p.checkMySQLSeedSource(ctx, seedIdentity, setStr); err != nil {
				return nil, err
			}
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
		return set, p.writeGlobal(ctx, set.String())
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
	return set, p.writeGlobal(ctx, set.String())
}

func (p *Pipeline) writeGlobal(ctx context.Context, applied string) error {
	err := p.store.WriteGlobal(ctx, checkpoint.Global{PipelineID: p.id, Epoch: p.epoch, Applied: applied})
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
			p.routed.Add(1) // review R03
		}
	}
}

func (p *Pipeline) markSeen(pos model.Pos) {
	p.seenMu.Lock()
	if p.srcEngine == model.EngineMySQL {
		// Chỉ snapshot (source MySQL) đọc p.seen; với PG tập LSN tường minh sẽ phình mãi.
		p.seen.Add(pos)
	}
	p.seenCond.Broadcast()
	p.seenMu.Unlock()
}

func (p *Pipeline) table(id model.TableID) *tableMeta {
	p.tablesMu.RLock()
	defer p.tablesMu.RUnlock()
	return p.tables[id]
}

func (p *Pipeline) route(ctx context.Context, t *model.Txn) error {
	pos := t.SourcePos
	if p.xids != nil {
		// Trước range-state: snapshot logic phải chờ Txn này visible (snapshot.XIDTracker).
		p.xids.Add(t.XID)
	}
	if skipEmptyPGTxn(p.strict, t) {
		p.noteRouted(pos, 0) // Txn PG rỗng: không có gì để apply
		return nil
	}
	if p.applied0.Contains(pos) {
		p.markSeen(pos) // CP-2: đã apply trước đó
		p.noteRouted(pos, 0)
		return nil
	}
	if t.DDL != "" {
		// P1–P2 (02 §12): DDL trên bảng thuộc pipeline (kể cả bảng MỚI khớp bộ lọc do CREATE/RENAME)
		// chỉ đi tiếp khi schema ở target đã khớp source; nếu không thì dừng schema_change_pending.
		if err := p.handleDDL(ctx, t); err != nil {
			return err
		}
	}

	out := &model.Txn{SourcePos: pos, SourceTS: t.SourceTS}
	// rereadTables: bảng vừa có khóa được đánh dấu đọc lại (UPDATE đổi PK mà cột TOAST không đổi, 02 §8);
	// plan của chúng phải được lưu bền TRƯỚC khi Txn này được Admit (xem snapshot/toast_pk.go).
	var rereadTables map[model.TableID]bool
	lane := false // Txn chạm bảng LaneTable (relaxed, source PG): đi làn tuần tự
	keep := func(rc *model.RowChange, emit func(model.RowChange) error) error {
		if p.table(rc.Table) == nil {
			// Partition lá của một bảng thuộc pipeline (publication không publish_via_partition_root):
			// bỏ qua là mất row âm thầm — dừng (review R02, pg_partition.go).
			if root, ok, err := p.leafOfPipelineTable(ctx, rc.Table); err != nil {
				return err
			} else if ok {
				return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
					"change at %s on partition %s of replicated table %s: the publication must use publish_via_partition_root = true "+
						"(ALTER PUBLICATION ... SET (publish_via_partition_root = true)), otherwise these rows are lost",
					pos, rc.Table, root), nil)
			}
			return nil // ngoài pipeline (bộ lọc, hoặc chưa snapshot add)
		}
		parts := []*model.RowChange{rc}
		if p.beforeRegister(rc.Table) {
			// Bảng chờ snapshot mà range chưa được đăng ký (source PG và MySQL, review N10): xem keepBeforeRegister.
			var err error
			if parts, err = p.keepBeforeRegister(rc); err != nil {
				return err
			}
		} else if p.snap.Manager.IsRegistered(rc.Table) {
			results := []snapshot.StreamActionResult(nil)
			if len(rc.OldKey) > 0 && snapshot.HasUnchanged(rc) {
				// UPDATE đổi PK mà cột TOAST không đổi: không gộp, không point read (02 §8, toast_pk.go).
				var reread bool
				results, reread = p.snap.Manager.HandleToastPKChangeXID(rc, pos, t.XID, time.Now())
				if reread {
					if rereadTables == nil {
						rereadTables = map[model.TableID]bool{}
					}
					rereadTables[rc.Table] = true
				}
			} else {
				results = p.snap.Manager.HandleStreamEventWithPKSplitXID(rc, pos, t.XID)
			}
			parts = parts[:0]
			for _, r := range results {
				switch r.Action {
				case snapshot.ActionApply:
					parts = append(parts, r.Event)
				case snapshot.ActionReject:
					return fmt.Errorf("snapshot range-state cannot place key %v of %s", r.Event.Key, rc.Table)
				}
			}
		}
		meta := p.table(rc.Table)
		if p.laneOn && meta != nil && meta.LaneTable && len(parts) > 0 {
			lane = true
		}
		var keyCodec *snapshot.PKCodec // PK tổng quát: khóa đã chạm lưu theo khóa mã hóa (việc C)
		if meta != nil && !meta.PKNumeric {
			keyCodec = meta.PKCodec
		}
		for _, part := range parts {
			p.touched.addKey(rc.Table, part.Key, keyCodec)
			if len(part.OldKey) > 0 {
				p.touched.addKey(rc.Table, part.OldKey, keyCodec)
			}
			c := *part
			if p.conv != nil {
				// MySQL→PG: giá trị đổi sang dạng PG (02 §8) sau range-state (range dùng khóa gốc).
				if err := p.conv.row(rc.Table, &c, meta.PK); err != nil {
					return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected, err.Error(), err)
				}
			}
			c.Cols = dropTargetGenerated(c.Cols, meta.TargetGenerated)
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
		// Gom writeset (chỉ hash khóa) ngay khi chép row đã định tuyến: Txn tràn đĩa ≤ table_level_threshold
		// khóa/bảng vẫn có phụ thuộc cấp row (02 §6) — tràn đĩa chỉ là chuyện RAM của payload. Khóa tính
		// trên row sau định tuyến (bảng target, giá trị đã đổi) đúng như Txn trong RAM. strict (Serial)
		// không dựng DAG nên không gom.
		var ws *model.WritesetCollector
		if scfg := p.sched.Config(); !scfg.Serial {
			ws = model.NewWritesetCollector(scfg.TableLevelThreshold, scfg.WritesetBudget)
		}
		err = t.ForEachRow(func(rc *model.RowChange) error {
			return keep(rc, func(c model.RowChange) error {
				n++
				tables[c.Table] = true
				for _, fk := range c.ForeignKeys {
					tables[fk.ParentTable] = true
				}
				ws.Add(&c)
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
			out.SpillWriteset = ws.Writeset()
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
	out.Lane = lane

	// Nghĩa vụ đọc lại phải bền trước khi Txn có thể commit ở target (checkpoint vượt Txn).
	for tid := range rereadTables {
		if err := p.snap.PersistReread(tid); err != nil {
			return fmt.Errorf("persist re-read keys of %s: %w", tid, err)
		}
	}

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
	p.noteRouted(pos, out.Seq())
	return nil
}

// skipEmptyPGTxn: Txn PG rỗng (không row, không DDL) không cần đi qua scheduler — relaxed vẫn dời
// được lwd qua nó vì compactor xác nhận định kỳ tới lsnTracker.point (vouchGlobal). Riêng strict:
// row toàn cục chỉ được ghi cùng commit của một batch, nên watermark (logical decoding message
// không transactional, XID = 0 — của snapshot logic hoặc marker fence của promote) phải đi qua
// scheduler như một Txn rỗng thì lwd bền vững mới tới được nó khi source đã ngừng ghi. Txn rỗng có
// XID (transaction không chạm bảng nào của publication) vẫn bị bỏ qua để không ghi target vô ích.
func skipEmptyPGTxn(strict bool, t *model.Txn) bool {
	if t.TotalRows() != 0 || t.DDL != "" || t.SourcePos.GTID != "" {
		return false
	}
	return !(strict && t.SourcePos.Engine == model.EnginePG && t.XID == 0)
}

// noteRouted báo bộ theo dõi LSN (chỉ source PG) rằng router đã xử lý xong Txn pos.
func (p *Pipeline) noteRouted(pos model.Pos, seq uint64) {
	if p.lsns != nil && pos.Engine == model.EnginePG {
		p.lsns.routed(pos.LSN, seq)
		// Điểm hẹn của snapshot PG chờ LSN router đã xử lý (rendezvousPG).
		p.seenMu.Lock()
		p.seenCond.Broadcast()
		p.seenMu.Unlock()
	}
}

func (p *Pipeline) waitIfPaused(ctx context.Context) {
	// Hủy ctx (SIGTERM lúc đang pause) phải đánh thức cond, không thì router kẹt và shutdown treo
	// ở <-routerDone (review R11).
	stop := context.AfterFunc(ctx, func() {
		p.pauseMu.Lock()
		p.pauseCond.Broadcast()
		p.pauseMu.Unlock()
	})
	defer stop()
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
	// drainPrefetch là nguồn duy nhất của router trước khi stream trực tiếp bắt đầu: mốc routed lúc
	// này + n = mọi Txn prefetch đã qua router (review R03).
	base := p.routed.Load()
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
		if !p.waitPrefetchApplied(ctx, base+uint64(n), p.sched.Lwd, 100*time.Millisecond) {
			return
		}
		_ = p.prefetch.Close()
		if err := os.RemoveAll(p.prefetchDir); err == nil {
			p.log.Info("prefetch spool applied and removed")
		}
	}()
	return nil
}

// waitPrefetchApplied chờ đến khi xóa được spool prefetch (review R03): trước hết mọi Txn prefetch
// phải đã qua router (routed >= target) — Txn còn trong channel hay router đã nhận nhưng chưa Admit
// thì admitted chưa tính, Lwd == admitted vẫn có thể đúng trong khi SpillPath của chúng nằm trong
// prefetchDir. Sau đó mới chốt admitted và chờ Lwd đạt mốc đó. Trả false khi ctx hủy / pipeline lỗi.
func (p *Pipeline) waitPrefetchApplied(ctx context.Context, target uint64, lwd func() uint64, tick time.Duration) bool {
	for p.routed.Load() < target {
		if ctx.Err() != nil || p.failed() != nil {
			return false
		}
		time.Sleep(tick)
	}
	mark := p.admitted.Load()
	for lwd() < mark {
		if ctx.Err() != nil || p.failed() != nil {
			return false
		}
		time.Sleep(tick)
	}
	return ctx.Err() == nil && p.failed() == nil
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

func (p *Pipeline) worker(ctx context.Context, id int, w batchWriter) {
	lim := batchLimits{maxRows: int64(p.cfg.Target.Batch.MaxRows), strict: p.strict,
		maxBytes: int64(parseBytes(p.cfg.Target.Batch.MaxBytes)), maxWait: durationOr(p.cfg.Target.Batch.MaxWait, 0)}
	if lim.maxRows <= 0 {
		lim.maxRows = 2000
	}
	if lim.maxBytes <= 0 {
		lim.maxBytes = 4 << 20 // 4 MiB (02 §10)
	}
	if !p.strict {
		// Relaxed: batch kéo theo chuỗi Txn phụ thuộc vào nó (row nóng, G2-11) — xem TakeChained.
		lim.chain = p.sched.TakeChained
	}
	var mine checkpoint.AppliedSet = checkpoint.NewMySQLAppliedSet()
	if p.srcEngine == model.EnginePG {
		mine = checkpoint.NewPGAppliedSet(0)
	}
	var stash *model.Txn // strict: Txn lấy ra nhưng không liền seq với batch, xử lý ở lượt sau
	for {
		var first *model.Txn
		if stash != nil {
			first, stash = stash, nil
		} else {
			select {
			case <-ctx.Done():
				return
			case first = <-p.readyCh:
			}
		}
		if first == nil {
			return
		}
		batch, rows, next := gatherBatch(ctx, first, p.readyCh, lim)
		stash = next
		if p.strict {
			b := batch
			err := w.ApplyBatch(ctx, b, writer.DeltaRow{Global: func() (string, error) {
				// Gọi sau khi đến lượt commit: mọi batch có seq nhỏ hơn đã commit vào row toàn cục.
				return p.strictGlobal(b), nil
			}})
			for attempt := 1; err != nil && isRetryable(err) && attempt < 20 && ctx.Err() == nil; attempt++ {
				time.Sleep(time.Duration(attempt*attempt) * 5 * time.Millisecond)
				err = w.ApplyBatch(ctx, b, writer.DeltaRow{Global: func() (string, error) {
					return p.strictGlobal(b), nil
				}})
			}
			if err == nil && p.srcEngine == model.EnginePG {
				// Row toàn cục vừa commit: lwd của nó là lwd bền vững cho slot.
				p.strictMu.Lock()
				p.strictDurable.Store(p.strictApplied.Lwd().LSN)
				p.strictMu.Unlock()
			}
			if !p.finishBatch(ctx, id, batch, rows, err) {
				return
			}
			continue
		}
		if !p.applyRelaxed(ctx, id, w, batch, rows, &mine) {
			return
		}
	}
}

// applyRelaxed apply một batch ở relaxed: thêm vào tập đã apply của worker (mine), ghi row delta cùng
// transaction dữ liệu, thử lại khi deadlock; source PG vi phạm unique → apply lại từng Txn theo seq
// (applyInSeqOrder). Trả false nếu worker phải dừng.
func (p *Pipeline) applyRelaxed(ctx context.Context, id int, w batchWriter, batch []*model.Txn, rows int64, mine *checkpoint.AppliedSet) bool {
	var prev checkpoint.AppliedSet
	if p.srcEngine == model.EnginePG {
		prev = (*mine).Clone() // để apply lại theo seq nếu batch vi phạm unique (applyInSeqOrder)
	}
	for _, t := range batch {
		(*mine).Add(t.SourcePos)
	}
	delta, err := p.deltaFor(ctx, *mine)
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
	if err != nil && p.srcEngine == model.EnginePG && writer.IsPGUniqueViolation(err) && ctx.Err() == nil {
		// Batch đã rollback. Xem applyInSeqOrder: apply lại từng Txn theo seq.
		p.reorderRetry.Add(1)
		*mine = prev
		return p.applyInSeqOrder(ctx, id, w, batch, *mine)
	}
	return p.finishBatch(ctx, id, batch, rows, err)
}

// laneWorker (relaxed, source PG): apply Txn làn tuần tự đúng thứ tự Admit, gom chuỗi Txn liên tiếp đã
// sẵn sàng vào một transaction (group commit, như strict) — thứ tự source giữa các Txn chạm bảng có
// ràng buộc duy nhất mà writeset không thấy giá trị cũ (model.Txn.Lane), mà không trả giá một commit
// cho mỗi Txn. Txn làn khác vẫn ghi song song ở các worker thường.
func (p *Pipeline) laneWorker(ctx context.Context, id int, w batchWriter) {
	maxRows := int64(p.cfg.Target.Batch.MaxRows)
	if maxRows <= 0 {
		maxRows = 2000
	}
	var mine checkpoint.AppliedSet = checkpoint.NewPGAppliedSet(0)
	for {
		batch := p.sched.NextLaneBatch(ctx, 256, maxRows)
		if batch == nil {
			return
		}
		var rows int64
		for _, t := range batch {
			rows += t.TotalRows()
		}
		if !p.applyRelaxed(ctx, id, w, batch, rows, &mine) {
			return
		}
	}
}

// applyInSeqOrder (relaxed, source PG): pgoutput không gửi giá trị cũ của cột UNIQUE ngoài PK
// (trừ REPLICA IDENTITY FULL), nên writeset không thấy xung đột kiểu "T1 giải phóng giá trị X,
// T2 sau đó chiếm X" và có thể apply T2 trước T1 → unique_violation ở target. Lỗi này chỉ là
// tạm thời: apply lại từng Txn theo seq, Txn nào vẫn vi phạm thì chờ mọi Txn có seq nhỏ hơn commit
// xong (target khi đó đúng bằng trạng thái source ngay trước Txn) rồi thử lại. Ghi theo PK không bao
// giờ bị ảnh hưởng (writeset theo PK vẫn đúng), vi phạm unique luôn là lỗi chứ không im lặng, nên
// thứ tự apply khác không thể cho trạng thái cuối khác. Trả false nếu worker phải dừng.
func (p *Pipeline) applyInSeqOrder(ctx context.Context, id int, w batchWriter, batch []*model.Txn, mine checkpoint.AppliedSet) bool {
	ordered := append([]*model.Txn(nil), batch...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Seq() < ordered[j].Seq() })
	for _, t := range ordered {
		one := []*model.Txn{t}
		var err error
	retry:
		for attempt := 1; ; attempt++ {
			m := mine.Clone()
			m.Add(t.SourcePos)
			var delta string
			if delta, err = p.deltaFor(ctx, m); err == nil {
				if err = w.ApplyBatch(ctx, one, writer.DeltaRow{Applied: delta}); err == nil {
					mine.Add(t.SourcePos)
					break
				}
			}
			if attempt >= 100 || ctx.Err() != nil {
				break
			}
			if attempt == 1 || attempt%10 == 0 {
				// Không im lặng khi Txn không apply được trong thời gian dài: chờ seq nhỏ hơn chỉ giải
				// được vi phạm do Txn TRƯỚC gây ra.
				p.log.Warn("transaction still failing after re-apply in source order",
					"seq", t.Seq(), "pos", t.SourcePos.String(), "attempt", attempt,
					"lwd", p.sched.Lwd(), "admitted", p.admitted.Load(), "error", err.Error())
			}
			switch {
			case writer.IsPGUniqueViolation(err):
				p.waitLwd(ctx, t.Seq()-1, 2*time.Second)
			case isRetryable(err):
				time.Sleep(time.Duration(min(attempt, 20)*min(attempt, 20)) * 5 * time.Millisecond)
			default:
				break retry
			}
		}
		if !p.finishBatch(ctx, id, one, t.TotalRows(), err) {
			return false
		}
	}
	return true
}

// waitLwd chờ tới khi mọi seq ≤ seq đã commit ở target (hoặc hết timeout / pipeline dừng).
func (p *Pipeline) waitLwd(ctx context.Context, seq uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for p.sched.Lwd() < seq && time.Now().Before(deadline) && ctx.Err() == nil && p.failed() == nil {
		time.Sleep(2 * time.Millisecond)
	}
}

// strictGlobal thêm batch b vào row toàn cục của strict và trả chuỗi applied mới. Source PG: mọi Txn
// có seq ≤ seq cuối của b đã commit hoặc commit cùng transaction này, nên mốc được xác nhận tới đó.
func (p *Pipeline) strictGlobal(b []*model.Txn) string {
	p.strictMu.Lock()
	defer p.strictMu.Unlock()
	for _, t := range b {
		p.strictApplied.Add(t.SourcePos)
	}
	if s, ok := p.strictApplied.(*checkpoint.PGAppliedSet); ok && len(b) > 0 {
		last := b[len(b)-1]
		v := p.lsns.point(last.Seq())
		// Strict commit đúng thứ tự seq, và seq tăng theo commit LSN: khi batch này commit thì mọi Txn
		// có LSN ≤ LSN của Txn cuối batch đã commit (Txn bị router bỏ qua thì đã apply / rỗng). Không chờ
		// router kịp ghi nhận Txn cuối vào lsnTracker (noteRouted chạy SAU Admit): nếu batch commit trước
		// đó, point chưa thấy nó và mốc dừng lại — với watermark/marker cuối cùng khi source đã ngừng
		// ghi, lwd bền vững sẽ không bao giờ tới được nó (promote bước 3).
		if l := last.SourcePos.LSN; l > v {
			v = l
		}
		s.Vouch(v, nil)
	}
	return p.strictApplied.(fmt.Stringer).String()
}

// finishBatch báo kết quả batch cho scheduler; false nếu worker phải dừng.
func (p *Pipeline) finishBatch(ctx context.Context, id int, batch []*model.Txn, rows int64, err error) bool {
	if err != nil {
		for _, t := range batch {
			p.sched.Done(t.Seq(), err)
		}
		if ctx.Err() == nil {
			var te *writer.ToastRowMissingError
			if errors.As(err, &te) {
				// 02 §8: partial UPDATE (TOAST) mà row không có ở target. Không dựng lại được row nên
				// pipeline dừng với verify_mismatch (exit 5) và chỉ ra range cần verify/sửa.
				src := te.Table
				if s, ok := p.sourceOf(te.Table); ok {
					src = s
				}
				p.metrics.VerifyMismatchTotal.WithLabelValues(p.id, src.String()).Inc()
				p.log.Error("toast row missing at target: verify and repair this key, then restart",
					"code", exitcode.ErrVerifyMismatch, "table", src.String(), "key", fmt.Sprint(te.Key),
					"hint", fmt.Sprintf("strand verify --mode full --table %s --repair fix", src))
			}
			var rn *writer.RowsNotWrittenError
			if errors.As(err, &rn) {
				// F11: batch đã rollback trước commit (cùng row delta), không Txn nào được đánh dấu đã apply.
				p.metrics.RowsNotWrittenTotal.WithLabelValues(p.id).Inc()
				p.log.Error("rows routed but not written", "worker", id, "routed", rn.Routed, "handed", rn.Handed,
					"written", rn.Written, "seqs", fmt.Sprint(rn.Seqs), "positions", strings.Join(rn.Positions, ","))
			}
			p.fail(fmt.Errorf("worker %d: %w", id, err))
		}
		return false
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
	return true
}

// deltaFor cắt khỏi tập của worker phần đã có trong row toàn cục (design §1, bước 3 của
// compaction) và bảo đảm row delta ≤ 64 KB (CP-4).
func (p *Pipeline) deltaFor(ctx context.Context, mine checkpoint.AppliedSet) (string, error) {
	trim := func() {
		checkpoint.SubtractApplied(mine, p.comp.LastGlobal())
	}
	trim()
	s := mine.(fmt.Stringer).String()
	if len(s) <= checkpoint.MaxDeltaBytes {
		return s, nil
	}
	if err := p.comp.Compact(ctx); err != nil {
		return "", fmt.Errorf("sync compaction: %w", err)
	}
	trim()
	s = mine.(fmt.Stringer).String()
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
	if hs, ok := high.(*checkpoint.PGAppliedSet); ok {
		return p.rendezvousPG(ctx, hs.Lwd().LSN)
	}
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
	seen, ok := p.seen.(*checkpoint.MySQLAppliedSet)
	if !ok {
		return fmt.Errorf("snapshot rendezvous needs a MySQL source")
	}
	for !h.IsSubsetOf(seen) {
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

func (p *Pipeline) pace(ctx context.Context, chunkBytes int64) error {
	if err := p.paceGate(ctx); err != nil {
		return err
	}
	// Chi phí chunk ước lượng (trang 16 KiB) chia cho tốc độ background R (token/giây), qua token bucket
	// chung: nhiều reader song song cùng chia một R.
	rate := p.backgroundRate()
	if rate <= 0 {
		return nil
	}
	cost := chunkReadPages(chunkBytes, p.curAvgRow.Load(), p.snapChunkRows.Load())
	wait := p.snapBucket.reserve(time.Now(), time.Duration(cost/rate*float64(time.Second)))
	if wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	// Nếu `pause --snapshot` được bật trong lúc đang ngủ giãn nhịp giữa hai chunk, chờ tiếp.
	for p.snapPaused.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.govTick):
		}
	}
	return nil
}

// paceGate chờ tới khi background được chạy: không `pause --snapshot`, breaker đóng, trong
// bootstrap.logical.windows và governor cho background chạy. bootstrap_retention_risk bỏ qua windows và
// việc governor dừng background (để snapshot không tự khóa chết, 02 §7), nhưng không bỏ qua breaker
// hay pause, và KHÔNG bỏ qua giãn nhịp: trước đây nhánh này trả về ngay trước bước giãn nhịp, nên
// snapshot chạy không giới hạn — vượt cả hard limit ("tăng ưu tiên … vẫn trong hard limit").
func (p *Pipeline) paceGate(ctx context.Context) error {
	gated := false
	defer func() {
		if gated {
			p.snapGated.Add(-1)
		}
	}()
	for {
		// Lệnh `pause --snapshot` dừng riêng snapshot, thắng cả nhánh retentionRisk.
		if !p.snapPaused.Load() {
			breaker := p.govAlgo != "static" && p.gov.BreakerOpen()
			// Breaker luôn thắng: background dừng (02 §7).
			risk := p.retentionRisk.Load()
			windowOK := risk || inWindows(p.cfg.Bootstrap.Logical.Windows, time.Now())
			running := risk || p.govAlgo == "static" || p.gov.BackgroundRunning()
			if windowOK && running && !breaker {
				return nil
			}
		}
		if !gated {
			gated = true
			p.snapGated.Add(1)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.govTick):
		}
	}
}

// hardRate: resources.source.iops.hard (0 = không cấu hình).
func (p *Pipeline) hardRate() float64 {
	if p.cfg == nil {
		return 0
	}
	return float64(p.cfg.Resources.Source.IOPS.Hard)
}

// backgroundRate: R của background (token/giây) mà token bucket của snapshot dùng.
//   - static: soft limit hiện hành.
//   - grace-v2: phần background của arbiter (đã gồm luật P_lag theo pha).
//   - onoff: soft limit khi chạy; bootstrap_retention_risk → hard limit (tăng ưu tiên, 02 §7).
//
// Governor trả 0 (chưa tick) thì lùi về soft. Mọi governor bị chặn bởi hard limit ở token bucket.
func (p *Pipeline) backgroundRate() float64 {
	var rate float64
	if p.govAlgo != "static" && p.gov != nil {
		_, rate = p.gov.CurrentRate()
		if _, grace := p.gov.(*governor.GraceGovernor); !grace && p.retentionRisk.Load() {
			rate = math.Max(rate, p.hardRate())
		}
	}
	if p.govAlgo == "static" || rate <= 0 {
		rate = p.softRate() // soft limit hiện hành (cấu hình, hoặc `throttle set --soft`)
	}
	if hard := p.hardRate(); hard > 0 && rate > hard {
		rate = hard
	}
	return rate
}

// nextChunkRows: luật cỡ chunk của governor (02 §7) — rows · clamp(250 ms / thời gian chunk trước,
// 0,5, 2), rồi chi phí ước lượng ≤ R · 0,25 s với R = backgroundRate. Chi phí một row tính theo trang
// 16 KiB từ byte thực đọc (không có thì AVG_ROW_LENGTH).
func (p *Pipeline) nextChunkRows(_ model.TableID, st snapshot.ChunkStat) int64 {
	avg := p.curAvgRow.Load()
	if st.ReadRows > 0 && st.Bytes > 0 {
		avg = st.Bytes / st.ReadRows
	}
	if avg <= 0 {
		avg = 200
	}
	rows := st.Rows
	if rows <= 0 {
		rows = p.snapChunkRows.Load()
	}
	n := int64(governor.NextChunkRows(int(rows), st.Elapsed, float64(avg)/16384, p.backgroundRate()))
	p.snapChunkRows.Store(n)
	return n
}

// tokenBucket giãn nhịp kiểu trả sau (virtual scheduling): chunk vừa đọc tốn d = chi phí / R; thời điểm
// được đọc chunk kế tiếp (tat) lùi thêm d, tính từ max(tat, now). Một reader: chờ đúng d sau mỗi chunk
// (như trước). Nhiều reader: các khoản d cộng dồn, nên tổng tốc độ vẫn ≤ R.
type tokenBucket struct {
	mu  sync.Mutex
	tat time.Time
}

// reserve ghi nhận chi phí d tại now và trả thời gian phải chờ.
func (b *tokenBucket) reserve(now time.Time, d time.Duration) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tat.Before(now) {
		b.tat = now
	}
	b.tat = b.tat.Add(d)
	return b.tat.Sub(now)
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
	if pl, ok := p.state.Plan(id.String()); ok && !pl.Empty && pl.NextPK > pl.MinPK {
		p.log.Info("snapshot resumes from saved plan", "table", id.String(), "next_pk", pl.NextPK, "max_pk", pl.MaxPK)
	}
	if p.pgChunks != nil {
		if err := p.pgChunks.WaitGate(ctx); err != nil {
			return err
		}
	}
	// Reader snapshot bắt đầu dùng cửa sổ L (windowSaturated); chờ cổng PG ở trên không tính.
	p.snapReaders.Add(1)
	defer p.snapReaders.Add(-1)
	rereads := p.snap.RereadKeysTotal.Load()
	var err error
	if m.PKNumeric {
		err = p.snap.SnapshotAdd(ctx, p.src, p.dst, id, m.PK[0], writableColumns(m), p.rendezvous)
	} else {
		err = p.snap.SnapshotAddKeys(ctx, p.src, p.dst, id, m.PKCodec, writableColumns(m), p.rendezvous)
	}
	if err != nil {
		return err
	}
	rereads = p.snap.RereadKeysTotal.Load() - rereads
	if err := p.state.SetDone(id.String(), true); err != nil {
		return err
	}
	p.routeMu.Lock()
	p.snap.Manager.RemoveTable(id)
	p.routeMu.Unlock()
	p.log.Info("snapshot done", "table", id.String(), "duration", time.Since(start).String(),
		"reread_keys", rereads) // khóa được đọc lại do UPDATE đổi PK mà cột TOAST không đổi (02 §8)
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
	if p.pgChunks != nil && p.pgChunks.GateErr != nil {
		return fmt.Errorf("logical snapshot from this PostgreSQL source is not possible: %w", p.pgChunks.GateErr)
	}
	// Giành quyền add bảng một cách nguyên tử: hai lệnh đồng thời cho cùng bảng chỉ một lệnh đi tiếp
	// (review R14). Nhả khi trả lỗi sớm, hoặc khi goroutine snapshot kết thúc.
	if !p.claimSnapshotAdd(id) {
		return fmt.Errorf("table %s is already being snapshotted", id)
	}
	started := false
	defer func() {
		if !started {
			p.releaseSnapshotAdd(id)
		}
	}()
	if p.snap.Manager.IsRegistered(id) {
		return fmt.Errorf("table %s is already being snapshotted", id)
	}
	m := p.table(id)
	if m == nil {
		one, err := newTableFilter([]string{id.String()}, nil, p.cfg.Tables.Rename)
		if err != nil {
			return err
		}
		metas, err := p.loadTables(ctx, one)
		if err != nil {
			return err
		}
		if m = metas[id]; m == nil {
			return fmt.Errorf("table %s does not exist on source", id)
		}
		if len(m.PK) == 0 {
			return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf("table %s has no primary key", id), nil)
		}
		// review R10: bảng mới không được trùng tên đích với một bảng đã thuộc pipeline.
		p.tablesMu.RLock()
		err = checkTargetCollisions(p.filter, map[model.TableID]*tableMeta{id: m}, p.tables)
		p.tablesMu.RUnlock()
		if err != nil {
			return err
		}
	}
	if err := p.checkSnapshotKey(ctx, id, m); err != nil {
		return err
	}
	if err := p.checkPGPartitionPublication(ctx, []model.TableID{id}); err != nil {
		return err
	}
	if p.conv != nil {
		if err := p.conv.load(ctx, p.src, id); err != nil {
			return err
		}
	}
	// invariant: RS-1/2 — bảng đích sẵn sàng trước đăng ký range và trước event mới.
	if p.cfg.Bootstrap.CreateSchemaAuto() {
		if err := p.createTargetTables(ctx, []model.TableID{id}); err != nil {
			return err
		}
	}
	if err := p.loadTargetGenerated(ctx, map[model.TableID]*tableMeta{id: m}); err != nil {
		return err
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
	started = true
	go func() {
		defer p.releaseSnapshotAdd(id)
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

// claimSnapshotAdd giành bảng id cho một lệnh snapshot add; false nếu lệnh khác đang giữ (review R14).
func (p *Pipeline) claimSnapshotAdd(id model.TableID) bool {
	p.snapAddMu.Lock()
	defer p.snapAddMu.Unlock()
	if p.snapAdding[id] {
		return false
	}
	if p.snapAdding == nil {
		p.snapAdding = map[model.TableID]bool{}
	}
	p.snapAdding[id] = true
	return true
}

func (p *Pipeline) releaseSnapshotAdd(id model.TableID) {
	p.snapAddMu.Lock()
	delete(p.snapAdding, id)
	p.snapAddMu.Unlock()
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
		p.throughput.sample(p.appliedRows.Load(), time.Now())
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
		if p.strict {
			// strict: không có row delta, row toàn cục ghi cùng mỗi commit (quy tắc 5).
			p.metrics.CheckpointCompactionLagSeconds.WithLabelValues(p.id).Set(0)
		} else if ls := p.comp.LastSuccess(); !ls.IsZero() {
			p.metrics.CheckpointCompactionLagSeconds.WithLabelValues(p.id).Set(time.Since(ls).Seconds())
		}
		if p.sched.Lwd() >= p.admitted.Load() {
			p.health.RecordProgress() // không có việc tồn đọng: vẫn coi là tiến
		}
		// P_lag theo pha (02 §7): ở bootstrap, P_lag ≥ 1 thì báo bootstrap_retention_risk và
		// TĂNG ưu tiên snapshot (không giãn chunk) thay vì dừng, để không tự khóa chết.
		if phase == "bootstrap" {
			pLag := math.Max(lag/lagSLO.Seconds(), p.walRetainedPressure())
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
			// Tính trên float64: NextPK-MinPK / MaxPK-MinPK+1 tràn int64 khi khoảng PK rộng hơn MaxInt64.
			ratio := (float64(pl.NextPK) - float64(pl.MinPK)) / (float64(pl.MaxPK) - float64(pl.MinPK) + 1)
			if pl.Finished {
				ratio = 1
			}
			m.SeedProgressRatio.WithLabelValues(p.id, table).Set(ratio)
		}
	}
	// Thao tác xuyên range đang chờ: khóa chờ đọc lại do UPDATE đổi PK mà cột TOAST không đổi (chỉ
	// source PG, 02 §8; MySQL luôn 0).
	pending, oldest := p.snap.Manager.PendingCrossRange(time.Now())
	m.PendingCrossRange.WithLabelValues(p.id).Set(float64(pending))
	m.PendingCrossRangeOldestSeconds.WithLabelValues(p.id).Set(oldest.Seconds())
	// onoff/static không ước lượng độ nhạy; GRACE v2 tự xuất metric này mỗi tick.
	if _, grace := p.gov.(*governor.GraceGovernor); !grace {
		m.SensitivityUnknown.WithLabelValues(p.id).Set(0)
	}
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
		m.GovernorRateTokens.WithLabelValues(p.id, "source_iops").Set(p.softRate())
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
	if p.srcEngine == model.EnginePG {
		// PG không có tập GTID để so: walsender của slot đã gửi tới head (02 §7: lag bootstrap = khoảng
		// cách reader → head). Trước đây luôn false → source PG rảnh vài giây là lag "tăng" mãi và
		// bootstrap_retention_risk bật sai.
		if p.pgSignals == nil {
			return false
		}
		qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		s, err := p.pgSignals.Sample(qctx)
		return err == nil && s.ReaderCaughtUp(governor.PGCaughtUpSlackBytes)
	}
	if p.srcEngine != model.EngineMySQL {
		return false
	}
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
	return set.IsSubsetOf(p.seen.(*checkpoint.MySQLAppliedSet))
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

// isRetryable: lỗi khóa của InnoDB (1213 deadlock, 1205 lock wait timeout) hoặc lỗi tạm thời
// của PG (deadlock, serialization failure): cả transaction đã rollback nên apply lại là an toàn.
func isRetryable(err error) bool {
	var me *drivermysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1213 || me.Number == 1205
	}
	return writer.IsPGRetryable(err)
}

// chunkReadPages: chi phí đọc một chunk tính theo trang 16 KiB, để giãn nhịp snapshot theo R (token/giây).
// Dùng số byte row thật của chunk vừa đọc. Trước đây luôn lấy AVG_ROW_LENGTH × số row MẶC ĐỊNH của chunk,
// bỏ qua chunk_rows cấu hình và dùng thống kê có thể đã cũ (bảng mới tạo có vài row → AVG_ROW_LENGTH
// hàng KB) — chunk 500 row bị tính như hàng nghìn trang, mỗi chunk chờ hàng phút (e2e N10). Không biết byte
// (≤ 0) thì lùi về AVG_ROW_LENGTH × chunk_rows hiệu lực. Tối thiểu một trang.
func chunkReadPages(chunkBytes, avgRow, chunkRows int64) float64 {
	if chunkBytes > 0 {
		return math.Max(1, float64(chunkBytes)/16384)
	}
	if avgRow <= 0 {
		avgRow = 200
	}
	if chunkRows <= 0 {
		chunkRows = snapshot.DefaultChunkOptions().DefaultChunkRows
	}
	return math.Max(1, float64(avgRow*chunkRows)/16384)
}
