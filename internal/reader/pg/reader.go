package pg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

// Reader hiện thực reader.Reader cho PostgreSQL bằng logical replication (pgoutput).
//
// Luồng sự kiện phát ra cho mỗi Txn (định danh bằng commit LSN, spec 02 §1):
//
//	EventQuery "BEGIN" → (EventTableMap khi schema của bảng chưa được báo / đã đổi) → EventRows… → EventQuery "COMMIT"
//
// Mọi Event của một Txn mang Pos = {EnginePG, commit LSN}. Txn có commit LSN mà `from.Contains` bị bỏ
// qua (CP-2). Txn streaming (proto ≥ 2) và two-phase (proto ≥ 3) được đệm trong RAM và chỉ phát khi
// commit thật (Stream Commit / Commit Prepared); Stream Abort / Rollback Prepared bị hủy, không phát.
//
// spec: 02 §1, §2 (CP-2), §3, §8; design "PG Logical Reader"
type Reader struct {
	cfg   Config
	pgCfg *pgconn.Config

	serverVersion   int
	inRecovery      bool
	consistentPoint uint64
	snapshotName    string         // tên snapshot export (chỉ khi cfg.ExportSnapshot)
	systemID        string         // systemid từ IDENTIFY_SYSTEM trên kết nối replication tạo slot
	exportConn      *pgconn.PgConn // kết nối replication giữ snapshot export sống; nil sau ReleaseSnapshot/Close

	mu           sync.Mutex
	conn         *pgconn.PgConn
	cancel       context.CancelFunc
	started      bool
	closed       bool
	pos          model.Pos
	protoVersion int
	twoPhase     bool
	streaming    bool
	messages     bool

	// confirmed: lwd do pipeline báo qua SetConfirmed (chỉ tăng).
	// progress: commit LSN của Txn cuối cùng reader đã xử lý xong (phát hoặc bỏ qua) trong phiên này.
	// hold: prepare LSN nhỏ nhất của Txn two-phase đang treo (đã PREPARE, chưa COMMIT/ROLLBACK PREPARED); 0 = không có.
	// flush_lsn gửi lên server = min(confirmed, progress, hold) — xem flushLSN.
	confirmed atomic.Uint64
	progress  atomic.Uint64
	hold      atomic.Uint64
	spills    atomic.Int64 // số lần phần thân Txn treo bị đổ ra file (review R08)

	writeMu sync.Mutex // tuần tự hóa việc ghi StandbyStatusUpdate lên kết nối
	wg      sync.WaitGroup
}

var _ reader.Reader = (*Reader)(nil)

// slotInfo là trạng thái slot đọc từ pg_replication_slots.
type slotInfo struct {
	plugin         string
	slotType       string
	database       string
	walStatus      string
	invalid        bool
	confirmedFlush uint64
	failover       bool
}

// New kiểm tra điều kiện tiên quyết (wal_level, phiên bản khi decode trên standby, trạng thái slot) và
// tạo slot nếu cfg.CreateSlot. Lỗi cấu hình trả exitcode.ErrBinlogConfigInvalid (exit 3); slot không tồn
// tại hoặc đã mất WAL/bị vô hiệu hóa trả exitcode.ErrLogGap (exit 5).
func New(cfg Config) (*Reader, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	pgCfg, err := pgconn.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "DSN PostgreSQL không hợp lệ", err)
	}
	r := &Reader{cfg: cfg, pgCfg: pgCfg, pos: model.Pos{Engine: model.EnginePG}}

	// Tạo slot trên standby phải chờ primary ghi running-xacts nên cho thời gian rộng.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := r.connectSQL(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	var walLevel, verStr string
	if err := conn.QueryRow(ctx, "SELECT current_setting('wal_level'), current_setting('server_version_num'), pg_is_in_recovery()").
		Scan(&walLevel, &verStr, &r.inRecovery); err != nil {
		return nil, fmt.Errorf("đọc cấu hình PostgreSQL: %w", err)
	}
	r.serverVersion, _ = strconv.Atoi(verStr)

	// spec: 02 §8 — PG: wal_level=logical.
	if walLevel != "logical" {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("wal_level phải là logical, hiện là %q", walLevel), nil)
	}
	// spec: 02 §8 — decode trên standby cần PG 16+ và hot_standby_feedback=on.
	if r.inRecovery {
		if r.serverVersion < 160000 {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("decode trên standby cần PostgreSQL 16+, server là %d", r.serverVersion), nil)
		}
		var hsf string
		if err := conn.QueryRow(ctx, "SELECT current_setting('hot_standby_feedback')").Scan(&hsf); err != nil {
			return nil, fmt.Errorf("đọc hot_standby_feedback: %w", err)
		}
		if hsf != "on" {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				"decode trên standby cần hot_standby_feedback=on", nil)
		}
	}

	info, found, err := r.inspectSlot(ctx, conn)
	if err != nil {
		return nil, err
	}
	if found && cfg.ExportSnapshot && !cfg.ExportOnCreate {
		// Snapshot chỉ export được lúc tạo slot; slot có sẵn không cho vị trí khớp snapshot nào.
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("replication slot %q đã tồn tại: ExportSnapshot cần tạo slot mới", cfg.Slot), nil)
	}
	if !found {
		if !cfg.CreateSlot {
			return nil, exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
				fmt.Sprintf("replication slot %q không tồn tại (CreateSlot=false)", cfg.Slot), nil)
		}
		if err := r.createSlot(ctx); err != nil {
			return nil, err
		}
		return r, nil
	}
	if err := r.checkSlot(conn.Config().Database, info); err != nil {
		return nil, err
	}
	return r, nil
}

// connectSQL mở kết nối SQL thường (không replication) để kiểm tra cấu hình và slot.
func (r *Reader) connectSQL(ctx context.Context) (*pgx.Conn, error) {
	ccfg, err := pgx.ParseConfig(r.cfg.DSN)
	if err != nil {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "DSN PostgreSQL không hợp lệ", err)
	}
	conn, err := pgx.ConnectConfig(ctx, ccfg)
	if err != nil {
		return nil, fmt.Errorf("kết nối PostgreSQL: %w", err)
	}
	return conn, nil
}

// replicationConfig trả cấu hình kết nối replication=database. Các GUC của phiên được cố định để
// định dạng text của pgoutput khớp bảng mapping trong mapping.go.
func (r *Reader) replicationConfig() *pgconn.Config {
	c := r.pgCfg.Copy()
	if c.RuntimeParams == nil {
		c.RuntimeParams = map[string]string{}
	}
	c.RuntimeParams["replication"] = "database"
	c.RuntimeParams["DateStyle"] = "ISO, YMD"
	c.RuntimeParams["TimeZone"] = "UTC"
	c.RuntimeParams["IntervalStyle"] = "postgres"
	c.RuntimeParams["bytea_output"] = "hex"
	c.RuntimeParams["extra_float_digits"] = "3"
	return c
}

// inspectSlot đọc trạng thái slot. invalid=true khi slot đã bị vô hiệu hóa (PG16: conflicting,
// PG17+: invalidation_reason) — ví dụ vì mất WAL hoặc xung đột recovery trên standby.
func (r *Reader) inspectSlot(ctx context.Context, conn *pgx.Conn) (slotInfo, bool, error) {
	invalidExpr, failoverExpr := "false", "false"
	switch {
	case r.serverVersion >= 170000:
		invalidExpr, failoverExpr = "invalidation_reason IS NOT NULL", "failover"
	case r.serverVersion >= 160000:
		invalidExpr = "coalesce(conflicting, false)"
	}
	q := fmt.Sprintf(`SELECT coalesce(plugin, ''), slot_type, coalesce(database::text, ''), coalesce(wal_status, ''),
		%s, coalesce(confirmed_flush_lsn::text, '0/0'), %s
		FROM pg_replication_slots WHERE slot_name = $1`, invalidExpr, failoverExpr)
	var info slotInfo
	var cf string
	err := conn.QueryRow(ctx, q, r.cfg.Slot).Scan(&info.plugin, &info.slotType, &info.database, &info.walStatus,
		&info.invalid, &cf, &info.failover)
	if errors.Is(err, pgx.ErrNoRows) {
		return info, false, nil
	}
	if err != nil {
		return info, false, fmt.Errorf("đọc pg_replication_slots: %w", err)
	}
	lsn, err := pglogrepl.ParseLSN(cf)
	if err != nil {
		return info, false, fmt.Errorf("parse confirmed_flush_lsn %q: %w", cf, err)
	}
	info.confirmedFlush = uint64(lsn)
	return info, true, nil
}

// checkSlot đối chiếu slot với yêu cầu của reader.
func (r *Reader) checkSlot(database string, info slotInfo) error {
	if info.walStatus == "lost" || info.invalid {
		// spec: 02 §12 — log_gap (exit 5): slot không còn đọc được dữ liệu liền mạch.
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("replication slot %q đã mất WAL hoặc bị vô hiệu hóa (wal_status=%q)", r.cfg.Slot, info.walStatus), nil)
	}
	if info.slotType != "logical" || info.plugin != "pgoutput" {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("slot %q phải là slot logical dùng pgoutput (type=%s, plugin=%s)", r.cfg.Slot, info.slotType, info.plugin), nil)
	}
	if database != "" && info.database != database {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("slot %q thuộc database %q, không phải %q", r.cfg.Slot, info.database, database), nil)
	}
	return nil
}

// createSlot chạy CREATE_REPLICATION_SLOT … LOGICAL pgoutput, kèm FAILOVER khi cfg.Failover và server ≥ 17.
// Mặc định SNAPSHOT 'nothing' và đóng kết nối ngay. Với cfg.ExportSnapshot: SNAPSHOT 'export' và giữ
// kết nối replication mở (snapshot chỉ sống khi kết nối chưa chạy lệnh khác / chưa đóng) tới
// ReleaseSnapshot hoặc Close. Trước đó chạy IDENTIFY_SYSTEM trên cùng kết nối để lấy systemid.
func (r *Reader) createSlot(ctx context.Context) error {
	var opts []string
	failover := false
	if r.cfg.Failover {
		switch {
		case r.inRecovery:
			// PG17 cấm failover slot trên standby (chỉ đồng bộ được từ primary xuống).
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				"Failover=true không dùng được khi tạo slot trên standby; tạo failover slot trên primary (PG17+)", nil)
		case r.serverVersion >= 170000:
			opts = append(opts, "FAILOVER")
			failover = true
		default:
			slog.Warn("server < PG17 không hỗ trợ failover slot, tạo slot thường",
				"slot", r.cfg.Slot, "server_version", r.serverVersion)
		}
	}
	var sql string
	if r.serverVersion >= 150000 {
		snapOpt := "SNAPSHOT 'nothing'"
		if r.cfg.ExportSnapshot || r.cfg.ExportOnCreate {
			snapOpt = "SNAPSHOT 'export'"
		}
		opts = append([]string{snapOpt}, opts...)
		sql = fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput (%s)", r.cfg.Slot, strings.Join(opts, ", "))
	} else {
		snapOpt := "NOEXPORT_SNAPSHOT"
		if r.cfg.ExportSnapshot || r.cfg.ExportOnCreate {
			snapOpt = "EXPORT_SNAPSHOT"
		}
		sql = fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput %s", r.cfg.Slot, snapOpt)
	}

	conn, err := pgconn.ConnectConfig(ctx, r.replicationConfig())
	if err != nil {
		return fmt.Errorf("kết nối replication để tạo slot: %w", err)
	}

	ident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		_ = conn.Close(context.Background())
		return fmt.Errorf("IDENTIFY_SYSTEM: %w", err)
	}
	r.systemID = ident.SystemID

	res, err := pglogrepl.ParseCreateReplicationSlot(conn.Exec(ctx, sql))
	if err != nil {
		_ = conn.Close(context.Background())
		return classifyErr(fmt.Errorf("tạo replication slot %q: %w", r.cfg.Slot, err))
	}
	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		_ = conn.Close(context.Background())
		return fmt.Errorf("parse consistent_point %q: %w", res.ConsistentPoint, err)
	}
	r.consistentPoint = uint64(lsn)
	r.snapshotName = res.SnapshotName
	r.pos.LSN = r.consistentPoint
	if r.cfg.ExportSnapshot || r.cfg.ExportOnCreate {
		if res.SnapshotName == "" {
			_ = conn.Close(context.Background())
			return fmt.Errorf("CREATE_REPLICATION_SLOT %q không trả snapshot_name dù yêu cầu export", r.cfg.Slot)
		}
		r.exportConn = conn
	} else {
		_ = conn.Close(context.Background())
	}
	slog.Info("đã tạo replication slot", "slot", r.cfg.Slot, "consistent_point", res.ConsistentPoint,
		"snapshot_name", res.SnapshotName, "failover", failover, "standby", r.inRecovery)
	return nil
}

// ConsistentPoint trả consistent_point khi slot được tạo bởi New (0 nếu slot có sẵn).
func (r *Reader) ConsistentPoint() model.Pos {
	return model.Pos{Engine: model.EnginePG, LSN: r.consistentPoint}
}

// SnapshotName trả snapshot_name do CREATE_REPLICATION_SLOT export khi cfg.ExportSnapshot ("" nếu không).
// Snapshot chỉ dùng được (SET TRANSACTION SNAPSHOT / pg_dump --snapshot) trước ReleaseSnapshot/Close.
func (r *Reader) SnapshotName() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotName
}

// SystemID trả systemid (system_identifier của cluster) đọc bằng IDENTIFY_SYSTEM khi reader tạo slot
// ("" nếu slot có sẵn).
func (r *Reader) SystemID() string { return r.systemID }

// ReleaseSnapshot đóng kết nối replication đang giữ snapshot export (giải phóng xmin trên source).
// Slot vẫn còn trên server. Gọi lại nhiều lần không sao.
func (r *Reader) ReleaseSnapshot() error {
	r.mu.Lock()
	conn := r.exportConn
	r.exportConn = nil
	r.mu.Unlock()
	if conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.Close(ctx)
}

// Position trả commit LSN của Txn cuối cùng reader đã xử lý (hoặc điểm bắt đầu).
func (r *Reader) Position() model.Pos {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pos
}

// ServerVersion trả server_version_num của source; InRecovery cho biết source là standby.
func (r *Reader) ServerVersion() int { return r.serverVersion }

// InRecovery cho biết source là standby (pg_is_in_recovery()).
func (r *Reader) InRecovery() bool { return r.inRecovery }

// Messages cho biết pgoutput có gửi logical decoding message (option messages, PG14+) hay không:
// watermark của snapshot logic từ source PG cần nó (xem WatermarkPrefix).
func (r *Reader) Messages() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.messages
}

// Protocol trả proto_version, streaming, two_phase thực sự đã thương lượng được với server.
func (r *Reader) Protocol() (protoVersion int, streaming, twoPhase bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.protoVersion, r.streaming, r.twoPhase
}

// SetConfirmed cập nhật lwd mà pipeline đã apply bền vững. flush_lsn của StandbyStatusUpdate không
// bao giờ vượt giá trị này (slot không được tiến quá phần đã apply).
func (r *Reader) SetConfirmed(lsn uint64) {
	for {
		cur := r.confirmed.Load()
		if lsn <= cur || r.confirmed.CompareAndSwap(cur, lsn) {
			return
		}
	}
}

// flushLSN = min(confirmed, progress, hold):
//   - confirmed: không xác nhận phần chưa apply;
//   - progress: không xác nhận vượt chỗ reader đã đọc trong phiên — nếu không, sau restart một Txn
//     two-phase đã PREPARE trước lwd mà reader chưa kịp thấy lại sẽ bị server bỏ qua;
//   - hold: giữ slot không vượt prepare LSN của Txn two-phase đang treo, vì Txn đó chỉ nằm trong RAM;
//     sau restart server phải gửi lại PREPARE.
func (r *Reader) flushLSN() uint64 {
	f := r.confirmed.Load()
	if p := r.progress.Load(); p < f {
		f = p
	}
	if h := r.hold.Load(); h != 0 && h < f {
		f = h
	}
	return f
}

// Start mở kết nối replication và bắt đầu stream từ lwd của from.
//
// Điểm bắt đầu: server luôn bắt đầu tại max(LSN yêu cầu, confirmed_flush_lsn của slot). Reader yêu cầu
// min(lwd, confirmed_flush): bình thường hai giá trị bằng nhau hoặc confirmed_flush thấp hơn một chút
// (status gửi theo chu kỳ, hoặc bị giữ lại vì Txn two-phase đang treo); các Txn đã apply trong khoảng đó
// được gửi lại và bị bỏ qua nhờ from.Contains (CP-2). Nếu confirmed_flush > lwd thì slot đã tiến quá phần
// đã apply — dữ liệu ở giữa không đọc lại được nữa — trả ErrLogGap. from rỗng (lwd = 0) → đọc từ
// confirmed_flush của slot (consistent_point với slot mới).
func (r *Reader) Start(ctx context.Context, from checkpoint.AppliedSet) (<-chan reader.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("pg reader đã đóng")
	}
	if r.started {
		return nil, errors.New("pg reader đã Start; mỗi Reader chỉ Start một lần")
	}
	if r.cfg.Publication == "" {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			"PostgreSQL publication name is required", nil)
	}
	if r.cfg.ownSpillDir {
		// File spill của lần chạy trước (bị kill): server stream lại các Txn đó, file cũ vô dụng.
		old, _ := filepath.Glob(filepath.Join(r.cfg.SpillDir, "pgtxn-*.spill"))
		for _, f := range old {
			_ = os.Remove(f)
		}
	}

	var applied checkpoint.AppliedSet
	var lwd uint64
	if from != nil {
		applied = from.Clone() // ảnh chụp: pipeline có thể sửa from song song
		lwd = applied.Lwd().LSN
	}

	pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
	defer pcancel()
	sqlConn, err := r.connectSQL(pctx)
	if err != nil {
		return nil, err
	}
	info, found, err := r.inspectSlot(pctx, sqlConn)
	database := sqlConn.Config().Database
	_ = sqlConn.Close(context.Background())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("replication slot %q không tồn tại", r.cfg.Slot), nil)
	}
	if err := r.checkSlot(database, info); err != nil {
		return nil, err
	}

	startLSN := lwd
	switch {
	case lwd == 0:
		startLSN = info.confirmedFlush
	case info.confirmedFlush > lwd+1:
		// Server stream mọi Txn có commit LSN ≥ confirmed_flush (commit record bắt đầu đúng tại
		// confirmed_flush KHÔNG bị bỏ). Đã apply mọi Txn ≤ lwd nên confirmed_flush = lwd+1 không hổng
		// (vd. seed tại P: tập đã apply là ≤ P−1, slot ở P).
		return nil, exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("slot %q đã xác nhận tới %s, vượt lwd đã apply %s: Txn ở giữa không đọc lại được",
				r.cfg.Slot, pglogrepl.LSN(info.confirmedFlush), pglogrepl.LSN(lwd)), nil)
	case info.confirmedFlush < lwd:
		startLSN = info.confirmedFlush
	}

	conn, opts, err := r.startReplication(ctx, startLSN)
	if err != nil {
		return nil, err
	}

	r.conn = conn
	r.protoVersion, r.streaming, r.twoPhase, r.messages = opts.proto, opts.streaming, opts.twoPhase, opts.messages
	r.started = true
	r.pos = model.Pos{Engine: model.EnginePG, LSN: startLSN}
	r.confirmed.Store(lwd)
	r.progress.Store(startLSN)
	r.hold.Store(0)

	sctx, scancel := context.WithCancel(ctx)
	r.cancel = scancel
	out := make(chan reader.Event, r.cfg.BufferSize)

	r.wg.Add(2)
	go r.statusLoop(sctx)
	go r.readLoop(sctx, scancel, out, applied)
	return out, nil
}

// streamOpts là bộ option START_REPLICATION đã thương lượng.
type streamOpts struct {
	proto     int
	streaming bool
	twoPhase  bool
	messages  bool
}

func (o streamOpts) pluginArgs(pub string) []string {
	args := []string{
		fmt.Sprintf("proto_version '%d'", o.proto),
		fmt.Sprintf("publication_names %s", quoteLiteral(quoteIdent(pub))),
	}
	if o.streaming {
		args = append(args, "streaming 'on'")
	}
	if o.twoPhase {
		args = append(args, "two_phase 'on'")
	}
	if o.messages {
		args = append(args, "messages 'true'")
	}
	return args
}

var protoMaxRe = regexp.MustCompile(`server only supports protocol (\d+) or lower`)

// startReplication chạy START_REPLICATION với proto_version '4', streaming 'on', two_phase 'on'.
// Lùi bậc khi server từ chối (không panic):
//   - "client sent proto_version=4 but server only supports protocol N or lower" → dùng proto N
//     (N < 3: tắt two_phase; N < 2: tắt streaming, đúng yêu cầu tối thiểu của pgoutput);
//   - "unrecognized pgoutput option: streaming|two_phase" (server cũ) → bỏ option đó;
//   - PG17 từ chối two-phase trên failover slot → bỏ two_phase (Txn PREPARE khi đó được server gửi như
//     Txn thường tại COMMIT PREPARED, ROLLBACK PREPARED không sinh dữ liệu).
//
// Mỗi lần thử dùng một kết nối mới.
func (r *Reader) startReplication(ctx context.Context, startLSN uint64) (*pgconn.PgConn, streamOpts, error) {
	o := streamOpts{proto: 4, streaming: true, twoPhase: true, messages: true}
	for attempt := 0; attempt < 6; attempt++ {
		conn, err := pgconn.ConnectConfig(ctx, r.replicationConfig())
		if err != nil {
			return nil, o, fmt.Errorf("kết nối replication: %w", err)
		}
		err = pglogrepl.StartReplication(ctx, conn, r.cfg.Slot, pglogrepl.LSN(startLSN), pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: o.pluginArgs(r.cfg.Publication),
		})
		if err == nil {
			slog.Info("pg reader bắt đầu stream", "slot", r.cfg.Slot, "start_lsn", pglogrepl.LSN(startLSN),
				"proto_version", o.proto, "streaming", o.streaming, "two_phase", o.twoPhase, "messages", o.messages)
			return conn, o, nil
		}
		_ = conn.Close(context.Background())

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			return nil, o, classifyErr(fmt.Errorf("START_REPLICATION: %w", err))
		}
		msg := pgErr.Message
		next := o
		switch {
		case protoMaxRe.MatchString(msg):
			n, _ := strconv.Atoi(protoMaxRe.FindStringSubmatch(msg)[1])
			if n >= o.proto || n < 1 {
				return nil, o, classifyErr(fmt.Errorf("START_REPLICATION: %w", err))
			}
			next.proto = n
			if n < 3 {
				next.twoPhase = false
			}
			if n < 2 {
				next.streaming = false
			}
		case strings.Contains(msg, "unrecognized pgoutput option") && strings.Contains(msg, "two_phase") && o.twoPhase:
			next.twoPhase = false
		case strings.Contains(msg, "unrecognized pgoutput option") && strings.Contains(msg, "streaming") && o.streaming:
			next.streaming = false
		case strings.Contains(msg, "unrecognized pgoutput option") && strings.Contains(msg, "messages") && o.messages:
			next.messages = false
		case o.twoPhase && (strings.Contains(msg, "two-phase") || strings.Contains(msg, "two_phase")):
			next.twoPhase = false
		default:
			return nil, o, classifyErr(fmt.Errorf("START_REPLICATION: %w", err))
		}
		slog.Warn("server từ chối option START_REPLICATION, lùi bậc", "slot", r.cfg.Slot, "err", msg,
			"proto_version", next.proto, "streaming", next.streaming, "two_phase", next.twoPhase)
		o = next
	}
	return nil, o, fmt.Errorf("START_REPLICATION: không thương lượng được option với server")
}

// classifyErr gắn mã lỗi spec cho lỗi từ server: slot không tồn tại / bị vô hiệu hóa / WAL đã bị xóa
// → log_gap (exit 5); thiếu wal_level=logical → binlog_config_invalid (exit 3).
func classifyErr(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	m := pgErr.Message + " " + pgErr.Detail
	switch {
	case pgErr.Code == "42704" && strings.Contains(m, "replication slot"),
		strings.Contains(m, "has already been removed"),
		strings.Contains(m, "invalidated"),
		strings.Contains(m, "can no longer"):
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "replication slot không còn đọc liền mạch được", err)
	case strings.Contains(m, "wal_level"):
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "cấu hình WAL không cho phép logical decoding", err)
	}
	return err
}

// statusLoop gửi StandbyStatusUpdate theo chu kỳ từ goroutine riêng, nên vẫn chạy khi readLoop đang
// bị chặn vì kênh Event đầy (backpressure): walsender đọc reply cả khi đang chờ socket ghi được, nên
// kết nối không bị wal_sender_timeout cắt.
func (r *Reader) statusLoop(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(r.cfg.StatusInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.sendStatus(false); err != nil && ctx.Err() == nil {
				slog.Warn("gửi StandbyStatusUpdate thất bại", "slot", r.cfg.Slot, "err", err)
			}
		}
	}
}

// sendStatus gửi StandbyStatusUpdate với write = flush = apply = flushLSN(). Với slot logical, server
// chỉ dùng flush để dời confirmed_flush_lsn; khi flushLSN() = 0 cả ba bằng 0 và server bỏ qua.
func (r *Reader) sendStatus(replyRequested bool) error {
	r.mu.Lock()
	conn := r.conn
	r.mu.Unlock()
	if conn == nil {
		return nil
	}
	f := pglogrepl.LSN(r.flushLSN())
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: f,
		WALFlushPosition: f,
		WALApplyPosition: f,
		ClientTime:       time.Now(),
		ReplyRequested:   replyRequested,
	})
}

// Close dừng các goroutine, đóng kết nối. Kênh Event được đóng khi readLoop thoát.
func (r *Reader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()

	r.wg.Wait()
	_ = r.ReleaseSnapshot()

	r.mu.Lock()
	conn := r.conn
	r.conn = nil
	r.mu.Unlock()
	if conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.Close(ctx)
}

// quoteIdent bọc định danh trong dấu nháy kép (publication_names là danh sách định danh).
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// quoteLiteral bọc chuỗi trong dấu nháy đơn.
func quoteLiteral(s string) string { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }

// errEvent tạo EventError.
func errEvent(err error) reader.Event { return reader.Event{Type: reader.EventError, Err: err} }

// Spills trả số lần phần thân một Txn streaming / two-phase bị đổ ra file vì vượt MaxBufferedBytes.
func (r *Reader) Spills() int64 { return r.spills.Load() }
