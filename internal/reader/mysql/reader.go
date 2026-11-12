package mysql

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

// Config chứa cấu hình kết nối của MySQLReader.
type Config struct {
	DSN        string
	Host       string
	Port       uint16
	User       string
	Password   string
	ServerID   uint32
	Flavor     string
	BufferSize int
	// TLS cho kết nối replication; New lấy từ tham số tls= của DSN (true, skip-verify, preferred hoặc
	// tên đã RegisterTLSConfig). Thiếu bước này thì binlog đi plaintext dù DSN yêu cầu TLS (review R15).
	TLS *tls.Config
	// CollationCharsets: collation id -> charset (information_schema.COLLATIONS của source). Khác nil thì
	// giá trị chuỗi của cột không phải UTF-8 được chuyển sang UTF-8 (charset.go). nil = giữ byte nguyên.
	CollationCharsets map[uint64]string
	// TableInfo: cấu trúc bảng source (information_schema) cho TABLE_MAP không có metadata cột (MySQL 5.7
	// không có binlog_row_metadata): tên cột, PK, unsigned, charset, nhãn ENUM/SET (tableinfo.go).
	// nil = chỉ dùng metadata của TABLE_MAP.
	TableInfo TableInfoProvider
	// SchemaHistory: nơi lưu bền các info đã đọc kèm khoảng hiệu lực GTID (schemahist.go), để sau khi
	// khởi động lại binlog trước một DDL đang chạy lúc Strand dừng vẫn giải mã theo cấu trúc lúc đó.
	// nil = không lưu (chỉ dùng information_schema hiện tại).
	SchemaHistory SchemaHistoryStore
}

// MySQLReader hiện thực interface reader.Reader cho MySQL binlog.
// spec: 02 §3, D §1
type MySQLReader struct {
	cfg        Config
	mu         sync.RWMutex
	currentPos model.Pos
	syncer     *replication.BinlogSyncer
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closed     bool
}

// New khởi tạo MySQLReader từ Config.
func New(cfg Config) (*MySQLReader, error) {
	if cfg.DSN != "" {
		dbCfg, err := drivermysql.ParseDSN(cfg.DSN)
		if err != nil {
			return nil, exitcode.New(
				exitcode.ErrBinlogConfigInvalid,
				exitcode.Rejected,
				fmt.Sprintf("invalid MySQL source DSN %q: %v", cfg.DSN, err),
				err,
			)
		}
		if cfg.User == "" {
			cfg.User = dbCfg.User
		}
		if cfg.Password == "" {
			cfg.Password = dbCfg.Passwd
		}
		if cfg.TLS == nil && dbCfg.TLS != nil {
			cfg.TLS = dbCfg.TLS.Clone()
			if cfg.TLS.ServerName == "" && !cfg.TLS.InsecureSkipVerify {
				if h, _, err := net.SplitHostPort(dbCfg.Addr); err == nil {
					cfg.TLS.ServerName = h
				}
			}
		}
		if cfg.Host == "" && dbCfg.Addr != "" {
			h, p, err := net.SplitHostPort(dbCfg.Addr)
			if err == nil {
				cfg.Host = h
				if portNum, err := strconv.Atoi(p); err == nil {
					cfg.Port = uint16(portNum)
				}
			} else {
				cfg.Host = dbCfg.Addr
				cfg.Port = 3306
			}
		}
	}

	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 3306
	}
	if cfg.User == "" {
		cfg.User = "root"
	}
	if cfg.Flavor == "" {
		cfg.Flavor = "mysql"
	}
	if cfg.ServerID == 0 {
		cfg.ServerID = 1001
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}

	return &MySQLReader{
		cfg: cfg,
		currentPos: model.Pos{
			Engine: model.EngineMySQL,
		},
	}, nil
}

// Position trả về vị trí GTID đọc được gần nhất.
func (r *MySQLReader) Position() model.Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentPos
}

func (r *MySQLReader) setPos(gtid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentPos = model.Pos{
		Engine: model.EngineMySQL,
		GTID:   gtid,
	}
}

// Start bắt đầu stream binlog từ vị trí xác định trong AppliedSet.
// spec: 02 §3
func (r *MySQLReader) Start(ctx context.Context, from checkpoint.AppliedSet) (<-chan reader.Event, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, fmt.Errorf("reader is already closed")
	}
	r.mu.Unlock()

	// Chế độ vị trí binlog (source không GTID): đọc từ (file, offset) = Lwd của tập đã apply, tức vị trí
	// bắt đầu của Txn chưa apply sớm nhất (CP-3). Mọi Txn ≥ đó mà đã apply nằm trong tập nên router bỏ (CP-2).
	var posSet *checkpoint.MySQLPosAppliedSet
	var startPos mysql.Position
	var gset mysql.GTIDSet
	if ps, ok := from.(*checkpoint.MySQLPosAppliedSet); ok {
		posSet = ps
		lw := ps.Lwd()
		if ps.IsEmpty() || !lw.IsBinlogPos() {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				"binlog position mode needs a start position (--start-at <file>:<position>)", nil)
		}
		startPos = mysql.Position{Name: lw.File, Pos: uint32(lw.BinlogPos)}
	} else {
		var err error
		if gset, err = ToMysqlGTIDSet(from); err != nil {
			return nil, err
		}
	}

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: r.cfg.ServerID,
		Flavor:   r.cfg.Flavor,
		Host:     r.cfg.Host,
		Port:     r.cfg.Port,
		User:     r.cfg.User,
		Password: r.cfg.Password,
		// Giá trị thời gian giữ dạng chuỗi, TIMESTAMP quy về UTC; writer đặt session time_zone='+00:00'.
		ParseTime:               false,
		TimestampStringLocation: time.UTC,
		TLSConfig:               r.cfg.TLS,
	}
	if r.cfg.Flavor == "mariadb" {
		// Cột COMPRESSED của MariaDB dùng kiểu TABLE_MAP mà go-mysql không biết (mariadb_tablemap.go).
		syncerCfg.RowsEventDecodeFunc = MariaDBRowsDecode
	}

	syncer := replication.NewBinlogSyncer(syncerCfg)
	var streamer *replication.BinlogStreamer
	var err error
	if posSet != nil {
		streamer, err = syncer.StartSync(startPos)
	} else {
		streamer, err = syncer.StartSyncGTID(gset)
	}
	if err != nil {
		syncer.Close()
		// Lỗi kết nối/mạng là lỗi tạm thời (exit 1, systemd sẽ restart), không phải
		// "cấu hình bị từ chối" (exit 3, RestartPreventExitStatus).
		return nil, fmt.Errorf("start binlog stream from %s:%d: %w", r.cfg.Host, r.cfg.Port, err)
	}

	ctxRead, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.syncer = syncer
	r.cancel = cancel
	r.mu.Unlock()

	out := make(chan reader.Event, r.cfg.BufferSize)
	r.wg.Add(1)

	go func() {
		defer r.wg.Done()
		defer close(out)
		defer syncer.Close()

		st := &streamState{tableMap: make(map[uint64]*replication.TableMapEvent), ctx: ctxRead}
		// Lịch sử schema bền: mốc From/Until theo GTID, hoặc theo khóa vị trí binlog ở chế độ vị trí (SH-1/SH-2,
		// schemahist.go): start = Lwd của tập đã apply = nơi luồng bắt đầu đọc lại.
		if r.cfg.TableInfo != nil {
			if posSet != nil {
				st.hist, st.infoCache = openSchemaHistoryPos(r.cfg.SchemaHistory, posSet.LwdKey())
			} else {
				st.hist, st.infoCache = openSchemaHistory(r.cfg.SchemaHistory, gset)
			}
		}
		emit := func(e reader.Event) bool {
			select {
			case out <- e:
				return true
			case <-ctxRead.Done():
				return false
			}
		}

		if posSet != nil {
			if err := st.initPosMode(posSet.FileBase(), startPos.Name); err != nil {
				emit(reader.Event{Type: reader.EventError, Err: err})
				return
			}
		}

		for {
			select {
			case <-ctxRead.Done():
				return
			default:
			}

			ev, err := streamer.GetEvent(ctxRead)
			if err != nil {
				select {
				case <-ctxRead.Done():
					return
				default:
					select {
					case out <- reader.Event{
						Type: reader.EventError,
						Err:  err,
					}:
					case <-ctxRead.Done():
					}
					return
				}
			}

			if !r.handleEvent(st, ev, emit) {
				return
			}
		}
	}()

	return out, nil
}

// streamState là trạng thái của một luồng binlog dùng chung giữa các event (kể cả event con
// trong TRANSACTION_PAYLOAD_EVENT).
type streamState struct {
	tableMap map[uint64]*replication.TableMapEvent
	curFile  string // file binlog hiện tại (từ ROTATE, kể cả rotate giả lúc kết nối)

	ctx  context.Context       // ctx của luồng đọc, dùng cho truy vấn information_schema (nil = Background)
	info map[uint64]*TableInfo // theo TableID của TABLE_MAP hiện hành; nil = bảng không có info
	// MariaDB: chỉ số cột COMPRESSED theo TableID của TABLE_MAP hiện hành (mariadb_tablemap.go).
	compressed map[uint64][]int
	infoCache  map[string]*TableInfo // schema.table -> info đã đọc (hoặc nạp từ lịch sử bền); xóa khi gặp DDL
	hist       *schemaHistory        // lịch sử bền (nil = không lưu)
	// Chế độ vị trí binlog (xem binlogpos.go).
	posMode  bool
	fileBase string // tên gốc của file binlog; đổi giữa chừng là lỗi
	curSeq   uint32 // số thứ tự của curFile; chỉ được tăng
	// start/end của event ngoài cùng đang xử lý; event con của TRANSACTION_PAYLOAD_EVENT có LogPos = 0
	// nên dùng vị trí của event bao ngoài.
	outerStart, outerEnd uint64
	// MariaDB: vị trí đầu của event GTID standalone (nhóm DDL) chờ QUERY kế tiếp.
	groupStart    uint64
	groupFile     string
	hasGroupStart bool
}

func (st *streamState) context() context.Context {
	if st.ctx != nil {
		return st.ctx
	}
	return context.Background()
}

// isTxnControlQuery: QUERY không phải DDL (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/XA ...).
func isTxnControlQuery(q string) bool {
	f := strings.Fields(strings.ToUpper(q))
	if len(f) == 0 {
		return true
	}
	switch f[0] {
	case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "XA":
		return true
	}
	return false
}

// resolveInfo trả info cho TABLE_MAP không có tên cột. Dùng cache; lệch số cột/kiểu thì đọc lại một lần từ
// information_schema, vẫn lệch thì lỗi (fail closed, không ánh xạ cột sai). Bảng không còn tồn tại: nil.
func (r *MySQLReader) resolveInfo(st *streamState, tm *replication.TableMapEvent) (*TableInfo, error) {
	if r.cfg.TableInfo == nil || len(tm.ColumnName) > 0 {
		return nil, nil
	}
	key := string(tm.Schema) + "." + string(tm.Table)
	if info, ok := st.infoCache[key]; ok && info != nil && infoMismatch(tm, info) == "" {
		return info, nil
	}
	info, found, err := r.cfg.TableInfo.LoadTable(st.context(), string(tm.Schema), string(tm.Table))
	if err != nil {
		return nil, err
	}
	if !found {
		delete(st.infoCache, key)
		return nil, nil
	}
	if why := infoMismatch(tm, info); why != "" {
		return nil, exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
			fmt.Sprintf("table %s: source schema cannot be reconciled with the binlog (%s); the table was probably altered again "+
				"after this position (MySQL 5.7 binlog has no column names). Restart strand after the source schema is stable", key, why), nil)
	}
	if st.infoCache == nil {
		st.infoCache = map[string]*TableInfo{}
	}
	st.infoCache[key] = info
	if st.hist != nil {
		if err := st.hist.record(key, info); err != nil {
			return nil, fmt.Errorf("persist schema of %s: %w", key, err)
		}
	}
	return info, nil
}

// handleEvent xử lý một event binlog và đẩy kết quả qua emit. Trả false khi luồng phải dừng
// (emit bị huỷ do ctx, hoặc đã phát EventError). Tách khỏi vòng đọc để event con của
// TRANSACTION_PAYLOAD_EVENT đi qua cùng logic và để test bằng event tổng hợp (review N03).
func (r *MySQLReader) handleEvent(st *streamState, ev *replication.BinlogEvent, emit func(reader.Event) bool) bool {
	return r.handleEventDepth(st, ev, emit, false)
}

func (r *MySQLReader) handleEventDepth(st *streamState, ev *replication.BinlogEvent, emit func(reader.Event) bool, inPayload bool) bool {
	ts := time.Unix(int64(ev.Header.Timestamp), 0)
	// Vị trí bắt đầu của event trong file binlog hiện tại (LogPos là vị trí KẾT THÚC).
	var evStart uint64
	if ev.Header.LogPos >= ev.Header.EventSize {
		evStart = uint64(ev.Header.LogPos - ev.Header.EventSize)
	}
	evEnd := uint64(ev.Header.LogPos)
	if st.posMode {
		if inPayload {
			evStart, evEnd = st.outerStart, st.outerEnd
		} else {
			st.outerStart, st.outerEnd = evStart, evEnd
		}
	}
	// Vị trí của event: GTID khi có GTID, ngược lại (chế độ vị trí binlog) là (file, end_log_pos).
	posAt := func() model.Pos {
		if st.posMode {
			p := model.Pos{Engine: model.EngineMySQL, File: st.curFile, BinlogPos: evEnd}
			r.mu.Lock()
			r.currentPos = p
			r.mu.Unlock()
			return p
		}
		return r.Position()
	}
	startFile, startPos := "", uint64(0)
	if st.posMode {
		startFile, startPos = st.curFile, evStart
		if st.hasGroupStart && !inPayload && ev.Header.EventType != replication.MARIADB_GTID_EVENT {
			// Event đầu tiên sau GTID standalone của MariaDB: Txn bắt đầu tại event GTID (cùng file: GTID và
			// QUERY không bị ROTATE chen giữa).
			if ev.Header.EventType == replication.QUERY_EVENT && st.groupFile == st.curFile {
				startPos = st.groupStart
			}
			st.hasGroupStart = false
		}
	}

	fail := func(err error) bool {
		emit(reader.Event{Type: reader.EventError, Err: err})
		return false
	}

	rows := func(op model.Op) bool {
		rowsEv, ok := ev.Event.(*replication.RowsEvent)
		if !ok {
			return true
		}
		schema, table := "", ""
		if tm, found := st.tableMap[rowsEv.TableID]; found {
			schema, table = string(tm.Schema), string(tm.Table)
		}
		tm, info := st.tableMap[rowsEv.TableID], st.info[rowsEv.TableID]
		if comp := st.compressed[rowsEv.TableID]; len(comp) > 0 {
			if err := decompressRows(tm, rowsEv.Rows, comp); err != nil {
				return fail(err)
			}
		}
		segs := []rowSeg{{op: op, rows: rowsEv.Rows}}
		if info != nil && info.SysVersioned {
			var err error
			if segs, err = splitSystemVersioned(op, rowsEv.Rows, len(info.Columns)); err != nil {
				return fail(fmt.Errorf("%s.%s: %w", schema, table, err))
			}
		}
		for _, sg := range segs {
			if err := NormalizeRowsInfo(tm, info, sg.rows, r.cfg.CollationCharsets); err != nil {
				return fail(err)
			}
			if !emit(reader.Event{
				Type:       reader.EventRows,
				Pos:        posAt(),
				BinlogFile: startFile,
				BinlogPos:  startPos,
				TableID:    rowsEv.TableID,
				Schema:     schema,
				Table:      table,
				Op:         sg.op,
				Rows:       sg.rows,
				Timestamp:  ts,
			}) {
				return false
			}
		}
		return true
	}

	switch ev.Header.EventType {
	case replication.GTID_EVENT:
		if st.posMode {
			// ANONYMOUS_GTID_EVENT là type khác (bị bỏ qua); GTID_EVENT thật nghĩa là source đã bật GTID
			// sau khi checkpoint tạo ở chế độ vị trí: định danh Txn đổi kiểu giữa chừng, không resume được.
			return fail(exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
				"GTID event at %s:%d while the pipeline runs in binlog position mode: the source enabled GTID (gtid_mode) after the checkpoint was created; re-seed the pipeline",
				st.curFile, evEnd), nil))
		}
		if gtidEv, ok := ev.Event.(*replication.GTIDEvent); ok {
			u, err := uuid.FromBytes(gtidEv.SID)
			if err != nil {
				return true
			}
			gtid := fmt.Sprintf("%s:%d", u.String(), gtidEv.GNO)
			r.setPos(gtid)
			return emit(reader.Event{
				Type:       reader.EventGTID,
				Pos:        r.Position(),
				GTID:       gtid,
				Timestamp:  ts,
				BinlogFile: st.curFile,
				BinlogPos:  evStart,
			})
		}

	case replication.GTID_TAGGED_LOG_EVENT:
		if st.posMode {
			return fail(exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
				"GTID event at %s:%d while the pipeline runs in binlog position mode: the source enabled GTID (gtid_mode) after the checkpoint was created; re-seed the pipeline",
				st.curFile, evEnd), nil))
		}
		if taggedEv, ok := ev.Event.(*replication.GtidTaggedLogEvent); ok {
			u, err := uuid.FromBytes(taggedEv.SID)
			if err != nil {
				return true
			}
			gtid := fmt.Sprintf("%s:%s:%d", u.String(), taggedEv.Tag, taggedEv.GNO)
			r.setPos(gtid)
			return emit(reader.Event{
				Type:       reader.EventGTID,
				Pos:        r.Position(),
				GTID:       gtid,
				Timestamp:  ts,
				BinlogFile: st.curFile,
				BinlogPos:  evStart,
			})
		}

	case replication.MARIADB_GTID_EVENT:
		// MariaDB: mỗi nhóm event bắt đầu bằng GTID của MariaDB (không có QUERY "BEGIN"). Chỉ hỗ trợ chế độ vị
		// trí binlog (GTID MariaDB domain-server-seq không so được với GTID MySQL).
		if !st.posMode {
			return fail(exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
				"MariaDB GTID event at %s:%d: a MariaDB source is supported only in binlog position mode", st.curFile, evEnd), nil))
		}
		gEv, ok := ev.Event.(*replication.MariadbGTIDEvent)
		if !ok {
			return fail(fmt.Errorf("undecoded MariaDB GTID event at %s:%d", st.curFile, evEnd))
		}
		if gEv.IsStandalone() {
			// Nhóm không có COMMIT/XID kết thúc (DDL): QUERY kế tiếp là cả Txn. Điểm đọc lại an toàn của Txn
			// là đầu event GTID này (đọc lại từ QUERY cũng đúng nhưng giữ đúng ranh giới nhóm).
			st.groupStart, st.groupFile, st.hasGroupStart = evStart, st.curFile, true
			return true
		}
		// Nhóm có kết thúc (XID hoặc QUERY COMMIT/ROLLBACK): phát QUERY "BEGIN" tại đầu event GTID như binlog
		// MySQL, để assembler mở Txn với vị trí đọc lại = đầu nhóm.
		return emit(reader.Event{
			Type:       reader.EventQuery,
			Pos:        posAt(),
			BinlogFile: startFile,
			BinlogPos:  startPos,
			Query:      "BEGIN",
			Timestamp:  ts,
		})

	case replication.MARIADB_ANNOTATE_ROWS_EVENT, replication.MARIADB_BINLOG_CHECKPOINT_EVENT,
		replication.MARIADB_GTID_LIST_EVENT, replication.MARIADB_START_ENCRYPTION_EVENT:
		// Chú thích câu lệnh, mốc checkpoint binlog, danh sách GTID đầu file: không mang dữ liệu.
		return true

	case replication.MARIADB_QUERY_COMPRESSED_EVENT,
		replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1,
		replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		// log_bin_compress=ON: chưa kiểm giải nén; bỏ qua sẽ mất dữ liệu nên fail closed.
		return fail(exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
			"compressed MariaDB binlog event at %s:%d is not supported: set log_bin_compress=OFF on the source", st.curFile, evEnd), nil))

	case replication.QUERY_EVENT:
		if qEv, ok := ev.Event.(*replication.QueryEvent); ok {
			if !isTxnControlQuery(string(qEv.Query)) {
				st.infoCache = nil // DDL: cấu trúc bảng có thể đã đổi, đọc lại ở TABLE_MAP kế tiếp
				if st.hist != nil {
					// Ghi bền "version cũ kết thúc ở DDL này" TRƯỚC khi phát QUERY: checkpoint không vượt DDL được.
					mark := r.Position().GTID
					if st.posMode {
						// Chế độ vị trí: mốc = khóa kết thúc của event QUERY (SH-2). Lỗi khóa → dừng, không đoán.
						k, kerr := checkpoint.BinlogKey(st.curSeq, evEnd)
						if kerr != nil {
							return fail(kerr)
						}
						mark = checkpoint.FormatBinlogKey(k)
					}
					if err := st.hist.supersede(mark); err != nil {
						return fail(fmt.Errorf("persist schema history at DDL: %w", err))
					}
				}
			}
			return emit(reader.Event{
				Type:       reader.EventQuery,
				Pos:        posAt(),
				BinlogFile: startFile,
				BinlogPos:  startPos,
				Query:      string(qEv.Query),
				Schema:     string(qEv.Schema),
				Timestamp:  ts,
			})
		}

	case replication.TABLE_MAP_EVENT:
		if tmEv, ok := ev.Event.(*replication.TableMapEvent); ok {
			if r.cfg.Flavor == "mariadb" {
				fixed, comp, err := fixMariaDBTableMap(tmEv)
				if err != nil {
					return fail(err)
				}
				tmEv = fixed
				if st.compressed == nil {
					st.compressed = map[uint64][]int{}
				}
				st.compressed[tmEv.TableID] = comp
			}
			st.tableMap[tmEv.TableID] = tmEv
			info, err := r.resolveInfo(st, tmEv)
			if err != nil {
				return fail(err)
			}
			if st.info == nil {
				st.info = map[uint64]*TableInfo{}
			}
			st.info[tmEv.TableID] = info
			var cols, pkCols []string
			if info != nil {
				for _, c := range info.Columns {
					cols = append(cols, c.Name)
				}
				pkCols = info.PK
			} else if len(tmEv.ColumnName) > 0 {
				cols = tmEv.ColumnNameString()
				for _, idx := range tmEv.PrimaryKey {
					if int(idx) < len(cols) {
						pkCols = append(pkCols, cols[idx])
					}
				}
			}
			return emit(reader.Event{
				Type:       reader.EventTableMap,
				Pos:        posAt(),
				BinlogFile: startFile,
				BinlogPos:  startPos,
				TableID:    tmEv.TableID,
				Schema:     string(tmEv.Schema),
				Table:      string(tmEv.Table),
				Columns:    cols,
				PKColumns:  pkCols,
				Timestamp:  ts,
			})
		}

	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return rows(model.OpInsert)

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return rows(model.OpUpdate)

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return rows(model.OpDelete)

	case replication.PARTIAL_UPDATE_ROWS_EVENT:
		// binlog_row_value_options=PARTIAL_JSON: after-image chỉ chứa diff JSON, không áp được như
		// UPDATE đầy đủ. Trước đây event này rơi khỏi switch, XID vẫn tới nên txn checkpoint rỗng
		// = mất dữ liệu. Fail closed (review N03).
		schema, table := "", ""
		if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
			if tm, found := st.tableMap[rowsEv.TableID]; found {
				schema, table = string(tm.Schema), string(tm.Table)
			}
		}
		return fail(exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("PARTIAL_UPDATE_ROWS_EVENT for table %s.%s: binlog_row_value_options=PARTIAL_JSON is not supported; set binlog_row_value_options to an empty value", schema, table),
			nil,
		))

	case replication.TRANSACTION_PAYLOAD_EVENT:
		// binlog_transaction_compression=ON: cả txn (BEGIN, TABLE_MAP, ROWS, XID) nằm nén trong một
		// event; go-mysql đã giải nén thành Events. Đưa từng event con qua cùng switch, trước đây
		// bị bỏ qua nên txn checkpoint rỗng (review N03). Pos của mọi event lấy từ GTID hiện tại
		// (GTID_EVENT nằm ngoài payload), không dùng LogPos con nên LogPos=0 không ảnh hưởng checkpoint.
		tpEv, ok := ev.Event.(*replication.TransactionPayloadEvent)
		if !ok || inPayload {
			return fail(exitcode.New(
				exitcode.ErrBinlogConfigInvalid,
				exitcode.Rejected,
				"unexpected TRANSACTION_PAYLOAD_EVENT (undecoded or nested payload)",
				nil,
			))
		}
		for _, inner := range tpEv.Events {
			if !r.handleEventDepth(st, inner, emit, true) {
				return false
			}
		}
		return true

	case replication.XID_EVENT:
		if xidEv, ok := ev.Event.(*replication.XIDEvent); ok {
			return emit(reader.Event{
				Type:       reader.EventXID,
				Pos:        posAt(),
				BinlogFile: startFile,
				BinlogPos:  startPos,
				XID:        xidEv.XID,
				Timestamp:  ts,
			})
		}

	case replication.ROTATE_EVENT:
		if rotEv, ok := ev.Event.(*replication.RotateEvent); ok {
			next := string(rotEv.NextLogName)
			if st.posMode {
				if err := st.rotate(next); err != nil {
					return fail(err)
				}
				return emit(reader.Event{
					Type:      reader.EventRotate,
					Pos:       model.Pos{Engine: model.EngineMySQL, File: next, BinlogPos: rotEv.Position},
					Query:     next,
					XID:       rotEv.Position,
					Timestamp: ts,
				})
			}
			st.curFile = next
			return emit(reader.Event{
				Type:      reader.EventRotate,
				Pos:       r.Position(),
				Query:     string(rotEv.NextLogName),
				XID:       rotEv.Position,
				Timestamp: ts,
			})
		}
	}
	return true
}

// Close dừng đồng bộ và đóng kết nối.
func (r *MySQLReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	if r.cancel != nil {
		r.cancel()
	}
	if r.syncer != nil {
		r.syncer.Close()
	}
	r.mu.Unlock()

	r.wg.Wait()
	return nil
}
