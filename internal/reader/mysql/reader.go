package mysql

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

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

	gset, err := ToMysqlGTIDSet(from)
	if err != nil {
		return nil, err
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

	syncer := replication.NewBinlogSyncer(syncerCfg)
	streamer, err := syncer.StartSyncGTID(gset)
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

		tableMap := make(map[uint64]*replication.TableMapEvent)
		curFile := "" // file binlog hiện tại (từ ROTATE, kể cả rotate giả lúc kết nối)

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

			ts := time.Unix(int64(ev.Header.Timestamp), 0)
			// Vị trí bắt đầu của event trong file binlog hiện tại (LogPos là vị trí KẾT THÚC).
			var evStart uint64
			if ev.Header.LogPos >= ev.Header.EventSize {
				evStart = uint64(ev.Header.LogPos - ev.Header.EventSize)
			}

			switch ev.Header.EventType {
			case replication.GTID_EVENT:
				if gtidEv, ok := ev.Event.(*replication.GTIDEvent); ok {
					u, err := uuid.FromBytes(gtidEv.SID)
					if err != nil {
						continue
					}
					gtid := fmt.Sprintf("%s:%d", u.String(), gtidEv.GNO)
					r.setPos(gtid)
					select {
					case out <- reader.Event{
						Type:       reader.EventGTID,
						Pos:        r.Position(),
						GTID:       gtid,
						Timestamp:  ts,
						BinlogFile: curFile,
						BinlogPos:  evStart,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.GTID_TAGGED_LOG_EVENT:
				if taggedEv, ok := ev.Event.(*replication.GtidTaggedLogEvent); ok {
					u, err := uuid.FromBytes(taggedEv.SID)
					if err != nil {
						continue
					}
					gtid := fmt.Sprintf("%s:%s:%d", u.String(), taggedEv.Tag, taggedEv.GNO)
					r.setPos(gtid)
					select {
					case out <- reader.Event{
						Type:       reader.EventGTID,
						Pos:        r.Position(),
						GTID:       gtid,
						Timestamp:  ts,
						BinlogFile: curFile,
						BinlogPos:  evStart,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.QUERY_EVENT:
				if qEv, ok := ev.Event.(*replication.QueryEvent); ok {
					select {
					case out <- reader.Event{
						Type:      reader.EventQuery,
						Pos:       r.Position(),
						Query:     string(qEv.Query),
						Schema:    string(qEv.Schema),
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.TABLE_MAP_EVENT:
				if tmEv, ok := ev.Event.(*replication.TableMapEvent); ok {
					tableMap[tmEv.TableID] = tmEv
					var cols, pkCols []string
					if len(tmEv.ColumnName) > 0 {
						cols = tmEv.ColumnNameString()
						for _, idx := range tmEv.PrimaryKey {
							if int(idx) < len(cols) {
								pkCols = append(pkCols, cols[idx])
							}
						}
					}
					select {
					case out <- reader.Event{
						Type:      reader.EventTableMap,
						Pos:       r.Position(),
						TableID:   tmEv.TableID,
						Schema:    string(tmEv.Schema),
						Table:     string(tmEv.Table),
						Columns:   cols,
						PKColumns: pkCols,
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
					schema, table := "", ""
					if tm, found := tableMap[rowsEv.TableID]; found {
						schema, table = string(tm.Schema), string(tm.Table)
					}
					if err := convertCharsets(tableMap[rowsEv.TableID], rowsEv.Rows, r.cfg.CollationCharsets); err != nil {
						select {
						case out <- reader.Event{Type: reader.EventError, Err: err}:
						case <-ctxRead.Done():
						}
						return
					}
					select {
					case out <- reader.Event{
						Type:      reader.EventRows,
						Pos:       r.Position(),
						TableID:   rowsEv.TableID,
						Schema:    schema,
						Table:     table,
						Op:        model.OpInsert,
						Rows:      rowsEv.Rows,
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
					schema, table := "", ""
					if tm, found := tableMap[rowsEv.TableID]; found {
						schema, table = string(tm.Schema), string(tm.Table)
					}
					if err := convertCharsets(tableMap[rowsEv.TableID], rowsEv.Rows, r.cfg.CollationCharsets); err != nil {
						select {
						case out <- reader.Event{Type: reader.EventError, Err: err}:
						case <-ctxRead.Done():
						}
						return
					}
					select {
					case out <- reader.Event{
						Type:      reader.EventRows,
						Pos:       r.Position(),
						TableID:   rowsEv.TableID,
						Schema:    schema,
						Table:     table,
						Op:        model.OpUpdate,
						Rows:      rowsEv.Rows,
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
					schema, table := "", ""
					if tm, found := tableMap[rowsEv.TableID]; found {
						schema, table = string(tm.Schema), string(tm.Table)
					}
					if err := convertCharsets(tableMap[rowsEv.TableID], rowsEv.Rows, r.cfg.CollationCharsets); err != nil {
						select {
						case out <- reader.Event{Type: reader.EventError, Err: err}:
						case <-ctxRead.Done():
						}
						return
					}
					select {
					case out <- reader.Event{
						Type:      reader.EventRows,
						Pos:       r.Position(),
						TableID:   rowsEv.TableID,
						Schema:    schema,
						Table:     table,
						Op:        model.OpDelete,
						Rows:      rowsEv.Rows,
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.XID_EVENT:
				if xidEv, ok := ev.Event.(*replication.XIDEvent); ok {
					select {
					case out <- reader.Event{
						Type:      reader.EventXID,
						Pos:       r.Position(),
						XID:       xidEv.XID,
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return
					}
				}

			case replication.ROTATE_EVENT:
				if rotEv, ok := ev.Event.(*replication.RotateEvent); ok {
					curFile = string(rotEv.NextLogName)
					select {
					case out <- reader.Event{
						Type:      reader.EventRotate,
						Pos:       r.Position(),
						Query:     string(rotEv.NextLogName),
						XID:       rotEv.Position,
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return
					}
				}
			}
		}
	}()

	return out, nil
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
