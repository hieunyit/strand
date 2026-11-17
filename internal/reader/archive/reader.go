// Package archive hiện thực reader đọc binlog từ file lưu trữ theo spec/02 §3 và §9.
package archive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
	mysqlreader "strand/internal/reader/mysql"
)

// Config cấu hình cho ArchiveReader.
type Config struct {
	Dir        string
	StartPos   model.Pos
	BufferSize int
	// CollationCharsets: collation id -> charset của source (mysqlreader.LoadCollationCharsets), để chuyển
	// chuỗi không phải UTF-8 như reader trực tiếp. nil: không chuyển (BINARY(n) vẫn được đệm).
	CollationCharsets map[uint64]string

	// Sidecar provides immutable operator-supplied or trusted seed snapshot schema identities.
	Sidecar *SchemaSidecar

	// TableScope returns true if a table is within the explicit verified replication scope.
	// If nil, all tables are in scope and must have verified metadata.
	TableScope func(schema, table string) bool
}

// ArchiveReader đọc sự kiện binlog từ các file archive trên đĩa.
// spec: 02 §3, §9
type ArchiveReader struct {
	cfg             Config
	mu              sync.RWMutex
	currentPos      model.Pos
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	closed          bool
	excludedTables  map[uint64]bool
	tableIdentities map[uint64]*TableIdentity
}

// New khởi tạo ArchiveReader.
func New(cfg Config) (*ArchiveReader, error) {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	return &ArchiveReader{
		cfg:             cfg,
		currentPos:      cfg.StartPos,
		excludedTables:  make(map[uint64]bool),
		tableIdentities: make(map[uint64]*TableIdentity),
	}, nil
}

// Start bắt đầu đọc các file binlog archive và đẩy event vào channel trả về.
func (r *ArchiveReader) Start(ctx context.Context, from checkpoint.AppliedSet) (<-chan reader.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, fmt.Errorf("archive reader is closed")
	}

	// Quét danh sách file archive trong thư mục
	files, err := r.scanFiles()
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, exitcode.New(
			exitcode.ErrLogGap,
			exitcode.DataError,
			fmt.Sprintf("no archive binlog files found in %s", r.cfg.Dir),
			nil,
		)
	}

	ctxRead, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	out := make(chan reader.Event, r.cfg.BufferSize)
	r.wg.Add(1)

	go func() {
		defer r.wg.Done()
		defer close(out)

		tableMap := make(map[uint64]*replication.TableMapEvent)
		parser := replication.NewBinlogParser()
		parser.SetTimestampStringLocation(time.UTC) // như reader trực tiếp
		parser.SetFlavor("mysql")

		for _, filePath := range files {
			if ctxRead.Err() != nil {
				return
			}

			// Kiểm tra nếu file bị xóa giữa chừng
			if _, statErr := os.Stat(filePath); statErr != nil {
				select {
				case out <- reader.Event{
					Type: reader.EventError,
					Err: exitcode.New(
						exitcode.ErrLogGap,
						exitcode.DataError,
						fmt.Sprintf("archive file missing or deleted: %s", filePath),
						statErr,
					),
				}:
				case <-ctxRead.Done():
				}
				return
			}

			onEvent := func(ev *replication.BinlogEvent) error {
				return r.handleEvent(ctxRead, tableMap, ev, out)
			}

			if err := parser.ParseFile(filePath, 0, onEvent); err != nil {
				if ctxRead.Err() != nil {
					return
				}
				evErr := exitcode.New(
					exitcode.ErrLogGap,
					exitcode.DataError,
					fmt.Sprintf("error parsing archive binlog file %s: %v", filePath, err),
					err,
				)
				var coded *exitcode.Error
				if errors.As(err, &coded) {
					evErr = coded // giữ nguyên mã lỗi spec (vd binlog_config_invalid) từ handleEvent
				}
				select {
				case out <- reader.Event{Type: reader.EventError, Err: evErr}:
				case <-ctxRead.Done():
				}
				return
			}
		}
	}()

	return out, nil
}

// handleEvent chuyển một event archive thành reader.Event. Fail closed: event không giải mã được, event
// thiếu TABLE_MAP/metadata, hoặc PARTIAL_UPDATE_ROWS_EVENT đều trả lỗi (không bỏ qua để XID kế tiếp
// biến transaction thành rỗng rồi được tính là đã phủ).
func (r *ArchiveReader) handleEvent(ctxRead context.Context, tableMap map[uint64]*replication.TableMapEvent, ev *replication.BinlogEvent, out chan<- reader.Event) error {
	return r.handleEventDepth(ctxRead, tableMap, ev, out, false)
}

func (r *ArchiveReader) handleEventDepth(ctxRead context.Context, tableMap map[uint64]*replication.TableMapEvent, ev *replication.BinlogEvent, out chan<- reader.Event, inPayload bool) error {
	if ctxRead.Err() != nil {
		return ctxRead.Err()
	}
	if ev == nil || ev.Header == nil {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "archive event has no header", nil)
	}
	ts := time.Unix(int64(ev.Header.Timestamp), 0).UTC()

	switch ev.Header.EventType {
	case replication.GTID_EVENT:
		if gtidEv, ok := ev.Event.(*replication.GTIDEvent); ok && gtidEv != nil {
			u, err := uuid.FromBytes(gtidEv.SID)
			if err != nil {
				return exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "archive GTID event has an invalid SID", err)
			}
			gtid := fmt.Sprintf("%s:%d", u.String(), gtidEv.GNO)
			r.mu.Lock()
			r.currentPos = model.Pos{Engine: model.EngineMySQL, GTID: gtid}
			r.mu.Unlock()

			select {
			case out <- reader.Event{
				Type:      reader.EventGTID,
				Pos:       r.Position(),
				GTID:      gtid,
				Timestamp: ts,
			}:
			case <-ctxRead.Done():
				return ctxRead.Err()
			}
		} else {
			return undecoded(ev)
		}

	case replication.GTID_TAGGED_LOG_EVENT:
		if taggedEv, ok := ev.Event.(*replication.GtidTaggedLogEvent); ok && taggedEv != nil {
			u, err := uuid.FromBytes(taggedEv.SID)
			if err != nil {
				return exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "archive tagged GTID event has an invalid SID", err)
			}
			gtid := fmt.Sprintf("%s:%s:%d", u.String(), taggedEv.Tag, taggedEv.GNO)
			r.mu.Lock()
			r.currentPos = model.Pos{Engine: model.EngineMySQL, GTID: gtid}
			r.mu.Unlock()

			select {
			case out <- reader.Event{
				Type:      reader.EventGTID,
				Pos:       r.Position(),
				GTID:      gtid,
				Timestamp: ts,
			}:
			case <-ctxRead.Done():
				return ctxRead.Err()
			}
		} else {
			return undecoded(ev)
		}

	case replication.QUERY_EVENT:
		if qEv, ok := ev.Event.(*replication.QueryEvent); ok && qEv != nil {
			qStr := strings.TrimSpace(string(qEv.Query))
			schemaName := string(qEv.Schema)
			if matchedTbl, isMismatch := r.isDDLMismatch(schemaName, qStr); isMismatch {
				return exitcode.New(
					exitcode.ErrSchemaChangePending,
					exitcode.NeedsDecision,
					fmt.Sprintf("archive binlog encountered query %q with uncertain scope %s and immutable schema sidecar; schema change requires operator decision before transaction covered", qStr, matchedTbl),
					nil,
				)
			}
			select {
			case out <- reader.Event{
				Type:      reader.EventQuery,
				Pos:       r.Position(),
				Query:     string(qEv.Query),
				Schema:    string(qEv.Schema),
				Timestamp: ts,
			}:
			case <-ctxRead.Done():
				return ctxRead.Err()
			}
		} else {
			return undecoded(ev)
		}

	case replication.TABLE_MAP_EVENT:
		tmEv, ok := ev.Event.(*replication.TableMapEvent)
		if !ok || tmEv == nil {
			return undecoded(ev)
		}
		schemaName := string(tmEv.Schema)
		tableName := string(tmEv.Table)

		// Filter outside-tables only with explicit verified scope
		if r.cfg.TableScope != nil && !r.cfg.TableScope(schemaName, tableName) {
			r.mu.Lock()
			r.excludedTables[tmEv.TableID] = true
			r.mu.Unlock()
			tableMap[tmEv.TableID] = tmEv
			return nil
		}
		r.mu.Lock()
		r.excludedTables[tmEv.TableID] = false
		r.mu.Unlock()

		var cols, pkCols []string
		var tid *TableIdentity
		if r.cfg.Sidecar != nil {
			if ident, ok := r.cfg.Sidecar.GetTable(schemaName, tableName); ok {
				tid = ident
			}
		}

		if tid != nil {
			var err error
			cols, pkCols, err = verifyAgainstIdentity(tmEv, tid)
			if err != nil {
				return err
			}
			r.mu.Lock()
			r.tableIdentities[tmEv.TableID] = tid
			r.mu.Unlock()
		} else {
			// spec: 02 §9 (S05): archive không có information_schema/schema history theo vị trí log, nên tên
			// cột và khóa chỉ lấy từ metadata của chính TABLE_MAP (binlog_row_metadata=FULL). Thiếu thì
			// từ chối thay vì phát Columns/PKColumns rỗng để assembler đoán col_N và khóa cột đầu.
			var err error
			cols, pkCols, err = verifiedColumns(tmEv)
			if err != nil {
				return err
			}
		}
		tableMap[tmEv.TableID] = tmEv
		select {
		case out <- reader.Event{
			Type:      reader.EventTableMap,
			Pos:       r.Position(),
			TableID:   tmEv.TableID,
			Schema:    schemaName,
			Table:     tableName,
			Columns:   cols,
			PKColumns: pkCols,
			Timestamp: ts,
		}:
		case <-ctxRead.Done():
			return ctxRead.Err()
		}

	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return r.emitRows(ctxRead, tableMap, ev, ts, model.OpInsert, out)

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return r.emitRows(ctxRead, tableMap, ev, ts, model.OpUpdate, out)

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return r.emitRows(ctxRead, tableMap, ev, ts, model.OpDelete, out)

	case replication.PARTIAL_UPDATE_ROWS_EVENT:
		// spec: 02 §9 (S04): binlog_row_value_options=PARTIAL_JSON: after-image chỉ chứa diff JSON, không áp
		// được như UPDATE đầy đủ. Bỏ qua event này thì XID vẫn tới và transaction rỗng bị tính là đã phủ.
		// Như reader trực tiếp: fail closed trước khi transaction có thể commit vào spool/covered.
		schema, table := "", ""
		if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
			if tm, found := tableMap[rowsEv.TableID]; found {
				schema, table = string(tm.Schema), string(tm.Table)
			}
		}
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("archive PARTIAL_UPDATE_ROWS_EVENT for table %s.%s: binlog_row_value_options=PARTIAL_JSON is not supported; set binlog_row_value_options to an empty value", schema, table),
			nil,
		)

	case replication.TRANSACTION_PAYLOAD_EVENT:
		payload, ok := ev.Event.(*replication.TransactionPayloadEvent)
		if !ok || payload == nil || inPayload || len(payload.Events) == 0 {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "archive compressed transaction is empty, undecoded, or nested", nil)
		}
		for _, inner := range payload.Events {
			if err := r.handleEventDepth(ctxRead, tableMap, inner, out, true); err != nil {
				return err
			}
		}
		return nil

	case replication.ANONYMOUS_GTID_EVENT, replication.INCIDENT_EVENT, replication.XA_PREPARE_LOG_EVENT:
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("archive event %s is unsupported", ev.Header.EventType), nil)

	case replication.XID_EVENT:
		if xidEv, ok := ev.Event.(*replication.XIDEvent); ok && xidEv != nil {
			select {
			case out <- reader.Event{
				Type:      reader.EventXID,
				Pos:       r.Position(),
				XID:       xidEv.XID,
				Timestamp: ts,
			}:
			case <-ctxRead.Done():
				return ctxRead.Err()
			}
		} else {
			return undecoded(ev)
		}

	case replication.ROTATE_EVENT:
		select {
		case out <- reader.Event{
			Type:      reader.EventRotate,
			Pos:       r.Position(),
			Timestamp: ts,
		}:
		case <-ctxRead.Done():
			return ctxRead.Err()
		}
	}

	return nil
}

func (r *ArchiveReader) isDDLMismatch(_ string, query string) (string, bool) {
	if r.cfg.Sidecar == nil || len(r.cfg.Sidecar.Tables) == 0 {
		return "", false
	}
	qUpper := strings.ToUpper(strings.TrimSpace(query))
	// Only known transaction-control queries are safe without parsing SQL.
	// Reject unknown statements, including DDL hidden behind comments.
	switch strings.TrimSuffix(qUpper, ";") {
	case "BEGIN", "COMMIT", "ROLLBACK":
		return "", false
	}
	// Query events can use quoted, qualified identifiers, multiple table names,
	// comments, or a default schema unrelated to the modified object. Without a
	// complete SQL parser and an interval-bound schema generation, no DDL can
	// safely be proven unrelated to the sidecar.
	return "unknown scope", true
}

func undecoded(ev *replication.BinlogEvent) error {
	return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
		fmt.Sprintf("archive event %s was not decoded (%T); refusing to skip it", ev.Header.EventType, ev.Event), nil)
}

// verifyAgainstIdentity đối chiếu TABLE_MAP với TableIdentity đã xác minh từ sidecar/snapshot.
func verifyAgainstIdentity(tm *replication.TableMapEvent, tid *TableIdentity) (cols, pkCols []string, err error) {
	name := fmt.Sprintf("%s.%s", tm.Schema, tm.Table)

	// 1. Số cột phải khớp chính xác
	if tm.ColumnCount != uint64(len(tid.Columns)) {
		return nil, nil, exitcode.New(
			exitcode.ErrSchemaChangePending,
			exitcode.NeedsDecision,
			fmt.Sprintf("archive TABLE_MAP for %s has %d columns, but verified sidecar specifies %d columns (schema change pending)",
				name, tm.ColumnCount, len(tid.Columns)),
			nil,
		)
	}

	// 2. Kiểu dữ liệu cột (nếu có)
	if len(tid.ColumnTypes) > 0 && len(tm.ColumnType) > 0 {
		for i := 0; i < len(tid.ColumnTypes) && i < len(tm.ColumnType); i++ {
			if tm.ColumnType[i] != tid.ColumnTypes[i] {
				return nil, nil, exitcode.New(
					exitcode.ErrSchemaChangePending,
					exitcode.NeedsDecision,
					fmt.Sprintf("archive TABLE_MAP for %s column %d type 0x%02x != verified sidecar type 0x%02x",
						name, i, tm.ColumnType[i], tid.ColumnTypes[i]),
					nil,
				)
			}
		}
	}

	// A sidecar has no trustworthy association with a binlog interval. Without
	// names in this TABLE_MAP, matching count and type cannot detect reordered
	// columns after historical DDL.
	if len(tm.ColumnName) == 0 {
		return nil, nil, exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
			fmt.Sprintf("archive TABLE_MAP for %s lacks column names; sidecar schema generation cannot be verified", name), nil)
	}
	// 3. Verify ordered column names from the binlog.
	if len(tm.ColumnName) > 0 {
		binlogCols := tm.ColumnNameString()
		if len(binlogCols) != len(tid.Columns) {
			return nil, nil, exitcode.New(
				exitcode.ErrSchemaChangePending,
				exitcode.NeedsDecision,
				fmt.Sprintf("archive TABLE_MAP for %s names %d columns, verified sidecar names %d",
					name, len(binlogCols), len(tid.Columns)),
				nil,
			)
		}
		for i := range binlogCols {
			if binlogCols[i] != tid.Columns[i] {
				return nil, nil, exitcode.New(
					exitcode.ErrSchemaChangePending,
					exitcode.NeedsDecision,
					fmt.Sprintf("archive TABLE_MAP for %s column %d name %q != verified sidecar name %q",
						name, i, binlogCols[i], tid.Columns[i]),
					nil,
				)
			}
		}
	}
	cols = tid.Columns

	// 4. Metadata khóa chính từ binlog phải nhất quán với sidecar
	if len(tm.PrimaryKey) > 0 {
		if tid.IdentityMode != IdentityModePK {
			return nil, nil, exitcode.New(
				exitcode.ErrSchemaChangePending,
				exitcode.NeedsDecision,
				fmt.Sprintf("archive TABLE_MAP for %s has primary key in binlog metadata, but verified sidecar specifies identity_mode=%s",
					name, tid.IdentityMode),
				nil,
			)
		}
		var tmPKCols []string
		for _, idx := range tm.PrimaryKey {
			if idx >= uint64(len(cols)) {
				return nil, nil, exitcode.New(
					exitcode.ErrBinlogConfigInvalid,
					exitcode.Rejected,
					fmt.Sprintf("archive TABLE_MAP for %s has primary key index %d outside %d columns", name, idx, len(cols)),
					nil,
				)
			}
			tmPKCols = append(tmPKCols, cols[idx])
		}
		if !sameStringSlice(tmPKCols, tid.KeyColumns) {
			return nil, nil, exitcode.New(
				exitcode.ErrSchemaChangePending,
				exitcode.NeedsDecision,
				fmt.Sprintf("archive TABLE_MAP for %s primary key %v != verified sidecar key %v",
					name, tmPKCols, tid.KeyColumns),
				nil,
			)
		}
	} else if tid.IdentityMode == IdentityModePK && len(tm.ColumnName) > 0 {
		return nil, nil, exitcode.New(
			exitcode.ErrSchemaChangePending,
			exitcode.NeedsDecision,
			fmt.Sprintf("archive TABLE_MAP for %s has no primary key in binlog metadata, but sidecar requires PK", name),
			nil,
		)
	}

	// 5. Phân giải identity mode
	switch tid.IdentityMode {
	case IdentityModePK:
		pkCols = tid.KeyColumns
	case IdentityModeUnique:
		pkCols = tid.KeyColumns
	case IdentityModeKeyless:
		pkCols = nil
	default:
		return nil, nil, exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("archive TABLE_MAP for %s has unsupported identity_mode %q", name, tid.IdentityMode),
			nil,
		)
	}

	// 6. Kiểm tra fingerprint
	if tid.Fingerprint != "" {
		computed := tid.ComputeFingerprint()
		if tid.Fingerprint != computed {
			return nil, nil, exitcode.New(
				exitcode.ErrBinlogConfigInvalid,
				exitcode.Rejected,
				fmt.Sprintf("archive TABLE_MAP for %s fingerprint mismatch: expected %s, got %s",
					name, tid.Fingerprint, computed),
				nil,
			)
		}
	}

	return cols, pkCols, nil
}

// verifiedColumns trả tên cột và cột khóa chính từ metadata của TABLE_MAP, hoặc lỗi khi metadata thiếu hay
// không nhất quán (không có tên cột, số tên khác số cột, chỉ số PK ngoài phạm vi).
// Prefetch không có keyless/schema registry, nên thiếu PK phải từ chối thay vì đoán cột đầu.
func verifiedColumns(tm *replication.TableMapEvent) (cols, pkCols []string, err error) {
	name := fmt.Sprintf("%s.%s", tm.Schema, tm.Table)
	if len(tm.ColumnName) == 0 {
		return nil, nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("archive TABLE_MAP for %s has no column names (binlog_row_metadata=MINIMAL or MySQL < 8.0.1); "+
				"archive prefetch cannot verify column names or key at this log position; set binlog_row_metadata=FULL", name), nil)
	}
	cols = tm.ColumnNameString()
	if uint64(len(cols)) != tm.ColumnCount {
		return nil, nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("archive TABLE_MAP for %s has %d column names for %d columns", name, len(cols), tm.ColumnCount), nil)
	}
	if len(tm.PrimaryKey) == 0 {
		return nil, nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("archive TABLE_MAP for %s has no primary key metadata; prefetch cannot verify a row key", name), nil)
	}
	seen := make(map[string]bool, len(cols))
	for _, col := range cols {
		if col == "" || seen[col] {
			return nil, nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf("archive TABLE_MAP for %s has empty or duplicate column names", name), nil)
		}
		seen[col] = true
	}
	seenPK := make(map[uint64]bool, len(tm.PrimaryKey))
	for _, idx := range tm.PrimaryKey {
		if seenPK[idx] {
			return nil, nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf("archive TABLE_MAP for %s repeats primary key index %d", name, idx), nil)
		}
		seenPK[idx] = true
		if idx >= uint64(len(cols)) {
			return nil, nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("archive TABLE_MAP for %s has primary key index %d outside %d columns", name, idx, len(cols)), nil)
		}
		pkCols = append(pkCols, cols[idx])
	}
	return cols, pkCols, nil
}

// emitRows phát một event rows INSERT/UPDATE/DELETE. Thiếu TABLE_MAP hoặc số cột của hàng không khớp
// metadata đã xác minh thì lỗi (không phát hàng với schema/table rỗng hay tên cột đoán).
func (r *ArchiveReader) emitRows(ctxRead context.Context, tableMap map[uint64]*replication.TableMapEvent, ev *replication.BinlogEvent, ts time.Time, op model.Op, out chan<- reader.Event) error {
	rowsEv, ok := ev.Event.(*replication.RowsEvent)
	if !ok || rowsEv == nil {
		return undecoded(ev)
	}
	r.mu.RLock()
	excluded := r.excludedTables[rowsEv.TableID]
	tid := r.tableIdentities[rowsEv.TableID]
	r.mu.RUnlock()

	if excluded {
		return nil
	}

	tm, found := tableMap[rowsEv.TableID]
	if !found {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("archive rows event for table id %d has no preceding TABLE_MAP", rowsEv.TableID), nil)
	}
	name := fmt.Sprintf("%s.%s", tm.Schema, tm.Table)
	if op == model.OpUpdate && len(rowsEv.Rows)%2 != 0 {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("archive UPDATE rows event for %s has %d row images (want before/after pairs)", name, len(rowsEv.Rows)), nil)
	}
	// go-mysql allocates a full-width row even for MINIMAL/NOBLOB images;
	// omitted columns are nil and only SkippedColumns distinguishes them from SQL NULL.
	for _, skipped := range rowsEv.SkippedColumns {
		if len(skipped) != 0 {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("archive rows event for %s omits columns %v; binlog_row_image must be FULL", name, skipped), nil)
		}
	}
	expectedCols := len(tm.ColumnName)
	if tid != nil {
		expectedCols = len(tid.Columns)
	}
	for _, row := range rowsEv.Rows {
		if len(row) != expectedCols {
			return exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
				fmt.Sprintf("archive rows event for %s has %d columns but TABLE_MAP metadata names %d", name, len(row), expectedCols), nil)
		}
	}
	// Như reader trực tiếp: đệm BINARY(n), chuyển chuỗi sang UTF-8 (review F1/F8).
	if err := mysqlreader.NormalizeRows(tm, rowsEv.Rows, r.cfg.CollationCharsets); err != nil {
		return err
	}
	select {
	case out <- reader.Event{
		Type:      reader.EventRows,
		Pos:       r.Position(),
		TableID:   rowsEv.TableID,
		Schema:    string(tm.Schema),
		Table:     string(tm.Table),
		Op:        op,
		Rows:      rowsEv.Rows,
		Timestamp: ts,
	}:
		return nil
	case <-ctxRead.Done():
		return ctxRead.Err()
	}
}

// Position trả về vị trí hiện tại của reader.
func (r *ArchiveReader) Position() model.Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentPos
}

// Close dừng ArchiveReader.
func (r *ArchiveReader) Close() error {
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
	return nil
}

func (r *ArchiveReader) scanFiles() ([]string, error) {
	entries, err := os.ReadDir(r.cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read archive dir %s: %w", r.cfg.Dir, err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// Bỏ qua các file ẩn hoặc file tạm
		name := entry.Name()
		if len(name) > 0 && name[0] == '.' {
			continue
		}
		files = append(files, filepath.Join(r.cfg.Dir, name))
	}
	sort.Strings(files)
	return files, nil
}
