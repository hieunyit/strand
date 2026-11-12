// Package assemble chịu trách nhiệm lắp ráp các sự kiện thô thành Txn hoàn chỉnh theo spec/02-specs.md §3.
package assemble

import (
	"context"
	"fmt"
	"os"
	"strand/internal/txntrace"
	"strings"
	"sync"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

// Option cấu hình Assembler.
type Option func(*Assembler)

// WithMaxInMemRows đặt giới hạn số dòng giữ trong RAM trước khi tràn đĩa.
func WithMaxInMemRows(n int) Option {
	return func(a *Assembler) {
		a.maxInMem = n
	}
}

// WithSpillDir đặt thư mục lưu file tràn đĩa.
func WithSpillDir(dir string) Option {
	return func(a *Assembler) {
		a.spillDir = dir
	}
}

// UniqueIndex mô tả một unique index (ngoài PK) theo tên cột.
type UniqueIndex struct {
	Name    string
	Columns []string
	// Prefix: index có ít nhất một cột prefix (MySQL SUB_PART, ví dụ UNIQUE(code(3))). Tính unique
	// theo prefix + collation không tái tạo được từ giá trị cột, nên writeset dùng một khóa hằng cho
	// (bảng, index): mọi row chạm index này xung đột với nhau — tuần tự hóa theo bảng (review N01).
	Prefix bool
}

// prefixIndexKey là khóa hằng cho unique index có cột prefix (review N01).
var prefixIndexKey = []model.Value{"\x00prefix-index"}

// ForeignKeyMeta mô tả một khóa ngoại: các cột ở bảng con tham chiếu tới bảng cha.
type ForeignKeyMeta struct {
	Columns     []string
	ParentTable model.TableID
	// RefColumns: cột được tham chiếu ở bảng cha, cùng thứ tự với Columns.
	RefColumns []string
	// ParentIndex: unique index của bảng cha mà FK trỏ tới (rỗng = PK). Khi đã phân giải
	// (ResolveForeignKeys), Columns được sắp lại theo thứ tự cột của index/PK cha để khóa FK của
	// con hash trùng với khóa của cha (invariant: CO-1, review R11).
	ParentIndex string
}

// ResolveForeignKey phân giải FK theo PK và các unique index của bảng cha. Trả về bản đã sắp
// Columns theo thứ tự cột của khóa cha và ParentIndex tương ứng. ok=false khi không khớp khóa nào
// (giữ nguyên fk: hash theo namespace PK như trước).
func ResolveForeignKey(fk ForeignKeyMeta, parentPK []string, parentUnique []UniqueIndex) (ForeignKeyMeta, bool) {
	if len(fk.RefColumns) != len(fk.Columns) || len(fk.RefColumns) == 0 {
		return fk, false
	}
	childOf := make(map[string]string, len(fk.RefColumns))
	for i, rc := range fk.RefColumns {
		childOf[rc] = fk.Columns[i]
	}
	match := func(cols []string) ([]string, bool) {
		if len(cols) != len(childOf) {
			return nil, false
		}
		out := make([]string, len(cols))
		for i, c := range cols {
			cc, ok := childOf[c]
			if !ok {
				return nil, false
			}
			out[i] = cc
		}
		return out, true
	}
	if cols, ok := match(parentPK); ok {
		fk.Columns, fk.ParentIndex = cols, ""
		return fk, true
	}
	for _, uk := range parentUnique {
		if cols, ok := match(uk.Columns); ok {
			fk.Columns, fk.ParentIndex = cols, uk.Name
			return fk, true
		}
	}
	return fk, false
}

// KeyMeta là metadata unique key và FK của một bảng, dùng để dựng writeset (spec 02 §6).
type KeyMeta struct {
	Unique      []UniqueIndex
	ForeignKeys []ForeignKeyMeta
}

// WithKeyMetadata cung cấp metadata unique key / FK để Assembler điền RowChange.UniqueKeys
// và RowChange.ForeignKeys (gồm cả giá trị cũ khi UPDATE đổi unique key).
func WithKeyMetadata(meta map[model.TableID]KeyMeta) Option {
	return func(a *Assembler) {
		a.keyMeta = func(t model.TableID) (KeyMeta, bool) {
			m, ok := meta[t]
			return m, ok
		}
	}
}

// WithKeyMetadataFunc như WithKeyMetadata nhưng tra cứu qua hàm (metadata có thể thay đổi
// khi pipeline đang chạy, ví dụ snapshot add một bảng mới).
func WithKeyMetadataFunc(fn func(model.TableID) (KeyMeta, bool)) Option {
	return func(a *Assembler) {
		a.keyMeta = fn
	}
}

// WithSchemaRegistry đặt SchemaRegistry tùy biến.
func WithSchemaRegistry(reg *SchemaRegistry) Option {
	return func(a *Assembler) {
		a.registry = reg
	}
}

// Assembler lắp ráp các sự kiện từ reader thành các Txn hoàn chỉnh.
// spec: 02 §3, D §1
type Assembler struct {
	mu       sync.Mutex
	registry *SchemaRegistry
	maxInMem int
	spillDir string

	currentTxn   *model.Txn
	currentSpill *SpillBuffer
	inTxn        bool
	sawBegin     bool // đã thấy QUERY "BEGIN" (Txn DML); chưa thấy mà gặp QUERY khác => DDL
	keyMeta      func(model.TableID) (KeyMeta, bool)
}

// colIndex trả về vị trí cột theo tên trong schema, -1 nếu không có.
func colIndex(t *TableSchema, name string) int {
	if t == nil {
		return -1
	}
	for i, c := range t.Columns {
		if c == name {
			return i
		}
	}
	return -1
}

func pickValues(t *TableSchema, row []any, cols []string) ([]model.Value, bool) {
	out := make([]model.Value, len(cols))
	for i, c := range cols {
		idx := colIndex(t, c)
		if idx < 0 || idx >= len(row) {
			return nil, false
		}
		v := row[idx]
		if cv, ok := v.(model.ColValue); ok {
			// Reader PG gửi cột TOAST không đổi dạng ColValue{Unchanged: true}: giá trị không có mặt nên
			// không tham gia unique/FK (cột không đổi thì không tạo/xóa chiếm chỗ khóa nào).
			if cv.Unchanged {
				return nil, false
			}
			v = cv.Value
		}
		if v == nil {
			return nil, false // NULL không tham gia unique/FK
		}
		out[i] = v
	}
	return out, true
}

// enrichKeys điền unique key và FK cho row từ metadata (before có thể nil).
func (a *Assembler) enrichKeys(tid model.TableID, t *TableSchema, before, after []any, rc *model.RowChange) {
	if a.keyMeta == nil {
		return
	}
	meta, ok := a.keyMeta(tid)
	if !ok {
		return
	}
	for _, uk := range meta.Unique {
		var key, old []model.Value
		var okNew, okOld bool
		if after != nil {
			key, okNew = pickValues(t, after, uk.Columns)
		}
		if before != nil {
			old, okOld = pickValues(t, before, uk.Columns)
		}
		if uk.Prefix {
			// review N01: không hash giá trị (hai giá trị khác nhau có thể trùng prefix) — khóa hằng
			// theo (bảng, index) cho cả before- lẫn after-image.
			if okNew || okOld {
				rc.AddUniqueKey(uk.Name, prefixIndexKey)
			}
			continue
		}
		switch {
		case okNew && okOld:
			if KeysEqual(key, old) {
				rc.AddUniqueKey(uk.Name, key)
			} else {
				rc.AddUniqueKey(uk.Name, key, old)
			}
		case okNew:
			rc.AddUniqueKey(uk.Name, key)
		case okOld:
			rc.AddUniqueKey(uk.Name, old)
		}
	}
	for _, fk := range meta.ForeignKeys {
		for _, img := range [][]any{after, before} {
			if img == nil {
				continue
			}
			if v, ok := pickValues(t, img, fk.Columns); ok {
				rc.AddForeignKeyIndex(fk.ParentTable, fk.ParentIndex, v)
			}
		}
	}
}

// isRowFormatTxnQuery báo QUERY (đã ToUpper + TrimSpace) mà MySQL ghi hợp lệ bên trong một txn
// binlog_format=ROW: SAVEPOINT, ROLLBACK TO [SAVEPOINT], RELEASE SAVEPOINT, XA ..., ROLLBACK
// (txn có bảng non-transactional bị rollback). Các QUERY này không mang thay đổi hàng (review R09).
func isRowFormatTxnQuery(q string) bool {
	f := strings.Fields(q)
	if len(f) == 0 {
		return false
	}
	switch f[0] {
	case "SAVEPOINT", "XA":
		return true
	case "ROLLBACK":
		return len(f) == 1 || f[1] == "TO"
	case "RELEASE":
		return len(f) > 1 && f[1] == "SAVEPOINT"
	}
	return false
}

// isXAQuery: q (đã chữ hoa, đã trim) là câu lệnh XA ("XA START/BEGIN/END/PREPARE/COMMIT/ROLLBACK ...").
func isXAQuery(q string) bool {
	f := strings.Fields(q)
	return len(f) > 1 && f[0] == "XA"
}

// currentTxnGTID trả GTID của txn đang lắp (nếu có), không thì GTID của pos.
func (a *Assembler) currentTxnGTID(pos model.Pos) string {
	if a.currentTxn != nil {
		return a.currentTxn.SourcePos.GTID
	}
	return pos.GTID
}

// queryPrefix rút gọn câu lệnh để đưa vào thông báo lỗi (review R09).
func queryPrefix(q string) string {
	q = strings.TrimSpace(q)
	const max = 64
	if r := []rune(q); len(r) > max {
		return string(r[:max]) + "..."
	}
	return q
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// New khởi tạo Assembler mới.
func New(opts ...Option) *Assembler {
	a := &Assembler{
		registry: NewSchemaRegistry(),
		maxInMem: 10000,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Registry trả về SchemaRegistry của Assembler.
func (a *Assembler) Registry() *SchemaRegistry {
	return a.registry
}

func (a *Assembler) ensureTxnStarted(pos model.Pos) {
	if !a.inTxn {
		a.inTxn = true
		a.currentTxn = &model.Txn{
			SourcePos: pos,
		}
		a.currentSpill = NewSpillBuffer(a.maxInMem, a.spillDir)
	}
}

// Push nhận một reader.Event và trả về *model.Txn nếu transaction hoàn tất (hoặc nil nếu chưa hoàn tất).
func (a *Assembler) Push(ev reader.Event) (*model.Txn, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch ev.Type {
	case reader.EventGTID:
		if a.inTxn {
			return nil, fmt.Errorf("GTID %s arrived before transaction %s terminated: events lost",
				ev.Pos.GTID, a.currentTxn.SourcePos.GTID)
		}
		a.ensureTxnStarted(ev.Pos)
		a.currentTxn.SourcePos = ev.Pos
		a.currentTxn.BinlogFile, a.currentTxn.BinlogPos = ev.BinlogFile, ev.BinlogPos
		if !ev.Timestamp.IsZero() {
			a.currentTxn.SourceTS = ev.Timestamp.Unix()
		}

	case reader.EventQuery:
		q := strings.ToUpper(strings.TrimSpace(ev.Query))
		switch {
		case q == "BEGIN":
			a.ensureTxnStarted(ev.Pos)
			a.sawBegin = true
			// Source PG không có event GTID: thời điểm commit đi kèm BEGIN (dùng để tính lag).
			if ev.Pos.Engine == model.EnginePG && a.currentTxn.SourceTS == 0 && !ev.Timestamp.IsZero() {
				a.currentTxn.SourceTS = ev.Timestamp.Unix()
			}
			if ev.Pos.Engine == model.EnginePG && ev.XID != 0 {
				a.currentTxn.XID = ev.XID // xid top-level: snapshot PG xét visibility theo nó
			}
		case q == "COMMIT":
			if a.inTxn {
				return a.finishTxn()
			}
		case a.sawBegin && ev.Pos.Engine != model.EnginePG && !isRowFormatTxnQuery(q):
			// review R09: QUERY khác trong txn đã BEGIN = DML dạng statement (session đặt
			// binlog_format=STATEMENT/MIXED). Bỏ qua sẽ để txn rỗng được checkpoint, mất dữ liệu: fail closed.
			// (Reader PG chỉ phát QUERY "BEGIN"/"COMMIT" nên không bị ảnh hưởng.)
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("QUERY %q trong transaction đã BEGIN (txn %s): DML dạng statement không replicate được, binlog_format phải là ROW (kể cả ở mức session)",
					queryPrefix(ev.Query), a.currentTxn.SourcePos.GTID), nil)
		case !a.sawBegin && isXAQuery(q):
			// review G2: XA ghi binlog dạng GTID; QUERY "XA START xid"; TABLE_MAP/ROWS; QUERY "XA END xid";
			// XA_PREPARE_LOG_EVENT (reader không phát), rồi về sau một GTID riêng + QUERY "XA COMMIT/ROLLBACK xid".
			// Trước đây "XA START" bị coi là DDL và các ROWS sau đó được apply ngay ở "XA END" — trước khi
			// biết txn commit hay rollback. Chưa hỗ trợ ngữ nghĩa prepare/commit (kể cả qua restart): fail closed.
			// Ngoại lệ: "XA ROLLBACK" đứng riêng không mang row nào — txn đã prepare trước điểm đọc mà không
			// thấy XA START thì cũng chưa có gì được apply; phát Txn rỗng để GTID được ghi nhận.
			if f := strings.Fields(q); len(f) > 1 && f[1] == "ROLLBACK" {
				a.ensureTxnStarted(ev.Pos)
				return a.finishTxn()
			}
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("QUERY %q (txn %s): XA transactions are not supported (strand cannot replicate prepared/two-phase "+
					"transactions on the source, including on tables outside the pipeline); stop using XA on the source",
					queryPrefix(ev.Query), a.currentTxnGTID(ev.Pos)), nil)
		case !a.sawBegin:
			// Txn DDL: GTID + QUERY, không có BEGIN/XID. Phát thành Txn riêng để GTID được ghi nhận.
			a.ensureTxnStarted(ev.Pos)
			a.currentTxn.DDL = ev.Query
			a.currentTxn.DDLSchema = ev.Schema
			return a.finishTxn()
		}

	case reader.EventTableMap:
		a.ensureTxnStarted(ev.Pos)
		// Schema lấy từ metadata của TABLE_MAP (binlog_row_metadata=FULL) khi có.
		if len(ev.Columns) > 0 {
			// Bảng không có PK trong event (MySQL 5.7 hoặc bảng không PK) và cột không đổi: giữ PK đã đăng ký
			// (pipeline chọn unique key thay PK).
			if cur := a.registry.GetTable(ev.Schema, ev.Table); len(ev.PKColumns) > 0 || cur == nil || !sameStrings(cur.Columns, ev.Columns) {
				a.registry.RegisterTable(ev.Schema, ev.Table, ev.PKColumns, ev.Columns)
			}
		} else if a.registry.GetTable(ev.Schema, ev.Table) == nil {
			a.registry.RegisterTable(ev.Schema, ev.Table, nil, nil)
		}

	case reader.EventRows:
		a.ensureTxnStarted(ev.Pos)
		tableID := model.TableID{Schema: ev.Schema, Table: ev.Table}
		tbl := a.registry.GetTable(ev.Schema, ev.Table)
		if tbl == nil {
			a.registry.RegisterTable(ev.Schema, ev.Table, nil, nil)
			tbl = a.registry.GetTable(ev.Schema, ev.Table)
		}

		// Registry (nạp từ information_schema khi TABLE_MAP không có tên cột, vd MySQL 5.7) phải cùng số cột với
		// row của binlog; lệch = bảng đã ALTER mà registry chưa nạp lại: fail closed thay vì gán sai tên cột.
		if n := len(tbl.Columns); n > 0 && ev.Pos.Engine == model.EngineMySQL {
			for _, row := range ev.Rows {
				if len(row) != n {
					return nil, exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
						fmt.Sprintf("table %s.%s: binlog row has %d columns but the registered schema has %d (table altered while streaming without column metadata); restart strand",
							ev.Schema, ev.Table, len(row), n), nil)
				}
			}
		}

		switch ev.Op {
		case model.OpInsert:
			for _, row := range ev.Rows {
				key := tbl.ExtractPK(row)
				cols := tbl.ExtractCols(row)
				rc := model.RowChange{
					Table: tableID,
					Op:    model.OpInsert,
					Key:   key,
					Cols:  cols,
				}
				a.enrichKeys(tableID, tbl, nil, row, &rc)
				if err := a.currentSpill.Append(rc); err != nil {
					return nil, err
				}
			}

		case model.OpDelete:
			for _, row := range ev.Rows {
				key := tbl.ExtractPK(row)
				cols := tbl.ExtractCols(row)
				rc := model.RowChange{
					Table: tableID,
					Op:    model.OpDelete,
					Key:   key,
					Cols:  cols,
				}
				a.enrichKeys(tableID, tbl, row, nil, &rc)
				if err := a.currentSpill.Append(rc); err != nil {
					return nil, err
				}
			}

		case model.OpUpdate:
			// Các hàng update trong binlog đi theo cặp: [Before, After, Before, After, ...]
			for i := 0; i < len(ev.Rows); i += 2 {
				before := ev.Rows[i]
				var after []any
				if i+1 < len(ev.Rows) {
					after = ev.Rows[i+1]
				} else {
					after = before
				}

				oldPK := tbl.ExtractPK(before)
				newPK := tbl.ExtractPK(after)

				var oldKey []model.Value
				if !KeysEqual(oldPK, newPK) {
					// spec: 02 §3 OldKey khác nil khi UPDATE đổi PK
					oldKey = oldPK
				}

				cols := tbl.ExtractCols(after)
				rc := model.RowChange{
					Table:  tableID,
					Op:     model.OpUpdate,
					Key:    newPK,
					OldKey: oldKey,
					Cols:   cols,
				}
				a.enrichKeys(tableID, tbl, before, after, &rc)
				if err := a.currentSpill.Append(rc); err != nil {
					return nil, err
				}
			}
		}

	case reader.EventXID:
		if a.inTxn {
			return a.finishTxn()
		}
	}

	return nil, nil
}

func (a *Assembler) finishTxn() (*model.Txn, error) {
	if err := a.currentSpill.Finish(a.currentTxn); err != nil {
		return nil, err
	}
	res := a.currentTxn
	txntrace.Put(9, res.SourcePos.LSN, res.XID, 0, res.TotalRows(), int64(len(res.Rows)))

	a.inTxn = false
	a.sawBegin = false
	a.currentTxn = nil
	a.currentSpill = nil

	return res, nil
}

// Process nhận stream reader.Event qua channel và xuất ra stream *model.Txn qua channel.
func (a *Assembler) Process(ctx context.Context, in <-chan reader.Event) (<-chan *model.Txn, <-chan error) {
	out := make(chan *model.Txn, 128)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)
		defer func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.currentSpill != nil {
				a.currentSpill.discard()
			}
			a.currentSpill = nil
			a.currentTxn = nil
			a.inTxn = false
			a.sawBegin = false
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-in:
				if !ok {
					return
				}
				if ev.Type == reader.EventError {
					select {
					case errCh <- ev.Err:
					case <-ctx.Done():
					}
					return
				}

				txn, err := a.Push(ev)
				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
					}
					return
				}

				if txn != nil {
					select {
					case out <- txn:
					case <-ctx.Done():
						if txn.SpillOwned && txn.SpillPath != "" {
							_ = os.Remove(txn.SpillPath)
						}
						return
					}
				}
			}
		}
	}()

	return out, errCh
}
