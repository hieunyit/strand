package snapshot

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// spec: 02 §5, design §6.1
// invariant: RS-1, RS-2

// WaitExecutedGTIDTimeoutSeconds là timeout (giây) của WAIT_FOR_EXECUTED_GTID_SET trước SELECT
// của chunk (02 §2 RS-2, §5 Watermark).
const WaitExecutedGTIDTimeoutSeconds = 30

// waitGTIDMaxAttempts giới hạn số lần chờ lại khi WAIT_FOR_EXECUTED_GTID_SET hết giờ (replica trễ).
const waitGTIDMaxAttempts = 10

// ChunkConn là một kết nối đọc chunk. WAIT_FOR_EXECUTED_GTID_SET và SELECT của chunk phải
// chạy trên CÙNG kết nối (cùng server) thì kết quả chờ mới có nghĩa với read view của SELECT (RS-2).
type ChunkConn interface {
	WaitForExecutedGTIDSet(ctx context.Context, gtidSet string, timeoutSeconds int) (sql.NullInt64, error)
	// QueryChunk: SELECT chunk theo PK; maxBytes > 0 thì dừng ngay sau row làm tổng byte đạt maxBytes
	// (xem QueryChunkFunc).
	QueryChunk(ctx context.Context, query string, args []any, columnNames []string, pkIdxs []int, table model.TableID, maxBytes int64) ([]*model.RowChange, int64, error)
	Close() error
}

// SourceQuerier là seam truy vấn watermark và mở kết nối đọc chunk trên source/replica.
type SourceQuerier interface {
	GetGlobalGTIDExecuted(ctx context.Context) (string, error)
	OpenChunkConn(ctx context.Context) (ChunkConn, error)
}

// WaitForExecutedGTIDSet chạy WAIT_FOR_EXECUTED_GTID_SET(low, timeout) trên conn (RS-2):
//   - 0: mọi GTID trong low đã visible ở server này -> nil.
//   - 1: hết giờ -> lỗi bọc MySQL 1205 (lock wait timeout), để đường retry 1213/1205 xử lý.
//   - NULL hoặc giá trị khác: lỗi.
//
// low rỗng thì không có gì phải chờ.
func WaitForExecutedGTIDSet(ctx context.Context, conn ChunkConn, gtidSet string, timeoutSeconds int) error {
	if strings.TrimSpace(gtidSet) == "" {
		return nil
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = WaitExecutedGTIDTimeoutSeconds
	}
	res, err := conn.WaitForExecutedGTIDSet(ctx, gtidSet, timeoutSeconds)
	if err != nil {
		return fmt.Errorf("wait for executed gtid set: %w", err)
	}
	if !res.Valid {
		return fmt.Errorf("WAIT_FOR_EXECUTED_GTID_SET(%q, %d) returned NULL", gtidSet, timeoutSeconds)
	}
	switch res.Int64 {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("WAIT_FOR_EXECUTED_GTID_SET(%q, %d) timed out: %w", gtidSet, timeoutSeconds,
			&drivermysql.MySQLError{Number: 1205, Message: "WAIT_FOR_EXECUTED_GTID_SET timed out"})
	default:
		return fmt.Errorf("WAIT_FOR_EXECUTED_GTID_SET(%q, %d) returned unexpected result %d", gtidSet, timeoutSeconds, res.Int64)
	}
}

// isRetryableMySQLError: lỗi khóa của InnoDB (1213 deadlock, 1205 lock wait timeout).
func isRetryableMySQLError(err error) bool {
	var me *drivermysql.MySQLError
	return errors.As(err, &me) && (me.Number == 1213 || me.Number == 1205)
}

// sqlSourceQuerier là SourceQuerier mặc định trên *sql.DB của source (hoặc replica).
type sqlSourceQuerier struct {
	db *sql.DB
}

func (q *sqlSourceQuerier) GetGlobalGTIDExecuted(ctx context.Context) (string, error) {
	var gtid string
	if err := q.db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&gtid); err != nil {
		return "", err
	}
	return gtid, nil
}

func (q *sqlSourceQuerier) OpenChunkConn(ctx context.Context) (ChunkConn, error) {
	conn, err := q.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &sqlChunkConn{conn: conn}, nil
}

type sqlChunkConn struct {
	conn *sql.Conn
}

func (c *sqlChunkConn) WaitForExecutedGTIDSet(ctx context.Context, gtidSet string, timeoutSeconds int) (sql.NullInt64, error) {
	var res sql.NullInt64
	err := c.conn.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, ?)", gtidSet, timeoutSeconds).Scan(&res)
	return res, err
}

func (c *sqlChunkConn) QueryChunk(ctx context.Context, query string, args []any, columnNames []string, pkIdxs []int, table model.TableID, maxBytes int64) ([]*model.RowChange, int64, error) {
	// Hủy query khi dừng sớm vì đủ ngân sách: đóng rows mà không hủy thì driver vẫn đọc (và bỏ) phần
	// còn lại của kết quả. Kết nối bị hủy không dùng lại (kết nối đọc chunk mở riêng cho mỗi lần đọc).
	qctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rows, err := c.conn.QueryContext(qctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var result []*model.RowChange
	var totalBytes int64

	for rows.Next() {
		// Quét các cột
		colVals := make([]any, len(columnNames))
		colValPtrs := make([]any, len(columnNames))
		for i := range colVals {
			colValPtrs[i] = &colVals[i]
		}
		if err := rows.Scan(colValPtrs...); err != nil {
			return nil, 0, err
		}

		// Tạo row change
		key := make([]model.Value, len(pkIdxs)) // mọi cột PK, theo thứ tự PK (việc C)
		for i, idx := range pkIdxs {
			key[i] = colVals[idx]
		}
		rc := &model.RowChange{
			Table: table,
			Op:    model.OpInsert,
			Key:   key,
		}
		for i, colName := range columnNames {
			var rawBytes []byte
			if b, ok := colVals[i].([]byte); ok {
				rawBytes = b
				rc.Cols = append(rc.Cols, model.ColValue{
					Name:  colName,
					Value: string(b),
				})
			} else {
				rc.Cols = append(rc.Cols, model.ColValue{
					Name:  colName,
					Value: colVals[i],
				})
				rawBytes = []byte(fmt.Sprintf("%v", colVals[i]))
			}
			totalBytes += int64(len(rawBytes))
		}
		result = append(result, rc)
		if maxBytes > 0 && totalBytes >= maxBytes {
			cancel()
			return result, totalBytes, nil // dừng sớm: phủ trọn [startPK, PK của row cuối]
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return result, totalBytes, nil
}

func (c *sqlChunkConn) Close() error {
	return c.conn.Close()
}

// readChunkWatermarked đọc một chunk theo đúng thứ tự RS-2 (02 §5): low = gtid_executed →
// WAIT_FOR_EXECUTED_GTID_SET(low) → SELECT → high. Chờ và SELECT chạy trên cùng một kết nối;
// mỗi lần SELECT (kể cả sub-chunk sau khi chia đôi vì timeout) mở kết nối mới và chờ lại,
// vì kết nối bị hủy giữa chừng không dùng lại được. Chờ hết giờ (1205) thì chờ lại có giới hạn.
func (s *MySQLSnapshotter) readChunkWatermarked(
	ctx context.Context,
	src SourceQuerier,
	chunk *Chunk,
	kq keySQL,
	colsStr string,
	columnNames []string,
	pkIdxs []int,
	table model.TableID,
) (ChunkRead, checkpoint.AppliedSet, error) {
	var discarded *checkpoint.MySQLAppliedSet
	var waitStr string
	if chunk.Continuation {
		// Phần đuôi tách ra khi chạm max_chunk_bytes (SplitReading): giữ low của chunk gốc; tập event đã
		// bỏ đã được xác nhận visible ở lần đọc đầu, nên chỉ cần chờ low.
		waitStr = chunk.LowWatermark.(fmt.Stringer).String()
	} else {
		// Event bị bỏ khi range còn "Chưa đọc" có thể chưa commit ở engine (2PC): chuyển chunk sang
		// "Đang đọc" (buffer mọi event mới) rồi chờ các GTID đó visible trên kết nối đọc chunk.
		discarded = s.Manager.BeginReading(chunk)
		chunkLowStr, err := src.GetGlobalGTIDExecuted(ctx)
		if err != nil {
			return ChunkRead{}, nil, fmt.Errorf("failed to read chunk low watermark: %w", err)
		}
		chunkLow := ParseMySQLGTIDSet(chunkLowStr)
		s.Manager.StartReading(chunk, chunkLow)
		waitSet := chunkLow.Clone().(*checkpoint.MySQLAppliedSet)
		waitSet.Merge(discarded)
		waitStr = waitSet.String()
	}

	queryFn := func(qCtx context.Context, c *Chunk, maxBytes int64) ([]*model.RowChange, int64, error) {
		conn, err := src.OpenChunkConn(ctx)
		if err != nil {
			return nil, 0, err
		}
		defer conn.Close()
		// Chờ dùng ctx cha: timeout của SELECT (5 s) không áp cho bước chờ replica.
		for attempt := 1; ; attempt++ {
			err = WaitForExecutedGTIDSet(ctx, conn, waitStr, WaitExecutedGTIDTimeoutSeconds)
			if err == nil {
				break
			}
			if !isRetryableMySQLError(err) || attempt >= waitGTIDMaxAttempts || ctx.Err() != nil {
				return nil, 0, err
			}
			time.Sleep(time.Duration(attempt*attempt) * 5 * time.Millisecond)
		}
		// Watermark (RS-2): SELECT chunk theo tuple PK, cùng thứ tự với range-state (pkey.go, keysql.go).
		cond, args, err := kq.rangeCond(c.Lo, c.Hi)
		if err != nil {
			return nil, 0, err
		}
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s", colsStr, quoteTable(table), cond, kq.orderBy(false))
		return conn.QueryChunk(qCtx, query, args, columnNames, pkIdxs, table, maxBytes)
	}

	// Đọc chunk có cơ chế adaptive split, timeout và trần byte (spec 02 §5)
	read, err := s.Chunker.ReadChunkAdaptive(ctx, chunk, s.opts, queryFn)
	if err != nil {
		return ChunkRead{}, nil, fmt.Errorf("adaptive read failed for chunk [%d, %d]: %w", chunk.StartPK, chunk.EndPK, err)
	}

	if discarded != nil {
		s.Manager.ConfirmVisible(chunk.Table, discarded)
	}

	chunkHighStr, err := src.GetGlobalGTIDExecuted(ctx)
	if err != nil {
		return ChunkRead{}, nil, fmt.Errorf("failed to read chunk high watermark: %w", err)
	}
	return read, ParseMySQLGTIDSet(chunkHighStr), nil
}

// MySQLSnapshotter điều phối quá trình snapshot chunking và range-state cho MySQL source sang MySQL target.
type MySQLSnapshotter struct {
	Manager *RangeStateManager
	Chunker *Chunker
	opts    ChunkOptions

	// Source (tùy chọn) thay srcDB cho bước đọc watermark và SELECT của chunk (seam kiểm thử RS-2).
	// Khi nil, dùng srcDB; WAIT_FOR_EXECUTED_GTID_SET luôn chạy cùng kết nối với SELECT.
	Source SourceQuerier

	// Barrier (tùy chọn) bao quanh bước đọc biên + đăng ký range + dọn target. Pipeline
	// truyền vào hàm dừng router và chờ mọi Txn đã nhận được apply xong, để không Txn cũ
	// nào của bảng còn nằm trong hàng đợi khi range-state bắt đầu có hiệu lực.
	Barrier func(ctx context.Context, fn func() error) error

	// TargetName (tùy chọn) ánh xạ tên bảng nguồn sang tên bảng đích (tables.rename).
	TargetName func(model.TableID) model.TableID

	// LoadPlan / SavePlan (tùy chọn) lưu tiến độ snapshot của bảng, để restart làm tiếp
	// thay vì đọc lại từ đầu. Plan gồm biên [Min, Max], cỡ chunk và khóa bắt đầu của
	// chunk chưa flush kế tiếp (chunk được đọc tuần tự theo PK nên một con trỏ là đủ).
	LoadPlan func(table model.TableID) (SnapshotPlan, bool)
	SavePlan func(table model.TableID, plan SnapshotPlan) error

	// Pace (tùy chọn) được gọi giữa các chunk; governor dùng để giãn khoảng cách chunk. chunkBytes:
	// số byte row thật của chunk vừa đọc (chi phí đọc dùng để giãn nhịp; ≤ 0 nếu không biết).
	Pace func(ctx context.Context, chunkBytes int64) error

	// FlushRows / ClearTarget (tùy chọn) thay phần ghi target khi target không phải MySQL
	// (MySQL→PG). FlushRows phải thay nguyên range [startPK, endPK] của bảng đích bằng rows trong
	// MỘT transaction (RS-1) và tự retry lỗi tạm thời; ClearTarget xóa mọi row của bảng đích.
	// table là tên bảng ĐÍCH (đã qua TargetName). Khi nil: ghi MySQL như cũ.
	FlushRows   func(ctx context.Context, table model.TableID, pkCol string, columnNames []string, rows []*model.RowChange, startPK, endPK int64) error
	ClearTarget func(ctx context.Context, table model.TableID) error
	// ClearUnread (tùy chọn, target không phải MySQL) xóa ở bảng đích các row PK trong [lo, hi] (khi
	// hasRange) và các khóa keys — phần CHƯA ĐỌC của plan dở khi chạy tiếp. Khi nil: DELETE trên MySQL
	// với FOREIGN_KEY_CHECKS=0 (không cascade sang bảng con).
	ClearUnread func(ctx context.Context, table model.TableID, pkCol string, lo, hi int64, hasRange bool, keys []int64) error
	// FlushKeys (tùy chọn) như FlushRows cho chunk re-read (Chunk.Keys): thay đúng tập khóa keys của bảng
	// đích bằng rows trong MỘT transaction. Bắt buộc khi source có thể sinh chunk re-read (PG, 02 §8).
	FlushKeys func(ctx context.Context, table model.TableID, pkCol string, columnNames []string, rows []*model.RowChange, keys []int64) error

	// planMu tuần tự hóa đọc-sửa-ghi plan giữa router (lưu khóa re-read) và vòng snapshot (NextPK).
	planMu sync.Mutex
	// RereadKeysTotal: tổng số khóa đã được đọc lại (chunk re-read đã flush, 02 §8), để ghi log.
	RereadKeysTotal atomic.Int64

	// FlushRange / FlushKeySet / ClearUnreadRange (việc C): như FlushRows / FlushKeys / ClearUnread cho
	// bảng có PK tổng quát, với cận khóa thay vì int64. Bắt buộc khi target không phải MySQL.
	FlushRange       func(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange, lo, hi Bound) error
	FlushKeySet      func(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange, keys []Bound) error
	ClearUnreadRange func(ctx context.Context, table model.TableID, lo, hi Bound, hasRange bool, keys []Bound) error
	// ExactKeys / FlushExact (Gate 2, cross_order.go): ExactKeys(table nguồn) = true khi target sắp PK
	// khác thứ tự source (CrossEngineKeySet) — chunk chính khi đó được flush bằng FlushExact: upsert rows và
	// DELETE đúng các khóa deletes (tombstone, giá trị chính xác) trong MỘT transaction, không xóa range.
	// ClearUnreadRange của bảng như vậy phải tự xóa theo khóa source (không theo range ở target).
	ExactKeys  func(table model.TableID) bool
	FlushExact func(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange, deletes []Bound) error

	// OnTruncatedRead (tùy chọn) được gọi mỗi lần đọc chunk dừng ở max_chunk_bytes (metric / test).
	OnTruncatedRead func()

	// Chunks (tùy chọn) thay phần đọc source khi source không phải MySQL (PG: PGChunkSource).
	// Khi nil: đọc MySQL như cũ (srcDB, Source).
	Chunks ChunkSource
}

// ChunkSource là phần phụ thuộc engine của SOURCE khi snapshot một bảng: biên PK, keyset walk và đọc
// một chunk kèm watermark (02 §5). Range-state, barrier, rendezvous, flush và plan là phần chung.
type ChunkSource interface {
	// BeforeRegister chạy trong barrier (router đã dừng, mọi Txn đã nhận đã commit ở target), trước khi
	// đọc biên và đăng ký range của bảng.
	BeforeRegister(ctx context.Context) error
	Bounds(ctx context.Context, table model.TableID, pkCol string) (*TableBounds, error)
	KeysetEnd(ctx context.Context, table model.TableID, pkCol string, start, max, rows int64) (int64, error)
	// ReadChunk chuyển chunk qua "Đang đọc" (low), đọc rows của chunk và trả high watermark: khi
	// stream đã xử lý hết high thì mọi Txn mà rows phản ánh đều đã đi qua range-state.
	// Chunk.Continuation: phần đuôi của SplitReading — đã "Đang đọc" với low của chunk gốc, không lấy low
	// mới. Kết quả có thể bị cắt theo max_chunk_bytes (ChunkRead.Truncated).
	ReadChunk(ctx context.Context, s *MySQLSnapshotter, chunk *Chunk, columnNames []string, pkIdxs []int, table model.TableID) (ChunkRead, checkpoint.AppliedSet, error)
}

// NewMySQLSnapshotter tạo mới MySQLSnapshotter.
func NewMySQLSnapshotter(opts ChunkOptions) *MySQLSnapshotter {
	return &MySQLSnapshotter{
		Manager: NewRangeStateManager(),
		Chunker: NewChunker(),
		opts:    opts,
	}
}

// SnapshotPlan là tiến độ snapshot của một bảng.
type SnapshotPlan struct {
	Empty     bool  `json:"empty"`
	MinPK     int64 `json:"min_pk"`
	MaxPK     int64 `json:"max_pk"`
	ChunkRows int64 `json:"chunk_rows"`
	NextPK    int64 `json:"next_pk"` // mọi chunk có EndPK < NextPK đã flush
	// Finished: đã flush tới MaxPK. Cần cờ riêng vì khi MaxPK == MaxInt64 thì NextPK = MaxPK+1
	// không biểu diễn được (tràn thành MinInt64 → resume đọc lại cả bảng).
	Finished bool `json:"finished,omitempty"`
	// Reread: khóa phải đọc lại (UPDATE đổi PK mà cột TOAST không đổi, 02 §8; xem toast_pk.go). Được lưu
	// bền trước khi Txn sinh ra nó được Admit, gỡ sau khi chunk re-read của khóa đã flush.
	Reread []int64 `json:"reread_keys,omitempty"`

	// Khóa tổng quát (việc C; PK không phải một cột số nguyên): các trường int64 ở trên không dùng.
	// MinKey/MaxKey: biên bảng; LastKey: khóa cuối đã flush (nil = chưa flush gì); RereadKeyVals: khóa
	// chờ đọc lại.
	GenericKey    bool       `json:"generic_key,omitempty"`
	MinKey        []KeyVal   `json:"min_key,omitempty"`
	MaxKey        []KeyVal   `json:"max_key,omitempty"`
	LastKey       []KeyVal   `json:"last_key,omitempty"`
	RereadKeyVals [][]KeyVal `json:"reread_key_vals,omitempty"`
}

// KeyVal là giá trị một cột PK lưu bền trong plan (giữ kiểu để bind lại đúng).
type KeyVal struct {
	I *int64  `json:"i,omitempty"`
	U *uint64 `json:"u,omitempty"`
	S *string `json:"s,omitempty"`
	B []byte  `json:"b,omitempty"`
	// Bin: B là chuỗi byte (kể cả rỗng) — phân biệt với "không có giá trị".
	Bin bool `json:"bin,omitempty"`
}

func toKeyVals(vals []model.Value) []KeyVal {
	out := make([]KeyVal, len(vals))
	for i, v := range vals {
		switch x := v.(type) {
		case int64:
			out[i].I = &x
		case uint64:
			out[i].U = &x
		case []byte:
			out[i].B, out[i].Bin = append([]byte{}, x...), true
		default:
			str, _ := toText(x)
			out[i].S = &str
		}
	}
	return out
}

func fromKeyVals(kv []KeyVal) []model.Value {
	out := make([]model.Value, len(kv))
	for i, v := range kv {
		switch {
		case v.I != nil:
			out[i] = *v.I
		case v.U != nil:
			out[i] = *v.U
		case v.Bin:
			out[i] = append([]byte{}, v.B...)
		case v.S != nil:
			out[i] = *v.S
		}
	}
	return out
}

// Pending trả về true nếu key thuộc phần chưa flush [NextPK, MaxPK] của plan.
func (p SnapshotPlan) Pending(key int64) bool {
	return !p.Empty && !p.Finished && key >= p.NextPK && key <= p.MaxPK
}

// FlushedEnd trả khóa cuối đã flush (ok=false nếu chưa flush gì).
func (p SnapshotPlan) FlushedEnd() (int64, bool) {
	if p.Empty {
		return 0, false
	}
	if p.Finished {
		return p.MaxPK, true
	}
	if p.NextPK > p.MinPK {
		return p.NextPK - 1, true
	}
	return 0, false
}

// updatePlan đọc-sửa-ghi plan của bảng dưới planMu (không có plan / không lưu plan thì bỏ qua).
func (s *MySQLSnapshotter) updatePlan(table model.TableID, fn func(*SnapshotPlan)) error {
	if s.SavePlan == nil || s.LoadPlan == nil {
		return nil
	}
	s.planMu.Lock()
	defer s.planMu.Unlock()
	plan, ok := s.LoadPlan(table)
	if !ok {
		return nil
	}
	fn(&plan)
	return s.SavePlan(table, plan)
}

// PersistReread lưu bền tập khóa chờ đọc lại hiện tại của bảng (đang snapshot) vào plan. Router gọi
// trước khi Admit Txn đã đánh dấu khóa (HandleToastPKChangeXID); tập được đọc từ range-state dưới
// planMu nên lần ghi sau cùng luôn chứa mọi khóa chưa flush.
func (s *MySQLSnapshotter) PersistReread(table model.TableID) error {
	return s.updatePlan(table, func(p *SnapshotPlan) {
		if p.GenericKey { // việc C: khóa tổng quát lưu theo giá trị có kiểu
			p.RereadKeyVals = nil
			for _, b := range s.Manager.RereadBounds(table) {
				p.RereadKeyVals = append(p.RereadKeyVals, toKeyVals(b.V))
			}
			return
		}
		p.Reread = s.Manager.RereadKeys(table)
	})
}

// AddPlanReread thêm khóa vào plan của bảng CHƯA đăng ký range (chờ tới lượt snapshot); đăng ký sẽ nạp
// lại thành chunk re-read. Trả false nếu bảng không có plan (cả bảng sẽ được đọc, không cần ghi gì).
func (s *MySQLSnapshotter) AddPlanReread(table model.TableID, key int64) (bool, error) {
	found := false
	err := s.updatePlan(table, func(p *SnapshotPlan) {
		found = true
		for _, k := range p.Reread {
			if k == key {
				return
			}
		}
		p.Reread = append(p.Reread, key)
	})
	return found, err
}

// TableBounds chứa thông tin biên khóa chính của bảng.
type TableBounds struct {
	MinPK int64
	MaxPK int64
	Count int64
}

// GetTableBounds truy vấn min(pk), max(pk) và count(*) của bảng nguồn.
func GetTableBounds(ctx context.Context, db *sql.DB, table model.TableID, pkCol string) (*TableBounds, error) {
	// Chỉ MIN/MAX (đọc đầu/cuối index, O(1)); COUNT(*) trên bảng TB tốn IO source không cần thiết.
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quoteIdent(pkCol), quoteIdent(pkCol), quoteTable(table))
	var b TableBounds
	var minV, maxV sql.NullInt64
	err := db.QueryRowContext(ctx, query).Scan(&minV, &maxV)
	if err == nil && minV.Valid && maxV.Valid {
		b.MinPK, b.MaxPK, b.Count = minV.Int64, maxV.Int64, SpanRows(minV.Int64, maxV.Int64)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query table bounds: %w", err)
	}
	return &b, nil
}

// FlushChunk thực hiện merge và ghi vào target DB trong một thao tác đồng bộ nguyên tử.
func (s *MySQLSnapshotter) FlushChunk(
	ctx context.Context,
	dstDB *sql.DB,
	chunk *Chunk,
	table model.TableID,
	pkCol string,
	columnNames []string,
) error {
	s.Manager.mu.Lock()
	defer s.Manager.mu.Unlock()

	mergedRows := s.Manager.MergeLocked(chunk)
	if chunk.mergeErr != nil {
		// Row snapshot không mã hóa được khóa (vd. WEIGHT_STRING lỗi/timeout): trước đây bị bỏ âm thầm
		// khỏi tập merge — đường range rồi XÓA nó ở target, đường tập khóa không ghi nó (mất row, RS-1).
		// Dừng: chunk được đọc lại khi chạy tiếp (plan chưa qua chunk này).
		return fmt.Errorf("snapshot flush of %s: %w", table, chunk.mergeErr)
	}
	if !codecOrInt(chunk.codec).IsSingleInt() && s.FlushRows != nil {
		// Khóa tổng quát, target không phải MySQL (việc C).
		var err error
		switch {
		case chunk.KeySet != nil:
			err = s.FlushKeySet(ctx, s.target(table), columnNames, mergedRows, chunk.KeySet)
		case s.ExactKeys != nil && s.ExactKeys(table):
			// Target sắp khóa khác thứ tự source: flush theo tập khóa (cross_order.go).
			if s.FlushExact == nil {
				return fmt.Errorf("snapshot flush of %s: the target orders the primary key differently and needs FlushExact", table)
			}
			var dels []Bound
			if dels, err = exactDeletes(chunk, codecOrInt(chunk.codec)); err == nil {
				err = s.FlushExact(ctx, s.target(table), columnNames, mergedRows, dels)
			}
		default:
			err = s.FlushRange(ctx, s.target(table), columnNames, mergedRows, chunk.Lo, chunk.Hi)
		}
		if err != nil {
			return err
		}
		s.Manager.MarkDoneLocked(chunk)
		return nil
	}
	if chunk.KeySet != nil {
		if s.FlushKeys == nil {
			return fmt.Errorf("snapshot flush: re-read chunk of %s needs FlushKeys", table)
		}
		if err := s.FlushKeys(ctx, s.target(table), pkCol, columnNames, mergedRows, chunk.Keys); err != nil {
			return err
		}
		s.Manager.MarkDoneLocked(chunk)
		return nil
	}
	if s.FlushRows != nil {
		if err := s.FlushRows(ctx, s.target(table), pkCol, columnNames, mergedRows, chunk.StartPK, chunk.EndPK); err != nil {
			return err
		}
		s.Manager.MarkDoneLocked(chunk)
		return nil
	}
	codec := chunk.codec
	if codec == nil { // PK một cột số nguyên: codec theo tên cột thật
		codec = &PKCodec{Cols: []PKColumn{{Name: pkCol, Kind: PKInt}}}
	}
	tq := keySQL{table: s.target(table), codec: codec}
	cond, condArgs, err := tq.rangeCond(chunk.Lo, chunk.Hi)
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		err := flushRowsRange(ctx, dstDB, s.target(table), pkCol, columnNames, mergedRows, cond, condArgs)
		if err == nil {
			break
		}
		if !isRetryableMySQLError(err) || attempt >= 20 {
			return err
		}
		time.Sleep(time.Duration(attempt*attempt) * 5 * time.Millisecond)
	}
	s.Manager.MarkDoneLocked(chunk)
	return nil
}

// SnapshotAdd thực hiện thêm và snapshot một bảng vào pipeline đang chạy song song với stream.
// spec: 02 §5, design §6.1, bất biến RS-1, RS-2.
func (s *MySQLSnapshotter) SnapshotAdd(
	ctx context.Context,
	srcDB, dstDB *sql.DB,
	table model.TableID,
	pkCol string,
	columnNames []string,
	rendezvousWaitFn func(ctx context.Context, high checkpoint.AppliedSet) error,
) error {
	return s.SnapshotAddKeys(ctx, srcDB, dstDB, table, &PKCodec{Cols: []PKColumn{{Name: pkCol, Kind: PKInt}}}, columnNames, rendezvousWaitFn)
}

// SnapshotAddKeys như SnapshotAdd với PK tổng quát (việc C): codec mô tả các cột PK theo thứ tự PK.
// PK một cột số nguyên có dấu đi đúng đường int64 cũ (plan, SQL); PK khác dùng cận khóa mã hóa, plan
// GenericKey và SQL theo tuple — hiện chỉ cho source MySQL và target MySQL (FlushRows/Chunks nil).
func (s *MySQLSnapshotter) SnapshotAddKeys(
	ctx context.Context,
	srcDB, dstDB *sql.DB,
	table model.TableID,
	codec *PKCodec,
	columnNames []string,
	rendezvousWaitFn func(ctx context.Context, high checkpoint.AppliedSet) error,
) error {
	codec = codecOrInt(codec)
	generic := !codec.IsSingleInt()
	pkCol := codec.Cols[0].Name
	if generic {
		if _, ok := s.Chunks.(KeyChunkSource); s.Chunks != nil && !ok {
			return fmt.Errorf("snapshot add %s: the source does not support a primary key that is not a single integer column", table)
		}
		if s.FlushRows != nil && (s.FlushRange == nil || s.FlushKeySet == nil || s.ClearUnreadRange == nil) {
			return fmt.Errorf("snapshot add %s: the target does not support a primary key that is not a single integer column", table)
		}
	}
	kq := newKeySQL(table, codec)
	pkIdxs := make([]int, len(codec.Cols))
	for j, c := range codec.Cols {
		pkIdxs[j] = -1
		for i, n := range columnNames {
			if n == c.Name {
				pkIdxs[j] = i
			}
		}
		if pkIdxs[j] < 0 {
			return fmt.Errorf("snapshot add %s: pk column %q not in column list", table, c.Name)
		}
	}
	var err error
	if rendezvousWaitFn == nil {
		// Thiếu điểm hẹn thì chunk được flush trước khi stream xử lý hết high watermark (RS-1).
		return fmt.Errorf("snapshot add %s: rendezvous wait function is required", table)
	}
	var chunks []*Chunk
	var cur SnapshotPlan
	register := func() error {
		var plan SnapshotPlan
		resumed := false
		if s.LoadPlan != nil {
			plan, resumed = s.LoadPlan(table)
		}
		if s.Chunks != nil {
			if err := s.Chunks.BeforeRegister(ctx); err != nil {
				return err
			}
		}
		if !resumed {
			// 1. Biên đọc sau khi barrier giữ router: khóa > b_last thuộc range "Xong" ngay từ đầu.
			plan = SnapshotPlan{ChunkRows: s.opts.DefaultChunkRows}
			if generic {
				lo, hi, ok, err := s.keyBounds(ctx, srcDB, kq)
				if err != nil {
					return err
				}
				plan.GenericKey = true
				if !ok {
					plan.Empty = true
				} else {
					plan.MinKey, plan.MaxKey = toKeyVals(lo.V), toKeyVals(hi.V)
				}
			} else {
				var bounds *TableBounds
				var err error
				if s.Chunks != nil {
					bounds, err = s.Chunks.Bounds(ctx, table, pkCol)
				} else {
					bounds, err = GetTableBounds(ctx, srcDB, table, pkCol)
				}
				if err != nil {
					return err
				}
				if bounds.Count == 0 || bounds.MaxPK < bounds.MinPK {
					plan.Empty = true
				} else {
					plan.MinPK, plan.MaxPK, plan.NextPK = bounds.MinPK, bounds.MaxPK, bounds.MinPK
				}
			}
			// Snapshot mới của bảng bắt đầu từ target rỗng: row cũ còn sót (lần chạy trước,
			// hoặc dữ liệu có sẵn khi snapshot add) có thể giữ giá trị unique key mà row mới
			// cần, và không bao giờ được dọn nếu range của nó bị DELETE ở source.
			if s.ClearTarget != nil {
				if err := s.ClearTarget(ctx, s.target(table)); err != nil {
					return err
				}
			} else if err := clearMySQLTable(ctx, dstDB, s.target(table)); err != nil {
				return err
			}
			if s.SavePlan != nil {
				if err := s.SavePlan(table, plan); err != nil {
					return err
				}
			}
		}
		if resumed && generic != plan.GenericKey {
			return fmt.Errorf("saved snapshot plan of %s was made for a different primary key shape: clear the pipeline state to snapshot it again", table)
		}
		if generic {
			return s.registerGeneric(ctx, dstDB, table, pkCol, kq, &plan, resumed, &cur)
		}
		if resumed && !plan.Empty {
			// Chạy tiếp plan dở: flush chunk và lưu plan là hai bước (target commit rồi mới fsync plan).
			// Crash ở giữa để lại ở target row của phần plan còn coi là "Chưa đọc" — ảnh tại high watermark
			// CŨ. Event của phần đó bị bỏ (chờ chunk đọc lại) nên các row cũ không bao giờ được cập nhật
			// trước lần flush, mà vẫn giữ giá trị unique key: event của range "Xong" nhận lại giá trị đó
			// (source đã giải phóng nó) → 1062 (kill -9 e2e, 2026-11-04). Xóa phần chưa đọc trước khi
			// đăng ký: chunk walk sẽ đọc lại toàn bộ phần này, nên không mất gì (RS-1/2, CP-5).
			lo, hi := plan.NextPK, plan.MaxPK
			hasRange := !plan.Finished && lo <= hi
			if hasRange || len(plan.Reread) > 0 {
				if err := s.clearUnread(ctx, dstDB, table, pkCol, lo, hi, hasRange, plan.Reread); err != nil {
					return fmt.Errorf("clear unread part of resumed snapshot of %s: %w", table, err)
				}
			}
		}
		cur = plan
		if plan.Empty {
			s.Manager.RegisterTable(table, pkCol, nil, math.MinInt64)
			return nil
		}
		// 2. Keyset walk (02 §5): chỉ đăng ký phần đã xong [Min, Next-1] và phần chưa đọc
		// [Next, Max]; chunk kế tiếp được cắt động theo khóa thật ngay trước khi đọc, nên PK
		// thưa hay lệch không sinh ra hàng nghìn chunk rỗng.
		chunks = nil
		if doneEnd, ok := plan.FlushedEnd(); ok {
			chunks = append(chunks, &Chunk{ID: 0, Table: table, PKCol: pkCol, StartPK: plan.MinPK,
				EndPK: doneEnd, State: StateDone, BufferedChanges: map[Key]*model.RowChange{}})
		}
		if !plan.Finished && plan.NextPK <= plan.MaxPK {
			chunks = append(chunks, &Chunk{ID: 1, Table: table, PKCol: pkCol, StartPK: plan.NextPK,
				EndPK: plan.MaxPK, State: StatePending, BufferedChanges: map[Key]*model.RowChange{}})
		}
		s.Manager.RegisterTable(table, pkCol, chunks, plan.MaxPK)
		if len(plan.Reread) > 0 {
			// Khóa chờ đọc lại từ lần chạy trước (hoặc từ lúc bảng chờ đăng ký): nghĩa vụ vẫn còn.
			s.Manager.AddReread(table, plan.Reread, time.Now())
		}
		return nil
	}
	// savePlan ghi tiến độ sau mỗi flush: NextPK/Finished (chỉ chunk chính) và tập khóa còn chờ đọc lại.
	// Không tính EndPK+1 khi EndPK == MaxInt64 (tràn); Finished đánh dấu đã flush tới MaxPK.
	savePlan := func(c *Chunk) error {
		return s.updatePlan(table, func(p *SnapshotPlan) {
			if generic {
				if c.KeySet == nil {
					p.LastKey = toKeyVals(c.Hi.V)
					if maxK, err := codec.Encode(fromKeyVals(p.MaxKey)); err == nil && c.Hi.K >= maxK {
						p.Finished = true
					}
				}
				p.RereadKeyVals = nil
				for _, b := range s.Manager.RereadBounds(table) {
					p.RereadKeyVals = append(p.RereadKeyVals, toKeyVals(b.V))
				}
				return
			}
			if c.KeySet == nil {
				if c.EndPK >= p.MaxPK {
					p.Finished = true
				}
				if c.EndPK < math.MaxInt64 {
					p.NextPK = c.EndPK + 1
				} else {
					p.NextPK = c.EndPK
				}
			}
			p.Reread = s.Manager.RereadKeys(table)
		})
	}
	if s.Barrier != nil {
		err = s.Barrier(ctx, register)
	} else {
		err = register()
	}
	if err != nil {
		return fmt.Errorf("snapshot add %s: register ranges: %w", table, err)
	}

	// 3. Xử lý lần lượt từng chunk
	quotedSel := make([]string, len(columnNames))
	for i, c := range columnNames {
		quotedSel[i] = quoteIdent(c)
	}
	colsStr := strings.Join(quotedSel, ", ")
	src := s.Source
	if src == nil {
		src = &sqlSourceQuerier{db: srcDB}
	}

	// processChunk: đọc (low → SELECT → high), điểm hẹn, flush trong barrier, lưu plan, giãn nhịp.
	processOne := func(chunk *Chunk) (*Chunk, error) {
		var read ChunkRead
		var chunkHigh checkpoint.AppliedSet
		var err error
		if s.Chunks != nil {
			read, chunkHigh, err = s.Chunks.ReadChunk(ctx, s, chunk, columnNames, pkIdxs, table)
		} else if chunk.KeySet != nil {
			return nil, fmt.Errorf("snapshot %s: re-read chunks need a PostgreSQL chunk source", table)
		} else {
			// Watermark (RS-2): low → WAIT_FOR_EXECUTED_GTID_SET(low) → SELECT → high, cùng server.
			read, chunkHigh, err = s.readChunkWatermarked(ctx, src, chunk, kq, colsStr, columnNames, pkIdxs, table)
		}
		if err != nil {
			return nil, err
		}
		rows := read.Rows
		var rest *Chunk
		if read.Truncated {
			// Chạm max_chunk_bytes: flush phần đã đọc như một chunk, phần còn lại đọc tiếp ngay sau
			// (cùng low, xem SplitReading) — RAM của chunk không vượt trần (review R10).
			if rest, err = s.Manager.SplitReading(chunk, read.Cut); err != nil {
				return nil, err
			}
			if s.OnTruncatedRead != nil {
				s.OnTruncatedRead()
			}
		}
		s.Manager.FinishReading(chunk, chunkHigh, rows)

		// Rendezvous: chờ stream xử lý hết high watermark
		if err := rendezvousWaitFn(ctx, chunkHigh); err != nil {
			return nil, fmt.Errorf("rendezvous wait failed: %w", err)
		}

		// Flush: merge có thứ tự snapshot và buffer trong 1 transaction ở target và chuyển sang StateDone
		// Flush trong barrier: mọi Txn router đã nhận phải commit xong trước, để cả bảng ở
		// target nhất quán tại đúng vị trí của router (không xung đột unique key giữa row
		// của chunk và row ở range khác do Txn còn trong hàng đợi).
		flush := func() error { return s.FlushChunk(ctx, dstDB, chunk, table, pkCol, columnNames) }
		if s.Barrier != nil {
			err = s.Barrier(ctx, flush)
		} else {
			err = flush()
		}
		if err == nil {
			err = savePlan(chunk)
		}
		if err == nil && chunk.KeySet != nil {
			s.RereadKeysTotal.Add(int64(len(chunk.KeySet)))
		}
		if err != nil {
			return nil, fmt.Errorf("failed to flush chunk [%d, %d] to target: %w", chunk.StartPK, chunk.EndPK, err)
		}

		// Throttling nếu có
		if s.Pace != nil {
			if err := s.Pace(ctx, read.Bytes); err != nil {
				return nil, err
			}
		} else if s.opts.SleepBetweenChunks > 0 {
			time.Sleep(s.opts.SleepBetweenChunks)
		}
		return rest, nil
	}
	// processChunk xử lý chunk và mọi phần đuôi tách ra khi chạm max_chunk_bytes, tuần tự.
	processChunk := func(chunk *Chunk) error {
		for chunk != nil {
			rest, err := processOne(chunk)
			if err != nil {
				return err
			}
			chunk = rest
		}
		return nil
	}

	if generic {
		return s.walkGeneric(ctx, srcDB, table, kq, cur, processChunk)
	}
	// mainDone: walk chính đã xong. Theo dõi bằng cờ thay vì next > MaxPK (tràn khi MaxPK == MaxInt64).
	mainDone := cur.Empty || cur.Finished || cur.NextPK > cur.MaxPK
	for next := cur.NextPK; ; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Chunk re-read (02 §8) đến hạn — đạt max_pending_cross_range, chờ quá 30 s, hoặc walk chính đã
		// xong — được đọc trước chunk chính kế tiếp (đầu hàng đợi đọc).
		if k := s.Manager.TakeReread(table, time.Now(), mainDone); k != nil {
			if err := processChunk(k); err != nil {
				return err
			}
			continue
		}
		if mainDone {
			// Không còn khóa chờ đọc lại và mọi range đã "Xong": từ đây không thể phát sinh khóa mới
			// (HandleToastPKChangeXID chỉ đánh dấu khi có range chưa xong), bảng snapshot xong.
			break
		}
		var end int64
		if s.Chunks != nil {
			end, err = s.Chunks.KeysetEnd(ctx, table, pkCol, next, cur.MaxPK, cur.ChunkRows)
		} else {
			end, err = keysetEnd(ctx, srcDB, table, pkCol, next, cur.MaxPK, cur.ChunkRows)
		}
		if err != nil {
			return fmt.Errorf("keyset walk %s from %d: %w", table, next, err)
		}
		chunk, err := s.Manager.CarveChunk(table, next, end)
		if err != nil {
			return err
		}
		if end >= cur.MaxPK {
			mainDone = true
		} else {
			next = end + 1
		}
		if err := processChunk(chunk); err != nil {
			return err
		}
	}

	return nil
}

// keysetEnd trả về khóa cuối của chunk bắt đầu ở start: khóa thứ `rows` tính từ start
// (SELECT pk ... LIMIT 1 OFFSET rows-1), hoặc max nếu không còn đủ khóa.
func keysetEnd(ctx context.Context, db *sql.DB, table model.TableID, pkCol string, start, max, rows int64) (int64, error) {
	if rows <= 0 {
		rows = 5000
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? AND %s <= ? ORDER BY %s LIMIT 1 OFFSET %d",
		quoteIdent(pkCol), quoteTable(table), quoteIdent(pkCol), quoteIdent(pkCol), quoteIdent(pkCol), rows-1)
	var end int64
	err := db.QueryRowContext(ctx, q, start, max).Scan(&end)
	if err == sql.ErrNoRows {
		return max, nil
	}
	if err != nil {
		return 0, err
	}
	return end, nil
}

// flushRowsToTarget ghi các row đã merge vào target database trong một transaction duy nhất.
func flushRowsToTarget(
	ctx context.Context,
	dstDB *sql.DB,
	table model.TableID,
	pkCol string,
	columnNames []string,
	rows []*model.RowChange,
	startPK, endPK int64,
) error {
	return flushRowsRange(ctx, dstDB, table, pkCol, columnNames, rows,
		fmt.Sprintf("%s >= ? AND %s <= ?", quoteIdent(pkCol), quoteIdent(pkCol)), []any{startPK, endPK})
}

// flushRowsRange như flushRowsToTarget với điều kiện range bất kỳ (tuple PK, việc C).
func flushRowsRange(
	ctx context.Context,
	dstDB *sql.DB,
	table model.TableID,
	pkCol string,
	columnNames []string,
	rows []*model.RowChange,
	rangeCond string,
	rangeArgs []any,
) error {
	// Kết nối riêng: tắt kiểm tra FK chỉ trong phiên flush (bảng con có thể được flush trước khi
	// Txn của row cha còn trong hàng đợi được apply; thứ tự stream vẫn do writeset bảo đảm).
	conn, err := dstDB.Conn(ctx)
	if err != nil {
		return err
	}
	// Kết nối trở lại pool với foreign_key_checks=0, đúng mặc định phiên ghi của pipeline
	// (DSN target). Không đặt lại 1, vì writer dùng chung pool.
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := checkpoint.CheckFence(ctx, tx, false); err != nil { // SG-1, review R07
		return err
	}

	// Range [startPK, endPK] ở target phải đúng bằng ảnh đã merge: xóa row không còn ở source
	// (bị DELETE trong lúc range "Chưa đọc", hoặc còn sót từ lần snapshot trước khi crash),
	// cùng transaction với upsert nên target không bao giờ hiển thị trạng thái lùi (RS-1).
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s", quoteTable(table), rangeCond), rangeArgs...); err != nil {
		return fmt.Errorf("clear chunk range on target: %w", err)
	}
	if len(rows) == 0 {
		return tx.Commit()
	}

	// Ghi theo batch multi-row upsert
	const batchSize = 1000
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		var valPlaceholders []string
		var args []any
		for _, row := range batch {
			var phs []string
			colMap := make(map[string]any)
			for _, c := range row.Cols {
				colMap[c.Name] = c.Value
			}
			for _, col := range columnNames {
				phs = append(phs, "?")
				args = append(args, colMap[col])
			}
			valPlaceholders = append(valPlaceholders, "("+strings.Join(phs, ", ")+")")
		}

		var updateExprs []string
		for _, col := range columnNames {
			if col != pkCol {
				updateExprs = append(updateExprs, fmt.Sprintf("%s = VALUES(%s)", quoteIdent(col), quoteIdent(col)))
			}
		}

		if len(updateExprs) == 0 {
			updateExprs = append(updateExprs, fmt.Sprintf("%s = %s", quoteIdent(pkCol), quoteIdent(pkCol)))
		}
		quotedCols := make([]string, len(columnNames))
		for i, c := range columnNames {
			quotedCols[i] = quoteIdent(c)
		}
		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			quoteTable(table),
			strings.Join(quotedCols, ", "),
			strings.Join(valPlaceholders, ", "),
			strings.Join(updateExprs, ", "),
		)

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("upsert flush batch failed: %w", err)
		}
	}

	return tx.Commit()
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quoteTable(t model.TableID) string {
	if t.Schema == "" {
		return quoteIdent(t.Table)
	}
	return quoteIdent(t.Schema) + "." + quoteIdent(t.Table)
}

func (s *MySQLSnapshotter) target(t model.TableID) model.TableID {
	if s.TargetName != nil {
		return s.TargetName(t)
	}
	return t
}

// clearMySQLTable xóa mọi row của bảng đích MySQL trong một transaction có kiểm fence (SG-1, R07).
func clearMySQLTable(ctx context.Context, dstDB *sql.DB, t model.TableID) error {
	tx, err := dstDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := checkpoint.CheckFence(ctx, tx, false); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", quoteTable(t))); err != nil {
		return err
	}
	return tx.Commit()
}

// clearUnread xóa phần "Chưa đọc" của plan dở ở bảng đích (xem SnapshotAdd, nhánh resumed).
func (s *MySQLSnapshotter) clearUnread(ctx context.Context, dstDB *sql.DB, table model.TableID, pkCol string, lo, hi int64, hasRange bool, keys []int64) error {
	t := s.target(table)
	if s.ClearUnread != nil {
		return s.ClearUnread(ctx, t, pkCol, lo, hi, hasRange, keys)
	}
	if dstDB == nil {
		return fmt.Errorf("no target to clear (set ClearUnread when FlushRows replaces the MySQL target)")
	}
	conn, err := dstDB.Conn(ctx)
	if err != nil {
		return err
	}
	// FOREIGN_KEY_CHECKS=0: không cascade xóa/đổi row con ở bảng khác (giống flushRowsToTarget). Trả
	// kết nối về pool với ĐÚNG giá trị cũ của phiên: kết nối ghi của pipeline mặc định
	// foreign_key_checks=0 (pipeline.targetDSN); để lại 1 thì writer dùng lại kết nối này sẽ bị 1452
	// khi row cha còn ở phần chưa đọc (đã gặp ở e2e kill -9). Không khôi phục được thì bỏ kết nối.
	var prev int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.foreign_key_checks").Scan(&prev); err != nil {
		conn.Close()
		return err
	}
	restored := false
	defer func() {
		if !restored {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn }) // loại khỏi pool
		}
		conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS = 0"); err != nil {
		return err
	}
	restore := func() error {
		_, err := conn.ExecContext(context.Background(), fmt.Sprintf("SET SESSION FOREIGN_KEY_CHECKS = %d", prev))
		restored = err == nil
		return err
	}
	if err := func() error {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := checkpoint.CheckFence(ctx, tx, false); err != nil { // SG-1, review R07
			return err
		}
		if hasRange {
			q := fmt.Sprintf("DELETE FROM %s WHERE %s >= ? AND %s <= ?", quoteTable(t), quoteIdent(pkCol), quoteIdent(pkCol))
			if _, err := tx.ExecContext(ctx, q, lo, hi); err != nil {
				return err
			}
		}
		for i := 0; i < len(keys); i += 1000 {
			end := i + 1000
			if end > len(keys) {
				end = len(keys)
			}
			ph := strings.TrimSuffix(strings.Repeat("?,", end-i), ",")
			args := make([]any, 0, end-i)
			for _, k := range keys[i:end] {
				args = append(args, k)
			}
			q := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", quoteTable(t), quoteIdent(pkCol), ph)
			if _, err := tx.ExecContext(ctx, q, args...); err != nil {
				return err
			}
		}
		return tx.Commit()
	}(); err != nil {
		return err
	}
	return restore()
}
