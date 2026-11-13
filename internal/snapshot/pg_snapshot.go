package snapshot

// Snapshot logic từ source PostgreSQL (02 §5, design §6.1): dùng chung range-state, barrier,
// rendezvous, flush và plan với MySQL; phần riêng là biên PK, keyset walk và watermark.
//
// Một chunk được đọc như sau (mọi bước trên source PG chính, không phải standby):
//
//  1. BeginReading: chunk "Chưa đọc" → "Đang đọc" CHƯA có low: mọi event mới của range được gộp vào
//     buffer (gộp luôn an toàn; chỉ BỎ mới cần chứng minh).
//  2. W = xid của mọi Txn router đã xử lý mà chưa được xác nhận visible (XIDTracker, chụp SAU bước 1:
//     chứa mọi event đã bị bỏ khi range còn "Chưa đọc").
//  3. Low: lặp S0 = pg_current_snapshot() tới khi mọi xid trong W visible với S0 (và cổng khởi động,
//     xem Gate). Txn đã stream nhưng chưa ProcArrayEndTransaction sẽ visible rất sớm.
//  4. SELECT chunk (có thể chia đôi khi quá hạn: mỗi SELECT là một snapshot S_i lấy SAU S0).
//  5. High: H = pg_current_wal_insert_lsn() đọc SAU mọi S_i; rồi ghi watermark
//     pg_logical_emit_message(false, 'strand_watermark', …) — record của nó bắt đầu ở LSN ≥ H.
//  6. Rendezvous: chờ router xử lý xong một Txn (thật, hoặc Txn rỗng của watermark) có LSN ≥ H.
//  7. Flush trong barrier: merge row snapshot với buffer theo PK, thay nguyên range ở target.
//
// Chunk re-read (Chunk.Keys, UPDATE đổi PK mà cột TOAST không đổi — toast_pk.go) đi đúng các bước trên,
// chỉ khác SELECT theo tập khóa và flush thay đúng tập khóa đó.
//
// Vì sao đúng (với mỗi PK p của chunk; các Txn chạm p có thứ tự commit LSN T1 < T2 < …):
//   - Visibility theo PK là một TIỀN TỐ của thứ tự stream: Tk+1 chỉ sửa được row p sau khi chờ khóa
//     row của Tk, mà khóa đó chỉ nhả sau ProcArrayEndTransaction của Tk; nên Tk+1 visible ⇒ Tk visible
//     với mọi snapshot. Mọi S_i ⊇ S0 (đã visible thì visible mãi).
//   - Event bị BỎ ở "Đang đọc" là của Txn visible với S0, nên row S_i đã phản ánh nó (và mọi Tk trước
//     nó); ảnh cũ hơn trong buffer cũng bị bỏ cùng lúc. Event bị BỎ ở "Chưa đọc" thuộc W, visible với
//     S0 theo bước 3. Event được GỘP thì ảnh cuối của buffer ghi đè row snapshot: ảnh cuối luôn là
//     trạng thái mới nhất của p tại lúc flush (nếu Txn cuối chạm p visible với S0 thì mọi Txn chạm p
//     đều visible, buffer rỗng và row S_i đúng bằng trạng thái đó).
//   - Không Txn nào mà row S_i phản ánh còn chưa qua range-state lúc flush: Txn visible với S_i có
//     commit record được chèn trước S_i, nên commit LSN < H; stream phát theo thứ tự commit LSN, nên
//     khi router đã xử lý một Txn có LSN ≥ H thì mọi Txn có commit LSN < H đã được xử lý. Watermark
//     (bước 5) bảo đảm luôn có Txn như vậy kể cả khi source không còn ghi gì.
//   - Cột TOAST không đổi (UPDATE không gửi giá trị): lấy từ ảnh trước trong buffer, hoặc từ row S_i
//     khi ảnh gộp vẫn còn dấu "không đổi" — khi đó không Txn nào từ Txn đầu tiên được gộp tới Txn cuối
//     đổi cột đó, và row S_i là một phiên bản trong đoạn [ngay trước Txn đầu tiên được gộp, cuối] (Txn
//     trước nó visible với S0 ⊆ S_i), nên giá trị của cột trong row S_i là đúng.
//
// Đăng ký bảng (BeforeRegister, trong barrier): Txn router đã xử lý trước đó đã được apply rồi bị
// ClearTarget xóa (hoặc bị bỏ vì bảng chưa thuộc pipeline), nên biên và mọi chunk phải thấy chúng:
// chờ mọi xid trong XIDTracker visible trước khi đọc biên. Txn có commit LSN nhỏ hơn điểm bắt đầu
// stream của tiến trình này không đi qua router: cổng khởi động (Gate) bảo đảm chúng visible.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	pgreader "strand/internal/reader/pg"
)

// PGChunkSource là ChunkSource cho source PostgreSQL.
type PGChunkSource struct {
	// DB: pool tới source PG (driver pgx), phiên đặt pgreader.SessionParams để định dạng text khớp
	// pgoutput.
	DB *sql.DB
	// Tracker: xid các Txn router đã xử lý (pipeline gọi Add cho mỗi Txn của source PG).
	Tracker *XIDTracker
	// Gate (tùy chọn): điều kiện trên snapshot phải đạt MỘT lần trước lần đăng ký đầu tiên — mọi Txn
	// có commit LSN nhỏ hơn điểm bắt đầu stream đều visible. nil = không cần.
	Gate func(*PGSnapshot) bool
	// GateErr: không dựng được cổng (vd. source là standby): snapshot logic bị từ chối.
	GateErr error
	// ServerVersion (server_version_num): ≥ 170000 thì watermark được flush ngay.
	ServerVersion int
	// WaitTimeout: thời gian tối đa chờ snapshot thỏa điều kiện (mặc định 10 phút).
	WaitTimeout time.Duration
	Log         *slog.Logger

	mu     sync.Mutex
	gateOK bool
	// boundSnap: snapshot của lần đọc biên gần nhất (BoundSnapshot).
	boundSnap *PGSnapshot
	types     map[model.TableID]map[string]uint32
}

var _ ChunkSource = (*PGChunkSource)(nil)

func quotePGIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func quotePGTable(t model.TableID) string {
	if t.Schema == "" {
		return quotePGIdent(t.Table)
	}
	return quotePGIdent(t.Schema) + "." + quotePGIdent(t.Table)
}

// CurrentPGSnapshot đọc pg_current_snapshot() (snapshot của chính câu lệnh).
func CurrentPGSnapshot(ctx context.Context, db *sql.DB) (*PGSnapshot, error) {
	var s string
	if err := db.QueryRowContext(ctx, "SELECT pg_current_snapshot()::text").Scan(&s); err != nil {
		return nil, fmt.Errorf("read pg_current_snapshot: %w", err)
	}
	return ParsePGSnapshot(s)
}

// waitSnapshot lặp lấy snapshot tới khi mọi xid trong xids visible (và cổng khởi động đạt); mỗi lần
// lấy cũng gỡ khỏi Tracker các xid đã visible.
func (c *PGChunkSource) waitSnapshot(ctx context.Context, xids []uint32) (*PGSnapshot, error) {
	if c.GateErr != nil {
		return nil, c.GateErr
	}
	timeout := c.WaitTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	start := time.Now()
	warned := false
	delay := 2 * time.Millisecond
	for {
		s, err := CurrentPGSnapshot(ctx, c.DB)
		if err != nil {
			return nil, err
		}
		if c.Tracker != nil {
			c.Tracker.Prune(s)
		}
		c.mu.Lock()
		gate := c.gateOK || c.Gate == nil || c.Gate(s)
		if gate {
			c.gateOK = true
		}
		c.mu.Unlock()
		if gate && AllVisible(s, xids) {
			return s, nil
		}
		if time.Since(start) > timeout {
			return nil, fmt.Errorf("source transactions did not become visible within %s (snapshot %s, gate passed %v)", timeout, s, gate)
		}
		if !warned && time.Since(start) > 5*time.Second && c.Log != nil {
			warned = true
			c.Log.Warn("snapshot waits for source transactions to become visible (a long-running transaction on the source delays the logical snapshot)",
				"snapshot", s.String(), "startup_gate_passed", gate)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		if delay < 50*time.Millisecond {
			delay *= 2
		}
	}
}

// WaitGate chờ cổng khởi động đạt (gọi NGOÀI barrier, để router không bị giữ khi phải chờ một Txn chạy
// lâu ở source kết thúc).
func (c *PGChunkSource) WaitGate(ctx context.Context) error {
	_, err := c.waitSnapshot(ctx, nil)
	return err
}

// BeforeRegister: router đã dừng (barrier) nên Tracker không đổi; chờ mọi Txn đã xử lý visible.
func (c *PGChunkSource) BeforeRegister(ctx context.Context) error {
	var xids []uint32
	if c.Tracker != nil {
		xids = c.Tracker.Pending()
	}
	_, err := c.waitSnapshot(ctx, xids)
	return err
}

// Bounds: MIN/MAX của PK (đọc đầu/cuối index).
func (c *PGChunkSource) Bounds(ctx context.Context, table model.TableID, pkCol string) (*TableBounds, error) {
	// pg_current_snapshot() trong CÙNG câu lệnh: snapshot mà MIN/MAX nhìn thấy (BoundSnapshot).
	q := fmt.Sprintf("SELECT MIN(%s)::bigint, MAX(%s)::bigint, pg_current_snapshot()::text FROM %s", quotePGIdent(pkCol), quotePGIdent(pkCol), quotePGTable(table))
	var minV, maxV sql.NullInt64
	var snapTxt string
	if err := c.DB.QueryRowContext(ctx, q).Scan(&minV, &maxV, &snapTxt); err != nil {
		return nil, fmt.Errorf("failed to query table bounds: %w", err)
	}
	if err := c.setBoundSnap(snapTxt); err != nil {
		return nil, err
	}
	var b TableBounds
	if minV.Valid && maxV.Valid {
		b.MinPK, b.MaxPK, b.Count = minV.Int64, maxV.Int64, SpanRows(minV.Int64, maxV.Int64)
	}
	return &b, nil
}

// KeysetEnd: khóa thứ rows tính từ start (hoặc max nếu không đủ khóa).
func (c *PGChunkSource) KeysetEnd(ctx context.Context, table model.TableID, pkCol string, start, max, rows int64) (int64, error) {
	if rows <= 0 {
		rows = 5000
	}
	pk := quotePGIdent(pkCol)
	q := fmt.Sprintf("SELECT %s::bigint FROM %s WHERE %s >= $1 AND %s <= $2 ORDER BY %s LIMIT 1 OFFSET %d",
		pk, quotePGTable(table), pk, pk, pk, rows-1)
	var end int64
	err := c.DB.QueryRowContext(ctx, q, start, max).Scan(&end)
	if err == sql.ErrNoRows {
		return max, nil
	}
	if err != nil {
		return 0, err
	}
	return end, nil
}

// columnTypes: OID kiểu của từng cột (atttypid, giống typeoid trong Relation của pgoutput).
func (c *PGChunkSource) columnTypes(ctx context.Context, table model.TableID) (map[string]uint32, error) {
	c.mu.Lock()
	if m, ok := c.types[table]; ok {
		c.mu.Unlock()
		return m, nil
	}
	c.mu.Unlock()
	rows, err := c.DB.QueryContext(ctx, `
		SELECT a.attname, a.atttypid::bigint FROM pg_attribute a
		  JOIN pg_class cl ON cl.oid = a.attrelid JOIN pg_namespace n ON n.oid = cl.relnamespace
		 WHERE n.nspname = $1 AND cl.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped`, table.Schema, table.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]uint32{}
	for rows.Next() {
		var n string
		var oid int64
		if err := rows.Scan(&n, &oid); err != nil {
			return nil, err
		}
		m[n] = uint32(oid)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.types == nil {
		c.types = map[model.TableID]map[string]uint32{}
	}
	c.types[table] = m
	c.mu.Unlock()
	return m, nil
}

// ReadChunk: các bước 1–5 ở đầu file.
func (c *PGChunkSource) ReadChunk(ctx context.Context, s *MySQLSnapshotter, chunk *Chunk, columnNames []string, pkIdxs []int,
	table model.TableID) (ChunkRead, checkpoint.AppliedSet, error) {
	types, err := c.columnTypes(ctx, table)
	if err != nil {
		return ChunkRead{}, nil, fmt.Errorf("read column types of %s: %w", table, err)
	}
	if !chunk.Continuation {
		// 1–3. "Đang đọc" không low → W → S0 thấy W. Phần đuôi của SplitReading giữ S0 của chunk gốc.
		s.Manager.BeginReading(chunk)
		var xids []uint32
		if c.Tracker != nil {
			xids = c.Tracker.Pending()
		}
		low, err := c.waitSnapshot(ctx, xids)
		if err != nil {
			return ChunkRead{}, nil, fmt.Errorf("chunk low watermark: %w", err)
		}
		s.Manager.StartReadingPG(chunk, low)
	}

	// 4. SELECT (mỗi lần một snapshot mới, đều sau S0), theo tuple PK (việc C).
	sel := make([]string, len(columnNames))
	for i, col := range columnNames {
		sel[i] = "t." + quotePGIdent(col) + "::text"
	}
	codec := chunk.codec
	if codec == nil {
		codec = &PKCodec{Cols: []PKColumn{{Name: columnNames[pkIdxs[0]], Kind: PKInt, SQLType: "int8"}}}
	}
	kq := PGKeySQL{Table: table, Codec: codec, Alias: "t"}
	var read ChunkRead
	if chunk.KeySet != nil {
		// Chunk re-read (02 §8, toast_pk.go): tập khóa rời rạc, tra theo index PK (≤ max_pending_cross_range
		// khóa) nên không cần chia đôi theo timeout hay cắt theo byte.
		cond, args, _, err := kq.InList(chunk.KeySet, 1)
		if err != nil {
			return ChunkRead{}, nil, err
		}
		// ORDER BY t.pk (cột của BẢNG), không phải cột kết quả pk::text (xem 9f614ab).
		query := fmt.Sprintf("SELECT %s FROM %s AS t WHERE %s ORDER BY %s",
			strings.Join(sel, ", "), quotePGTable(table), cond, kq.OrderBy(false))
		if read.Rows, read.Bytes, err = c.queryChunk(ctx, query, columnNames, pkIdxs, types, table, 0, args...); err != nil {
			return ChunkRead{}, nil, fmt.Errorf("re-read %d keys of %s: %w", len(chunk.KeySet), table, err)
		}
	} else {
		queryFn := func(qCtx context.Context, ch *Chunk, maxBytes int64) ([]*model.RowChange, int64, error) {
			cond, args, _, err := kq.RangeCond(ch.Lo, ch.Hi, 1)
			if err != nil {
				return nil, 0, err
			}
			// ORDER BY t.pk (cột của BẢNG): `ORDER BY pk` trỏ vào cột kết quả `pk::text` và sắp theo chuỗi
			// ("10" < "9") — chunk bị cắt theo max_chunk_bytes khi đó không phủ trọn [Start, Cut] (mất row).
			query := fmt.Sprintf("SELECT %s FROM %s AS t WHERE %s ORDER BY %s",
				strings.Join(sel, ", "), quotePGTable(table), cond, kq.OrderBy(false))
			return c.queryChunk(qCtx, query, columnNames, pkIdxs, types, table, maxBytes, args...)
		}
		if read, err = s.Chunker.ReadChunkAdaptive(ctx, chunk, s.opts, queryFn); err != nil {
			return ChunkRead{}, nil, fmt.Errorf("adaptive read failed for chunk [%x, %x]: %w", chunk.Lo.K, chunk.Hi.K, err)
		}
	}

	// 5. High = insert LSN sau mọi SELECT, rồi watermark (record bắt đầu ở LSN ≥ high).
	var hs string
	if err := c.DB.QueryRowContext(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&hs); err != nil {
		return ChunkRead{}, nil, fmt.Errorf("read chunk high watermark: %w", err)
	}
	high, err := checkpoint.ParseLSN(hs)
	if err != nil {
		return ChunkRead{}, nil, err
	}
	if err := EmitPGWatermark(ctx, c.DB, c.ServerVersion, hs); err != nil {
		return ChunkRead{}, nil, err
	}
	return read, checkpoint.NewPGAppliedSet(high), nil
}

// EmitPGWatermark ghi logical decoding message không transactional (reader phát thành Txn rỗng).
// PG17+ flush ngay; bản cũ hơn chờ walwriter (wal_writer_delay).
func EmitPGWatermark(ctx context.Context, db *sql.DB, serverVersion int, content string) error {
	q := "SELECT pg_logical_emit_message(false, $1, $2)::text"
	if serverVersion >= 170000 {
		q = "SELECT pg_logical_emit_message(false, $1, $2, true)::text"
	}
	var lsn string
	if err := db.QueryRowContext(ctx, q, pgreader.WatermarkPrefix, content).Scan(&lsn); err != nil {
		return fmt.Errorf("emit snapshot watermark (pg_logical_emit_message): %w", err)
	}
	return nil
}

// queryChunk chạy SELECT của chunk; maxBytes > 0 thì dừng (và hủy query) ngay sau row làm tổng byte
// đạt maxBytes (xem QueryChunkFunc).
func (c *PGChunkSource) queryChunk(ctx context.Context, query string, columnNames []string, pkIdxs []int,
	types map[string]uint32, table model.TableID, maxBytes int64, args ...any) ([]*model.RowChange, int64, error) {
	qctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rows, err := c.DB.QueryContext(qctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*model.RowChange
	var total int64
	vals := make([]sql.NullString, len(columnNames))
	ptrs := make([]any, len(columnNames))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, 0, err
		}
		rc := &model.RowChange{Table: table, Op: model.OpInsert, Cols: make([]model.ColValue, len(columnNames))}
		for i, col := range columnNames {
			var v any
			if vals[i].Valid {
				total += int64(len(vals[i].String))
				if v, err = pgreader.ParseTextValue(types[col], vals[i].String); err != nil {
					return nil, 0, fmt.Errorf("%s column %s: %w", table, col, err)
				}
			}
			rc.Cols[i] = model.ColValue{Name: col, Value: v}
		}
		rc.Key = make([]model.Value, len(pkIdxs)) // mọi cột PK theo thứ tự PK, đã parse như row stream
		for i, idx := range pkIdxs {
			if !vals[idx].Valid {
				return nil, 0, fmt.Errorf("%s: NULL primary key", table)
			}
			rc.Key[i] = rc.Cols[idx].Value
		}
		out = append(out, rc)
		if maxBytes > 0 && total >= maxBytes {
			cancel()
			return out, total, nil
		}
	}
	return out, total, rows.Err()
}

// KeyChunkSource: ChunkSource có biên và keyset walk theo tuple PK (việc C).
type KeyChunkSource interface {
	KeyBounds(ctx context.Context, table model.TableID, codec *PKCodec) (lo, hi Bound, ok bool, err error)
	KeyKeysetEnd(ctx context.Context, table model.TableID, codec *PKCodec, lo, max Bound, n int64) (Bound, error)
}

// pgScanBound đọc các cột PK (SELECT col::text theo thứ tự codec) và parse như row stream.
func (c *PGChunkSource) pgScanBound(ctx context.Context, qr interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, table model.TableID, codec *PKCodec, query string, args ...any) (Bound, bool, error) {
	types, err := c.columnTypes(ctx, table)
	if err != nil {
		return Bound{}, false, err
	}
	rows, err := qr.QueryContext(ctx, query, args...)
	if err != nil {
		return Bound{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return Bound{}, false, rows.Err()
	}
	txt := make([]sql.NullString, len(codec.Cols))
	ptrs := make([]any, len(txt))
	for i := range txt {
		ptrs[i] = &txt[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return Bound{}, false, err
	}
	vals := make([]model.Value, len(txt))
	for i, col := range codec.Cols {
		if !txt[i].Valid {
			return Bound{}, false, fmt.Errorf("%s: NULL primary key", table)
		}
		if vals[i], err = pgreader.ParseTextValue(types[col.Name], txt[i].String); err != nil {
			return Bound{}, false, err
		}
	}
	b, err := codec.BoundOf(vals)
	return b, err == nil, err
}

func (c *PGChunkSource) keyCols(codec *PKCodec) string {
	q := PGKeySQL{Codec: codec, Alias: "t"}
	cols := q.Cols()
	for i := range cols {
		cols[i] += "::text"
	}
	return strings.Join(cols, ", ")
}

// setBoundSnap lưu snapshot của lần đọc biên gần nhất (đăng ký bảng chạy tuần tự trong barrier).
func (c *PGChunkSource) setBoundSnap(txt string) error {
	snap, err := ParsePGSnapshot(txt)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.boundSnap = snap
	c.mu.Unlock()
	return nil
}

// BoundSnapshot: snapshot mà lần Bounds/KeyBounds gần nhất nhìn thấy. Ngoài [min, max] đó bảng KHÔNG có
// row nào tại snapshot này (xem TableRanges.BoundSnap).
func (c *PGChunkSource) BoundSnapshot() *PGSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.boundSnap
}

// KeyBounds: khóa nhỏ nhất / lớn nhất (ORDER BY cột của bảng, LIMIT 1). Hai truy vấn chạy trong MỘT
// transaction REPEATABLE READ nên cùng một snapshot (BoundSnapshot).
func (c *PGChunkSource) KeyBounds(ctx context.Context, table model.TableID, codec *PKCodec) (lo, hi Bound, ok bool, err error) {
	tx, err := c.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return
	}
	defer tx.Rollback()
	var snapTxt string
	if err = tx.QueryRowContext(ctx, "SELECT pg_current_snapshot()::text").Scan(&snapTxt); err != nil {
		return
	}
	if err = c.setBoundSnap(snapTxt); err != nil {
		return
	}
	q := PGKeySQL{Table: table, Codec: codec, Alias: "t"}
	one := func(desc bool) (Bound, bool, error) {
		return c.pgScanBound(ctx, tx, table, codec, fmt.Sprintf("SELECT %s FROM %s AS t ORDER BY %s LIMIT 1",
			c.keyCols(codec), quotePGTable(table), q.OrderBy(desc)))
	}
	if lo, ok, err = one(false); err != nil || !ok {
		return
	}
	hi, ok, err = one(true)
	return
}

// KeyKeysetEnd: khóa thứ n trong [lo, max] (hoặc max nếu không đủ khóa).
func (c *PGChunkSource) KeyKeysetEnd(ctx context.Context, table model.TableID, codec *PKCodec, lo, max Bound, n int64) (Bound, error) {
	if n <= 0 {
		n = 5000
	}
	q := PGKeySQL{Table: table, Codec: codec, Alias: "t"}
	cond, args, _, err := q.RangeCond(lo, max, 1)
	if err != nil {
		return Bound{}, err
	}
	b, ok, err := c.pgScanBound(ctx, c.DB, table, codec, fmt.Sprintf("SELECT %s FROM %s AS t WHERE %s ORDER BY %s LIMIT 1 OFFSET %d",
		c.keyCols(codec), quotePGTable(table), cond, q.OrderBy(false), n-1), args...)
	if err != nil {
		return Bound{}, err
	}
	if !ok {
		return max, nil
	}
	return b, nil
}
