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
	"strconv"
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
	types  map[model.TableID]map[string]uint32
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
	q := fmt.Sprintf("SELECT MIN(%s)::bigint, MAX(%s)::bigint FROM %s", quotePGIdent(pkCol), quotePGIdent(pkCol), quotePGTable(table))
	var minV, maxV sql.NullInt64
	if err := c.DB.QueryRowContext(ctx, q).Scan(&minV, &maxV); err != nil {
		return nil, fmt.Errorf("failed to query table bounds: %w", err)
	}
	var b TableBounds
	if minV.Valid && maxV.Valid {
		b.MinPK, b.MaxPK, b.Count = minV.Int64, maxV.Int64, maxV.Int64-minV.Int64+1
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
func (c *PGChunkSource) ReadChunk(ctx context.Context, s *MySQLSnapshotter, chunk *Chunk, columnNames []string, pkIdx int,
	table model.TableID) ([]*model.RowChange, checkpoint.AppliedSet, error) {
	types, err := c.columnTypes(ctx, table)
	if err != nil {
		return nil, nil, fmt.Errorf("read column types of %s: %w", table, err)
	}
	// 1–3. "Đang đọc" không low → W → S0 thấy W.
	s.Manager.BeginReading(chunk)
	var xids []uint32
	if c.Tracker != nil {
		xids = c.Tracker.Pending()
	}
	low, err := c.waitSnapshot(ctx, xids)
	if err != nil {
		return nil, nil, fmt.Errorf("chunk low watermark: %w", err)
	}
	s.Manager.StartReadingPG(chunk, low)

	// 4. SELECT (mỗi lần một snapshot mới, đều sau S0).
	sel := make([]string, len(columnNames))
	for i, col := range columnNames {
		sel[i] = quotePGIdent(col) + "::text"
	}
	pk := quotePGIdent(columnNames[pkIdx])
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= $1 AND %s <= $2 ORDER BY %s",
		strings.Join(sel, ", "), quotePGTable(table), pk, pk, pk)
	queryFn := func(qCtx context.Context, ch *Chunk) ([]*model.RowChange, int64, error) {
		return c.queryChunk(qCtx, query, ch.StartPK, ch.EndPK, columnNames, pkIdx, types, table)
	}
	rows, err := s.Chunker.ReadChunkAdaptive(ctx, chunk, s.opts, queryFn)
	if err != nil {
		return nil, nil, fmt.Errorf("adaptive read failed for chunk [%d, %d]: %w", chunk.StartPK, chunk.EndPK, err)
	}

	// 5. High = insert LSN sau mọi SELECT, rồi watermark (record bắt đầu ở LSN ≥ high).
	var hs string
	if err := c.DB.QueryRowContext(ctx, "SELECT pg_current_wal_insert_lsn()::text").Scan(&hs); err != nil {
		return nil, nil, fmt.Errorf("read chunk high watermark: %w", err)
	}
	high, err := checkpoint.ParseLSN(hs)
	if err != nil {
		return nil, nil, err
	}
	if err := EmitPGWatermark(ctx, c.DB, c.ServerVersion, hs); err != nil {
		return nil, nil, err
	}
	return rows, checkpoint.NewPGAppliedSet(high), nil
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

func (c *PGChunkSource) queryChunk(ctx context.Context, query string, startPK, endPK int64, columnNames []string, pkIdx int,
	types map[string]uint32, table model.TableID) ([]*model.RowChange, int64, error) {
	rows, err := c.DB.QueryContext(ctx, query, startPK, endPK)
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
		if !vals[pkIdx].Valid {
			return nil, 0, fmt.Errorf("%s: NULL primary key", table)
		}
		pkv, err := strconv.ParseInt(vals[pkIdx].String, 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: primary key %q is not an integer: %w", table, vals[pkIdx].String, err)
		}
		rc.Key = []model.Value{pkv}
		out = append(out, rc)
	}
	return out, total, rows.Err()
}
