package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// spec: 02 §5, design §6.1

// MySQLSnapshotter điều phối quá trình snapshot chunking và range-state cho MySQL source sang MySQL target.
type MySQLSnapshotter struct {
	Manager *RangeStateManager
	Chunker *Chunker
	opts    ChunkOptions

	// Barrier (tùy chọn) bao quanh bước đọc biên + đăng ký range + dọn target. Pipeline
	// truyền vào hàm dừng router và chờ mọi Txn đã nhận được apply xong, để không Txn cũ
	// nào của bảng còn nằm trong hàng đợi khi range-state bắt đầu có hiệu lực.
	Barrier func(ctx context.Context, fn func() error) error

	// TargetName (tùy chọn) ánh xạ tên bảng nguồn sang tên bảng đích (tables.rename).
	TargetName func(model.TableID) model.TableID

	// Pace (tùy chọn) được gọi giữa các chunk; governor dùng để giãn khoảng cách chunk.
	Pace func(ctx context.Context) error
}

// NewMySQLSnapshotter tạo mới MySQLSnapshotter.
func NewMySQLSnapshotter(opts ChunkOptions) *MySQLSnapshotter {
	return &MySQLSnapshotter{
		Manager: NewRangeStateManager(),
		Chunker: NewChunker(),
		opts:    opts,
	}
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
		b.MinPK, b.MaxPK, b.Count = minV.Int64, maxV.Int64, maxV.Int64-minV.Int64+1
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
	if err := flushRowsToTarget(ctx, dstDB, s.target(table), pkCol, columnNames, mergedRows, chunk.StartPK, chunk.EndPK); err != nil {
		return err
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
	pkIdx := -1
	for i, c := range columnNames {
		if c == pkCol {
			pkIdx = i
		}
	}
	if pkIdx < 0 {
		return fmt.Errorf("snapshot add %s: pk column %q not in column list", table, pkCol)
	}
	var err error
	if rendezvousWaitFn == nil {
		// Thiếu điểm hẹn thì chunk được flush trước khi stream xử lý hết high watermark (RS-1).
		return fmt.Errorf("snapshot add %s: rendezvous wait function is required", table)
	}
	var chunks []*Chunk
	register := func() error {
		// 1. Biên đọc sau khi barrier giữ router: khóa > b_last thuộc range "Xong" ngay từ đầu.
		bounds, err := GetTableBounds(ctx, srcDB, table, pkCol)
		if err != nil {
			return err
		}
		if bounds.Count == 0 || bounds.MaxPK < bounds.MinPK {
			s.Manager.RegisterTable(table, pkCol, nil, math.MinInt64)
			// Source rỗng: target cũng phải rỗng (dọn row thừa từ lần chạy trước).
			_, err := dstDB.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", quoteTable(s.target(table))))
			return err
		}
		// 2. Chia các chunk trong khoảng [MinPK, MaxPK]
		chunks = s.Chunker.PlanChunks(table, pkCol, bounds.MinPK, bounds.MaxPK, s.opts.DefaultChunkRows)
		s.Manager.RegisterTable(table, pkCol, chunks, bounds.MaxPK)
		// Row ở target nằm ngoài [min, max] không còn ở source tại thời điểm này.
		_, err = dstDB.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s < ? OR %s > ?",
			quoteTable(s.target(table)), quoteIdent(pkCol), quoteIdent(pkCol)), bounds.MinPK, bounds.MaxPK)
		return err
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

	for _, chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Watermark: mỗi chunk đọc low = @@GLOBAL.gtid_executed
		chunkLowStr, err := GetGlobalGTIDExecuted(srcDB)
		if err != nil {
			return fmt.Errorf("failed to read chunk low watermark: %w", err)
		}
		chunkLow := ParseMySQLGTIDSet(chunkLowStr)

		// Bắt đầu đọc chunk
		s.Manager.StartReading(chunk, chunkLow)

		// Đọc chunk có cơ chế adaptive split và timeout (spec 02 §5)
		queryFn := func(qCtx context.Context, c *Chunk) ([]*model.RowChange, int64, error) {
			query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? AND %s <= ? ORDER BY %s",
				colsStr, quoteTable(table), quoteIdent(pkCol), quoteIdent(pkCol), quoteIdent(pkCol))
			rows, err := srcDB.QueryContext(qCtx, query, c.StartPK, c.EndPK)
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
				rc := &model.RowChange{
					Table: table,
					Op:    model.OpInsert,
					Key:   []model.Value{colVals[pkIdx]},
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
			}
			if err := rows.Err(); err != nil {
				return nil, 0, err
			}
			return result, totalBytes, nil
		}

		rows, err := s.Chunker.ReadChunkAdaptive(ctx, chunk, s.opts, queryFn)
		if err != nil {
			return fmt.Errorf("adaptive read failed for chunk [%d, %d]: %w", chunk.StartPK, chunk.EndPK, err)
		}

		// Đọc high watermark
		chunkHighStr, err := GetGlobalGTIDExecuted(srcDB)
		if err != nil {
			return fmt.Errorf("failed to read chunk high watermark: %w", err)
		}
		chunkHigh := ParseMySQLGTIDSet(chunkHighStr)

		s.Manager.FinishReading(chunk, chunkHigh, rows)

		// Rendezvous: chờ stream xử lý hết high watermark
		if err := rendezvousWaitFn(ctx, chunkHigh); err != nil {
			return fmt.Errorf("rendezvous wait failed: %w", err)
		}

		// Flush: merge có thứ tự snapshot và buffer trong 1 transaction ở target và chuyển sang StateDone
		if err := s.FlushChunk(ctx, dstDB, chunk, table, pkCol, columnNames); err != nil {
			return fmt.Errorf("failed to flush chunk [%d, %d] to target: %w", chunk.StartPK, chunk.EndPK, err)
		}

		// Throttling nếu có
		if s.Pace != nil {
			if err := s.Pace(ctx); err != nil {
				return err
			}
		} else if s.opts.SleepBetweenChunks > 0 {
			time.Sleep(s.opts.SleepBetweenChunks)
		}
	}

	return nil
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
	// Kết nối riêng: tắt kiểm tra FK chỉ trong phiên flush (bảng con có thể được flush trước khi
	// Txn của row cha còn trong hàng đợi được apply; thứ tự stream vẫn do writeset bảo đảm).
	conn, err := dstDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SET SESSION foreign_key_checks = 1")
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Range [startPK, endPK] ở target phải đúng bằng ảnh đã merge: xóa row không còn ở source
	// (bị DELETE trong lúc range "Chưa đọc", hoặc còn sót từ lần snapshot trước khi crash),
	// cùng transaction với upsert nên target không bao giờ hiển thị trạng thái lùi (RS-1).
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s >= ? AND %s <= ?",
		quoteTable(table), quoteIdent(pkCol), quoteIdent(pkCol)), startPK, endPK); err != nil {
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
