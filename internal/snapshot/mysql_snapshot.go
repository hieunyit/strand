package snapshot

import (
	"context"
	"database/sql"
	"fmt"
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
	query := fmt.Sprintf("SELECT COALESCE(MIN(%s), 0), COALESCE(MAX(%s), 0), COUNT(*) FROM %s", pkCol, pkCol, table.String())
	var b TableBounds
	err := db.QueryRowContext(ctx, query).Scan(&b.MinPK, &b.MaxPK, &b.Count)
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
	if err := flushRowsToTarget(ctx, dstDB, table, pkCol, columnNames, mergedRows); err != nil {
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
	// 1. Quy tắc chunk đuôi [b_last, +inf):
	// spec: 02 §5: "Đọc max(pk) sau low; khóa lớn hơn thuộc range Xong ngay từ đầu"
	lowGTIDStr, err := GetGlobalGTIDExecuted(srcDB)
	if err != nil {
		return fmt.Errorf("failed to get low gtid: %w", err)
	}
	_ = ParseMySQLGTIDSet(lowGTIDStr)

	bounds, err := GetTableBounds(ctx, srcDB, table, pkCol)
	if err != nil {
		return err
	}

	if bounds.Count == 0 || bounds.MaxPK < bounds.MinPK {
		// Bảng trống: không có chunk nào cần đọc
		s.Manager.RegisterTable(table, pkCol, nil, 0)
		return nil
	}

	bLast := bounds.MaxPK

	// 2. Chia các chunk trong khoảng [MinPK, bLast]
	chunks := s.Chunker.PlanChunks(table, pkCol, bounds.MinPK, bLast, s.opts.DefaultChunkRows)
	s.Manager.RegisterTable(table, pkCol, chunks, bLast)

	// 3. Xử lý lần lượt từng chunk
	colsStr := strings.Join(columnNames, ", ")

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
				colsStr, table.String(), pkCol, pkCol, pkCol)
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
					Key:   []model.Value{colVals[0]}, // Giả định cột đầu tiên là PK
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
		if rendezvousWaitFn != nil {
			if err := rendezvousWaitFn(ctx, chunkHigh); err != nil {
				return fmt.Errorf("rendezvous wait failed: %w", err)
			}
		}

		// Flush: merge có thứ tự snapshot và buffer trong 1 transaction ở target và chuyển sang StateDone
		if err := s.FlushChunk(ctx, dstDB, chunk, table, pkCol, columnNames); err != nil {
			return fmt.Errorf("failed to flush chunk [%d, %d] to target: %w", chunk.StartPK, chunk.EndPK, err)
		}

		// Throttling nếu có
		if s.opts.SleepBetweenChunks > 0 {
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
) error {
	if len(rows) == 0 {
		return nil
	}

	tx, err := dstDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
				updateExprs = append(updateExprs, fmt.Sprintf("%s = VALUES(%s)", col, col))
			}
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			table.String(),
			strings.Join(columnNames, ", "),
			strings.Join(valPlaceholders, ", "),
			strings.Join(updateExprs, ", "),
		)

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("upsert flush batch failed: %w", err)
		}
	}

	return tx.Commit()
}
