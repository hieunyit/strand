package snapshot

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"strand/internal/model"
)

// spec: 02 §5, design §6.1

// Chunker cung cấp các tiện ích chia chunk, xử lý timeout và giới hạn byte theo spec.
type Chunker struct {
	splitCounter atomic.Int64 // Đếm số lần chia chunk do timeout hoặc byte vượt ngưỡng
}

// NewChunker tạo Chunker mới.
func NewChunker() *Chunker {
	return &Chunker{}
}

// SplitCount trả về số lần chia chunk đã diễn ra.
func (c *Chunker) SplitCount() int64 {
	return c.splitCounter.Load()
}

// PlanChunks tạo danh sách các chunk có độ lớn defaultRows trong khoảng [minPK, maxPK].
func (c *Chunker) PlanChunks(table model.TableID, pkCol string, minPK, maxPK int64, defaultRows int64) []*Chunk {
	if defaultRows <= 0 {
		defaultRows = 5000
	}
	if minPK > maxPK {
		return nil
	}

	var chunks []*Chunk
	var chunkID int64 = 1
	curr := minPK
	for curr <= maxPK {
		// Không tính curr+defaultRows-1 trực tiếp: tràn int64 khi curr gần MaxInt64.
		end := maxPK
		if uint64(maxPK)-uint64(curr) >= uint64(defaultRows) {
			end = curr + defaultRows - 1
		}
		chunks = append(chunks, &Chunk{
			ID:              chunkID,
			Table:           table,
			PKCol:           pkCol,
			StartPK:         curr,
			EndPK:           end,
			EstimatedRows:   SpanRows(curr, end),
			State:           StatePending,
			BufferedChanges: make(map[int64]*model.RowChange),
		})
		chunkID++
		if end >= maxPK {
			break // end == maxPK có thể là MaxInt64: end+1 tràn thành MinInt64 và lặp vô hạn
		}
		curr = end + 1
	}
	return chunks
}

// SplitChunk chia đôi một chunk thành hai sub-chunks [Start, Mid] và [Mid+1, End].
// Trả về false nếu không thể chia thêm (ví dụ chunk chỉ có <= MinChunkRows hàng).
func (c *Chunker) SplitChunk(chunk *Chunk, minRows int64) (*Chunk, *Chunk, bool) {
	if minRows <= 0 {
		minRows = 1
	}
	if chunk.RowCount() <= minRows || chunk.EndPK <= chunk.StartPK {
		return nil, nil, false
	}

	mid := MidPK(chunk.StartPK, chunk.EndPK)
	if mid >= chunk.EndPK {
		mid = chunk.StartPK
	}

	left := &Chunk{
		ID:              chunk.ID*10 + 1,
		Table:           chunk.Table,
		PKCol:           chunk.PKCol,
		StartPK:         chunk.StartPK,
		EndPK:           mid,
		EstimatedRows:   SpanRows(chunk.StartPK, mid),
		State:           chunk.State,
		LowWatermark:    chunk.LowWatermark,
		HighWatermark:   chunk.HighWatermark,
		BufferedChanges: make(map[int64]*model.RowChange),
	}

	right := &Chunk{
		ID:              chunk.ID*10 + 2,
		Table:           chunk.Table,
		PKCol:           chunk.PKCol,
		StartPK:         mid + 1,
		EndPK:           chunk.EndPK,
		EstimatedRows:   SpanRows(mid+1, chunk.EndPK),
		State:           chunk.State,
		LowWatermark:    chunk.LowWatermark,
		HighWatermark:   chunk.HighWatermark,
		BufferedChanges: make(map[int64]*model.RowChange),
	}

	c.splitCounter.Add(1)
	return left, right, true
}

// QueryChunkFunc là hàm thực hiện query SELECT cho một chunk.
// Trả về danh sách rows, kích thước bytes ước tính, và lỗi (nếu có).
type QueryChunkFunc func(ctx context.Context, chunk *Chunk) ([]*model.RowChange, int64, error)

// ReadChunkAdaptive đọc dữ liệu của một chunk và tự động thích ứng theo các quy tắc trong spec/02 §5:
//  1. Timeout SELECT của chunk (mặc định 5s): Hủy, chia đôi, chạy lại.
//     QUY TẮC BẮT BUỘC: Không áp timeout khi chunk chỉ còn 1 row (min_chunk_rows = 1).
//  2. max_chunk_bytes (mặc định 64MB): Vượt ngân sách thì chia đôi.
//     QUY TẮC BẮT BUỘC: Row lớn hơn max_chunk_bytes không gây vòng lặp chia chunk vô tận (chặn bởi min_chunk_rows = 1).
func (c *Chunker) ReadChunkAdaptive(
	ctx context.Context,
	chunk *Chunk,
	opts ChunkOptions,
	queryFn QueryChunkFunc,
) ([]*model.RowChange, error) {
	if opts.MinChunkRows <= 0 {
		opts.MinChunkRows = 1
	}
	if opts.MaxChunkBytes <= 0 {
		opts.MaxChunkBytes = 64 * 1024 * 1024
	}

	// Kiểm tra nếu chunk chỉ có <= min_chunk_rows:
	// "Không áp khi chunk chỉ còn 1 row" -> Chạy trực tiếp với parent ctx (không áp opts.Timeout)
	isSingleRow := chunk.RowCount() <= opts.MinChunkRows

	var queryCtx context.Context
	var cancel context.CancelFunc

	if isSingleRow || opts.Timeout <= 0 {
		queryCtx = ctx
		cancel = func() {}
	} else {
		queryCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
	}
	defer cancel()

	rows, totalBytes, err := queryFn(queryCtx, chunk)
	if err != nil {
		// Kiểm tra nếu lỗi do timeout của câu SELECT chunk
		if errors.Is(err, context.DeadlineExceeded) && !isSingleRow {
			// Hủy, chia đôi chunk, chạy lại
			left, right, ok := c.SplitChunk(chunk, opts.MinChunkRows)
			if ok {
				leftRows, errLeft := c.ReadChunkAdaptive(ctx, left, opts, queryFn)
				if errLeft != nil {
					return nil, errLeft
				}
				rightRows, errRight := c.ReadChunkAdaptive(ctx, right, opts, queryFn)
				if errRight != nil {
					return nil, errRight
				}
				return append(leftRows, rightRows...), nil
			}
		}
		return nil, fmt.Errorf("chunk read query failed (chunk [%d, %d]): %w", chunk.StartPK, chunk.EndPK, err)
	}

	// Kiểm tra điều kiện max_chunk_bytes
	if totalBytes > opts.MaxChunkBytes && !isSingleRow {
		// Kích thước chunk vượt max_chunk_bytes và còn có thể chia đôi
		left, right, ok := c.SplitChunk(chunk, opts.MinChunkRows)
		if ok {
			leftRows, errLeft := c.ReadChunkAdaptive(ctx, left, opts, queryFn)
			if errLeft != nil {
				return nil, errLeft
			}
			rightRows, errRight := c.ReadChunkAdaptive(ctx, right, opts, queryFn)
			if errRight != nil {
				return nil, errRight
			}
			return append(leftRows, rightRows...), nil
		}
	}

	// Nếu isSingleRow nhưng row lớn hơn max_chunk_bytes:
	// "Row lớn hơn max_chunk_bytes không gây vòng lặp":
	// Đã chạm min_chunk_rows = 1 nên không chia đôi tiếp, trả về row trực tiếp.
	return rows, nil
}
