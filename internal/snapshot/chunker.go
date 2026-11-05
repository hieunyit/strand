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
			BufferedChanges: make(map[Key]*model.RowChange),
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
	if !codecOrInt(chunk.codec).IsSingleInt() {
		// Khóa tổng quát không có "điểm giữa" tính được trong Go: không chia đôi theo khóa (chunk của
		// keyset walk đã giới hạn theo số row; byte được giới hạn bằng cắt chunk, xem ReadChunkAdaptive).
		return nil, nil, false
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
		BufferedChanges: make(map[Key]*model.RowChange),
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
		BufferedChanges: make(map[Key]*model.RowChange),
	}

	left.codec, right.codec = chunk.codec, chunk.codec
	left.fillFromIntFields()
	right.fillFromIntFields()
	c.splitCounter.Add(1)
	return left, right, true
}

// QueryChunkFunc là hàm thực hiện query SELECT cho một chunk, theo thứ tự PK tăng dần. Khi maxBytes > 0,
// hàm PHẢI dừng quét ngay sau row làm tổng byte đạt maxBytes (các row đã trả phủ trọn [StartPK, pk của
// row cuối]) — để RAM giữ cho một chunk không vượt trần (review R10). maxBytes ≤ 0: không giới hạn.
// Trả về rows, tổng byte ước tính của rows, và lỗi.
type QueryChunkFunc func(ctx context.Context, chunk *Chunk, maxBytes int64) ([]*model.RowChange, int64, error)

// ChunkRead là kết quả ReadChunkAdaptive. Truncated: chỉ đọc được [chunk.StartPK, Cut] trong ngân sách
// max_chunk_bytes; phần [Cut+1, chunk.EndPK] chưa đọc và phải được tách thành chunk riêng
// (RangeStateManager.SplitReading) rồi đọc tiếp — KHÔNG ghép lại trong RAM.
type ChunkRead struct {
	Rows      []*model.RowChange
	Bytes     int64
	Cut       Bound
	CutPK     int64 // Cut của PK số nguyên (bản sao int64)
	Truncated bool
}

// ReadChunkAdaptive đọc dữ liệu của một chunk và tự động thích ứng theo các quy tắc trong spec/02 §5:
//  1. Timeout SELECT của chunk (mặc định 5s): Hủy, chia đôi, chạy lại.
//     QUY TẮC BẮT BUỘC: Không áp timeout khi chunk chỉ còn 1 row (min_chunk_rows = 1).
//  2. max_chunk_bytes (mặc định 64MB) là trần RAM của phần đã đọc (review R10): truy vấn dừng khi chạm
//     trần và kết quả bị cắt ở khóa cuối đã đọc (Truncated). Trần vượt tối đa một row: row lớn hơn
//     max_chunk_bytes vẫn được đọc (một mình), không gây vòng lặp.
func (c *Chunker) ReadChunkAdaptive(
	ctx context.Context,
	chunk *Chunk,
	opts ChunkOptions,
	queryFn QueryChunkFunc,
) (ChunkRead, error) {
	if opts.MinChunkRows <= 0 {
		opts.MinChunkRows = 1
	}
	if opts.MaxChunkBytes <= 0 {
		opts.MaxChunkBytes = 64 * 1024 * 1024
	}
	chunk.fillFromIntFields() // chunk dựng bằng API int64 (không qua range-state)
	return c.readBounded(ctx, chunk, opts, queryFn, opts.MaxChunkBytes)
}

func (c *Chunker) readBounded(ctx context.Context, chunk *Chunk, opts ChunkOptions, queryFn QueryChunkFunc, budget int64) (ChunkRead, error) {
	// Kiểm tra nếu chunk chỉ có <= min_chunk_rows:
	// "Không áp khi chunk chỉ còn 1 row" -> Chạy trực tiếp với parent ctx (không áp opts.Timeout)
	isSingleRow := chunk.RowCount() <= opts.MinChunkRows

	queryCtx, cancel := ctx, context.CancelFunc(func() {})
	if !isSingleRow && opts.Timeout > 0 {
		queryCtx, cancel = context.WithTimeout(ctx, opts.Timeout)
	}
	rows, totalBytes, err := queryFn(queryCtx, chunk, budget)
	cancel()
	if err != nil {
		// Timeout của câu SELECT chunk: chia đôi, đọc nửa trái rồi nửa phải với phần ngân sách còn lại.
		if errors.Is(err, context.DeadlineExceeded) && !isSingleRow && ctx.Err() == nil {
			if left, right, ok := c.SplitChunk(chunk, opts.MinChunkRows); ok {
				l, err := c.readBounded(ctx, left, opts, queryFn, budget)
				if err != nil {
					return ChunkRead{}, err
				}
				if l.Truncated {
					return l, nil
				}
				if l.Bytes >= budget {
					return ChunkRead{Rows: l.Rows, Bytes: l.Bytes, Cut: left.Hi, CutPK: left.EndPK, Truncated: true}, nil
				}
				r, err := c.readBounded(ctx, right, opts, queryFn, budget-l.Bytes)
				if err != nil {
					return ChunkRead{}, err
				}
				return ChunkRead{Rows: append(l.Rows, r.Rows...), Bytes: l.Bytes + r.Bytes, Cut: r.Cut, CutPK: r.CutPK, Truncated: r.Truncated}, nil
			}
		}
		return ChunkRead{}, fmt.Errorf("chunk read query failed (chunk [%d, %d]): %w", chunk.StartPK, chunk.EndPK, err)
	}
	out := ChunkRead{Rows: rows, Bytes: totalBytes, Cut: chunk.Hi, CutPK: chunk.EndPK}
	if budget > 0 && totalBytes >= budget && len(rows) > 0 {
		codec := codecOrInt(chunk.codec)
		vals := rows[len(rows)-1].Key
		k, err := codec.Encode(vals)
		if err != nil {
			return ChunkRead{}, fmt.Errorf("chunk [%x, %x]: %w", chunk.Lo.K, chunk.Hi.K, err)
		}
		if k < chunk.Hi.K {
			out.Cut, out.Truncated = Bound{K: k, V: append([]model.Value(nil), vals...)}, true
			if codec.IsSingleInt() {
				out.CutPK, _ = toInt64(vals[0])
				out.Cut = IntBound(out.CutPK)
			}
		}
	}
	return out, nil
}
