package snapshot

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// spec: 02 §5, design §6.1
// Invariant: RS-1 (Target không bao giờ hiển thị một row cũ hơn trạng thái nó đã có)

// RangeStateManager điều phối trạng thái range cho các bảng trong quá trình snapshot song song với stream.
type RangeStateManager struct {
	mu     sync.RWMutex
	tables map[string]*TableRanges // schema.table -> TableRanges
}

// TableRanges quản lý tập các chunk/range của một bảng.
type TableRanges struct {
	Table        model.TableID
	PKCol        string
	TailBLast    int64 // b_last: max(pk) sau khi đọc low
	HasTailBLast bool
	Chunks       []*Chunk
}

// NewRangeStateManager khởi tạo RangeStateManager mới.
func NewRangeStateManager() *RangeStateManager {
	return &RangeStateManager{
		tables: make(map[string]*TableRanges),
	}
}

// RegisterTable đăng ký bảng và các chunk cần snapshot.
func (m *RangeStateManager) RegisterTable(table model.TableID, pkCol string, chunks []*Chunk, tailBLast ...int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tr := &TableRanges{
		Table:  table,
		PKCol:  pkCol,
		Chunks: chunks,
	}
	if len(tailBLast) > 0 {
		tr.TailBLast = tailBLast[0]
		tr.HasTailBLast = true
	}
	for _, c := range chunks {
		if c.BufferedChanges == nil {
			c.BufferedChanges = make(map[int64]*model.RowChange)
		}
	}
	m.tables[table.String()] = tr
}

// SetTailBLast thiết lập mốc max(pk) cho chunk đuôi [b_last, +inf).
func (m *RangeStateManager) SetTailBLast(table model.TableID, bLast int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tr, ok := m.tables[table.String()]
	if ok {
		tr.TailBLast = bLast
		tr.HasTailBLast = true
	}
}

// extractInt64PK trích xuất giá trị int64 của PK từ slice model.Value.
func extractInt64PK(key []model.Value) (int64, error) {
	if len(key) == 0 || key[0] == nil {
		return 0, fmt.Errorf("empty key")
	}
	switch v := key[0].(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported pk type %T", key[0])
	}
}

// FindChunk tìm chunk chứa PK tương ứng.
func (tr *TableRanges) FindChunk(pk int64) *Chunk {
	// Quy tắc chunk đuôi: nếu pk > TailBLast, thuộc range "Xong" ngay từ đầu
	if tr.HasTailBLast && pk > tr.TailBLast {
		return nil // Nil biểu thị ngoài phạm vi snapshot -> Done
	}
	for _, c := range tr.Chunks {
		if pk >= c.StartPK && pk <= c.EndPK {
			return c
		}
	}
	return nil
}

// HandleStreamEvent quyết định hành vi (Discard, Buffer, Apply) đối với một stream row change.
// spec: 02 §5, design §6.1
func (m *RangeStateManager) HandleStreamEvent(event *model.RowChange, pos model.Pos) (Action, *Chunk) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tr, ok := m.tables[event.Table.String()]
	if !ok {
		// Bảng không nằm trong snapshot -> Apply trực tiếp
		return ActionApply, nil
	}

	pk, err := extractInt64PK(event.Key)
	if err != nil {
		return ActionApply, nil
	}

	chunk := tr.FindChunk(pk)
	if chunk == nil {
		// Không thuộc bất kỳ chunk snapshot nào (ví dụ nằm sau b_last hoặc đã xong) -> Apply bình thường
		return ActionApply, nil
	}

	switch chunk.State {
	case StatePending:
		// spec: 02 §5 "Chưa đọc: Bỏ. Chunk đọc sau đó có read view mới hơn, nên đã thấy event này"
		return ActionDiscard, chunk

	case StateReading, StateRendezvous, StateFlushing:
		// spec: 02 §5 "Đang đọc / Chờ điểm hẹn: GTID ∈ low: bỏ. Ngược lại: gộp theo PK vào buffer có thứ tự, chỉ giữ image cuối"
		if chunk.LowWatermark != nil && chunk.LowWatermark.Contains(pos) {
			return ActionDiscard, chunk
		}
		// Gộp theo PK vào buffer
		if chunk.BufferedChanges == nil {
			chunk.BufferedChanges = make(map[int64]*model.RowChange)
		}
		chunk.BufferedChanges[pk] = event
		return ActionBuffer, chunk

	case StateDone:
		// Range đã hoàn thành -> Apply bình thường
		return ActionApply, chunk

	default:
		return ActionApply, chunk
	}
}

// StreamActionResult chứa kết quả xử lý của một stream event hoặc sub-event (sau khi tách đổi PK).
type StreamActionResult struct {
	Action Action
	Chunk  *Chunk
	Event  *model.RowChange
}

// SplitStreamEvent tách một row change thành hai nửa độc lập nếu đây là UPDATE đổi PK (OldKey != nil):
// 1. Tombstone (OpDelete) cho OldKey
// 2. Upsert (OpInsert) cho Key mới
// spec: 02 §5, §8, design §6.1
func SplitStreamEvent(event *model.RowChange) []*model.RowChange {
	if event == nil {
		return nil
	}
	if len(event.OldKey) == 0 {
		return []*model.RowChange{event}
	}

	tombstone := &model.RowChange{
		Table: event.Table,
		Op:    model.OpDelete,
		Key:   event.OldKey,
	}

	upsert := &model.RowChange{
		Table:       event.Table,
		Op:          model.OpInsert,
		Key:         event.Key,
		UniqueKeys:  event.UniqueKeys,
		ForeignKeys: event.ForeignKeys,
		Cols:        event.Cols,
	}

	return []*model.RowChange{tombstone, upsert}
}

// HandleStreamEventWithPKSplit xử lý một stream event, tự động tách thành tombstone và upsert nếu là UPDATE đổi PK.
func (m *RangeStateManager) HandleStreamEventWithPKSplit(event *model.RowChange, pos model.Pos) []StreamActionResult {
	subEvents := SplitStreamEvent(event)
	results := make([]StreamActionResult, 0, len(subEvents))
	for _, sub := range subEvents {
		action, chunk := m.HandleStreamEvent(sub, pos)
		results = append(results, StreamActionResult{
			Action: action,
			Chunk:  chunk,
			Event:  sub,
		})
	}
	return results
}

// StartReading chuyển trạng thái chunk sang StateReading kèm low watermark.
func (m *RangeStateManager) StartReading(chunk *Chunk, low checkpoint.AppliedSet) {
	m.mu.Lock()
	defer m.mu.Unlock()

	chunk.LowWatermark = low
	chunk.State = StateReading
}

// FinishReading chuyển trạng thái chunk sang StateRendezvous kèm high watermark và rows snapshot.
func (m *RangeStateManager) FinishReading(chunk *Chunk, high checkpoint.AppliedSet, rows []*model.RowChange) {
	m.mu.Lock()
	defer m.mu.Unlock()

	chunk.SnapshotRows = rows
	chunk.HighWatermark = high
	chunk.State = StateRendezvous
}

// MergeLocked thực hiện merge có thứ tự snapshot và buffer trong khi đã giữ lock.
func (m *RangeStateManager) MergeLocked(chunk *Chunk) []*model.RowChange {
	chunk.State = StateFlushing

	mergedMap := make(map[int64]*model.RowChange)

	// 1. Nạp snapshot rows
	for _, row := range chunk.SnapshotRows {
		pk, err := extractInt64PK(row.Key)
		if err == nil {
			mergedMap[pk] = row
		}
	}

	// 2. Gộp buffer (buffer override snapshot)
	for pk, bufChange := range chunk.BufferedChanges {
		if bufChange.Op == model.OpDelete {
			// Tombstone: xóa khỏi tập merge
			delete(mergedMap, pk)
		} else {
			// Update hoặc Insert mới: override giá trị snapshot
			mergedMap[pk] = bufChange
		}
	}

	// 3. Sắp xếp kết quả theo thứ tự PK
	var keys []int64
	for k := range mergedMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	var result []*model.RowChange
	for _, k := range keys {
		result = append(result, mergedMap[k])
	}

	return result
}

// MarkDoneLocked đánh dấu hoàn thành cho chunk và chuyển sang StateDone trong khi đã giữ lock.
func (m *RangeStateManager) MarkDoneLocked(chunk *Chunk) {
	chunk.State = StateDone
	chunk.SnapshotRows = nil
	chunk.BufferedChanges = make(map[int64]*model.RowChange)
}

// MergeAndFlush thực hiện merge có thứ tự row snapshot với buffer theo PK (phiên bản thread-safe).
func (m *RangeStateManager) MergeAndFlush(chunk *Chunk) []*model.RowChange {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.MergeLocked(chunk)
}

// MarkDone đánh dấu hoàn thành flush cho chunk và giải phóng bộ nhớ tạm (phiên bản thread-safe).
func (m *RangeStateManager) MarkDone(chunk *Chunk) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MarkDoneLocked(chunk)
}
