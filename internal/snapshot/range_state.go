package snapshot

import (
	"fmt"
	"math"
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

	// PendingDiscards: GTID của các event stream đã BỎ vì range còn "Chưa đọc". Ở MySQL, event
	// có trong binlog (stream thấy) TRƯỚC khi InnoDB commit xong (2PC: flush/sync binlog rồi mới
	// commit engine), nên SELECT của chunk đọc ngay sau đó có thể chưa thấy nó. Trước SELECT phải
	// chờ WAIT_FOR_EXECUTED_GTID_SET(tập này) để mọi event đã bỏ chắc chắn nằm trong read view.
	PendingDiscards *checkpoint.MySQLAppliedSet
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
	sort.Slice(tr.Chunks, func(i, j int) bool { return tr.Chunks[i].StartPK < tr.Chunks[j].StartPK })
	m.tables[table.String()] = tr
}

// RemoveTable gỡ bảng khỏi range-state sau khi snapshot xong (mọi event apply bình thường).
func (m *RangeStateManager) RemoveTable(table model.TableID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tables, table.String())
}

// CarveChunk cắt chunk [start, end] ra khỏi range "Chưa đọc" bắt đầu đúng ở start
// (phần còn lại [end+1, cũ.End] vẫn "Chưa đọc"). Dùng cho keyset walk.
func (m *RangeStateManager) CarveChunk(table model.TableID, start, end int64) (*Chunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tr, ok := m.tables[table.String()]
	if !ok {
		return nil, fmt.Errorf("table %s not registered", table)
	}
	for i, c := range tr.Chunks {
		if c.StartPK != start {
			continue
		}
		if c.State != StatePending || end < start || end > c.EndPK {
			return nil, fmt.Errorf("cannot carve [%d,%d] from chunk [%d,%d] in state %s", start, end, c.StartPK, c.EndPK, c.State)
		}
		if end == c.EndPK {
			return c, nil
		}
		rest := &Chunk{ID: c.ID + 1, Table: c.Table, PKCol: c.PKCol, StartPK: end + 1, EndPK: c.EndPK,
			State: StatePending, BufferedChanges: map[int64]*model.RowChange{}}
		c.EndPK = end
		c.EstimatedRows = end - start + 1
		tr.Chunks = append(tr.Chunks[:i+1], append([]*Chunk{rest}, tr.Chunks[i+1:]...)...)
		return c, nil
	}
	return nil, fmt.Errorf("no pending chunk starts at %d in %s", start, table)
}

// StateCounts trả về số range theo trạng thái của từng bảng đang snapshot (metric).
func (m *RangeStateManager) StateCounts() map[string]map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]map[string]int{}
	for name, tr := range m.tables {
		c := map[string]int{}
		for _, ch := range tr.Chunks {
			c[ch.State.String()]++
		}
		out[name] = c
	}
	return out
}

// TableCount trả về số bảng đang snapshot.
func (m *RangeStateManager) TableCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tables)
}

// IsRegistered cho biết bảng có đang được snapshot không.
func (m *RangeStateManager) IsRegistered(table model.TableID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.tables[table.String()]
	return ok
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
// Giá trị không biểu diễn chính xác được bằng int64 thì trả lỗi (không làm tròn, không tràn).
func extractInt64PK(key []model.Value) (int64, error) {
	if len(key) == 0 || key[0] == nil {
		return 0, fmt.Errorf("empty key")
	}
	switch v := key[0].(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return 0, fmt.Errorf("pk %d overflows int64", v)
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("pk %d overflows int64", v)
		}
		return int64(v), nil
	case float64:
		if v != math.Trunc(v) || v > math.MaxInt64 || v < math.MinInt64 {
			return 0, fmt.Errorf("pk %v is not an exact integer", v)
		}
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unsupported pk type %T", key[0])
	}
}

// FindChunk tìm chunk chứa PK tương ứng (tìm kiếm nhị phân; Chunks được sắp theo StartPK).
func (tr *TableRanges) FindChunk(pk int64) *Chunk {
	// Quy tắc chunk đuôi: nếu pk > TailBLast, thuộc range "Xong" ngay từ đầu
	if tr.HasTailBLast && pk > tr.TailBLast {
		return nil // Nil biểu thị ngoài phạm vi snapshot -> Done
	}
	i := sort.Search(len(tr.Chunks), func(k int) bool { return tr.Chunks[k].EndPK >= pk })
	if i < len(tr.Chunks) && pk >= tr.Chunks[i].StartPK {
		return tr.Chunks[i]
	}
	return nil
}

// HandleStreamEvent quyết định hành vi (Discard, Buffer, Apply) đối với một stream row change.
// spec: 02 §5, design §6.1
func (m *RangeStateManager) HandleStreamEvent(event *model.RowChange, pos model.Pos) (Action, *Chunk) {
	return m.HandleStreamEventXID(event, pos, 0)
}

// HandleStreamEventXID như HandleStreamEvent, kèm xid top-level của Txn (source PG; 0 với MySQL).
// Chunk có LowSnapshot (PG) bỏ event khi Txn visible với snapshot đó, ngược lại gộp theo PK.
func (m *RangeStateManager) HandleStreamEventXID(event *model.RowChange, pos model.Pos, xid uint64) (Action, *Chunk) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tr, ok := m.tables[event.Table.String()]
	if !ok {
		// Bảng không nằm trong snapshot -> Apply trực tiếp
		return ActionApply, nil
	}

	pk, err := extractInt64PK(event.Key)
	if err != nil {
		// Bảng đang snapshot nhưng không xác định được range: KHÔNG được apply thẳng (RS-1).
		return ActionReject, nil
	}

	chunk := tr.FindChunk(pk)
	if chunk == nil {
		// Không thuộc bất kỳ chunk snapshot nào (ví dụ nằm sau b_last hoặc đã xong) -> Apply bình thường
		return ActionApply, nil
	}

	switch chunk.State {
	case StatePending:
		// spec: 02 §5 "Chưa đọc: Bỏ. Chunk đọc sau đó có read view mới hơn, nên đã thấy event này".
		// Điều này chỉ đúng khi event đã commit ở engine: ghi lại GTID để BeginReading/chờ (xem PendingDiscards).
		if pos.GTID != "" {
			if tr.PendingDiscards == nil {
				tr.PendingDiscards = checkpoint.NewMySQLAppliedSet()
			}
			tr.PendingDiscards.Add(pos)
		}
		return ActionDiscard, chunk

	case StateReading, StateRendezvous, StateFlushing:
		// spec: 02 §5 "Đang đọc / Chờ điểm hẹn: GTID ∈ low: bỏ. Ngược lại: gộp theo PK vào buffer có thứ tự, chỉ giữ image cuối"
		// Source PG: "∈ low" là "Txn visible với snapshot S0 của chunk" (pg_watermark.go).
		inLow := chunk.LowWatermark != nil && chunk.LowWatermark.Contains(pos)
		if chunk.LowSnapshot != nil {
			inLow = chunk.LowSnapshot.Visible(xid)
		}
		if inLow {
			// Read view của chunk (sau low) đã phản ánh event này hoặc trạng thái mới hơn, nên mọi
			// ảnh CŨ HƠN của cùng PK còn trong buffer (vd. buffer lúc chunk chưa có low) đã lỗi thời:
			// phải bỏ cả nó, nếu không flush sẽ ghi ảnh cũ đè lên snapshot (vd. INSERT rồi DELETE ∈ low
			// → row "hồi sinh" ở target).
			delete(chunk.BufferedChanges, pk)
			return ActionDiscard, chunk
		}
		// Gộp theo PK vào buffer
		if chunk.BufferedChanges == nil {
			chunk.BufferedChanges = make(map[int64]*model.RowChange)
		}
		if prev, ok := chunk.BufferedChanges[pk]; ok && prev.Op != model.OpDelete && hasUnchanged(event) {
			// UPDATE của PG không gửi cột TOAST không đổi: giá trị của nó là giá trị của ảnh trước
			// (ảnh đang nằm trong buffer). Ảnh cuối phải đầy đủ để flush thay cả row.
			event = fillUnchanged(event, prev)
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
	return m.HandleStreamEventWithPKSplitXID(event, pos, 0)
}

// HandleStreamEventWithPKSplitXID như HandleStreamEventWithPKSplit, kèm xid top-level (source PG).
func (m *RangeStateManager) HandleStreamEventWithPKSplitXID(event *model.RowChange, pos model.Pos, xid uint64) []StreamActionResult {
	subEvents := SplitStreamEvent(event)
	results := make([]StreamActionResult, 0, len(subEvents))
	for _, sub := range subEvents {
		action, chunk := m.HandleStreamEventXID(sub, pos, xid)
		results = append(results, StreamActionResult{
			Action: action,
			Chunk:  chunk,
			Event:  sub,
		})
	}
	return results
}

// BeginReading chuyển chunk sang "Đang đọc" khi CHƯA có low watermark (mọi event sau đó được
// buffer, không bỏ) và trả về tập GTID các event đã bị bỏ của bảng. Bên đọc phải chờ tập đó
// visible trên kết nối đọc chunk (WAIT_FOR_EXECUTED_GTID_SET) trước SELECT, rồi gọi
// ConfirmVisible để thu gọn tập.
func (m *RangeStateManager) BeginReading(chunk *Chunk) *checkpoint.MySQLAppliedSet {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunk.LowWatermark = nil
	chunk.State = StateReading
	if tr, ok := m.tables[chunk.Table.String()]; ok && tr.PendingDiscards != nil {
		return tr.PendingDiscards.Clone().(*checkpoint.MySQLAppliedSet)
	}
	return checkpoint.NewMySQLAppliedSet()
}

// ConfirmVisible loại khỏi PendingDiscards các GTID đã được xác nhận visible (đã commit nên
// visible với mọi read view sau này).
func (m *RangeStateManager) ConfirmVisible(table model.TableID, visible *checkpoint.MySQLAppliedSet) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if tr, ok := m.tables[table.String()]; ok && tr.PendingDiscards != nil && visible != nil {
		tr.PendingDiscards.Subtract(visible)
	}
}

// StartReading chuyển trạng thái chunk sang StateReading kèm low watermark.
func (m *RangeStateManager) StartReading(chunk *Chunk, low checkpoint.AppliedSet) {
	m.mu.Lock()
	defer m.mu.Unlock()

	chunk.LowWatermark = low
	chunk.State = StateReading
}

// StartReadingPG gắn low = snapshot S0 (source PG) cho chunk đang "Đang đọc".
func (m *RangeStateManager) StartReadingPG(chunk *Chunk, low *PGSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunk.LowSnapshot = low
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
			if base, ok := mergedMap[pk]; ok && hasUnchanged(bufChange) {
				// Cột TOAST không đổi từ ảnh đầu tiên trong buffer trở đi: giá trị = giá trị của row
				// trước ảnh đó, mà row snapshot phản ánh (đọc sau S0, xem pg_snapshot.go).
				bufChange = fillUnchanged(bufChange, base)
			}
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

// HasUnchanged: row có cột TOAST không đổi (giá trị vắng mặt, chỉ source PG).
func HasUnchanged(rc *model.RowChange) bool { return hasUnchanged(rc) }

func hasUnchanged(rc *model.RowChange) bool {
	for _, c := range rc.Cols {
		if c.Unchanged {
			return true
		}
	}
	return false
}

// fillUnchanged trả bản sao rc với cột Unchanged lấy giá trị từ base (cùng tên cột); cột base không có
// thì giữ nguyên dấu Unchanged (bên flush sẽ từ chối).
func fillUnchanged(rc, base *model.RowChange) *model.RowChange {
	vals := make(map[string]model.ColValue, len(base.Cols))
	for _, c := range base.Cols {
		vals[c.Name] = c
	}
	out := *rc
	out.Cols = make([]model.ColValue, len(rc.Cols))
	for i, c := range rc.Cols {
		out.Cols[i] = c
		if c.Unchanged {
			if b, ok := vals[c.Name]; ok && !b.Unchanged {
				out.Cols[i] = model.ColValue{Name: c.Name, Value: b.Value}
			}
		}
	}
	return &out
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
