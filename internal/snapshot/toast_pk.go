package snapshot

// UPDATE đổi PK mà cột TOAST không đổi (source PG) khi bảng đang snapshot — 02 §8, design §6.1.
//
// Event có OldKey = o, Key = n và ít nhất một cột Unchanged: pgoutput không gửi giá trị cột TOAST, nên
// từ event không dựng được row n đầy đủ; giá trị đó chỉ nằm ở row o NGAY TRƯỚC Txn T của event. Event
// không bao giờ được gộp vào buffer như một ảnh, và không đọc điểm (point read) từ source.
//
// Quyết định theo trạng thái range chứa n (newC) và range chứa o (oldC) LÚC router xử lý event:
//
//  1. o và n đều "Xong" (gồm cả khóa sau b_last): target đã có row o đúng tại vị trí của T (mọi event
//     trước T của o đã được apply theo thứ tự, writeset gồm cả o lẫn n). Apply nguyên event: writer
//     chạy SQL điều kiện của 02 §8 trong transaction của T
//     UPDATE t SET pk = :new, <cột đổi> WHERE pk = :old AND NOT EXISTS (SELECT 1 FROM t WHERE pk = :new);
//     DELETE FROM t WHERE pk = :old;
//     (writer.pgApplier.partialUpdate). Không chờ gì vì cả hai range đã flush.
//  2. n "Chưa đọc": chỉ nửa tombstone của o đi tiếp, theo trạng thái range của o; nửa upsert bị bỏ. Chunk
//     đọc n sau này có S0 thấy T (xid của T đã nằm trong XIDTracker trước khi range-state quyết định,
//     pg_snapshot.go bước 2–3), nên mang về row n đầy đủ TOAST (hoặc trạng thái mới hơn).
//  3. n "Đang đọc"/"Chờ điểm hẹn" và T visible với S0 của chunk đó: row snapshot đã phản ánh T (và mọi
//     Txn trước T chạm n) — như 2, bỏ nửa upsert (cùng ảnh cũ hơn của n trong buffer).
//  4. Còn lại (n "Đang đọc" mà T chưa visible với S0, hoặc n "Xong" mà o chưa "Xong"): khóa n được
//     ĐÁNH DẤU ĐỌC LẠI — từ đây n thuộc một chunk re-read "Chưa đọc" (TableRanges.Reread), tức là quay
//     về trường hợp 2: nửa upsert bị bỏ, mọi event sau của n bị bỏ cho tới khi chunk re-read bắt đầu
//     đọc, rồi được gộp như mọi chunk. Nửa tombstone của o đi theo trạng thái range của o.
//
// Vì sao không dùng nguyên văn "chờ cả hai range flush rồi chạy SQL điều kiện" cho trường hợp 4:
//   - o "Chưa đọc" (hoặc "Đang đọc" mà T visible với S0 của nó): chunk của o đọc SAU T nên không còn row
//     o; khi cả hai range đã flush, UPDATE … WHERE pk = :old không khớp row nào và row n (range "Xong")
//     bị mất vĩnh viễn — không còn nguồn nào cho giá trị TOAST ngoài source.
//   - Event sau T chạm n khi n "Đang đọc" đã được gộp vào buffer và ghi bởi flush TRƯỚC thao tác chờ;
//     vd. T2 xóa n: flush làm n vắng mặt, rồi SQL điều kiện (NOT EXISTS đúng) dựng lại n → row "sống lại".
//
// Đọc lại n bằng giao thức chunk thì đúng theo đúng chứng minh của chunk thường (pg_snapshot.go): S0 thấy
// mọi Txn đã đi qua router trước khi chunk bắt đầu đọc (gồm T và mọi event của n đã bị bỏ), event sau S0
// được gộp, flush chạy trong barrier sau điểm hẹn nên target nhận đúng trạng thái của n tại vị trí router
// — không bao giờ là "trạng thái tương lai" vượt checkpoint (lý do 02 §8 cấm point read).
//
// Thứ tự với event sau (cùng khóa o hoặc n):
//   - n: trong lúc chờ, FindChunk trả chunk re-read nên mọi event của n bị bỏ/gộp như range chưa xong; sau
//     flush (Reread của n bị gỡ) event apply bình thường. Không event nào của n được apply trước khi row n
//     được dựng; row n không bao giờ được ghi hai lần cho cùng một phiên bản.
//   - o: xử lý như mọi event theo range của o (không phụ thuộc thao tác chờ).
//   - Chunk chính đang đọc chứa n: merge của nó bỏ qua n (MergeLocked) — flush xóa n ở target, chunk
//     re-read (luôn chạy SAU vì chunk được đọc tuần tự) ghi lại. Khóa được đánh dấu lại khi chunk re-read
//     của nó đang đọc: chuyển sang chunk re-read mới, chunk đang đọc bỏ qua nó theo cùng luật.
//
// Checkpoint (CP-1..5): T vẫn được Admit/commit như mọi Txn (CO-1/2 không đổi), nên applied set/lwd có
// thể vượt T trước khi n được đọc lại. Điều này an toàn vì nghĩa vụ đọc lại được lưu BỀN trong plan
// snapshot của bảng (SnapshotPlan.Reread, fsync) TRƯỚC khi T được Admit — giống hệt event bị bỏ ở range
// "Chưa đọc": checkpoint vượt Txn đó, nghĩa vụ đọc lại nằm trong plan ([NextPK, MaxPK]). Restart nạp lại
// các khóa này thành chunk re-read "Chưa đọc" (khóa nằm trong phần "Chưa đọc" của plan thì bỏ: walk chính
// đọc chúng). Mất file state thì mọi bảng được snapshot lại từ đầu (đúng).
//
// Ngân sách (max_pending_cross_range, 02 §6): thao tác chờ chỉ là một khóa (không giữ ảnh row nào), đã
// nằm trên đĩa trong plan, nên không có dữ liệu để spill. Đạt ngưỡng hoặc khóa sớm nhất chờ quá 30 s thì
// chunk re-read được đọc ngay ở ranh giới chunk kế tiếp (đầu hàng đợi đọc); mỗi chunk re-read đọc tối đa
// max_pending_cross_range khóa. Không chặn router (backpressure) vì sẽ khóa chết: chunk đang đọc chỉ flush
// được sau khi router đi qua high watermark của nó.

import (
	"sort"
	"time"

	"strand/internal/model"
)

func isDoneChunk(c *Chunk) bool { return c == nil || c.State == StateDone }

// HandleToastPKChangeXID xử lý UPDATE đổi PK (OldKey khác nil) có cột Unchanged của bảng đang snapshot
// (xem đầu file). Trả các phần như HandleStreamEventWithPKSplitXID (ActionApply = phần phải apply; có
// thể là nguyên event) và reread = true khi một khóa vừa được đánh dấu đọc lại: bên gọi phải lưu bền
// plan (RereadKeys) TRƯỚC khi Txn được Admit.
func (m *RangeStateManager) HandleToastPKChangeXID(event *model.RowChange, pos model.Pos, xid uint64, now time.Time) ([]StreamActionResult, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tr, ok := m.tables[event.Table.String()]
	if !ok {
		return []StreamActionResult{{Action: ActionApply, Event: event}}, false
	}
	oldPK, err1 := extractInt64PK(event.OldKey)
	newPK, err2 := extractInt64PK(event.Key)
	if err1 != nil || err2 != nil {
		return []StreamActionResult{{Action: ActionReject, Event: event}}, false
	}
	oldC, newC := tr.FindChunk(oldPK), tr.FindChunk(newPK)
	if isDoneChunk(oldC) && isDoneChunk(newC) {
		// 1. Cả hai range đã flush: SQL điều kiện của writer, trong transaction của T.
		return []StreamActionResult{{Action: ActionApply, Chunk: newC, Event: event}}, false
	}

	parts := SplitStreamEvent(event)
	tomb, ups := parts[0], parts[1]
	a, c := m.handleLocked(tr, tomb, pos, xid)
	res := []StreamActionResult{{Action: a, Chunk: c, Event: tomb}}

	reread := false
	switch {
	case newC != nil && newC.State == StatePending:
		// 2. Chunk đọc n sau này thấy T.
		a, c = m.handleLocked(tr, ups, pos, xid)
		res = append(res, StreamActionResult{Action: a, Chunk: c, Event: ups})
		return res, false
	case !isDoneChunk(newC) && inLow(newC, pos, xid):
		// 3. Row snapshot của chunk đang đọc đã phản ánh T: ảnh cũ hơn của n trong buffer cũng lỗi thời.
		delete(newC.BufferedChanges, newPK)
	default:
		// 4. Đọc lại n.
		m.markRereadLocked(tr, newPK, now)
		reread = true
	}
	return append(res, StreamActionResult{Action: ActionDiscard, Chunk: newC, Event: ups}), reread
}

// markRereadLocked đánh dấu khóa pk phải đọc lại (đã giữ m.mu).
func (m *RangeStateManager) markRereadLocked(tr *TableRanges, pk int64, now time.Time) {
	if tr.Reread == nil {
		tr.Reread = map[int64]*Chunk{}
	}
	cur := tr.Reread[pk]
	if cur != nil && cur.State == StatePending {
		// Đã chờ một chunk re-read chưa bắt đầu đọc: S0 của nó sẽ thấy Txn đang xử lý.
		return
	}
	if cur != nil {
		// Chunk re-read của pk đang đọc (Txn này chưa visible với S0 của nó): merge của nó bỏ qua pk.
		delete(cur.BufferedChanges, pk)
	}
	if mc := tr.findMain(pk); mc != nil && mc.State != StatePending && mc.State != StateDone {
		// Chunk chính đang đọc chứa pk: merge của nó bỏ qua pk (MergeLocked).
		delete(mc.BufferedChanges, pk)
	}
	if tr.rereadPending == nil {
		tr.rereadPending = &Chunk{Table: tr.Table, PKCol: tr.PKCol, State: StatePending, Keys: []int64{},
			BufferedChanges: map[int64]*model.RowChange{}, Since: now}
		tr.rereadSince = nil
	}
	tr.rereadPending.Keys = append(tr.rereadPending.Keys, pk)
	tr.rereadSince = append(tr.rereadSince, now)
	tr.Reread[pk] = tr.rereadPending
}

func (m *RangeStateManager) maxPending() int {
	if m.MaxPendingCrossRange > 0 {
		return m.MaxPendingCrossRange
	}
	return DefaultMaxPendingCrossRange
}

func (m *RangeStateManager) maxWait() time.Duration {
	if m.PendingMaxWait > 0 {
		return m.PendingMaxWait
	}
	return DefaultPendingMaxWait
}

// TakeReread trả chunk re-read kế tiếp của bảng ("Chưa đọc", tối đa max_pending_cross_range khóa, theo
// thứ tự đánh dấu) khi đến hạn: force (walk chính đã xong), số khóa chờ ≥ max_pending_cross_range, hoặc
// khóa sớm nhất đã chờ ≥ PendingMaxWait. nil = không có gì đến hạn.
func (m *RangeStateManager) TakeReread(table model.TableID, now time.Time, force bool) *Chunk {
	m.mu.Lock()
	defer m.mu.Unlock()
	tr, ok := m.tables[table.String()]
	if !ok || tr.rereadPending == nil || len(tr.rereadPending.Keys) == 0 {
		return nil
	}
	pc, limit := tr.rereadPending, m.maxPending()
	if !force && len(pc.Keys) < limit && now.Sub(tr.rereadSince[0]) < m.maxWait() {
		return nil
	}
	n := len(pc.Keys)
	if n > limit {
		n = limit
	}
	keys := append([]int64(nil), pc.Keys[:n]...)
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	k := &Chunk{ID: -1, Table: tr.Table, PKCol: tr.PKCol, StartPK: keys[0], EndPK: keys[len(keys)-1],
		State: StatePending, Keys: keys, Since: tr.rereadSince[0], BufferedChanges: map[int64]*model.RowChange{}}
	for _, key := range keys {
		tr.Reread[key] = k
	}
	if n == len(pc.Keys) {
		tr.rereadPending, tr.rereadSince = nil, nil
	} else {
		pc.Keys = append([]int64(nil), pc.Keys[n:]...)
		tr.rereadSince = append([]time.Time(nil), tr.rereadSince[n:]...)
		pc.Since = tr.rereadSince[0]
	}
	return k
}

// AddReread nạp lại các khóa chờ đọc lại từ plan (restart). Khóa thuộc phần "Chưa đọc" của walk chính
// bị bỏ qua: walk chính sẽ đọc chúng.
func (m *RangeStateManager) AddReread(table model.TableID, keys []int64, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tr, ok := m.tables[table.String()]
	if !ok {
		return
	}
	for _, k := range keys {
		if mc := tr.findMain(k); mc != nil && mc.State == StatePending {
			continue
		}
		m.markRereadLocked(tr, k, now)
	}
}

// RereadKeys trả (sắp tăng) mọi khóa của bảng còn chờ đọc lại hoặc đang đọc lại — tập phải lưu bền
// trong plan.
func (m *RangeStateManager) RereadKeys(table model.TableID) []int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tr, ok := m.tables[table.String()]
	if !ok || len(tr.Reread) == 0 {
		return nil
	}
	out := make([]int64, 0, len(tr.Reread))
	for k := range tr.Reread {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// PendingCrossRange trả số thao tác xuyên range đang chờ (khóa chờ/đang đọc lại, mọi bảng) và tuổi của
// thao tác chờ lâu nhất (metric strand_pending_cross_range, strand_pending_cross_range_oldest_seconds).
func (m *RangeStateManager) PendingCrossRange(now time.Time) (int, time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n := 0
	var oldest time.Time
	for _, tr := range m.tables {
		n += len(tr.Reread)
		for _, c := range tr.Reread {
			since := c.Since
			if c == tr.rereadPending && len(tr.rereadSince) > 0 {
				since = tr.rereadSince[0]
			}
			if !since.IsZero() && (oldest.IsZero() || since.Before(oldest)) {
				oldest = since
			}
		}
	}
	if n == 0 || oldest.IsZero() {
		return n, 0
	}
	return n, now.Sub(oldest)
}
