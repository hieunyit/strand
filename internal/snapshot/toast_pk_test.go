package snapshot_test

// spec: 02 §8 (đổi PK khi cột TOAST không đổi), §6 (max_pending_cross_range), design §6.1

import (
	"fmt"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

var toastTbl = model.TableID{Schema: "app", Table: "t"}

// toastRanges: A = [1,100] "Xong", B = [101,200] "Chưa đọc" (test chuyển sang "Đang đọc" khi cần),
// C = [201,300] "Chưa đọc"; b_last = 300 nên khóa > 300 thuộc range "Xong".
func toastRanges() (*snapshot.RangeStateManager, *snapshot.Chunk, *snapshot.Chunk, *snapshot.Chunk) {
	m := snapshot.NewRangeStateManager()
	a := &snapshot.Chunk{Table: toastTbl, PKCol: "id", StartPK: 1, EndPK: 100, State: snapshot.StateDone}
	b := &snapshot.Chunk{Table: toastTbl, PKCol: "id", StartPK: 101, EndPK: 200, State: snapshot.StatePending}
	c := &snapshot.Chunk{Table: toastTbl, PKCol: "id", StartPK: 201, EndPK: 300, State: snapshot.StatePending}
	m.RegisterTable(toastTbl, "id", []*snapshot.Chunk{a, b, c}, 300)
	return m, a, b, c
}

// toastMove: UPDATE đổi PK from → to, cột doc không đổi (pgoutput 'u').
func toastMove(from, to int64) *model.RowChange {
	return &model.RowChange{Table: toastTbl, Op: model.OpUpdate, Key: []model.Value{to}, OldKey: []model.Value{from},
		Cols: []model.ColValue{col("id", to), col("q", 1), unchanged("doc")}}
}

var pgPos = model.Pos{Engine: model.EnginePG, LSN: 1}

func actions(rs []snapshot.StreamActionResult) string {
	s := ""
	for _, r := range rs {
		s += fmt.Sprintf("%s:%s%v ", r.Action, r.Event.Op, r.Event.Key)
	}
	return s
}

func snapRow(pk int64, doc string) *model.RowChange {
	return &model.RowChange{Table: toastTbl, Op: model.OpInsert, Key: []model.Value{pk},
		Cols: []model.ColValue{col("id", pk), col("q", 0), col("doc", doc)}}
}

func mergedKeys(rows []*model.RowChange) map[int64]*model.RowChange {
	out := map[int64]*model.RowChange{}
	for _, r := range rows {
		out[r.Key[0].(int64)] = r
	}
	return out
}

// PK mới "Chưa đọc": chỉ xóa PK cũ theo trạng thái range của PK cũ; không chờ, không đọc lại.
func TestToastPK_NewKeyPending_OnlyOldKeyDelete(t *testing.T) {
	m, _, b, _ := toastRanges()
	// PK cũ "Xong": DELETE PK cũ được apply.
	rs, reread := m.HandleToastPKChangeXID(toastMove(5, 250), pgPos, 101, time.Now())
	if reread || actions(rs) != "Apply:delete[5] Discard:insert[250] " {
		t.Fatalf("old Done: %s reread=%v", actions(rs), reread)
	}
	// PK cũ "Chưa đọc": bỏ cả hai nửa.
	rs, reread = m.HandleToastPKChangeXID(toastMove(150, 250), pgPos, 102, time.Now())
	if reread || actions(rs) != "Discard:delete[150] Discard:insert[250] " {
		t.Fatalf("old Pending: %s reread=%v", actions(rs), reread)
	}
	// PK cũ "Đang đọc" (chưa có low): tombstone vào buffer.
	m.BeginReading(b)
	rs, reread = m.HandleToastPKChangeXID(toastMove(150, 250), pgPos, 103, time.Now())
	if reread || actions(rs) != "Buffer:delete[150] Discard:insert[250] " {
		t.Fatalf("old Reading: %s reread=%v", actions(rs), reread)
	}
	if n, _ := m.PendingCrossRange(time.Now()); n != 0 {
		t.Fatalf("pending cross-range ops %d, want 0", n)
	}
}

// Cả hai range đã "Xong": nguyên event đi tới writer (SQL điều kiện UPDATE … NOT EXISTS; DELETE).
func TestToastPK_BothDone_AppliesConditionalUpdate(t *testing.T) {
	m, _, _, _ := toastRanges()
	for _, mv := range [][2]int64{{5, 50}, {5, 1000}, {1000, 7}} {
		ev := toastMove(mv[0], mv[1])
		rs, reread := m.HandleToastPKChangeXID(ev, pgPos, 101, time.Now())
		if reread || len(rs) != 1 || rs[0].Action != snapshot.ActionApply || rs[0].Event != ev {
			t.Fatalf("move %v: %s reread=%v, want the original event applied", mv, actions(rs), reread)
		}
		if len(rs[0].Event.OldKey) == 0 || !snapshot.HasUnchanged(rs[0].Event) {
			t.Fatal("applied event must keep OldKey and the unchanged marker (writer partial update)")
		}
	}
}

// PK mới "Xong" mà PK cũ chưa "Xong": khóa mới được đánh dấu đọc lại; event sau của khóa mới bị bỏ
// cho tới khi chunk re-read đọc nó.
func TestToastPK_NewKeyDone_OldKeyNotDone_Rereads(t *testing.T) {
	m, _, b, _ := toastRanges()
	now := time.Now()
	rs, reread := m.HandleToastPKChangeXID(toastMove(250, 50), pgPos, 101, now)
	if !reread || actions(rs) != "Discard:delete[250] Discard:insert[50] " {
		t.Fatalf("old Pending/new Done: %s reread=%v", actions(rs), reread)
	}
	m.BeginReading(b)
	rs, reread = m.HandleToastPKChangeXID(toastMove(150, 2000), pgPos, 102, now)
	if !reread || actions(rs) != "Buffer:delete[150] Discard:insert[2000] " {
		t.Fatalf("old Reading/new tail: %s reread=%v", actions(rs), reread)
	}
	if got := fmt.Sprint(m.RereadKeys(toastTbl)); got != "[50 2000]" {
		t.Fatalf("reread keys %s", got)
	}
	// Event sau chạm khóa mới (ảnh đầy đủ, hoặc partial TOAST) bị bỏ: chunk re-read sẽ thấy nó.
	if a, _ := m.HandleStreamEventXID(pgRow(50, col("id", int64(50)), col("q", 2), col("doc", "x")), pgPos, 103); a != snapshot.ActionDiscard {
		t.Fatalf("later event on pending re-read key: %s, want Discard", a)
	}
	if a, _ := m.HandleStreamEventXID(pgRow(2000, col("id", int64(2000)), col("q", 2), unchanged("doc")), pgPos, 104); a != snapshot.ActionDiscard {
		t.Fatalf("later partial event on pending re-read key: %s, want Discard", a)
	}
	n, age := m.PendingCrossRange(now.Add(5 * time.Second))
	if n != 2 || age != 5*time.Second {
		t.Fatalf("pending cross-range %d oldest %s, want 2, 5s", n, age)
	}
}

// PK mới "Đang đọc": Txn visible với S0 → row snapshot đã có row mới (bỏ); chưa visible → đọc lại, và
// merge của chunk đang đọc bỏ qua khóa đó (kể cả row snapshot và ảnh đã gộp).
func TestToastPK_NewKeyReading(t *testing.T) {
	m, _, b, _ := toastRanges()
	m.BeginReading(b)
	// Ảnh của 160 gộp trước low (Txn 111): phải bị bỏ khi 160 được đánh dấu đọc lại.
	m.HandleStreamEventXID(pgRow(160, col("id", int64(160)), col("q", 7), col("doc", "old")), pgPos, 111)
	m.StartReadingPG(b, mustSnap(t, "100:110:105"))

	// Txn 102 visible với S0: bỏ, không đọc lại.
	rs, reread := m.HandleToastPKChangeXID(toastMove(5, 150), pgPos, 102, time.Now())
	if reread || actions(rs) != "Apply:delete[5] Discard:insert[150] " {
		t.Fatalf("visible txn: %s reread=%v", actions(rs), reread)
	}
	// Txn 105 (trong xip) chưa visible: đọc lại 160.
	rs, reread = m.HandleToastPKChangeXID(toastMove(6, 160), pgPos, 105, time.Now())
	if !reread || actions(rs) != "Apply:delete[6] Discard:insert[160] " {
		t.Fatalf("invisible txn: %s reread=%v", actions(rs), reread)
	}
	if _, ok := b.BufferedChanges[snapshot.EncodeInt64(160)]; ok {
		t.Fatal("buffered image of the re-read key must be dropped")
	}
	m.FinishReading(b, nil, []*model.RowChange{snapRow(150, "D150"), snapRow(160, "stale")})
	got := mergedKeys(m.MergeAndFlush(b))
	if _, ok := got[160]; ok {
		t.Fatal("chunk flush must not write a key waiting for a re-read")
	}
	if r := got[150]; r == nil || r.Cols[2].Value != "D150" {
		t.Fatalf("pk 150 = %+v, want snapshot row", r)
	}
}

// Cả PK cũ lẫn PK mới trong cùng range đang đọc.
func TestToastPK_BothInSameReadingRange(t *testing.T) {
	m, _, b, _ := toastRanges()
	m.BeginReading(b)
	m.StartReadingPG(b, mustSnap(t, "110:110:"))
	rs, reread := m.HandleToastPKChangeXID(toastMove(150, 170), pgPos, 111, time.Now())
	if !reread || actions(rs) != "Buffer:delete[150] Discard:insert[170] " {
		t.Fatalf("same range: %s reread=%v", actions(rs), reread)
	}
	m.FinishReading(b, nil, []*model.RowChange{snapRow(150, "D"), snapRow(180, "E")})
	got := mergedKeys(m.MergeAndFlush(b))
	if len(got) != 1 || got[180] == nil {
		t.Fatalf("merged %v, want only 180 (150 deleted, 170 re-read later)", got)
	}
	m.MarkDone(b)
	k := m.TakeReread(toastTbl, time.Now(), true)
	if k == nil || fmt.Sprint(k.Keys) != "[170]" {
		t.Fatalf("re-read chunk %+v, want keys [170]", k)
	}
}

// Khóa đổi qua đổi lại (5 → 150 → 5) khi range của 150 đang đọc; event sau trên khóa mới khi chunk
// re-read đang đọc được gộp như chunk thường; khóa được đánh dấu lại khi chunk re-read đang đọc thì
// chuyển sang chunk re-read kế tiếp.
func TestToastPK_BackAndForthAndLaterUpdates(t *testing.T) {
	m, a, b, _ := toastRanges()
	m.BeginReading(b)
	m.StartReadingPG(b, mustSnap(t, "110:110:"))
	// T1: 5 ("Xong") → 150 ("Đang đọc", chưa visible): xóa 5, đọc lại 150.
	if rs, reread := m.HandleToastPKChangeXID(toastMove(5, 150), pgPos, 111, time.Now()); !reread ||
		actions(rs) != "Apply:delete[5] Discard:insert[150] " {
		t.Fatalf("T1: %s", actions(rs))
	}
	// T2: 150 (chờ đọc lại) → 5 ("Xong"): PK cũ chưa xong → đọc lại 5; tombstone 150 bị bỏ.
	if rs, reread := m.HandleToastPKChangeXID(toastMove(150, 5), pgPos, 112, time.Now()); !reread ||
		actions(rs) != "Discard:delete[150] Discard:insert[5] " {
		t.Fatalf("T2: %s", actions(rs))
	}
	if got := fmt.Sprint(m.RereadKeys(toastTbl)); got != "[5 150]" {
		t.Fatalf("reread keys %s", got)
	}
	m.FinishReading(b, nil, []*model.RowChange{snapRow(150, "stale")})
	if got := m.MergeAndFlush(b); len(got) != 0 {
		t.Fatalf("chunk B merged %d rows, want 0 (150 re-read later)", len(got))
	}
	m.MarkDone(b)

	k := m.TakeReread(toastTbl, time.Now(), true)
	if k == nil || fmt.Sprint(k.Keys) != "[5 150]" {
		t.Fatalf("re-read chunk %+v", k)
	}
	if m.FindChunkForTest(toastTbl, 5) != k {
		t.Fatal("key 5 must belong to the re-read chunk")
	}
	m.BeginReading(k)
	m.StartReadingPG(k, mustSnap(t, "113:113:"))
	// Event sau S0 trên 5: partial TOAST → gộp, doc lấy từ row snapshot của chunk re-read.
	if a, _ := m.HandleStreamEventXID(pgRow(5, col("id", int64(5)), col("q", 9), unchanged("doc")), pgPos, 113); a != snapshot.ActionBuffer {
		t.Fatalf("event on key being re-read: %s, want Buffer", a)
	}
	// Khóa 150 bị chuyển tới lần nữa khi chunk re-read đang đọc (Txn chưa visible): sang chunk kế tiếp.
	if _, reread := m.HandleToastPKChangeXID(toastMove(7, 150), pgPos, 114, time.Now()); !reread {
		t.Fatal("re-marking a key being re-read must queue another re-read")
	}
	m.FinishReading(k, nil, []*model.RowChange{snapRow(5, "D5"), snapRow(150, "stale")})
	got := mergedKeys(m.MergeAndFlush(k))
	if len(got) != 1 || got[5] == nil || got[5].Cols[1].Value != 9 || got[5].Cols[2].Value != "D5" {
		t.Fatalf("re-read merge %v, want only 5 with q=9 doc=D5", got)
	}
	m.MarkDone(k)
	if got := fmt.Sprint(m.RereadKeys(toastTbl)); got != "[150]" {
		t.Fatalf("after re-read flush keys %s, want [150]", got)
	}
	if c := m.FindChunkForTest(toastTbl, 5); c != a {
		t.Fatalf("key 5 must be back in its Done range, got %+v", c)
	}
}

// max_pending_cross_range và luật 30 s: chunk re-read chỉ đến hạn khi đạt ngưỡng, chờ quá hạn, hoặc
// walk chính đã xong; mỗi chunk re-read tối đa max_pending_cross_range khóa, theo thứ tự đánh dấu.
func TestToastPK_TakeRereadCapAndMaxWait(t *testing.T) {
	m, _, _, _ := toastRanges()
	m.MaxPendingCrossRange = 3
	t0 := time.Now()
	m.HandleToastPKChangeXID(toastMove(250, 40), pgPos, 101, t0)
	m.HandleToastPKChangeXID(toastMove(251, 30), pgPos, 102, t0.Add(time.Second))
	if k := m.TakeReread(toastTbl, t0.Add(29*time.Second), false); k != nil {
		t.Fatal("2 keys < cap, oldest 29 s: not due yet")
	}
	if k := m.TakeReread(toastTbl, t0.Add(30*time.Second), false); k == nil || fmt.Sprint(k.Keys) != "[30 40]" {
		t.Fatalf("oldest waited 30 s: want re-read of [30 40], got %+v", k)
	}
	for i := int64(0); i < 4; i++ {
		m.HandleToastPKChangeXID(toastMove(260+i, 60+i), pgPos, 110+uint64(i), t0.Add(time.Duration(40+i)*time.Second))
	}
	k := m.TakeReread(toastTbl, t0.Add(40*time.Second), false)
	if k == nil || fmt.Sprint(k.Keys) != "[60 61 62]" || !k.Since.Equal(t0.Add(40*time.Second)) {
		t.Fatalf("cap reached: want first 3 keys, got %+v", k)
	}
	if k := m.TakeReread(toastTbl, t0.Add(44*time.Second), false); k != nil {
		t.Fatal("1 key left, waited 1 s: not due")
	}
	n, age := m.PendingCrossRange(t0.Add(50 * time.Second))
	// Đang chờ/đọc: 30, 40 (chunk thứ nhất), 60..62 (chunk thứ hai), 63 (chờ). Sớm nhất: t0.
	if n != 6 || age != 50*time.Second {
		t.Fatalf("pending %d oldest %s", n, age)
	}
	if k := m.TakeReread(toastTbl, t0.Add(44*time.Second), true); k == nil || fmt.Sprint(k.Keys) != "[63]" {
		t.Fatalf("force: want [63], got %+v", k)
	}
	m.PendingMaxWait = time.Millisecond
	m.HandleToastPKChangeXID(toastMove(270, 70), pgPos, 120, t0)
	if k := m.TakeReread(toastTbl, t0.Add(time.Millisecond), false); k == nil {
		t.Fatal("custom max wait")
	}
}

// Restart: khóa chờ đọc lại trong plan nằm ở phần "Chưa đọc" bị bỏ (walk chính đọc chúng), còn lại
// được nạp thành chunk re-read "Chưa đọc".
func TestToastPK_AddRereadFromPlan(t *testing.T) {
	m, _, _, _ := toastRanges()
	m.AddReread(toastTbl, []int64{5, 150, 250, 5000}, time.Now())
	if got := fmt.Sprint(m.RereadKeys(toastTbl)); got != "[5 5000]" {
		t.Fatalf("reread keys %s, want [5 5000]", got)
	}
	if a, _ := m.HandleStreamEventXID(pgRow(5000, col("id", int64(5000))), pgPos, 1); a != snapshot.ActionDiscard {
		t.Fatalf("event on restored re-read key: %s, want Discard", a)
	}
}
