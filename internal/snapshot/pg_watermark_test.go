package snapshot_test

import (
	"testing"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// spec: 02 §5 (watermark của source PG), design §6.1

func mustSnap(t *testing.T, s string) *snapshot.PGSnapshot {
	t.Helper()
	ps, err := snapshot.ParsePGSnapshot(s)
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

func TestPGSnapshot_ParseAndVisible(t *testing.T) {
	s := mustSnap(t, "100:110:102,105")
	if s.String() != "100:110:102,105" {
		t.Fatalf("round trip: %s", s)
	}
	cases := []struct {
		xid  uint64
		want bool
	}{
		{0, false},   // không biết xid → không bao giờ coi là visible (gộp, an toàn)
		{99, true},   // < xmin: đã kết thúc
		{100, true},  // = xmin, không trong xip (xmin luôn là xid đang chạy nhỏ nhất, ở đây coi như đã xong)
		{102, false}, // trong xip: đang chạy lúc lấy snapshot
		{105, false},
		{109, true},
		{110, false}, // ≥ xmax: commit sau snapshot
		{200, false},
	}
	for _, c := range cases {
		if got := s.Visible(c.xid); got != c.want {
			t.Errorf("Visible(%d) = %v, want %v", c.xid, got, c.want)
		}
	}
	for _, bad := range []string{"", "1:2", "a:2:", "1:b:", "1:2:x"} {
		if _, err := snapshot.ParsePGSnapshot(bad); err == nil {
			t.Errorf("ParsePGSnapshot(%q) must fail", bad)
		}
	}
}

// xid 32 bit của pgoutput được đổi sang xid8 theo epoch của snapshot (kể cả khi vừa qua wraparound).
func TestPGSnapshot_EpochMapping(t *testing.T) {
	const epoch1 = uint64(1) << 32
	// Snapshot ngay sau wraparound: xmax = epoch 1, xid 5.
	s := mustSnap(t, "4294967290:4294967306:4294967295")
	// 4294967290 = epoch0:0xFFFFFFFA; 4294967306 = epoch1:10; xip = epoch0:0xFFFFFFFF.
	if !s.Visible(0xFFFFFFFB) { // epoch 0, ≥ xmin, không trong xip, < xmax
		t.Error("xid 0xFFFFFFFB (epoch 0) must be visible")
	}
	if s.Visible(0xFFFFFFFF) { // trong xip
		t.Error("xid 0xFFFFFFFF in xip must not be visible")
	}
	if !s.Visible(3) { // epoch 1, 3 < 10
		t.Error("xid 3 (epoch 1) must be visible")
	}
	if s.Visible(11) { // epoch 1, ≥ xmax
		t.Error("xid 11 (epoch 1) must not be visible")
	}
	if !s.Visible(epoch1 + 9) { // xid8 đầy đủ
		t.Error("xid8 epoch1+9 must be visible")
	}
}

func TestPGSnapshot_Covers(t *testing.T) {
	e := mustSnap(t, "100:110:103,107") // E thấy 100..109 trừ 103, 107
	cases := []struct {
		s    string
		want bool
	}{
		{"100:110:103,107", true},
		{"103:112:103,107,111", true}, // còn 103/107 chạy (E cũng không thấy) → vẫn phủ
		{"100:109:", false},           // xmax nhỏ hơn: 109 visible với E nhưng chưa chắc với S
		{"100:115:104", false},        // 104 visible với E nhưng còn trong xip của S
		{"99:115:99", false},          // 99 < xmin của E (đã xong theo E) nhưng S còn thấy chạy
		{"108:120:", true},
	}
	for _, c := range cases {
		if got := mustSnap(t, c.s).Covers(e); got != c.want {
			t.Errorf("%s Covers(%s) = %v, want %v", c.s, e, got, c.want)
		}
	}
}

func TestXIDTracker_Prune(t *testing.T) {
	tr := snapshot.NewXIDTracker()
	for _, x := range []uint64{0, 101, 102, 103, 120} {
		tr.Add(x)
	}
	if tr.Len() != 4 {
		t.Fatalf("len %d, want 4 (xid 0 ignored)", tr.Len())
	}
	pending := tr.Pending()
	s := mustSnap(t, "102:110:102,103")
	if snapshot.AllVisible(s, pending) {
		t.Fatal("102/103/120 are not visible yet")
	}
	if left := tr.Prune(s); left != 3 {
		t.Fatalf("after prune %d left, want 3 (102, 103, 120)", left)
	}
	s2 := mustSnap(t, "130:130:")
	if !snapshot.AllVisible(s2, pending) || tr.Prune(s2) != 0 {
		t.Fatal("everything is visible to 130:130:")
	}
}

func pgRow(pk int64, cols ...model.ColValue) *model.RowChange {
	return &model.RowChange{Table: model.TableID{Schema: "app", Table: "t"}, Op: model.OpUpdate, Key: []model.Value{pk}, Cols: cols}
}

func col(n string, v any) model.ColValue { return model.ColValue{Name: n, Value: v} }
func unchanged(n string) model.ColValue  { return model.ColValue{Name: n, Unchanged: true} }

func pgChunk(m *snapshot.RangeStateManager) *snapshot.Chunk {
	tbl := model.TableID{Schema: "app", Table: "t"}
	c := &snapshot.Chunk{Table: tbl, PKCol: "id", StartPK: 1, EndPK: 100, State: snapshot.StatePending}
	m.RegisterTable(tbl, "id", []*snapshot.Chunk{c}, 100)
	return c
}

// Quyết định của range-state với low là snapshot MVCC (source PG).
func TestRS_PG_LowSnapshotDecisions(t *testing.T) {
	m := snapshot.NewRangeStateManager()
	c := pgChunk(m)
	pos := model.Pos{Engine: model.EnginePG, LSN: 1000}

	// "Chưa đọc": bỏ (Txn sẽ được chờ visible trước SELECT nhờ XIDTracker).
	if a, _ := m.HandleStreamEventXID(pgRow(5, col("v", "p")), pos, 50); a != snapshot.ActionDiscard {
		t.Fatalf("pending: %s, want Discard", a)
	}
	// "Đang đọc" chưa có low: gộp mọi thứ, kể cả Txn sẽ visible.
	m.BeginReading(c)
	if a, _ := m.HandleStreamEventXID(pgRow(7, col("v", "pre-low")), pos, 101); a != snapshot.ActionBuffer {
		t.Fatalf("reading without low: %s, want Buffer", a)
	}
	m.StartReadingPG(c, mustSnap(t, "100:110:105"))
	// Txn visible với S0 (101 < xmax, không trong xip): bỏ, kèm ảnh cũ hơn của cùng PK.
	if a, _ := m.HandleStreamEventXID(pgRow(7, col("v", "visible")), pos, 102); a != snapshot.ActionDiscard {
		t.Fatalf("visible txn: %s, want Discard", a)
	}
	if _, ok := c.BufferedChanges[snapshot.EncodeInt64(7)]; ok {
		t.Fatal("older buffered image of pk 7 must be dropped when a later visible event is discarded")
	}
	// Txn trong xip (commit record đã stream nhưng chưa visible lúc S0): gộp.
	if a, _ := m.HandleStreamEventXID(pgRow(8, col("v", "in-xip")), pos, 105); a != snapshot.ActionBuffer {
		t.Fatalf("xid in xip: %s, want Buffer", a)
	}
	// Txn commit sau S0: gộp.
	if a, _ := m.HandleStreamEventXID(pgRow(9, col("v", "after")), pos, 111); a != snapshot.ActionBuffer {
		t.Fatalf("xid >= xmax: %s, want Buffer", a)
	}
	// xid không biết: gộp (an toàn).
	if a, _ := m.HandleStreamEventXID(pgRow(10, col("v", "noxid")), pos, 0); a != snapshot.ActionBuffer {
		t.Fatalf("xid 0: %s, want Buffer", a)
	}
}

// Race bắt buộc phải đúng: Txn T đã có commit record (commit LSN < LSN đọc ngay trước SELECT, và đã
// được stream) nhưng CHƯA ProcArrayEndTransaction khi SELECT lấy snapshot. Low theo LSN sẽ bỏ event
// của T dù row snapshot không có T → mất dữ liệu. Low theo snapshot (T ∈ xip) gộp T và flush ghi ảnh
// của T đè lên row snapshot.
func TestRS_PG_StreamedButInvisibleTxnIsKept(t *testing.T) {
	m := snapshot.NewRangeStateManager()
	c := pgChunk(m)
	m.BeginReading(c)
	m.StartReadingPG(c, mustSnap(t, "200:210:204")) // T có xid 204 đang chạy với S0
	snapRows := []*model.RowChange{
		{Table: c.Table, Op: model.OpInsert, Key: []model.Value{int64(3)}, Cols: []model.ColValue{col("id", int64(3)), col("v", "old")}},
	}
	// SELECT (sau S0, T vẫn chưa visible) thấy v=old.
	m.FinishReading(c, nil, snapRows)
	// T được stream với commit LSN nhỏ (đã nằm dưới mọi "LSN trước SELECT").
	if a, _ := m.HandleStreamEventXID(pgRow(3, col("id", int64(3)), col("v", "new")), model.Pos{Engine: model.EnginePG, LSN: 10}, 204); a != snapshot.ActionBuffer {
		t.Fatalf("streamed-but-invisible txn: %s, want Buffer", a)
	}
	merged := m.MergeAndFlush(c)
	if len(merged) != 1 || merged[0].Cols[1].Value != "new" {
		t.Fatalf("merged %+v, want v=new", merged)
	}
}

// Cột TOAST không đổi: ảnh gộp lấy giá trị từ ảnh trước trong buffer, hoặc từ row snapshot.
func TestRS_PG_ToastUnchangedMerge(t *testing.T) {
	m := snapshot.NewRangeStateManager()
	c := pgChunk(m)
	m.BeginReading(c)
	m.StartReadingPG(c, mustSnap(t, "300:300:"))
	pos := model.Pos{Engine: model.EnginePG, LSN: 1}
	// pk 4: hai UPDATE sau S0, cái đầu đổi doc, cái sau không đổi doc → doc lấy từ ảnh đầu.
	m.HandleStreamEventXID(pgRow(4, col("id", int64(4)), col("q", 1), col("doc", "D2")), pos, 301)
	m.HandleStreamEventXID(pgRow(4, col("id", int64(4)), col("q", 2), unchanged("doc")), pos, 302)
	// pk 5: một UPDATE không đổi doc, buffer chưa có ảnh → doc lấy từ row snapshot.
	m.HandleStreamEventXID(pgRow(5, col("id", int64(5)), col("q", 9), unchanged("doc")), pos, 303)
	// pk 6: UPDATE không đổi doc nhưng không có row snapshot → giữ dấu Unchanged (bên flush từ chối).
	m.HandleStreamEventXID(pgRow(6, col("id", int64(6)), col("q", 9), unchanged("doc")), pos, 304)
	m.FinishReading(c, nil, []*model.RowChange{
		{Table: c.Table, Op: model.OpInsert, Key: []model.Value{int64(4)}, Cols: []model.ColValue{col("id", int64(4)), col("q", 0), col("doc", "D1")}},
		{Table: c.Table, Op: model.OpInsert, Key: []model.Value{int64(5)}, Cols: []model.ColValue{col("id", int64(5)), col("q", 0), col("doc", "S5")}},
	})
	got := map[int64]*model.RowChange{}
	for _, r := range m.MergeAndFlush(c) {
		got[r.Key[0].(int64)] = r
	}
	if r := got[4]; r.Cols[1].Value != 2 || r.Cols[2].Value != "D2" || r.Cols[2].Unchanged {
		t.Fatalf("pk 4 = %+v, want q=2 doc=D2", r.Cols)
	}
	if r := got[5]; r.Cols[1].Value != 9 || r.Cols[2].Value != "S5" || r.Cols[2].Unchanged {
		t.Fatalf("pk 5 = %+v, want q=9 doc=S5 (from snapshot row)", r.Cols)
	}
	if r := got[6]; !r.Cols[2].Unchanged {
		t.Fatalf("pk 6 = %+v, doc must stay Unchanged (no base image)", r.Cols)
	}
}

// MySQL không đổi: chunk không có LowSnapshot vẫn dùng GTID ∈ low.
func TestRS_MySQLPathUnaffectedByXID(t *testing.T) {
	m := snapshot.NewRangeStateManager()
	c := pgChunk(m)
	m.BeginReading(c)
	low := snapshot.ParseMySQLGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10")
	m.StartReading(c, low)
	in := model.Pos{Engine: model.EngineMySQL, GTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:5"}
	out := model.Pos{Engine: model.EngineMySQL, GTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:11"}
	if a, _ := m.HandleStreamEvent(pgRow(1, col("v", 1)), in); a != snapshot.ActionDiscard {
		t.Fatalf("gtid in low: %s", a)
	}
	if a, _ := m.HandleStreamEvent(pgRow(1, col("v", 2)), out); a != snapshot.ActionBuffer {
		t.Fatalf("gtid not in low: %s", a)
	}
}
