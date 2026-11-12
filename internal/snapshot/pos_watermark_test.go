package snapshot

import (
	"context"
	"strings"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// invariant: RS-1', RS-2' (chế độ vị trí binlog)

type fakePos struct {
	log  *[]string
	base string
	low  uint64
	high uint64
}

func (f *fakePos) Low(ctx context.Context, t model.TableID) (checkpoint.AppliedSet, error) {
	*f.log = append(*f.log, "low:"+t.String())
	return checkpoint.NewMySQLPosAppliedSet(f.base, f.low), nil
}
func (f *fakePos) High(ctx context.Context) (checkpoint.AppliedSet, error) {
	*f.log = append(*f.log, "high")
	return checkpoint.NewMySQLPosAppliedSet(f.base, f.high), nil
}

// Thứ tự: low (dưới khóa) → StartReading → SELECT (không WAIT GTID, không đọc gtid_executed) → high.
func TestRS2Pos_OrderLowSelectHigh(t *testing.T) {
	s, chunk := rs2Snapshotter()
	src := &fakeSource{gtids: []string{"x"}}
	var log []string
	k := func(seq uint32, off uint64) uint64 { v, _ := checkpoint.BinlogKey(seq, off); return v }
	s.Pos = &fakePos{log: &log, base: "mysql-bin", low: k(1, 100), high: k(1, 300)}
	rows, high, err := s.readChunkWatermarked(context.Background(), src, chunk, newKeySQL(chunk.Table, nil), "`id`", []string{"id"}, []int{0}, chunk.Table)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != 1 || high.(*checkpoint.MySQLPosAppliedSet).Base() != k(1, 300) {
		t.Fatalf("rows=%d high=%v", len(rows.Rows), high)
	}
	if got := strings.Join(append(log, src.log...), "|"); got != "low:app.t|high|select@conn1" && got != "low:app.t|select@conn1|high" {
		// log của fakePos và fakeSource tách nhau: kiểm riêng từng thứ tự + không có GTID.
		t.Fatalf("unexpected: %s", got)
	}
	if strings.Join(log, "|") != "low:app.t|high" {
		t.Fatalf("pos order: %v", log)
	}
	if strings.Join(src.log, "|") != "select@conn1" {
		t.Fatalf("source must only run the SELECT (no GTID wait/read): %v", src.log)
	}
	// Chunk đang ở "Đang đọc" với low theo vị trí.
	if chunk.State != StateReading {
		t.Fatalf("state %v", chunk.State)
	}
}

// Quyết định inLow: event có vị trí KẾT THÚC ≤ low bị bỏ (đã ở read view); > low được gộp buffer (RS-2').
func TestInLowPos(t *testing.T) {
	k := func(seq uint32, off uint64) uint64 { v, _ := checkpoint.BinlogKey(seq, off); return v }
	chunk := &Chunk{LowWatermark: checkpoint.NewMySQLPosAppliedSet("mysql-bin", k(3, 500))}
	pos := func(f string, p uint64) model.Pos { return model.Pos{Engine: model.EngineMySQL, File: f, BinlogPos: p} }
	cases := []struct {
		p    model.Pos
		want bool
	}{
		{pos("mysql-bin.000003", 500), true}, // kết thúc đúng tại low
		{pos("mysql-bin.000003", 499), true},
		{pos("mysql-bin.000003", 501), false}, // sau low
		{pos("mysql-bin.000002", 9999), true}, // file trước
		{pos("mysql-bin.000004", 120), false}, // file sau (rotate)
		{pos("other-bin.000001", 4), false},   // khác tên gốc: không khẳng định
	}
	for _, c := range cases {
		if got := inLow(chunk, c.p, 0); got != c.want {
			t.Errorf("inLow(%s:%d)=%v want %v", c.p.File, c.p.BinlogPos, got, c.want)
		}
	}
}

// Range-state đầu-cuối: event trước low bị bỏ + xóa ảnh cũ trong buffer; event sau low vào buffer;
// event lúc "Chưa đọc" bị bỏ mà không ghi PendingDiscards (Pos không GTID).
func TestRangeStatePosFlow(t *testing.T) {
	s, chunk := rs2Snapshotter()
	k := func(seq uint32, off uint64) uint64 { v, _ := checkpoint.BinlogKey(seq, off); return v }
	pos := func(p uint64) model.Pos {
		return model.Pos{Engine: model.EngineMySQL, File: "mysql-bin.000001", BinlogPos: p}
	}
	ev := func(id int64, v string) *model.RowChange {
		return &model.RowChange{Table: chunk.Table, Op: model.OpUpdate, Key: []model.Value{id}, Cols: []model.ColValue{{Name: "v", Value: v}}}
	}
	if a, _ := s.Manager.HandleStreamEvent(ev(1, "a"), pos(50)); a != ActionDiscard {
		t.Fatalf("unread: %v", a)
	}
	if d := s.Manager.BeginReading(chunk); d.String() != "" {
		t.Fatalf("pos mode must not record PendingDiscards, got %q", d.String())
	}
	if a, _ := s.Manager.HandleStreamEvent(ev(2, "old"), pos(80)); a != ActionBuffer { // trước khi có low: buffer
		t.Fatalf("before low: %v", a)
	}
	s.Manager.StartReading(chunk, checkpoint.NewMySQLPosAppliedSet("mysql-bin", k(1, 100)))
	if a, _ := s.Manager.HandleStreamEvent(ev(2, "mid"), pos(90)); a != ActionDiscard { // ≤ low: bỏ + xóa ảnh cũ
		t.Fatalf("<= low: %v", a)
	}
	if _, ok := chunk.BufferedChanges[posKey(t, 2)]; ok {
		t.Fatal("stale buffered image of key 2 must be dropped")
	}
	if a, _ := s.Manager.HandleStreamEvent(ev(3, "new"), pos(130)); a != ActionBuffer {
		t.Fatalf("> low: %v", a)
	}
}

func posKey(t *testing.T, id int64) Key {
	t.Helper()
	tr := &TableRanges{}
	k, err := tr.encodeKey([]model.Value{id})
	if err != nil {
		t.Fatal(err)
	}
	return k
}
