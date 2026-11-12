package mysql

// Chế độ vị trí binlog của reader (source không GTID): thuần unit, đưa event tổng hợp vào handleEvent.

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

func posState(t *testing.T, file string) *streamState {
	t.Helper()
	st := &streamState{tableMap: make(map[uint64]*replication.TableMapEvent)}
	if err := st.initPosMode("mysql-bin", file); err != nil {
		t.Fatal(err)
	}
	return st
}

func posCollect(r *MySQLReader, st *streamState, evs ...*replication.BinlogEvent) ([]reader.Event, bool) {
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	for _, ev := range evs {
		if !r.handleEvent(st, ev, emit) {
			return got, false
		}
	}
	return got, true
}

func rotateEv(next string, pos uint64, logPos uint32) *replication.BinlogEvent {
	return n03Ev(replication.ROTATE_EVENT, logPos, 40, &replication.RotateEvent{NextLogName: []byte(next), Position: pos})
}

// LogPos là vị trí KẾT THÚC của event: Txn kết thúc tại LogPos của XID, bắt đầu tại LogPos−EventSize của
// event đầu tiên; file lấy từ ROTATE (kể cả rotate giả lúc kết nối, LogPos = 0).
func TestBinlogPosEventPositions(t *testing.T) {
	r := n03Reader(t)
	st := posState(t, "mysql-bin.000007")
	got, ok := posCollect(r, st,
		rotateEv("mysql-bin.000007", 4, 0), // rotate giả lúc kết nối
		n03Ev(replication.ANONYMOUS_GTID_EVENT, 220, 60, &replication.GTIDEvent{}),
		n03Ev(replication.QUERY_EVENT, 290, 70, &replication.QueryEvent{Schema: []byte("app"), Query: []byte("BEGIN")}),
		n03Ev(replication.TABLE_MAP_EVENT, 340, 50, n03TableMap()),
		n03Ev(replication.WRITE_ROWS_EVENTv2, 400, 60, &replication.RowsEvent{TableID: 42, Rows: [][]any{{int64(1), "a"}}}),
		n03Ev(replication.XID_EVENT, 431, 31, &replication.XIDEvent{XID: 9}),
	)
	if !ok {
		t.Fatalf("stream stopped: %+v", got)
	}
	// rotate + (anonymous gtid bị bỏ) + 5 event
	if len(got) != 5 {
		t.Fatalf("got %d events: %+v", len(got), got)
	}
	begin, xid := got[1], got[4]
	if begin.Type != reader.EventQuery || begin.BinlogFile != "mysql-bin.000007" || begin.BinlogPos != 220 {
		t.Errorf("BEGIN start = %s:%d, want mysql-bin.000007:220", begin.BinlogFile, begin.BinlogPos)
	}
	for i, e := range got[1:] {
		if e.Pos.File != "mysql-bin.000007" || e.Pos.GTID != "" || !e.Pos.IsBinlogPos() {
			t.Errorf("event %d pos = %+v", i, e.Pos)
		}
	}
	if xid.Type != reader.EventXID || xid.Pos.BinlogPos != 431 {
		t.Errorf("XID end = %d, want 431", xid.Pos.BinlogPos)
	}
	if r.Position().BinlogPos != 431 {
		t.Errorf("Position() = %+v", r.Position())
	}
}

func TestBinlogPosRotateMonotonic(t *testing.T) {
	r := n03Reader(t)
	st := posState(t, "mysql-bin.000007")
	// Tăng hợp lệ (kể cả nhảy số), event sau rotate mang file mới.
	got, ok := posCollect(r, st,
		rotateEv("mysql-bin.000008", 4, 900),
		n03Ev(replication.QUERY_EVENT, 220, 70, &replication.QueryEvent{Query: []byte("BEGIN")}),
		rotateEv("mysql-bin.000010", 4, 900),
		n03Ev(replication.XID_EVENT, 60, 31, &replication.XIDEvent{XID: 1}),
	)
	if !ok || got[1].Pos.File != "mysql-bin.000008" || got[3].Pos.File != "mysql-bin.000010" {
		t.Fatalf("rotate tracking: ok=%v %+v", ok, got)
	}
	// Giảm: RESET MASTER — fail closed.
	got, ok = posCollect(r, st, rotateEv("mysql-bin.000009", 4, 900))
	if ok || got[len(got)-1].Type != reader.EventError {
		t.Fatalf("backwards rotate accepted: %+v", got)
	}
	if !strings.Contains(got[len(got)-1].Err.Error(), "backwards") || exitcode.GetExitCode(got[len(got)-1].Err) != exitcode.Rejected {
		t.Errorf("error: %v", got[len(got)-1].Err)
	}
	// Đổi tên gốc.
	st = posState(t, "mysql-bin.000007")
	got, ok = posCollect(r, st, rotateEv("other-bin.000008", 4, 900))
	if ok || !strings.Contains(got[len(got)-1].Err.Error(), "base name") {
		t.Fatalf("base name change accepted: %+v", got)
	}
	// Tên không chuẩn.
	st = posState(t, "mysql-bin.000007")
	if _, ok = posCollect(r, st, rotateEv("mysql-bin.8", 4, 900)); ok {
		t.Fatal("non-canonical rotate name accepted")
	}
	// Rotate bằng file đang đọc (rotate giả lặp lại) hợp lệ.
	st = posState(t, "mysql-bin.000007")
	if _, ok = posCollect(r, st, rotateEv("mysql-bin.000007", 4, 0)); !ok {
		t.Fatal("same-file rotate rejected")
	}
}

func TestBinlogPosInitErrors(t *testing.T) {
	st := &streamState{}
	if err := st.initPosMode("mysql-bin", "weird"); err == nil {
		t.Error("bad resume file accepted")
	}
	if err := st.initPosMode("mysql-bin", "other.000001"); err == nil {
		t.Error("resume file with another base name accepted")
	}
}

// GTID_EVENT thật khi đang ở chế độ vị trí: source đã bật GTID giữa chừng — dừng.
func TestBinlogPosRealGTIDEventFailsClosed(t *testing.T) {
	r := n03Reader(t)
	st := posState(t, "mysql-bin.000007")
	got, ok := posCollect(r, st, n03GTID())
	if ok || got[len(got)-1].Type != reader.EventError || !strings.Contains(got[len(got)-1].Err.Error(), "binlog position mode") {
		t.Fatalf("GTID event accepted in position mode: %+v", got)
	}
}

// Txn nén (TRANSACTION_PAYLOAD_EVENT, LogPos con = 0): vị trí lấy từ event bao ngoài.
func TestBinlogPosPayloadUsesOuterPositions(t *testing.T) {
	r := n03Reader(t)
	st := posState(t, "mysql-bin.000003")
	payload := &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{
		n03Ev(replication.QUERY_EVENT, 0, 0, &replication.QueryEvent{Query: []byte("BEGIN")}),
		n03Ev(replication.XID_EVENT, 0, 0, &replication.XIDEvent{XID: 5}),
	}}
	got, ok := posCollect(r, st, n03Ev(replication.TRANSACTION_PAYLOAD_EVENT, 900, 400, payload))
	if !ok || len(got) != 2 {
		t.Fatalf("payload: ok=%v %+v", ok, got)
	}
	for _, e := range got {
		if e.BinlogFile != "mysql-bin.000003" || e.BinlogPos != 500 || e.Pos.BinlogPos != 900 {
			t.Errorf("inner event positions: start=%d end=%d", e.BinlogPos, e.Pos.BinlogPos)
		}
	}
}

// GTID mode không đổi: Pos vẫn là GTID, không có file/offset trong Pos của event.
func TestBinlogPosGTIDModeUnchanged(t *testing.T) {
	r := n03Reader(t)
	got, ok := n03Collect(r, n03GTID(), n03Ev(replication.XID_EVENT, 531, 31, &replication.XIDEvent{XID: 5}))
	if !ok || len(got) != 2 {
		t.Fatal(got)
	}
	for _, e := range got {
		if e.Pos.File != "" || e.Pos.BinlogPos != 0 || e.Pos.GTID == "" {
			t.Errorf("GTID mode event pos = %+v", e.Pos)
		}
	}
}

// Start ở chế độ vị trí cần tập không rỗng.
func TestBinlogPosStartRequiresPosition(t *testing.T) {
	r := n03Reader(t)
	_, err := r.Start(t.Context(), checkpoint.NewMySQLPosAppliedSet("mysql-bin", 0))
	if err == nil || exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Fatalf("empty position set accepted: %v", err)
	}
	var _ = model.Pos{}
}
