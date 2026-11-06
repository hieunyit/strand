package mysql

// Regression tests cho review N03: event binlog không hỗ trợ từng bị bỏ qua im lặng.
// Unit test thuần: đưa *replication.BinlogEvent tổng hợp vào handleEvent, không cần MySQL.

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

func n03Reader(t *testing.T) *MySQLReader {
	t.Helper()
	r, err := New(Config{Host: "127.0.0.1", Port: 3306, User: "x", ServerID: 9})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func n03Collect(r *MySQLReader, evs ...*replication.BinlogEvent) ([]reader.Event, bool) {
	st := &streamState{tableMap: make(map[uint64]*replication.TableMapEvent)}
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	for _, ev := range evs {
		if !r.handleEvent(st, ev, emit) {
			return got, false
		}
	}
	return got, true
}

func n03Ev(typ replication.EventType, logPos, size uint32, e replication.Event) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: typ, LogPos: logPos, EventSize: size, Timestamp: 1700000000},
		Event:  e,
	}
}

var n03SID = uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")

func n03GTID() *replication.BinlogEvent {
	return n03Ev(replication.GTID_EVENT, 500, 65, &replication.GTIDEvent{SID: n03SID[:], GNO: 7})
}

func n03TableMap() *replication.TableMapEvent {
	return &replication.TableMapEvent{TableID: 42, Schema: []byte("app"), Table: []byte("orders")}
}

// binlog_transaction_compression=ON: cả txn nằm trong TRANSACTION_PAYLOAD_EVENT. Trước fix không có
// EventRows nào được phát nhưng XID (nằm trong payload — hoặc txn kế tiếp) vẫn checkpoint => mất dữ liệu.
func TestN03TransactionPayloadInnerEventsAreProcessed(t *testing.T) {
	r := n03Reader(t)
	// MySQL ghi event con với LogPos=0 (go-mysql có thể stamp lại); handler không được phụ thuộc vào nó.
	payload := &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{
		n03Ev(replication.QUERY_EVENT, 0, 0, &replication.QueryEvent{Schema: []byte("app"), Query: []byte("BEGIN")}),
		n03Ev(replication.TABLE_MAP_EVENT, 0, 0, n03TableMap()),
		n03Ev(replication.WRITE_ROWS_EVENTv2, 0, 0, &replication.RowsEvent{TableID: 42, Rows: [][]any{{int64(1), "a"}}}),
		n03Ev(replication.UPDATE_ROWS_EVENTv2, 0, 0, &replication.RowsEvent{TableID: 42, Rows: [][]any{{int64(1), "a"}, {int64(1), "b"}}}),
		n03Ev(replication.DELETE_ROWS_EVENTv2, 0, 0, &replication.RowsEvent{TableID: 42, Rows: [][]any{{int64(1), "b"}}}),
		n03Ev(replication.XID_EVENT, 0, 0, &replication.XIDEvent{XID: 99}),
	}}
	got, ok := n03Collect(r, n03GTID(), n03Ev(replication.TRANSACTION_PAYLOAD_EVENT, 900, 400, payload))
	if !ok {
		t.Fatalf("handler stopped the stream: %+v", got)
	}
	wantTypes := []reader.EventType{reader.EventGTID, reader.EventQuery, reader.EventTableMap,
		reader.EventRows, reader.EventRows, reader.EventRows, reader.EventXID}
	if len(got) != len(wantTypes) {
		t.Fatalf("got %d events, want %d: %+v", len(got), len(wantTypes), got)
	}
	wantGTID := n03SID.String() + ":7"
	wantOps := []model.Op{model.OpInsert, model.OpUpdate, model.OpDelete}
	for i, e := range got {
		if e.Type != wantTypes[i] {
			t.Fatalf("event %d type = %v, want %v", i, e.Type, wantTypes[i])
		}
		if e.Pos.GTID != wantGTID {
			t.Errorf("event %d Pos.GTID = %q, want %q", i, e.Pos.GTID, wantGTID)
		}
		if e.Type == reader.EventRows {
			if e.Schema != "app" || e.Table != "orders" || e.TableID != 42 {
				t.Errorf("rows event %d table = %s.%s/%d", i, e.Schema, e.Table, e.TableID)
			}
			if e.Op != wantOps[i-3] {
				t.Errorf("rows event %d op = %v, want %v", i, e.Op, wantOps[i-3])
			}
		}
	}
	if got[0].BinlogPos != 435 {
		t.Errorf("GTID BinlogPos = %d, want 435", got[0].BinlogPos)
	}
	if got[6].XID != 99 {
		t.Errorf("XID = %d, want 99", got[6].XID)
	}
}

// binlog_row_value_options=PARTIAL_JSON: phải dừng với lỗi cấu hình (exit Rejected) nêu tên bảng,
// không được bỏ qua rồi để XID theo sau checkpoint txn rỗng.
func TestN03PartialUpdateRowsFailsClosed(t *testing.T) {
	r := n03Reader(t)
	got, ok := n03Collect(r,
		n03GTID(),
		n03Ev(replication.TABLE_MAP_EVENT, 600, 50, n03TableMap()),
		n03Ev(replication.PARTIAL_UPDATE_ROWS_EVENT, 700, 100, &replication.RowsEvent{TableID: 42, Rows: [][]any{{int64(1), "{}"}, {int64(1), "{}"}}}),
		n03Ev(replication.XID_EVENT, 731, 31, &replication.XIDEvent{XID: 5}),
	)
	if ok {
		t.Fatalf("handler did not stop on PARTIAL_UPDATE_ROWS_EVENT: %+v", got)
	}
	last := got[len(got)-1]
	if last.Type != reader.EventError || last.Err == nil {
		t.Fatalf("last event = %+v, want EventError", last)
	}
	for _, e := range got {
		if e.Type == reader.EventXID || e.Type == reader.EventRows {
			t.Fatalf("emitted %v after unsupported event", e.Type)
		}
	}
	if code := exitcode.GetExitCode(last.Err); code != exitcode.Rejected {
		t.Errorf("exit code = %d, want %d (Rejected)", code, exitcode.Rejected)
	}
	msg := last.Err.Error()
	for _, want := range []string{"app.orders", "binlog_row_value_options", "PARTIAL_JSON"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not mention %q", msg, want)
		}
	}
}

// Payload lồng nhau hoặc chưa giải mã: fail closed, không bỏ qua.
func TestN03UndecodedTransactionPayloadFailsClosed(t *testing.T) {
	r := n03Reader(t)
	got, ok := n03Collect(r, n03Ev(replication.TRANSACTION_PAYLOAD_EVENT, 900, 400, &replication.GenericEvent{}))
	if ok || len(got) != 1 || got[0].Type != reader.EventError {
		t.Fatalf("got ok=%v events=%+v, want a single EventError", ok, got)
	}
}
