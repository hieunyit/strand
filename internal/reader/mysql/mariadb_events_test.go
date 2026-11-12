package mysql

// Event binlog riêng của MariaDB 10.6 (flavor mariadb, chế độ vị trí binlog). Unit test thuần trên event
// tổng hợp; luồng thật MariaDB → MySQL 8.4 kiểm ở tests/e2e/mariadb_test.go.

import (
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/exitcode"
	"strand/internal/reader"
)

func mdbCollect(t *testing.T, posMode bool, evs ...*replication.BinlogEvent) ([]reader.Event, bool) {
	t.Helper()
	r := n03Reader(t)
	st := &streamState{tableMap: make(map[uint64]*replication.TableMapEvent)}
	if posMode {
		if err := st.initPosMode("mysql-bin", "mysql-bin.000003"); err != nil {
			t.Fatal(err)
		}
		st.curFile = "mysql-bin.000003"
	}
	var got []reader.Event
	emit := func(e reader.Event) bool { got = append(got, e); return true }
	for _, ev := range evs {
		if !r.handleEvent(st, ev, emit) {
			return got, false
		}
	}
	return got, true
}

func mdbGTID(end uint32, flags byte) *replication.BinlogEvent {
	return n03Ev(replication.MARIADB_GTID_EVENT, end, 39, &replication.MariadbGTIDEvent{
		GTID: gomysql.MariadbGTID{DomainID: 0, ServerID: 1, SequenceNumber: 5}, Flags: flags})
}

// Nhóm DML của MariaDB không có QUERY "BEGIN": event GTID phải thành BEGIN tại ĐẦU event GTID, nếu không
// assembler không mở Txn (ROWS bị coi là thiếu BEGIN) và điểm đọc lại không phải ranh giới nhóm.
func TestMariaDBGTIDGroupBecomesBegin(t *testing.T) {
	got, ok := mdbCollect(t, true,
		mdbGTID(400, replication.BINLOG_MARIADB_FL_TRANSACTIONAL),
		n03Ev(replication.MARIADB_ANNOTATE_ROWS_EVENT, 460, 60, &replication.MariadbAnnotateRowsEvent{Query: []byte("INSERT ...")}),
		n03Ev(replication.TABLE_MAP_EVENT, 520, 60, n03TableMap()),
		n03Ev(replication.XID_EVENT, 551, 31, &replication.XIDEvent{XID: 9}),
	)
	if !ok {
		t.Fatalf("stream stopped: %+v", got)
	}
	if len(got) != 3 {
		t.Fatalf("want BEGIN, TABLE_MAP, XID; got %d events: %+v", len(got), got)
	}
	if got[0].Type != reader.EventQuery || got[0].Query != "BEGIN" || got[0].BinlogFile != "mysql-bin.000003" || got[0].BinlogPos != 361 {
		t.Fatalf("first event must be BEGIN at the GTID event start 361: %+v", got[0])
	}
	if got[1].Type != reader.EventTableMap || got[2].Type != reader.EventXID || got[2].Pos.BinlogPos != 551 {
		t.Fatalf("unexpected events: %+v", got[1:])
	}
}

// Nhóm standalone (DDL): không phát BEGIN; QUERY là cả Txn, bắt đầu tại event GTID.
func TestMariaDBStandaloneGTIDStartsDDLAtGTID(t *testing.T) {
	got, ok := mdbCollect(t, true,
		mdbGTID(600, replication.BINLOG_MARIADB_FL_STANDALONE|replication.BINLOG_MARIADB_FL_DDL),
		n03Ev(replication.QUERY_EVENT, 700, 100, &replication.QueryEvent{Schema: []byte("app"), Query: []byte("ALTER TABLE t ADD c INT")}),
		mdbGTID(800, replication.BINLOG_MARIADB_FL_TRANSACTIONAL),
	)
	if !ok || len(got) != 2 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
	if got[0].Type != reader.EventQuery || !strings.HasPrefix(got[0].Query, "ALTER") || got[0].BinlogPos != 561 || got[0].Pos.BinlogPos != 700 {
		t.Fatalf("DDL must start at GTID start 561 and end at 700: %+v", got[0])
	}
	if got[1].Query != "BEGIN" || got[1].BinlogPos != 761 {
		t.Fatalf("next group: %+v", got[1])
	}
}

func TestMariaDBMetadataEventsIgnored(t *testing.T) {
	got, ok := mdbCollect(t, true,
		n03Ev(replication.MARIADB_GTID_LIST_EVENT, 300, 40, &replication.MariadbGTIDListEvent{}),
		n03Ev(replication.MARIADB_BINLOG_CHECKPOINT_EVENT, 350, 50, &replication.MariadbBinlogCheckPointEvent{Info: []byte("mysql-bin.000003")}),
	)
	if !ok || len(got) != 0 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

// log_bin_compress=ON: event nén chưa được giải mã/kiểm; bỏ qua là mất dữ liệu nên phải dừng.
func TestMariaDBCompressedRowsFailClosed(t *testing.T) {
	for _, typ := range []replication.EventType{replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1, replication.MARIADB_QUERY_COMPRESSED_EVENT} {
		got, ok := mdbCollect(t, true, n03Ev(typ, 900, 80, &replication.GenericEvent{}))
		if ok || len(got) != 1 || got[0].Type != reader.EventError || !strings.Contains(got[0].Err.Error(), "log_bin_compress") {
			t.Fatalf("%v: got %+v ok=%v", typ, got, ok)
		}
		if code := exitcode.GetExitCode(got[0].Err); code != exitcode.Rejected {
			t.Fatalf("%v: exit code %d, want Rejected", typ, code)
		}
	}
}

// GTID MariaDB ngoài chế độ vị trí (vd checkpoint GTID MySQL): không so được, dừng.
func TestMariaDBGTIDOutsidePosModeFailsClosed(t *testing.T) {
	got, ok := mdbCollect(t, false, mdbGTID(400, 0))
	if ok || len(got) != 1 || got[0].Type != reader.EventError || !strings.Contains(got[0].Err.Error(), "binlog position mode") {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

// TEXT latin1: go-mysql trả []byte. Sau khi chuyển sang UTF-8 phải là string: []byte được writer MySQL gửi
// dạng _binary và server lưu nguyên byte UTF-8 vào cột latin1 (ký tự bị mã hóa hai lần, "café" → "cafÃ©").
func TestConvertedTextValueIsString(t *testing.T) {
	tm := &replication.TableMapEvent{Schema: []byte("app"), Table: []byte("t"), ColumnCount: 1}
	rows := [][]any{{[]byte{'c', 'a', 'f', 0xE9}}}
	if err := convertCharsetsBy(tm, rows, map[int]string{0: "latin1"}); err != nil {
		t.Fatal(err)
	}
	s, ok := rows[0][0].(string)
	if !ok || s != "café" {
		t.Fatalf("got %T %q, want string \"café\"", rows[0][0], rows[0][0])
	}
}
