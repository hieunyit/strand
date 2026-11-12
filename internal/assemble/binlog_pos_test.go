package assemble_test

// Txn ở chế độ vị trí binlog (source MySQL không GTID): ranh giới Txn do BEGIN / XID / COMMIT / DDL,
// vị trí định danh là vị trí KẾT THÚC; vị trí bắt đầu lưu ở Txn.BinlogFile/BinlogPos.

import (
	"strings"
	"testing"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
)

func evAt(typ reader.EventType, file string, start, end uint64) reader.Event {
	return reader.Event{Type: typ, Pos: model.Pos{Engine: model.EngineMySQL, File: file, BinlogPos: end},
		BinlogFile: file, BinlogPos: start}
}

func qAt(q, file string, start, end uint64) reader.Event {
	e := evAt(reader.EventQuery, file, start, end)
	e.Query, e.Schema = q, "app"
	return e
}

func rowsAt(file string, start, end uint64) reader.Event {
	e := evAt(reader.EventRows, file, start, end)
	e.Schema, e.Table, e.Op, e.Rows = "app", "t", model.OpInsert, [][]any{{int64(1)}}
	return e
}

func newPosAsm() *assemble.Assembler {
	a := assemble.New()
	a.Registry().RegisterTable("app", "t", []string{"id"}, []string{"id"})
	return a
}

func pushAll(t *testing.T, a *assemble.Assembler, evs ...reader.Event) []*model.Txn {
	t.Helper()
	var out []*model.Txn
	for _, ev := range evs {
		txn, err := a.Push(ev)
		if err != nil {
			t.Fatalf("push %s: %v", ev.Type, err)
		}
		if txn != nil {
			out = append(out, txn)
		}
	}
	return out
}

func TestBinlogPosTxnBoundaries(t *testing.T) {
	const f = "mysql-bin.000005"
	a := newPosAsm()
	txns := pushAll(t, a,
		qAt("BEGIN", f, 200, 270),
		evAt(reader.EventTableMap, f, 270, 320),
		rowsAt(f, 320, 380),
		evAt(reader.EventXID, f, 380, 411),
		// Txn rỗng sau BEGIN (không row): vẫn là một Txn có vị trí riêng.
		qAt("BEGIN", f, 411, 480),
		evAt(reader.EventXID, f, 480, 511),
		// DDL: QUERY đứng riêng.
		qAt("CREATE TABLE x (id int primary key)", f, 511, 650),
		// Bảng không giao dịch: BEGIN ... COMMIT (query).
		qAt("BEGIN", f, 650, 720),
		rowsAt(f, 720, 780),
		qAt("COMMIT", f, 780, 850),
	)
	if len(txns) != 4 {
		t.Fatalf("got %d txns", len(txns))
	}
	want := []struct{ start, end uint64 }{{200, 411}, {411, 511}, {511, 650}, {650, 850}}
	for i, w := range want {
		g := txns[i]
		if !g.SourcePos.IsBinlogPos() || g.SourcePos.File != f || g.SourcePos.BinlogPos != w.end || g.BinlogFile != f || g.BinlogPos != w.start {
			t.Errorf("txn %d: pos=%+v start=%s:%d, want [%d,%d]", i, g.SourcePos, g.BinlogFile, g.BinlogPos, w.start, w.end)
		}
	}
	if txns[0].TotalRows() != 1 || txns[1].TotalRows() != 0 || txns[2].DDL == "" || txns[3].TotalRows() != 1 {
		t.Errorf("contents: %d %d %q %d", txns[0].TotalRows(), txns[1].TotalRows(), txns[2].DDL, txns[3].TotalRows())
	}
}

// ROLLBACK toàn Txn (row của bảng không giao dịch đã vào binlog) kết thúc Txn: nếu không, Txn kế tiếp bị
// gộp vào và vị trí kết thúc nhảy qua Txn đầu.
func TestBinlogPosPlainRollbackEndsTxn(t *testing.T) {
	const f = "mysql-bin.000005"
	a := newPosAsm()
	txns := pushAll(t, a,
		qAt("BEGIN", f, 100, 170),
		rowsAt(f, 170, 230),
		qAt("ROLLBACK", f, 230, 300),
		qAt("BEGIN", f, 300, 370),
		evAt(reader.EventXID, f, 370, 401),
	)
	if len(txns) != 2 || txns[0].SourcePos.BinlogPos != 300 || txns[0].TotalRows() != 1 || txns[1].SourcePos.BinlogPos != 401 || txns[1].BinlogPos != 300 {
		t.Fatalf("rollback handling: %+v", txns)
	}
	// ROLLBACK TO SAVEPOINT không kết thúc Txn.
	a = newPosAsm()
	txns = pushAll(t, a,
		qAt("BEGIN", f, 100, 170),
		qAt("SAVEPOINT sp", f, 170, 230),
		qAt("ROLLBACK TO sp", f, 230, 300),
		evAt(reader.EventXID, f, 300, 331),
	)
	if len(txns) != 1 || txns[0].SourcePos.BinlogPos != 331 {
		t.Fatalf("rollback to savepoint: %+v", txns)
	}
}

// Không có event GTID đánh dấu ranh giới: BEGIN khi Txn trước chưa kết thúc là mất event — fail closed.
func TestBinlogPosBeginInsideOpenTxnFailsClosed(t *testing.T) {
	const f = "mysql-bin.000005"
	a := newPosAsm()
	pushAll(t, a, qAt("BEGIN", f, 100, 170), rowsAt(f, 170, 230))
	if _, err := a.Push(qAt("BEGIN", f, 300, 370)); err == nil || !strings.Contains(err.Error(), "before the transaction") {
		t.Fatalf("BEGIN inside open txn accepted: %v", err)
	}
	// Tương tự DDL đứng riêng chen vào Txn đang mở.
	a = newPosAsm()
	pushAll(t, a, evAt(reader.EventTableMap, f, 100, 150), rowsAt(f, 150, 230))
	if _, err := a.Push(qAt("ALTER TABLE t ADD c int", f, 230, 300)); err == nil {
		t.Fatal("DDL inside an open txn accepted")
	}
}

// Txn không bao giờ vắt qua hai file binlog; vị trí kết thúc phải sau vị trí bắt đầu.
func TestBinlogPosTxnSpanningFilesFailsClosed(t *testing.T) {
	a := newPosAsm()
	pushAll(t, a, qAt("BEGIN", "mysql-bin.000005", 100, 170))
	if _, err := a.Push(evAt(reader.EventXID, "mysql-bin.000006", 100, 131)); err == nil || !strings.Contains(err.Error(), "spans") {
		t.Fatalf("cross-file txn accepted: %v", err)
	}
	a = newPosAsm()
	pushAll(t, a, qAt("BEGIN", "mysql-bin.000005", 200, 270))
	if _, err := a.Push(evAt(reader.EventXID, "mysql-bin.000005", 100, 150)); err == nil {
		t.Fatal("txn ending before its start accepted")
	}
}

// XA không có GTID cũng bị từ chối (không rơi vào nhánh DDL).
func TestBinlogPosXAStillRejected(t *testing.T) {
	a := newPosAsm()
	if _, err := a.Push(qAt("XA START X'78',X'',1", "mysql-bin.000005", 100, 170)); err == nil || !strings.Contains(err.Error(), "XA") {
		t.Fatalf("XA accepted: %v", err)
	}
}

// Txn bắt đầu bằng TABLE_MAP (không BEGIN, như cắt giữa chừng) vẫn có vị trí bắt đầu đúng.
func TestBinlogPosStartAtFirstEvent(t *testing.T) {
	const f = "mysql-bin.000005"
	a := newPosAsm()
	txns := pushAll(t, a, evAt(reader.EventTableMap, f, 100, 150), rowsAt(f, 150, 210), qAt("COMMIT", f, 210, 280))
	if len(txns) != 1 || txns[0].BinlogPos != 100 || txns[0].SourcePos.BinlogPos != 280 {
		t.Fatalf("%+v", txns)
	}
}
