package assemble_test

import (
	"errors"
	"strings"
	"testing"

	"strand/internal/assemble"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

// review G2: txn XA (GTID + "XA START" không có BEGIN) từng bị coi là DDL; ROWS sau đó được phát thành
// Txn ở "XA END" — tức là apply trước khi biết XA COMMIT hay XA ROLLBACK. Phải fail closed (exit 3).
func TestG2_XAStartIsRejected(t *testing.T) {
	mysql := model.Pos{Engine: model.EngineMySQL}
	for _, q := range []string{"XA START X'78',X'',1", "xa begin 'x'", "XA COMMIT X'78',X'',1", "XA COMMIT X'78',X'',1 ONE PHASE"} {
		asm := assemble.New()
		mustPush(t, asm, gtid("1"))
		txn, err := asm.Push(reader.Event{Type: reader.EventQuery, Query: q, Schema: "app", Pos: mysql})
		var se *exitcode.Error
		if !errors.As(err, &se) || se.Code != exitcode.ErrBinlogConfigInvalid || se.ExitCode != exitcode.Rejected {
			t.Fatalf("%q: want binlog_config_invalid/exit 3, got txn=%+v err=%v", q, txn, err)
		}
		if !strings.Contains(err.Error(), "XA transactions are not supported") {
			t.Fatalf("%q: message must say XA is unsupported: %v", q, err)
		}
	}
}

// review G2: luồng XA đầy đủ không được phát ra Txn nào mang row (trước đây Txn "XA END" chứa row).
func TestG2_XARowsNeverEmitted(t *testing.T) {
	mysql := model.Pos{Engine: model.EngineMySQL}
	asm := assemble.New()
	evs := []reader.Event{
		gtid("1"),
		{Type: reader.EventQuery, Query: "XA START X'78',X'',1", Schema: "app", Pos: mysql},
		{Type: reader.EventTableMap, Schema: "app", Table: "t", Pos: mysql},
		{Type: reader.EventRows, Schema: "app", Table: "t", Op: model.OpInsert, Rows: [][]any{{int64(1)}}, Pos: mysql},
		{Type: reader.EventQuery, Query: "XA END X'78',X'',1", Schema: "app", Pos: mysql},
	}
	for _, ev := range evs {
		txn, err := asm.Push(ev)
		if err != nil {
			return // fail closed
		}
		if txn != nil && txn.TotalRows() > 0 {
			t.Fatalf("XA rows emitted before XA COMMIT: %+v", txn)
		}
	}
	t.Fatalf("XA transaction must be rejected")
}

// review G2: "XA ROLLBACK" đứng riêng (txn đã prepare trước điểm đọc) không mang row: Txn rỗng, không DDL.
func TestG2_StandaloneXARollbackIsEmptyTxn(t *testing.T) {
	asm := assemble.New()
	mustPush(t, asm, gtid("2"))
	txn, err := asm.Push(reader.Event{Type: reader.EventQuery, Query: "XA ROLLBACK X'78',X'',1", Schema: "app", Pos: model.Pos{Engine: model.EngineMySQL}})
	if err != nil || txn == nil || txn.DDL != "" || txn.TotalRows() != 0 || !strings.HasSuffix(txn.SourcePos.GTID, ":2") {
		t.Fatalf("want empty non-DDL txn at GTID :2, got %+v err=%v", txn, err)
	}
}
