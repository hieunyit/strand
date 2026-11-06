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

// review R09: DML dạng statement (session binlog_format=STATEMENT/MIXED) nằm trong txn đã BEGIN
// từng bị bỏ qua im lặng, txn rỗng vẫn được checkpoint. Phải fail closed với exit 3.
func TestR09_StatementDMLInsideBeginIsError(t *testing.T) {
	for _, q := range []string{
		"UPDATE t SET c = 1 WHERE id = 2",
		"insert into t values (1)",
		"DELETE FROM t WHERE id = 1 AND note = '" + strings.Repeat("x", 200) + "'",
	} {
		asm := assemble.New()
		mustPush(t, asm, gtid("1"))
		mustPush(t, asm, reader.Event{Type: reader.EventQuery, Query: "BEGIN", Pos: model.Pos{Engine: model.EngineMySQL}})
		txn, err := asm.Push(reader.Event{Type: reader.EventQuery, Query: q, Schema: "app", Pos: model.Pos{Engine: model.EngineMySQL}})
		if err == nil {
			t.Fatalf("%q: expected error, got txn=%+v", q, txn)
		}
		var se *exitcode.Error
		if !errors.As(err, &se) || se.Code != exitcode.ErrBinlogConfigInvalid || se.ExitCode != exitcode.Rejected {
			t.Fatalf("%q: want binlog_config_invalid/exit 3, got %v", q, err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "ROW") || !strings.Contains(msg, q[:20]) {
			t.Fatalf("%q: message must name statement prefix and ROW: %s", q, msg)
		}
		if len(msg) > 400 {
			t.Fatalf("statement not truncated in message (%d bytes)", len(msg))
		}
	}
}

// review R09: các QUERY MySQL ghi hợp lệ trong txn ROW (SAVEPOINT, ROLLBACK TO, XA ...) không được lỗi,
// và txn vẫn kết thúc bình thường ở XID với đủ các hàng.
func TestR09_SavepointInsideBeginIsOK(t *testing.T) {
	asm := assemble.New()
	mustPush(t, asm, gtid("1"))
	mustPush(t, asm, reader.Event{Type: reader.EventQuery, Query: "BEGIN"})
	for _, q := range []string{"SAVEPOINT `sp1`", "ROLLBACK TO `sp1`", "rollback to savepoint sp1", "RELEASE SAVEPOINT sp1", "XA END X'31',X'',1"} {
		if txn, err := asm.Push(reader.Event{Type: reader.EventQuery, Query: q, Schema: "app"}); err != nil || txn != nil {
			t.Fatalf("%q: unexpected txn=%v err=%v", q, txn, err)
		}
	}
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "t", Op: model.OpInsert, Rows: [][]any{{int64(1)}}})
	txn, err := asm.Push(reader.Event{Type: reader.EventXID})
	if err != nil || txn == nil || len(txn.Rows) != 1 || txn.DDL != "" {
		t.Fatalf("txn wrong: %+v err=%v", txn, err)
	}
}

// review R09: DDL (GTID + QUERY, không BEGIN) vẫn là txn DDL riêng; PG BEGIN/COMMIT không bị ảnh hưởng.
func TestR09_DDLWithoutBeginStillDDLTxn(t *testing.T) {
	asm := assemble.New()
	mustPush(t, asm, gtid("1"))
	ddl, err := asm.Push(reader.Event{Type: reader.EventQuery, Query: "UPDATE looks_like_dml SET x=1", Schema: "app"})
	if err != nil || ddl == nil || ddl.DDL == "" {
		t.Fatalf("DDL txn not emitted: %+v err=%v", ddl, err)
	}

	pg := assemble.New()
	pos := model.Pos{Engine: model.EnginePG, LSN: 16}
	mustPush(t, pg, reader.Event{Type: reader.EventQuery, Query: "BEGIN", Pos: pos, XID: 7})
	mustPush(t, pg, reader.Event{Type: reader.EventRows, Schema: "public", Table: "t", Op: model.OpInsert, Rows: [][]any{{int64(1)}}, Pos: pos})
	txn, err := pg.Push(reader.Event{Type: reader.EventQuery, Query: "COMMIT", Pos: pos})
	if err != nil || txn == nil || len(txn.Rows) != 1 {
		t.Fatalf("PG txn wrong: %+v err=%v", txn, err)
	}
}
