package writer

import (
	"context"
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
)

var klT = model.TableID{Schema: "d", Table: "k"}

func TestKeylessMySQLSQL(t *testing.T) {
	if got, want := buildKeylessInsertMySQL(klT, []string{"a", "b"}), "INSERT INTO `d`.`k` (`a`, `b`) VALUES (?, ?)"; got != want {
		t.Fatalf("insert: %s", got)
	}
	if strings.Contains(buildKeylessInsertMySQL(klT, []string{"a"}), "ON DUPLICATE") {
		t.Fatal("keyless insert must be a plain INSERT (identical rows are distinct rows)")
	}
	if got, want := buildKeylessUpdateMySQL(klT, []string{"a", "b"}, []string{"a", "b"}, nil),
		"UPDATE `d`.`k` SET `a` = ?, `b` = ? WHERE `a` <=> ? AND `b` <=> ? LIMIT 1"; got != want {
		t.Fatalf("update: %s", got)
	}
	if got, want := buildKeylessDeleteMySQL(klT, []string{"a", "b"}, nil), "DELETE FROM `d`.`k` WHERE `a` <=> ? AND `b` <=> ? LIMIT 1"; got != want {
		t.Fatalf("delete: %s", got)
	}
}

// Cột chuỗi khớp thêm theo byte (collation _ci / PAD SPACE coi 'abc', 'ABC', 'abc ' là bằng nhau).
func TestKeylessMySQLStringColumnsMatchByBytes(t *testing.T) {
	str := map[string]bool{"b": true}
	got := buildKeylessDeleteMySQL(klT, []string{"a", "b"}, str)
	want := "DELETE FROM `d`.`k` WHERE `a` <=> ? AND `b` <=> ? AND BINARY CONVERT(`b` USING utf8mb4) <=> BINARY ? LIMIT 1"
	if got != want {
		t.Fatalf("delete: %s", got)
	}
	args := keylessMatchArgs([]string{"a", "b"}, str, []model.Value{int64(1), "ABC"})
	if len(args) != 3 || args[1] != "ABC" || args[2] != "ABC" {
		t.Fatalf("string column value must be passed twice: %v", args)
	}
	if n := len(keylessMatchArgs([]string{"a", "b"}, nil, []model.Value{int64(1), "x"})); n != 2 {
		t.Fatalf("non-string columns pass one arg each, got %d", n)
	}
}

func TestKeylessPGSQL(t *testing.T) {
	if got, want := buildKeylessInsertPG(klT, []string{"a", "b"}), `INSERT INTO "d"."k" ("a", "b") OVERRIDING SYSTEM VALUE VALUES ($1, $2)`; got != want {
		t.Fatalf("insert: %s", got)
	}
	if got, want := buildKeylessUpdatePG(klT, []string{"a", "b"}, []string{"a", "b"}),
		`UPDATE "d"."k" SET "a" = $1, "b" = $2 WHERE ctid = (SELECT ctid FROM "d"."k" WHERE "a" IS NOT DISTINCT FROM $3 AND "b" IS NOT DISTINCT FROM $4 LIMIT 1)`; got != want {
		t.Fatalf("update: %s", got)
	}
	if got, want := buildKeylessDeletePG(klT, []string{"a"}),
		`DELETE FROM "d"."k" WHERE ctid = (SELECT ctid FROM "d"."k" WHERE "a" IS NOT DISTINCT FROM $1 LIMIT 1)`; got != want {
		t.Fatalf("delete: %s", got)
	}
}

func klWriter(t *testing.T, pipelining bool) (*MySQLWriter, *fakeMyState) {
	w, st := newFakeMySQLWriter(t, pipelining)
	w.SetPrimaryKey(klT, []string{"a", "b"})
	w.SetKeyless(klT, true)
	return w, st
}

func klRow(op model.Op, before, after []model.Value) model.RowChange {
	rc := model.RowChange{Table: klT, Op: op}
	switch op {
	case model.OpInsert:
		rc.Key = after
	case model.OpDelete:
		rc.Key = before
	case model.OpUpdate:
		rc.Key, rc.OldKey = after, before
	}
	if op != model.OpDelete {
		rc.Cols = []model.ColValue{{Name: "a", Value: after[0]}, {Name: "b", Value: after[1]}}
	}
	return rc
}

// INSERT/UPDATE/DELETE của bảng keyless đi qua câu lệnh keyless, không có upsert; UPDATE trước=sau không ghi gì.
func TestKeylessMySQLApplyStatements(t *testing.T) {
	for _, pipelining := range []bool{false, true} {
		w, st := klWriter(t, pipelining)
		tx := &model.Txn{Rows: []model.RowChange{
			klRow(model.OpInsert, nil, []model.Value{int64(1), nil}),
			klRow(model.OpInsert, nil, []model.Value{int64(1), nil}), // duplicate identical row is a second row
			klRow(model.OpUpdate, []model.Value{int64(1), nil}, []model.Value{int64(1), "x"}),
			klRow(model.OpUpdate, []model.Value{int64(2), "q"}, []model.Value{int64(2), "q"}), // no-op
			klRow(model.OpDelete, []model.Value{int64(1), "x"}, nil),
		}}
		var stats ApplyStats
		if err := w.ApplyBatch(context.Background(), []*model.Txn{tx}, DeltaRow{Applied: "a", Stats: &stats}); err != nil {
			t.Fatalf("pipelining %v: %v", pipelining, err)
		}
		all := strings.Join(st.execs, "\n")
		for _, want := range []string{"INSERT INTO `d`.`k`", "UPDATE `d`.`k` SET", "<=> ?", "DELETE FROM `d`.`k` WHERE", "LIMIT 1"} {
			if !strings.Contains(all, want) {
				t.Fatalf("pipelining %v: missing %q in %s", pipelining, want, all)
			}
		}
		if strings.Contains(all, "ON DUPLICATE KEY") {
			t.Fatalf("keyless must not upsert: %s", all)
		}
		if strings.Count(all, "UPDATE `d`.`k`") != 1 {
			t.Fatalf("no-op UPDATE must not be sent: %s", all)
		}
		if stats.Rows != 5 || !st.committed {
			t.Fatalf("pipelining %v: rows %d committed %v", pipelining, stats.Rows, st.committed)
		}
	}
}

// UPDATE/DELETE không khớp hàng nào ở target = lệch: dừng với keyless_row_missing (exit 5), rollback, không bỏ qua im lặng.
func TestKeylessZeroRowMatchFailsClosed(t *testing.T) {
	for _, op := range []model.Op{model.OpUpdate, model.OpDelete} {
		w, st := klWriter(t, true)
		st.zeroOn = map[model.Op]string{model.OpUpdate: "UPDATE `d`.`k`", model.OpDelete: "DELETE FROM `d`.`k`"}[op]
		rc := klRow(op, []model.Value{int64(1), "x"}, []model.Value{int64(1), "y"})
		err := w.ApplyBatch(context.Background(), []*model.Txn{{Rows: []model.RowChange{rc}}}, DeltaRow{Applied: "a"})
		var ee *exitcode.Error
		if !errors.As(err, &ee) || ee.Code != exitcode.ErrKeylessRowMissing || ee.ExitCode != exitcode.DataError {
			t.Fatalf("%s: want keyless_row_missing, got %v", op, err)
		}
		if st.committed || !st.rolled {
			t.Fatalf("%s: batch must roll back (committed %v rolled %v)", op, st.committed, st.rolled)
		}
	}
}

// Thứ tự trong cùng Txn được giữ: câu đang xếp lô (không keyless) được gửi trước câu keyless.
func TestKeylessKeepsOrderWithOtherTables(t *testing.T) {
	w, st := klWriter(t, true)
	tx := &model.Txn{Rows: []model.RowChange{
		myRow(myT, model.OpInsert, 1),
		klRow(model.OpInsert, nil, []model.Value{int64(1), "x"}),
		klRow(model.OpDelete, []model.Value{int64(1), "x"}, nil),
		myRow(myT, model.OpDelete, 1),
	}}
	if err := w.ApplyBatch(context.Background(), []*model.Txn{tx}, DeltaRow{Applied: "a"}); err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, q := range st.execs {
		for _, part := range strings.Split(q, ";\n") {
			switch {
			case strings.Contains(part, "INSERT INTO `d`.`t`"):
				order = append(order, "t-ins")
			case strings.Contains(part, "INSERT INTO `d`.`k`"):
				order = append(order, "k-ins")
			case strings.Contains(part, "DELETE FROM `d`.`k`"):
				order = append(order, "k-del")
			case strings.Contains(part, "DELETE FROM `d`.`t`"):
				order = append(order, "t-del")
			}
		}
	}
	if got := strings.Join(order, ","); got != "t-ins,k-ins,k-del,t-del" {
		t.Fatalf("order = %s", got)
	}
}
