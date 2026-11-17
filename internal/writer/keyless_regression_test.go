package writer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/assemble"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

// nkAssemble chạy một UPDATE keyless qua assembler thật để lấy RowChange đúng như pipeline sinh ra.
func nkAssemble(t *testing.T, before, after any) *model.Txn {
	t.Helper()
	a := assemble.New(assemble.WithKeylessFunc(func(id model.TableID) bool { return id == klT }))
	p := model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}
	evs := []reader.Event{
		{Type: reader.EventGTID, Pos: p},
		{Type: reader.EventQuery, Query: "BEGIN", Pos: p},
		{Type: reader.EventTableMap, Schema: "d", Table: "k", Columns: []string{"a", "b"}, Pos: p},
		{Type: reader.EventRows, Schema: "d", Table: "k", Op: model.OpUpdate, Rows: [][]any{{int64(1), before}, {int64(1), after}}, Pos: p},
		{Type: reader.EventXID, XID: 1, Pos: p},
	}
	var txn *model.Txn
	for _, ev := range evs {
		got, err := a.Push(ev)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			txn = got
		}
	}
	if txn == nil || len(txn.Rows) != 1 || len(txn.Rows[0].OldKey) != 2 {
		t.Fatalf("assembler must retain the changed before-image: %+v", txn)
	}
	return txn
}

// nkPGTx: pgx.Tx giả ghi lại câu lệnh và tham số; câu keyless UPDATE/DELETE trả số hàng affected cấu hình được.
type nkPGTx struct {
	pgx.Tx
	sql       []string
	args      [][]any
	affected  int64
	committed bool
	rolled    bool
}

func (x *nkPGTx) Exec(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	x.sql = append(x.sql, q)
	x.args = append(x.args, args)
	if strings.Contains(q, `"d"."k"`) {
		verb := strings.Fields(q)[0]
		return pgconn.NewCommandTag(fmt.Sprintf("%s %d", verb, x.affected)), nil
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}
func (x *nkPGTx) Commit(context.Context) error   { x.committed = true; return nil }
func (x *nkPGTx) Rollback(context.Context) error { x.rolled = true; return nil }

func nkPGWriter(tx *nkPGTx) *PGWriter {
	w := NewPGWriter(nil, "p", 1, 1)
	w.SetPrimaryKey(klT, []string{"a", "b"})
	w.SetKeyless(klT, true)
	w.SetTxBeginner(func(context.Context) (pgx.Tx, error) { return tx, nil })
	return w
}

func nkKeylessSQL(stmts []string, marker string) []string {
	var out []string
	for _, q := range stmts {
		for _, part := range strings.Split(q, ";\n") {
			if strings.Contains(part, marker) {
				out = append(out, part)
			}
		}
	}
	return out
}

// NK01: UPDATE keyless NULL ↔ chuỗi literal "<nil>" (cùng fmt.Sprint) phải phát đúng một UPDATE với ảnh trước
// đúng kiểu — cả MySQL và PostgreSQL, cả hai chiều — thay vì chỉ commit checkpoint.
func TestNK01_KeylessNullVsNilLiteralUpdateIsApplied(t *testing.T) {
	cases := []struct {
		name          string
		before, after any
	}{
		{"null_to_literal", nil, "<nil>"},
		{"literal_to_null", "<nil>", nil},
	}
	for _, c := range cases {
		c := c
		t.Run("mysql/"+c.name, func(t *testing.T) {
			for _, pipelining := range []bool{false, true} {
				txn := nkAssemble(t, c.before, c.after)
				w, st := klWriter(t, pipelining)
				var stats ApplyStats
				if err := w.ApplyBatch(context.Background(), []*model.Txn{txn}, DeltaRow{Applied: "gtid:1", Stats: &stats}); err != nil {
					t.Fatalf("pipelining %v: %v", pipelining, err)
				}
				ups := nkKeylessSQL(st.execs, "UPDATE `d`.`k`")
				if len(ups) != 1 {
					t.Fatalf("pipelining %v: want exactly one keyless UPDATE, got %d (execs %q)", pipelining, len(ups), st.execs)
				}
				if !strings.Contains(ups[0], "LIMIT 1") || !strings.Contains(ups[0], "`b` <=> ?") {
					t.Fatalf("pipelining %v: UPDATE must match the before-image NULL-safely: %s", pipelining, ups[0])
				}
				if !st.committed || !st.deltaUpdated() || stats.Rows != 1 {
					t.Fatalf("pipelining %v: committed %v delta %v rows %d", pipelining, st.committed, st.deltaUpdated(), stats.Rows)
				}
			}
		})
		t.Run("pg/"+c.name, func(t *testing.T) {
			txn := nkAssemble(t, c.before, c.after)
			tx := &nkPGTx{affected: 1}
			w := nkPGWriter(tx)
			var stats ApplyStats
			if err := w.ApplyBatch(context.Background(), []*model.Txn{txn}, DeltaRow{Applied: "gtid:1", Stats: &stats}); err != nil {
				t.Fatal(err)
			}
			var idx = -1
			for i, q := range tx.sql {
				if strings.HasPrefix(q, `UPDATE "d"."k"`) {
					if idx >= 0 {
						t.Fatalf("more than one keyless UPDATE: %q", tx.sql)
					}
					idx = i
				}
			}
			if idx < 0 {
				t.Fatalf("changed keyless row was skipped while the checkpoint committed: %q", tx.sql)
			}
			// SET a=$1, b=$2; ảnh trước a=$3, b=$4: giá trị phải giữ nguyên kiểu (nil khác "<nil>").
			args := tx.args[idx]
			if len(args) != 4 {
				t.Fatalf("args = %#v", args)
			}
			if args[1] != c.after || args[3] != c.before {
				t.Fatalf("SET b=%#v (want %#v), before b=%#v (want %#v)", args[1], c.after, args[3], c.before)
			}
			if !tx.committed || stats.Rows != 1 {
				t.Fatalf("committed %v rows %d", tx.committed, stats.Rows)
			}
		})
	}
}

// NK01: so sánh ảnh keyless phân biệt NULL/kiểu, nhưng ảnh thực sự giống nhau vẫn là no-op.
func TestNK01_KeylessImagesDifferIsTyped(t *testing.T) {
	differ := [][2][]model.Value{
		{{nil}, {"<nil>"}},
		{{"<nil>"}, {nil}},
		{{int64(1)}, {"1"}},
		{{[]byte("x")}, {"x"}},
		{{[]byte("x")}, {[]byte("y")}},
		{{[]byte(nil)}, {[]byte{}}},
		{{int64(1), nil}, {int64(1), []byte(nil)}},
		{{int64(1)}, {int64(1), int64(2)}},
	}
	for _, d := range differ {
		if !keylessImagesDiffer(d[0], d[1]) {
			t.Fatalf("%#v vs %#v must differ", d[0], d[1])
		}
	}
	same := [][2][]model.Value{
		{{nil, int64(1)}, {nil, int64(1)}},
		{{[]byte("x"), "s"}, {[]byte("x"), "s"}},
	}
	for _, s := range same {
		if keylessImagesDiffer(s[0], s[1]) {
			t.Fatalf("%#v vs %#v must be equal", s[0], s[1])
		}
	}
	rc := &model.RowChange{OldKey: []model.Value{int64(2), "q"}, Key: []model.Value{int64(2), "q"}}
	if keylessBefore(rc) != nil {
		t.Fatal("identical images must be a no-op")
	}
}

// NK05 (S02): PG keyless UPDATE/DELETE định danh hàng vật lý bằng (tableoid, ctid), không chỉ ctid.
func TestNK05_KeylessPGMatchUsesTableoidCtid(t *testing.T) {
	for _, q := range []string{
		buildKeylessUpdatePG(klT, []string{"a", "b"}, []string{"a", "b"}),
		buildKeylessDeletePG(klT, []string{"a", "b"}),
	} {
		if !strings.Contains(q, "WHERE (tableoid, ctid) = (SELECT tableoid, ctid FROM \"d\".\"k\" WHERE") {
			t.Fatalf("physical row identity must be (tableoid, ctid): %s", q)
		}
		if strings.Contains(q, "WHERE ctid =") || !strings.HasSuffix(q, "LIMIT 1)") {
			t.Fatalf("bare ctid match or missing LIMIT 1: %s", q)
		}
	}
}

// NK05 (S02): UPDATE/DELETE keyless phải tác động ĐÚNG một hàng; 0 hoặc >1 → lỗi, rollback, không commit checkpoint.
func TestNK05_KeylessPGRequiresExactlyOneAffectedRow(t *testing.T) {
	for _, op := range []model.Op{model.OpUpdate, model.OpDelete} {
		for _, n := range []int64{0, 1, 2} {
			tx := &nkPGTx{affected: n}
			w := nkPGWriter(tx)
			rc := klRow(op, []model.Value{int64(1), "x"}, []model.Value{int64(1), "y"})
			err := w.ApplyBatch(context.Background(), []*model.Txn{{Rows: []model.RowChange{rc}}}, DeltaRow{Applied: "a"})
			if n == 1 {
				if err != nil || !tx.committed {
					t.Fatalf("%s affected=1: err %v committed %v", op, err, tx.committed)
				}
				continue
			}
			var ee *exitcode.Error
			if !errors.As(err, &ee) || ee.Code != exitcode.ErrKeylessRowMissing || ee.ExitCode != exitcode.DataError {
				t.Fatalf("%s affected=%d: want keyless_row_missing, got %v", op, n, err)
			}
			if n > 1 && !strings.Contains(err.Error(), "expected exactly one") {
				t.Fatalf("%s affected=%d: error must say more than one row was hit: %v", op, n, err)
			}
			if tx.committed || !tx.rolled {
				t.Fatalf("%s affected=%d: must roll back (committed %v rolled %v)", op, n, tx.committed, tx.rolled)
			}
		}
	}
}
