package writer

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"strand/internal/model"
)

// Kiểm thử thuần (không DB) cho review N13: lô câu lệnh và khối upsert MySQL phải bị giới hạn theo
// byte payload thật (gồm blob/text trong args), không chỉ theo độ dài SQL hay số row.

type sentStmt struct {
	query string
	args  []any
}

func fakeQueue(on bool, budget int, maxPacket int64) (*stmtQueue, *[]sentStmt) {
	var sent []sentStmt
	q := &stmtQueue{on: on, budget: budget, maxPacket: maxPacket}
	q.send = func(_ context.Context, query string, args ...any) error {
		sent = append(sent, sentStmt{query: query, args: append([]any(nil), args...)})
		return nil
	}
	return q, &sent
}

func blobTxn(table model.TableID, n, size int) *model.Txn {
	txn := &model.Txn{}
	for i := 0; i < n; i++ {
		txn.Rows = append(txn.Rows, model.RowChange{
			Table: table, Op: model.OpInsert, Key: []model.Value{int64(i + 1)},
			Cols: []model.ColValue{{Name: "id", Value: int64(i + 1)}, {Name: "data", Value: bytes.Repeat([]byte{'x'}, size)}},
		})
	}
	return txn
}

func sentBytes(s sentStmt) int { return len(s.query) + argsBytes(s.args) }

func TestN13_StmtQueueFlushesOnArgBytes(t *testing.T) {
	const budget = 1 << 20
	q, sent := fakeQueue(true, budget, 64<<20)
	ctx := context.Background()
	blob := bytes.Repeat([]byte{'b'}, 600<<10)
	for i := 0; i < 3; i++ {
		if err := q.exec(ctx, "INSERT INTO t (d) VALUES (?)", blob); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 3 {
		t.Fatalf("SQL ngắn mang blob 600 KiB, ngân sách 1 MiB: mong 3 lần gửi, có %d", len(*sent))
	}
	for i, s := range *sent {
		if sentBytes(s) > budget {
			t.Fatalf("lần gửi %d mang ~%d byte > ngân sách %d", i, sentBytes(s), budget)
		}
	}
}

func TestN13_StmtQueueOversizedStatementAlone(t *testing.T) {
	const budget = 1 << 20
	q, sent := fakeQueue(true, budget, 64<<20)
	ctx := context.Background()
	small := "INSERT INTO t (d) VALUES (?)"
	if err := q.exec(ctx, small, "a"); err != nil {
		t.Fatal(err)
	}
	if err := q.exec(ctx, small, bytes.Repeat([]byte{'b'}, 3<<20)); err != nil {
		t.Fatal(err)
	}
	if err := q.exec(ctx, small, "c"); err != nil {
		t.Fatal(err)
	}
	if err := q.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 3 {
		t.Fatalf("câu lớn hơn ngân sách phải đi một mình: mong 3 lần gửi, có %d", len(*sent))
	}
	if len((*sent)[1].args) != 1 || strings.Contains((*sent)[1].query, ";") {
		t.Fatalf("lần gửi thứ 2 phải chỉ gồm câu lớn: %q (%d args)", (*sent)[1].query, len((*sent)[1].args))
	}
}

func TestN13_UpsertChunksByBytesNotMidRow(t *testing.T) {
	for _, on := range []bool{true, false} {
		const budget = 1 << 20
		q, sent := fakeQueue(on, budget, 64<<20)
		w := &MySQLWriter{pipelining: on}
		ctx := context.Background()
		var cur *mysqlUpsertBatch
		tbl := model.TableID{Schema: "s", Table: "t"}
		if err := w.applyTxnMySQL(ctx, q, blobTxn(tbl, 5, 400<<10), &cur); err != nil {
			t.Fatal(err)
		}
		if cur != nil {
			if err := cur.flush(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
		if err := q.flush(ctx); err != nil {
			t.Fatal(err)
		}
		rows := 0
		for i, s := range *sent {
			if sentBytes(s) > budget {
				t.Fatalf("pipelining=%v: lần gửi %d mang ~%d byte > ngân sách %d", on, i, sentBytes(s), budget)
			}
			if len(s.args)%2 != 0 {
				t.Fatalf("pipelining=%v: row bị tách giữa chừng (%d args)", on, len(s.args))
			}
			rows += len(s.args) / 2
		}
		if rows != 5 || len(*sent) < 3 {
			t.Fatalf("pipelining=%v: mong 5 row trong >= 3 lần gửi, có %d row / %d lần", on, rows, len(*sent))
		}
	}
}

func TestN13_SingleRowOverBudgetSentAlone(t *testing.T) {
	const budget = 1 << 20
	q, sent := fakeQueue(true, budget, 64<<20)
	w := &MySQLWriter{pipelining: true}
	ctx := context.Background()
	var cur *mysqlUpsertBatch
	tbl := model.TableID{Table: "t"}
	txn := blobTxn(tbl, 1, 100)
	big := blobTxn(tbl, 1, 3<<20)
	big.Rows[0].Key = []model.Value{int64(2)}
	big.Rows[0].Cols[0].Value = int64(2)
	txn.Rows = append(txn.Rows, big.Rows[0], blobTxn(tbl, 1, 100).Rows[0])
	txn.Rows[2].Key = []model.Value{int64(3)}
	if err := w.applyTxnMySQL(ctx, q, txn, &cur); err != nil {
		t.Fatalf("row 3 MiB < max_allowed_packet phải được gửi: %v", err)
	}
	if err := cur.flush(ctx, q); err != nil {
		t.Fatal(err)
	}
	if err := q.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 3 || len((*sent)[1].args) != 2 {
		t.Fatalf("row lớn phải đi nguyên một mình: %d lần gửi", len(*sent))
	}
}

func TestN13_RowOverMaxAllowedPacketFailsClearly(t *testing.T) {
	q, sent := fakeQueue(true, budgetFor(4<<20), 4<<20)
	w := &MySQLWriter{pipelining: true}
	var cur *mysqlUpsertBatch
	tbl := model.TableID{Schema: "app", Table: "docs"}
	err := w.applyTxnMySQL(context.Background(), q, blobTxn(tbl, 1, 5<<20), &cur)
	if err == nil {
		t.Fatal("row 5 MiB với max_allowed_packet 4 MiB phải bị từ chối")
	}
	for _, want := range []string{"app.docs", "max_allowed_packet", "[1]"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("lỗi phải nêu %q: %v", want, err)
		}
	}
	if len(*sent) != 0 {
		t.Fatalf("không được gửi gì: %d", len(*sent))
	}
}

func TestN13_BudgetFor(t *testing.T) {
	cases := map[int64]int{0: stmtQueueMaxBytes, 64 << 20: 16 << 20, 1 << 30: stmtQueueMaxBytes, 4 << 20: 1 << 20, 1024: stmtQueueMinBytes}
	for mp, want := range cases {
		if got := budgetFor(mp); got != want {
			t.Fatalf("budgetFor(%d) = %d, mong %d", mp, got, want)
		}
	}
}

func TestN13_PGUpsertBlockCutsByBytes(t *testing.T) {
	tbl := model.TableID{Table: "t"}
	cols := []string{"id", "data"}
	b := &pgUpsertBlock{table: tbl, cols: cols, pkCols: []string{"id"}, idx: map[string]int{}}
	row := func(i int) []any { return []any{int64(i), bytes.Repeat([]byte{'x'}, 6<<20)} }
	rb := argsBytes(row(0))
	if b.full(tbl, cols, rb) {
		t.Fatal("khối rỗng không được đầy")
	}
	b.add([]model.Value{int64(1)}, row(1))
	b.add([]model.Value{int64(2)}, row(2))
	if !b.full(tbl, cols, rb) {
		t.Fatalf("khối ~%d byte + row ~%d byte > %d phải cắt", b.bytes, rb, pgBlockMaxBytes)
	}
	// Ghi đè cùng PK không làm phồng bộ đếm.
	before := b.bytes
	b.add([]model.Value{int64(2)}, row(2))
	if b.bytes != before {
		t.Fatalf("ghi đè cùng PK đổi bytes %d -> %d", before, b.bytes)
	}
	// Row lớn hơn ngân sách vào khối rỗng: đi một mình.
	e := &pgUpsertBlock{table: tbl, cols: cols, idx: map[string]int{}}
	if e.full(tbl, cols, pgBlockMaxBytes*2) {
		t.Fatal("row lớn hơn ngân sách vẫn phải vào được khối rỗng")
	}
}

func TestN13_PGOrderedQueueFullByBytes(t *testing.T) {
	a := &pgApplier{}
	big := bytes.Repeat([]byte{'x'}, 9<<20)
	if err := a.enqueue(model.TableID{Table: "t"}, "UPDATE t SET d = $1 WHERE id = $2", []any{big, int64(1)}); err != nil {
		t.Fatal(err)
	}
	if a.queueFull() {
		t.Fatal("một câu 9 MiB chưa đủ ngân sách")
	}
	a.q.Queue("UPDATE t SET d = $1 WHERE id = $2", big, int64(2))
	a.qBytes += argsBytes([]any{big, int64(2)})
	if !a.queueFull() {
		t.Fatalf("hàng đợi ~%d byte phải đầy (ngân sách %d)", a.qBytes, pgBlockMaxBytes)
	}
}
