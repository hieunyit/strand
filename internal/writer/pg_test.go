package writer_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/writer"
)

// spec: 02 §3, §4, §6 (COPY + MERGE), §8 (PG TOAST); invariant: CP-1, SG-1, CP-5

// pgEnv: một pipeline mới trên target PG dùng chung container, đã có metadata, epoch và row worker 1.
type pgEnv struct {
	db    *sql.DB
	pid   string
	epoch int64
	store *checkpoint.PGStore
}

func newPGEnv(tb testing.TB, prefix string) *pgEnv {
	tb.Helper()
	ctx := context.Background()
	db := getPostgres(tb)
	pid := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	store := checkpoint.NewPGStore(db, pid, model.EnginePG)
	if err := store.EnsureTables(ctx); err != nil {
		tb.Fatalf("ensure tables: %v", err)
	}
	epoch, err := store.AdvanceEpoch(ctx, pid, "owner", "hash")
	if err != nil {
		tb.Fatalf("advance epoch: %v", err)
	}
	return &pgEnv{db: db, pid: pid, epoch: epoch, store: store}
}

// writer tạo PGWriter cho worker 1 và bảo đảm row delta tồn tại.
func (e *pgEnv) writer(tb testing.TB, pks map[model.TableID][]string) *writer.PGWriter {
	tb.Helper()
	w := writer.NewPGWriter(e.db, e.pid, 1, e.epoch)
	for t, cols := range pks {
		w.SetPrimaryKey(t, cols)
	}
	if err := w.EnsureWorkerRow(context.Background()); err != nil {
		tb.Fatalf("ensure worker row: %v", err)
	}
	return w
}

func (e *pgEnv) delta(applied string) writer.DeltaRow {
	return writer.DeltaRow{PipelineID: e.pid, WorkerID: 1, Epoch: e.epoch, Applied: applied}
}

// workerRow đọc (applied, batch_no) của worker 1.
func (e *pgEnv) workerRow(tb testing.TB) (string, int64) {
	tb.Helper()
	var applied string
	var batchNo int64
	if err := e.db.QueryRow("SELECT applied, batch_no FROM _strand_worker_delta WHERE pipeline_id = $1 AND worker_id = 1", e.pid).
		Scan(&applied, &batchNo); err != nil {
		tb.Fatalf("read worker delta: %v", err)
	}
	return applied, batchNo
}

func pgExec(tb testing.TB, db *sql.DB, q string, args ...any) {
	tb.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		tb.Fatalf("exec %q: %v", q, err)
	}
}

// pgTable tạo (lại) bảng public.<name> rỗng.
func pgTable(tb testing.TB, db *sql.DB, name, cols string) model.TableID {
	tb.Helper()
	pgExec(tb, db, fmt.Sprintf(`DROP TABLE IF EXISTS public.%q`, name))
	pgExec(tb, db, fmt.Sprintf(`CREATE TABLE public.%q (%s)`, name, cols))
	return model.TableID{Schema: "public", Table: name}
}

func randomText(tb testing.TB, n int) string {
	tb.Helper()
	b := make([]byte, n/2)
	if _, err := rand.Read(b); err != nil {
		tb.Fatal(err)
	}
	return hex.EncodeToString(b) // ngẫu nhiên → nén kém → lưu ngoài dòng (TOAST)
}

func upd(t model.TableID, key []model.Value, cols ...model.ColValue) model.RowChange {
	return model.RowChange{Table: t, Op: model.OpUpdate, Key: key, Cols: cols}
}

func ins(t model.TableID, key []model.Value, cols ...model.ColValue) model.RowChange {
	return model.RowChange{Table: t, Op: model.OpInsert, Key: key, Cols: cols}
}

func cv(name string, v any) model.ColValue { return model.ColValue{Name: name, Value: v} }

func unchanged(name string) model.ColValue { return model.ColValue{Name: name, Unchanged: true} }

// AC1: cột text 10 KB (TOAST); UPDATE chỉ đổi cột khác, cột TOAST Unchanged → cột 10 KB còn nguyên,
// không NULL. Kèm: partial UPDATE xen giữa các upsert đầy đủ cùng batch (thứ tự giữ đúng),
// partial UPDATE mà mọi cột không-PK đều Unchanged, và đổi PK khi cột TOAST không đổi (02 §8).
func TestPGWriter_AC1_ToastPartialUpdate(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "toast-ac1")
	tbl := pgTable(t, e.db, "toast_ac1", "id int PRIMARY KEY, name text, large_data text")
	big := randomText(t, 10240)
	big2 := randomText(t, 10240)
	pgExec(t, e.db, `INSERT INTO public.toast_ac1 VALUES (1, 'initial', $1), (2, 'two', $2), (5, 'five', $1)`, big, big2)

	var ext int
	if err := e.db.QueryRow(`SELECT pg_column_size(large_data) FROM public.toast_ac1 WHERE id = 1`).Scan(&ext); err != nil {
		t.Fatal(err)
	}
	if ext < 2048 {
		t.Fatalf("giá trị thử không đủ lớn để nằm ở TOAST (pg_column_size=%d)", ext)
	}

	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
	k := func(id int) []model.Value { return []model.Value{id} }
	batch := []*model.Txn{
		{Rows: []model.RowChange{
			// upsert đầy đủ id=3, rồi partial update id=1 (cắt khối), rồi upsert đầy đủ id=3 lần nữa
			ins(tbl, k(3), cv("id", 3), cv("name", "three"), cv("large_data", "x")),
			upd(tbl, k(1), cv("id", 1), cv("name", "updated"), unchanged("large_data")),
		}},
		{Rows: []model.RowChange{
			upd(tbl, k(3), cv("id", 3), cv("name", "three-v2"), cv("large_data", "y")),
			// mọi cột không-PK Unchanged: chỉ kiểm row tồn tại
			upd(tbl, k(2), cv("id", 2), unchanged("name"), unchanged("large_data")),
			// đổi PK 5 -> 6, cột TOAST không đổi
			{Table: tbl, Op: model.OpUpdate, OldKey: k(5), Key: k(6), Cols: []model.ColValue{cv("id", 6), cv("name", "six"), unchanged("large_data")}},
		}},
	}
	if err := w.ApplyBatch(ctx, batch, e.delta("toast-1")); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	got := map[int][2]sql.NullString{}
	rows, err := e.db.Query(`SELECT id, name, large_data FROM public.toast_ac1`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int
		var n, d sql.NullString
		if err := rows.Scan(&id, &n, &d); err != nil {
			t.Fatal(err)
		}
		got[id] = [2]sql.NullString{n, d}
	}
	rows.Close()
	want := map[int][2]string{1: {"updated", big}, 2: {"two", big2}, 3: {"three-v2", "y"}, 6: {"six", big}}
	if len(got) != len(want) {
		t.Fatalf("số row = %d, muốn %d (%v)", len(got), len(want), got)
	}
	for id, wv := range want {
		g, ok := got[id]
		if !ok || !g[0].Valid || !g[1].Valid || g[0].String != wv[0] || g[1].String != wv[1] {
			t.Fatalf("id=%d: name=%v len(large_data)=%d valid=%v; muốn name=%q len=%d (cột TOAST bị ghi đè/NULL?)",
				id, g[0], len(g[1].String), g[1].Valid, wv[0], len(wv[1]))
		}
	}
	if applied, _ := e.workerRow(t); applied != "toast-1" {
		t.Fatalf("delta applied = %q", applied)
	}
}

// AC2: partial UPDATE cho PK không tồn tại → verify_mismatch (exit 5) kèm bảng/PK, không ghi gì
// (kể cả row hợp lệ đứng trước trong cùng batch và row delta), transaction rollback.
func TestPGWriter_AC2_ToastMissingRow(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "toast-ac2")
	tbl := pgTable(t, e.db, "toast_ac2", "id int PRIMARY KEY, name text, large_data text")
	pgExec(t, e.db, `INSERT INTO public.toast_ac2 VALUES (1, 'keep', 'data')`)
	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
	_, batchNo0 := e.workerRow(t)

	cases := []struct {
		name    string
		row     model.RowChange
		wantKey string
	}{
		{"set-cols", upd(tbl, []model.Value{999}, cv("id", 999), cv("name", "x"), unchanged("large_data")), "999"},
		{"all-unchanged", upd(tbl, []model.Value{999}, cv("id", 999), unchanged("name"), unchanged("large_data")), "999"},
		{"pk-change", model.RowChange{Table: tbl, Op: model.OpUpdate, OldKey: []model.Value{998}, Key: []model.Value{997},
			Cols: []model.ColValue{cv("id", 997), cv("name", "x"), unchanged("large_data")}}, "998"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			batch := []*model.Txn{
				{Rows: []model.RowChange{
					ins(tbl, []model.Value{2}, cv("id", 2), cv("name", "new"), cv("large_data", "d")),
					upd(tbl, []model.Value{1}, cv("id", 1), cv("name", "changed"), cv("large_data", "d")),
				}},
				{Rows: []model.RowChange{c.row}},
			}
			err := w.ApplyBatch(ctx, batch, e.delta("must-not-commit"))
			if err == nil {
				t.Fatal("kỳ vọng lỗi khi partial update cho row không tồn tại")
			}
			var xe *exitcode.Error
			if !errors.As(err, &xe) || xe.Code != exitcode.ErrVerifyMismatch || exitcode.GetExitCode(err) != exitcode.DataError {
				t.Fatalf("muốn mã %s exit %d, got exit %d: %v", exitcode.ErrVerifyMismatch, exitcode.DataError, exitcode.GetExitCode(err), err)
			}
			var te *writer.ToastRowMissingError
			if !errors.As(err, &te) {
				t.Fatalf("thiếu *ToastRowMissingError trong %v", err)
			}
			if te.Table != tbl || len(te.Key) != 1 || fmt.Sprint(te.Key[0]) != c.wantKey {
				t.Fatalf("bảng/PK trong lỗi = %s %v, muốn %s [%s]", te.Table, te.Key, tbl, c.wantKey)
			}
			var n int
			var name string
			if err := e.db.QueryRow(`SELECT count(*), max(name) FROM public.toast_ac2`).Scan(&n, &name); err != nil {
				t.Fatal(err)
			}
			if n != 1 || name != "keep" {
				t.Fatalf("có dữ liệu bị ghi dù batch lỗi: count=%d name=%q", n, name)
			}
			if applied, bn := e.workerRow(t); applied == "must-not-commit" || bn != batchNo0 {
				t.Fatalf("row delta bị ghi dù batch lỗi: applied=%q batch_no=%d", applied, bn)
			}
		})
	}
}

// AC3: 10.000 row INSERT/UPDATE/DELETE trộn (kể cả đổi PK ảnh đầy đủ, khóa ghép, khóa nóng lặp lại
// nhiều lần trong một khối), chia nhiều batch → bảng khớp mô hình in-memory (ảnh cuối thắng).
// Chạy hai chế độ: mọi khối qua COPY + MERGE (SetCopyMinRows(0)) và mặc định (khối nhỏ dùng
// INSERT … ON CONFLICT).
func TestPGWriter_AC3_CopyMergeMixed10k(t *testing.T) {
	t.Run("copy-merge-only", func(t *testing.T) { runAC3(t, 0) })
	t.Run("default", func(t *testing.T) { runAC3(t, -1) })
}

func runAC3(t *testing.T, copyMinRows int) {
	ctx := context.Background()
	e := newPGEnv(t, "copy-merge-ac3")
	t1 := pgTable(t, e.db, "cm_ac3", "id int PRIMARY KEY, val bigint, status text")
	t2 := pgTable(t, e.db, "cm_ac3_comp", `a int, "B b" text, val text, PRIMARY KEY (a, "B b")`)
	w := e.writer(t, map[model.TableID][]string{t1: {"id"}, t2: {"a", "B b"}})
	if copyMinRows >= 0 {
		w.SetCopyMinRows(copyMinRows)
	}

	type rec struct {
		val    int64
		status string
	}
	m1 := map[int]rec{}
	m2 := map[[2]any]string{}
	rng := mrand.New(mrand.NewSource(42))
	const totalOps, maxKey = 10000, 1500
	rows := make([]model.RowChange, 0, totalOps)
	for i := 0; i < totalOps; i++ {
		if rng.Intn(10) == 0 { // ~10% vào bảng khóa ghép, xen giữa để cắt khối
			a, b := rng.Intn(50), fmt.Sprintf("k'%d\"", rng.Intn(5))
			key := []model.Value{a, b}
			if _, ok := m2[[2]any{a, b}]; ok && rng.Intn(3) == 0 {
				rows = append(rows, model.RowChange{Table: t2, Op: model.OpDelete, Key: key})
				delete(m2, [2]any{a, b})
				continue
			}
			v := fmt.Sprintf("v%d", i)
			rows = append(rows, ins(t2, key, cv("a", a), cv("B b", b), cv("val", v)))
			m2[[2]any{a, b}] = v
			continue
		}
		// khóa nóng: 30% thao tác dồn vào 20 khóa → cùng PK lặp lại trong một khối
		k := rng.Intn(maxKey) + 1
		if rng.Intn(10) < 3 {
			k = rng.Intn(20) + 1
		}
		_, exists := m1[k]
		switch {
		case !exists:
			r := rec{int64(i) * 7, fmt.Sprintf("ins_%d", i)}
			rows = append(rows, ins(t1, []model.Value{k}, cv("id", k), cv("val", r.val), cv("status", r.status)))
			m1[k] = r
		case rng.Intn(10) < 7:
			r := rec{int64(i) * 11, fmt.Sprintf("upd_%d", i)}
			rows = append(rows, upd(t1, []model.Value{k}, cv("id", k), cv("val", r.val), cv("status", r.status)))
			m1[k] = r
		case rng.Intn(2) == 0:
			// đổi PK (ảnh đầy đủ) sang khóa chưa có
			nk := maxKey + 1 + rng.Intn(maxKey)
			if _, ok := m1[nk]; ok {
				continue
			}
			r := rec{int64(i), fmt.Sprintf("mv_%d", i)}
			rows = append(rows, model.RowChange{Table: t1, Op: model.OpUpdate, OldKey: []model.Value{k}, Key: []model.Value{nk},
				Cols: []model.ColValue{cv("id", nk), cv("val", r.val), cv("status", r.status)}})
			delete(m1, k)
			m1[nk] = r
		default:
			rows = append(rows, model.RowChange{Table: t1, Op: model.OpDelete, Key: []model.Value{k}})
			delete(m1, k)
		}
	}

	// Txn ~250 row, batch ~10 Txn: nhiều batch → bảng tạm và câu lệnh được dùng lại qua transaction.
	var txns []*model.Txn
	for i := 0; i < len(rows); i += 250 {
		txns = append(txns, &model.Txn{Rows: rows[i:min(i+250, len(rows))]})
	}
	nb := 0
	for i := 0; i < len(txns); i += 10 {
		nb++
		if err := w.ApplyBatch(ctx, txns[i:min(i+10, len(txns))], e.delta(fmt.Sprintf("b%d", nb))); err != nil {
			t.Fatalf("ApplyBatch %d: %v", nb, err)
		}
	}

	got1 := map[int]rec{}
	r1, err := e.db.Query(`SELECT id, val, status FROM public.cm_ac3`)
	if err != nil {
		t.Fatal(err)
	}
	for r1.Next() {
		var id int
		var r rec
		if err := r1.Scan(&id, &r.val, &r.status); err != nil {
			t.Fatal(err)
		}
		got1[id] = r
	}
	r1.Close()
	if len(got1) != len(m1) {
		t.Fatalf("cm_ac3: %d row, mô hình %d", len(got1), len(m1))
	}
	for k, r := range m1 {
		if got1[k] != r {
			t.Fatalf("cm_ac3 id=%d: got %+v, muốn %+v", k, got1[k], r)
		}
	}
	got2 := map[[2]any]string{}
	r2, err := e.db.Query(`SELECT a, "B b", val FROM public.cm_ac3_comp`)
	if err != nil {
		t.Fatal(err)
	}
	for r2.Next() {
		var a int
		var b, v string
		if err := r2.Scan(&a, &b, &v); err != nil {
			t.Fatal(err)
		}
		got2[[2]any{a, b}] = v
	}
	r2.Close()
	if len(got2) != len(m2) {
		t.Fatalf("cm_ac3_comp: %d row, mô hình %d", len(got2), len(m2))
	}
	for k, v := range m2 {
		if got2[k] != v {
			t.Fatalf("cm_ac3_comp %v: got %q, muốn %q", k, got2[k], v)
		}
	}
	if applied, bn := e.workerRow(t); applied != fmt.Sprintf("b%d", nb) || bn != int64(1+nb) {
		t.Fatalf("delta = (%q, %d), muốn (b%d, %d)", applied, bn, nb, 1+nb)
	}
	upserts := 0
	for _, r := range rows {
		if r.Op != model.OpDelete {
			upserts++
		}
	}
	mc, mr := w.GetStats()
	t.Logf("MERGE: %d câu, %d row (upsert trong luồng: %d)", mc, mr, upserts)
	if copyMinRows == 0 && (mc == 0 || mr == 0 || mr >= int64(upserts)) {
		t.Fatalf("thống kê MERGE bất thường: merges=%d rows=%d (khóa lặp phải được gộp ảnh cuối)", mc, mr)
	}
}

// AC4: lỗi giữa batch (MERGE thứ hai vi phạm ràng buộc, sau khi khối đầu đã MERGE thành công)
// → không row nào và không delta nào được ghi.
func TestCP1_PGWriter_AC4_AtomicRollback(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "atomic-ac4")
	ta := pgTable(t, e.db, "atomic_ac4_a", "id int PRIMARY KEY, val text")
	tb := pgTable(t, e.db, "atomic_ac4_b", "id int PRIMARY KEY, val int CHECK (val >= 0)")
	pgExec(t, e.db, `INSERT INTO public.atomic_ac4_a VALUES (100, 'old')`)
	w := e.writer(t, map[model.TableID][]string{ta: {"id"}, tb: {"id"}})
	w.SetCopyMinRows(0) // mọi khối qua COPY + MERGE
	_, bn0 := e.workerRow(t)

	var good []model.RowChange
	for i := 1; i <= 10; i++ {
		good = append(good, ins(ta, []model.Value{i}, cv("id", i), cv("val", fmt.Sprintf("row-%d", i))))
	}
	good = append(good, model.RowChange{Table: ta, Op: model.OpDelete, Key: []model.Value{100}})
	bad := []model.RowChange{
		ins(tb, []model.Value{1}, cv("id", 1), cv("val", 5)),
		ins(tb, []model.Value{2}, cv("id", 2), cv("val", -1)), // vi phạm CHECK
	}
	err := w.ApplyBatch(ctx, []*model.Txn{{Rows: good}, {Rows: bad}}, e.delta("atomic-fail"))
	if err == nil {
		t.Fatal("kỳ vọng ApplyBatch lỗi")
	}
	var na, nb int
	var v string
	_ = e.db.QueryRow(`SELECT count(*), max(val) FROM public.atomic_ac4_a`).Scan(&na, &v)
	_ = e.db.QueryRow(`SELECT count(*) FROM public.atomic_ac4_b`).Scan(&nb)
	if na != 1 || v != "old" || nb != 0 {
		t.Fatalf("vi phạm CP-1: dữ liệu commit dở dang (a=%d %q, b=%d)", na, v, nb)
	}
	if applied, bn := e.workerRow(t); applied == "atomic-fail" || bn != bn0 {
		t.Fatalf("vi phạm CP-1: row delta bị ghi (applied=%q batch_no=%d)", applied, bn)
	}

	// Kết nối dùng cho batch lỗi không được để lại transaction dở: batch sau phải chạy bình thường.
	if err := w.ApplyBatch(ctx, []*model.Txn{{Rows: good}}, e.delta("ok")); err != nil {
		t.Fatalf("batch sau lỗi: %v", err)
	}
	if applied, _ := e.workerRow(t); applied != "ok" {
		t.Fatalf("applied = %q", applied)
	}
}

// AC5: epoch fence: instance khác nâng epoch → ApplyBatch với epoch cũ bị từ chối (fenced, exit 4),
// không ghi dữ liệu. Cả nhánh row toàn cục (strict, DeltaRow.Global).
func TestSG1_PGWriter_AC5_EpochFenced(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "fence-ac5")
	tbl := pgTable(t, e.db, "fence_ac5", "id int PRIMARY KEY, val text")
	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})

	epoch2, err := e.store.AdvanceEpoch(ctx, e.pid, "owner-2", "hash-2")
	if err != nil || epoch2 <= e.epoch {
		t.Fatalf("advance epoch 2: %d %v", epoch2, err)
	}
	// Instance mới nhận row worker với epoch mới.
	if err := writer.NewPGWriter(e.db, e.pid, 1, epoch2).EnsureWorkerRow(ctx); err != nil {
		t.Fatal(err)
	}

	batch := []*model.Txn{{Rows: []model.RowChange{ins(tbl, []model.Value{10}, cv("id", 10), cv("val", "fenced"))}}}
	check := func(err error) {
		t.Helper()
		if err == nil {
			t.Fatal("kỳ vọng fenced với epoch cũ")
		}
		var xe *exitcode.Error
		if !errors.As(err, &xe) || xe.Code != exitcode.ErrFenced || exitcode.GetExitCode(err) != exitcode.Fenced {
			t.Fatalf("muốn fenced exit 4, got exit %d: %v", exitcode.GetExitCode(err), err)
		}
		var n int
		_ = e.db.QueryRow(`SELECT count(*) FROM public.fence_ac5`).Scan(&n)
		if n != 0 {
			t.Fatal("vi phạm SG-1: dữ liệu bị ghi dù epoch cũ")
		}
	}
	check(w.ApplyBatch(ctx, batch, e.delta("stale")))
	if applied, _ := e.workerRow(t); applied == "stale" {
		t.Fatal("vi phạm SG-1: row delta bị ghi")
	}

	// Nhánh strict: row toàn cục có điều kiện epoch.
	co := writer.NewCommitOrderer("strict", 1)
	w.SetCommitOrderer(co)
	batch[0].SetSeq(1)
	check(w.ApplyBatch(ctx, batch, writer.DeltaRow{PipelineID: e.pid, Epoch: e.epoch,
		Global: func() (string, error) { return "stale-global", nil }}))
}

// Strict (CP-5): WaitTurn trước BeginTx. seq 2 chạy trước và ghi CÙNG row với seq 1; nếu writer
// mở transaction rồi mới chờ lượt, seq 2 giữ row lock mà seq 1 cần → treo. Row toàn cục ghi trong
// transaction commit, tính sau khi đến lượt (không mất cập nhật).
func TestCP5_PGWriter_StrictWaitTurnBeforeBegin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e := newPGEnv(t, "strict-pg")
	tbl := pgTable(t, e.db, "strict_pg", "id int PRIMARY KEY, v int")
	pgExec(t, e.db, `INSERT INTO public.strict_pg VALUES (1, 0)`)

	co := writer.NewCommitOrderer("strict", 1)
	var mu sync.Mutex
	var appliedSeqs []string
	mk := func(seq uint64) *model.Txn {
		tx := &model.Txn{Rows: []model.RowChange{upd(tbl, []model.Value{1}, cv("id", 1), cv("v", int(seq)))}}
		tx.SetSeq(seq)
		return tx
	}
	apply := func(wid int, tx *model.Txn) error {
		w := writer.NewPGWriter(e.db, e.pid, wid, e.epoch)
		w.SetPrimaryKey(tbl, []string{"id"})
		w.SetCommitOrderer(co)
		return w.ApplyBatch(ctx, []*model.Txn{tx}, writer.DeltaRow{Global: func() (string, error) {
			mu.Lock()
			defer mu.Unlock()
			appliedSeqs = append(appliedSeqs, fmt.Sprint(tx.Seq()))
			return strings.Join(appliedSeqs, ","), nil
		}})
	}
	errs := make(chan error, 2)
	go func() { errs <- apply(2, mk(2)) }()
	time.Sleep(200 * time.Millisecond)
	go func() { errs <- apply(1, mk(1)) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var v int
	var applied string
	if err := e.db.QueryRow(`SELECT v FROM public.strict_pg WHERE id = 1`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1`, e.pid).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if v != 2 || applied != "1,2" {
		t.Fatalf("v=%d applied=%q, muốn v=2 applied=1,2", v, applied)
	}
}

// Kiểu giá trị qua COPY (binary) và qua tham số partial UPDATE/DELETE: time.Time, []byte → bytea,
// numeric dạng chuỗi, int64/uint64, float, bool, NULL, uuid/jsonb dạng chuỗi.
// Chạy cả hai đường ghi khối: COPY + MERGE và INSERT … ON CONFLICT.
func TestPGWriter_ValueTypes(t *testing.T) {
	t.Run("copy-merge", func(t *testing.T) { runValueTypes(t, 0) })
	t.Run("insert-on-conflict", func(t *testing.T) { runValueTypes(t, 1000) })
}

func runValueTypes(t *testing.T, copyMinRows int) {
	ctx := context.Background()
	e := newPGEnv(t, "pg-types")
	tbl := pgTable(t, e.db, "pg_types", `id bigint PRIMARY KEY, n numeric(30,10), ts timestamptz, tsn timestamp, d date,
		b bytea, f double precision, ok boolean, u uuid, j jsonb, s text, big text`)
	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
	w.SetCopyMinRows(copyMinRows)

	ts := time.Date(2024, 2, 29, 13, 14, 15, 123456000, time.UTC)
	blob := []byte{0, 1, 2, 0xff, '\\', 'x'}
	full := func(id int64, n string) model.RowChange {
		return ins(tbl, []model.Value{id}, cv("id", id), cv("n", n), cv("ts", ts), cv("tsn", ts), cv("d", ts.Truncate(24*time.Hour)),
			cv("b", blob), cv("f", 1.5), cv("ok", true), cv("u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
			cv("j", `{"k": [1, 2]}`), cv("s", "tab\tnew\nline \\N"), cv("big", "x"))
	}
	batch := []*model.Txn{{Rows: []model.RowChange{
		full(1, "12345678901234567890.0123456789"),
		full(2, "-0.5"),
		ins(tbl, []model.Value{uint64(3)}, cv("id", uint64(3)), cv("n", nil), cv("ts", nil), cv("tsn", nil), cv("d", nil),
			cv("b", nil), cv("f", nil), cv("ok", nil), cv("u", nil), cv("j", nil), cv("s", nil), cv("big", nil)),
	}}, {Rows: []model.RowChange{
		// partial UPDATE truyền kiểu qua tham số
		upd(tbl, []model.Value{int64(2)}, cv("id", int64(2)), cv("n", "7.25"), cv("ts", ts.Add(time.Hour)), cv("tsn", ts), cv("d", ts),
			cv("b", []byte{9}), cv("f", 2.5), cv("ok", false), cv("u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"), cv("j", `[]`), cv("s", "p"), unchanged("big")),
	}}}
	if err := w.ApplyBatch(ctx, batch, e.delta("types")); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	type row struct {
		n, s, u, j, big sql.NullString
		ts, tsn, d      sql.NullTime
		b               []byte
		f               sql.NullFloat64
		ok              sql.NullBool
	}
	read := func(id int) row {
		var r row
		if err := e.db.QueryRow(`SELECT n::text, ts, tsn, d, b, f, ok, u::text, j::text, s, big FROM public.pg_types WHERE id = $1`, id).
			Scan(&r.n, &r.ts, &r.tsn, &r.d, &r.b, &r.f, &r.ok, &r.u, &r.j, &r.s, &r.big); err != nil {
			t.Fatalf("read id=%d: %v", id, err)
		}
		return r
	}
	r1 := read(1)
	if r1.n.String != "12345678901234567890.0123456789" || !r1.ts.Time.Equal(ts) || !r1.tsn.Time.Equal(ts) ||
		r1.d.Time.Format("2006-01-02") != "2024-02-29" || !bytes.Equal(r1.b, blob) || r1.f.Float64 != 1.5 || !r1.ok.Bool ||
		r1.u.String != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" || r1.j.String != `{"k": [1, 2]}` || r1.s.String != "tab\tnew\nline \\N" {
		t.Fatalf("id=1 sai: %+v", r1)
	}
	r2 := read(2)
	if r2.n.String != "7.2500000000" || !r2.ts.Time.Equal(ts.Add(time.Hour)) || !bytes.Equal(r2.b, []byte{9}) || r2.ok.Bool ||
		r2.j.String != "[]" || r2.s.String != "p" || r2.big.String != "x" {
		t.Fatalf("id=2 sai: %+v", r2)
	}
	r3 := read(3)
	if r3.n.Valid || r3.ts.Valid || r3.b != nil || r3.ok.Valid || r3.s.Valid || r3.big.Valid {
		t.Fatalf("id=3 phải toàn NULL: %+v", r3)
	}
}

// INSERT mang cột Unchanged là ảnh hỏng: bị từ chối, không ghi NULL.
func TestPGWriter_InsertWithUnchangedRejected(t *testing.T) {
	e := newPGEnv(t, "pg-ins-unch")
	tbl := pgTable(t, e.db, "pg_ins_unch", "id int PRIMARY KEY, big text")
	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
	err := w.ApplyBatch(context.Background(), []*model.Txn{{Rows: []model.RowChange{
		ins(tbl, []model.Value{1}, cv("id", 1), unchanged("big")),
	}}}, e.delta("x"))
	if err == nil {
		t.Fatal("kỳ vọng lỗi")
	}
	var n int
	_ = e.db.QueryRow(`SELECT count(*) FROM public.pg_ins_unch`).Scan(&n)
	if n != 0 {
		t.Fatal("row bị ghi")
	}
}

// PGReplaceKeys (chunk re-read, 02 §8): chỉ đúng tập khóa được thay trong một transaction — khóa trong
// tập mà không có row thì bị xóa, khóa ngoài tập (kể cả nằm giữa min/max của tập) giữ nguyên.
func TestPGReplaceKeys_OnlyTheKeySet(t *testing.T) {
	e := newPGEnv(t, "pg-replace-keys")
	tbl := pgTable(t, e.db, "pg_replace_keys", "id bigint PRIMARY KEY, v text")
	pgExec(t, e.db, `INSERT INTO public.pg_replace_keys SELECT g, 'old' FROM generate_series(1, 10) g`)
	rows := []*model.RowChange{{Table: tbl, Op: model.OpInsert, Key: []model.Value{int64(3)}, Cols: []model.ColValue{cv("id", int64(3)), cv("v", "new3")}},
		{Table: tbl, Op: model.OpInsert, Key: []model.Value{int64(20)}, Cols: []model.ColValue{cv("id", int64(20)), cv("v", "new20")}}}
	if err := writer.PGReplaceKeys(context.Background(), e.db, tbl, "id", []string{"id", "v"}, rows, []int64{3, 7, 20}); err != nil {
		t.Fatal(err)
	}
	got := map[int64]string{}
	rs, err := e.db.Query(`SELECT id, v FROM public.pg_replace_keys`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	for rs.Next() {
		var id int64
		var v string
		_ = rs.Scan(&id, &v)
		got[id] = v
	}
	if len(got) != 10 || got[3] != "new3" || got[20] != "new20" || got[4] != "old" || got[10] != "old" {
		t.Fatalf("target %v: want 1..10 except 7 (old, 3 replaced) plus 20", got)
	}
	if _, ok := got[7]; ok {
		t.Fatal("key 7 is in the key set without a row: must be deleted")
	}
}

// BenchmarkPGWriter_10k so sánh 10.000 row upsert trên PostgreSQL:
//   - CopyMerge: PGWriter.ApplyBatch (COPY vào bảng tạm rồi MERGE, kèm row delta).
//   - MultiRowOnConflict: INSERT … VALUES (…),(…) ON CONFLICT DO UPDATE theo khối 500 row trong một transaction.
//
// Mỗi vòng: nửa đầu khóa đã tồn tại (UPDATE), nửa sau mới (INSERT); bảng được dựng lại ngoài giờ đo.
// Chạy: go test ./internal/writer -run '^$' -bench BenchmarkPGWriter_10k -benchtime 10x
func BenchmarkPGWriter_10k(b *testing.B) {
	ctx := context.Background()
	e := newPGEnv(b, "bench-pg")
	const numRows = 10000
	now := time.Now().UTC().Truncate(time.Microsecond)
	ddl := "id int PRIMARY KEY, name text, val int, created_at timestamptz"
	reset := func(b *testing.B, name string) model.TableID {
		tbl := pgTable(b, e.db, name, ddl)
		pgExec(b, e.db, fmt.Sprintf(`INSERT INTO public.%q SELECT g, 'old', 0, now() FROM generate_series(1, %d) g`, name, numRows/2))
		pgExec(b, e.db, fmt.Sprintf(`ANALYZE public.%q`, name))
		return tbl
	}

	b.Run("CopyMerge", func(b *testing.B) {
		tbl := model.TableID{Schema: "public", Table: "bench_cm"}
		w := e.writer(b, map[model.TableID][]string{tbl: {"id"}})
		rows := make([]model.RowChange, numRows)
		for i := range rows {
			rows[i] = ins(tbl, []model.Value{i + 1}, cv("id", i+1), cv("name", fmt.Sprintf("bench_%d", i+1)), cv("val", i*10), cv("created_at", now))
		}
		batch := []*model.Txn{{Rows: rows}}
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			reset(b, "bench_cm")
			b.StartTimer()
			if err := w.ApplyBatch(ctx, batch, e.delta(fmt.Sprintf("bench-%d", i))); err != nil {
				b.Fatalf("ApplyBatch: %v", err)
			}
		}
		b.ReportMetric(float64(numRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
	})

	b.Run("MultiRowOnConflict", func(b *testing.B) {
		const chunk = 500
		for iter := 0; iter < b.N; iter++ {
			b.StopTimer()
			reset(b, "bench_oc")
			b.StartTimer()
			tx, err := e.db.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			for c := 0; c < numRows; c += chunk {
				var vals []string
				var args []any
				for id := c + 1; id <= min(c+chunk, numRows); id++ {
					n := len(args)
					vals = append(vals, fmt.Sprintf("($%d, $%d, $%d, $%d)", n+1, n+2, n+3, n+4))
					args = append(args, id, fmt.Sprintf("bench_%d", id), (id-1)*10, now)
				}
				q := `INSERT INTO public.bench_oc (id, name, val, created_at) VALUES ` + strings.Join(vals, ", ") +
					` ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val, created_at = EXCLUDED.created_at`
				if _, err := tx.ExecContext(ctx, q, args...); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(numRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
	})
}

// Bảng có UNIQUE ngoài PK (hoặc ràng buộc loại trừ): row phải được áp đúng thứ tự nguồn, không gộp
// ảnh cuối, không MERGE. Hoán đổi cột UNIQUE trong MỘT Txn (a: Ca→tmp, b: Cb→Ca, a: tmp→Cb) và xoay
// vòng qua nhiều Txn liên tiếp trong một batch. Chạy cả khi ép mọi khối qua COPY + MERGE.
//
// Ghi chú: trước khi có đường "ordered", test này thất bại ở cả hai chế độ (postgres:18):
//   - copy-merge-only: merge into public.uniq_accounts: ERROR: duplicate key value violates unique
//     constraint "uniq_accounts_code_key" (SQLSTATE 23505)
//   - default: upsert into public.uniq_accounts: ERROR: duplicate key value violates unique
//     constraint "uniq_accounts_code_key" (SQLSTATE 23505)
//
// vì gộp ảnh cuối bỏ mất bước trung gian (a=tmp-Ca) và MERGE/INSERT nhiều row thấy a=Cb khi b vẫn là Cb.
func TestPGWriter_UniqueSwapOrdered(t *testing.T) {
	for _, mode := range []struct {
		name string
		min  int
	}{{"copy-merge-only", 0}, {"default", -1}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := context.Background()
			e := newPGEnv(t, "uniq-swap")
			acc := pgTable(t, e.db, "uniq_accounts", "id int PRIMARY KEY, code text UNIQUE, bal int")
			rng := pgTable(t, e.db, "uniq_ranges", "id int PRIMARY KEY, r int4range, EXCLUDE USING gist (r WITH &&)")
			pgExec(t, e.db, `INSERT INTO public.uniq_accounts VALUES (1, 'Ca', 10), (2, 'Cb', 20), (11, 'C1', 0), (12, 'C2', 0), (13, 'C3', 0)`)
			pgExec(t, e.db, `INSERT INTO public.uniq_ranges VALUES (1, '[1,2)'), (2, '[3,4)')`)
			w := e.writer(t, map[model.TableID][]string{acc: {"id"}, rng: {"id"}})
			if mode.min >= 0 {
				w.SetCopyMinRows(mode.min)
			}
			a := func(id int, code string, bal int) model.RowChange {
				return upd(acc, []model.Value{id}, cv("id", id), cv("code", code), cv("bal", bal))
			}
			r := func(id int, rg string) model.RowChange {
				return upd(rng, []model.Value{id}, cv("id", id), cv("r", rg))
			}
			batch := []*model.Txn{
				// hoán đổi trong một Txn
				{Rows: []model.RowChange{a(1, "tmp-Ca", 10), a(2, "Ca", 20), a(1, "Cb", 11)}},
				// xoay vòng 11→C2, 12→C3, 13→C1 qua các Txn liên tiếp
				{Rows: []model.RowChange{a(11, "tmp-11", 0)}},
				{Rows: []model.RowChange{a(13, "C1", 0)}},
				{Rows: []model.RowChange{a(12, "C3", 0)}},
				{Rows: []model.RowChange{a(11, "C2", 0)}},
				// ràng buộc loại trừ: hoán đổi khoảng trong một Txn
				{Rows: []model.RowChange{r(1, "[10,11)"), r(2, "[1,2)"), r(1, "[3,4)")}},
			}
			if err := w.ApplyBatch(ctx, batch, e.delta("swap")); err != nil {
				t.Fatalf("ApplyBatch: %v", err)
			}
			want := map[int]string{1: "Cb", 2: "Ca", 11: "C2", 12: "C3", 13: "C1"}
			for id, code := range want {
				var got string
				if err := e.db.QueryRow(`SELECT code FROM public.uniq_accounts WHERE id = $1`, id).Scan(&got); err != nil || got != code {
					t.Fatalf("id=%d code=%q err=%v, muốn %q", id, got, err, code)
				}
			}
			var bal int
			_ = e.db.QueryRow(`SELECT bal FROM public.uniq_accounts WHERE id = 1`).Scan(&bal)
			if bal != 11 {
				t.Fatalf("bal id=1 = %d, muốn 11", bal)
			}
			for id, rg := range map[int]string{1: "[3,4)", 2: "[1,2)"} {
				var got string
				if err := e.db.QueryRow(`SELECT r::text FROM public.uniq_ranges WHERE id = $1`, id).Scan(&got); err != nil || got != rg {
					t.Fatalf("range id=%d = %q err=%v, muốn %q", id, got, err, rg)
				}
			}
			if applied, _ := e.workerRow(t); applied != "swap" {
				t.Fatalf("applied = %q", applied)
			}
		})
	}
}
