package writer

// Mô phỏng tất định (F11) phía target: pgApplier THẬT (gom khối COPY+MERGE có khử trùng PK, INSERT …
// ON CONFLICT cho khối nhỏ, gom DELETE, hàng đợi tuần tự của bảng "ordered", partial UPDATE TOAST, đổi
// PK) chạy trên một pgx.Tx GIẢ diễn giải đúng các câu lệnh writer sinh ra trên bảng trong bộ nhớ (có
// ràng buộc UNIQUE ngoài PK như PG: kiểm ngay từng câu lệnh), cùng sched.Scheduler THẬT và vòng worker
// relaxed của pipeline chép lại (gatherBatch, applyRelaxed, finishBatch, worker làn, applyInSeqOrder khi
// vi phạm unique). Kiểm tra: không row nào của Txn đã Admit bị mất (target cuối == source), ver của mỗi
// khóa ở target không bao giờ giảm sau mỗi commit, không lỗi ngoài dự kiến.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/model"
	"strand/internal/sched"
)

// ---------------------------------------------------------------------------
// DB giả
// ---------------------------------------------------------------------------

type fRow map[string]any

type fDB struct {
	tabs    map[string]map[int64]fRow // bảng (quotePGTable) → id → row
	ordered map[string]bool
	unique  map[string]string // bảng → cột UNIQUE ngoài PK
	stmts   int
	delta   string // row delta đã commit gần nhất
}

func (d *fDB) clone() map[string]map[int64]fRow {
	out := map[string]map[int64]fRow{}
	for t, rows := range d.tabs {
		m := map[int64]fRow{}
		for k, r := range rows {
			c := fRow{}
			for n, v := range r {
				c[n] = v
			}
			m[k] = c
		}
		out[t] = m
	}
	return out
}

type fTx struct {
	db      *fDB
	tabs    map[string]map[int64]fRow
	tmpOf   map[string]string // bảng tạm → bảng đích
	tmpRows map[string][]fRow
	done    bool
	delta   string
}

func (d *fDB) begin() *fTx {
	return &fTx{db: d, tabs: d.clone(), tmpOf: map[string]string{}, tmpRows: map[string][]fRow{}}
}

var (
	reTemp   = regexp.MustCompile(`^CREATE TEMP TABLE "([^"]+)" \(LIKE (\S+) INCLUDING DEFAULTS\) ON COMMIT DROP$`)
	reMerge  = regexp.MustCompile(`^MERGE INTO (\S+) AS _strand_t USING pg_temp\."([^"]+)" AS _strand_s ON \(.*?\) (WHEN MATCHED THEN (?:DO NOTHING|UPDATE SET .*?)) WHEN NOT MATCHED THEN INSERT \(.*?\) OVERRIDING SYSTEM VALUE VALUES \(.*?\); TRUNCATE pg_temp\."([^"]+)"$`)
	reInsert = regexp.MustCompile(`^INSERT INTO (\S+) \((.*?)\) OVERRIDING SYSTEM VALUE VALUES .* ON CONFLICT \((.*?)\) (DO NOTHING|DO UPDATE SET .*)$`)
	reDelete = regexp.MustCompile(`^DELETE FROM (\S+) WHERE \((.*?)\) IN \((.*)\)$`)
	reUpdate = regexp.MustCompile(`^UPDATE (\S+) SET (.*?) WHERE (.*?)(?: AND NOT EXISTS \(SELECT 1 FROM \S+ WHERE (.*)\))?$`)
	reSel1   = regexp.MustCompile(`^SELECT 1 FROM (\S+) WHERE (.*)$`)
	reAssign = regexp.MustCompile(`"(\w+)" = \$(\d+)`)
)

func splitCols(s string) []string {
	var out []string
	for _, c := range strings.Split(s, ", ") {
		out = append(out, strings.Trim(c, `"`))
	}
	return out
}

func toI64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	}
	panic(fmt.Sprintf("key %T", v))
}

func errCode(code, msg string) error { return &pgconn.PgError{Code: code, Message: msg} }

// checkUnique: UNIQUE ngoài PK kiểm ngay sau mỗi câu lệnh (không deferrable).
func (tx *fTx) checkUnique(tab string) error {
	col, ok := tx.db.unique[tab]
	if !ok {
		return nil
	}
	seen := map[any]int64{}
	for id, r := range tx.tabs[tab] {
		v := r[col]
		if o, dup := seen[v]; dup {
			return errCode("23505", fmt.Sprintf("duplicate key value %v in %s (ids %d, %d)", v, tab, o, id))
		}
		seen[v] = id
	}
	return nil
}

func (tx *fTx) upsert(tab string, rows []fRow, update bool) error {
	touched := map[int64]bool{}
	for _, r := range rows {
		id := toI64(r["id"])
		if touched[id] {
			return errCode("21000", "command cannot affect row a second time")
		}
		touched[id] = true
		cur, ok := tx.tabs[tab][id]
		if !ok {
			c := fRow{}
			for n, v := range r {
				c[n] = v
			}
			tx.tabs[tab][id] = c
			continue
		}
		if update {
			for n, v := range r {
				cur[n] = v
			}
		}
	}
	return tx.checkUnique(tab)
}

func (tx *fTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tx.db.stmts++
	if strings.HasPrefix(sql, "UPDATE _strand_worker_delta SET applied = $1") {
		tx.delta = args[0].(string) // row delta của worker (ApplyBatch qua SetTxBeginner)
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	if m := reTemp.FindStringSubmatch(sql); m != nil {
		tx.tmpOf[m[1]] = m[2]
		return pgconn.NewCommandTag("CREATE TABLE"), nil
	}
	if m := reMerge.FindStringSubmatch(sql); m != nil {
		rows := tx.tmpRows[m[2]]
		err := tx.upsert(m[1], rows, strings.Contains(m[3], "UPDATE SET"))
		tx.tmpRows[m[4]] = nil
		return pgconn.NewCommandTag(fmt.Sprintf("MERGE %d", len(rows))), err
	}
	if m := reInsert.FindStringSubmatch(sql); m != nil {
		cols := splitCols(m[2])
		var rows []fRow
		for i := 0; i < len(args); i += len(cols) {
			r := fRow{}
			for j, c := range cols {
				r[c] = args[i+j]
			}
			rows = append(rows, r)
		}
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", len(rows))), tx.upsert(m[1], rows, m[4] != "DO NOTHING")
	}
	if m := reDelete.FindStringSubmatch(sql); m != nil {
		n := 0
		for _, a := range args {
			if _, ok := tx.tabs[m[1]][toI64(a)]; ok {
				n++
			}
			delete(tx.tabs[m[1]], toI64(a))
		}
		return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
	}
	if m := reUpdate.FindStringSubmatch(sql); m != nil {
		tab := m[1]
		arg := func(s string) any { i, _ := strconv.Atoi(s); return args[i-1] }
		where := reAssign.FindStringSubmatch(m[3])
		id := toI64(arg(where[2]))
		cur, ok := tx.tabs[tab][id]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		if m[4] != "" {
			nw := reAssign.FindStringSubmatch(m[4])
			if _, exists := tx.tabs[tab][toI64(arg(nw[2]))]; exists {
				return pgconn.NewCommandTag("UPDATE 0"), nil
			}
		}
		for _, s := range reAssign.FindAllStringSubmatch(m[2], -1) {
			cur[s[1]] = arg(s[2])
		}
		if nid := toI64(cur["id"]); nid != id {
			if _, exists := tx.tabs[tab][nid]; exists {
				return pgconn.CommandTag{}, errCode("23505", "duplicate pk on update")
			}
			delete(tx.tabs[tab], id)
			tx.tabs[tab][nid] = cur
		}
		return pgconn.NewCommandTag("UPDATE 1"), tx.checkUnique(tab)
	}
	return pgconn.CommandTag{}, fmt.Errorf("fake tx: unsupported statement %q", sql)
}

type fRowRes struct {
	vals []any
	err  error
}

func (r fRowRes) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *int:
			*p = r.vals[i].(int)
		case *bool:
			*p = r.vals[i].(bool)
		}
	}
	return nil
}

func (tx *fTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	if strings.HasPrefix(sql, "SELECT EXISTS") {
		return fRowRes{vals: []any{tx.db.ordered[args[0].(string)]}}
	}
	if m := reSel1.FindStringSubmatch(sql); m != nil {
		w := reAssign.FindStringSubmatch(m[2])
		i, _ := strconv.Atoi(w[2])
		if _, ok := tx.tabs[m[1]][toI64(args[i-1])]; !ok {
			return fRowRes{err: pgx.ErrNoRows}
		}
		return fRowRes{vals: []any{1}}
	}
	return fRowRes{err: fmt.Errorf("fake tx: unsupported query %q", sql)}
}

func (tx *fTx) CopyFrom(_ context.Context, name pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	tmp := name[len(name)-1]
	if _, ok := tx.tmpOf[tmp]; !ok {
		return 0, fmt.Errorf("copy into unknown temp table %s", tmp)
	}
	var n int64
	for src.Next() {
		vals, err := src.Values()
		if err != nil {
			return n, err
		}
		r := fRow{}
		for i, c := range cols {
			r[c] = vals[i]
		}
		tx.tmpRows[tmp] = append(tx.tmpRows[tmp], r)
		n++
	}
	return n, src.Err()
}

type fBatchRes struct {
	tx  *fTx
	q   []*pgx.QueuedQuery
	i   int
	err error
}

func (b *fBatchRes) Exec() (pgconn.CommandTag, error) {
	if b.err != nil {
		return pgconn.CommandTag{}, b.err
	}
	q := b.q[b.i]
	b.i++
	tag, err := b.tx.Exec(context.Background(), q.SQL, q.Arguments...)
	if err != nil {
		b.err = err // PG: phần còn lại của pipeline bị bỏ
	}
	return tag, err
}
func (b *fBatchRes) Query() (pgx.Rows, error) { panic("unused") }
func (b *fBatchRes) QueryRow() pgx.Row        { panic("unused") }
func (b *fBatchRes) Close() error             { return b.err }
func (b *fBatchRes) Tx() pgx.Tx               { return b.tx }

func (tx *fTx) SendBatch(_ context.Context, b *pgx.Batch) pgx.BatchResults {
	return &fBatchRes{tx: tx, q: b.QueuedQueries}
}

func (tx *fTx) Begin(context.Context) (pgx.Tx, error) { panic("unused") }
func (tx *fTx) Commit(context.Context) error {
	if tx.done {
		return pgx.ErrTxClosed
	}
	tx.db.tabs, tx.done = tx.tabs, true
	tx.db.delta = tx.delta
	return nil
}
func (tx *fTx) Rollback(context.Context) error { tx.done = true; return nil }
func (tx *fTx) LargeObjects() pgx.LargeObjects { panic("unused") }
func (tx *fTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("unused")
}
func (tx *fTx) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unused") }
func (tx *fTx) Conn() *pgx.Conn                                         { panic("unused") }

// ---------------------------------------------------------------------------
// Source + pipeline (vòng worker relaxed chép từ pipeline.go)
// ---------------------------------------------------------------------------

var (
	wsItems    = model.TableID{Schema: "app", Table: "items"}    // chỉ PK, cột TOAST doc
	wsEvents   = model.TableID{Schema: "app", Table: "events"}   // chỉ PK
	wsAccounts = model.TableID{Schema: "app", Table: "accounts"} // UNIQUE(code): ordered, làn
)

type wsWorker struct {
	lane    bool
	batch   []*model.Txn
	ordered []*model.Txn // applyInSeqOrder đang chạy
	idx     int
	attempt int
}

type wSim struct {
	t       *testing.T
	r       *rand.Rand
	name    string
	src     map[model.TableID]map[int64]fRow
	db      *fDB
	w       *PGWriter
	sch     *sched.Scheduler
	workers []*wsWorker
	pending []*model.Txn // source đã commit, chưa route
	adm     uint64
	ver     int64
	nextID  int64
	lastVer map[string]int64
	copyMin int
	laneOn  bool // relaxed source PG bật làn; tắt: Txn accounts đi worker thường (đường applyInSeqOrder)
	reorder int
	errs    []string
	st      map[string]int
}

func newWSim(t *testing.T, seed int64) *wSim {
	s := &wSim{t: t, r: rand.New(rand.NewSource(seed)), name: fmt.Sprintf("seed %d", seed),
		src: map[model.TableID]map[int64]fRow{wsItems: {}, wsEvents: {}, wsAccounts: {}},
		db: &fDB{tabs: map[string]map[int64]fRow{}, ordered: map[string]bool{quotePGTable(wsAccounts): true},
			unique: map[string]string{quotePGTable(wsAccounts): "code"}},
		w: NewPGWriter(nil, "p", 1, 1), sch: sched.NewScheduler(sched.DefaultConfig()), ver: 1000, nextID: 1000,
		lastVer: map[string]int64{}, st: map[string]int{}}
	s.copyMin = []int{0, 2, 3, 1000}[s.r.Intn(4)]
	s.laneOn = s.r.Intn(2) == 0
	s.w.SetCopyMinRows(s.copyMin)
	for _, tb := range []model.TableID{wsItems, wsEvents, wsAccounts} {
		s.w.SetPrimaryKey(tb, []string{"id"})
		s.db.tabs[quotePGTable(tb)] = map[int64]fRow{}
	}
	for id := int64(1); id <= 12; id++ {
		s.ver++
		s.src[wsItems][id] = fRow{"id": id, "ver": s.ver, "qty": int64(0), "doc": fmt.Sprintf("d%d", id)}
		s.src[wsEvents][id] = fRow{"id": id, "ver": s.ver}
		s.src[wsAccounts][id] = fRow{"id": id, "ver": s.ver, "code": fmt.Sprintf("C%d", id)}
	}
	for tb, rows := range s.src {
		for id, r := range rows {
			c := fRow{}
			for n, v := range r {
				c[n] = v
			}
			s.db.tabs[quotePGTable(tb)][id] = c
		}
	}
	for i := 0; i < 3; i++ {
		s.workers = append(s.workers, &wsWorker{})
	}
	s.workers = append(s.workers, &wsWorker{lane: true})
	return s
}

func (s *wSim) nv() int64 { s.ver++; return s.ver }

func (s *wSim) pick(tb model.TableID) (int64, bool) {
	var ids []int64
	for id := range s.src[tb] {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return 0, false
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if s.r.Intn(2) == 0 && len(ids) > 4 {
		ids = ids[:4] // khóa nóng
	}
	return ids[s.r.Intn(len(ids))], true
}

// image dựng RowChange như reader PG + assembler (UPDATE thiếu cột TOAST → Unchanged; UNIQUE chỉ có giá trị mới).
func (s *wSim) image(tb model.TableID, op model.Op, id int64, oldID int64, toastUnchanged bool) model.RowChange {
	rc := model.RowChange{Table: tb, Op: op, Key: []model.Value{id}}
	if oldID != 0 {
		rc.OldKey = []model.Value{oldID}
	}
	if op == model.OpDelete {
		return rc
	}
	r := s.src[tb][id]
	names := []string{"id", "ver"}
	switch tb {
	case wsItems:
		names = append(names, "qty", "doc")
	case wsAccounts:
		names = append(names, "code")
		rc.UniqueKeys = []model.UniqueKey{{Name: "accounts_code_key", Key: []model.Value{r["code"]}}}
	}
	for _, n := range names {
		cv := model.ColValue{Name: n, Value: r[n]}
		if n == "doc" && toastUnchanged {
			cv = model.ColValue{Name: n, Unchanged: true}
		}
		rc.Cols = append(rc.Cols, cv)
	}
	return rc
}

func (s *wSim) freeID(tb model.TableID) int64 {
	for {
		id := 1 + s.r.Int63n(30)
		if s.r.Intn(3) == 0 {
			s.nextID++
			id = s.nextID
		}
		if _, ok := s.src[tb][id]; !ok {
			return id
		}
	}
}

// genTxn: một Txn ở source (commit theo thứ tự sinh).
func (s *wSim) genTxn() {
	tx := &model.Txn{}
	add := func(rc model.RowChange) { tx.Rows = append(tx.Rows, rc) }
	itemUpd := func(id int64, toast bool) {
		r := s.src[wsItems][id]
		r["ver"], r["qty"] = s.nv(), r["qty"].(int64)+1
		if !toast {
			r["doc"] = fmt.Sprintf("d%d-%d", id, s.ver)
		}
		add(s.image(wsItems, model.OpUpdate, id, 0, toast))
	}
	switch c := s.r.Intn(100); {
	case c < 20:
		if id, ok := s.pick(wsItems); ok {
			itemUpd(id, s.r.Intn(3) > 0)
		}
	case c < 26:
		id := s.freeID(wsItems)
		s.src[wsItems][id] = fRow{"id": id, "ver": s.nv(), "qty": int64(1), "doc": fmt.Sprintf("n%d", s.ver)}
		add(s.image(wsItems, model.OpInsert, id, 0, false))
	case c < 30:
		if id, ok := s.pick(wsItems); ok {
			delete(s.src[wsItems], id)
			add(s.image(wsItems, model.OpDelete, id, 0, false))
		}
	case c < 36: // đổi PK items (ảnh đầy đủ hoặc doc không đổi)
		if id, ok := s.pick(wsItems); ok {
			nid := s.freeID(wsItems)
			r := s.src[wsItems][id]
			delete(s.src[wsItems], id)
			r["id"], r["ver"] = nid, s.nv()
			s.src[wsItems][nid] = r
			add(s.image(wsItems, model.OpUpdate, nid, id, s.r.Intn(2) == 0))
		}
	case c < 52: // upsert events / chèn nhiều events
		for n := 1 + s.r.Intn(3); n > 0; n-- {
			id := int64(1 + s.r.Intn(8))
			op := model.OpUpdate
			if _, ok := s.src[wsEvents][id]; !ok {
				op = model.OpInsert
			}
			s.src[wsEvents][id] = fRow{"id": id, "ver": s.nv()}
			add(s.image(wsEvents, op, id, 0, false))
		}
	case c < 60: // đổi PK events / xóa
		if id, ok := s.pick(wsEvents); ok {
			if s.r.Intn(3) == 0 {
				delete(s.src[wsEvents], id)
				add(s.image(wsEvents, model.OpDelete, id, 0, false))
				break
			}
			nid := s.freeID(wsEvents)
			delete(s.src[wsEvents], id)
			s.src[wsEvents][nid] = fRow{"id": nid, "ver": s.nv()}
			add(s.image(wsEvents, model.OpUpdate, nid, id, false))
		}
	case c < 70: // hoán đổi code giữa hai account (ba UPDATE trong một Txn)
		a, ok1 := s.pick(wsAccounts)
		b, ok2 := s.pick(wsAccounts)
		if !ok1 || !ok2 || a == b {
			return
		}
		ra, rb := s.src[wsAccounts][a], s.src[wsAccounts][b]
		ca, cb := ra["code"], rb["code"]
		ra["code"], ra["ver"] = "tmp-"+ca.(string), s.nv()
		add(s.image(wsAccounts, model.OpUpdate, a, 0, false))
		rb["code"], rb["ver"] = ca, s.nv()
		add(s.image(wsAccounts, model.OpUpdate, b, 0, false))
		ra["code"], ra["ver"] = cb, s.nv()
		add(s.image(wsAccounts, model.OpUpdate, a, 0, false))
	case c < 78: // moveCode: Txn 1 giải phóng X, Txn 2 gán X cho account khác
		a, ok1 := s.pick(wsAccounts)
		b, ok2 := s.pick(wsAccounts)
		if !ok1 || !ok2 || a == b {
			return
		}
		ra, rb := s.src[wsAccounts][a], s.src[wsAccounts][b]
		x := ra["code"]
		ra["code"], ra["ver"] = fmt.Sprintf("M%d-%d", a, s.ver), s.nv()
		add(s.image(wsAccounts, model.OpUpdate, a, 0, false))
		s.pending = append(s.pending, tx)
		tx = &model.Txn{}
		rb["code"], rb["ver"] = x, s.nv()
		add(s.image(wsAccounts, model.OpUpdate, b, 0, false))
	case c < 82: // chèn account
		id := s.freeID(wsAccounts)
		s.src[wsAccounts][id] = fRow{"id": id, "ver": s.nv(), "code": fmt.Sprintf("N%d", id)}
		add(s.image(wsAccounts, model.OpInsert, id, 0, false))
	default: // Txn nhiều bảng: item (partial) + events mới + account
		id, ok := s.pick(wsItems)
		if !ok {
			return
		}
		itemUpd(id, true)
		for n := 1 + s.r.Intn(3); n > 0; n-- {
			s.nextID++
			e := s.nextID
			s.src[wsEvents][e] = fRow{"id": e, "ver": s.nv()}
			add(s.image(wsEvents, model.OpInsert, e, 0, false))
		}
		if e := int64(1 + s.r.Intn(8)); s.r.Intn(2) == 0 { // khóa events nóng: chuỗi lây làn xóa/chèn cùng khóa
			if _, ok := s.src[wsEvents][e]; ok && s.r.Intn(2) == 0 {
				delete(s.src[wsEvents], e)
				add(s.image(wsEvents, model.OpDelete, e, 0, false))
			} else {
				op := model.OpUpdate
				if !ok {
					op = model.OpInsert
				}
				s.src[wsEvents][e] = fRow{"id": e, "ver": s.nv()}
				add(s.image(wsEvents, op, e, 0, false))
			}
		}
		if _, ok := s.src[wsAccounts][id]; ok && s.r.Intn(3) > 0 {
			r := s.src[wsAccounts][id]
			r["ver"] = s.nv()
			add(s.image(wsAccounts, model.OpUpdate, id, 0, false))
		}
	}
	if len(tx.Rows) > 0 {
		s.pending = append(s.pending, tx)
	}
}

// admit: router (đường không snapshot): Lane khi chạm bảng làn, Admit vào scheduler thật.
func (s *wSim) admit() bool {
	if len(s.pending) == 0 {
		return false
	}
	tx := s.pending[0]
	s.pending = s.pending[1:]
	for _, rc := range tx.Rows {
		if rc.Table == wsAccounts && s.laneOn {
			tx.Lane = true
		}
	}
	lane := tx.Lane
	if err := s.sch.Admit(context.Background(), tx); err != nil {
		s.t.Fatal(err)
	}
	if tx.Lane && !lane {
		s.st["contagion"]++
	}
	s.adm = tx.Seq()
	return true
}

// applyBatch: PGWriter.applyOnConn (phần dữ liệu) trên tx giả, một transaction.
func (s *wSim) applyBatch(batch []*model.Txn) error {
	tx := s.db.begin()
	a := &pgApplier{w: s.w, ctx: context.Background(), tx: tx, tmp: make(map[model.TableID]string), copyMinRows: s.copyMin}
	for _, t := range batch {
		if err := t.ForEachRow(a.apply); err != nil {
			_ = tx.Rollback(context.Background())
			return err
		}
	}
	if err := a.flushAll(); err != nil {
		_ = tx.Rollback(context.Background())
		return err
	}
	// Bộ phát hiện F11 (checkRowsWritten) không được báo nhầm trên mọi đường apply của writer.
	var routed int64
	for _, t := range batch {
		routed += t.TotalRows()
	}
	if err := checkRowsWritten(batch, routed, a.handed, a.written); err != nil {
		s.errs = append(s.errs, err.Error())
	}
	s.st["rows"] += int(a.written)
	_ = tx.Commit(context.Background())
	s.checkMonotonic()
	return nil
}

func (s *wSim) checkMonotonic() {
	for tab, rows := range s.db.tabs {
		for id, r := range rows {
			k := fmt.Sprintf("%s/%d", tab, id)
			v := toI64(r["ver"])
			if v < s.lastVer[k] {
				s.errs = append(s.errs, fmt.Sprintf("%s ver %d -> %d", k, s.lastVer[k], v))
			}
			if v > s.lastVer[k] {
				s.lastVer[k] = v
			}
		}
	}
}

func (s *wSim) done(batch []*model.Txn) {
	for _, t := range batch {
		s.sch.Done(t.Seq(), nil)
	}
}

// workerStep: một bước của worker (lấy batch / applyRelaxed / một lần thử của applyInSeqOrder).
func (s *wSim) workerStep(w *wsWorker, mayTimeout bool) bool {
	switch {
	case w.ordered != nil:
		t := w.ordered[w.idx]
		if w.attempt > 0 && s.sch.Lwd() < t.Seq()-1 && !mayTimeout {
			return false // waitLwd(seq-1, 2s) chưa xong
		}
		err := s.applyBatch([]*model.Txn{t})
		switch {
		case err == nil:
			s.done([]*model.Txn{t})
			w.idx, w.attempt = w.idx+1, 0
			if w.idx == len(w.ordered) {
				w.ordered = nil
			}
		case IsPGUniqueViolation(err):
			w.attempt++
			if w.attempt >= 100 {
				s.errs = append(s.errs, fmt.Sprintf("seq %d still failing after 100 attempts: %v", t.Seq(), err))
				w.ordered = nil
			}
		default:
			s.errs = append(s.errs, fmt.Sprintf("seq %d in-order apply: %v", t.Seq(), err))
			w.ordered = nil
		}
		return true
	case w.batch != nil:
		err := s.applyBatch(w.batch)
		switch {
		case err == nil:
			s.done(w.batch)
		case IsPGUniqueViolation(err):
			s.reorder++
			w.ordered = append([]*model.Txn(nil), w.batch...)
			sort.Slice(w.ordered, func(i, j int) bool { return w.ordered[i].Seq() < w.ordered[j].Seq() })
			w.idx, w.attempt = 0, 0
		default:
			var te *ToastRowMissingError
			if errors.As(err, &te) {
				s.errs = append(s.errs, fmt.Sprintf("toast row missing: %v", err))
			} else {
				s.errs = append(s.errs, fmt.Sprintf("batch: %v", err))
			}
		}
		w.batch = nil
		return true
	case w.lane:
		if s.sch.LanePending() == 0 {
			return false
		}
		wait := 5 * time.Millisecond
		if mayTimeout {
			wait = 2 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		w.batch = s.sch.NextLaneBatch(ctx, 1+s.r.Intn(6), 0)
		return w.batch != nil
	default: // gatherBatch: lấy mọi Txn đang sẵn sàng tới trần
		n := 1 + s.r.Intn(6)
		for len(w.batch) < n {
			select {
			case t := <-s.sch.ReadyCh():
				w.batch = append(w.batch, t)
				continue
			default:
			}
			break
		}
		return w.batch != nil
	}
}

func (s *wSim) run() {
	for i := 0; i < 400; i++ {
		switch x := s.r.Intn(10); {
		case x < 3:
			s.genTxn()
		case x < 5:
			s.admit()
		default:
			s.workerStep(s.workers[s.r.Intn(len(s.workers))], s.r.Intn(40) == 0)
		}
		if len(s.errs) > 0 {
			return
		}
	}
	for s.admit() {
	}
	for guard := 0; s.sch.Lwd() < s.adm && len(s.errs) == 0; guard++ {
		progressed := false
		for _, w := range s.workers {
			if s.workerStep(w, false) {
				progressed = true
			}
		}
		if !progressed {
			for _, w := range s.workers {
				if s.workerStep(w, true) {
					progressed = true
				}
			}
		}
		if !progressed || guard > 100000 {
			s.errs = append(s.errs, fmt.Sprintf("stuck: lwd %d admitted %d", s.sch.Lwd(), s.adm))
		}
	}
}

func (s *wSim) diff() []string {
	var out []string
	for tb, rows := range s.src {
		got := s.db.tabs[quotePGTable(tb)]
		for id, r := range rows {
			g, ok := got[id]
			if !ok {
				out = append(out, fmt.Sprintf("%s/%d missing (source %v)", tb, id, r))
				continue
			}
			for n, v := range r {
				if fmt.Sprint(g[n]) != fmt.Sprint(v) {
					out = append(out, fmt.Sprintf("%s/%d col %s source %v target %v", tb, id, n, v, g[n]))
				}
			}
		}
		for id := range got {
			if _, ok := rows[id]; !ok {
				out = append(out, fmt.Sprintf("%s/%d extra on target %v", tb, id, got[id]))
			}
		}
	}
	sort.Strings(out)
	return out
}

func TestF11ApplySim_RelaxedPGWriter(t *testing.T) {
	n := 200 // F11_SEEDS=4000 để khám phá kỹ (≈ 2–3 phút)
	if v, err := strconv.Atoi(os.Getenv("F11_SEEDS")); err == nil && v > 0 {
		n = v
	}
	reorders, contagion, stmts, rows := 0, 0, 0, 0
	for seed := int64(1); seed <= int64(n); seed++ {
		s := newWSim(t, seed)
		s.run()
		reorders += s.reorder
		contagion += s.st["contagion"]
		stmts += s.db.stmts
		rows += s.st["rows"]
		bad := append(s.errs, s.diff()...)
		if len(bad) > 0 {
			t.Fatalf("%s (copyMin %d, lane %v): %d problems: %v", s.name, s.copyMin, s.laneOn, len(bad), bad[:min(len(bad), 8)])
		}
	}
	t.Logf("unique-violation reorders %d, lane contagion %d, statements %d, rows written %d", reorders, contagion, stmts, rows)
}
