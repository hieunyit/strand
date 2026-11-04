package snapshot_test

// Mô phỏng SnapshotAdd của source PG (ChunkSource giả trong bộ nhớ) chạy xen với một stream có UPDATE
// đổi PK mà cột TOAST không đổi: router giả theo đúng luật của pipeline (HandleToastPKChangeXID, lưu
// plan trước khi "commit"), writer giả theo đúng ngữ nghĩa của writer PG (partial UPDATE, SQL điều kiện
// khi đổi PK). Trạng thái cuối của target phải giống hệt source, doc không bao giờ thiếu, không phiên bản
// row nào được ghi hai lần, và thứ tự đọc (re-read lên đầu hàng đợi) đúng luật 30 s / max_pending_cross_range.
// spec: 02 §5, §6, §8

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

type simRow struct {
	ver int64
	doc string
}

type simTxn struct {
	xid    uint64
	events []*model.RowChange
}

type toastSim struct {
	t      *testing.T
	r      *rand.Rand
	s      *snapshot.MySQLSnapshotter
	src    map[int64]simRow
	dst    map[int64]simRow
	writes map[int64]int // ver → số lần được ghi ở target (phát hiện apply trùng)
	plan   snapshot.SnapshotPlan
	hasPl  bool
	xid    uint64
	ver    int64
	order  []string // thứ tự chunk được đọc: "main[a,b]" / "reread[k…]"
	moves  int
	pause  func()   // gọi trong ReadChunk (giữ chunk ở "Đang đọc" lâu hơn)
	gen    bool     // false: không sinh Txn (kiểm tra thứ tự đọc tất định)
	script []func() // Txn sinh theo kịch bản thay vì ngẫu nhiên
}

func newToastSim(t *testing.T, seed int64, rows int) *toastSim {
	sim := &toastSim{t: t, r: rand.New(rand.NewSource(seed)), src: map[int64]simRow{}, dst: map[int64]simRow{},
		writes: map[int64]int{}, xid: 1000, ver: 1, gen: true}
	for k := int64(1); k <= int64(rows); k++ {
		sim.src[k] = simRow{ver: sim.nextVer(), doc: fmt.Sprintf("doc-%d", k)}
	}
	s := snapshot.NewMySQLSnapshotter(snapshot.ChunkOptions{DefaultChunkRows: 10, Timeout: time.Second, MinChunkRows: 1, MaxChunkBytes: 1 << 30})
	s.Chunks = sim
	s.LoadPlan = func(model.TableID) (snapshot.SnapshotPlan, bool) { return sim.plan, sim.hasPl }
	s.SavePlan = func(_ model.TableID, p snapshot.SnapshotPlan) error { sim.plan, sim.hasPl = p, true; return nil }
	s.ClearTarget = func(context.Context, model.TableID) error { sim.dst = map[int64]simRow{}; return nil }
	s.ClearUnread = func(_ context.Context, _ model.TableID, _ string, lo, hi int64, hasRange bool, keys []int64) error {
		for k := range sim.dst {
			if hasRange && k >= lo && k <= hi {
				delete(sim.dst, k)
			}
		}
		for _, k := range keys {
			delete(sim.dst, k)
		}
		return nil
	}
	s.FlushRows = func(_ context.Context, _ model.TableID, _ string, _ []string, rows []*model.RowChange, a, b int64) error {
		for k := range sim.dst {
			if k >= a && k <= b {
				delete(sim.dst, k)
			}
		}
		return sim.writeRows(rows, func(k int64) bool { return k >= a && k <= b })
	}
	s.FlushKeys = func(_ context.Context, _ model.TableID, _ string, _ []string, rows []*model.RowChange, keys []int64) error {
		in := map[int64]bool{}
		for _, k := range keys {
			in[k] = true
			delete(sim.dst, k)
		}
		return sim.writeRows(rows, func(k int64) bool { return in[k] })
	}
	sim.s = s
	return sim
}

func (sim *toastSim) nextVer() int64 { sim.ver++; return sim.ver }

func (sim *toastSim) writeRows(rows []*model.RowChange, in func(int64) bool) error {
	for _, r := range rows {
		k := r.Key[0].(int64)
		if !in(k) {
			return fmt.Errorf("flush row %d outside its chunk", k)
		}
		row, err := rowOf(r)
		if err != nil {
			return err
		}
		sim.put(k, row)
	}
	return nil
}

func (sim *toastSim) put(k int64, row simRow) {
	sim.dst[k] = row
	sim.writes[row.ver]++
}

func rowOf(r *model.RowChange) (simRow, error) {
	var row simRow
	for _, c := range r.Cols {
		if c.Unchanged {
			return row, fmt.Errorf("row %v written with an unchanged (TOAST) column", r.Key)
		}
		switch c.Name {
		case "ver":
			row.ver = c.Value.(int64)
		case "doc":
			row.doc = c.Value.(string)
		}
	}
	if row.doc == "" {
		return row, fmt.Errorf("row %v without doc", r.Key)
	}
	return row, nil
}

func simImage(op model.Op, k int64, row simRow, docUnchanged bool) *model.RowChange {
	doc := col("doc", row.doc)
	if docUnchanged {
		doc = unchanged("doc")
	}
	return &model.RowChange{Table: toastTbl, Op: op, Key: []model.Value{k}, Cols: []model.ColValue{col("id", k), col("ver", row.ver), doc}}
}

// randomTxn sửa source (Txn commit và visible ngay) và trả event của nó theo đúng định dạng pgoutput.
func (sim *toastSim) randomTxn() simTxn {
	sim.xid++
	tx := simTxn{xid: sim.xid}
	keys := func() []int64 {
		out := make([]int64, 0, len(sim.src))
		for k := range sim.src {
			out = append(out, k)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}
	free := func() int64 {
		for {
			k := 1 + sim.r.Int63n(int64(len(sim.src))+150)
			if _, ok := sim.src[k]; !ok {
				return k
			}
		}
	}
	for n := 1 + sim.r.Intn(3); n > 0; n-- {
		ks := keys()
		if len(ks) == 0 {
			break
		}
		k := ks[sim.r.Intn(len(ks))]
		cur := sim.src[k]
		switch op := sim.r.Intn(10); {
		case op < 2: // partial UPDATE (doc không đổi)
			cur.ver = sim.nextVer()
			sim.src[k] = cur
			tx.events = append(tx.events, simImage(model.OpUpdate, k, cur, true))
		case op < 3: // đổi chính doc
			cur.ver, cur.doc = sim.nextVer(), fmt.Sprintf("doc-%d-v%d", k, sim.ver)
			sim.src[k] = cur
			tx.events = append(tx.events, simImage(model.OpUpdate, k, cur, false))
		case op < 4: // INSERT
			nk := free()
			row := simRow{ver: sim.nextVer(), doc: fmt.Sprintf("doc-%d-new%d", nk, sim.ver)}
			sim.src[nk] = row
			tx.events = append(tx.events, simImage(model.OpInsert, nk, row, false))
		case op < 5: // DELETE
			delete(sim.src, k)
			tx.events = append(tx.events, &model.RowChange{Table: toastTbl, Op: model.OpDelete, Key: []model.Value{k}})
		case op < 9: // đổi PK, doc không đổi (đối tượng của test)
			nk := free()
			cur.ver = sim.nextVer()
			delete(sim.src, k)
			sim.src[nk] = cur
			ev := simImage(model.OpUpdate, nk, cur, true)
			ev.OldKey = []model.Value{k}
			tx.events = append(tx.events, ev)
			sim.moves++
		default: // đổi PK với ảnh đầy đủ
			nk := free()
			cur.ver, cur.doc = sim.nextVer(), fmt.Sprintf("doc-%d-mv%d", nk, sim.ver)
			delete(sim.src, k)
			sim.src[nk] = cur
			ev := simImage(model.OpUpdate, nk, cur, false)
			ev.OldKey = []model.Value{k}
			tx.events = append(tx.events, ev)
		}
	}
	return tx
}

// route: router của pipeline (route/keep) + writer PG, đồng bộ (Txn "commit" ngay sau khi route).
func (sim *toastSim) route(tx simTxn) {
	t := sim.t
	m := sim.s.Manager
	pos := model.Pos{Engine: model.EnginePG, LSN: tx.xid}
	var apply []*model.RowChange
	reread := false
	for _, ev := range tx.events {
		if !m.IsRegistered(toastTbl) {
			apply = append(apply, ev)
			continue
		}
		var rs []snapshot.StreamActionResult
		if len(ev.OldKey) > 0 && snapshot.HasUnchanged(ev) {
			var rr bool
			rs, rr = m.HandleToastPKChangeXID(ev, pos, tx.xid, time.Now())
			reread = reread || rr
		} else {
			rs = m.HandleStreamEventWithPKSplitXID(ev, pos, tx.xid)
		}
		for _, r := range rs {
			switch r.Action {
			case snapshot.ActionApply:
				apply = append(apply, r.Event)
			case snapshot.ActionReject:
				t.Fatalf("reject %v", r.Event.Key)
			}
		}
	}
	if reread {
		if err := sim.s.PersistReread(toastTbl); err != nil {
			t.Fatal(err)
		}
		// CP: trước khi Txn commit, mọi khóa chờ đọc lại phải nằm trong plan bền vững.
		want := fmt.Sprint(m.RereadKeys(toastTbl))
		if got := fmt.Sprint(sim.plan.Reread); got != want {
			t.Fatalf("plan reread %s, range-state %s", got, want)
		}
	}
	for _, ev := range apply {
		if err := sim.applyTarget(ev); err != nil {
			t.Fatalf("apply txn %d: %v", tx.xid, err)
		}
	}
}

// applyTarget: ngữ nghĩa của writer.PGWriter cho một row change.
func (sim *toastSim) applyTarget(ev *model.RowChange) error {
	k := ev.Key[0].(int64)
	if ev.Op == model.OpDelete {
		delete(sim.dst, k)
		return nil
	}
	partial := snapshot.HasUnchanged(ev)
	if partial && ev.Op == model.OpInsert {
		return fmt.Errorf("insert %d with unchanged (toast) column", k)
	}
	var ver int64
	doc := ""
	for _, c := range ev.Cols {
		if c.Name == "ver" {
			ver = c.Value.(int64)
		}
		if c.Name == "doc" && !c.Unchanged {
			doc = c.Value.(string)
		}
	}
	if len(ev.OldKey) > 0 {
		o := ev.OldKey[0].(int64)
		if !partial {
			delete(sim.dst, o)
			sim.put(k, simRow{ver: ver, doc: doc})
			return nil
		}
		// UPDATE t SET pk=:new, … WHERE pk=:old AND NOT EXISTS (pk=:new); DELETE pk=:old.
		old, okOld := sim.dst[o]
		if _, okNew := sim.dst[k]; !okNew {
			if !okOld {
				return fmt.Errorf("toast row missing: pk change %d -> %d, old row not on target", o, k)
			}
			sim.put(k, simRow{ver: ver, doc: old.doc})
		}
		delete(sim.dst, o)
		return nil
	}
	if partial {
		cur, ok := sim.dst[k]
		if !ok {
			return fmt.Errorf("toast row missing: partial update of %d", k)
		}
		sim.put(k, simRow{ver: ver, doc: cur.doc})
		return nil
	}
	sim.put(k, simRow{ver: ver, doc: doc})
	return nil
}

// tick: một vài Txn của stream (khi gen bật), hoặc bước kế tiếp của kịch bản.
func (sim *toastSim) tick() {
	if len(sim.script) > 0 {
		f := sim.script[0]
		sim.script = sim.script[1:]
		f()
		return
	}
	if !sim.gen {
		return
	}
	for n := sim.r.Intn(4); n > 0; n-- {
		sim.route(sim.randomTxn())
	}
}

func (sim *toastSim) snap() *snapshot.PGSnapshot {
	x := sim.xid + 1
	s, err := snapshot.ParsePGSnapshot(fmt.Sprintf("%d:%d:", x, x))
	if err != nil {
		sim.t.Fatal(err)
	}
	return s
}

// --- snapshot.ChunkSource ---

func (sim *toastSim) BeforeRegister(context.Context) error { return nil }

func (sim *toastSim) Bounds(context.Context, model.TableID, string) (*snapshot.TableBounds, error) {
	var b snapshot.TableBounds
	first := true
	for k := range sim.src {
		if first || k < b.MinPK {
			b.MinPK = k
		}
		if first || k > b.MaxPK {
			b.MaxPK = k
		}
		first = false
	}
	if !first {
		b.Count = b.MaxPK - b.MinPK + 1
	}
	return &b, nil
}

func (sim *toastSim) KeysetEnd(_ context.Context, _ model.TableID, _ string, start, max, rows int64) (int64, error) {
	var ks []int64
	for k := range sim.src {
		if k >= start && k <= max {
			ks = append(ks, k)
		}
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	if int64(len(ks)) < rows {
		return max, nil
	}
	return ks[rows-1], nil
}

// ReadChunk: các bước 1–5 của pg_snapshot.go; Txn xen vào giữa mọi bước.
func (sim *toastSim) ReadChunk(_ context.Context, s *snapshot.MySQLSnapshotter, chunk *snapshot.Chunk, _ []string, _ int,
	_ model.TableID) ([]*model.RowChange, checkpoint.AppliedSet, error) {
	if chunk.Keys != nil {
		sim.order = append(sim.order, fmt.Sprintf("reread%v", chunk.Keys))
	} else {
		sim.order = append(sim.order, fmt.Sprintf("main[%d,%d]", chunk.StartPK, chunk.EndPK))
	}
	s.Manager.BeginReading(chunk)
	sim.tick() // "Đang đọc" chưa có low: gộp
	s.Manager.StartReadingPG(chunk, sim.snap())
	sim.tick() // sau S0, trước SELECT: gộp (row SELECT cũng thấy)
	if sim.pause != nil {
		sim.pause()
	}
	in := func(k int64) bool { return k >= chunk.StartPK && k <= chunk.EndPK }
	if chunk.Keys != nil {
		set := map[int64]bool{}
		for _, k := range chunk.Keys {
			set[k] = true
		}
		in = func(k int64) bool { return set[k] }
	}
	var rows []*model.RowChange
	for k, row := range sim.src {
		if in(k) {
			rows = append(rows, simImage(model.OpInsert, k, row, false))
		}
	}
	sim.tick() // sau SELECT, trước high: gộp, row SELECT không thấy
	return rows, checkpoint.NewPGAppliedSet(sim.xid), nil
}

func (sim *toastSim) run(ctx context.Context) error {
	return sim.s.SnapshotAdd(ctx, nil, nil, toastTbl, "id", []string{"id", "ver", "doc"},
		func(context.Context, checkpoint.AppliedSet) error { sim.tick(); return nil })
}

func (sim *toastSim) check(t *testing.T) {
	t.Helper()
	var bad []string
	for k, sv := range sim.src {
		if dv, ok := sim.dst[k]; !ok {
			bad = append(bad, fmt.Sprintf("%d missing", k))
		} else if dv != sv {
			bad = append(bad, fmt.Sprintf("%d differs: src %+v dst %+v", k, sv, dv))
		}
	}
	for k := range sim.dst {
		if _, ok := sim.src[k]; !ok {
			bad = append(bad, fmt.Sprintf("%d extra", k))
		}
	}
	for v, n := range sim.writes {
		if n > 1 {
			bad = append(bad, fmt.Sprintf("version %d written %d times", v, n))
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		if len(bad) > 10 {
			bad = bad[:10]
		}
		t.Fatalf("target != source:\n%s", strings.Join(bad, "\n"))
	}
}

// Ngẫu nhiên: snapshot song song với stream nhiều UPDATE đổi PK mà doc không đổi, qua mọi tổ hợp trạng
// thái range (PK mới "Chưa đọc"/"Đang đọc"/"Xong", PK cũ mọi trạng thái), với max_pending_cross_range
// nhỏ (spill sang chunk re-read kế tiếp) và luật chờ tối đa khác nhau.
func TestToastPK_SimulatedSnapshotUnderLoad(t *testing.T) {
	totalMoves, totalRereads := 0, 0
	for seed := int64(1); seed <= 60; seed++ {
		sim := newToastSim(t, seed, 120+int(seed%5)*20)
		switch seed % 3 {
		case 0:
			sim.s.Manager.MaxPendingCrossRange = 2
		case 1:
			sim.s.Manager.PendingMaxWait = time.Nanosecond
		}
		// Một ít Txn trước khi đăng ký (chưa thuộc snapshot: target sẽ bị xóa khi đăng ký).
		for i := 0; i < 5; i++ {
			sim.randomTxn()
		}
		if err := sim.run(context.Background()); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if n, _ := sim.s.Manager.PendingCrossRange(time.Now()); n != 0 {
			t.Fatalf("seed %d: %d keys still waiting after snapshot", seed, n)
		}
		if len(sim.plan.Reread) != 0 {
			t.Fatalf("seed %d: plan still lists re-read keys %v", seed, sim.plan.Reread)
		}
		sim.s.Manager.RemoveTable(toastTbl)
		for i := 0; i < 50; i++ { // stream tiếp sau khi bảng đã "Xong"
			sim.route(sim.randomTxn())
		}
		sim.check(t)
		for _, o := range sim.order {
			if strings.HasPrefix(o, "reread") {
				totalRereads++
			}
		}
		totalMoves += sim.moves
	}
	if totalRereads == 0 {
		t.Fatal("simulation never exercised a re-read")
	}
	t.Logf("60 runs: %d TOAST PK moves, %d re-read chunks", totalMoves, totalRereads)
}

// Thứ tự đọc: khóa chờ đọc lại được đọc ở cuối walk chính khi chưa đến hạn; đẩy lên đầu hàng đợi (ngay
// ranh giới chunk kế tiếp) khi chờ quá PendingMaxWait (30 s mặc định) hoặc đạt max_pending_cross_range.
func TestToastPK_RereadPrioritization(t *testing.T) {
	setup := func(t *testing.T, cfg func(*snapshot.RangeStateManager), pause func()) *toastSim {
		sim := newToastSim(t, 7, 30) // 3 chunk chính [1,10] [11,20] [21,30]
		sim.gen = false
		cfg(sim.s.Manager)
		first := true
		sim.script = []func(){
			// Trong lúc chunk [1,10] "Đang đọc" (chưa low): 25 ("Chưa đọc") → 1000 (sau b_last, "Xong").
			func() {
				sim.xid++
				cur := sim.src[25]
				cur.ver = sim.nextVer()
				delete(sim.src, 25)
				sim.src[1000] = cur
				ev := simImage(model.OpUpdate, 1000, cur, true)
				ev.OldKey = []model.Value{int64(25)}
				sim.route(simTxn{xid: sim.xid, events: []*model.RowChange{ev}})
			},
		}
		sim.pause = func() {
			if first && pause != nil {
				first = false
				pause()
			}
		}
		return sim
	}
	cases := []struct {
		name  string
		cfg   func(*snapshot.RangeStateManager)
		pause func()
		want  string
	}{
		{"not due: read after the main walk", func(m *snapshot.RangeStateManager) {}, nil,
			"main[1,10] main[11,20] main[21,30] reread[1000]"},
		{"waited longer than max wait: front of the queue", func(m *snapshot.RangeStateManager) { m.PendingMaxWait = 20 * time.Millisecond },
			func() { time.Sleep(30 * time.Millisecond) }, "main[1,10] reread[1000] main[11,20] main[21,30]"},
		{"max_pending_cross_range reached: front of the queue", func(m *snapshot.RangeStateManager) { m.MaxPendingCrossRange = 1 }, nil,
			"main[1,10] reread[1000] main[11,20] main[21,30]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sim := setup(t, tc.cfg, tc.pause)
			if err := sim.run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(sim.order, " "); got != tc.want {
				t.Fatalf("read order %q, want %q", got, tc.want)
			}
			sim.s.Manager.RemoveTable(toastTbl)
			sim.check(t)
			if r := sim.dst[1000]; r.doc != "doc-25" {
				t.Fatalf("moved row = %+v, want doc of the old row (doc-25)", r)
			}
		})
	}
}

// Restart: plan còn khóa chờ đọc lại ở phần đã "Xong" (target giữ ảnh cũ/thiếu) → được đọc lại; khóa ở
// phần "Chưa đọc" do walk chính đọc.
func TestToastPK_RereadKeysSurviveRestart(t *testing.T) {
	sim := newToastSim(t, 3, 40)
	sim.gen = false
	// Lần chạy trước đã flush [1,20]; 5 vừa nhận row chuyển tới (chưa có ở target), 30 ở phần chưa đọc.
	for k := int64(1); k <= 20; k++ {
		if k != 5 {
			sim.dst[k] = sim.src[k]
		}
	}
	for k, v := range sim.dst {
		_ = k
		sim.writes[v.ver]++
	}
	sim.plan = snapshot.SnapshotPlan{MinPK: 1, MaxPK: 40, ChunkRows: 10, NextPK: 21, Reread: []int64{5, 30}}
	sim.hasPl = true
	if err := sim.run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(sim.order, " "); got != "main[21,30] main[31,40] reread[5]" {
		t.Fatalf("read order %q", got)
	}
	sim.s.Manager.RemoveTable(toastTbl)
	sim.check(t)
}
