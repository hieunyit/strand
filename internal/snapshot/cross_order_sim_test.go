package snapshot_test

// Gate 2 "_ci → C" (cross_order.go): mô phỏng SnapshotAddKeys khi target sắp PK khác thứ tự source và có
// phép bằng MỊN HƠN source. Source: khóa chuỗi với collation giả "không phân biệt hoa thường" (PAD SPACE:
// bỏ cả space cuối; NO PAD: giữ) — 'abc' = 'ABC' (= 'abc ' với PAD SPACE), nên PK source chứa tối đa một
// biểu diễn của mỗi lớp. Target: map theo BYTE chính xác (text COLLATE "C"). Stream chạy xen mọi bước của
// chunk với INSERT / UPDATE / DELETE / đổi khóa sang biến thể cùng lớp ('a' → 'A', 'a' → 'a ') và sang lớp
// khác; writer giả apply theo byte (như writer PG). Có crash ngay sau khi một flush commit (trước khi plan
// được lưu) rồi chạy tiếp: event lúc chưa đăng ký đi theo keepBeforeRegister (PendingKey), phần chưa đọc
// được dọn bằng ClearUnreadRange theo khóa SOURCE (InUnread) như pgClearUnreadExact.
//
// Kiểm: target == source đúng từng byte khóa và phiên bản; flush range KHÔNG bao giờ được gọi; ngay trước
// mỗi flush, target không có row nào có khóa source trong chunk (bất biến T của cross_order.go).
// Không cần DB.

import (
	"context"
	"errors"
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

var xoTbl = model.TableID{Schema: "app", Table: "ci"}

var errXOCrash = errors.New("simulated crash after flush commit")

type xoRow struct {
	exact string
	ver   int64
}

type xoSim struct {
	t       *testing.T
	r       *rand.Rand
	pad     bool
	codec   *snapshot.PKCodec
	s       *snapshot.MySQLSnapshotter
	src     map[string]xoRow // lớp (fold) → row source
	dst     map[string]int64 // byte khóa → ver
	plan    snapshot.SnapshotPlan
	hasPl   bool
	xid     uint64
	ver     int64
	cur     *snapshot.Chunk
	flushes int
	crashAt int // flush thứ crashAt commit rồi "crash" (0 = không)
	renames int
}

func (sim *xoSim) fold(s string) string {
	if sim.pad {
		s = strings.TrimRight(s, " ")
	}
	return strings.ToLower(s)
}

func newXOSim(t *testing.T, seed int64, rows int, pad bool) *xoSim {
	sim := &xoSim{t: t, r: rand.New(rand.NewSource(seed)), pad: pad, src: map[string]xoRow{}, dst: map[string]int64{}, xid: 1000}
	sim.codec = &snapshot.PKCodec{
		Cols: []snapshot.PKColumn{{Name: "k", Kind: snapshot.PKWeight, Charset: "utf8mb4", Collation: "fake_ci", PadSpace: pad}},
		Weigh: func(_ snapshot.PKColumn, v model.Value) ([]byte, error) {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected %T", v)
			}
			return []byte(sim.fold(s)), nil
		},
	}
	for len(sim.src) < rows {
		sim.insert()
	}
	sim.s = sim.newSnapshotter()
	return sim
}

func (sim *xoSim) newSnapshotter() *snapshot.MySQLSnapshotter {
	t := sim.t
	s := snapshot.NewMySQLSnapshotter(snapshot.ChunkOptions{DefaultChunkRows: 7, Timeout: time.Second, MinChunkRows: 1, MaxChunkBytes: 1 << 30})
	s.Chunks = sim
	s.LoadPlan = func(model.TableID) (snapshot.SnapshotPlan, bool) { return sim.plan, sim.hasPl }
	s.SavePlan = func(_ model.TableID, p snapshot.SnapshotPlan) error { sim.plan, sim.hasPl = p, true; return nil }
	s.ClearTarget = func(context.Context, model.TableID) error { sim.dst = map[string]int64{}; return nil }
	s.FlushRows = func(context.Context, model.TableID, string, []string, []*model.RowChange, int64, int64) error {
		t.Errorf("integer flush called")
		return errors.New("integer flush")
	}
	s.FlushRange = func(context.Context, model.TableID, []string, []*model.RowChange, snapshot.Bound, snapshot.Bound) error {
		t.Errorf("range flush called on a target that orders the key differently")
		return errors.New("range flush")
	}
	s.FlushKeySet = func(context.Context, model.TableID, []string, []*model.RowChange, []snapshot.Bound) error {
		t.Errorf("re-read flush called")
		return errors.New("re-read flush")
	}
	s.ClearUnreadRange = func(_ context.Context, _ model.TableID, lo, hi snapshot.Bound, hasRange bool, keys []snapshot.Bound) error {
		reread := map[snapshot.Key]bool{}
		for _, k := range keys {
			reread[k.K] = true
		}
		for exact := range sim.dst {
			k := sim.key(exact)
			if snapshot.InUnread(k, lo, hi, hasRange, reread) {
				delete(sim.dst, exact)
			}
		}
		return nil
	}
	s.ExactKeys = func(model.TableID) bool { return true }
	s.FlushExact = func(_ context.Context, _ model.TableID, _ []string, rows []*model.RowChange, deletes []snapshot.Bound) error {
		// Bất biến T: target chưa có row nào có khóa source trong chunk đang flush.
		for exact := range sim.dst {
			if sim.inChunk(sim.cur, sim.key(exact)) {
				t.Fatalf("before flush of [%q,%q]: target already has %q", sim.cur.Lo.V, sim.cur.Hi.V, exact)
			}
		}
		for _, d := range deletes {
			delete(sim.dst, d.V[0].(string))
		}
		for _, r := range rows {
			exact := r.Key[0].(string)
			if !sim.inChunk(sim.cur, sim.key(exact)) {
				t.Fatalf("flush row %q outside its chunk", exact)
			}
			if _, dup := sim.dst[exact]; dup {
				t.Fatalf("flush writes %q twice", exact)
			}
			sim.dst[exact] = verOf(r)
		}
		sim.flushes++
		if sim.flushes == sim.crashAt {
			return errXOCrash // đã "commit" ở target, plan chưa được lưu
		}
		return nil
	}
	return s
}

func verOf(r *model.RowChange) int64 {
	for _, c := range r.Cols {
		if c.Name == "ver" {
			return c.Value.(int64)
		}
	}
	return -1
}

func (sim *xoSim) key(exact string) snapshot.Key {
	k, err := sim.codec.Encode([]model.Value{exact})
	if err != nil {
		sim.t.Fatal(err)
	}
	return k
}

func (sim *xoSim) inChunk(c *snapshot.Chunk, k snapshot.Key) bool {
	if c == nil {
		return false
	}
	lo := k >= c.Lo.K
	if c.Lo.Excl {
		lo = k > c.Lo.K
	}
	return lo && k <= c.Hi.K
}

// variant: một biểu diễn khác (hoặc giống) của cùng lớp: đổi hoa/thường, thêm/bớt space cuối (PAD SPACE).
func (sim *xoSim) variant(s string) string {
	b := []rune(strings.TrimRight(s, " "))
	for i := range b {
		if sim.r.Intn(2) == 0 {
			b[i] = []rune(strings.ToUpper(string(b[i])))[0]
		} else {
			b[i] = []rune(strings.ToLower(string(b[i])))[0]
		}
	}
	out := string(b)
	if sim.pad {
		out += strings.Repeat(" ", sim.r.Intn(3))
	} else {
		out += s[len(strings.TrimRight(s, " ")):]
	}
	return out
}

func (sim *xoSim) freshKey() string {
	alpha := []rune("aAbBzZéÉ_")
	for {
		n := 1 + sim.r.Intn(3)
		b := make([]rune, n)
		for i := range b {
			b[i] = alpha[sim.r.Intn(len(alpha))]
		}
		s := string(b) + strings.Repeat(" ", sim.r.Intn(2))
		if _, ok := sim.src[sim.fold(s)]; !ok {
			return s
		}
	}
}

func (sim *xoSim) image(op model.Op, row xoRow) *model.RowChange {
	return &model.RowChange{Table: xoTbl, Op: op, Key: []model.Value{row.exact},
		Cols: []model.ColValue{{Name: "k", Value: row.exact}, {Name: "ver", Value: row.ver}}}
}

func (sim *xoSim) insert() *model.RowChange {
	sim.ver++
	row := xoRow{exact: sim.freshKey(), ver: sim.ver}
	sim.src[sim.fold(row.exact)] = row
	return sim.image(model.OpInsert, row)
}

func (sim *xoSim) classes() []string {
	out := make([]string, 0, len(sim.src))
	for c := range sim.src {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// txn sửa source (commit ngay) và trả các event theo đúng định dạng của assembler (OldKey khi byte khóa đổi).
func (sim *xoSim) txn() []*model.RowChange {
	var evs []*model.RowChange
	for n := 1 + sim.r.Intn(3); n > 0; n-- {
		cs := sim.classes()
		if len(cs) == 0 {
			evs = append(evs, sim.insert())
			continue
		}
		c := cs[sim.r.Intn(len(cs))]
		cur := sim.src[c]
		sim.ver++
		switch op := sim.r.Intn(10); {
		case op < 2:
			evs = append(evs, sim.insert())
		case op < 4:
			cur.ver = sim.ver
			sim.src[c] = cur
			evs = append(evs, sim.image(model.OpUpdate, cur))
		case op < 5:
			delete(sim.src, c)
			evs = append(evs, &model.RowChange{Table: xoTbl, Op: model.OpDelete, Key: []model.Value{cur.exact}})
		case op < 8: // đổi khóa sang biến thể cùng lớp
			old := cur.exact
			cur.exact, cur.ver = sim.variant(old), sim.ver
			sim.src[c] = cur
			ev := sim.image(model.OpUpdate, cur)
			if cur.exact != old {
				ev.OldKey = []model.Value{old}
				sim.renames++
			}
			evs = append(evs, ev)
		default: // đổi khóa sang lớp khác
			old := cur.exact
			nk := sim.freshKey()
			delete(sim.src, c)
			cur.exact, cur.ver = nk, sim.ver
			sim.src[sim.fold(nk)] = cur
			ev := sim.image(model.OpUpdate, cur)
			ev.OldKey = []model.Value{old}
			evs = append(evs, ev)
		}
	}
	return evs
}

// route: router của pipeline (range-state, hoặc keepBeforeRegister khi bảng chưa đăng ký) + writer PG.
func (sim *xoSim) route(evs []*model.RowChange) {
	sim.xid++
	pos := model.Pos{Engine: model.EnginePG, LSN: sim.xid}
	m := sim.s.Manager
	var apply []*model.RowChange
	for _, ev := range evs {
		if !m.IsRegistered(xoTbl) {
			if !sim.hasPl {
				continue // bảng mới: ClearTarget sẽ xóa sạch, snapshot đọc lại mọi thứ
			}
			for _, part := range snapshot.SplitStreamEvent(ev) {
				in, err := sim.plan.PendingKey(sim.codec, part.Key)
				if err != nil || in {
					continue
				}
				apply = append(apply, part)
			}
			continue
		}
		for _, r := range m.HandleStreamEventWithPKSplitXID(ev, pos, sim.xid) {
			switch r.Action {
			case snapshot.ActionApply:
				apply = append(apply, r.Event)
			case snapshot.ActionReject:
				sim.t.Fatalf("reject %v", r.Event.Key)
			}
		}
	}
	for _, ev := range apply {
		exact := ev.Key[0].(string)
		if len(ev.OldKey) > 0 {
			delete(sim.dst, ev.OldKey[0].(string))
		}
		if ev.Op == model.OpDelete {
			delete(sim.dst, exact)
			continue
		}
		sim.dst[exact] = verOf(ev)
	}
}

func (sim *xoSim) tick() {
	for n := sim.r.Intn(4); n > 0; n-- {
		sim.route(sim.txn())
	}
}

// --- ChunkSource / KeyChunkSource ---

func (sim *xoSim) BeforeRegister(context.Context) error { return nil }
func (sim *xoSim) Bounds(context.Context, model.TableID, string) (*snapshot.TableBounds, error) {
	return nil, errors.New("integer bounds")
}
func (sim *xoSim) KeysetEnd(context.Context, model.TableID, string, int64, int64, int64) (int64, error) {
	return 0, errors.New("integer keyset")
}

func (sim *xoSim) sortedExact() []string {
	var out []string
	for _, r := range sim.src {
		out = append(out, r.exact)
	}
	sort.Slice(out, func(i, j int) bool { return sim.key(out[i]) < sim.key(out[j]) })
	return out
}

func (sim *xoSim) bound(exact string) snapshot.Bound {
	b, err := sim.codec.BoundOf([]model.Value{exact})
	if err != nil {
		sim.t.Fatal(err)
	}
	return b
}

func (sim *xoSim) KeyBounds(context.Context, model.TableID, *snapshot.PKCodec) (snapshot.Bound, snapshot.Bound, bool, error) {
	ks := sim.sortedExact()
	if len(ks) == 0 {
		return snapshot.Bound{}, snapshot.Bound{}, false, nil
	}
	return sim.bound(ks[0]), sim.bound(ks[len(ks)-1]), true, nil
}

func (sim *xoSim) KeyKeysetEnd(_ context.Context, _ model.TableID, _ *snapshot.PKCodec, lo, max snapshot.Bound, n int64) (snapshot.Bound, error) {
	c := &snapshot.Chunk{Lo: lo, Hi: max}
	var in []string
	for _, e := range sim.sortedExact() {
		if sim.inChunk(c, sim.key(e)) {
			in = append(in, e)
		}
	}
	if int64(len(in)) < n {
		return max, nil
	}
	return sim.bound(in[n-1]), nil
}

func (sim *xoSim) snap() *snapshot.PGSnapshot {
	x := sim.xid + 1
	s, err := snapshot.ParsePGSnapshot(fmt.Sprintf("%d:%d:", x, x))
	if err != nil {
		sim.t.Fatal(err)
	}
	return s
}

func (sim *xoSim) ReadChunk(_ context.Context, s *snapshot.MySQLSnapshotter, chunk *snapshot.Chunk, _ []string, _ []int,
	_ model.TableID) (snapshot.ChunkRead, checkpoint.AppliedSet, error) {
	sim.cur = chunk
	if !chunk.Continuation {
		s.Manager.BeginReading(chunk)
		sim.tick()
		s.Manager.StartReadingPG(chunk, sim.snap())
	}
	sim.tick()
	read := snapshot.ChunkRead{Cut: chunk.Hi}
	for _, e := range sim.sortedExact() {
		if sim.inChunk(chunk, sim.key(e)) {
			read.Rows = append(read.Rows, sim.image(model.OpInsert, sim.src[sim.fold(e)]))
		}
	}
	sim.tick()
	return read, checkpoint.NewPGAppliedSet(sim.xid), nil
}

func (sim *xoSim) run(ctx context.Context) error {
	return sim.s.SnapshotAddKeys(ctx, nil, nil, xoTbl, sim.codec, []string{"k", "ver"},
		func(context.Context, checkpoint.AppliedSet) error { sim.tick(); return nil })
}

func (sim *xoSim) check() {
	t := sim.t
	t.Helper()
	var bad []string
	want := map[string]int64{}
	for _, r := range sim.src {
		want[r.exact] = r.ver
	}
	for e, v := range want {
		if dv, ok := sim.dst[e]; !ok {
			bad = append(bad, fmt.Sprintf("%q missing", e))
		} else if dv != v {
			bad = append(bad, fmt.Sprintf("%q ver %d, want %d", e, dv, v))
		}
	}
	for e := range sim.dst {
		if _, ok := want[e]; !ok {
			bad = append(bad, fmt.Sprintf("%q extra", e))
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

func TestG2_ExactKeySnapshotUnderLoad(t *testing.T) {
	renames := 0
	for seed := int64(1); seed <= 80; seed++ {
		pad := seed%2 == 0
		sim := newXOSim(t, seed, 40+int(seed%4)*15, pad)
		// Dữ liệu cũ ở target (bảng mới): ClearTarget phải dọn.
		sim.dst["stale"], sim.dst["ABC"] = 1, 1
		if err := sim.run(context.Background()); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		for i := 0; i < 5; i++ { // stream sau khi bảng xong: apply thẳng
			sim.tick()
		}
		sim.check()
		renames += sim.renames
	}
	if renames == 0 {
		t.Fatal("no same-class key renames were generated")
	}
}

func TestG2_ExactKeySnapshotCrashAndResume(t *testing.T) {
	for seed := int64(1); seed <= 80; seed++ {
		pad := seed%2 == 1
		sim := newXOSim(t, 100+seed, 60, pad)
		sim.crashAt = 1 + int(seed%6)
		err := sim.run(context.Background())
		if !errors.Is(err, errXOCrash) {
			t.Fatalf("seed %d: run = %v, want the simulated crash", seed, err)
		}
		// Khởi động lại: range-state mới, plan và target giữ nguyên; stream chạy trong lúc chờ đăng ký.
		sim.crashAt = 0
		sim.s = sim.newSnapshotter()
		for i := 0; i < 3; i++ {
			sim.tick()
		}
		if err := sim.run(context.Background()); err != nil {
			t.Fatalf("seed %d: resume: %v", seed, err)
		}
		for i := 0; i < 5; i++ {
			sim.tick()
		}
		sim.check()
	}
}
