package pg

// Mô phỏng tất định (F11) phần reader PG + assembler: sinh luồng message pgoutput hợp lệ (protocol 2/3:
// streaming, two-phase, subtransaction) theo đúng cách logical decoding phát ra, đưa vào streamState.handle
// THẬT (không kết nối), rồi qua assemble.Assembler THẬT. Kiểm tra: mỗi Txn đã commit ra đúng một lần,
// đúng thứ tự commit, đúng XID top-level và commit LSN, đủ mọi row (thứ tự, giá trị, cột TOAST vắng mặt),
// không row nào của subtransaction đã rollback; Txn abort / ROLLBACK PREPARED không ra; watermark ra thành
// Txn rỗng. Trần RAM nhỏ ép eventLog spill ra file (kể cả cắt file khi rollback subxact).

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
)

// --- mã hóa message pgoutput ---

type pgoBuf []byte

func (b pgoBuf) u8(v byte) pgoBuf    { return append(b, v) }
func (b pgoBuf) u16(v uint16) pgoBuf { return binary.BigEndian.AppendUint16(b, v) }
func (b pgoBuf) u32(v uint32) pgoBuf { return binary.BigEndian.AppendUint32(b, v) }
func (b pgoBuf) u64(v uint64) pgoBuf { return binary.BigEndian.AppendUint64(b, v) }
func (b pgoBuf) str(s string) pgoBuf { return append(append(b, s...), 0) }
func (b pgoBuf) xid(x uint32) pgoBuf {
	if x != 0 {
		return b.u32(x)
	}
	return b
}

// simTuple: giá trị cột theo thứ tự relation; nil = NULL; "\x00u" = TOAST không đổi.
const simUnchanged = "\x00u"

func (b pgoBuf) tuple(vals []*string) pgoBuf {
	b = b.u16(uint16(len(vals)))
	for _, v := range vals {
		switch {
		case v == nil:
			b = b.u8('n')
		case *v == simUnchanged:
			b = b.u8('u')
		default:
			b = b.u8('t').u32(uint32(len(*v)))
			b = append(b, *v...)
		}
	}
	return b
}

func sp(s string) *string { return &s }

type simRel struct {
	id   uint32
	name string
}

var simRels = []simRel{{16401, "t1"}, {16402, "t2"}}

// relation: (id int8 PK, v text, doc text).
func msgRelation(streamXid uint32, r simRel) []byte {
	b := pgoBuf{'R'}.xid(streamXid).u32(r.id).str("app").str(r.name).u8('d').u16(3)
	b = b.u8(1).str("id").u32(20).u32(0xFFFFFFFF)
	b = b.u8(0).str("v").u32(25).u32(0xFFFFFFFF)
	b = b.u8(0).str("doc").u32(25).u32(0xFFFFFFFF)
	return b
}

// --- mô hình phía server ---

type simChange struct {
	sub     uint32
	rel     simRel
	op      model.Op
	key     int64
	oldKey  int64 // ≠ 0: UPDATE đổi PK
	v       string
	doc     string // simUnchanged: TOAST không đổi
	streamd bool
	aborted bool
}

func (c *simChange) msg(streamXid uint32) []byte {
	k := sp(strconv.FormatInt(c.key, 10))
	switch c.op {
	case model.OpInsert:
		return pgoBuf{'I'}.xid(streamXid).u32(c.rel.id).u8('N').tuple([]*string{k, sp(c.v), sp(c.doc)})
	case model.OpUpdate:
		b := pgoBuf{'U'}.xid(streamXid).u32(c.rel.id)
		if c.oldKey != 0 {
			b = b.u8('K').tuple([]*string{sp(strconv.FormatInt(c.oldKey, 10)), nil, nil})
		}
		return b.u8('N').tuple([]*string{k, sp(c.v), sp(c.doc)})
	default:
		return pgoBuf{'D'}.xid(streamXid).u32(c.rel.id).u8('K').tuple([]*string{k, nil, nil})
	}
}

type simSub struct {
	xid    uint32
	merged []uint32 // subxact con đã RELEASE vào nó
}

type simSrvTxn struct {
	xid      uint32
	stream   bool // có thể bị stream (Txn lớn)
	twoPhase bool
	stack    []*simSub
	changes  []*simChange
	streamed bool // đã có ít nhất một khối stream
	gid      string
	subStrm  map[uint32]bool
}

type simExpect struct {
	xid  uint32
	lsn  uint64
	rows []*simChange
	wm   bool
}

type streamSim struct {
	t        *testing.T
	r        *rand.Rand
	nextXid  uint32
	lsn      uint64
	open     []*simSrvTxn
	prepared []*simSrvTxn
	msgs     [][]byte
	expect   []simExpect
	n        int
	keys     map[string]int64 // khóa đang có theo bảng (chỉ để sinh UPDATE/DELETE hợp lý)
	nAbortSx int
}

func (s *streamSim) emit(b []byte) { s.msgs = append(s.msgs, b) }

func (s *streamSim) newLSN() uint64 { s.lsn += 1 + uint64(s.r.Intn(50)); return s.lsn }

func (s *streamSim) alive(tx *simSrvTxn) []*simChange {
	var out []*simChange
	for _, c := range tx.changes {
		if !c.aborted {
			out = append(out, c)
		}
	}
	return out
}

func (s *streamSim) step() {
	switch x := s.r.Intn(100); {
	case x < 10 && len(s.open) < 5:
		s.nextXid++
		tx := &simSrvTxn{xid: s.nextXid, stream: s.r.Intn(2) == 0, twoPhase: s.r.Intn(4) == 0, subStrm: map[uint32]bool{}}
		tx.stack = []*simSub{{xid: tx.xid}}
		tx.gid = fmt.Sprintf("g%d", tx.xid)
		s.open = append(s.open, tx)
	case x < 55 && len(s.open) > 0:
		tx := s.open[s.r.Intn(len(s.open))]
		s.n++
		c := &simChange{sub: tx.stack[len(tx.stack)-1].xid, rel: simRels[s.r.Intn(2)], v: fmt.Sprintf("x%d-%d", tx.xid, s.n),
			key: int64(1 + s.r.Intn(40)), doc: fmt.Sprintf("doc%d", s.n)}
		switch y := s.r.Intn(10); {
		case y < 4:
			c.op = model.OpInsert
		case y < 8:
			c.op = model.OpUpdate
			if s.r.Intn(2) == 0 {
				c.doc = simUnchanged
			}
			if s.r.Intn(4) == 0 {
				c.oldKey = int64(41 + s.r.Intn(40))
			}
		default:
			c.op = model.OpDelete
		}
		tx.changes = append(tx.changes, c)
	case x < 62 && len(s.open) > 0: // SAVEPOINT
		tx := s.open[s.r.Intn(len(s.open))]
		s.nextXid++
		tx.stack = append(tx.stack, &simSub{xid: s.nextXid})
	case x < 67 && len(s.open) > 0: // RELEASE SAVEPOINT
		tx := s.open[s.r.Intn(len(s.open))]
		if len(tx.stack) > 1 {
			top := tx.stack[len(tx.stack)-1]
			tx.stack = tx.stack[:len(tx.stack)-1]
			p := tx.stack[len(tx.stack)-1]
			p.merged = append(append(p.merged, top.xid), top.merged...)
		}
	case x < 72 && len(s.open) > 0: // ROLLBACK TO SAVEPOINT
		tx := s.open[s.r.Intn(len(s.open))]
		if len(tx.stack) > 1 {
			top := tx.stack[len(tx.stack)-1]
			tx.stack = tx.stack[:len(tx.stack)-1]
			ab := map[uint32]bool{top.xid: true}
			for _, m := range top.merged {
				ab[m] = true
			}
			for _, c := range tx.changes {
				if ab[c.sub] {
					c.aborted = true
				}
			}
			// DecodeAbort: các subxact con trước, rồi chính subxact; chỉ subxact đã có thay đổi được stream.
			order := append(append([]uint32(nil), top.merged...), top.xid)
			if s.r.Intn(2) == 0 {
				s.r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
			}
			for _, sx := range order {
				if tx.subStrm[sx] {
					s.emit(pgoBuf{'A'}.u32(tx.xid).u32(sx))
					s.nAbortSx++
				}
			}
		}
	case x < 82 && len(s.open) > 0: // một khối stream của Txn lớn
		tx := s.open[s.r.Intn(len(s.open))]
		if tx.stream {
			s.streamBlock(tx)
		}
	case x < 92 && len(s.open) > 0: // kết thúc Txn
		i := s.r.Intn(len(s.open))
		tx := s.open[i]
		s.open = append(s.open[:i], s.open[i+1:]...)
		s.finish(tx)
	case x < 96 && len(s.prepared) > 0:
		i := s.r.Intn(len(s.prepared))
		tx := s.prepared[i]
		s.prepared = append(s.prepared[:i], s.prepared[i+1:]...)
		if s.r.Intn(5) == 0 {
			l := s.newLSN()
			s.emit(buildMsg(msgRollbackPrepared, true, []uint64{l, l + 1}, []int64{1, 2}, tx.xid, tx.gid))
		} else {
			l := s.newLSN()
			s.emit(buildMsg(msgCommitPrepared, true, []uint64{l, l + 1}, []int64{1}, tx.xid, tx.gid))
			s.expect = append(s.expect, simExpect{xid: tx.xid, lsn: l, rows: s.alive(tx)})
		}
	case x < 99: // watermark không transactional (ngoài mọi khối)
		l := s.newLSN()
		s.emit(pgoBuf{'M'}.u8(0).u64(l).str(WatermarkPrefix).u32(3).u8('a').u8('b').u8('c'))
		s.expect = append(s.expect, simExpect{lsn: l, wm: true})
	default: // Relation gửi lại (invalidation) ngoài Txn
		s.emit(msgRelation(0, simRels[s.r.Intn(2)]))
	}
}

// streamBlock: ReorderBufferStreamTXN — gửi mọi thay đổi chưa gửi (thay đổi của subxact đã rollback
// TRƯỚC khi được stream thì đã bị hủy, không gửi).
func (s *streamSim) streamBlock(tx *simSrvTxn) {
	var pend []*simChange
	for _, c := range tx.changes {
		if !c.streamd && !c.aborted {
			pend = append(pend, c)
		}
	}
	if len(pend) == 0 && tx.streamed {
		return
	}
	first := byte(0)
	if !tx.streamed {
		first = 1
	}
	s.emit(pgoBuf{'S'}.u32(tx.xid).u8(first))
	if s.r.Intn(3) == 0 {
		s.emit(msgRelation(tx.xid, simRels[s.r.Intn(2)]))
	}
	for _, c := range pend {
		c.streamd = true
		tx.subStrm[c.sub] = true
		s.emit(c.msg(c.sub))
	}
	s.emit(pgoBuf{'E'})
	tx.streamed = true
}

func (s *streamSim) finish(tx *simSrvTxn) {
	abort := s.r.Intn(8) == 0
	if tx.streamed {
		if abort {
			s.emit(pgoBuf{'A'}.u32(tx.xid).u32(tx.xid))
			return
		}
		s.streamBlock(tx)
		l := s.newLSN()
		if tx.twoPhase {
			s.emit(buildMsg(msgStreamPrepare, true, []uint64{l, l + 1}, []int64{1}, tx.xid, tx.gid))
			s.prepared = append(s.prepared, tx)
			return
		}
		s.emit(pgoBuf{'c'}.u32(tx.xid).u8(0).u64(l).u64(l + 1).u64(1))
		s.expect = append(s.expect, simExpect{xid: tx.xid, lsn: l, rows: s.alive(tx)})
		return
	}
	if abort {
		return
	}
	rows := s.alive(tx)
	l := s.newLSN()
	if tx.twoPhase {
		s.emit(buildMsg(msgBeginPrepare, false, []uint64{l, l + 1}, []int64{1}, tx.xid, tx.gid))
	} else {
		if len(rows) == 0 {
			return // pgoutput không gửi Txn rỗng (không thay đổi thuộc publication)
		}
		s.emit(pgoBuf{'B'}.u64(l).u64(1).u32(tx.xid))
	}
	if s.r.Intn(3) == 0 {
		s.emit(msgRelation(0, simRels[s.r.Intn(2)]))
	}
	for _, c := range rows {
		s.emit(c.msg(0))
	}
	if tx.twoPhase {
		s.emit(buildMsg(msgPrepare, true, []uint64{l, l + 1}, []int64{1}, tx.xid, tx.gid))
		s.prepared = append(s.prepared, tx)
		return
	}
	s.emit(pgoBuf{'C'}.u8(0).u64(l).u64(l + 1).u64(1))
	s.expect = append(s.expect, simExpect{xid: tx.xid, lsn: l, rows: rows})
}

// --- chạy ---

func runStreamSim(t *testing.T, seed int64, maxBuf int64, asmRows int) (spills, aborts int, err error) {
	s := &streamSim{t: t, r: rand.New(rand.NewSource(seed)), nextXid: 700, lsn: 1 << 20}
	s.emit(msgRelation(0, simRels[0]))
	s.emit(msgRelation(0, simRels[1]))
	for i := 0; i < 300; i++ {
		s.step()
	}
	for len(s.open) > 0 {
		tx := s.open[0]
		s.open = s.open[1:]
		s.finish(tx)
	}
	for len(s.prepared) > 0 {
		tx := s.prepared[0]
		s.prepared = s.prepared[1:]
		l := s.newLSN()
		s.emit(buildMsg(msgCommitPrepared, true, []uint64{l, l + 1}, []int64{1}, tx.xid, tx.gid))
		s.expect = append(s.expect, simExpect{xid: tx.xid, lsn: l, rows: s.alive(tx)})
	}

	dir := t.TempDir()
	rd := &Reader{cfg: Config{SpillDir: dir, MaxBufferedBytes: maxBuf}}
	ch := make(chan reader.Event, 64)
	st := &streamState{r: rd, ctx: context.Background(), out: ch,
		rels: map[uint32]*relState{}, announced: map[uint32]uint64{},
		streams: map[uint32]*txnBuf{}, prepared: map[uint32]*txnBuf{}}
	asm := assemble.New(assemble.WithMaxInMemRows(asmRows), assemble.WithSpillDir(t.TempDir()))
	var got []*model.Txn
	done := make(chan error, 1)
	go func() {
		var aerr error
		for ev := range ch {
			if aerr != nil {
				continue
			}
			if ev.Type == reader.EventError {
				aerr = fmt.Errorf("reader error event: %v", ev.Err)
				continue
			}
			tx, e := asm.Push(ev)
			if e != nil {
				aerr = e
				continue
			}
			if tx != nil {
				got = append(got, tx)
			}
		}
		done <- aerr
	}()
	var herr error
	for i, m := range s.msgs {
		if herr = st.handle(m); herr != nil {
			herr = fmt.Errorf("message %d %q: %w", i, m[0], herr)
			break
		}
	}
	close(ch)
	if aerr := <-done; herr == nil {
		herr = aerr
	}
	if herr != nil {
		return st.spills, s.nAbortSx, herr
	}
	if len(st.streams) != 0 || len(st.prepared) != 0 || st.bufBytes != 0 {
		return st.spills, s.nAbortSx, fmt.Errorf("leftover state: %d streams, %d prepared, %d bytes", len(st.streams), len(st.prepared), st.bufBytes)
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		return st.spills, s.nAbortSx, fmt.Errorf("%d spill files left", len(left))
	}
	return st.spills, s.nAbortSx, compareStreamSim(s.expect, got)
}

func compareStreamSim(want []simExpect, got []*model.Txn) error {
	if len(want) != len(got) {
		return fmt.Errorf("want %d txns, got %d", len(want), len(got))
	}
	for i, w := range want {
		g := got[i]
		if g.SourcePos.LSN != w.lsn || g.XID != uint64(w.xid) {
			return fmt.Errorf("txn %d: want xid %d lsn %d, got xid %d lsn %d", i, w.xid, w.lsn, g.XID, g.SourcePos.LSN)
		}
		var rows []model.RowChange
		if err := g.ForEachRow(func(rc *model.RowChange) error { rows = append(rows, *rc); return nil }); err != nil {
			return err
		}
		if len(rows) != len(w.rows) {
			return fmt.Errorf("txn %d xid %d lsn %d: want %d rows, got %d", i, w.xid, w.lsn, len(w.rows), len(rows))
		}
		for j, wr := range w.rows {
			gr := rows[j]
			if err := matchRow(wr, gr); err != nil {
				return fmt.Errorf("txn %d xid %d row %d: %w", i, w.xid, j, err)
			}
		}
	}
	return nil
}

func matchRow(w *simChange, g model.RowChange) error {
	if g.Table != (model.TableID{Schema: "app", Table: w.rel.name}) || g.Op != w.op {
		return fmt.Errorf("want %s %s, got %s %s", w.op, w.rel.name, g.Op, g.Table)
	}
	if len(g.Key) != 1 || fmt.Sprint(g.Key[0]) != strconv.FormatInt(w.key, 10) {
		return fmt.Errorf("want key %d, got %v", w.key, g.Key)
	}
	if w.oldKey != 0 && (len(g.OldKey) != 1 || fmt.Sprint(g.OldKey[0]) != strconv.FormatInt(w.oldKey, 10)) {
		return fmt.Errorf("want old key %d, got %v", w.oldKey, g.OldKey)
	}
	if w.oldKey == 0 && len(g.OldKey) != 0 {
		return fmt.Errorf("unexpected old key %v", g.OldKey)
	}
	if w.op == model.OpDelete {
		return nil
	}
	vals := map[string]model.ColValue{}
	for _, c := range g.Cols {
		vals[c.Name] = c
	}
	if v := vals["v"]; v.Unchanged || fmt.Sprint(v.Value) != w.v {
		return fmt.Errorf("want v %q, got %+v", w.v, v)
	}
	d := vals["doc"]
	if w.doc == simUnchanged {
		if !d.Unchanged {
			return fmt.Errorf("want doc unchanged, got %+v", d)
		}
	} else if d.Unchanged || fmt.Sprint(d.Value) != w.doc {
		return fmt.Errorf("want doc %q, got %+v", w.doc, d)
	}
	return nil
}

func TestF11StreamSim_ReaderAssembler(t *testing.T) {
	n := 300
	if v, err := strconv.Atoi(os.Getenv("F11_SEEDS")); err == nil && v > 0 {
		n = v
	}
	var spills, aborts int
	for seed := int64(1); seed <= int64(n); seed++ {
		for _, c := range []struct {
			maxBuf  int64
			asmRows int
		}{{64 << 20, 10000}, {1, 3}, {2000, 2}, {700, 10000}} {
			sp, ab, err := runStreamSim(t, seed, c.maxBuf, c.asmRows)
			spills += sp
			aborts += ab
			if err != nil {
				t.Fatalf("seed %d maxBuf %d asmRows %d: %v", seed, c.maxBuf, c.asmRows, err)
			}
		}
	}
	t.Logf("reader spills %d, subxact stream aborts %d", spills, aborts)
}
