package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

// txnBuf đệm Event của một Txn streaming hoặc two-phase cho tới khi biết kết cục.
type txnBuf struct {
	xid        uint32
	prepareLSN uint64
	events     []reader.Event
	// relVer: phiên bản Relation đã chèn EventTableMap vào buffer này (để phát lại đúng schema lúc ghi).
	relVer map[uint32]uint64
	// subxOff: vị trí event đầu tiên của từng (sub)xid trong buffer, để Stream Abort một subtransaction
	// cắt đúng phần của nó (giống apply worker của PostgreSQL).
	subxOff map[uint32]int
}

func newTxnBuf(xid uint32) *txnBuf {
	return &txnBuf{xid: xid, relVer: map[uint32]uint64{}, subxOff: map[uint32]int{}}
}

// openTxn là Txn thường (Begin…Commit) đang đọc.
type openTxn struct {
	pos  model.Pos
	ts   time.Time
	xid  uint32
	skip bool // commit LSN đã nằm trong tập đã apply (CP-2)
}

// relState là Relation đã nhận cùng phiên bản (tăng mỗi lần server gửi lại Relation).
type relState struct {
	msg *pglogrepl.RelationMessage
	ver uint64
}

// streamState là toàn bộ trạng thái giải mã, chỉ readLoop truy cập.
type streamState struct {
	r       *Reader
	ctx     context.Context
	out     chan<- reader.Event
	applied checkpoint.AppliedSet

	rels      map[uint32]*relState
	relSeq    uint64
	announced map[uint32]uint64 // phiên bản Relation đã phát EventTableMap trực tiếp

	cur      *openTxn
	inStream bool
	streamX  uint32
	streams  map[uint32]*txnBuf // Txn streaming đang mở, theo xid top-level
	prep     *txnBuf            // Txn two-phase giữa Begin Prepare và Prepare
	prepared map[uint32]*txnBuf // Txn đã PREPARE, chờ COMMIT/ROLLBACK PREPARED
}

// WatermarkPrefix là prefix của logical decoding message (pg_logical_emit_message, không
// transactional) mà snapshot logic ghi sau khi đọc một chunk; reader phát nó thành một Txn rỗng.
const WatermarkPrefix = "strand_watermark"

// errStop báo readLoop dừng (ctx hủy khi đang gửi Event).
var errStop = fmt.Errorf("dừng")

// readLoop đọc CopyData từ server, giải mã pgoutput và phát Event theo thứ tự. Lỗi giải mã hoặc lỗi
// kết nối được phát thành một EventError rồi kênh đóng.
func (r *Reader) readLoop(ctx context.Context, stop context.CancelFunc, out chan<- reader.Event, applied checkpoint.AppliedSet) {
	defer r.wg.Done()
	defer close(out)
	defer stop() // dừng statusLoop khi readLoop thoát vì lỗi

	st := &streamState{
		r: r, ctx: ctx, out: out, applied: applied,
		rels:      map[uint32]*relState{},
		announced: map[uint32]uint64{},
		streams:   map[uint32]*txnBuf{},
		prepared:  map[uint32]*txnBuf{},
	}

	fail := func(err error) {
		if ctx.Err() != nil {
			return
		}
		select {
		case out <- errEvent(err):
		case <-ctx.Done():
		}
	}
	defer func() {
		// pglogrepl không kiểm biên khi giải mã tuple: không để panic làm sập tiến trình.
		if p := recover(); p != nil {
			fail(fmt.Errorf("pg reader panic khi giải mã: %v", p))
		}
	}()

	for {
		msg, err := r.conn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				fail(classifyErr(fmt.Errorf("nhận dữ liệu replication: %w", err)))
			}
			return
		}
		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			fail(classifyErr(fmt.Errorf("server báo lỗi: %w", pgconn.ErrorResponseToPgError(m))))
			return
		case *pgproto3.CopyDone:
			fail(fmt.Errorf("server kết thúc stream replication (CopyDone)"))
			return
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}
			switch m.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
				if err != nil {
					fail(fmt.Errorf("parse keepalive: %w", err))
					return
				}
				if pkm.ReplyRequested {
					if err := r.sendStatus(false); err != nil && ctx.Err() == nil {
						fail(fmt.Errorf("trả lời keepalive: %w", err))
						return
					}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(m.Data[1:])
				if err != nil {
					fail(fmt.Errorf("parse XLogData: %w", err))
					return
				}
				if len(xld.WALData) == 0 {
					continue
				}
				if err := st.handle(xld.WALData); err != nil {
					if err != errStop {
						fail(err)
					}
					return
				}
			}
		}
	}
}

// handle xử lý một message pgoutput.
func (st *streamState) handle(data []byte) error {
	switch data[0] {
	case msgBeginPrepare, msgPrepare, msgStreamPrepare, msgCommitPrepared, msgRollbackPrepared:
		pm, err := parsePrepareMsg(data)
		if err != nil {
			return err
		}
		return st.handleTwoPhase(pm)
	}

	msg, err := pglogrepl.ParseV2(data, st.inStream)
	if err != nil {
		return fmt.Errorf("giải mã message pgoutput %q: %w", data[0], err)
	}
	switch m := msg.(type) {
	case *pglogrepl.BeginMessage:
		if st.cur != nil || st.prep != nil {
			return fmt.Errorf("BEGIN khi Txn trước chưa kết thúc")
		}
		pos := model.Pos{Engine: model.EnginePG, LSN: uint64(m.FinalLSN)}
		st.cur = &openTxn{pos: pos, ts: m.CommitTime, xid: m.Xid, skip: st.contains(pos)}
		if !st.cur.skip {
			return st.emit(reader.Event{Type: reader.EventQuery, Query: "BEGIN", Pos: pos, Timestamp: m.CommitTime, XID: uint64(m.Xid)})
		}
		return nil

	case *pglogrepl.CommitMessage:
		if st.cur == nil {
			return fmt.Errorf("COMMIT %s không có BEGIN", m.CommitLSN)
		}
		cur := st.cur
		st.cur = nil
		if !cur.skip {
			if err := st.emit(reader.Event{Type: reader.EventQuery, Query: "COMMIT", Pos: cur.pos, Timestamp: cur.ts, XID: uint64(cur.xid)}); err != nil {
				return err
			}
		}
		st.done(cur.pos)
		return nil

	case *pglogrepl.RelationMessageV2:
		st.relSeq++
		st.rels[m.RelationID] = &relState{msg: &m.RelationMessage, ver: st.relSeq}
		return nil

	case *pglogrepl.InsertMessageV2:
		return st.row(m.Xid, m.RelationID, model.OpInsert, nil, m.Tuple)
	case *pglogrepl.UpdateMessageV2:
		return st.row(m.Xid, m.RelationID, model.OpUpdate, m.OldTuple, m.NewTuple)
	case *pglogrepl.DeleteMessageV2:
		return st.row(m.Xid, m.RelationID, model.OpDelete, m.OldTuple, nil)

	case *pglogrepl.TruncateMessageV2:
		if st.cur != nil && st.cur.skip && !st.inStream && st.prep == nil {
			return nil
		}
		// Model Event/RowChange chưa có thao tác TRUNCATE: dừng thay vì bỏ qua im lặng (lệch dữ liệu).
		return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected,
			"pgoutput gửi TRUNCATE nhưng Strand chưa hỗ trợ; tạo publication với publish = 'insert, update, delete'", nil)

	case *pglogrepl.StreamStartMessageV2:
		st.inStream = true
		st.streamX = m.Xid
		if st.streams[m.Xid] == nil {
			st.streams[m.Xid] = newTxnBuf(m.Xid)
		}
		return nil

	case *pglogrepl.StreamStopMessageV2:
		st.inStream = false
		st.streamX = 0
		return nil

	case *pglogrepl.StreamCommitMessageV2:
		buf := st.streams[m.Xid]
		delete(st.streams, m.Xid)
		pos := model.Pos{Engine: model.EnginePG, LSN: uint64(m.CommitLSN)}
		if !st.contains(pos) {
			// Txn streaming luôn được server stream lại từ đầu sau restart, nên buf rỗng chỉ khi Txn
			// không có thay đổi nào thuộc publication.
			if err := st.emitBuffered(buf, pos, m.CommitTime, m.Xid); err != nil {
				return err
			}
		}
		st.done(pos)
		return nil

	case *pglogrepl.StreamAbortMessageV2:
		buf := st.streams[m.Xid]
		if buf == nil {
			return nil
		}
		if m.SubXid == m.Xid {
			delete(st.streams, m.Xid) // abort cả Txn: hủy, không phát
			return nil
		}
		// Rollback một subtransaction (ROLLBACK TO SAVEPOINT): cắt từ event đầu tiên của nó.
		if off, ok := buf.subxOff[m.SubXid]; ok {
			buf.events = buf.events[:off]
			for x, o := range buf.subxOff {
				if o >= off {
					delete(buf.subxOff, x)
				}
			}
			buf.relVer = map[uint32]uint64{} // TableMap có thể đã bị cắt: chèn lại khi cần
		}
		return nil

	case *pglogrepl.LogicalDecodingMessageV2:
		if m.Transactional || m.Prefix != WatermarkPrefix || st.cur != nil || st.prep != nil || st.inStream {
			return nil // message của ứng dụng khác: không ảnh hưởng dữ liệu
		}
		// Watermark của snapshot logic (02 §5): message không transactional được giải mã đúng tại vị
		// trí của nó trong WAL, nên mọi Txn có commit LSN nhỏ hơn đã được phát trước. Phát thành một
		// Txn rỗng có "commit LSN" = LSN của message để pipeline biết stream đã qua điểm này.
		pos := model.Pos{Engine: model.EnginePG, LSN: uint64(m.LSN)}
		if !st.contains(pos) {
			if err := st.emit(reader.Event{Type: reader.EventQuery, Query: "BEGIN", Pos: pos}); err != nil {
				return err
			}
			if err := st.emit(reader.Event{Type: reader.EventQuery, Query: "COMMIT", Pos: pos}); err != nil {
				return err
			}
		}
		st.done(pos)
		return nil

	case *pglogrepl.OriginMessage, *pglogrepl.TypeMessageV2:
		// Origin, kiểu tự định nghĩa (giá trị vẫn ở dạng text) và pg_logical_emit_message: không ảnh hưởng dữ liệu.
		return nil
	}
	return nil
}

// handleTwoPhase xử lý Begin Prepare / Prepare / Stream Prepare / Commit Prepared / Rollback Prepared.
func (st *streamState) handleTwoPhase(pm *prepareMsg) error {
	switch pm.Tag {
	case msgBeginPrepare:
		if st.cur != nil || st.prep != nil {
			return fmt.Errorf("BEGIN PREPARE khi Txn trước chưa kết thúc")
		}
		st.prep = newTxnBuf(pm.Xid)
		st.prep.prepareLSN = uint64(pm.LSN)
		st.prepared[pm.Xid] = st.prep // giữ slot ngay từ Begin Prepare
		st.updateHold()
		return nil

	case msgPrepare:
		if st.prep == nil || st.prep.xid != pm.Xid {
			return fmt.Errorf("PREPARE xid %d không khớp BEGIN PREPARE", pm.Xid)
		}
		// Chưa commit: KHÔNG phát, chỉ giữ trong RAM.
		st.prep = nil
		return nil

	case msgStreamPrepare:
		buf := st.streams[pm.Xid]
		delete(st.streams, pm.Xid)
		if buf == nil {
			buf = newTxnBuf(pm.Xid)
		}
		buf.prepareLSN = uint64(pm.LSN)
		st.prepared[pm.Xid] = buf
		st.updateHold()
		return nil

	case msgCommitPrepared:
		buf := st.prepared[pm.Xid]
		delete(st.prepared, pm.Xid)
		st.updateHold()
		pos := model.Pos{Engine: model.EnginePG, LSN: uint64(pm.LSN)}
		if !st.contains(pos) {
			if buf == nil {
				// Server chỉ gửi COMMIT PREPARED mà không gửi lại PREPARE: phần thân Txn đã mất khỏi RAM
				// (không được xảy ra nhờ cơ chế hold; nếu xảy ra thì dừng thay vì phát Txn rỗng).
				return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
					fmt.Sprintf("COMMIT PREPARED %q (xid %d, LSN %s) mà không có nội dung PREPARE", pm.GID, pm.Xid, pm.LSN), nil)
			}
			if err := st.emitBuffered(buf, pos, pm.Time, pm.Xid); err != nil {
				return err
			}
		}
		st.done(pos)
		return nil

	case msgRollbackPrepared:
		// Hủy, không phát.
		delete(st.prepared, pm.Xid)
		st.updateHold()
		return nil
	}
	return nil
}

// updateHold đặt hold = prepare LSN nhỏ nhất đang treo.
func (st *streamState) updateHold() {
	var h uint64
	for _, b := range st.prepared {
		if b.prepareLSN != 0 && (h == 0 || b.prepareLSN < h) {
			h = b.prepareLSN
		}
	}
	st.r.hold.Store(h)
}

func (st *streamState) contains(pos model.Pos) bool {
	return st.applied != nil && st.applied.Contains(pos)
}

// done ghi nhận đã xử lý xong Txn có commit LSN pos (phát hoặc bỏ qua).
func (st *streamState) done(pos model.Pos) {
	if pos.LSN > st.r.progress.Load() {
		st.r.progress.Store(pos.LSN)
	}
	st.r.mu.Lock()
	st.r.pos = pos
	st.r.mu.Unlock()
}

// row giải mã một thay đổi dòng và chuyển tới đích đúng: buffer streaming, buffer two-phase, hoặc
// phát thẳng trong Txn thường.
func (st *streamState) row(xid uint32, relID uint32, op model.Op, oldT, newT *pglogrepl.TupleData) error {
	var buf *txnBuf
	switch {
	case st.inStream:
		buf = st.streams[st.streamX]
		if buf == nil {
			buf = newTxnBuf(st.streamX)
			st.streams[st.streamX] = buf
		}
	case st.prep != nil:
		buf = st.prep
	case st.cur != nil:
		if st.cur.skip {
			return nil
		}
	default:
		return fmt.Errorf("thay đổi dòng nằm ngoài transaction")
	}

	rs := st.rels[relID]
	if rs == nil {
		return fmt.Errorf("thay đổi dòng của relation %d chưa nhận Relation message", relID)
	}
	ev, err := rowEvent(rs.msg, op, oldT, newT)
	if err != nil {
		return err
	}

	if buf != nil {
		if st.inStream {
			if _, ok := buf.subxOff[xid]; !ok {
				buf.subxOff[xid] = len(buf.events)
			}
		}
		if buf.relVer[relID] != rs.ver {
			buf.events = append(buf.events, tableMapEvent(rs.msg))
			buf.relVer[relID] = rs.ver
		}
		buf.events = append(buf.events, ev)
		return nil
	}

	ev.Pos, ev.Timestamp = st.cur.pos, st.cur.ts
	if st.announced[relID] != rs.ver {
		tm := tableMapEvent(rs.msg)
		tm.Pos, tm.Timestamp = st.cur.pos, st.cur.ts
		if err := st.emit(tm); err != nil {
			return err
		}
		st.announced[relID] = rs.ver
	}
	return st.emit(ev)
}

// emitBuffered phát một Txn đã đệm: BEGIN, các event (gắn commit LSN), COMMIT.
func (st *streamState) emitBuffered(buf *txnBuf, pos model.Pos, ts time.Time, xid uint32) error {
	if err := st.emit(reader.Event{Type: reader.EventQuery, Query: "BEGIN", Pos: pos, Timestamp: ts, XID: uint64(xid)}); err != nil {
		return err
	}
	if buf != nil {
		for _, ev := range buf.events {
			ev.Pos, ev.Timestamp = pos, ts
			if err := st.emit(ev); err != nil {
				return err
			}
		}
		// Schema mà assembler đang giữ có thể là phiên bản cũ trong buffer: buộc phát lại TableMap.
		clear(st.announced)
	}
	return st.emit(reader.Event{Type: reader.EventQuery, Query: "COMMIT", Pos: pos, Timestamp: ts, XID: uint64(xid)})
}

// emit gửi Event; trả errStop nếu ctx bị hủy trong lúc chờ (backpressure).
func (st *streamState) emit(ev reader.Event) error {
	select {
	case st.out <- ev:
		return nil
	case <-st.ctx.Done():
		return errStop
	}
}

// tableMapEvent tạo EventTableMap từ Relation. PKColumns là các cột replica identity (cờ 1): với
// REPLICA IDENTITY DEFAULT/USING INDEX chính là khóa; với FULL thì là mọi cột.
func tableMapEvent(rel *pglogrepl.RelationMessage) reader.Event {
	cols := make([]string, len(rel.Columns))
	var pk []string
	for i, c := range rel.Columns {
		cols[i] = c.Name
		if c.Flags&1 == 1 {
			pk = append(pk, c.Name)
		}
	}
	return reader.Event{
		Type: reader.EventTableMap, Schema: rel.Namespace, Table: rel.RelationName,
		TableID: uint64(rel.RelationID), Columns: cols, PKColumns: pk,
	}
}

// rowEvent tạo EventRows. UPDATE luôn có cặp [before, after] như MySQL; khi server không gửi old tuple
// (replica identity không đổi) thì before = after.
func rowEvent(rel *pglogrepl.RelationMessage, op model.Op, oldT, newT *pglogrepl.TupleData) (reader.Event, error) {
	ev := reader.Event{Type: reader.EventRows, Op: op, Schema: rel.Namespace, Table: rel.RelationName, TableID: uint64(rel.RelationID)}
	switch op {
	case model.OpInsert:
		row, err := tupleToRow(rel, newT)
		if err != nil {
			return ev, err
		}
		ev.Rows = [][]any{row}
	case model.OpDelete:
		row, err := tupleToRow(rel, oldT)
		if err != nil {
			return ev, err
		}
		ev.Rows = [][]any{row}
	case model.OpUpdate:
		after, err := tupleToRow(rel, newT)
		if err != nil {
			return ev, err
		}
		before := after
		if oldT != nil {
			if before, err = tupleToRow(rel, oldT); err != nil {
				return ev, err
			}
		}
		ev.Rows = [][]any{before, after}
	}
	return ev, nil
}

// tupleToRow map TupleData sang []any theo thứ tự cột của Relation.
func tupleToRow(rel *pglogrepl.RelationMessage, t *pglogrepl.TupleData) ([]any, error) {
	if t == nil {
		return nil, fmt.Errorf("%s.%s: thiếu tuple data", rel.Namespace, rel.RelationName)
	}
	if len(t.Columns) != len(rel.Columns) {
		return nil, fmt.Errorf("%s.%s: tuple có %d cột, relation có %d", rel.Namespace, rel.RelationName, len(t.Columns), len(rel.Columns))
	}
	row := make([]any, len(t.Columns))
	for i, c := range t.Columns {
		v, err := formatColValue(c, rel.Columns[i])
		if err != nil {
			return nil, fmt.Errorf("%s.%s cột %s: %w", rel.Namespace, rel.RelationName, rel.Columns[i].Name, err)
		}
		row[i] = v
	}
	return row, nil
}
