package pg

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"strand/internal/model"
	"strand/internal/reader"
)

// eventLog là phần thân của một Txn streaming / two-phase mà reader phải giữ tới khi biết kết cục
// (review R08). Event nằm trong RAM tới khi bị spill (streamState quyết định theo tổng byte của mọi
// Txn đang treo, xem spillIfOver); sau đó toàn bộ phần thân nằm trong một file tạm và event mới được
// ghi tiếp vào file. Rollback subtransaction cắt tại mốc của subxid (mark), kể cả khi đã ở file.
type eventLog struct {
	dir string

	mem      []reader.Event
	memSizes []int64
	memBytes int64

	f       *os.File
	w       *bufio.Writer
	fileOff int64 // số byte đã ghi
	n       int   // tổng số event

	// marks: mốc event đầu tiên của từng (sub)xid — chỉ số event và (khi đã spill) vị trí byte.
	marks map[uint32]logMark
}

type logMark struct {
	idx int
	off int64 // vị trí byte trong file; -1 khi mốc còn trong RAM
}

func newEventLog(dir string) *eventLog {
	return &eventLog{dir: dir, marks: map[uint32]logMark{}}
}

func (l *eventLog) len() int { return l.n }

func (l *eventLog) spilled() bool { return l.f != nil }

// mark ghi mốc event kế tiếp cho xid nếu chưa có.
func (l *eventLog) mark(xid uint32) {
	if _, ok := l.marks[xid]; ok {
		return
	}
	m := logMark{idx: l.n, off: -1}
	if l.spilled() {
		m.off = l.fileOff
	}
	l.marks[xid] = m
}

// append thêm event; trả số byte RAM tăng thêm (0 nếu đã spill).
func (l *eventLog) append(ev reader.Event) (int64, error) {
	if l.spilled() {
		n, err := l.writeEvent(ev)
		if err != nil {
			return 0, err
		}
		l.fileOff += n
		l.n++
		return 0, nil
	}
	sz := eventSize(ev)
	l.mem = append(l.mem, ev)
	l.memSizes = append(l.memSizes, sz)
	l.memBytes += sz
	l.n++
	return sz, nil
}

// spill chuyển toàn bộ event trong RAM ra file tạm; trả số byte RAM được giải phóng.
func (l *eventLog) spill() (int64, error) {
	if l.spilled() {
		return 0, nil
	}
	if err := os.MkdirAll(l.dir, 0o750); err != nil {
		return 0, fmt.Errorf("pg reader spill dir: %w", err)
	}
	f, err := os.CreateTemp(l.dir, "pgtxn-*.spill")
	if err != nil {
		return 0, fmt.Errorf("pg reader spill file: %w", err)
	}
	l.f, l.w, l.fileOff = f, bufio.NewWriterSize(f, 256<<10), 0
	offs := make([]int64, len(l.mem))
	for i, ev := range l.mem {
		offs[i] = l.fileOff
		n, err := l.writeEvent(ev)
		if err != nil {
			return 0, err
		}
		l.fileOff += n
	}
	for x, m := range l.marks {
		if m.off < 0 {
			if m.idx < len(offs) {
				m.off = offs[m.idx]
			} else {
				m.off = l.fileOff
			}
			l.marks[x] = m
		}
	}
	freed := l.memBytes
	l.mem, l.memSizes, l.memBytes = nil, nil, 0
	return freed, nil
}

// truncateTo cắt bỏ từ mốc của xid trở đi (ROLLBACK TO SAVEPOINT); trả số byte RAM được giải phóng.
// ok=false nếu xid không có mốc.
func (l *eventLog) truncateTo(xid uint32) (freed int64, ok bool, err error) {
	m, ok := l.marks[xid]
	if !ok {
		return 0, false, nil
	}
	if l.spilled() {
		if err := l.w.Flush(); err != nil {
			return 0, true, err
		}
		if err := l.f.Truncate(m.off); err != nil {
			return 0, true, err
		}
		if _, err := l.f.Seek(m.off, io.SeekStart); err != nil {
			return 0, true, err
		}
		l.w.Reset(l.f)
		l.fileOff = m.off
	} else {
		for _, sz := range l.memSizes[m.idx:] {
			freed += sz
		}
		l.mem, l.memSizes = l.mem[:m.idx], l.memSizes[:m.idx]
		l.memBytes -= freed
	}
	l.n = m.idx
	for x, o := range l.marks {
		if o.idx >= m.idx {
			delete(l.marks, x)
		}
	}
	return freed, true, nil
}

// replay gọi fn cho từng event theo thứ tự.
func (l *eventLog) replay(fn func(reader.Event) error) error {
	if !l.spilled() {
		for _, ev := range l.mem {
			if err := fn(ev); err != nil {
				return err
			}
		}
		return nil
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	rf, err := os.Open(l.f.Name())
	if err != nil {
		return err
	}
	defer rf.Close()
	r := bufio.NewReaderSize(rf, 256<<10)
	for i := 0; i < l.n; i++ {
		ev, err := readEvent(r)
		if err != nil {
			return fmt.Errorf("read spilled event %d/%d of %s: %w", i, l.n, filepath.Base(l.f.Name()), err)
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

// close xóa file tạm (nếu có); trả số byte RAM được giải phóng.
func (l *eventLog) close() int64 {
	freed := l.memBytes
	l.mem, l.memSizes, l.memBytes = nil, nil, 0
	if l.f != nil {
		name := l.f.Name()
		_ = l.f.Close()
		_ = os.Remove(name)
		l.f, l.w = nil, nil
	}
	return freed
}

// eventSize ước lượng byte RAM của event (giá trị + phần cố định).
func eventSize(ev reader.Event) int64 {
	n := int64(96 + len(ev.Schema) + len(ev.Table))
	for _, c := range ev.Columns {
		n += int64(len(c)) + 16
	}
	for _, row := range ev.Rows {
		n += 24
		for _, v := range row {
			n += 16
			switch x := v.(type) {
			case string:
				n += int64(len(x))
			case []byte:
				n += int64(len(x))
			case model.ColValue:
				n += int64(len(x.Name)) + 32
			default:
				n += 8
			}
		}
	}
	return n
}

// Định dạng một event trong file: độ dài payload (uvarint) rồi payload.
const valUnchangedCol byte = 0xF0 // model.ColValue{Unchanged: true} — cột TOAST vắng mặt

func (l *eventLog) writeEvent(ev reader.Event) (int64, error) {
	b, err := encodeEvent(nil, ev)
	if err != nil {
		return 0, err
	}
	var hdr [binary.MaxVarintLen64]byte
	h := binary.PutUvarint(hdr[:], uint64(len(b)))
	if _, err := l.w.Write(hdr[:h]); err != nil {
		return 0, err
	}
	if _, err := l.w.Write(b); err != nil {
		return 0, err
	}
	return int64(h + len(b)), nil
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func encodeEvent(b []byte, ev reader.Event) ([]byte, error) {
	b = appendStr(b, string(ev.Type))
	b = appendStr(b, string(ev.Op))
	b = appendStr(b, ev.Schema)
	b = appendStr(b, ev.Table)
	b = binary.AppendUvarint(b, ev.TableID)
	b = binary.AppendUvarint(b, ev.XID)
	for _, list := range [][]string{ev.Columns, ev.PKColumns} {
		b = binary.AppendUvarint(b, uint64(len(list)))
		for _, c := range list {
			b = appendStr(b, c)
		}
	}
	b = binary.AppendUvarint(b, uint64(len(ev.Rows)))
	for _, row := range ev.Rows {
		b = binary.AppendUvarint(b, uint64(len(row)))
		for _, v := range row {
			if cv, ok := v.(model.ColValue); ok {
				if !cv.Unchanged || cv.Value != nil {
					return nil, fmt.Errorf("pg reader spill: unexpected column value %+v", cv)
				}
				b = appendStr(append(b, valUnchangedCol), cv.Name)
				continue
			}
			var err error
			if b, err = model.AppendValue(b, v); err != nil {
				return nil, err
			}
		}
	}
	return b, nil
}

func readStr(r *bufio.Reader) (string, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readEvent(r *bufio.Reader) (reader.Event, error) {
	var ev reader.Event
	if _, err := binary.ReadUvarint(r); err != nil { // độ dài payload (chỉ để kiểm/bỏ qua)
		return ev, err
	}
	t, err := readStr(r)
	if err != nil {
		return ev, err
	}
	ev.Type = reader.EventType(t)
	op, err := readStr(r)
	if err != nil {
		return ev, err
	}
	ev.Op = model.Op(op)
	if ev.Schema, err = readStr(r); err != nil {
		return ev, err
	}
	if ev.Table, err = readStr(r); err != nil {
		return ev, err
	}
	if ev.TableID, err = binary.ReadUvarint(r); err != nil {
		return ev, err
	}
	if ev.XID, err = binary.ReadUvarint(r); err != nil {
		return ev, err
	}
	for _, dst := range []*[]string{&ev.Columns, &ev.PKColumns} {
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return ev, err
		}
		if n > 0 {
			*dst = make([]string, n)
		}
		for i := range *dst {
			if (*dst)[i], err = readStr(r); err != nil {
				return ev, err
			}
		}
	}
	nr, err := binary.ReadUvarint(r)
	if err != nil {
		return ev, err
	}
	if nr > 0 {
		ev.Rows = make([][]any, nr)
	}
	for i := range ev.Rows {
		nc, err := binary.ReadUvarint(r)
		if err != nil {
			return ev, err
		}
		row := make([]any, nc)
		for j := range row {
			tag, err := r.ReadByte()
			if err != nil {
				return ev, err
			}
			if tag == valUnchangedCol {
				name, err := readStr(r)
				if err != nil {
					return ev, err
				}
				row[j] = model.ColValue{Name: name, Unchanged: true}
				continue
			}
			if err := r.UnreadByte(); err != nil {
				return ev, err
			}
			if row[j], err = model.ReadValue(r); err != nil {
				return ev, err
			}
		}
		ev.Rows[i] = row
	}
	return ev, nil
}
