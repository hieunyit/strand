package archive

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"

	"strand/internal/exitcode"
)

var testSID = uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")

// rawEvent dựng event binlog thô: header 19 byte (timestamp, type, server_id, event_size, log_pos,
// flags) + body.
func rawEvent(t replication.EventType, ts, logPos uint32, body []byte) []byte {
	b := make([]byte, replication.EventHeaderSize, replication.EventHeaderSize+len(body))
	binary.LittleEndian.PutUint32(b[0:], ts)
	b[4] = byte(t)
	binary.LittleEndian.PutUint32(b[5:], 1)
	binary.LittleEndian.PutUint32(b[9:], uint32(replication.EventHeaderSize+len(body)))
	binary.LittleEndian.PutUint32(b[13:], logPos)
	return append(b, body...)
}

func ev(t replication.EventType, ts, logPos uint32, body []byte, inner replication.Event) *replication.BinlogEvent {
	raw := rawEvent(t, ts, logPos, body)
	return &replication.BinlogEvent{
		RawData: raw,
		Header:  &replication.EventHeader{Timestamp: ts, EventType: t, EventSize: uint32(len(raw)), LogPos: logPos},
		Event:   inner,
	}
}

func rotate(next string, fake bool) *replication.BinlogEvent {
	ts, pos := uint32(1700000000), uint32(500)
	if fake {
		ts, pos = 0, 0
	}
	body := append(make([]byte, 8), next...)
	return ev(replication.ROTATE_EVENT, ts, pos, body, &replication.RotateEvent{Position: 4, NextLogName: []byte(next)})
}

func fde() *replication.BinlogEvent {
	return ev(replication.FORMAT_DESCRIPTION_EVENT, 1700000000, 0, bytes.Repeat([]byte{0xFD}, 40), &replication.FormatDescriptionEvent{})
}

func gtid(gno int64) *replication.BinlogEvent {
	body := make([]byte, 1+16+8+17)
	copy(body[1:17], testSID[:])
	binary.LittleEndian.PutUint64(body[17:25], uint64(gno))
	return ev(replication.GTID_EVENT, 1700000000, 100, body, nil)
}

func query(tag byte) *replication.BinlogEvent {
	return ev(replication.QUERY_EVENT, 1700000000, 200, bytes.Repeat([]byte{tag}, 30), nil)
}

func openTest(t *testing.T, dir, start string) *Archiver {
	t.Helper()
	a, err := OpenArchiver(ArchiverConfig{Dir: dir, StartGTID: start, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func feed(t *testing.T, a *Archiver, evs ...*replication.BinlogEvent) {
	t.Helper()
	for _, e := range evs {
		if err := a.handle(e); err != nil {
			t.Fatalf("handle %s: %v", e.Header.EventType, err)
		}
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func readState(t *testing.T, dir string) archiveState {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, stateFile))
	if err != nil {
		t.Fatal(err)
	}
	var st archiveState
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

// Một file của server được ghi nguyên byte (magic + event, trừ rotate giả và heartbeat), chỉ có tên
// chốt sau ROTATE thật; tập GTID chỉ tăng khi file chốt; file dở bị bỏ khi đóng.
func TestArchiver_FinalizesOnRotateAndKeepsRawBytes(t *testing.T) {
	dir := t.TempDir()
	a := openTest(t, dir, testSID.String()+":1-10")
	f1 := []*replication.BinlogEvent{fde(), gtid(11), query('a'), gtid(12), query('b')}
	end := rotate("binlog.000008", false)
	feed(t, a, rotate("binlog.000007", true))
	feed(t, a, f1...)
	feed(t, a, ev(replication.HEARTBEAT_EVENT, 0, 0, []byte("binlog.000007"), nil))
	if st := readState(t, dir); st.Executed != testSID.String()+":1-10" {
		t.Fatalf("state advanced before the file was finalized: %q", st.Executed)
	}
	feed(t, a, end, rotate("binlog.000008", true), fde(), gtid(13), query('c'))

	want := append([]byte{}, replication.BinLogFileHeader...)
	for _, e := range append(f1, end) {
		want = append(want, e.RawData...)
	}
	got, err := os.ReadFile(filepath.Join(dir, "00000001-binlog.000007"))
	if err != nil {
		t.Fatalf("finalized file: %v (dir %v)", err, listDir(t, dir))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file content differs: %d bytes, want %d", len(got), len(want))
	}
	st := readState(t, dir)
	if st.Executed != testSID.String()+":1-12" || st.LastSeq != 1 {
		t.Fatalf("state = %+v, want :1-12 last_seq 1", st)
	}
	if _, err := os.Stat(filepath.Join(dir, ".partial-00000002-binlog.000008")); err != nil {
		t.Fatalf("partial file of the open server file: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	names := listDir(t, dir)
	for _, n := range names {
		if n[0] == '.' && n != stateFile && n != lockFile {
			t.Fatalf("leftover %s after close: %v", n, names)
		}
	}
	// ArchiveReader chỉ thấy file đã chốt.
	r, _ := New(Config{Dir: dir})
	files, err := r.scanFiles()
	if err != nil || len(files) != 1 || filepath.Base(files[0]) != "00000001-binlog.000007" {
		t.Fatalf("reader sees %v, %v", files, err)
	}
}

// File của server kết thúc không có ROTATE thật (server dừng): rotate giả báo tên file MỚI trước FDE;
// file cũ phải được chốt dưới tên CŨ.
func TestArchiver_FileEndingWithoutRotateKeepsItsName(t *testing.T) {
	dir := t.TempDir()
	a := openTest(t, dir, "")
	defer a.Close()
	feed(t, a, rotate("binlog.000001", true), fde(), gtid(1), query('a'),
		rotate("binlog.000002", true), fde(), gtid(2))
	if _, err := os.Stat(filepath.Join(dir, "00000001-binlog.000001")); err != nil {
		t.Fatalf("old file not finalized under its own name: %v (dir %v)", err, listDir(t, dir))
	}
	if _, err := os.Stat(filepath.Join(dir, ".partial-00000002-binlog.000002")); err != nil {
		t.Fatalf("new file: %v (dir %v)", err, listDir(t, dir))
	}
	if got := a.Executed(); got != testSID.String()+":1" {
		t.Fatalf("executed = %q", got)
	}
}

// Crash giữa đổi tên file và ghi trạng thái: lần mở sau quét GTID của file chốt mà trạng thái chưa
// biết, xóa file dở, và đánh số tiếp — không mất, không lặp transaction.
func TestArchiver_RecoversFileFinalizedAfterLastStateWrite(t *testing.T) {
	dir := t.TempDir()
	a := openTest(t, dir, "")
	feed(t, a, rotate("binlog.000001", true), fde(), gtid(1), query('a'), rotate("binlog.000002", false),
		rotate("binlog.000002", true), fde(), gtid(2), gtid(3), rotate("binlog.000003", false),
		rotate("binlog.000003", true), fde(), gtid(4))
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	// Giả lập crash: trạng thái chỉ kịp ghi tới file 1.
	if err := writeFileSync(filepath.Join(dir, stateFile), []byte(`{"version":1,"executed":"`+testSID.String()+`:1","last_seq":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".partial-00000003-binlog.000003"), []byte("junk"), 0o640); err != nil {
		t.Fatal(err)
	}
	b := openTest(t, dir, "")
	defer b.Close()
	if got := b.Executed(); got != testSID.String()+":1-3" {
		t.Fatalf("recovered executed = %q, want :1-3", got)
	}
	if b.nextSeq != 3 {
		t.Fatalf("next seq = %d, want 3", b.nextSeq)
	}
	if st := readState(t, dir); st.LastSeq != 2 || st.Executed != testSID.String()+":1-3" {
		t.Fatalf("state after recovery = %+v", st)
	}
	for _, n := range listDir(t, dir) {
		if len(n) > len(partialPref) && n[:len(partialPref)] == partialPref {
			t.Fatalf("partial file kept: %s", n)
		}
	}
}

func TestArchiver_OneWriterPerDir(t *testing.T) {
	dir := t.TempDir()
	a := openTest(t, dir, "")
	defer a.Close()
	_, err := OpenArchiver(ArchiverConfig{Dir: dir, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	var se *exitcode.Error
	if !errors.As(err, &se) || se.Code != exitcode.ErrPipelineLocked {
		t.Fatalf("second archiver on the same dir: %v", err)
	}
}

// Thư mục có file binlog nhưng không có trạng thái (không do archiver ghi, hoặc trạng thái bị xóa):
// từ chối thay vì đoán vị trí.
func TestArchiver_RejectsForeignDirWithoutState(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "00000001-binlog.000001"), replication.BinLogFileHeader, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenArchiver(ArchiverConfig{Dir: dir, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}); err == nil {
		t.Fatal("expected rejection")
	}
}

func TestArchiver_TaggedGTIDRejected(t *testing.T) {
	a := openTest(t, t.TempDir(), "")
	defer a.Close()
	feed(t, a, rotate("binlog.000001", true), fde())
	err := a.handle(ev(replication.GTID_TAGGED_LOG_EVENT, 1700000000, 100, make([]byte, 40), nil))
	var se *exitcode.Error
	if !errors.As(err, &se) {
		t.Fatalf("tagged GTID: %v", err)
	}
}

func TestArchiver_PurgedErrorIsLogGap(t *testing.T) {
	if !isPurgedErr(&gomysql.MyError{Code: 1236, Message: "purged"}) {
		t.Fatal("1236 must be a purged-log error")
	}
	if isPurgedErr(errors.New("connection reset")) {
		t.Fatal("network error is not purged")
	}
}
