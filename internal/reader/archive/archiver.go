package archive

// Archiver: `strand archive binlog` (spec 02 §10, design §3 "Lưu trữ binlog") — thay cho
// `mysqlbinlog --read-from-remote-server --raw --stop-never`. Ghi binlog thô (byte của server) vào một
// thư mục mà ArchiveReader (seed apply --archive) đọc được.
//
// Bố cục thư mục:
//
//	00000001-binlog.000123      file đã chốt: đủ transaction, đã fsync — ArchiveReader đọc
//	00000002-binlog.000124
//	.partial-00000003-binlog.000125   file đang ghi — reader bỏ qua (tên bắt đầu bằng '.')
//	.strand-archive.json        tập GTID đã chốt + số thứ tự file chốt cuối
//	.strand-archive.lock        khóa: một archiver cho mỗi thư mục
//
// Tên file mang số thứ tự tăng dần của archive trước tên file trên server: ArchiveReader đọc theo thứ
// tự tên, nên thứ tự vẫn đúng khi replica được dựng lại và đánh số binlog lại từ đầu (design §11.3 #22).
//
// Đúng đắn khi crash: file chỉ được đổi sang tên chốt ở ranh giới file của server (ROTATE), là ranh
// giới transaction. Trạng thái được ghi SAU khi đổi tên; nếu crash ở giữa, lúc khởi động file đã chốt
// mà trạng thái chưa biết được quét lại để lấy GTID. File .partial bị xóa và phần của nó được stream
// lại: kết nối dùng GTID auto-position với tập đã chốt, nên server gửi đúng các transaction còn thiếu —
// không lặp, không hở. Server đã purge transaction còn thiếu → lỗi 1236 → log_gap (exit 5).

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"

	"strand/internal/exitcode"
)

const (
	stateFile   = ".strand-archive.json"
	lockFile    = ".strand-archive.lock"
	partialPref = ".partial-"
)

var finalName = regexp.MustCompile(`^([0-9]{8})-(.+)$`)

// ArchiverConfig cấu hình Archiver.
type ArchiverConfig struct {
	Dir string
	// Syncer: kết nối replication (host, user, TLS, ServerID…). RawModeEnabled, DisableRetrySync và
	// HeartbeatPeriod do Archiver đặt.
	Syncer replication.BinlogSyncerConfig
	// StartGTID: tập GTID KHÔNG cần lưu ở lần chạy đầu (thư mục chưa có trạng thái). Lần sau đọc từ
	// trạng thái và giá trị này bị bỏ qua.
	StartGTID string
	Heartbeat time.Duration
	// RetryWait: chờ trước khi kết nối lại sau lỗi tạm (mặc định 5s).
	RetryWait time.Duration
	Log       *slog.Logger
}

type archiveState struct {
	Version  int    `json:"version"`
	Executed string `json:"executed"`
	LastSeq  int    `json:"last_seq"`
}

// Archiver ghi binlog liên tục vào Dir.
type Archiver struct {
	cfg      ArchiverConfig
	log      *slog.Logger
	executed *gomysql.MysqlGTIDSet // GTID của mọi file đã chốt (và StartGTID)
	nextSeq  int
	lock     *os.File

	// file đang ghi
	curName  string // tên file kế tiếp trên server (từ ROTATE)
	fileName string // tên trên server của file đang ghi
	f        *os.File
	w        *bufio.Writer
	partial  string
	seq      int
	pending  []gtidRef // GTID trong file đang ghi
}

type gtidRef struct {
	sid uuid.UUID
	gno int64
}

// OpenArchiver khóa thư mục, khôi phục trạng thái (xem đầu file) và trả Archiver sẵn sàng Run.
func OpenArchiver(cfg ArchiverConfig) (*Archiver, error) {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.RetryWait <= 0 {
		cfg.RetryWait = 5 * time.Second
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("archive dir: %w", err)
	}
	lf, err := os.OpenFile(filepath.Join(cfg.Dir, lockFile), os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, fmt.Errorf("archive lock: %w", err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lf.Close()
		return nil, exitcode.New(exitcode.ErrPipelineLocked, exitcode.Fenced,
			fmt.Sprintf("another strand archive binlog is writing to %s", cfg.Dir), err)
	}
	a := &Archiver{cfg: cfg, log: cfg.Log, lock: lf}
	if err := a.recover(); err != nil {
		a.Close()
		return nil, err
	}
	return a, nil
}

// Executed trả tập GTID đã chốt trong archive.
func (a *Archiver) Executed() string { return a.executed.String() }

// Close bỏ file đang ghi dở (sẽ được stream lại) và nhả khóa.
func (a *Archiver) Close() error {
	a.discardPartial()
	if a.lock != nil {
		_ = syscall.Flock(int(a.lock.Fd()), syscall.LOCK_UN)
		err := a.lock.Close()
		a.lock = nil
		return err
	}
	return nil
}

func (a *Archiver) recover() error {
	st := archiveState{Version: 1}
	b, err := os.ReadFile(filepath.Join(a.cfg.Dir, stateFile))
	fresh := errors.Is(err, os.ErrNotExist)
	switch {
	case fresh:
		st.Executed = a.cfg.StartGTID
	case err != nil:
		return fmt.Errorf("read archive state: %w", err)
	default:
		if err := json.Unmarshal(b, &st); err != nil {
			return fmt.Errorf("parse archive state %s: %w", stateFile, err)
		}
	}
	gs, err := gomysql.ParseMysqlGTIDSet(st.Executed)
	if err != nil {
		return fmt.Errorf("archive GTID set %q: %w", st.Executed, err)
	}
	a.executed = gs.(*gomysql.MysqlGTIDSet)

	entries, err := os.ReadDir(a.cfg.Dir)
	if err != nil {
		return err
	}
	maxSeq := st.LastSeq
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if len(name) > len(partialPref) && name[:len(partialPref)] == partialPref {
			if err := os.Remove(filepath.Join(a.cfg.Dir, name)); err != nil {
				return err
			}
			a.log.Info("archive: removed incomplete file (it will be streamed again)", "file", name)
			continue
		}
		m := finalName.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		seq, _ := strconv.Atoi(m[1])
		if fresh && seq > 0 {
			return fmt.Errorf("archive dir %s has binlog files but no %s; it was not written by strand archive binlog (or the state was deleted)", a.cfg.Dir, stateFile)
		}
		if seq > st.LastSeq {
			// Chốt sau lần ghi trạng thái cuối (crash giữa đổi tên và ghi trạng thái).
			refs, err := scanFileGTIDs(filepath.Join(a.cfg.Dir, name))
			if err != nil {
				return fmt.Errorf("rescan %s: %w", name, err)
			}
			for _, r := range refs {
				a.executed.AddGTID(r.sid, r.gno)
			}
			a.log.Info("archive: recovered GTIDs of a file finalized after the last state write", "file", name, "gtids", len(refs))
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	a.nextSeq = maxSeq + 1
	return a.saveState(maxSeq)
}

func (a *Archiver) saveState(lastSeq int) error {
	b, err := json.Marshal(archiveState{Version: 1, Executed: a.executed.String(), LastSeq: lastSeq})
	if err != nil {
		return err
	}
	tmp := filepath.Join(a.cfg.Dir, stateFile+".tmp")
	if err := writeFileSync(tmp, b); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(a.cfg.Dir, stateFile)); err != nil {
		return err
	}
	return syncDir(a.cfg.Dir)
}

// Run stream binlog từ tập đã chốt tới khi ctx bị hủy (trả nil) hoặc gặp lỗi không thể tự hồi phục.
// Lỗi tạm (mất kết nối): bỏ file dở, chờ RetryWait rồi kết nối lại từ tập đã chốt.
func (a *Archiver) Run(ctx context.Context) error {
	for {
		err := a.streamOnce(ctx)
		a.discardPartial()
		if ctx.Err() != nil {
			return nil
		}
		if isPurgedErr(err) {
			return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
				fmt.Sprintf("source no longer has binlog for transactions after %s: the archive has a gap", a.executed.String()), err)
		}
		var se *exitcode.Error
		if errors.As(err, &se) {
			return err
		}
		a.log.Warn("archive: stream interrupted; reconnecting", "err", fmt.Sprint(err), "retry_in", a.cfg.RetryWait.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(a.cfg.RetryWait):
		}
	}
}

func (a *Archiver) streamOnce(ctx context.Context) error {
	scfg := a.cfg.Syncer
	scfg.RawModeEnabled = true
	scfg.DisableRetrySync = true // vị trí khởi động lại do Archiver quyết (tập đã chốt), không phải syncer
	if a.cfg.Heartbeat > 0 {
		scfg.HeartbeatPeriod = a.cfg.Heartbeat
		scfg.ReadTimeout = 3 * a.cfg.Heartbeat
	}
	syncer := replication.NewBinlogSyncer(scfg)
	defer syncer.Close()
	streamer, err := syncer.StartSyncGTID(a.executed.Clone())
	if err != nil {
		return fmt.Errorf("start binlog stream: %w", err)
	}
	a.log.Info("archive: streaming", "from_gtid_executed", a.executed.String())
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return err
		}
		if err := a.handle(ev); err != nil {
			return err
		}
	}
}

// handle ghi một event (raw mode: chỉ ROTATE và FORMAT_DESCRIPTION được parse).
func (a *Archiver) handle(ev *replication.BinlogEvent) error {
	h := ev.Header
	switch h.EventType {
	case replication.HEARTBEAT_EVENT, replication.HEARTBEAT_LOG_EVENT_V2:
		return nil
	case replication.ROTATE_EVENT:
		re, ok := ev.Event.(*replication.RotateEvent)
		if !ok {
			return fmt.Errorf("rotate event without body")
		}
		if h.Timestamp == 0 || h.LogPos == 0 {
			// Rotate giả của dump thread: chỉ cho biết tên file kế tiếp, không nằm trong file.
			a.curName = string(re.NextLogName)
			return nil
		}
		// Rotate thật: event cuối của file hiện tại.
		if err := a.write(ev.RawData); err != nil {
			return err
		}
		a.curName = string(re.NextLogName)
		return a.finalize()
	case replication.FORMAT_DESCRIPTION_EVENT:
		// Đầu một file của server. File trước kết thúc không có ROTATE (ví dụ server dừng: STOP_EVENT)
		// thì chốt nó ở đây.
		if a.f != nil {
			if err := a.finalize(); err != nil {
				return err
			}
		}
		if err := a.open(); err != nil {
			return err
		}
		return a.write(ev.RawData)
	case replication.GTID_EVENT:
		r, err := gtidOf(ev.RawData)
		if err != nil {
			return err
		}
		a.pending = append(a.pending, r)
	case replication.GTID_TAGGED_LOG_EVENT:
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			"tagged GTIDs (MySQL 8.4 gtid tags) are not supported by strand", nil)
	}
	return a.write(ev.RawData)
}

func (a *Archiver) open() error {
	if a.curName == "" {
		return fmt.Errorf("format description event before any rotate: binlog file name unknown")
	}
	a.seq, a.fileName = a.nextSeq, filepath.Base(a.curName)
	a.partial = filepath.Join(a.cfg.Dir, fmt.Sprintf("%s%08d-%s", partialPref, a.seq, a.fileName))
	f, err := os.OpenFile(a.partial, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	a.f, a.w, a.pending = f, bufio.NewWriterSize(f, 1<<20), nil
	_, err = a.w.Write(replication.BinLogFileHeader)
	return err
}

func (a *Archiver) write(raw []byte) error {
	if a.w == nil {
		return fmt.Errorf("binlog event before the format description event of the file")
	}
	_, err := a.w.Write(raw)
	return err
}

// finalize: flush + fsync, đổi sang tên chốt, fsync thư mục, rồi ghi trạng thái.
func (a *Archiver) finalize() error {
	if a.f == nil {
		return nil
	}
	if err := a.w.Flush(); err != nil {
		return err
	}
	if err := a.f.Sync(); err != nil {
		return err
	}
	if err := a.f.Close(); err != nil {
		return err
	}
	final := filepath.Join(a.cfg.Dir, fmt.Sprintf("%08d-%s", a.seq, a.fileName))
	if err := os.Rename(a.partial, final); err != nil {
		return err
	}
	if err := syncDir(a.cfg.Dir); err != nil {
		return err
	}
	for _, r := range a.pending {
		a.executed.AddGTID(r.sid, r.gno)
	}
	n := len(a.pending)
	a.f, a.w, a.partial, a.pending = nil, nil, "", nil
	a.nextSeq = a.seq + 1
	if err := a.saveState(a.seq); err != nil {
		return err
	}
	a.log.Info("archive: file finalized", "file", filepath.Base(final), "transactions", n)
	return nil
}

func (a *Archiver) discardPartial() {
	if a.f != nil {
		_ = a.f.Close()
		_ = os.Remove(a.partial)
	}
	a.f, a.w, a.partial, a.pending = nil, nil, "", nil
}

// gtidOf đọc SID và GNO từ event GTID thô: header 19 byte, rồi flags (1), SID (16), GNO (8, LE).
func gtidOf(raw []byte) (gtidRef, error) {
	const off = replication.EventHeaderSize
	if len(raw) < off+25 {
		return gtidRef{}, fmt.Errorf("short GTID event (%d bytes)", len(raw))
	}
	sid, err := uuid.FromBytes(raw[off+1 : off+17])
	if err != nil {
		return gtidRef{}, err
	}
	return gtidRef{sid: sid, gno: int64(binary.LittleEndian.Uint64(raw[off+17 : off+25]))}, nil
}

// scanFileGTIDs đọc GTID của mọi transaction trong một file binlog (duyệt theo event_size ở header).
func scanFileGTIDs(path string) ([]gtidRef, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	magic := make([]byte, len(replication.BinLogFileHeader))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != string(replication.BinLogFileHeader) {
		return nil, fmt.Errorf("not a binlog file")
	}
	var out []gtidRef
	hdr := make([]byte, replication.EventHeaderSize)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, fmt.Errorf("truncated event header: %w", err)
		}
		size := binary.LittleEndian.Uint32(hdr[9:13])
		if size < uint32(replication.EventHeaderSize) {
			return nil, fmt.Errorf("bad event size %d", size)
		}
		body := make([]byte, size-uint32(replication.EventHeaderSize))
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, fmt.Errorf("truncated event: %w", err)
		}
		switch replication.EventType(hdr[4]) {
		case replication.GTID_EVENT:
			g, err := gtidOf(append(append([]byte{}, hdr...), body...))
			if err != nil {
				return nil, err
			}
			out = append(out, g)
		case replication.GTID_TAGGED_LOG_EVENT:
			return nil, fmt.Errorf("tagged GTIDs are not supported")
		}
	}
}

func isPurgedErr(err error) bool {
	var me *gomysql.MyError
	if errors.As(err, &me) {
		// ER_SOURCE_FATAL_ERROR_READING_BINLOG: binlog cần đã bị purge (hoặc GTID không còn).
		return me.Code == 1236
	}
	return false
}

func writeFileSync(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
