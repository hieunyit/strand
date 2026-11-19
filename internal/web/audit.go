package web

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AuditEntry là một dòng của audit log (JSON lines).
type AuditEntry struct {
	Time     time.Time      `json:"time"`
	IP       string         `json:"ip"`
	Action   string         `json:"action"`
	Pipeline string         `json:"pipeline,omitempty"`
	Args     []string       `json:"args,omitempty"`
	Result   string         `json:"result"` // ok | denied | error
	ExitCode *int           `json:"exit_code,omitempty"`
	Detail   map[string]any `json:"detail,omitempty"`
}

// AuditLog ghi audit log dạng JSON lines, chỉ thêm vào cuối, quyền 0600.
// Mọi thao tác ghi của M3+ phải đi qua Log (xem actions.go).
//
// Xoay vòng file (chế độ file, và bản sao file ở chế độ DB audit=both|file): khi audit.log vượt
// audit_rotate_mb MiB (kiểm ở mỗi lần ghi) hoặc dòng đầu cũ hơn audit_rotate_days ngày (kiểm hằng ngày), file được
// đổi tên thành audit-<thời điểm>.log, nén thành .log.gz (fsync), kèm .log.gz.sha256; audit.log mới bắt đầu bằng
// dòng `audit.rotate` ghi tên và sha256 của file vừa nén (chuỗi băm nối các file: sửa/xóa một file cũ lộ ra
// ở file sau). File đã xoay không bao giờ bị Strand xóa.
type AuditLog struct {
	mu   sync.Mutex
	path string
	f    *os.File
	now  func() time.Time
	// db: sink PostgreSQL (nil = chỉ file). mode: db | file | both.
	db   *metaDB
	mode string

	size  int64     // kích thước hiện tại của audit.log
	first time.Time // thời điểm dòng đầu của audit.log (zero: rỗng / không đọc được)
	// rotateBytes: ngưỡng xoay theo kích thước (0 = tắt); Server gắn theo cài đặt audit_rotate_mb.
	rotateBytes func() int64
}

// OpenAuditLog mở (tạo nếu chưa có) <dir>/audit.log.
func OpenAuditLog(dir string, now func() time.Time) (*AuditLog, error) {
	if now == nil {
		now = time.Now
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	p := filepath.Join(dir, "audit.log")
	a := &AuditLog{path: p, now: now}
	if err := a.open(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *AuditLog) open() error {
	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open audit log: %w", err)
	}
	a.f, a.size, a.first = f, 0, time.Time{}
	if st, err := f.Stat(); err == nil {
		a.size = st.Size()
	}
	if a.size > 0 {
		a.first = firstAuditTime(a.path)
	}
	return nil
}

// firstAuditTime: "time" của dòng đầu audit.log.
func firstAuditTime(path string) time.Time {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}
	}
	defer f.Close()
	ln, _ := bufio.NewReaderSize(f, 64<<10).ReadBytes('\n')
	var e struct {
		Time time.Time `json:"time"`
	}
	_ = json.Unmarshal(ln, &e)
	return e.Time
}

// useDB gắn sink PostgreSQL. mode "" = both.
func (a *AuditLog) useDB(m *metaDB, mode string) {
	if mode == "" {
		mode = AuditBoth
	}
	a.db, a.mode = m, mode
}

// Path trả đường dẫn file audit.
func (a *AuditLog) Path() string { return a.path }

// Log ghi một dòng. Lỗi ghi được trả về để thao tác ghi có thể từ chối chạy khi audit hỏng.
func (a *AuditLog) Log(e AuditEntry) error {
	if e.Time.IsZero() {
		e.Time = a.now().UTC()
	}
	if e.Result == "" {
		e.Result = "ok"
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	// DB là nguồn chính khi có; lỗi DB trả về (thao tác ghi từ chối chạy khi audit hỏng), file vẫn được thử.
	var dbErr error
	if a.db != nil && a.mode != AuditFile {
		dbErr = a.db.appendAudit(e)
	}
	if a.db == nil || a.mode != AuditDB {
		a.mu.Lock()
		ferr := a.writeLocked(append(b, '\n'), e.Time)
		if ferr == nil && a.rotateBytes != nil {
			if max := a.rotateBytes(); max > 0 && a.size >= max {
				if rerr := a.rotateLocked("size"); rerr != nil {
					ferr = rerr
				}
			}
		}
		a.mu.Unlock()
		if ferr != nil && dbErr == nil {
			return ferr
		}
	}
	return dbErr
}

func (a *AuditLog) writeLocked(line []byte, t time.Time) error {
	n, err := a.f.Write(line)
	a.size += int64(n)
	if a.first.IsZero() {
		a.first = t
	}
	return err
}

// rotateIfOlder xoay audit.log nếu dòng đầu cũ hơn maxAge.
func (a *AuditLog) rotateIfOlder(maxAge time.Duration) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.db != nil && a.mode == AuditDB {
		return nil
	}
	if a.size == 0 || a.first.IsZero() || a.now().Sub(a.first) < maxAge {
		return nil
	}
	return a.rotateLocked("age")
}

// rotateLocked: audit.log -> audit-<ts>.log.gz (+ .sha256), audit.log mới bắt đầu bằng dòng audit.rotate.
func (a *AuditLog) rotateLocked(reason string) error {
	if a.size == 0 {
		return nil
	}
	dir := filepath.Dir(a.path)
	name := "audit-" + a.now().UTC().Format("20060102T150405.000000000Z") + ".log"
	raw := filepath.Join(dir, name)
	if err := a.f.Close(); err != nil {
		return fmt.Errorf("rotate audit log: %w", err)
	}
	size := a.size
	if err := os.Rename(a.path, raw); err != nil {
		_ = a.open()
		return fmt.Errorf("rotate audit log: %w", err)
	}
	if err := a.open(); err != nil {
		return err
	}
	sum, err := gzipFile(raw, raw+".gz")
	gz := name + ".gz"
	if err != nil {
		// Không nén được: giữ file chưa nén (vẫn là bản lưu), ghi lại lý do.
		gz, sum = name, ""
	} else {
		_ = writeFileSync(raw+".gz.sha256", []byte(sum+"  "+name+".gz\n"))
		_ = os.Remove(raw)
		syncDir(dir)
	}
	e := AuditEntry{Time: a.now().UTC(), Action: "audit.rotate", Result: "ok", Detail: map[string]any{
		"previous": gz, "sha256": sum, "bytes": size, "reason": reason}}
	if err != nil {
		e.Result, e.Detail["error"] = "error", err.Error()
	}
	b, _ := json.Marshal(e)
	return a.writeLocked(append(b, '\n'), e.Time)
}

// gzipFile nén src thành dst (tmp + fsync + rename), trả sha256 của dst.
func gzipFile(src, dst string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	zw := gzip.NewWriter(io.MultiWriter(out, h))
	if _, err = io.Copy(zw, in); err == nil {
		err = zw.Close()
	}
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, dst)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Close đóng file.
func (a *AuditLog) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Close()
}
