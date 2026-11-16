package web

import (
	"encoding/json"
	"fmt"
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
type AuditLog struct {
	mu   sync.Mutex
	path string
	f    *os.File
	now  func() time.Time
	// db: sink PostgreSQL (nil = chỉ file). mode: db | file | both.
	db   *metaDB
	mode string
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
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	return &AuditLog{path: p, f: f, now: now}, nil
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
		_, ferr := a.f.Write(append(b, '\n'))
		a.mu.Unlock()
		if ferr != nil && dbErr == nil {
			return ferr
		}
	}
	return dbErr
}

// Close đóng file.
func (a *AuditLog) Close() error { return a.f.Close() }
