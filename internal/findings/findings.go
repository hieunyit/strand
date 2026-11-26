// Package findings gom kết quả preflight thành danh sách vấn đề (strand doctor): mỗi vấn đề có mức độ, mã
// ổn định, đối tượng liên quan, thông điệp một dòng và cách sửa cụ thể. Các kiểm tra dùng chung với
// validate/sync nhận một *Collector: nil giữ hành vi dừng ở lỗi đầu tiên, khác nil thì ghi vấn đề và đi tiếp.
package findings

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"strand/internal/exitcode"
)

// Severity là mức độ của một vấn đề.
type Severity string

const (
	// Error: sync sẽ từ chối chạy (hoặc chắc chắn hỏng) cho tới khi sửa.
	Error Severity = "error"
	// Warning: sync chạy được nhưng có rủi ro hoặc chậm.
	Warning Severity = "warning"
	// Info: thông tin cho người vận hành, không cần sửa.
	Info Severity = "info"
)

func (s Severity) rank() int {
	switch s {
	case Error:
		return 0
	case Warning:
		return 1
	}
	return 2
}

// Finding là một vấn đề doctor tìm thấy.
type Finding struct {
	Severity Severity `json:"severity"`
	// Code: mã snake_case ổn định (vd. source_binlog_format, target_fk_missing).
	Code string `json:"code"`
	// Object: đối tượng liên quan: "source", "target", "schema.table" hoặc "schema.table.constraint".
	Object  string `json:"object,omitempty"`
	Message string `json:"message"`
	// Fix: câu SQL hoặc thay đổi cấu hình cụ thể để sửa; rỗng khi không có cách sửa tự động.
	Fix string `json:"fix,omitempty"`
}

// Collector gom các Finding; an toàn khi dùng từ nhiều goroutine. Vấn đề trùng (cùng mã, đối tượng và
// thông điệp) chỉ giữ một lần. Mọi chuỗi bí mật đăng ký bằng AddSecret bị che trước khi lưu.
type Collector struct {
	mu      sync.Mutex
	items   []Finding
	seen    map[string]bool
	secrets []string
}

// AddSecret đăng ký một chuỗi (mật khẩu DSN...) không bao giờ được xuất hiện trong kết quả.
func (c *Collector) AddSecret(s string) {
	if c == nil || len(s) < 4 { // chuỗi quá ngắn sẽ che nhầm chữ thường
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secrets = append(c.secrets, s)
}

func (c *Collector) redact(s string) string {
	for _, sec := range c.secrets {
		s = strings.ReplaceAll(s, sec, "xxxxx")
	}
	return s
}

// Add ghi một vấn đề. Gọi trên Collector nil không làm gì.
func (c *Collector) Add(f Finding) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	f.Message = c.redact(strings.TrimSpace(f.Message))
	f.Fix = c.redact(strings.TrimSpace(f.Fix))
	f.Object = c.redact(f.Object)
	key := string(f.Severity) + "\x00" + f.Code + "\x00" + f.Object + "\x00" + f.Message
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	if c.seen[key] {
		return
	}
	c.seen[key] = true
	c.items = append(c.items, f)
}

// Errorf, Warn, Info: viết tắt của Add.
func (c *Collector) Errorf(code, object, message, fix string) {
	c.Add(Finding{Severity: Error, Code: code, Object: object, Message: message, Fix: fix})
}

// Warn ghi một vấn đề mức warning.
func (c *Collector) Warn(code, object, message, fix string) {
	c.Add(Finding{Severity: Warning, Code: code, Object: object, Message: message, Fix: fix})
}

// Info ghi một thông tin.
func (c *Collector) Info(code, object, message, fix string) {
	c.Add(Finding{Severity: Info, Code: code, Object: object, Message: message, Fix: fix})
}

// Err ghi err thành vấn đề mức sev: lấy Message của *exitcode.Error (không kèm tiền tố "[mã]").
func (c *Collector) Err(sev Severity, code, object string, err error, fix string) {
	if err == nil {
		return
	}
	c.Add(Finding{Severity: sev, Code: code, Object: object, Message: Message(err), Fix: fix})
}

// Message trả thông điệp của err: Message (và lỗi bọc trong) của *exitcode.Error, hoặc err.Error().
func Message(err error) string {
	var e *exitcode.Error
	if errors.As(err, &e) && e.Message != "" {
		if e.Err != nil {
			return e.Message + ": " + e.Err.Error()
		}
		return e.Message
	}
	return err.Error()
}

// Findings trả bản sao các vấn đề, xếp theo mức độ (error, warning, info), giữ thứ tự phát hiện trong cùng mức.
func (c *Collector) Findings() []Finding {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	out := append([]Finding(nil), c.items...)
	c.mu.Unlock()
	sort.SliceStable(out, func(i, j int) bool { return out[i].Severity.rank() < out[j].Severity.rank() })
	return out
}

// Count đếm số vấn đề ở mức sev.
func (c *Collector) Count(sev Severity) int {
	n := 0
	for _, f := range c.Findings() {
		if f.Severity == sev {
			n++
		}
	}
	return n
}

// Has: đã có vấn đề mã code về object (để một kiểm tra phụ không báo lại điều kiểm tra chính đã báo).
func (c *Collector) Has(code, object string) bool {
	for _, f := range c.Findings() {
		if f.Code == code && f.Object == object {
			return true
		}
	}
	return false
}

// HasErrors: có ít nhất một vấn đề mức error.
func (c *Collector) HasErrors() bool { return c.Count(Error) > 0 }

// Codes trả tập mã đã ghi (dùng cho test và tóm tắt).
func (c *Collector) Codes() map[string]bool {
	out := map[string]bool{}
	for _, f := range c.Findings() {
		out[f.Code] = true
	}
	return out
}
