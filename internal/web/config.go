// Package web là Web UI của Strand (`strand web`): server HTTP chạy cùng máy với các pipeline,
// đọc trạng thái qua admin socket, /metrics, systemctl/journalctl hoặc file pid/log của chế độ nền.
// Quyết định: questions/DECISIONS-WEB-UI-2026-11-15.md. API: docs/WEB-API.md.
package web

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// PipelineEntry khai báo một pipeline trong web.yaml (mục `pipelines`).
type PipelineEntry struct {
	Name string `yaml:"name" json:"name"`
	// Config: đường dẫn file cấu hình pipeline (YAML của `strand sync -c`).
	Config string `yaml:"config" json:"config"`
	// EnvFile: file KEY=VALUE nạp vào môi trường tiến trình con (chế độ nền); tùy chọn.
	EnvFile string `yaml:"env_file,omitempty" json:"env_file,omitempty"`
	// SystemdUnit: có thì dùng systemctl/journalctl; không có thì dùng `strand sync --background`.
	SystemdUnit string `yaml:"systemd_unit,omitempty" json:"systemd_unit,omitempty"`
	// LogFile, PidFile: ghi đè đường dẫn mặc định của chế độ nền (<resources.spool.path>/strand.log|pid).
	LogFile string `yaml:"log_file,omitempty" json:"log_file,omitempty"`
	PidFile string `yaml:"pid_file,omitempty" json:"pid_file,omitempty"`
}

// Config là web.yaml.
type Config struct {
	// Listen: địa chỉ bind. Không phải loopback thì bắt buộc TLS.
	Listen  string `yaml:"listen"`
	TLSCert string `yaml:"tls_cert,omitempty"`
	TLSKey  string `yaml:"tls_key,omitempty"`
	// PasswordHash: bcrypt của mật khẩu admin (đặt bằng `strand web passwd`). Rỗng = không khởi động.
	PasswordHash string `yaml:"password_hash,omitempty"`
	// APITokenHash: sha256 hex của token tĩnh cho script (tùy chọn; tạo bằng `strand web token`).
	APITokenHash string `yaml:"api_token_hash,omitempty"`
	// StateDir: thư mục ghi audit log (và trạng thái web về sau).
	StateDir string `yaml:"state_dir,omitempty"`
	// ConfigDir: thư mục lưu cấu hình pipeline mới (M5). Mặc định /etc/strand nếu ghi được, nếu không thì state_dir/pipelines.
	ConfigDir string `yaml:"config_dir,omitempty"`
	// Scan: glob các file cấu hình pipeline, ví dụ /etc/strand/*.yaml.
	Scan      []string        `yaml:"scan,omitempty"`
	Pipelines []PipelineEntry `yaml:"pipelines,omitempty"`
	// StrandBinary: đường dẫn binary strand cho thao tác chạy tiến trình con (M3+); mặc định chính binary này.
	StrandBinary string `yaml:"strand_binary,omitempty"`
	// SecureCookies: ép cờ Secure cho cookie khi chạy sau reverse proxy TLS (listen loopback, không TLS).
	SecureCookies bool `yaml:"secure_cookies,omitempty"`
	// TrustForwardedFor: lấy IP client từ X-Forwarded-For (chỉ bật khi sau reverse proxy tin cậy).
	TrustForwardedFor bool `yaml:"trust_forwarded_for,omitempty"`
	// SampleInterval, tính bằng giây (mặc định 5); chủ yếu cho test.
	SampleIntervalSec int `yaml:"sample_interval_sec,omitempty"`

	// SourcePath: đường dẫn tuyệt đối của chính web.yaml (không đọc từ file); scan bỏ qua file này.
	SourcePath string `yaml:"-"`
}

// DefaultStateDir là thư mục trạng thái mặc định của `strand web`.
const DefaultStateDir = "/var/lib/strand-web"

// NameRE là whitelist tên pipeline: dùng làm tên unit, tên file, tham số dòng lệnh, nên phải chặt.
var NameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

// ValidName báo name có hợp lệ theo whitelist (và không chứa ".." để khỏi nhầm với đường dẫn).
func ValidName(name string) bool {
	return NameRE.MatchString(name) && !strings.Contains(name, "..")
}

// LoadConfig đọc web.yaml (strict: khóa lạ là lỗi).
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read web config: %w", err)
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse web config %s: %w", path, err)
	}
	if abs, err := filepath.Abs(path); err == nil {
		c.SourcePath = abs
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8080"
	}
	if c.StateDir == "" {
		c.StateDir = DefaultStateDir
	}
	return &c, nil
}

// Validate kiểm các điều kiện bắt buộc để khởi động: có mật khẩu, TLS khi không loopback,
// tên pipeline hợp lệ và không trùng.
func (c *Config) Validate() error {
	if c.PasswordHash == "" {
		return errors.New("no admin password set: run `strand web passwd --config <web.yaml>` first")
	}
	if _, err := bcrypt.Cost([]byte(c.PasswordHash)); err != nil {
		return fmt.Errorf("password_hash is not a valid bcrypt hash: %w", err)
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return errors.New("tls_cert and tls_key must be set together")
	}
	if c.TLSCert == "" && !IsLoopbackAddr(c.Listen) {
		return fmt.Errorf("listen %q is not a loopback address: TLS (tls_cert, tls_key) is required", c.Listen)
	}
	seen := map[string]bool{}
	for _, p := range c.Pipelines {
		if !ValidName(p.Name) {
			return fmt.Errorf("pipelines: invalid name %q (allowed: letters, digits, _ . - ; up to 63 chars)", p.Name)
		}
		if seen[p.Name] {
			return fmt.Errorf("pipelines: duplicate name %q", p.Name)
		}
		seen[p.Name] = true
		if p.Config == "" {
			return fmt.Errorf("pipelines[%s]: config is required", p.Name)
		}
		if p.SystemdUnit != "" && !validUnit(p.SystemdUnit) {
			return fmt.Errorf("pipelines[%s]: invalid systemd_unit %q", p.Name, p.SystemdUnit)
		}
	}
	return nil
}

var unitRE = regexp.MustCompile(`^[A-Za-z0-9:_.@\\-]{1,128}(\.service)?$`)

func validUnit(u string) bool { return unitRE.MatchString(u) && !strings.HasPrefix(u, "-") }

// IsLoopbackAddr: host của addr là localhost hoặc IP loopback. Host rỗng (":8080") = mọi interface.
func IsLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// HashPassword băm mật khẩu bằng bcrypt.
func HashPassword(pw string) (string, error) {
	if len(pw) < 8 {
		return "", errors.New("password must be at least 8 characters")
	}
	if len(pw) > 72 {
		return "", errors.New("password must be at most 72 bytes (bcrypt limit)")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(h), err
}

// SetConfigKey ghi (hoặc đổi) một khóa cấp cao của web.yaml mà giữ nguyên chú thích và các khóa khác,
// rồi thay file nguyên tử với quyền 0600. File chưa có thì tạo.
func SetConfigKey(path, key, value string) error {
	var doc yaml.Node
	b, err := os.ReadFile(path)
	switch {
	case err == nil && len(bytes.TrimSpace(b)) > 0:
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
	case err == nil || errors.Is(err, os.ErrNotExist):
	default:
		return err
	}
	if doc.Kind == 0 {
		doc = yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode}}}
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("%s: top level must be a mapping", path)
	}
	found := false
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == key {
			root.Content[i+1] = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value, Style: yaml.DoubleQuotedStyle}
			found = true
		}
	}
	if !found {
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value, Style: yaml.DoubleQuotedStyle})
	}
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return err
	}
	_ = enc.Close()
	return writeFilePreserving(path, out.Bytes(), true, true)
}
