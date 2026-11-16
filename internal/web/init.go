package web

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"strand/internal/web/store"
)

// Seams cho test: `strand web init` không được gọi systemctl/mạng/chown thật trong unit test.
var (
	initIsRoot = func() bool { return os.Geteuid() == 0 }
	// initLookupStrand trả uid, gid của user/group `strand`; ok=false nếu không có group.
	initLookupStrand = func() (uid, gid int, ok bool) {
		g, err := user.LookupGroup("strand")
		if err != nil {
			return 0, 0, false
		}
		gid, _ = strconv.Atoi(g.Gid)
		if u, err := user.Lookup("strand"); err == nil {
			uid, _ = strconv.Atoi(u.Uid)
		}
		return uid, gid, true
	}
)

// CmdRunner chạy một lệnh ngoài (systemctl). Trả lỗi nếu exit khác 0.
type CmdRunner func(name string, args ...string) error

func execRunner(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// InitOptions là đầu vào của `strand web init`.
type InitOptions struct {
	ConfigPath string
	Listen     string
	StateDir   string
	Scan       []string
	TLSCert    string
	TLSKey     string
	// SecureCookies, TrustForwardedFor: chỉ ghi khi true.
	SecureCookies     bool
	TrustForwardedFor bool
	// AllowPlainHTTP: ghi allow_plain_http: true (bind ngoài loopback không TLS).
	AllowPlainHTTP bool
	Force          bool

	// Password: mật khẩu đã chọn (rỗng = giữ hash hiện có; không có hash thì lỗi).
	Password string
	// PrintPassword: in Password ra stdout (mật khẩu do máy sinh, hiện một lần).
	PrintPassword bool
	// NewToken: tạo (hoặc đổi) token API và in ra một lần.
	NewToken bool
	// Store: có thì mật khẩu/token ghi vào PostgreSQL thay vì web.yaml (chế độ DB).
	Store store.Store
	// IfNoAdmin: chỉ đặt mật khẩu/token khi kho chưa có admin (bootstrap lần đầu, chạy lại an toàn).
	IfNoAdmin bool
	// Quiet: bỏ phần tóm tắt "next steps" (dành cho container).
	Quiet bool

	Enable bool
	// Runner mặc định chạy lệnh thật; Poll mặc định GET url tới khi 200.
	Runner CmdRunner
	Poll   func(url string, timeout time.Duration) error

	Stdout io.Writer // bí mật in một lần + tóm tắt; không bao giờ có hash
	Stderr io.Writer
}

// InitResult mô tả kết quả (cho test).
type InitResult struct {
	Created      bool
	KeptSettings bool
	URL          string
	Token        string
}

// DefaultInitScan là glob quét pipeline mặc định.
var DefaultInitScan = []string{"/etc/strand/*.yaml"}

// GeneratePassword sinh mật khẩu ngẫu nhiên mạnh (24 ký tự, bảng chữ cái không có ký tự dễ nhầm).
func GeneratePassword() (string, error) {
	const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
	b := make([]byte, 24)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", err
		}
		b[i] = alphabet[n.Int64()]
	}
	return string(b), nil
}

const initTemplate = `# web.yaml: cấu hình Web UI (strand web --config %[1]s). Tạo bởi ` + "`strand web init`" + `.
# Quyền file: root:strand 0640 khi tạo bằng root (user strand của strand-web.service phải đọc được), không thì 0600.
# Mật khẩu: strand web passwd --config %[1]s   Token script: strand web token --config %[1]s

# Địa chỉ bind. Không phải loopback (127.0.0.1, ::1, localhost) thì phải đặt tls_cert và tls_key,
# hoặc allow_plain_http: true (HTTP thường: mật khẩu/cookie không mã hoá, chỉ dùng trong mạng tin cậy).
listen: %[2]s
%[3]s
%[7]s
# Sau reverse proxy TLS (listen loopback, không TLS): secure_cookies cho cookie cờ Secure;
# trust_forwarded_for lấy IP client từ X-Forwarded-For (chỉ khi proxy tin cậy).
%[4]s
# Thư mục ghi audit.log và trạng thái web.
state_dir: %[5]s

# Pipeline: quét các file cấu hình theo glob (tên pipeline = tên file bỏ .yaml).
scan:
%[6]s
# password_hash và api_token_hash do strand web init/passwd/token ghi vào cuối file.
`

func renderInitTemplate(o InitOptions) string {
	tlsBlock := "# tls_cert: /etc/strand/web.crt\n# tls_key: /etc/strand/web.key"
	if o.TLSCert != "" {
		tlsBlock = "tls_cert: " + yamlQuote(o.TLSCert) + "\ntls_key: " + yamlQuote(o.TLSKey)
	}
	flags := []string{"# secure_cookies: true", "# trust_forwarded_for: true"}
	if o.SecureCookies {
		flags[0] = "secure_cookies: true"
	}
	if o.TrustForwardedFor {
		flags[1] = "trust_forwarded_for: true"
	}
	var scan []string
	for _, s := range o.Scan {
		scan = append(scan, "  - "+yamlQuote(s))
	}
	plain := "# allow_plain_http: true"
	if o.AllowPlainHTTP {
		plain = "allow_plain_http: true"
	}
	return fmt.Sprintf(initTemplate, o.ConfigPath, yamlQuote(o.Listen), tlsBlock, strings.Join(flags, "\n"), yamlQuote(o.StateDir), strings.Join(scan, "\n"), plain)
}

func yamlQuote(s string) string {
	b, _ := yaml.Marshal(s)
	return strings.TrimRight(string(b), "\n")
}

// ValidateInit kiểm đầu vào y như `strand web` sẽ kiểm khi khởi động (trừ mật khẩu). Không ghi gì.
func ValidateInit(o InitOptions) error {
	c := &Config{Listen: o.Listen, TLSCert: o.TLSCert, TLSKey: o.TLSKey, AllowPlainHTTP: o.AllowPlainHTTP}
	return c.ValidateListen()
}

// RunInit thực hiện `strand web init`. Thứ tự: validate (không ghi gì nếu lỗi) -> tạo/giữ web.yaml ->
// ghi hash mật khẩu/token -> tạo state_dir -> (tuỳ chọn) bật dịch vụ -> tóm tắt.
func RunInit(o InitOptions) (*InitResult, error) {
	if o.Stdout == nil {
		o.Stdout = io.Discard
	}
	if o.Stderr == nil {
		o.Stderr = io.Discard
	}
	if o.Runner == nil {
		o.Runner = execRunner
	}
	if o.Poll == nil {
		o.Poll = pollLogin
	}
	if o.ConfigPath == "" {
		o.ConfigPath = "/etc/strand/web.yaml"
	}
	if abs, err := filepath.Abs(o.ConfigPath); err == nil {
		o.ConfigPath = abs
	}
	res := &InitResult{}

	_, statErr := os.Stat(o.ConfigPath)
	exists := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}

	// Cấu hình hiệu lực: file có sẵn (không --force) hoặc đầu vào.
	eff := o
	var existing *Config
	if exists {
		c, err := LoadConfig(o.ConfigPath)
		if err != nil {
			return nil, err
		}
		existing = c
		if !o.Force {
			res.KeptSettings = true
			eff.Listen, eff.StateDir, eff.TLSCert, eff.TLSKey = c.Listen, c.StateDir, c.TLSCert, c.TLSKey
		}
	}
	if err := ValidateInit(eff); err != nil {
		return nil, err
	}
	haveAdmin := existing != nil && existing.PasswordHash != ""
	if o.Store != nil {
		adm, ok, err := o.Store.Admin(context.Background())
		if err != nil {
			return nil, err
		}
		haveAdmin = ok && adm.PasswordHash != ""
		if o.IfNoAdmin && haveAdmin {
			// Admin đã có: không đổi mật khẩu/token, không in mật khẩu sinh ra.
			o.Password, o.PrintPassword, o.NewToken = "", false, false
			fmt.Fprintln(o.Stderr, "admin account already exists in the database: keeping it")
		}
	}
	if o.Password == "" && !haveAdmin {
		return nil, errors.New("no password: use --password-stdin or --generate-password (or run interactively on a terminal)")
	}
	var hash string
	if o.Password != "" {
		h, err := HashPassword(o.Password) // sai độ dài thì lỗi trước khi ghi gì
		if err != nil {
			return nil, err
		}
		hash = h
	}

	shared := false
	var suid, sgid int
	if initIsRoot() {
		var ok bool
		suid, sgid, ok = initLookupStrand()
		shared = ok
	}

	switch {
	case !exists:
		if err := os.MkdirAll(filepath.Dir(o.ConfigPath), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(o.ConfigPath, []byte(renderInitTemplate(o)), 0o600); err != nil {
			return nil, err
		}
		if shared {
			if err := chownFunc(o.ConfigPath, 0, sgid); err != nil {
				return nil, fmt.Errorf("chown %s: %w", o.ConfigPath, err)
			}
			if err := os.Chmod(o.ConfigPath, 0o640); err != nil {
				return nil, err
			}
		}
		res.Created = true
		fmt.Fprintf(o.Stderr, "created %s\n", o.ConfigPath)
	case o.Force:
		kvs := []configKV{strKV("listen", o.Listen), strKV("state_dir", o.StateDir), {"scan", seqNode(o.Scan)}}
		kvs = append(kvs, optKV("tls_cert", o.TLSCert), optKV("tls_key", o.TLSKey))
		if o.SecureCookies {
			kvs = append(kvs, configKV{"secure_cookies", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"}})
		}
		if o.TrustForwardedFor {
			kvs = append(kvs, configKV{"trust_forwarded_for", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"}})
		}
		if o.AllowPlainHTTP {
			kvs = append(kvs, configKV{"allow_plain_http", &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"}})
		}
		if err := setConfigNodes(o.ConfigPath, dropNil(kvs)); err != nil {
			return nil, err
		}
		fmt.Fprintf(o.Stderr, "updated listen/state_dir/scan in %s (--force)\n", o.ConfigPath)
	default:
		fmt.Fprintf(o.Stderr, "%s exists: keeping its listen/state_dir/scan/TLS settings (use --force to overwrite); only the password/token you asked for are (re)set\n", o.ConfigPath)
	}

	if hash != "" {
		if o.Store != nil {
			if err := SetStorePassword(context.Background(), o.Store, hash); err != nil {
				return nil, err
			}
		} else if err := SetConfigKey(o.ConfigPath, "password_hash", hash); err != nil {
			return nil, err
		}
	}
	if o.NewToken {
		res.Token = NewAPIToken()
		if o.Store != nil {
			if err := o.Store.SetAPITokenHash(context.Background(), HashAPIToken(res.Token)); err != nil {
				return nil, err
			}
		} else if err := SetConfigKey(o.ConfigPath, "api_token_hash", HashAPIToken(res.Token)); err != nil {
			return nil, err
		}
	}

	if _, err := os.Stat(eff.StateDir); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(eff.StateDir, 0o750); err != nil {
			fmt.Fprintf(o.Stderr, "warning: cannot create state_dir %s: %v (strand-web's StateDirectory will create it)\n", eff.StateDir, err)
		} else {
			_ = os.Chmod(eff.StateDir, 0o750)
			if shared {
				if err := chownFunc(eff.StateDir, suid, sgid); err != nil {
					fmt.Fprintf(o.Stderr, "warning: chown %s: %v\n", eff.StateDir, err)
				}
			}
		}
	}

	scheme := "http"
	if eff.TLSCert != "" {
		scheme = "https"
	}
	res.URL = scheme + "://" + eff.Listen
	probe := scheme + "://" + probeHost(eff.Listen)

	if o.PrintPassword {
		fmt.Fprintf(o.Stdout, "GENERATED ADMIN PASSWORD (shown once, store it now): %s\n", o.Password)
	}
	if res.Token != "" {
		fmt.Fprintf(o.Stdout, "API TOKEN (shown once; use: Authorization: Bearer <token>): %s\n", res.Token)
	}

	if o.Enable {
		wasActive := o.Runner("systemctl", "is-active", "--quiet", "strand-web") == nil
		if err := o.Runner("systemctl", "enable", "--now", "strand-web"); err != nil {
			return res, err
		}
		if wasActive { // đã chạy: nạp lại mật khẩu/cấu hình mới
			if err := o.Runner("systemctl", "restart", "strand-web"); err != nil {
				return res, err
			}
		}
		if err := o.Poll(probe+"/login", 30*time.Second); err != nil {
			return res, fmt.Errorf("strand-web did not become ready: %w (see: journalctl -u strand-web -n 30)", err)
		}
		fmt.Fprintf(o.Stdout, "strand-web is up: %s/login\n", res.URL)
	}

	if o.Quiet {
		return res, nil
	}
	fmt.Fprintf(o.Stdout, "\nSummary\n  config: %s\n  URL:    %s\n  login:  with the admin password set above\n  next:\n", o.ConfigPath, res.URL)
	if !o.Enable {
		fmt.Fprintf(o.Stdout, "    - start the service: sudo systemctl enable --now strand-web   (or re-run with --enable)\n")
	}
	if IsLoopbackAddr(eff.Listen) {
		fmt.Fprintf(o.Stdout, "    - remote access: ssh -L %s:%s user@this-host, then open %s\n", portOf(eff.Listen), eff.Listen, res.URL)
	} else if probeHost(eff.Listen) != eff.Listen {
		fmt.Fprintf(o.Stdout, "    - open from another machine: %s://<this-server-ip>:%s (allow the port in the firewall)\n", scheme, portOf(eff.Listen))
	}
	if (&Config{Listen: eff.Listen, TLSCert: eff.TLSCert}).PlainHTTPExposed() {
		fmt.Fprintln(o.Stderr, PlainHTTPWarning)
	}
	fmt.Fprintf(o.Stdout, "    - change password later: strand web passwd --config %s\n", o.ConfigPath)
	return res, nil
}

func strKV(k, v string) configKV {
	return configKV{k, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v, Style: yaml.DoubleQuotedStyle}}
}

// optKV: giá trị rỗng thì val=nil để dropNil bỏ qua (giữ khóa cũ).
func optKV(k, v string) configKV {
	if v == "" {
		return configKV{k, nil}
	}
	return strKV(k, v)
}

func dropNil(in []configKV) []configKV {
	var out []configKV
	for _, kv := range in {
		if kv.val != nil {
			out = append(out, kv)
		}
	}
	return out
}

func seqNode(items []string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.SequenceNode}
	for _, s := range items {
		n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s, Style: yaml.DoubleQuotedStyle})
	}
	return n
}

func portOf(addr string) string {
	_, p, _ := net.SplitHostPort(addr)
	return p
}

// probeHost đổi địa chỉ bind thành địa chỉ gọi được (0.0.0.0, [::], rỗng -> 127.0.0.1).
func probeHost(listen string) string {
	h, p, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if ip := net.ParseIP(h); h == "" || (ip != nil && ip.IsUnspecified()) {
		h = "127.0.0.1"
	}
	return net.JoinHostPort(h, p)
}

// pollLogin gọi GET url tới khi 200 hoặc hết timeout (bỏ qua kiểm chứng cert: chỉ là thăm dò cục bộ).
func pollLogin(url string, timeout time.Duration) error {
	cl := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec
	deadline := time.Now().Add(timeout)
	var last error
	for {
		resp, err := cl.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
			last = fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
		} else {
			last = err
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(time.Second)
	}
}
