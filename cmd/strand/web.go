package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"strand/internal/exitcode"
	"strand/internal/web"
)

// newWebCmd: `strand web --config web.yaml` (Web UI, docs/PLAN-WEB-UI.md) và các lệnh con passwd, token.
// Cờ -c/--config là cờ chung của root: ở đây nó trỏ tới web.yaml, không phải file cấu hình pipeline.
func newWebCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "web",
		Short: "Web UI: xem trạng thái các pipeline (strand web --config web.yaml)",
		Long: `Chạy Web UI trên cùng máy với các pipeline, dưới cùng user chạy strand (không cần root).
Chỉ quản lý được pipeline mà user này có quyền: đọc file cấu hình, kết nối admin socket, đọc log.
Chưa đặt mật khẩu (strand web passwd) thì không khởi động.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing web config path (use --config web.yaml)", nil)
			}
			wc, err := web.LoadConfig(cfgPath)
			if err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}
			srv, err := web.New(web.Options{Config: wc, Version: versionString()})
			if err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			scheme := "http"
			if wc.TLSCert != "" {
				scheme = "https"
			}
			if wc.PlainHTTPExposed() {
				fmt.Fprintln(cmd.ErrOrStderr(), web.PlainHTTPWarning)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "strand web listening on %s://%s (audit log: %s)\n", scheme, wc.Listen, srv.Audit().Path())
			return srv.ListenAndServe(ctx)
		},
	}
	cmd.AddCommand(newWebPasswdCmd(), newWebTokenCmd(), newWebInitCmd())
	return cmd
}

func newWebPasswdCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "passwd",
		Short: "Đặt mật khẩu admin của Web UI (bcrypt hash vào web.yaml, quyền 0600)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing web config path (use --config web.yaml)", nil)
			}
			pw, err := readNewPassword(cmd)
			if err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}
			h, err := web.HashPassword(pw)
			if err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}
			if err := web.SetConfigKey(cfgPath, "password_hash", h); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "password hash written to %s\n", cfgPath)
			return nil
		},
	}
}

func newWebTokenCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "token",
		Short: "Sinh token API tĩnh cho script (in ra một lần, ghi sha256 vào web.yaml)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing web config path (use --config web.yaml)", nil)
			}
			tok := web.NewAPIToken()
			if err := web.SetConfigKey(cfgPath, "api_token_hash", web.HashAPIToken(tok)); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s\n", tok)
			fmt.Fprintf(cmd.ErrOrStderr(), "token printed once; only its sha256 is stored in %s. Use: Authorization: Bearer <token>\n", cfgPath)
			return nil
		},
	}
}

// readNewPassword: terminal thì hỏi hai lần (không echo); stdin không phải terminal (script) thì đọc một dòng.
func readNewPassword(cmd *cobra.Command) (string, error) {
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		fmt.Fprint(cmd.ErrOrStderr(), "New admin password: ")
		a, err := term.ReadPassword(fd)
		fmt.Fprintln(cmd.ErrOrStderr())
		if err != nil {
			return "", err
		}
		fmt.Fprint(cmd.ErrOrStderr(), "Repeat password: ")
		b, err := term.ReadPassword(fd)
		fmt.Fprintln(cmd.ErrOrStderr())
		if err != nil {
			return "", err
		}
		if string(a) != string(b) {
			return "", fmt.Errorf("passwords do not match")
		}
		return string(a), nil
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("read password from stdin: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// newWebInitCmd: `strand web init`: tạo web.yaml, đặt mật khẩu (và token), tuỳ chọn bật dịch vụ, trong một lệnh.
func newWebInitCmd() *cobra.Command {
	var (
		o        web.InitOptions
		stdinPw  bool
		genPw    bool
		tlsCert  string
		tlsKey   string
		scan     []string
		wantEnbl bool
	)
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Cài nhanh Web UI: tạo web.yaml, đặt mật khẩu/token, tuỳ chọn bật dịch vụ (một lệnh)",
		Long: `Tạo /etc/strand/web.yaml (nếu chưa có), đặt mật khẩu admin, tuỳ chọn tạo token API và bật strand-web.

Mật khẩu: hỏi hai lần khi stdin là terminal; --password-stdin đọc một dòng; --generate-password sinh mật khẩu
ngẫu nhiên và in MỘT lần ra stdout. Chạy lại an toàn: web.yaml có sẵn giữ nguyên listen/state_dir/scan
(thêm --force để ghi đè), chỉ mật khẩu/token được đặt lại; không đưa mật khẩu thì giữ hash cũ.
Chạy bằng root và có group strand thì file là root:strand 0640 và state_dir thuộc strand:strand 0750.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			o.ConfigPath = cfgPath
			if o.ConfigPath == "" {
				o.ConfigPath = "/etc/strand/web.yaml"
			}
			o.Scan = scan
			if len(o.Scan) == 0 {
				o.Scan = web.DefaultInitScan
			}
			o.TLSCert, o.TLSKey = tlsCert, tlsKey
			o.Enable = wantEnbl
			o.Stdout, o.Stderr = cmd.OutOrStdout(), cmd.ErrOrStderr()
			fail := func(err error) error {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}
			if stdinPw && genPw {
				return fail(fmt.Errorf("--password-stdin and --generate-password are mutually exclusive"))
			}
			// Validate trước khi hỏi mật khẩu hay ghi gì (file có sẵn và không --force: validate trong RunInit).
			if _, err := os.Stat(o.ConfigPath); err != nil || o.Force {
				if err := web.ValidateInit(o); err != nil {
					return fail(err)
				}
			}
			switch {
			case genPw:
				pw, err := web.GeneratePassword()
				if err != nil {
					return fail(err)
				}
				o.Password, o.PrintPassword = pw, true
			case stdinPw:
				line, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if err != nil && line == "" {
					return fail(fmt.Errorf("read password from stdin: %w", err))
				}
				o.Password = strings.TrimRight(line, "\r\n")
			case term.IsTerminal(int(os.Stdin.Fd())):
				// Đã có mật khẩu thì không hỏi lại (chạy lại phải idempotent); đổi bằng `strand web passwd`.
				if c, err := web.LoadConfig(o.ConfigPath); err != nil || c.PasswordHash == "" {
					pw, err := readNewPassword(cmd)
					if err != nil {
						return fail(err)
					}
					o.Password = pw
				}
			}
			if _, err := web.RunInit(o); err != nil {
				return fail(err)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.Listen, "listen", "127.0.0.1:8080", "địa chỉ bind của Web UI, ví dụ 0.0.0.0:8080 (không phải loopback thì cần --tls-cert/--tls-key hoặc --allow-plain-http)")
	f.StringVar(&o.StateDir, "state-dir", web.DefaultStateDir, "thư mục audit log và trạng thái web")
	f.StringArrayVar(&scan, "scan", nil, "glob file pipeline cần quét (lặp lại được; mặc định /etc/strand/*.yaml)")
	f.StringVar(&tlsCert, "tls-cert", "", "file chứng chỉ TLS")
	f.StringVar(&tlsKey, "tls-key", "", "file khóa TLS")
	f.BoolVar(&o.SecureCookies, "secure-cookies", false, "ép cờ Secure cho cookie (sau reverse proxy TLS)")
	f.BoolVar(&o.TrustForwardedFor, "trust-forwarded-for", false, "lấy IP client từ X-Forwarded-For (chỉ khi proxy tin cậy)")
	f.BoolVar(&o.AllowPlainHTTP, "allow-plain-http", false, "cho phép nghe ngoài loopback (0.0.0.0) bằng HTTP thường, không TLS: mật khẩu/cookie không mã hoá, chỉ dùng trong mạng tin cậy")
	f.BoolVar(&stdinPw, "password-stdin", false, "đọc mật khẩu admin (một dòng) từ stdin")
	f.BoolVar(&genPw, "generate-password", false, "sinh mật khẩu ngẫu nhiên mạnh và in MỘT lần ra stdout")
	f.BoolVar(&o.NewToken, "token", false, "tạo thêm token API (in một lần, lưu sha256)")
	f.BoolVar(&wantEnbl, "enable", false, "chạy `systemctl enable --now strand-web` rồi chờ /login trả 200 (tối đa 30 giây)")
	f.BoolVar(&o.Force, "force", false, "ghi đè listen/state_dir/scan của web.yaml có sẵn")
	return cmd
}
