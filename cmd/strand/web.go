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
			fmt.Fprintf(cmd.OutOrStdout(), "strand web listening on %s://%s (audit log: %s)\n", scheme, wc.Listen, srv.Audit().Path())
			return srv.ListenAndServe(ctx)
		},
	}
	cmd.AddCommand(newWebPasswdCmd(), newWebTokenCmd())
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
