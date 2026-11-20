package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"strand/internal/exitcode"
	"strand/internal/web"
	"strand/internal/web/store"
)

var cliUsernameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)

func newWebUserCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Quản lý người dùng Web UI (add, list, disable, enable, set-role, reset-password)",
	}
	cmd.AddCommand(
		newWebUserAddCmd(),
		newWebUserListCmd(),
		newWebUserDisableCmd(),
		newWebUserEnableCmd(),
		newWebUserSetRoleCmd(),
		newWebUserResetPasswordCmd(),
	)
	return cmd
}

func getCLIStore(cmd *cobra.Command) (store.Store, error) {
	if cfgPath == "" && os.Getenv(web.EnvDatabaseDSN) == "" {
		return nil, exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing web config path (use --config web.yaml, or set "+web.EnvDatabaseDSN+")", nil)
	}
	st, err := web.CLIStore(cmd.Context(), cfgPath, nil)
	if err != nil {
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
	}
	return st, nil
}

func newWebUserAddCmd() *cobra.Command {
	var (
		roleStr    string
		dispName   string
		password   string
		stdinPw    bool
		genPw      bool
		mustChange bool
	)
	cmd := &cobra.Command{
		Use:   "add <username>",
		Short: "Tạo người dùng mới cho Web UI",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			username := strings.TrimSpace(args[0])
			if !cliUsernameRE.MatchString(username) {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "invalid username: 1-63 alphanumeric characters (may contain . _ -)", nil)
			}
			role := store.Role(roleStr)
			if !role.Valid() {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "invalid role: must be admin, operator, or viewer", nil)
			}
			if stdinPw && genPw {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "--password-stdin and --generate-password are mutually exclusive", nil)
			}

			pw := password
			generated := false
			switch {
			case genPw:
				g, err := web.GeneratePassword()
				if err != nil {
					return err
				}
				pw = g
				generated = true
			case stdinPw:
				line, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if err != nil && line == "" {
					return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "read password from stdin: "+err.Error(), err)
				}
				pw = strings.TrimRight(line, "\r\n")
			case pw == "":
				if term.IsTerminal(int(os.Stdin.Fd())) {
					p, err := readNewPassword(cmd)
					if err != nil {
						return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
					}
					pw = p
				} else {
					g, err := web.GeneratePassword()
					if err != nil {
						return err
					}
					pw = g
					generated = true
				}
			}

			if err := web.ValidatePassword(username, pw); err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}
			h, err := web.HashPassword(pw)
			if err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}

			st, err := getCLIStore(cmd)
			if err != nil {
				return err
			}
			defer st.Close()

			if generated {
				mustChange = true
			}
			u, err := st.CreateUser(cmd.Context(), store.User{
				Username:           username,
				DisplayName:        dispName,
				PasswordHash:       h,
				Role:               role,
				MustChangePassword: mustChange,
			})
			if err != nil {
				return exitcode.New("create_user_failed", exitcode.Unclassified, err.Error(), err)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "user %q created with role %s (id: %d)\n", u.Username, u.Role, u.ID)
			if generated {
				fmt.Fprintf(cmd.OutOrStdout(), "GENERATED PASSWORD (shown once, change on first login): %s\n", pw)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&roleStr, "role", "operator", "vai trò của người dùng (admin, operator, viewer)")
	f.StringVar(&dispName, "display-name", "", "tên hiển thị của người dùng")
	f.StringVar(&password, "password", "", "mật khẩu khởi tạo")
	f.BoolVar(&stdinPw, "password-stdin", false, "đọc mật khẩu từ stdin")
	f.BoolVar(&genPw, "generate-password", false, "sinh mật khẩu ngẫu nhiên")
	f.BoolVar(&mustChange, "must-change-password", false, "buộc người dùng đổi mật khẩu ở lần đăng nhập đầu")
	return cmd
}

func newWebUserListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "Liệt kê danh sách người dùng Web UI",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			st, err := getCLIStore(cmd)
			if err != nil {
				return err
			}
			defer st.Close()

			users, err := st.ListUsers(cmd.Context())
			if err != nil {
				return exitcode.New("list_users_failed", exitcode.Unclassified, err.Error(), err)
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "USERNAME\tROLE\tDISPLAY NAME\tSTATUS\tMUST CHANGE PW\tLAST LOGIN")
			for _, u := range users {
				status := "active"
				if u.Disabled {
					status = "disabled"
				} else if u.LockedUntil != nil && u.LockedUntil.After(time.Now()) {
					status = fmt.Sprintf("locked (%ds)", int(u.LockedUntil.Sub(time.Now()).Seconds()))
				}
				lastLogin := "never"
				if u.LastLoginAt != nil {
					lastLogin = u.LastLoginAt.Format(time.RFC3339)
				}
				mustChangeStr := "no"
				if u.MustChangePassword {
					mustChangeStr = "yes"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", u.Username, u.Role, u.DisplayName, status, mustChangeStr, lastLogin)
			}
			return w.Flush()
		},
	}
}

func newWebUserDisableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "disable <username>",
		Short: "Vô hiệu hoá tài khoản người dùng và hủy mọi phiên làm việc",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			username := strings.TrimSpace(args[0])
			st, err := getCLIStore(cmd)
			if err != nil {
				return err
			}
			defer st.Close()

			u, ok, err := st.GetUser(cmd.Context(), username)
			if err != nil || !ok {
				return exitcode.New("user_not_found", exitcode.InvalidSyntax, fmt.Sprintf("user %q not found", username), nil)
			}
			u.Disabled = true
			if err := st.UpdateUser(cmd.Context(), u); err != nil {
				return exitcode.New("disable_failed", exitcode.Unclassified, err.Error(), err)
			}
			_ = st.PurgeUserSessions(cmd.Context(), u.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "user %q disabled; all active sessions were terminated\n", username)
			return nil
		},
	}
}

func newWebUserEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "enable <username>",
		Short: "Kích hoạt lại tài khoản người dùng",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			username := strings.TrimSpace(args[0])
			st, err := getCLIStore(cmd)
			if err != nil {
				return err
			}
			defer st.Close()

			u, ok, err := st.GetUser(cmd.Context(), username)
			if err != nil || !ok {
				return exitcode.New("user_not_found", exitcode.InvalidSyntax, fmt.Sprintf("user %q not found", username), nil)
			}
			u.Disabled = false
			if err := st.UpdateUser(cmd.Context(), u); err != nil {
				return exitcode.New("enable_failed", exitcode.Unclassified, err.Error(), err)
			}
			_ = st.ResetLoginFailures(cmd.Context(), u.ID)
			fmt.Fprintf(cmd.OutOrStdout(), "user %q enabled\n", username)
			return nil
		},
	}
}

func newWebUserSetRoleCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set-role <username> <role>",
		Short: "Cập nhật vai trò RBAC của người dùng (admin, operator, viewer)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			username := strings.TrimSpace(args[0])
			role := store.Role(strings.TrimSpace(args[1]))
			if !role.Valid() {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "invalid role: must be admin, operator, or viewer", nil)
			}

			st, err := getCLIStore(cmd)
			if err != nil {
				return err
			}
			defer st.Close()

			u, ok, err := st.GetUser(cmd.Context(), username)
			if err != nil || !ok {
				return exitcode.New("user_not_found", exitcode.InvalidSyntax, fmt.Sprintf("user %q not found", username), nil)
			}
			u.Role = role
			if err := st.UpdateUser(cmd.Context(), u); err != nil {
				return exitcode.New("set_role_failed", exitcode.Unclassified, err.Error(), err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "role for user %q updated to %s\n", username, role)
			return nil
		},
	}
}

func newWebUserResetPasswordCmd() *cobra.Command {
	var (
		password   string
		stdinPw    bool
		genPw      bool
		mustChange bool
	)
	cmd := &cobra.Command{
		Use:   "reset-password <username>",
		Short: "Đặt lại mật khẩu cho người dùng và hủy mọi phiên làm việc",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			username := strings.TrimSpace(args[0])
			if stdinPw && genPw {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "--password-stdin and --generate-password are mutually exclusive", nil)
			}

			pw := password
			generated := false
			switch {
			case genPw:
				g, err := web.GeneratePassword()
				if err != nil {
					return err
				}
				pw = g
				generated = true
			case stdinPw:
				line, err := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if err != nil && line == "" {
					return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "read password from stdin: "+err.Error(), err)
				}
				pw = strings.TrimRight(line, "\r\n")
			case pw == "":
				if term.IsTerminal(int(os.Stdin.Fd())) {
					p, err := readNewPassword(cmd)
					if err != nil {
						return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
					}
					pw = p
				} else {
					g, err := web.GeneratePassword()
					if err != nil {
						return err
					}
					pw = g
					generated = true
				}
			}

			if err := web.ValidatePassword(username, pw); err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}
			h, err := web.HashPassword(pw)
			if err != nil {
				return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, err.Error(), err)
			}

			st, err := getCLIStore(cmd)
			if err != nil {
				return err
			}
			defer st.Close()

			u, ok, err := st.GetUser(cmd.Context(), username)
			if err != nil || !ok {
				return exitcode.New("user_not_found", exitcode.InvalidSyntax, fmt.Sprintf("user %q not found", username), nil)
			}

			if generated && !cmd.Flags().Changed("must-change-password") {
				mustChange = true
			}
			if err := st.SetUserPassword(cmd.Context(), u.ID, h, mustChange); err != nil {
				return exitcode.New("set_password_failed", exitcode.Unclassified, err.Error(), err)
			}
			_ = st.PurgeUserSessions(cmd.Context(), u.ID)
			if strings.EqualFold(username, "admin") {
				_ = web.SetStorePassword(cmd.Context(), st, h)
			}

			fmt.Fprintf(cmd.OutOrStdout(), "password reset for user %q (takes effect immediately; all sessions were signed out)\n", username)
			if generated {
				fmt.Fprintf(cmd.OutOrStdout(), "GENERATED PASSWORD (shown once): %s\n", pw)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&password, "password", "", "mật khẩu mới")
	f.BoolVar(&stdinPw, "password-stdin", false, "đọc mật khẩu từ stdin")
	f.BoolVar(&genPw, "generate-password", false, "sinh mật khẩu ngẫu nhiên")
	f.BoolVar(&mustChange, "must-change-password", true, "buộc người dùng đổi mật khẩu ở lần đăng nhập tiếp theo")
	return cmd
}
