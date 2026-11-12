package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/objects"
)

var (
	objOut         string
	objPhase       string
	objFrom        string
	objKeepDefiner bool
	objAuth        string
	objSourceDSN   string
	objTargetDSN   string
)

const errObjectsUnsupported = "objects_unsupported"

// objectsConfig nạp cấu hình và từ chối (exit 3) cặp ngoài họ MySQL → MySQL.
func objectsConfig() (*config.Config, error) {
	if cfgPath == "" {
		return nil, exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing config file path (use -c or --config)", nil)
	}
	cfg, err := config.LoadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(cfg.Source.Kind, "mysql") || !strings.EqualFold(cfg.Target.Kind, "mysql") {
		return nil, exitcode.New(errObjectsUnsupported, exitcode.Rejected,
			fmt.Sprintf("strand objects supports only MySQL-family source (MariaDB, MySQL) → MySQL target, got %s → %s", cfg.Source.Kind, cfg.Target.Kind), nil)
	}
	return cfg, nil
}

func objectsOptions(cfg *config.Config, withUsers bool) (objects.Options, error) {
	o := objects.Options{Include: cfg.Tables.Include, Exclude: cfg.Tables.Exclude, Rename: cfg.Tables.Rename,
		KeepDefiner: objKeepDefiner, WithUsers: withUsers, Auth: objects.AuthNative}
	switch objAuth {
	case "", "native":
	case "reset":
		o.Auth = objects.AuthReset
	default:
		return o, exitcode.New("invalid_flag", exitcode.InvalidSyntax, "--auth must be native or reset", nil)
	}
	if c, err := drivermysql.ParseDSN(cfg.Source.DSN); err == nil {
		o.SelfUser = c.User
	}
	return o, nil
}

func sourceDSN(cfg *config.Config) string {
	if objSourceDSN != "" {
		return objSourceDSN
	}
	return cfg.Source.DSN
}

func targetDSN(cfg *config.Config) string {
	if objTargetDSN != "" {
		return objTargetDSN
	}
	return cfg.Target.DSN
}

func openMySQL(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, exitcode.New("invalid_connection", exitcode.InvalidSyntax, fmt.Sprintf("invalid DSN: %v", err), err)
	}
	return db, nil
}

// loadObjects đọc đối tượng từ source, hoặc từ thư mục --from (objects.json do export ghi).
func loadObjects(ctx context.Context, cfg *config.Config, from string, withUsers bool) (*objects.Set, error) {
	if from != "" {
		return objects.LoadManifest(from)
	}
	db, err := openMySQL(sourceDSN(cfg))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	o, err := objectsOptions(cfg, withUsers)
	if err != nil {
		return nil, err
	}
	return objects.Discover(ctx, db, o)
}

func printWarnings(w io.Writer, set *objects.Set) {
	for _, n := range set.Notes {
		fmt.Fprintln(w, "note: "+n)
	}
	for _, x := range set.AllWarnings() {
		fmt.Fprintln(w, "warning: "+x)
	}
	if set.Users != nil {
		for _, s := range set.Users.Skipped {
			fmt.Fprintln(w, "note: user skipped: "+s)
		}
		for _, it := range set.Users.Items {
			for _, n := range it.Notes {
				if !strings.Contains(n, "random value") { // mật khẩu reset chỉ ghi vào users.sql
					fmt.Fprintf(w, "warning: %s: %s\n", it.Account, n)
				}
			}
		}
	}
}

func newObjectsCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "objects",
		Short: "Chuyển view, procedure, function, trigger, event, user và quyền (nguồn MySQL/MariaDB → target MySQL)",
		Long: `Strand đồng bộ bảng; nhóm lệnh này chuyển phần còn lại.
Trigger KHÔNG được tồn tại ở target trong lúc sync (binlog ROW đã chứa các dòng trigger ghi ở source) và event không được chạy
ở target trước cutover. Trình tự: apply --phase pre (trước hoặc trong sync), apply --phase users, rồi --phase post lúc cutover
(hoặc promote --objects-post).`,
	}
	root.PersistentFlags().StringVar(&objSourceDSN, "source-dsn", "", "DSN source thay cho source.dsn của cấu hình (cần SHOW VIEW, SHOW_ROUTINE/mysql.proc, TRIGGER, EVENT, SELECT mysql.user)")
	root.PersistentFlags().StringVar(&objTargetDSN, "target-dsn", "", "DSN target thay cho target.dsn (cần CREATE VIEW, CREATE ROUTINE, TRIGGER, EVENT, CREATE USER...)")
	root.PersistentFlags().BoolVar(&objKeepDefiner, "keep-definer", false, "Giữ DEFINER của source (mặc định bỏ: định nghĩa thuộc về tài khoản chạy lệnh; user DEFINER phải có sẵn ở target)")

	list := &cobra.Command{
		Use:   "list",
		Short: "Liệt kê view, procedure, function, trigger, event và user/quyền trong các schema được chọn",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := objectsConfig()
			if err != nil {
				return err
			}
			set, err := loadObjects(cmd.Context(), cfg, "", true)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			for _, o := range set.Objects {
				line := fmt.Sprintf("%-10s %s", o.Kind, o.Qualified())
				if o.Kind == objects.KindTrigger {
					line += "  ON " + o.Table
				}
				if o.Kind == objects.KindEvent && o.EnableStmt == "" && o.Skipped == "" {
					line += "  (disabled on source)"
				}
				if o.Skipped != "" {
					line += "  SKIPPED: " + o.Skipped
				} else if len(o.Warnings) > 0 {
					line += fmt.Sprintf("  (%d warning)", len(o.Warnings))
				}
				fmt.Fprintln(out, line)
			}
			if set.Users != nil {
				for _, it := range set.Users.Items {
					kind := "user"
					if it.Role {
						kind = "role"
					}
					fmt.Fprintf(out, "%-10s %s  (%d statement)\n", kind, it.Account, len(it.Stmts))
				}
			}
			c := set.Counts()
			fmt.Fprintf(out, "total: %d view, %d procedure, %d function, %d trigger, %d event", c[objects.KindView], c[objects.KindProcedure], c[objects.KindFunction], c[objects.KindTrigger], c[objects.KindEvent])
			if set.Users != nil {
				fmt.Fprintf(out, ", %d user/role", len(set.Users.Items))
			}
			fmt.Fprintln(out)
			printWarnings(cmd.ErrOrStderr(), set)
			return nil
		},
	}

	export := &cobra.Command{
		Use:   "export",
		Short: "Ghi 01-views-routines.sql, 02-events-disabled.sql, 03-triggers.sql, 04-events-enable.sql, users.sql, objects.json",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if objOut == "" {
				return exitcode.New("invalid_flag", exitcode.InvalidSyntax, "missing required flag --out", nil)
			}
			cfg, err := objectsConfig()
			if err != nil {
				return err
			}
			set, err := loadObjects(cmd.Context(), cfg, "", true)
			if err != nil {
				return err
			}
			files, err := objects.Export(set, objOut, true)
			if err != nil {
				return err
			}
			for _, f := range files {
				fmt.Fprintln(cmd.OutOrStdout(), f)
			}
			printWarnings(cmd.ErrOrStderr(), set)
			return nil
		},
	}
	export.Flags().StringVar(&objOut, "out", "", "Thư mục ghi các tệp SQL")
	export.Flags().StringVar(&objAuth, "auth", "native", "Mật khẩu user: native (giữ hash mysql_native_password) | reset (mật khẩu ngẫu nhiên mới, caching_sha2_password)")

	apply := &cobra.Command{
		Use:   "apply",
		Short: "Áp lên target: --phase pre (view, routine, event DISABLE), users, post (trigger, bật event); idempotent",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := objectsConfig()
			if err != nil {
				return err
			}
			phase := objects.Phase(objPhase)
			if phase != objects.PhasePre && phase != objects.PhasePost && phase != objects.PhaseUsers {
				return exitcode.New("invalid_flag", exitcode.InvalidSyntax, "--phase must be pre, post or users", nil)
			}
			set, err := loadObjects(cmd.Context(), cfg, objFrom, phase == objects.PhaseUsers)
			if err != nil {
				return err
			}
			if phase == objects.PhaseUsers && set.Users == nil {
				return exitcode.New("invalid_flag", exitcode.InvalidSyntax, "no users in the manifest: export it with `strand objects export`", nil)
			}
			printWarnings(cmd.ErrOrStderr(), set)
			tdb, err := openMySQL(targetDSN(cfg))
			if err != nil {
				return err
			}
			defer tdb.Close()
			return runApply(cmd.Context(), tdb, set, phase, dryRun, cmd.OutOrStdout())
		},
	}
	apply.Flags().StringVar(&objPhase, "phase", "", "pre | post | users")
	apply.Flags().StringVar(&objFrom, "from", "", "Đọc định nghĩa từ thư mục export (objects.json) thay vì source")
	apply.Flags().StringVar(&objAuth, "auth", "native", "Mật khẩu user (phase users): native | reset")
	_ = apply.MarkFlagRequired("phase")

	root.AddCommand(list, export, apply)
	return root
}

// runApply áp một phase, in kết quả từng đối tượng; có lỗi thì trả error (exit 1) sau khi đã thử hết.
func runApply(ctx context.Context, db *sql.DB, set *objects.Set, phase objects.Phase, dry bool, out io.Writer) error {
	results := objects.Apply(ctx, db, set, objects.ApplyOptions{Phase: phase, DryRun: dry, OnResult: func(r objects.Result) {
		line := fmt.Sprintf("%-12s %s", r.Action, r.Label)
		if r.Err != nil {
			line += ": " + r.Err.Error()
		} else if r.Detail != "" {
			line += " (" + r.Detail + ")"
		}
		fmt.Fprintln(out, line)
		if dry && r.Stmt != "" && r.Action == objects.ActWould {
			fmt.Fprintln(out, r.Stmt+";")
		}
	}})
	if err := objects.FailureError(results); err != nil {
		return err
	}
	counts := map[objects.Action]int{}
	for _, r := range results {
		counts[r.Action]++
	}
	var parts []string
	for a, n := range counts {
		parts = append(parts, fmt.Sprintf("%d %s", n, a))
	}
	sort.Strings(parts)
	fmt.Fprintf(out, "phase %s: %s\n", phase, strings.Join(parts, ", "))
	return nil
}
