package main

import (
	"github.com/spf13/cobra"

	"strand/internal/exitcode"
	"strand/internal/web"
)

// newWebDBCmd: `strand web db migrate|export`: chuyển chế độ file sang PostgreSQL (có kiểm chứng) và xuất DB ra
// cây file (sao lưu, quay lại chế độ file).
func newWebDBCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Kho metadata PostgreSQL của Web UI: migrate (file -> DB), export (DB -> file)",
	}
	var dsn string
	var dry bool
	mig := &cobra.Command{
		Use:   "migrate",
		Short: "Nhập trạng thái chế độ file (web.yaml, pipelines.json, scan, jobs, wizards, audit.log) vào PostgreSQL, rồi kiểm chứng",
		Long: `Đọc bản cài chế độ file (--config web.yaml và state_dir của nó), in kế hoạch (số lượng mỗi loại: new, present,
conflict, skipped), nhập những gì còn thiếu (idempotent: chạy lại an toàn, dừng giữa chừng thì chạy lại tiếp tục),
rồi kiểm chứng (sha256 cấu hình, id job/wizard, số dòng audit) và in báo cáo. Không xóa hay sửa file nào.
--dry-run: chỉ in kế hoạch, không ghi gì vào DB (kể cả lược đồ).
DSN: --dsn, hoặc STRAND_WEB_DATABASE_DSN (khuyến nghị: không lộ qua ps), hoặc database.dsn trong web.yaml.
.env của pipeline chỉ được nhập (mã hóa) khi có STRAND_WEB_SECRET_KEY.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := web.MigrateFiles(cmd.Context(), web.MigrateOptions{ConfigPath: cfgPath, DSN: dsn, DryRun: dry, Out: cmd.OutOrStdout()})
			if err != nil {
				return exitcode.New("web_db_migrate", exitcode.InvalidSyntax, err.Error(), err)
			}
			return nil
		},
	}
	mig.Flags().StringVar(&dsn, "dsn", "", "DSN PostgreSQL (mặc định: "+web.EnvDatabaseDSN+" hoặc database.dsn của web.yaml)")
	mig.Flags().BoolVar(&dry, "dry-run", false, "chỉ in kế hoạch, không ghi gì")

	var dir, dsn2 string
	exp := &cobra.Command{
		Use:   "export",
		Short: "Xuất toàn bộ trạng thái trong DB ra thư mục theo bố cục chế độ file (sao lưu / quay lại chế độ file)",
		Long: `Ghi vào --dir (rỗng hoặc chưa có): web.yaml (hash mật khẩu/token, cài đặt từ trang Settings, không có
mục database), state/pipelines.json, state/jobs/<id>.json|.log, state/wizards/*.json, state/audit.log,
pipelines/<name>.yaml|.env (bản trong DB; .env chỉ khi có STRAND_WEB_SECRET_KEY) và pipelines/MANIFEST.json
(đường dẫn gốc). --config (tùy chọn): web.yaml hiện tại, để giữ listen/TLS/state_dir trong file xuất ra.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := web.ExportDB(cmd.Context(), web.ExportOptions{Dir: dir, DSN: dsn2, ConfigPath: cfgPath, Out: cmd.OutOrStdout()}); err != nil {
				return exitcode.New("web_db_export", exitcode.InvalidSyntax, err.Error(), err)
			}
			return nil
		},
	}
	exp.Flags().StringVar(&dir, "dir", "", "thư mục đích (rỗng hoặc chưa có)")
	exp.Flags().StringVar(&dsn2, "dsn", "", "DSN PostgreSQL (mặc định: "+web.EnvDatabaseDSN+" hoặc database.dsn của web.yaml)")
	cmd.AddCommand(mig, exp)
	return cmd
}
