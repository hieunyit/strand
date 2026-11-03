package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/spf13/cobra"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/typemap"
	"strand/internal/validate"
)

func newValidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Preflight kiểm tra cấu hình và môi trường",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New(
					"missing_config_path",
					exitcode.InvalidSyntax,
					"missing config file path (use -c or --config)",
					nil,
				)
			}

			cfg, err := config.LoadFile(cfgPath)
			if err != nil {
				return err
			}

			// Key đã nhận nhưng chưa triển khai: luôn cảnh báo, không bỏ qua im lặng.
			for _, k := range cfg.UnimplementedKeys() {
				fmt.Fprintf(os.Stderr, "warning: %s\n", config.UnimplementedWarning(k))
			}

			if err := validate.Preflight(cmd.Context(), cfg); err != nil {
				return err
			}

			// Kiểm source thật (spec 02 §8): binlog, partial JSON, PK.
			if strings.EqualFold(cfg.Source.Kind, "mysql") {
				if cfg.Source.DSN == "" {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "source.dsn is empty", nil)
				}
				db, err := sql.Open("mysql", cfg.Source.DSN)
				if err != nil {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid source DSN: %v", err), err)
				}
				defer db.Close()
				if err := validate.CheckMySQLSource(cmd.Context(), db, cfg.Tables.Include); err != nil {
					return err
				}
				// Giá trị session của binlog_format/binlog_row_image trên các kết nối đang mở.
				warns, err := validate.CheckMySQLSessionBinlog(cmd.Context(), db)
				for _, w := range warns {
					fmt.Fprintln(os.Stderr, "warning: "+w)
				}
				if err != nil {
					return err
				}

				// spec 02 §8, TASK P2-04: Khi target là postgres, kiểm tra mapping kiểu, collation cột khóa và giá trị thực tế
				if strings.EqualFold(cfg.Target.Kind, "postgres") {
					var dstDB *sql.DB
					if cfg.Target.DSN != "" {
						var dstErr error
						dstDB, dstErr = sql.Open("pgx", cfg.Target.DSN)
						if dstErr != nil {
							return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid target DSN: %v", dstErr), dstErr)
						}
						defer dstDB.Close()
					}
					mpWarns, mpErr := validate.CheckMySQLToPGTables(cmd.Context(), db, dstDB, cfg.Tables.Include, typemap.DefaultOptions())
					for _, w := range mpWarns {
						fmt.Fprintln(os.Stderr, "warning: "+w)
					}
					if mpErr != nil {
						return mpErr
					}
				}
			}

			fmt.Fprintf(os.Stdout, "Preflight validation passed for pipeline: %s\n", cfg.Name)
			return nil
		},
	}
}
