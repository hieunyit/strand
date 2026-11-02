package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/spf13/cobra"
	"strand/internal/config"
	"strand/internal/exitcode"
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
			}

			fmt.Fprintf(os.Stdout, "Preflight validation passed for pipeline: %s\n", cfg.Name)
			return nil
		},
	}
}
