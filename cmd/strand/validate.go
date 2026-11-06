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
	"strand/internal/model"
	"strand/internal/schemagen"
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
				tables, err := validate.ExpandTables(cmd.Context(), db, cfg.Tables.Include, cfg.Tables.Exclude)
				if err != nil {
					return err
				}
				if err := validate.CheckMySQLSource(cmd.Context(), db, tables); err != nil {
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
					mpWarns, mpErr := typemap.CheckMySQLToPGTablesMapped(cmd.Context(), db, dstDB, tables, typemap.DefaultOptions(), cfg.Tables.Rename)
					for _, w := range mpWarns {
						fmt.Fprintln(os.Stderr, "warning: "+w)
					}
					if mpErr != nil {
						return mpErr
					}
				}
			}

			r, err := schemaRequest(cfg)
			if err != nil {
				return err
			}
			defer r.Source.Close()
			defer r.Target.Close()
			tables, warnings, err := schemagen.Discover(cmd.Context(), r.Source, r.SourceKind, cfg.Tables.Include, cfg.Tables.Exclude)
			if err != nil {
				return err
			}
			r.Tables = tables
			plan, err := schemagen.Build(cmd.Context(), r)
			if err != nil {
				return err
			}
			if len(plan.Tables) > 0 && !cfg.Bootstrap.CreateSchemaAuto() {
				return exitcode.New(exitcode.ErrSchemaChangePending, exitcode.Rejected,
					fmt.Sprintf("%d target table(s) missing and bootstrap.create_schema=off; run strand schema init", len(plan.Tables)), nil)
			}
			for _, w := range append(warnings, plan.Warnings...) {
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: "+w)
			}
			if len(plan.Tables) > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "%d table(s) will be created on target\n", len(plan.Tables))
			}
			// review R26: target MySQL phải dùng InnoDB cho bảng đích (sau rename) và bảng _strand_*.
			if strings.EqualFold(cfg.Target.Kind, "mysql") {
				targets := make([]model.TableID, 0, len(tables))
				for _, id := range tables {
					targets = append(targets, renamedTarget(cfg.Tables.Rename, id))
				}
				if err := validate.CheckMySQLTargetEngines(cmd.Context(), r.Target, targets); err != nil {
					return err
				}
			}
			fmt.Fprintf(os.Stdout, "Preflight validation passed for pipeline: %s\n", cfg.Name)
			return nil
		},
	}
}

// renamedTarget áp tables.rename ("schema.table" → "schema.table") lên tên bảng nguồn.
func renamedTarget(rename map[string]string, id model.TableID) model.TableID {
	if to, ok := rename[id.String()]; ok {
		if s, t, ok := strings.Cut(to, "."); ok {
			return model.TableID{Schema: s, Table: t}
		}
	}
	return id
}
