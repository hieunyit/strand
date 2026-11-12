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
	"strand/internal/mysqlver"
	"strand/internal/objects"
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
				// MariaDB chỉ sang MySQL: từ chối trước mọi kiểm tra khác (kiểm tra của nhánh PG báo lỗi khó hiểu).
				if ver, err := mysqlver.Cached(cmd.Context(), db); err != nil {
					return err
				} else if err := mysqlver.CheckTargetForSource(ver, cfg.Target.Kind); err != nil {
					return err
				}
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

				// View/routine/trigger/event/user không được Strand chép: báo để không quên ở cutover.
				if sum, err := objects.Summary(cmd.Context(), db, objects.Options{Include: cfg.Tables.Include, Exclude: cfg.Tables.Exclude}); err != nil {
					fmt.Fprintln(os.Stderr, "warning: cannot count views/routines/triggers/events (source account lacks privileges?): "+err.Error())
				} else if w := objectsSummaryWarning(sum, strings.EqualFold(cfg.Target.Kind, "mysql")); w != "" {
					fmt.Fprintln(os.Stderr, "warning: "+w)
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
			if strings.EqualFold(cfg.Source.Kind, "postgres") {
				pgWarnings, err := validate.CheckPGSource(cmd.Context(), r.Source, cfg, tables)
				if err != nil {
					return err
				}
				warnings = append(warnings, pgWarnings...)
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
			printed := map[string]bool{}
			for _, w := range append(warnings, plan.Warnings...) {
				printed[w] = true
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
				if strings.EqualFold(cfg.Source.Kind, "mysql") {
					if ver, err := mysqlver.Cached(cmd.Context(), r.Source); err != nil {
						return err
					} else if ver.IsMariaDB() {
						names := make([]string, 0, len(tables))
						for _, id := range tables {
							names = append(names, id.Schema+"."+id.Table)
						}
						collWarnings, err := validate.CheckSourceCollationsOnTarget(cmd.Context(), r.Source, r.Target, names)
						for _, w := range collWarnings {
							if !printed[w] { // schemagen.Build đã in cùng cảnh báo
								fmt.Fprintln(cmd.ErrOrStderr(), "warning: "+w)
							}
						}
						if err != nil {
							return err
						}
					}
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

// objectsSummaryWarning: "N views, M triggers... are not migrated automatically"; rỗng nếu không có gì.
func objectsSummaryWarning(c map[objects.Kind]int, toMySQL bool) string {
	var parts []string
	for _, k := range []objects.Kind{objects.KindView, objects.KindProcedure, objects.KindFunction, objects.KindTrigger, objects.KindEvent} {
		if n := c[k]; n > 0 {
			name := string(k)
			if n != 1 {
				name += "s"
			}
			parts = append(parts, fmt.Sprintf("%d %s", n, name))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	msg := strings.Join(parts, ", ") + " are not migrated automatically (nor are users and grants; counts cover only what the source account can see)"
	if toMySQL {
		return msg + "; use `strand objects list|export|apply`"
	}
	return msg + ": recreate them on the target by hand"
}
