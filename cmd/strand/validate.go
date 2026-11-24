package main

import (
	"database/sql"
	"errors"
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
	"strand/internal/pipeline"
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

			closeTunnels, err := config.OpenSSHTunnels(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			defer closeTunnels()
			if cfg.IsMongoDB() {
				return mongoValidate(cmd.Context(), os.Stdout, cfg, false, false)
			}
			// Key đã nhận nhưng chưa triển khai: luôn cảnh báo, không bỏ qua im lặng.
			for _, k := range cfg.UnimplementedKeys() {
				fmt.Fprintf(os.Stderr, "warning: %s\n", config.UnimplementedWarning(k))
			}
			if w := maxParallelWarning(cfg); w != "" {
				fmt.Fprintln(os.Stderr, "warning: "+w)
			}
			var autoSeedKeylessTables []model.TableID

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
				keyOpts := keylessOpts(cfg)
				if cfg.Bootstrap.AutoSeed {
					keyOpts.FoundKeyless = func(id model.TableID) { autoSeedKeylessTables = append(autoSeedKeylessTables, id) }
				}
				kw, err := validate.CheckMySQLSourceOpts(cmd.Context(), db, tables, keyOpts)
				for _, w := range kw {
					fmt.Fprintln(os.Stderr, "warning: "+w)
				}
				if err != nil {
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
				if w := validate.ReplicaStatusWarning(cmd.Context(), db); w != "" {
					fmt.Fprintln(os.Stderr, "warning: "+w)
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
						dstDB, dstErr = config.OpenSQL("pgx", cfg.Target.DSN)
						if dstErr != nil {
							return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid target DSN: %v", dstErr), dstErr)
						}
						defer dstDB.Close()
					}
					tmOpts := typemap.DefaultOptions()
					tmOpts.AllowKeyless = cfg.Tables.KeylessOn()
					mpWarns, mpErr := typemap.CheckMySQLToPGTablesMapped(cmd.Context(), db, dstDB, tables, tmOpts, cfg.Tables.Rename)
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
			if r.SourceKind == r.TargetKind {
				engine := model.EngineMySQL
				if strings.EqualFold(cfg.Source.Kind, "postgres") {
					engine = model.EnginePG
				}
				if err := validate.CheckTableOverlap(cmd.Context(), r.Source, r.Target, engine, tables, func(id model.TableID) model.TableID {
					return renamedTarget(cfg.Tables.Rename, id)
				}); err != nil {
					return err
				}
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
			keyOpts := keylessOpts(cfg)
			keyLines, err := validate.CheckKeys(cmd.Context(), r.Source, r.SourceKind, r.Target, r.TargetKind, tables, cfg.Tables.Rename, keyOpts)
			for _, l := range keyLines {
				fmt.Fprintln(cmd.OutOrStdout(), l)
			}
			if err != nil {
				return err
			}
			if cfg.Bootstrap.AutoSeed && strings.EqualFold(cfg.Source.Kind, "mysql") && strings.EqualFold(cfg.Target.Kind, "mysql") {
				resumed := false
				var targetSchema sql.NullString
				if err := r.Target.QueryRowContext(cmd.Context(), "SELECT DATABASE()").Scan(&targetSchema); err != nil {
					return err
				}
				if !targetSchema.Valid || targetSchema.String == "" {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "target DSN must select the MySQL checkpoint database for auto-seed", nil)
				}
				hasCheckpointTable, err := schemagen.Exists(cmd.Context(), r.Target, "mysql", model.TableID{Schema: targetSchema.String, Table: "_strand_checkpoint"})
				if err != nil {
					return err
				}
				if hasCheckpointTable {
					var applied string
					err := r.Target.QueryRowContext(cmd.Context(), "SELECT applied FROM _strand_checkpoint WHERE pipeline_id=?", cfg.Name).Scan(&applied)
					// Start creates an empty checkpoint row before attempting the copy;
					// only a published applied position means this is a real resume.
					resumed = err == nil && applied != ""
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return err
					}
				}
				if !resumed {
					if err := pipeline.ValidateAutoSeedPreflight(cmd.Context(), r.Source, r.Target, cfg.Bootstrap.CreateSchemaAuto(), tables, autoSeedKeylessTables, func(id model.TableID) model.TableID {
						return renamedTarget(cfg.Tables.Rename, id)
					}); err != nil {
						return err
					}
				}
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

// keylessOpts: cấu hình keyless cho validate; một bảng "có seed" khi per_table[bảng].mode = seed, hoặc (không có
// cấu hình riêng) bootstrap.default là seed/start_at.
func keylessOpts(cfg *config.Config) validate.KeylessOpts {
	return validate.KeylessOpts{
		On:                    cfg.Tables.KeylessOn(),
		AllowFloat:            cfg.Tables.KeylessAllowFloat,
		RequireBinlogPosition: cfg.Bootstrap.AutoSeed,
		Seeded: func(table string) bool {
			if cfg.Bootstrap.AutoSeed {
				return true
			}
			if pt, ok := cfg.Bootstrap.PerTable[table]; ok && pt.Mode != "" {
				return strings.EqualFold(pt.Mode, "seed")
			}
			switch strings.ToLower(strings.TrimSpace(cfg.Bootstrap.Default)) {
			case "seed", "start_at":
				return true
			}
			return false
		},
	}
}

// maxParallelWarning: bootstrap.logical.max_parallel > 1 với source PostgreSQL không có tác dụng (snapshot
// logic của source PG luôn tuần tự từng bảng).
func maxParallelWarning(cfg *config.Config) string {
	if cfg.Bootstrap.Logical.MaxParallel > 1 && !strings.EqualFold(cfg.Source.Kind, "mysql") {
		return fmt.Sprintf("bootstrap.logical.max_parallel = %d is ignored for PostgreSQL sources: tables are snapshotted one at a time",
			cfg.Bootstrap.Logical.MaxParallel)
	}
	return ""
}
