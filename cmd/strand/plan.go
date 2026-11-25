package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/rowkey"
	"strand/internal/schemagen"
)

// `strand plan -c p.yaml` (02 §10): ước lượng ETA của snapshot logic; từ chối chế độ A khi
// ETA > 50% retention binlog, trừ khi có --force.
func newPlanCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Ước lượng ETA; từ chối chế độ A khi ETA > 50% retention",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing -c", nil)
			}
			cfg, err := config.LoadFile(cfgPath)
			if err != nil {
				return err
			}
			if cfg.IsMongoDB() {
				return mongoValidate(cmd.Context(), cmd.OutOrStdout(), cfg, true, force)
			}
			// Planning reads only the source.
			cfg.Target.SSHTunnel = nil
			closeTunnels, err := config.OpenSSHTunnels(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			defer closeTunnels()
			if engineOf(cfg.Source.Kind) == model.EnginePG {
				return planPGSnapshot(cmd.Context(), cmd.OutOrStdout(), cfg)
			}
			db, err := sql.Open("mysql", cfg.Source.DSN)
			if err != nil {
				return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
			}
			defer db.Close()
			ctx := cmd.Context()
			ver, err := mysqlver.Cached(ctx, db)
			if err != nil {
				return err
			} else if err := mysqlver.CheckSupported(ver); err != nil {
				return err
			}

			match := func(name string) bool {
				in := false
				for _, p := range cfg.Tables.Include {
					if ok, _ := path.Match(p, name); ok {
						in = true
					}
				}
				for _, p := range cfg.Tables.Exclude {
					if ok, _ := path.Match(p, name); ok {
						return false
					}
				}
				return in
			}
			rows, err := db.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(DATA_LENGTH,0), COALESCE(TABLE_ROWS,0)
				FROM information_schema.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED')
				AND TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema') ORDER BY 1, 2`)
			if err != nil {
				return err
			}
			defer rows.Close()

			iops := cfg.Resources.Source.IOPS.Soft
			if iops <= 0 {
				iops = 600
			}
			// 1 token ≈ 1 page IO (16 KiB) theo 02 §7; snapshot đọc tuần tự clustered index.
			bytesPerSec := float64(iops) * 16 * 1024
			var total int64
			var noKey []string
			ko := keylessOpts(cfg)
			flavor := "mysql"
			if ver.IsMariaDB() {
				flavor = "mariadb"
			}
			fmt.Printf("%-40s %14s %14s %12s  %s\n", "table", "rows(est)", "bytes", "mode", "key")
			for rows.Next() {
				var s, t string
				var size, n int64
				if err := rows.Scan(&s, &t, &size, &n); err != nil {
					return err
				}
				name := s + "." + t
				if !match(name) {
					continue
				}
				mode := strings.ToLower(cfg.Bootstrap.Default)
				if mode == "" {
					mode = "logical"
				}
				if pt, ok := cfg.Bootstrap.PerTable[name]; ok && pt.Mode != "" {
					mode = pt.Mode
				}

				res, err := rowkey.ResolveMySQL(ctx, db, s, t)
				if err != nil {
					return fmt.Errorf("inspect table %s: %w", name, err)
				}
				var keyDesc string
				switch {
				case res.Info.PKPrefix:
					keyDesc = "NONE (missing_pk)"
					noKey = append(noKey, fmt.Sprintf("table %s: primary key with prefix part is not supported; use a full-column primary key", name))
				case res.Key != nil:
					keyDesc = res.Key.Label()
				default:
					keyDesc = "NONE (missing_pk)"
					var why string
					if ko.On {
						why, err = ko.DecideMySQL(ctx, db, s, t)
						if err != nil {
							return fmt.Errorf("inspect table %s: %w", name, err)
						}
					}
					if ko.On && why == "" {
						keyDesc = rowkey.KeylessLabel
						if cfg.Bootstrap.AutoSeed {
							mode = "auto_seed"
						}
					} else {
						noKey = append(noKey, rowkey.Suggest(rowkey.SuggestInput{Flavor: flavor, Major: ver.Major, Minor: ver.Minor, Patch: ver.Patch,
							Table: model.TableID{Schema: s, Table: t}, EstRows: n, Notes: rowkey.Explain(res.Rejected), KeylessWhy: why}))
					}
				}
				if mode == "logical" || mode == "auto_seed" {
					total += size
				}
				fmt.Printf("%-40s %14d %14d %12s  %s\n", name, n, size, mode, keyDesc)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if len(noKey) > 0 {
				return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected,
					fmt.Sprintf("%d table(s) have no usable row key for this plan:\n\n%s", len(noKey), strings.Join(noKey, "\n\n")), nil)
			}
			eta := time.Duration(float64(total) / bytesPerSec * float64(time.Second))
			// 5.7: expire_logs_days * 86400 (không có binlog_expire_logs_seconds).
			expire, _ := mysqlver.BinlogRetentionSeconds(ctx, db)
			retention := time.Duration(expire) * time.Second
			label := "logical snapshot"
			if cfg.Bootstrap.AutoSeed {
				label = "initial copy"
				fmt.Println("\nAuto-seed includes foreign-key dependencies when needed. The estimate covers the initial copy of all selected tables.")
			}
			fmt.Printf("\n%s: %d bytes at source soft limit %d IOPS (~%.1f MiB/s) → ETA %s\n",
				label, total, iops, bytesPerSec/1024/1024, eta.Round(time.Second))
			if expire <= 0 {
				fmt.Println("binlog retention: unlimited")
				return nil
			}
			fmt.Printf("binlog retention: %s (ETA must be ≤ 50%% = %s)\n", retention, retention/2)
			if eta > retention/2 {
				if force {
					fmt.Println("WARNING: ETA exceeds 50% of binlog retention (--force given)")
					return nil
				}
				return exitcode.New("bootstrap_retention_risk", exitcode.Rejected,
					fmt.Sprintf("mode A ETA %s exceeds 50%% of binlog retention %s: use seed (mode B), raise retention, or --force",
						eta.Round(time.Second), retention), nil)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Chấp nhận chế độ A dù ETA > 50% retention")
	return cmd
}

func planPGSnapshot(ctx context.Context, out io.Writer, cfg *config.Config) error {
	db, err := config.OpenSQL("pgx", cfg.Source.DSN)
	if err != nil {
		return err
	}
	defer db.Close()
	tables, warnings, err := schemagen.Discover(ctx, db, "postgres", cfg.Tables.Include, cfg.Tables.Exclude)
	if err != nil {
		return err
	}
	for _, warning := range warnings {
		fmt.Fprintln(out, "warning:", warning)
	}
	var total int64
	for _, table := range tables {
		var size int64
		err := db.QueryRowContext(ctx, `
			WITH relation AS (SELECT to_regclass(quote_ident($1) || '.' || quote_ident($2)) AS oid)
			SELECT COALESCE(
				(SELECT sum(pg_total_relation_size(p.relid)) FROM pg_partition_tree(relation.oid) p WHERE p.isleaf),
				pg_total_relation_size(relation.oid)
			)::bigint FROM relation
		`, table.Schema, table.Table).Scan(&size)
		if err != nil {
			return fmt.Errorf("estimate size of %s: %w", table, err)
		}
		fmt.Fprintf(out, "%-40s %14d bytes\n", table, size)
		total += size
	}
	fmt.Fprintf(out, "\nPostgreSQL snapshot scope: %d table(s), %d bytes (including indexes and TOAST).\n", len(tables), total)
	return nil
}
