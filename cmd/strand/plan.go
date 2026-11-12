package main

import (
	"database/sql"
	"fmt"
	"path"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/mysqlver"
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
			db, err := sql.Open("mysql", cfg.Source.DSN)
			if err != nil {
				return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
			}
			defer db.Close()
			ctx := cmd.Context()
			if ver, err := mysqlver.Cached(ctx, db); err != nil {
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
				FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
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
			fmt.Printf("%-40s %14s %14s %12s\n", "table", "rows(est)", "bytes", "mode")
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
				if mode == "logical" {
					total += size
				}
				fmt.Printf("%-40s %14d %14d %12s\n", name, n, size, mode)
			}
			eta := time.Duration(float64(total) / bytesPerSec * float64(time.Second))
			// 5.7: expire_logs_days * 86400 (không có binlog_expire_logs_seconds).
			expire, _ := mysqlver.BinlogRetentionSeconds(ctx, db)
			retention := time.Duration(expire) * time.Second
			fmt.Printf("\nlogical snapshot: %d bytes at source soft limit %d IOPS (~%.1f MiB/s) → ETA %s\n",
				total, iops, bytesPerSec/1024/1024, eta.Round(time.Second))
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
