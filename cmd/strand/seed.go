package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/seed"
)

// `strand seed plan` / `strand seed apply` (02 §9, §10; design §3.3).
func newSeedCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "seed", Short: "Seed từ backup (chế độ B): plan kiểm SD-1..SD-4, apply điều phối restore"}

	var (
		backup, kind, sourceDSN, targetDSN, archiveUntil, out, dumpLog string
		tables                                                         []string
		allowPre, acceptUnverifiedLockMode                             bool
	)
	plan := &cobra.Command{
		Use:   "plan",
		Short: "Kiểm SD-1..SD-4 và ghi seed.json",
		RunE: func(cmd *cobra.Command, args []string) error {
			if kind == "" {
				return exitcode.New("missing_type", exitcode.InvalidSyntax, "missing --type xtrabackup|mydumper|mysqldump|clone", nil)
			}
			ctx := cmd.Context()
			src := seed.SeedSource{
				Engine:                   "mysql",
				Kind:                     kind,
				BackupRef:                backup,
				AllowPrereleaseTool:      allowPre,
				Tables:                   tables,
				ArchiveUntilGTID:         archiveUntil,
				DumpLog:                  dumpLog,
				AcceptUnverifiedLockMode: acceptUnverifiedLockMode,
			}
			if sourceDSN != "" {
				db, err := sql.Open("mysql", sourceDSN)
				if err != nil {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
				}
				defer db.Close()
				if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_purged").Scan(&src.SourcePurgedGTID); err != nil {
					return fmt.Errorf("read source gtid_purged: %w", err)
				}
			}
			if targetDSN != "" {
				db, err := sql.Open("mysql", targetDSN)
				if err != nil {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
				}
				defer db.Close()
				if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.version, @@GLOBAL.gtid_executed").
					Scan(&src.TargetVersion, &src.TargetRestoredGTID); err != nil {
					return fmt.Errorf("read target version/gtid_executed: %w", err)
				}
				src.TargetRestoredGTID = strings.ReplaceAll(src.TargetRestoredGTID, "\n", "")
			}
			var planner interface {
				Plan(context.Context, seed.SeedSource) (*seed.SeedManifest, error)
			}
			switch kind {
			case "xtrabackup":
				planner = seed.NewXtraBackupPlanner()
			case "mydumper":
				planner = seed.NewMydumperPlanner()
			case "mysqldump":
				planner = seed.NewMysqldumpPlanner()
			case "clone":
				planner = seed.NewClonePlanner()
			default:
				return exitcode.New("invalid_type", exitcode.InvalidSyntax, "unknown --type "+kind, nil)
			}
			m, err := planner.Plan(ctx, src)
			if err != nil {
				return err
			}
			for _, w := range m.Warnings {
				fmt.Fprintln(os.Stderr, w)
			}
			raw, _ := json.MarshalIndent(m, "", "  ")
			if dryRun {
				fmt.Println(string(raw))
				return nil
			}
			if err := os.WriteFile(out, raw, 0o640); err != nil {
				return err
			}
			fmt.Printf("seed plan OK: P = %s, manifest %s\n", m.StartPosition.GTIDSet, out)
			return nil
		},
	}
	plan.Flags().StringVar(&backup, "backup", "", "Đường dẫn/URI bản backup")
	plan.Flags().StringVar(&kind, "type", "", "xtrabackup | mydumper | mysqldump | clone")
	plan.Flags().StringVar(&sourceDSN, "source", "", "DSN source (đọc gtid_purged, SD-2)")
	plan.Flags().StringVar(&targetDSN, "target", "", "DSN target sau restore (đối chiếu P, SD-1; version, SD-4)")
	plan.Flags().StringVar(&archiveUntil, "archive-until", "", "GTID set đã có trong archive binlog")
	plan.Flags().StringSliceVar(&tables, "tables", nil, "Danh sách bảng")
	plan.Flags().BoolVar(&allowPre, "allow-prerelease-tool", false, "Chấp nhận backup tạo bằng công cụ bản RC")
	plan.Flags().StringVar(&dumpLog, "dump-log", "", "log stderr của mydumper (bắt buộc với --type mydumper, để kiểm chế độ khóa)")
	plan.Flags().BoolVar(&acceptUnverifiedLockMode, "accept-unverified-lock-mode", false, "Chấp nhận backup mydumper không có log xác minh chế độ khóa")
	plan.Flags().StringVar(&out, "out", "seed.json", "File seed.json")

	var manifestPath, applyTarget, pfSpool, pfArchive, pfSource string
	var printOnly, pfOn bool
	var pfHard, pfLocalHard int64
	var pfOverflow, restoreCmd string
	apply := &cobra.Command{
		Use:   "apply",
		Short: "Điều phối restore/load rồi chuẩn bị target cho catch-up; chạy lại được khi bị ngắt",
		RunE: func(cmd *cobra.Command, args []string) error {
			raw, err := os.ReadFile(manifestPath)
			if err != nil {
				return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, err.Error(), err)
			}
			var m seed.SeedManifest
			if err := json.Unmarshal(raw, &m); err != nil {
				return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, err.Error(), err)
			}
			fmt.Println("# Bước restore (Strand gọi công cụ native, không tự restore — design §3):")
			for _, c := range restoreCommands(&m) {
				fmt.Println("  " + c)
			}
			if printOnly || dryRun {
				return nil
			}
			// Prefetch (mặc định bật): đọc log từ P vào spool trong lúc restore (02 §9).
			if pfOn && pfSpool != "" {
				var n int
				if restoreCmd != "" {
					n, err = coordinateRestoreAndPrefetch(
						cmd.Context(),
						func(rctx context.Context) error {
							return runRestoreShellCmd(rctx, restoreCmd)
						},
						func(pctx context.Context, until <-chan struct{}) (int, error) {
							return prefetch(pctx, &seedManifestLite{P: m.StartPosition.GTIDSet}, pfSpool, pfLocalHard, pfHard, pfOverflow, pfArchive, pfSource, until)
						},
					)
				} else {
					n, err = prefetch(cmd.Context(), &seedManifestLite{P: m.StartPosition.GTIDSet}, pfSpool, pfLocalHard, pfHard, pfOverflow, pfArchive, pfSource, nil)
				}
				if err != nil {
					return err
				}
				fmt.Printf("prefetch: %d transactions after P stored in spool %s\n", n, pfSpool)
			} else if restoreCmd != "" {
				if err := runRestoreShellCmd(cmd.Context(), restoreCmd); err != nil {
					return fmt.Errorf("restore-cmd failed: %w", err)
				}
			}
			if applyTarget == "" {
				return exitcode.New("missing_target", exitcode.InvalidSyntax, "missing --target", nil)
			}
			db, err := sql.Open("mysql", applyTarget)
			if err != nil {
				return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
			}
			defer db.Close()
			if err := prepareRestoredTarget(cmd.Context(), db, &m); err != nil {
				return err
			}
			raw, _ = json.MarshalIndent(m, "", "  ")
			if err := os.WriteFile(manifestPath, raw, 0o640); err != nil {
				return err
			}
			fmt.Printf("seed apply OK: target at P = %s; next: strand sync --start-at seed --seed-manifest %s\n",
				m.StartPosition.GTIDSet, manifestPath)
			return nil
		},
	}
	apply.Flags().StringVar(&manifestPath, "manifest", "seed.json", "File seed.json")
	apply.Flags().StringVar(&applyTarget, "target", "", "DSN target đã restore")
	apply.Flags().BoolVar(&printOnly, "print-only", false, "Chỉ in các bước, không thực hiện")
	apply.Flags().BoolVar(&pfOn, "prefetch", true, "Đọc log từ P vào spool trong lúc restore (02 §9)")
	apply.Flags().StringVar(&pfSpool, "spool", "", "Thư mục spool prefetch (= <resources.spool.path>/prefetch của pipeline)")
	apply.Flags().Int64Var(&pfHard, "spool-hard", 0, "Hard limit (byte) của spool; 0 = không giới hạn")
	apply.Flags().StringVar(&pfArchive, "archive", "", "Thư mục binlog archive")
	apply.Flags().Int64Var(&pfLocalHard, "spool-local-hard", 0, "Hard limit (byte) tầng local; vượt thì tràn sang --overflow")
	apply.Flags().StringVar(&pfOverflow, "overflow", "", "Tầng bucket s3://bucket/prefix (endpoint/credential qua STRAND_S3_*)")
	apply.Flags().StringVar(&pfSource, "source", "", "DSN source/replica để prefetch phần chưa archive")
	apply.Flags().StringVar(&restoreCmd, "restore-cmd", "", "Lệnh restore chạy bằng sh -c song song với prefetch")

	cmd.AddCommand(plan, apply)
	return cmd
}

func restoreCommands(m *seed.SeedManifest) []string {
	switch m.Kind {
	case "xtrabackup":
		return []string{
			"xtrabackup --prepare --target-dir=" + m.BackupRef,
			"xtrabackup --copy-back --target-dir=" + m.BackupRef + "   # trên máy target, mysqld đang dừng",
			"khởi động mysqld với skip_replica_start=ON",
		}
	case "mydumper":
		return []string{"myloader --directory=" + m.BackupRef + " --drop-table=DROP"}
	case "mysqldump":
		return []string{"mysql < " + m.BackupRef}
	case "clone":
		return []string{"CLONE INSTANCE FROM <user>@<donor>:<port> IDENTIFIED BY <password>   # trên target"}
	}
	return []string{"(không có lệnh restore cho kind " + m.Kind + ")"}
}

// prepareRestoredTarget là phần idempotent sau restore:
//  1. Backup lấy từ replica mang theo cấu hình replication; target không được tự replicate
//     (nếu không, dữ liệu trôi khỏi P và Strand apply chồng lên): dừng và xóa mọi channel.
//  2. gtid_executed của target phải đúng bằng P (SD-1).
func prepareRestoredTarget(ctx context.Context, db *sql.DB, m *seed.SeedManifest) error {
	var channels int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.replication_connection_configuration").Scan(&channels); err != nil {
		return fmt.Errorf("check target replication: %w", err)
	}
	if channels > 0 {
		if _, err := db.ExecContext(ctx, "STOP REPLICA"); err != nil {
			return fmt.Errorf("stop replication on target: %w", err)
		}
		if _, err := db.ExecContext(ctx, "RESET REPLICA ALL"); err != nil {
			return fmt.Errorf("reset replication on target: %w", err)
		}
		fmt.Printf("target had %d replication channel(s) from the backup: stopped and removed\n", channels)
	}
	var executed string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return err
	}
	executed = strings.ReplaceAll(executed, "\n", "")
	got, err := checkpoint.ParseMySQLGTIDSet(executed)
	if err != nil {
		return err
	}
	want, err := checkpoint.ParseMySQLGTIDSet(m.StartPosition.GTIDSet)
	if err != nil {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "invalid P in manifest", err)
	}
	if !got.Equal(want) {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("target gtid_executed (%s) != P (%s): restore incomplete or target replicated past P", executed, m.StartPosition.GTIDSet), nil)
	}
	m.PositionCrossCheck = &seed.PositionCrossCheck{RestoredGTIDExecuted: executed, Match: true}
	return nil
}

// runRestoreShellCmd chạy lệnh restore qua sh -c, chuyển toàn bộ stdout/stderr sang stderr của strand.
// Đặt process group riêng để khi hủy context (do prefetch lỗi) thì dừng cả tiến trình con.
func runRestoreShellCmd(ctx context.Context, cmdStr string) error {
	c := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	c.Stdout = os.Stderr
	c.Stderr = os.Stderr
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error {
		if c.Process != nil {
			_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			return c.Process.Kill()
		}
		return nil
	}
	return c.Run()
}

// coordinateRestoreAndPrefetch điều phối restoreFn chạy song song với prefetchFn:
//   - restoreFn thoát 0 -> đóng until để prefetchFn đọc nốt tới gtid_executed hiện tại rồi dừng;
//   - restoreFn thoát != 0 -> hủy context của prefetchFn và trả lỗi restore (exit 1);
//   - prefetchFn lỗi -> hủy context của restoreFn và trả nguyên lỗi prefetch (giữ exit code).
func coordinateRestoreAndPrefetch(
	ctx context.Context,
	restoreFn func(ctx context.Context) error,
	prefetchFn func(ctx context.Context, until <-chan struct{}) (int, error),
) (int, error) {
	rctx, cancelRestore := context.WithCancel(ctx)
	defer cancelRestore()
	pctx, cancelPrefetch := context.WithCancel(ctx)
	defer cancelPrefetch()

	until := make(chan struct{})
	var prefetchFailed atomic.Bool
	restoreErrCh := make(chan error, 1)
	go func() {
		err := restoreFn(rctx)
		if err == nil {
			close(until)
		} else if !prefetchFailed.Load() {
			cancelPrefetch()
		}
		restoreErrCh <- err
	}()

	n, pErr := prefetchFn(pctx, until)
	if pErr != nil {
		prefetchFailed.Store(true)
		cancelRestore()
	}
	rErr := <-restoreErrCh
	if pErr != nil {
		// Nếu prefetchFn chỉ dừng vì restoreFn lỗi và hủy pctx, trả lỗi của restore-cmd (exit 1).
		if rErr != nil && errors.Is(pErr, context.Canceled) && ctx.Err() == nil {
			return n, fmt.Errorf("restore-cmd failed: %w", rErr)
		}
		return n, pErr
	}
	if rErr != nil {
		return n, fmt.Errorf("restore-cmd failed: %w", rErr)
	}
	return n, nil
}
