package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"strand/internal/exitcode"
	"strand/internal/promote"
)

var (
	promoteTargetDSN        string
	promoteFenceSourceDSN   string
	promotePipelineID       string
	promoteOverrideSeqsJSON string
	promotePeakRate         float64
	promoteSyncIntervalSec  float64
	promoteCatchUpTimeout   time.Duration
)

func newPromoteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promote",
		Short: "Mở quyền ghi cho target sau khi fence source và setval (spec/02 §8, §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if promoteTargetDSN == "" {
				return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, "missing required flag --target", nil)
			}
			if promotePipelineID == "" {
				promotePipelineID = "default"
			}

			var seqOverrides map[string]int64
			if promoteOverrideSeqsJSON != "" {
				if err := json.Unmarshal([]byte(promoteOverrideSeqsJSON), &seqOverrides); err != nil {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --override-sequences JSON: %v", err), err)
				}
			}

			// Kết nối Target DB
			targetDB, err := sql.Open("mysql", promoteTargetDSN)
			if err != nil {
				return fmt.Errorf("connect target DB: %w", err)
			}
			defer targetDB.Close()

			logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
			targetDriver := promote.NewMySQLTargetDriver(targetDB)
			stateStore := promote.NewDBStateStore(targetDB)

			// Kết nối Source DB (nếu có)
			var sourceDriver promote.SourceDriver
			if promoteFenceSourceDSN != "" {
				sourceDB, err := sql.Open("mysql", promoteFenceSourceDSN)
				if err != nil {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --fence DSN: %v", err), err)
				}
				defer sourceDB.Close()
				sourceDriver = promote.NewMySQLSourceDriver(sourceDB)
				targetDriver.WithCatchUpCheck(sourceDB, promotePipelineID, promoteCatchUpTimeout, logger)
			}

			cfg := promote.Config{
				PipelineID:        promotePipelineID,
				TargetDSN:         promoteTargetDSN,
				SourceDSN:         promoteFenceSourceDSN,
				DryRun:            dryRun,
				PeakRate:          promotePeakRate,
				SyncIntervalSec:   promoteSyncIntervalSec,
				SequenceOverrides: seqOverrides,
			}

			p := promote.NewPromoter(cfg, stateStore, sourceDriver, targetDriver, nil, logger)

			progress, err := p.Run(cmd.Context())
			if err != nil {
				if progress != nil && progress.State == promote.StateAwaitingOperator {
					fmt.Printf("\n=== PROMOTE NEEDS OPERATOR DECISION ===\n")
					fmt.Printf("Trạng thái pipeline: %s\n", progress.State)
					fmt.Printf("Các sequence độc lập không đọc được từ source:\n")
					for seqName, suggested := range progress.SuggestedSequences {
						fmt.Printf("  - %s: gợi ý %d (lần sync cuối + margin)\n", seqName, suggested)
					}
					fmt.Printf("Vui lòng duyệt hoặc chỉ định giá trị qua --override-sequences '<json>' và chạy lại.\n")
				}
				return err
			}

			if dryRun {
				fmt.Println("Plan promote (dry-run): OK")
				return nil
			}

			fmt.Printf("Promote pipeline %s thành công. Target đã mở quyền ghi.\n", progress.PipelineID)
			return nil
		},
	}

	cmd.Flags().StringVar(&promoteTargetDSN, "target", "", "DSN của cơ sở dữ liệu target")
	cmd.Flags().StringVar(&promoteFenceSourceDSN, "fence", "", "DSN của cơ sở dữ liệu source cần fence")
	cmd.Flags().StringVar(&promotePipelineID, "pipeline", "default", "ID định danh của pipeline")
	cmd.Flags().StringVar(&promoteOverrideSeqsJSON, "override-sequences", "", "JSON chỉ định giá trị cho các sequence độc lập khi source hỏng")
	cmd.Flags().Float64Var(&promotePeakRate, "peak-rate", 1000, "Tốc độ cấp phát đỉnh r_peak (keys/giây)")
	cmd.Flags().DurationVar(&promoteCatchUpTimeout, "catchup-timeout", 5*time.Minute, "Thời gian tối đa chờ target apply hết log của source ở bước 3")
	cmd.Flags().Float64Var(&promoteSyncIntervalSec, "sync-interval", 5, "Chu kỳ đồng bộ sequence Delta t_sync (giây)")

	return cmd
}
