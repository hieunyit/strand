package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"

	"strand/internal/exitcode"
	"strand/internal/model"
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
	promoteSourceFencedExt  bool
	promoteEngine           string
	promoteAppRole          string
	promoteSchemas          []string
	promoteSourceAppRoles   []string
)

// isPGDSN nhận diện DSN PostgreSQL dạng URL.
func isPGDSN(dsn string) bool {
	l := strings.ToLower(strings.TrimSpace(dsn))
	return strings.HasPrefix(l, "postgres://") || strings.HasPrefix(l, "postgresql://")
}

func newPromoteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "promote",
		Short: "Mở quyền ghi cho target sau khi fence source và setval (spec/02 §8, §10)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := normalizeDSNFlags(&promoteTargetDSN, &promoteFenceSourceDSN); err != nil {
				return err
			}
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

			// Xác định engine của target: PG hoặc MySQL
			engine := strings.ToLower(strings.TrimSpace(promoteEngine))
			isPG := false
			if engine == "pg" || engine == "postgres" || engine == "postgresql" {
				isPG = true
			} else if engine == "mysql" {
				isPG = false
			} else {
				// auto: nhận diện từ DSN (postgres:// hoặc postgresql://)
				isPG = isPGDSN(promoteTargetDSN)
			}

			logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
			var targetDriver promote.TargetDriver
			var sourceDriver promote.SourceDriver
			var stateStore promote.StateStore
			var sequences []promote.SequenceInfo

			if isPG {
				// Kết nối Target DB PostgreSQL
				targetDB, err := sql.Open("pgx", promoteTargetDSN)
				if err != nil {
					return fmt.Errorf("connect target DB (PG): %w", err)
				}
				defer targetDB.Close()

				pgTargetDriver := promote.NewPGTargetDriver(targetDB, promoteAppRole).
					WithSchemas(promoteSchemas...).
					WithPipeline(promotePipelineID)
				targetDriver = pgTargetDriver
				stateStore = promote.NewPGStateStore(targetDB)

				// Kết nối Source DB (nếu có): PG→PG hoặc MySQL→PG, nhận diện theo DSN của --fence.
				if promoteFenceSourceDSN != "" {
					if isPGDSN(promoteFenceSourceDSN) {
						sourceDB, err := sql.Open("pgx", promoteFenceSourceDSN)
						if err != nil {
							return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --fence DSN: %v", err), err)
						}
						defer sourceDB.Close()
						sourceDriver = promote.NewPGSourceDriver(sourceDB, promoteSourceAppRoles...)
						pgTargetDriver.WithCatchUpCheck(sourceDB, promotePipelineID, promoteCatchUpTimeout, logger)
					} else {
						sourceDB, err := sql.Open("mysql", promoteFenceSourceDSN)
						if err != nil {
							return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --fence DSN: %v", err), err)
						}
						defer sourceDB.Close()
						sourceDriver = promote.NewMySQLSourceDriver(sourceDB)
						pgTargetDriver.WithSourceEngine(model.EngineMySQL).
							WithCatchUpCheck(sourceDB, promotePipelineID, promoteCatchUpTimeout, logger)
					}
				}

				// Khám phá toàn bộ sequences và identity trên PostgreSQL target
				seqs, err := promote.DiscoverPGSequences(cmd.Context(), targetDB, promoteSchemas)
				if err != nil {
					return fmt.Errorf("discover PG sequences: %w", err)
				}
				sequences = seqs
			} else {
				// Kết nối Target DB MySQL (giữ nguyên 100% hành vi cũ)
				targetDSN := promoteTargetDSN
				if c, err := drivermysql.ParseDSN(promoteTargetDSN); err == nil {
					c.ParseTime = true
					targetDSN = c.FormatDSN()
				}
				targetDB, err := sql.Open("mysql", targetDSN)
				if err != nil {
					return fmt.Errorf("connect target DB: %w", err)
				}
				defer targetDB.Close()

				myTargetDriver := promote.NewMySQLTargetDriver(targetDB)
				myTargetDriver.WithPipeline(promotePipelineID)
				targetDriver = myTargetDriver
				stateStore = promote.NewDBStateStore(targetDB)

				// Kết nối Source DB (nếu có)
				if promoteFenceSourceDSN != "" {
					sourceDB, err := sql.Open("mysql", promoteFenceSourceDSN)
					if err != nil {
						return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --fence DSN: %v", err), err)
					}
					defer sourceDB.Close()
					sourceDriver = promote.NewMySQLSourceDriver(sourceDB)
					myTargetDriver.WithCatchUpCheck(sourceDB, promotePipelineID, promoteCatchUpTimeout, logger)
				}
				sequences = nil
			}

			cfg := promote.Config{
				PipelineID:             promotePipelineID,
				TargetDSN:              promoteTargetDSN,
				SourceDSN:              promoteFenceSourceDSN,
				DryRun:                 dryRun,
				PeakRate:               promotePeakRate,
				SyncIntervalSec:        promoteSyncIntervalSec,
				SequenceOverrides:      seqOverrides,
				SourceFencedExternally: promoteSourceFencedExt,
			}

			p := promote.NewPromoter(cfg, stateStore, sourceDriver, targetDriver, sequences, logger)

			progress, err := p.Run(cmd.Context())
			if err != nil {
				if progress != nil && progress.State == promote.StateAwaitingOperator {
					fmt.Printf("\n=== PROMOTE NEEDS OPERATOR DECISION ===\n")
					fmt.Printf("Trạng thái pipeline: %s\n", progress.State)
					if !progress.IsStepCompleted(promote.StepFenceSource) {
						fmt.Printf("Không fence được source cũ (không truy cập được hoặc thiếu --fence).\n")
						fmt.Printf("Cô lập source bằng cách khác rồi chạy lại với --source-fenced-externally.\n")
					}
					if progress.FailoverSummary != "" {
						fmt.Printf("Dữ liệu sẽ mất nếu tiếp tục: %s\n", progress.FailoverSummary)
					}
					if len(progress.SuggestedSequences) > 0 {
						fmt.Printf("Các sequence độc lập không đọc được từ source:\n")
						for seqName, suggested := range progress.SuggestedSequences {
							fmt.Printf("  - %s: gợi ý %d (lần sync cuối + margin)\n", seqName, suggested)
						}
						fmt.Printf("Duyệt hoặc chỉ định giá trị qua --override-sequences '<json>' khi chạy lại.\n")
					}
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
	cmd.Flags().BoolVar(&promoteSourceFencedExt, "source-fenced-externally", false, "Xác nhận source cũ đã bị cô lập bằng cách khác khi Strand không fence được (failover)")
	cmd.Flags().DurationVar(&promoteCatchUpTimeout, "catchup-timeout", 5*time.Minute, "Thời gian tối đa chờ target apply hết log của source ở bước 3")
	cmd.Flags().Float64Var(&promoteSyncIntervalSec, "sync-interval", 5, "Chu kỳ đồng bộ sequence Delta t_sync (giây)")
	cmd.Flags().StringVar(&promoteEngine, "engine", "auto", "Loại CSDL target: mysql | pg | auto (tự động nhận diện từ DSN)")
	cmd.Flags().StringVar(&promoteAppRole, "app-role", "app_rw", "Role ứng dụng trên target PostgreSQL: bị chặn ghi đến bước 5 rồi được cấp quyền ghi")
	cmd.Flags().StringSliceVar(&promoteSourceAppRoles, "source-app-roles", nil, "Role ứng dụng trên source PostgreSQL bị ngắt session khi fence (rỗng = mọi session client trừ user của Strand)")
	cmd.Flags().StringSliceVar(&promoteSchemas, "schemas", []string{"public"}, "Danh sách schema PostgreSQL chứa bảng và sequence cần promote")

	return cmd
}
