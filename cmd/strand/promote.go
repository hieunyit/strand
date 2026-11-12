package main

import (
	"context"
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

	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/objects"
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
	promoteFenceTimeout     time.Duration
	promoteRename           map[string]string
	promoteObjectsPost      bool
	promoteObjectsFrom      string
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

			// tables.rename từ cấu hình pipeline (nếu có -c): ánh xạ bảng nguồn → target cho counter AUTO_INCREMENT.
			if cfgPath != "" {
				c, err := config.LoadFile(cfgPath)
				if err != nil {
					return err
				}
				promoteRename = c.Tables.Rename
			}
			var seqOverrides map[string]int64
			if promoteOverrideSeqsJSON != "" {
				if err := json.Unmarshal([]byte(promoteOverrideSeqsJSON), &seqOverrides); err != nil {
					return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --override-sequences JSON: %v", err), err)
				}
			}

			// --objects-post: nạp định nghĩa trigger/event TRƯỚC khi đụng tới bất kỳ DB nào (đọc source không được thì dừng sớm).
			var objectsSet *objects.Set
			if promoteObjectsPost {
				if promoteEngine == "pg" || promoteEngine == "postgres" || promoteEngine == "postgresql" || isPGDSN(promoteTargetDSN) {
					return exitcode.New(errObjectsUnsupported, exitcode.Rejected, "--objects-post supports only a MySQL target", nil)
				}
				ocfg, err := objectsConfig()
				if err != nil {
					return fmt.Errorf("--objects-post needs the pipeline config (-c): %w", err)
				}
				if promoteObjectsFrom == "" && promoteFenceSourceDSN != "" {
					objSourceDSN = promoteFenceSourceDSN // --fence là DSN có quyền hơn user Strand
				}
				if objectsSet, err = loadObjects(cmd.Context(), ocfg, promoteObjectsFrom, false); err != nil {
					return fmt.Errorf("--objects-post: read objects: %w", err)
				}
				c := objectsSet.Counts()
				fmt.Printf("objects post phase: %d trigger, %d event to enable at cutover\n", c[objects.KindTrigger], countEnable(objectsSet))
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
			var objectsHook func(ctx context.Context) error

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
				var pgSourceDB *sql.DB
				if promoteFenceSourceDSN != "" {
					if isPGDSN(promoteFenceSourceDSN) {
						sourceDB, err := sql.Open("pgx", promoteFenceSourceDSN)
						if err != nil {
							return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --fence DSN: %v", err), err)
						}
						defer sourceDB.Close()
						pgSourceDB = sourceDB
						sourceDriver = promote.NewPGSourceDriver(sourceDB, promoteSourceAppRoles...)
						if len(promoteSourceAppRoles) == 0 {
							// review R06: không biết role ứng dụng thì fence chỉ là default_transaction_read_only
							// (role thường tự đặt lại được) + ngắt session — chặn hợp tác, không phải hàng rào cứng.
							logger.Warn("source fence is advisory without --source-app-roles: sessions are terminated and default_transaction_read_only is set, " +
								"but a role can override it; pass --source-app-roles to also set those roles NOLOGIN")
						}
						pgTargetDriver.WithCatchUpCheck(sourceDB, promotePipelineID, promoteCatchUpTimeout, logger)
					} else {
						sourceDB, err := sql.Open("mysql", promoteFenceSourceDSN)
						if err != nil {
							return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, fmt.Sprintf("invalid --fence DSN: %v", err), err)
						}
						defer sourceDB.Close()
						mySrc := promote.NewMySQLSourceDriver(sourceDB).WithAppUsers(promoteSourceAppRoles...).WithRename(promoteRename).
							WithFenceTimeout(promoteFenceTimeout).WithLogger(logger)
						defer mySrc.ReleaseFence()
						sourceDriver = mySrc
						pgTargetDriver.WithSourceEngine(model.EngineMySQL).
							WithCatchUpCheck(sourceDB, promotePipelineID, promoteCatchUpTimeout, logger)
					}
				}

				// Khám phá toàn bộ sequences và identity trên PostgreSQL target
				seqs, err := promote.DiscoverPGSequences(cmd.Context(), targetDB, promoteSchemas)
				if err != nil {
					return fmt.Errorf("discover PG sequences: %w", err)
				}
				// Margin theo CACHE của source (02 §8): giá trị session ở source đã giữ trong cache là thứ có
				// thể chưa xuất hiện ở target. Source PG đọc được thì lấy max(CACHE source, CACHE target);
				// không đọc được thì dùng CACHE target, và promoter còn lấy max với CACHE đã lưu bởi đồng bộ
				// định kỳ (đọc từ source).
				if pgSourceDB != nil {
					dctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second) // source có thể đã chết (failover)
					srcSeqs, err := promote.DiscoverPGSequences(dctx, pgSourceDB, promoteSchemas)
					cancel()
					if err != nil {
						logger.Warn("cannot read sequence CACHE from the source; margin uses the target CACHE", "error", err)
					} else {
						seqs = promote.MergeSourceCache(seqs, srcSeqs)
					}
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
					mySrc := promote.NewMySQLSourceDriver(sourceDB).WithAppUsers(promoteSourceAppRoles...).WithRename(promoteRename).
						WithFenceTimeout(promoteFenceTimeout).WithLogger(logger)
					defer mySrc.ReleaseFence()
					sourceDriver = mySrc
					myTargetDriver.WithCatchUpCheck(sourceDB, promotePipelineID, promoteCatchUpTimeout, logger)
				}
				sequences = nil
				if objectsSet != nil {
					// Chạy trước khi mở ghi: tạo trigger + bật event trên target (đang read_only; user có SUPER vẫn tạo được).
					hookDB := targetDB
					objectsHook = func(ctx context.Context) error {
						if dryRun {
							return nil
						}
						// Tắt event của source ở đây (sau fence + catch-up), không phải lúc promote mới bắt đầu: promote
						// lỗi trước fence thì source vẫn phục vụ ứng dụng và không được mất event.
						stopSourceEvents(ctx, logger)
						var sb strings.Builder
						err := runApply(ctx, hookDB, objectsSet, objects.PhasePost, false, &sb)
						fmt.Print(sb.String())
						if err != nil {
							return fmt.Errorf("objects post phase (triggers + enable events): %v; fix the cause then re-run the same promote command (it resumes before opening writes; apply is idempotent)", err)
						}
						return nil
					}
				}
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
			if objectsHook != nil {
				p.WithBeforeOpenWrites(objectsHook)
			}

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
						for _, seqName := range promote.SortedSequenceNames(progress.SuggestedSequences) {
							fmt.Printf("  - %s: gợi ý %d (lần sync cuối hoặc max cột target, + margin)\n", seqName, progress.SuggestedSequences[seqName])
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
			if len(promoteFenceSourceDSN) > 0 && !isPGDSN(promoteFenceSourceDSN) {
				fmt.Println("Lưu ý nguồn không có super_read_only (MySQL 5.5/5.6/5.7<5.7.8, MariaDB; fence bằng read_only + FLUSH TABLES WITH READ LOCK): khóa FTWRL nhả khi lệnh này thoát, " +
					"read_only=ON vẫn giữ nhưng KHÔNG chặn user SUPER. Giữ source tắt hoặc read_only và cắt kết nối ứng dụng cho tới khi cut-over xong.")
			}
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
	cmd.Flags().StringSliceVar(&promoteSourceAppRoles, "source-app-roles", nil, "Role (PG) / user (MySQL) ứng dụng trên source: MySQL kill session của user này sau khi giữ FTWRL. PG: fence đặt NOLOGIN rồi ngắt session (rỗng = chỉ ngắt mọi session client trừ user của Strand, fence mang tính hợp tác)")
	cmd.Flags().DurationVar(&promoteFenceTimeout, "fence-timeout", promote.DefaultFenceTimeout, "Thời gian tối đa chờ FLUSH TABLES WITH READ LOCK và chờ session ứng dụng thoát khi fence source MySQL không có super_read_only (MySQL 5.5/5.6/5.7<5.7.8, MariaDB)")
	cmd.Flags().BoolVar(&promoteObjectsPost, "objects-post", false, "MySQL target: trước khi mở ghi, tạo trigger và bật event (strand objects apply --phase post); cần -c. Tắt event_scheduler trên source khi fence. Lỗi thì dừng, target vẫn read-only")
	cmd.Flags().StringVar(&promoteObjectsFrom, "objects-from", "", "Với --objects-post: đọc định nghĩa từ thư mục strand objects export (khi source không còn truy cập được)")
	cmd.Flags().StringSliceVar(&promoteSchemas, "schemas", []string{"public"}, "Danh sách schema PostgreSQL chứa bảng và sequence cần promote")

	return cmd
}

func countEnable(s *objects.Set) int {
	n := 0
	for _, o := range s.Of(objects.KindEvent) {
		if o.EnableStmt != "" && o.Skipped == "" {
			n++
		}
	}
	return n
}

// stopSourceEvents tắt event scheduler của source (đã fence) để event không còn ghi vào source cũ
// (SET GLOBAL không ghi bảng, không vào binlog, chạy được cả dưới FTWRL). Lỗi chỉ cảnh báo: fence vẫn chặn ghi.
func stopSourceEvents(ctx context.Context, logger *slog.Logger) {
	if promoteFenceSourceDSN == "" || isPGDSN(promoteFenceSourceDSN) {
		return
	}
	db, err := sql.Open("mysql", promoteFenceSourceDSN)
	if err != nil {
		logger.Warn("cannot disable the source event scheduler", "error", err)
		return
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "SET GLOBAL event_scheduler = OFF"); err != nil {
		logger.Warn("cannot disable the source event scheduler (the fence still blocks writes)", "error", err)
	}
}
