package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"

	"strand/internal/admin"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/verify"
)

// `strand verify --table <t> --mode sample|delta|full --repair report|fix` (02 §10).
func newVerifyCmd() *cobra.Command {
	var table, mode, repair, replicaDSN string
	var rate float64
	var resume bool
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Đối chiếu source với target theo range (sample | delta | full)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing -c", nil)
			}
			cfg, err := config.LoadFile(cfgPath)
			if err != nil {
				return err
			}
			switch mode {
			case "delta":
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Minute)
				defer cancel()
				msg, err := admin.NewClient(getAdminSocketPath()).VerifyDelta(ctx)
				if err != nil {
					if strings.Contains(err.Error(), exitcode.ErrVerifyMismatch) {
						return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError, err.Error(), err)
					}
					return err
				}
				fmt.Println(msg)
				return nil
			case "sample":
			case "full":
			default:
				return exitcode.New("invalid_mode", exitcode.InvalidSyntax, "--mode must be sample|delta|full", nil)
			}
			if repair != "report" && repair != "fix" {
				return exitcode.New("invalid_repair", exitcode.InvalidSyntax, "--repair must be report|fix", nil)
			}

			srcEngine, dstEngine := engineOf(cfg.Source.Kind), engineOf(cfg.Target.Kind)
			if mode == "sample" && (srcEngine != model.EngineMySQL || dstEngine != model.EngineMySQL) {
				return exitcode.New("invalid_mode", exitcode.InvalidSyntax, "verify --mode sample supports mysql -> mysql only; use --mode full", nil)
			}
			if repair == "fix" && srcEngine != dstEngine {
				return exitcode.New("invalid_repair", exitcode.InvalidSyntax, "--repair fix needs source and target of the same engine", nil)
			}
			// --replica-dsn: đọc phía source từ replica (design §7: full chạy trên replica).
			srcDSN := cfg.Source.DSN
			if replicaDSN != "" {
				if srcEngine == model.EngineMySQL {
					if err := normalizeDSNFlags(&replicaDSN); err != nil {
						return err
					}
				}
				srcDSN = replicaDSN
			}
			src, err := sql.Open(driverOf(srcEngine), srcDSN)
			if err != nil {
				return err
			}
			defer src.Close()
			dst, err := sql.Open(driverOf(dstEngine), cfg.Target.DSN)
			if err != nil {
				return err
			}
			defer dst.Close()

			target := func(x model.TableID) model.TableID {
				if r, ok := cfg.Tables.Rename[x.String()]; ok {
					if rs, rt, ok := strings.Cut(r, "."); ok {
						return model.TableID{Schema: rs, Table: rt}
					}
				}
				return x
			}

			if mode == "full" {
				var ids []model.TableID
				names := cfg.Tables.Include
				if table != "" {
					names = []string{table}
				}
				if len(names) == 0 {
					return exitcode.New("missing_tables", exitcode.InvalidSyntax, "verify --mode full needs --table or tables.include", nil)
				}
				for _, n := range names {
					s, t, ok := strings.Cut(n, ".")
					if !ok || strings.ContainsAny(n, "*?") {
						return exitcode.New("invalid_table", exitcode.InvalidSyntax, fmt.Sprintf("table %q must be schema.table (no wildcard)", n), nil)
					}
					ids = append(ids, model.TableID{Schema: s, Table: t})
				}
				// --rate mặc định 0.01 là tỉ lệ của sample; với full, không đặt thì dùng DefaultFullRate chunk/giây.
				fullRate := verify.DefaultFullRate
				if cmd.Flags().Changed("rate") {
					fullRate = rate
				}
				var bad []string
				for _, id := range ids {
					pk, err := verify.FindSinglePKColumn(cmd.Context(), src, srcEngine, id)
					if err != nil {
						return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, "verify needs a single-column primary key: "+err.Error(), err)
					}
					var lv *verify.LiveVerifier
					if srcEngine == model.EngineMySQL && dstEngine == model.EngineMySQL {
						// Chạy độc lập, không có pipeline ghi _strand_checkpoint: không chờ target apply.
						lv = verify.NewLiveVerifier(verify.LiveConfig{Source: src, Target: dst, Meta: dst, PipelineID: cfg.Name,
							TargetName: target, SkipApplyWait: true})
					}
					mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{
						Source: src, Target: dst, Meta: dst, PipelineID: cfg.Name,
						SourceEngine: srcEngine, TargetEngine: dstEngine, TargetName: target,
						ChunkRows: verify.DefaultChunkRows, Rate: fullRate, Resume: resume, LiveVerifier: lv,
					})
					rep, err := mv.VerifyTableFull(cmd.Context(), id, pk)
					if err != nil {
						return err
					}
					fmt.Printf("verify %s (full): %d chunks (%d hashed, %d resumed), %d mismatched chunks, %d rows compared, %d mismatches, %d unverifiable_hot\n",
						id, rep.TotalChunks, rep.HashedChunks, rep.ResumedChunks, rep.MismatchedChunks, rep.RowsChecked, len(rep.Mismatches), len(rep.UnverifiableHot))
					if len(rep.Mismatches) == 0 {
						continue
					}
					if repair == "fix" && !dryRun {
						if err := repairRows(cmd.Context(), src, dst, srcEngine, dstEngine, id, target(id), pk, rep.Mismatches); err != nil {
							return err
						}
						fmt.Printf("repaired %d rows of %s from source\n", len(rep.Mismatches), id)
						continue
					}
					bad = append(bad, fmt.Sprintf("%d mismatched rows in %s (pk %s)", len(rep.Mismatches), id, strings.Join(firstN(rep.Mismatches, 10), ",")))
				}
				if len(bad) > 0 {
					return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError, strings.Join(bad, "; "), nil)
				}
				return nil
			}
			// Chế độ sample (mặc định của Phase 1)
			s, t, ok := strings.Cut(table, ".")
			if !ok {
				return exitcode.New("invalid_table", exitcode.InvalidSyntax, "--table must be schema.table", nil)
			}
			id := model.TableID{Schema: s, Table: t}

			pk, err := verify.FindSinglePKColumn(cmd.Context(), src, srcEngine, id)
			if err != nil {
				return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, "verify needs a single-column primary key", err)
			}

			lv := verify.NewLiveVerifier(verify.LiveConfig{Source: src, Target: dst, Meta: dst, PipelineID: cfg.Name, TargetName: target})
			ranges, err := verify.SampleRanges(cmd.Context(), src, id, pk, rate, 1000, rand.New(rand.NewSource(time.Now().UnixNano())))
			if err != nil {
				return err
			}
			rep, err := lv.VerifyRanges(cmd.Context(), id, pk, ranges)
			if err != nil {
				return err
			}
			fmt.Printf("verify %s (sample %.2f%%): %d ranges, %d rows, %d mismatches, %d unverifiable_hot\n",
				id, rate*100, rep.RangesChecked, rep.RowsChecked, len(rep.Mismatches), len(rep.UnverifiableHot))
			if len(rep.Mismatches) == 0 {
				return nil
			}
			if repair == "fix" && !dryRun {
				if err := repairRows(cmd.Context(), src, dst, srcEngine, dstEngine, id, target(id), pk, rep.Mismatches); err != nil {
					return err
				}
				fmt.Printf("repaired %d rows from source\n", len(rep.Mismatches))
				return nil
			}
			return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError,
				fmt.Sprintf("%d mismatched rows in %s (pk %s)", len(rep.Mismatches), id, strings.Join(firstN(rep.Mismatches, 10), ",")), nil)
		},
	}
	cmd.Flags().StringVar(&table, "table", "", "Bảng schema.table (mặc định tất cả bảng của pipeline khi mode=full)")
	cmd.Flags().StringVar(&mode, "mode", "sample", "sample | delta | full")
	cmd.Flags().StringVar(&repair, "repair", "report", "report | fix")
	cmd.Flags().Float64Var(&rate, "rate", 0.01, "Tỉ lệ row lấy mẫu (sample) hoặc số chunk/giây (full)")
	cmd.Flags().BoolVar(&resume, "resume", false, "Chạy tiếp một lượt full verify bị ngắt: chunk đã hash khớp ở lượt đó không hash lại (lượt chạy hết thì tiến độ bị xóa)")
	cmd.Flags().StringVar(&replicaDSN, "replica-dsn", "", "DSN của replica để đọc (thay cho source DSN)")
	cmd.Flags().StringVar(&adminSocketPath, "socket", "", "Admin socket (delta)")
	return cmd
}

// engineOf đổi kind trong cấu hình (mysql | postgres) sang model.Engine.
func engineOf(kind string) model.Engine {
	if kind == "postgres" || kind == string(model.EnginePG) {
		return model.EnginePG
	}
	return model.EngineMySQL
}

func driverOf(e model.Engine) string {
	if e == model.EnginePG {
		return "pgx"
	}
	return "mysql"
}

func firstN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

// repairRows chép lại row lệch từ source (xóa ở target nếu source không còn). Pipeline vẫn
// stream sau đó nên trạng thái hội tụ về source; trong lúc sửa, row có thể tạm lùi rồi tiến lại.
func repairRows(ctx context.Context, src, dst *sql.DB, srcEngine, dstEngine model.Engine, srcT, dstT model.TableID, pk string, keys []string) error {
	srcQPK := verify.QuoteIdent(srcEngine, pk)
	srcQT := verify.QuoteTable(srcEngine, srcT)
	dstQPK := verify.QuoteIdent(dstEngine, pk)
	dstQT := verify.QuoteTable(dstEngine, dstT)

	for _, k := range keys {
		srcQ := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", srcQT, srcQPK)
		if srcEngine == model.EnginePG {
			srcQ = fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", srcQT, srcQPK)
		}
		rows, err := src.QueryContext(ctx, srcQ, k)
		if err != nil {
			return err
		}
		cols, _ := rows.Columns()
		var vals []any
		if rows.Next() {
			raw := make([]sql.RawBytes, len(cols))
			ptrs := make([]any, len(cols))
			for i := range raw {
				ptrs[i] = &raw[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return err
			}
			for _, r := range raw {
				if r == nil {
					vals = append(vals, nil)
				} else {
					vals = append(vals, string(r))
				}
			}
		}
		rows.Close()
		tx, err := dst.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		delQ := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", dstQT, dstQPK)
		if dstEngine == model.EnginePG {
			delQ = fmt.Sprintf("DELETE FROM %s WHERE %s = $1", dstQT, dstQPK)
		}
		if _, err := tx.ExecContext(ctx, delQ, k); err != nil {
			tx.Rollback()
			return err
		}
		if vals != nil {
			q := make([]string, len(cols))
			ph := make([]string, len(cols))
			for i, c := range cols {
				q[i] = verify.QuoteIdent(dstEngine, c)
				if dstEngine == model.EnginePG {
					ph[i] = fmt.Sprintf("$%d", i+1)
				} else {
					ph[i] = "?"
				}
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", dstQT, strings.Join(q, ","), strings.Join(ph, ",")), vals...); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
