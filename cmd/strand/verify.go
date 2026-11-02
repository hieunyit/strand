package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"strand/internal/admin"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/verify"
)

// `strand verify --table <t> --mode sample|delta|full --repair report|fix` (02 §10).
func newVerifyCmd() *cobra.Command {
	var table, mode, repair string
	var rate float64
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Đối chiếu source với target theo range (sample | delta); full ở P2",
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
				return exitcode.New("not_available", exitcode.InvalidSyntax, "verify --mode full (Merkle) is a Phase 2 feature", nil)
			default:
				return exitcode.New("invalid_mode", exitcode.InvalidSyntax, "--mode must be sample|delta|full", nil)
			}
			if repair != "report" && repair != "fix" {
				return exitcode.New("invalid_repair", exitcode.InvalidSyntax, "--repair must be report|fix", nil)
			}
			s, t, ok := strings.Cut(table, ".")
			if !ok {
				return exitcode.New("invalid_table", exitcode.InvalidSyntax, "--table must be schema.table", nil)
			}
			id := model.TableID{Schema: s, Table: t}
			src, err := sql.Open("mysql", cfg.Source.DSN)
			if err != nil {
				return err
			}
			defer src.Close()
			dst, err := sql.Open("mysql", cfg.Target.DSN)
			if err != nil {
				return err
			}
			defer dst.Close()
			var pk string
			var npk int
			if err := src.QueryRowContext(cmd.Context(), `SELECT MIN(COLUMN_NAME), COUNT(*) FROM information_schema.KEY_COLUMN_USAGE
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'`, s, t).Scan(&pk, &npk); err != nil || npk != 1 {
				return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, "verify needs a single-column primary key", err)
			}
			target := func(x model.TableID) model.TableID {
				if r, ok := cfg.Tables.Rename[x.String()]; ok {
					if rs, rt, ok := strings.Cut(r, "."); ok {
						return model.TableID{Schema: rs, Table: rt}
					}
				}
				return x
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
				if err := repairRows(cmd.Context(), src, dst, id, target(id), pk, rep.Mismatches); err != nil {
					return err
				}
				fmt.Printf("repaired %d rows from source\n", len(rep.Mismatches))
				return nil
			}
			return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError,
				fmt.Sprintf("%d mismatched rows in %s (pk %s)", len(rep.Mismatches), id, strings.Join(firstN(rep.Mismatches, 10), ",")), nil)
		},
	}
	cmd.Flags().StringVar(&table, "table", "", "Bảng schema.table (sample)")
	cmd.Flags().StringVar(&mode, "mode", "sample", "sample | delta | full")
	cmd.Flags().StringVar(&repair, "repair", "report", "report | fix")
	cmd.Flags().Float64Var(&rate, "rate", 0.01, "Tỉ lệ row lấy mẫu (sample)")
	cmd.Flags().StringVar(&adminSocketPath, "socket", "", "Admin socket (delta)")
	return cmd
}

func firstN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

// repairRows chép lại row lệch từ source (xóa ở target nếu source không còn). Pipeline vẫn
// stream sau đó nên trạng thái hội tụ về source; trong lúc sửa, row có thể tạm lùi rồi tiến lại.
func repairRows(ctx context.Context, src, dst *sql.DB, srcT, dstT model.TableID, pk string, keys []string) error {
	qi := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	qt := func(t model.TableID) string { return qi(t.Schema) + "." + qi(t.Table) }
	for _, k := range keys {
		rows, err := src.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", qt(srcT), qi(pk)), k)
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
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = ?", qt(dstT), qi(pk)), k); err != nil {
			tx.Rollback()
			return err
		}
		if vals != nil {
			q := make([]string, len(cols))
			ph := make([]string, len(cols))
			for i, c := range cols {
				q[i], ph[i] = qi(c), "?"
			}
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qt(dstT), strings.Join(q, ","), strings.Join(ph, ",")), vals...); err != nil {
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
