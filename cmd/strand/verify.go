package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"

	"strand/internal/admin"
	"strand/internal/checkpoint"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/snapshot"
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

			// --repair fix chỉ chạy khi pipeline đã dừng, và giữ khóa pipeline suốt lúc verify + sửa
			// (review R04): nếu CDC chạy song song, repair có thể đọc ảnh source cũ, CDC ghi ảnh mới +
			// checkpoint, rồi repair ghi đè ảnh cũ — checkpoint không lùi nên không gì chữa lại. Khi
			// pipeline dừng, mọi thay đổi source sau lần đọc của repair nằm sau checkpoint và được phát lại
			// khi pipeline chạy tiếp, nên target hội tụ về source.
			fixing := repair == "fix" && !dryRun
			if fixing {
				release, err := lockPipelineForRepair(cmd.Context(), dst, dstEngine, cfg.Name)
				if err != nil {
					return err
				}
				defer release()
			}

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
					pkCols, err := verify.FindPKColumns(cmd.Context(), src, srcEngine, id)
					if err != nil {
						return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, "verify needs a primary key: "+err.Error(), err)
					}
					intPK := false
					if len(pkCols) == 1 {
						if intPK, err = verify.IsIntegerPK(cmd.Context(), src, srcEngine, id, pkCols[0]); err != nil {
							return err
						}
					}
					if !intPK {
						// Việc C: PK nhiều cột / không phải số nguyên — full verify theo tuple.
						rep, err := verifyFullKeys(cmd, src, dst, srcEngine, dstEngine, id, target(id), pkCols, fullRate)
						if err != nil {
							return err
						}
						fmt.Printf("verify %s (full, key %s): %d chunks, %d mismatched chunks, %d rows compared, %d mismatches\n",
							id, strings.Join(pkCols, ","), rep.TotalChunks, rep.MismatchedChunks, rep.RowsChecked, len(rep.Mismatches))
						if len(rep.Mismatches) == 0 {
							continue
						}
						if repair == "fix" && !dryRun {
							if srcEngine != model.EngineMySQL {
								return exitcode.New("invalid_repair", exitcode.InvalidSyntax,
									fmt.Sprintf("--repair fix of %s (primary key %s) is supported from MySQL to MySQL only; %d mismatched rows",
										id, strings.Join(pkCols, ","), len(rep.Mismatches)), nil)
							}
							if err := repairKeysMySQL(cmd.Context(), src, dst, id, target(id), pkCols, rep.MismatchKeys); err != nil {
								return err
							}
							fmt.Printf("repaired %d rows of %s from source\n", len(rep.MismatchKeys), id)
							continue
						}
						bad = append(bad, fmt.Sprintf("%d mismatched rows in %s (key %s)", len(rep.Mismatches), id, strings.Join(firstN(rep.Mismatches, 10), "; ")))
						continue
					}
					pk := pkCols[0]
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

			pkCols, err := verify.FindPKColumns(cmd.Context(), src, srcEngine, id)
			if err != nil {
				return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, "verify needs a primary key", err)
			}
			intPK := false
			if len(pkCols) == 1 {
				if intPK, err = verify.IsIntegerPK(cmd.Context(), src, srcEngine, id, pkCols[0]); err != nil {
					return err
				}
			}
			pk := pkCols[0]

			// Đang giữ khóa để sửa: pipeline không chạy, không chờ target apply (sẽ không bao giờ tới).
			lv := verify.NewLiveVerifier(verify.LiveConfig{Source: src, Target: dst, Meta: dst, PipelineID: cfg.Name, TargetName: target, SkipApplyWait: fixing})
			if !intPK {
				// Việc C: PK nhiều cột / không phải số nguyên — sample theo tuple PK.
				codec, err := snapshot.MySQLPKCodec(cmd.Context(), src, id, pkCols)
				if err != nil {
					return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected, "verify: "+err.Error(), err)
				}
				dc, err := snapshot.MySQLPKCodec(cmd.Context(), dst, target(id), pkCols)
				if err == nil {
					err = codec.SameOrder(dc)
				}
				if err != nil {
					return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected, "verify: target primary key: "+err.Error(), err)
				}
				ranges, err := verify.SampleKeyRanges(cmd.Context(), src, id, codec, rate, 1000, rand.New(rand.NewSource(time.Now().UnixNano())))
				if err != nil {
					return err
				}
				rep, err := lv.VerifyKeyRanges(cmd.Context(), id, codec, ranges)
				if err != nil {
					return err
				}
				fmt.Printf("verify %s (sample %.2f%%, key %s): %d ranges, %d rows, %d mismatches, %d unverifiable_hot\n",
					id, rate*100, strings.Join(pkCols, ","), rep.RangesChecked, rep.RowsChecked, len(rep.Mismatches), len(rep.UnverifiableHot))
				if len(rep.Mismatches) == 0 {
					return nil
				}
				if repair == "fix" && !dryRun {
					if err := repairKeysMySQL(cmd.Context(), src, dst, id, target(id), pkCols, rep.MismatchKeys); err != nil {
						return err
					}
					fmt.Printf("repaired %d rows from source\n", len(rep.MismatchKeys))
					return nil
				}
				return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError,
					fmt.Sprintf("%d mismatched rows in %s (key %s)", len(rep.Mismatches), id, strings.Join(firstN(rep.Mismatches, 10), "; ")), nil)
			}
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

// verifyFullKeys: full verify theo tuple PK (việc C) sau khi kiểm hai phía cùng thứ tự khóa.
func verifyFullKeys(cmd *cobra.Command, src, dst *sql.DB, srcEngine, dstEngine model.Engine, id, dstID model.TableID,
	pkCols []string, rate float64) (*verify.FullVerifyReport, error) {
	ctx := cmd.Context()
	codecOf := func(db *sql.DB, engine model.Engine, t model.TableID) (*snapshot.PKCodec, error) {
		if engine == model.EnginePG {
			return snapshot.PGPKCodec(ctx, db, t, pkCols)
		}
		return snapshot.MySQLPKCodec(ctx, db, t, pkCols)
	}
	reject := func(why string) error {
		return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected, fmt.Sprintf("full verify of %s: %s", id, why), nil)
	}
	sc, err := codecOf(src, srcEngine, id)
	if err != nil {
		return nil, reject(err.Error())
	}
	dc, err := codecOf(dst, dstEngine, dstID)
	if err != nil {
		return nil, reject(err.Error())
	}
	if srcEngine == dstEngine {
		err = sc.SameOrder(dc)
	} else {
		err = snapshot.CrossEngineSameOrder(sc, dc)
	}
	if err != nil {
		return nil, reject("source and target order the primary key differently: " + err.Error())
	}
	mv := verify.NewMerkleVerifier(verify.FullVerifyConfig{Source: src, Target: dst, Meta: dst,
		SourceEngine: srcEngine, TargetEngine: dstEngine, TargetName: func(model.TableID) model.TableID { return dstID },
		ChunkRows: verify.DefaultChunkRows, Rate: rate})
	return mv.VerifyTableFullKeys(ctx, id, sc, dc)
}

// repairKeysMySQL: repair của PK tổng quát (việc C), MySQL→MySQL — như repairRows: đọc ảnh source của
// mọi khóa lệch rồi, trong MỘT transaction với FOREIGN_KEY_CHECKS=0, xóa các khóa đó ở target và chèn lại
// ảnh source (bỏ cột generated). Gọi khi đang giữ khóa pipeline.
func repairKeysMySQL(ctx context.Context, src, dst *sql.DB, srcT, dstT model.TableID, pkCols []string, keys []snapshot.Bound) error {
	codec, err := snapshot.MySQLPKCodec(ctx, src, srcT, pkCols)
	if err != nil {
		return err
	}
	skip, _, err := targetWriteExclusions(ctx, dst, model.EngineMySQL, dstT)
	if err != nil {
		return err
	}
	sq := snapshot.MySQLKeySQL{Table: srcT, Codec: codec}
	dq := snapshot.MySQLKeySQL{Table: dstT, Codec: codec}
	type img struct {
		del   string
		dargs []any
		names []string
		vals  []any
	}
	var imgs []img
	for _, k := range keys {
		w, a, err := sq.Cmp("=", k)
		if err != nil {
			return err
		}
		rows, err := src.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s WHERE %s", verify.QuoteTable(model.EngineMySQL, srcT), w), a...)
		if err != nil {
			return err
		}
		cols, _ := rows.Columns()
		var it img
		if it.del, it.dargs, err = dq.Cmp("=", k); err != nil {
			rows.Close()
			return err
		}
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
			for i, r := range raw {
				if skip[cols[i]] {
					continue
				}
				it.names = append(it.names, verify.QuoteIdent(model.EngineMySQL, cols[i]))
				if r == nil {
					it.vals = append(it.vals, nil)
				} else {
					it.vals = append(it.vals, string(r))
				}
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		imgs = append(imgs, it)
	}
	conn, err := dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var prev int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.foreign_key_checks").Scan(&prev); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), fmt.Sprintf("SET SESSION foreign_key_checks = %d", prev))
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	qt := verify.QuoteTable(model.EngineMySQL, dstT)
	for _, it := range imgs {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+qt+" WHERE "+it.del, it.dargs...); err != nil {
			return fmt.Errorf("repair %s: %w", dstT, err)
		}
	}
	for _, it := range imgs {
		if it.names == nil {
			continue
		}
		ph := strings.TrimSuffix(strings.Repeat("?,", len(it.names)), ",")
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qt, strings.Join(it.names, ","), ph), it.vals...); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("repair %s: %w — the source changed while repairing: let the pipeline catch up, stop it and repair again", dstT, err)
			}
			return fmt.Errorf("repair %s: %w", dstT, err)
		}
	}
	return tx.Commit()
}

// lockPipelineForRepair lấy khóa advisory của pipeline trên target (cùng khóa `strand sync` giữ).
func lockPipelineForRepair(ctx context.Context, dst *sql.DB, engine model.Engine, pipelineID string) (func() error, error) {
	var lock func(context.Context, string) (func() error, error)
	if engine == model.EnginePG {
		lock = checkpoint.NewPGStore(dst, pipelineID, engine).Lock
	} else {
		lock = checkpoint.NewMySQLStore(dst, pipelineID, engine).Lock
	}
	release, err := lock(ctx, pipelineID)
	if errors.Is(err, checkpoint.ErrPipelineLocked) {
		return nil, exitcode.New(exitcode.ErrPipelineLocked, exitcode.Fenced, fmt.Sprintf(
			"pipeline %s is running: stop `strand sync` before --repair fix (a repair concurrent with the stream can overwrite newer, already checkpointed rows with a stale image); "+
				"use --repair report while it runs", pipelineID), err)
	}
	return release, err
}

// repairRows chép lại row lệch từ source: upsert ảnh source, hoặc xóa ở target nếu source không còn
// row. Gọi khi đang giữ khóa pipeline (lockPipelineForRepair). Không DELETE rồi INSERT row còn tồn tại:
// với FK ON DELETE CASCADE/SET NULL ở target, DELETE row cha xóa/sửa row con mà không chép lại. Cột
// generated ở target không được ghi; cột identity ALWAYS (PG) ghi bằng OVERRIDING SYSTEM VALUE.
func repairRows(ctx context.Context, src, dst *sql.DB, srcEngine, dstEngine model.Engine, srcT, dstT model.TableID, pk string, keys []string) error {
	srcQPK := verify.QuoteIdent(srcEngine, pk)
	srcQT := verify.QuoteTable(srcEngine, srcT)
	skip, identityAlways, err := targetWriteExclusions(ctx, dst, dstEngine, dstT)
	if err != nil {
		return err
	}
	srcQ := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", srcQT, srcQPK)
	if srcEngine == model.EnginePG {
		srcQ = fmt.Sprintf("SELECT * FROM %s WHERE %s = $1", srcQT, srcQPK)
	}
	// 1. Ảnh source của mọi khóa lệch (nil = source không còn row).
	imgs := make([]repairImage, 0, len(keys))
	for _, k := range keys {
		img := repairImage{key: k}
		rows, err := src.QueryContext(ctx, srcQ, k)
		if err != nil {
			return err
		}
		cols, _ := rows.Columns()
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
			for i, r := range raw {
				if skip[cols[i]] {
					continue
				}
				img.names = append(img.names, cols[i])
				if r == nil {
					img.vals = append(img.vals, nil)
				} else {
					img.vals = append(img.vals, string(r))
				}
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		imgs = append(imgs, img)
	}
	// 2. Ghi mọi khóa lệch trong MỘT transaction: xóa rồi chèn lại ảnh source. Sửa từng row không được
	// khi các row lệch đổi chéo giá trị UNIQUE với nhau (target tụt sau source: row nào sửa trước cũng
	// va UNIQUE của row chưa sửa — e2e P1-15). Không cascade sang bảng con: MySQL foreign_key_checks=0,
	// PG session_replication_role=replica (cần quyền; không có thì sửa từng row như trước).
	// Trùng UNIQUE với row không nằm trong danh sách lệch chỉ xảy ra khi source đổi giữa lúc so và lúc
	// sửa (source vẫn ghi trong khi pipeline dừng): báo rõ cách làm thay vì chỉ trả 1062.
	const batch = 20000
	for lo := 0; lo < len(imgs); lo += batch {
		hi := min(lo+batch, len(imgs))
		var ok bool
		if dstEngine == model.EnginePG {
			ok, err = repairBulkPG(ctx, dst, dstT, pk, identityAlways, imgs[lo:hi])
		} else {
			ok, err = true, repairBulkMySQL(ctx, dst, dstT, pk, imgs[lo:hi])
		}
		if err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w — the source changed while repairing (writes continue while the pipeline is stopped): "+
					"start the pipeline, let the target catch up, stop it and run --repair fix again", err)
			}
			return err
		}
		if !ok {
			if err := repairOneByOne(ctx, dst, dstEngine, dstT, pk, identityAlways, imgs[lo:hi]); err != nil {
				return err
			}
		}
	}
	return nil
}

// isUniqueViolation: MySQL 1062 hoặc PG 23505.
func isUniqueViolation(err error) bool {
	var me *drivermysql.MySQLError
	if errors.As(err, &me) && me.Number == 1062 {
		return true
	}
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

// repairImage: ảnh source của một khóa lệch (names == nil: source không còn row).
type repairImage struct {
	key   string
	names []string
	vals  []any
}

func repairBulkMySQL(ctx context.Context, dst *sql.DB, t model.TableID, pk string, imgs []repairImage) error {
	conn, err := dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var prev int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.foreign_key_checks").Scan(&prev); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), fmt.Sprintf("SET SESSION foreign_key_checks = %d", prev))
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	qt, qpk := verify.QuoteTable(model.EngineMySQL, t), verify.QuoteIdent(model.EngineMySQL, pk)
	for _, img := range imgs {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = ?", qt, qpk), img.key); err != nil {
			return fmt.Errorf("repair %s pk %s: %w", t, img.key, err)
		}
	}
	for _, img := range imgs {
		if img.names == nil {
			continue
		}
		q := make([]string, len(img.names))
		for i, c := range img.names {
			q[i] = verify.QuoteIdent(model.EngineMySQL, c)
		}
		ph := strings.TrimSuffix(strings.Repeat("?,", len(q)), ",")
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qt, strings.Join(q, ","), ph), img.vals...); err != nil {
			return fmt.Errorf("repair %s pk %s: %w", t, img.key, err)
		}
	}
	return tx.Commit()
}

// repairBulkPG: ok=false (không lỗi) khi phiên không được đặt session_replication_role.
func repairBulkPG(ctx context.Context, dst *sql.DB, t model.TableID, pk string, identityAlways bool, imgs []repairImage) (bool, error) {
	tx, err := dst.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		return false, nil
	}
	qt, qpk := verify.QuoteTable(model.EnginePG, t), verify.QuoteIdent(model.EnginePG, pk)
	for _, img := range imgs {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = $1", qt, qpk), img.key); err != nil {
			return false, fmt.Errorf("repair %s pk %s: %w", t, img.key, err)
		}
	}
	over := ""
	if identityAlways {
		over = " OVERRIDING SYSTEM VALUE"
	}
	for _, img := range imgs {
		if img.names == nil {
			continue
		}
		q := make([]string, len(img.names))
		ph := make([]string, len(img.names))
		for i, c := range img.names {
			q[i], ph[i] = verify.QuoteIdent(model.EnginePG, c), fmt.Sprintf("$%d", i+1)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s)%s VALUES (%s)", qt, strings.Join(q, ","), over, strings.Join(ph, ",")), img.vals...); err != nil {
			return false, fmt.Errorf("repair %s pk %s: %w", t, img.key, err)
		}
	}
	return true, tx.Commit()
}

// repairOneByOne: sửa từng khóa tại chỗ (PG ON CONFLICT (pk); MySQL khóa row rồi UPDATE/INSERT) — đường
// dự phòng khi không tắt được cascade cho cả lô.
func repairOneByOne(ctx context.Context, dst *sql.DB, dstEngine model.Engine, dstT model.TableID, pk string, identityAlways bool, imgs []repairImage) error {
	dstQPK := verify.QuoteIdent(dstEngine, pk)
	dstQT := verify.QuoteTable(dstEngine, dstT)
	ph := func(i int) string {
		if dstEngine == model.EnginePG {
			return fmt.Sprintf("$%d", i)
		}
		return "?"
	}
	for _, img := range imgs {
		k, names, vals := img.key, img.names, img.vals
		if names == nil {
			delQ := fmt.Sprintf("DELETE FROM %s WHERE %s = %s", dstQT, dstQPK, ph(1))
			if _, err := dst.ExecContext(ctx, delQ, k); err != nil {
				return err
			}
			continue
		}
		q := make([]string, len(names))
		phs := make([]string, len(names))
		var set []string
		for i, c := range names {
			q[i] = verify.QuoteIdent(dstEngine, c)
			phs[i] = ph(i + 1)
			if c == pk {
				continue
			}
			set = append(set, fmt.Sprintf("%s = EXCLUDED.%s", q[i], q[i]))
		}
		if dstEngine == model.EnginePG {
			over := ""
			if identityAlways {
				over = " OVERRIDING SYSTEM VALUE"
			}
			action := "DO NOTHING"
			if len(set) > 0 {
				action = "DO UPDATE SET " + strings.Join(set, ", ")
			}
			// ON CONFLICT (pk): chỉ xung đột PK được gộp; trùng unique khác báo lỗi.
			stmt := fmt.Sprintf("INSERT INTO %s (%s)%s VALUES (%s) ON CONFLICT (%s) %s", dstQT, strings.Join(q, ","), over, strings.Join(phs, ","), dstQPK, action)
			if _, err := dst.ExecContext(ctx, stmt, vals...); err != nil {
				return fmt.Errorf("repair %s pk %s: %w", dstT, k, err)
			}
			continue
		}
		// MySQL: không dùng ON DUPLICATE KEY UPDATE — nó cũng kích hoạt khi trùng UNIQUE phụ và ghi đè
		// row KHÁC (cùng lỗi của review R01). Khóa row theo PK rồi UPDATE hoặc INSERT; trùng unique báo lỗi.
		if err := repairMySQLRow(ctx, dst, dstQT, dstQPK, k, q, names, pk, vals); err != nil {
			return fmt.Errorf("repair %s pk %s: %w", dstT, k, err)
		}
	}
	return nil
}

// repairMySQLRow ghi ảnh source của một PK lên target MySQL trong một transaction.
func repairMySQLRow(ctx context.Context, dst *sql.DB, qt, qpk, k string, qcols, names []string, pk string, vals []any) error {
	tx, err := dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var one int
	err = tx.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE %s = ? FOR UPDATE", qt, qpk), k).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		ph := strings.TrimSuffix(strings.Repeat("?,", len(qcols)), ",")
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qt, strings.Join(qcols, ","), ph), vals...); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		var set []string
		var args []any
		for i, c := range names {
			if c == pk {
				continue
			}
			set = append(set, qcols[i]+" = ?")
			args = append(args, vals[i])
		}
		if len(set) > 0 {
			args = append(args, k)
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", qt, strings.Join(set, ", "), qpk), args...); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// targetWriteExclusions trả cột generated của bảng đích (không ghi được) và có cột identity ALWAYS không.
func targetWriteExclusions(ctx context.Context, dst *sql.DB, engine model.Engine, t model.TableID) (map[string]bool, bool, error) {
	skip := map[string]bool{}
	var q string
	if engine == model.EnginePG {
		q = `SELECT a.attname, a.attgenerated <> '', a.attidentity = 'a' FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped`
	} else {
		q = `SELECT COLUMN_NAME, GENERATION_EXPRESSION <> '', FALSE FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	}
	rows, err := dst.QueryContext(ctx, q, t.Schema, t.Table)
	if err != nil {
		return nil, false, fmt.Errorf("read target columns of %s: %w", t, err)
	}
	defer rows.Close()
	identity := false
	for rows.Next() {
		var c string
		var gen, ident bool
		if err := rows.Scan(&c, &gen, &ident); err != nil {
			return nil, false, err
		}
		if gen {
			skip[c] = true
		}
		identity = identity || ident
	}
	return skip, identity, rows.Err()
}
