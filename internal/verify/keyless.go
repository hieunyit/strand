package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/model"
)

// Bảng keyless (tables.keyless=full_row) không có khóa để chia range hay đối soát từng hàng: verify chỉ so
// SỐ HÀNG và CHECKSUM không phụ thuộc thứ tự (tổng 60 bit đầu MD5 của dạng chính tắc từng hàng, như HashChunk).
// Không chỉ ra được hàng nào lệch và không sửa (repair) được: lệch = nạp lại bảng từ seed.

// KeylessReport là kết quả verify một bảng keyless.
type KeylessReport struct {
	Table          string
	SourceRows     string // "số_row:checksum"
	TargetRows     string
	Match          bool
	StableOnSource bool // source không đổi giữa hai lần đọc (nếu false, kết quả chỉ là tham khảo)
	Attempts       int
}

func allColumns(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID) ([]ColumnInfo, error) {
	schema := schemaOrDefault(ctx, db, engine, t)
	q := `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`
	if engine == model.EnginePG {
		q = `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
	}
	rows, err := db.QueryContext(ctx, q, schema, t.Table)
	if err != nil {
		return nil, fmt.Errorf("columns of %s: %w", t, err)
	}
	defer rows.Close()
	var cols []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		if err := rows.Scan(&c.Name, &c.DataType); err != nil {
			return nil, err
		}
		c.DataType = strings.ToLower(c.DataType)
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// hashTableSQL: câu SQL đếm hàng và tính checksum cả bảng (không điều kiện).
func hashTableSQL(engine model.Engine, t model.TableID, cols []ColumnInfo) string {
	row := rowExpression(engine, cols)
	if engine == model.EnginePG {
		return fmt.Sprintf("SELECT count(*)::text, COALESCE(sum(('x' || substr(md5(%s), 1, 15))::bit(60)::bigint), 0)::text FROM %s",
			row, QuoteTable(engine, t))
	}
	return fmt.Sprintf("SELECT CAST(COUNT(*) AS CHAR), CAST(COALESCE(SUM(CAST(CONV(SUBSTRING(MD5(%s), 1, 15), 16, 10) AS UNSIGNED)), 0) AS CHAR) FROM %s",
		row, QuoteTable(engine, t))
}

// HashTable trả "số_row:checksum" của cả bảng.
func HashTable(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID, cols []ColumnInfo) (string, error) {
	var cnt, sum string
	if err := db.QueryRowContext(ctx, hashTableSQL(engine, t, cols)).Scan(&cnt, &sum); err != nil {
		return "", fmt.Errorf("hash table %s: %w", t, err)
	}
	return cnt + ":" + sum, nil
}

// VerifyTableKeyless so số hàng + checksum của bảng t ở source và target. Khi pipeline đang chạy (ApplyWaiter),
// chờ target apply tới vị trí source rồi so; source đổi giữa hai lần đọc thì thử lại (tối đa 5 lần).
func (mv *MerkleVerifier) VerifyTableKeyless(ctx context.Context, t model.TableID) (*KeylessReport, error) {
	src, dstTable := mv.srcDB(), mv.targetTable(t)
	srcCols, err := allColumns(ctx, src, mv.cfg.SourceEngine, t)
	if err != nil {
		return nil, err
	}
	allDst, err := allColumns(ctx, mv.cfg.Target, mv.cfg.TargetEngine, dstTable)
	if err != nil {
		return nil, err
	}
	dstCols, err := matchTargetColumns(srcCols, allDst, dstTable)
	if err != nil {
		return nil, err
	}
	rep := &KeylessReport{Table: t.String()}
	for attempt := 1; attempt <= 5; attempt++ {
		rep.Attempts = attempt
		s1, err := HashTable(ctx, src, mv.cfg.SourceEngine, t, srcCols)
		if err != nil {
			return nil, err
		}
		if mv.cfg.ApplyWaiter != nil {
			mark, err := mv.cfg.ApplyWaiter.Mark(ctx)
			if err != nil {
				return nil, err
			}
			if err := mv.cfg.ApplyWaiter.Wait(ctx, mark); err != nil {
				return nil, err
			}
		}
		d, err := HashTable(ctx, mv.cfg.Target, mv.cfg.TargetEngine, dstTable, dstCols)
		if err != nil {
			return nil, err
		}
		s2, err := HashTable(ctx, src, mv.cfg.SourceEngine, t, srcCols)
		if err != nil {
			return nil, err
		}
		rep.SourceRows, rep.TargetRows = s1, d
		rep.StableOnSource = s1 == s2
		rep.Match = s1 == d
		if rep.Match || rep.StableOnSource && mv.cfg.ApplyWaiter == nil {
			return rep, nil
		}
		if rep.StableOnSource {
			return rep, nil
		}
	}
	return rep, nil
}
