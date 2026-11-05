package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"strand/internal/model"
)

// MySQLPKCodec dựng codec PK (MySQLPKColumn) từ information_schema của bảng.
func MySQLPKCodec(ctx context.Context, db *sql.DB, id model.TableID, pk []string) (*PKCodec, error) {
	c := &PKCodec{}
	for _, col := range pk {
		var dt, ct string
		var cs, coll, pad sql.NullString
		err := db.QueryRowContext(ctx, `SELECT c.DATA_TYPE, c.COLUMN_TYPE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, co.PAD_ATTRIBUTE
			FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS co ON co.COLLATION_NAME = c.COLLATION_NAME
			WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?`, id.Schema, id.Table, col).Scan(&dt, &ct, &cs, &coll, &pad)
		if err != nil {
			return nil, fmt.Errorf("read primary key column %s.%s: %w", id, col, err)
		}
		pc, err := MySQLPKColumn(col, dt, ct, cs.String, coll.String, pad.String)
		if err != nil {
			return nil, err
		}
		pc.Collation, pc.Charset = coll.String, cs.String
		c.Cols = append(c.Cols, pc)
		if pc.Kind == PKWeight && c.Weigh == nil {
			c.Weigh = newMySQLWeigher(db)
		}
	}
	return c, nil
}

// newMySQLWeigher: WEIGHT_STRING(CONVERT(v USING charset) COLLATE collation) tính trên db, có cache (giá
// trị lặp lại nhiều: biên chunk, khóa nóng). Weight là khóa sắp xếp của collation: so byte cho đúng thứ
// tự ORDER BY, và hai giá trị bằng nhau theo collation có cùng weight (kiểm ở TestGPK_MySQL…).
func newMySQLWeigher(db *sql.DB) func(PKColumn, model.Value) ([]byte, error) {
	var mu sync.Mutex
	cache := map[string][]byte{}
	return func(col PKColumn, v model.Value) ([]byte, error) {
		s, err := toText(v)
		if err != nil {
			return nil, err
		}
		ck := col.Charset + "\x00" + col.Collation + "\x00" + s
		mu.Lock()
		w, ok := cache[ck]
		mu.Unlock()
		if ok {
			return w, nil
		}
		q := fmt.Sprintf("SELECT WEIGHT_STRING(CONVERT(? USING %s) COLLATE %s)", col.Charset, col.Collation)
		if err := db.QueryRow(q, s).Scan(&w); err != nil {
			return nil, err
		}
		mu.Lock()
		if len(cache) > 100000 {
			cache = map[string][]byte{}
		}
		cache[ck] = w
		mu.Unlock()
		return w, nil
	}
}

// PGPKCodec dựng codec PK (PGPKColumn) từ catalog: kiểu gốc (typname, qua domain) và collation.
func PGPKCodec(ctx context.Context, db *sql.DB, id model.TableID, pk []string) (*PKCodec, error) {
	c := &PKCodec{}
	for _, col := range pk {
		var typ, coll string
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(bt.typname, ty.typname), COALESCE(co.collname, '')
			  FROM pg_attribute a
			  JOIN pg_class cl ON cl.oid = a.attrelid JOIN pg_namespace n ON n.oid = cl.relnamespace
			  JOIN pg_type ty ON ty.oid = a.atttypid
			  LEFT JOIN pg_type bt ON ty.typtype = 'd' AND bt.oid = ty.typbasetype
			  LEFT JOIN pg_collation co ON co.oid = a.attcollation
			 WHERE n.nspname = $1 AND cl.relname = $2 AND a.attname = $3`, id.Schema, id.Table, col).Scan(&typ, &coll)
		if err != nil {
			return nil, fmt.Errorf("read primary key column %s.%s: %w", id, col, err)
		}
		if coll == "default" {
			// Collation mặc định của database: nhận khi nó là thứ tự byte (xem pgDefaultCollation).
			if coll, err = pgDefaultCollation(ctx, db); err != nil {
				return nil, err
			}
		}
		pc, err := PGPKColumn(col, typ, coll)
		if err != nil {
			return nil, err
		}
		c.Cols = append(c.Cols, pc)
	}
	return c, nil
}

// pgDefaultCollation trả tên collation tương đương của collation "default" của database hiện tại khi nó
// là thứ tự byte — "C" (provider libc C/POSIX, hoặc builtin C / C.UTF-8 của PG 17+: thứ tự code point =
// thứ tự byte UTF-8); ngược lại trả "default:<provider>:<locale>" (PGPKColumn từ chối, mức 3). Đọc qua
// to_jsonb để chạy được trên mọi phiên bản (datlocprovider từ PG 15, datlocale từ PG 17).
func pgDefaultCollation(ctx context.Context, db *sql.DB) (string, error) {
	var provider, collate, locale sql.NullString
	err := db.QueryRowContext(ctx, `SELECT to_jsonb(d)->>'datlocprovider', d.datcollate, to_jsonb(d)->>'datlocale'
		FROM pg_database d WHERE d.datname = current_database()`).Scan(&provider, &collate, &locale)
	if err != nil {
		return "", fmt.Errorf("read database collation: %w", err)
	}
	p := provider.String
	if p == "" {
		p = "c" // trước PG 15: chỉ có libc
	}
	switch {
	case p == "c" && (collate.String == "C" || collate.String == "POSIX"):
		return "C", nil
	case p == "b" && (locale.String == "C" || locale.String == "C.UTF-8"):
		return "C", nil
	}
	return fmt.Sprintf("default:%s:%s%s", p, collate.String, locale.String), nil
}
