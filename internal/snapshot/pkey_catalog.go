package snapshot

import (
	"context"
	"database/sql"
	"fmt"

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
		pc.Collation = coll.String
		c.Cols = append(c.Cols, pc)
	}
	return c, nil
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
		pc, err := PGPKColumn(col, typ, coll)
		if err != nil {
			return nil, err
		}
		c.Cols = append(c.Cols, pc)
	}
	return c, nil
}
