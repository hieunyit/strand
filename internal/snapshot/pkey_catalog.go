package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"strand/internal/model"
	"strand/internal/mysqlver"
)

// MySQLPKCodec dựng codec PK (MySQLPKColumn) từ information_schema của bảng.
func MySQLPKCodec(ctx context.Context, db *sql.DB, id model.TableID, pk []string) (*PKCodec, error) {
	c := &PKCodec{}
	ver, err := mysqlver.Cached(ctx, db)
	if err != nil {
		return nil, err
	}
	query := pkColumnQuery(ver)
	for _, col := range pk {
		var dt, ct string
		var cs, coll, pad sql.NullString
		err := db.QueryRowContext(ctx, query, id.Schema, id.Table, col).Scan(&dt, &ct, &cs, &coll, &pad)
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
			w := newMySQLWeigherState(db)
			c.Weigh, c.WeighMany = w.weigh, w.weighMany
		}
	}
	return c, nil
}

// pkColumnQuery: truy vấn catalog của một cột PK. COLLATIONS.PAD_ATTRIBUTE chỉ có từ MySQL 8.0; ở 5.7 mọi
// collation là PAD SPACE, trả NULL (MySQLPKColumn coi rỗng là PAD SPACE).
func pkColumnQuery(ver mysqlver.Version) string {
	if ver.IsMariaDB() {
		// MariaDB: không có PAD_ATTRIBUTE; collation NO PAD có tên chứa "nopad" (vd utf8mb4_general_nopad_ci).
		return `SELECT c.DATA_TYPE, c.COLUMN_TYPE, c.CHARACTER_SET_NAME, c.COLLATION_NAME,
				CASE WHEN c.COLLATION_NAME LIKE '%nopad%' THEN 'NO PAD' WHEN c.COLLATION_NAME IS NULL THEN NULL ELSE 'PAD SPACE' END
			FROM information_schema.COLUMNS c
			WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?`
	}
	if !ver.HasPadAttribute() {
		return `SELECT c.DATA_TYPE, c.COLUMN_TYPE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, NULL
			FROM information_schema.COLUMNS c
			WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?`
	}
	return `SELECT c.DATA_TYPE, c.COLUMN_TYPE, c.CHARACTER_SET_NAME, c.COLLATION_NAME, co.PAD_ATTRIBUTE
			FROM information_schema.COLUMNS c LEFT JOIN information_schema.COLLATIONS co ON co.COLLATION_NAME = c.COLLATION_NAME
			WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.COLUMN_NAME = ?`
}

// weighQueryTimeout giới hạn một truy vấn WEIGHT_STRING (review N05).
var weighQueryTimeout = 30 * time.Second

// weighBatch: số giá trị tối đa trong một truy vấn WEIGHT_STRING của WeighMany.
const weighBatch = 128

// mysqlWeigher: WEIGHT_STRING(CONVERT(v USING charset) COLLATE collation) tính trên db, có cache (giá
// trị lặp lại nhiều: biên chunk, khóa nóng). Weight là khóa sắp xếp của collation: so byte cho đúng thứ
// tự ORDER BY. Collation PAD SPACE: weight thô không xét phần đệm space ('a' ≠ 'a ', 'a' < 'a\t' trong
// khi DB coi 'a' = 'a ' và 'a' > 'a\t'), nên được chuyển qua PadSpaceWeightKey (review N04; kiểm với DB ở
// TestN04_MySQLPadSpaceKeyEqualityAndOrderMatchDB).
type mysqlWeigher struct {
	db     *sql.DB
	mu     sync.Mutex
	cache  map[string][]byte
	spaces map[string][]byte // charset\x00collation → weight của một space
}

// newMySQLWeigher trả hàm Weigh của codec (một giá trị mỗi truy vấn, có cache).
func newMySQLWeigher(db *sql.DB) func(PKColumn, model.Value) ([]byte, error) {
	return newMySQLWeigherState(db).weigh
}

func newMySQLWeigherState(db *sql.DB) *mysqlWeigher {
	return &mysqlWeigher{db: db, cache: map[string][]byte{}, spaces: map[string][]byte{}}
}

// raw tính weight thô của các giá trị ss trong MỘT truy vấn (một cột kết quả mỗi giá trị).
func (w *mysqlWeigher) raw(col PKColumn, ss []string) ([][]byte, error) {
	exprs := make([]string, len(ss))
	args := make([]any, len(ss))
	for i, s := range ss {
		exprs[i] = fmt.Sprintf("WEIGHT_STRING(CONVERT(? USING %s) COLLATE %s)", col.Charset, col.Collation)
		args[i] = s
	}
	// Có timeout: trước đây QueryRow không context, source treo thì luồng CDC (verify delta) treo
	// theo vô hạn (review N05). Weigh không nhận ctx (gọi sâu trong Encode/BoundOf) nên dùng
	// context.Background có giới hạn; không giữ ctx lúc dựng codec vì ctx đó có thể đã bị hủy.
	qctx, cancel := context.WithTimeout(context.Background(), weighQueryTimeout)
	defer cancel()
	// NullString phân biệt NULL với weight rỗng (giá trị '' có weight rỗng, không phải NULL).
	ns := make([]sql.NullString, len(ss))
	ptrs := make([]any, len(ss))
	for i := range ns {
		ptrs[i] = &ns[i]
	}
	if err := w.db.QueryRowContext(qctx, "SELECT "+strings.Join(exprs, ", "), args...).Scan(ptrs...); err != nil {
		return nil, err
	}
	out := make([][]byte, len(ss))
	for i, n := range ns {
		if !n.Valid {
			// WEIGHT_STRING trả NULL khi CONVERT thất bại: không có khóa thì không được đoán.
			return nil, fmt.Errorf("WEIGHT_STRING of %q under %s is NULL", ss[i], col.Collation)
		}
		out[i] = []byte(n.String)
	}
	return out, nil
}

// finish áp PadSpaceWeightKey cho collation PAD SPACE.
func (w *mysqlWeigher) finish(col PKColumn, wt []byte) ([]byte, error) {
	if !col.PadSpace {
		return wt, nil
	}
	sk := col.Charset + "\x00" + col.Collation
	w.mu.Lock()
	sp, ok := w.spaces[sk]
	w.mu.Unlock()
	if !ok {
		r, err := w.raw(col, []string{" "})
		if err != nil {
			return nil, err
		}
		sp = r[0]
		w.mu.Lock()
		w.spaces[sk] = sp
		w.mu.Unlock()
	}
	out, err := PadSpaceWeightKey(wt, sp)
	if err != nil {
		return nil, fmt.Errorf("collation %s: %w", col.Collation, err)
	}
	return out, nil
}

func cacheKey(col PKColumn, s string) string {
	return col.Charset + "\x00" + col.Collation + "\x00" + s
}

func (w *mysqlWeigher) put(ck string, wt []byte) {
	w.mu.Lock()
	if len(w.cache) > 100000 {
		w.cache = map[string][]byte{}
	}
	w.cache[ck] = wt
	w.mu.Unlock()
}

func (w *mysqlWeigher) weigh(col PKColumn, v model.Value) ([]byte, error) {
	s, err := toText(v)
	if err != nil {
		return nil, err
	}
	ck := cacheKey(col, s)
	w.mu.Lock()
	wt, ok := w.cache[ck]
	w.mu.Unlock()
	if ok {
		return wt, nil
	}
	r, err := w.raw(col, []string{s})
	if err != nil {
		return nil, err
	}
	if wt, err = w.finish(col, r[0]); err != nil {
		return nil, err
	}
	w.put(ck, wt)
	return wt, nil
}

// weighMany tính trước (vào cache) weight của vs, weighBatch giá trị mỗi truy vấn: quét khóa target khi
// dọn phần chưa đọc / verify full khác thứ tự (CrossEngineKeySet) cần khóa source của mọi row target —
// một truy vấn mỗi row thì quá chậm. Chỉ là tối ưu: Encode sau đó vẫn đi qua weigh (cache bị xóa giữa
// chừng thì weigh tự hỏi lại MySQL), nên kết quả không phụ thuộc cache.
func (w *mysqlWeigher) weighMany(col PKColumn, vs []model.Value) error {
	var todo []string
	seen := map[string]bool{}
	for _, v := range vs {
		s, err := toText(v)
		if err != nil {
			return err
		}
		ck := cacheKey(col, s)
		w.mu.Lock()
		_, ok := w.cache[ck]
		w.mu.Unlock()
		if !ok && !seen[s] {
			seen[s] = true
			todo = append(todo, s)
		}
	}
	for i := 0; i < len(todo); i += weighBatch {
		part := todo[i:min(i+weighBatch, len(todo))]
		r, err := w.raw(col, part)
		if err != nil {
			return err
		}
		for j, s := range part {
			wt, err := w.finish(col, r[j])
			if err != nil {
				return err
			}
			w.put(cacheKey(col, s), wt)
		}
	}
	return nil
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
