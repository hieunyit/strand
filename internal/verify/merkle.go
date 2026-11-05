package verify

// MerkleVerifier: verify --mode full bằng cây Merkle (design §7 "full: chạy theo vòng, tốc độ
// thấp, trên replica. Lưu hash theo chunk (cây Merkle) để dừng giữa chừng rồi chạy tiếp được";
// 02 §3, §10, §11).
//
// Cách làm:
//  1. Chia bảng (PK một cột số nguyên) theo keyset walk trên source thành chunk ~ChunkRows row,
//     cộng hai chunk biên mở [MinInt64, min-1] và [max+1, MaxInt64] để bắt row thừa ở target.
//  2. Hash từng chunk ngay trong DB ở cả hai phía, không kéo row về: mỗi row được chuẩn hóa thành
//     một chuỗi text chính tắc (canonicalValueExpr), hash row = 60 bit đầu của MD5, hash chunk =
//     "COUNT:SUM(hash row)". Cùng một thuật toán cho MySQL và PG nên MySQL→PG, PG→PG,
//     MySQL→MySQL đều so được.
//  3. Hash lá được lưu vào _strand_verify trên target ngay sau khi tính, để dừng giữa chừng rồi
//     chạy tiếp (Resume) mà không hash lại chunk đã xong.
//  4. Dựng cây Merkle (lá = hash chunk, nút cha = sha256(trái:phải)) ở hai phía, so từ gốc, chỉ
//     đi xuống nhánh khác hash. Lá khác hash là chunk duy nhất phải so từng row.
//  5. So từng row trong chunk lệch: dùng LiveVerifier (MySQL→MySQL, nếu được cấu hình) hoặc so
//     chuỗi chính tắc theo PK (mọi cặp engine).
//
// Giới hạn của chuẩn hóa (canonicalValueExpr):
//   - Số thực (FLOAT/DOUBLE, real/double precision): so theo chữ số ngắn nhất round-trip của giá trị
//     double, làm tròn tới 30 chữ số thập phân. |x| < 1e-30 coi như 0. |x| >= 1e35 so theo text dạng mũ
//     của double. FLOAT 4 byte được nâng lên double trước khi so. PG cần extra_float_digits >= 1 (mặc
//     định từ PG 12). NaN/Infinity chỉ có ở PG nên luôn báo lệch với MySQL.
//   - Thời gian: TIMESTAMP (MySQL) và timestamptz (PG) so theo thời điểm tuyệt đối, quy về UTC.
//     DATETIME (MySQL) và timestamp (PG) là giờ tường, so nguyên văn. Ngày 0000-00-00 của MySQL và
//     infinity của PG không có tương đương.
//   - Chuỗi so theo ký tự UTF-8; BINARY(n) của MySQL có đệm 0x00 nên chỉ khớp bytea cùng độ dài.
//   - BIT, kiểu hình học, mảng, interval: chỉ chắc chắn khi cùng engine.
//   - Cột chỉ có ở target bị bỏ qua; cột của source thiếu ở target là lỗi.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"strand/internal/metrics"
	"strand/internal/model"
)

// DefaultChunkRows là kích thước chunk mặc định (10.000 row).
const DefaultChunkRows = 10000

// DefaultFullRate là tốc độ mặc định của full verify (chunk/giây).
const DefaultFullRate = 10.0

// ErrVerifyInterrupted báo full verify dừng giữa chừng (StopAfter); tiến độ đã lưu, chạy lại với Resume.
var ErrVerifyInterrupted = errors.New("verify interrupted")

// ColumnInfo là tên và kiểu (DATA_TYPE của information_schema, chữ thường) của một cột.
type ColumnInfo struct {
	Name     string
	DataType string
}

// ChunkRange là dải PK [Start, End] của một chunk.
type ChunkRange struct {
	ID    int
	Start int64
	End   int64
}

// MerkleNode là một nút của cây Merkle.
type MerkleNode struct {
	Hash   string
	Range  ChunkRange
	IsLeaf bool
	Left   *MerkleNode
	Right  *MerkleNode
}

// FullVerifyConfig cấu hình MerkleVerifier.
type FullVerifyConfig struct {
	Source *sql.DB
	// SourceReplica, nếu có, được dùng thay Source cho mọi lần đọc (design §7: full chạy trên replica).
	SourceReplica *sql.DB
	Target        *sql.DB
	// Meta là kết nối tới schema chứa _strand_verify trên target (nil = Target).
	Meta         *sql.DB
	PipelineID   string
	SourceEngine model.Engine
	TargetEngine model.Engine
	TargetName   func(model.TableID) model.TableID
	ChunkRows    int
	Rate         float64 // tối đa N chunk/giây; <= 0 là không giới hạn
	Resume       bool    // dùng lại hash chunk đã lưu trong _strand_verify
	StopAfter    int     // dừng sau khi hash N chunk mới (phục vụ test resume); 0 = không dừng
	// LiveVerifier, nếu có và cả hai phía là MySQL, được dùng để so từng row trong chunk lệch
	// (có kiểm lại PK để tách unverifiable_hot). Chạy độc lập thì tạo nó với SkipApplyWait.
	LiveVerifier *LiveVerifier
	Metrics      *metrics.Metrics
}

// FullVerifyReport là kết quả full verify một bảng.
type FullVerifyReport struct {
	Table            string
	TotalChunks      int
	HashedChunks     int // số chunk được hash trong lượt này
	ResumedChunks    int // số chunk lấy hash từ _strand_verify (không hash lại)
	MismatchedChunks int // số lá khác hash
	RangesChecked    int // số chunk được so từng row
	RowsChecked      int
	Mismatches       []string // PK lệch thật
	UnverifiableHot  []string // PK quá nóng
}

// VerifyMode là chế độ verify (02 §3, §10).
type VerifyMode string

const (
	VerifyModeSample VerifyMode = "sample"
	VerifyModeDelta  VerifyMode = "delta"
	VerifyModeFull   VerifyMode = "full"
)

// VerifyReport là báo cáo chung của interface Verifier (02 §3).
type VerifyReport struct {
	Table           string
	RangesChecked   int
	RowsChecked     int
	Mismatches      []string
	UnverifiableHot []string
}

// Verifier là interface của 02 §3.
type Verifier interface {
	Verify(ctx context.Context, mode VerifyMode, t model.TableID) (VerifyReport, error)
}

// MerkleVerifier — xem chú thích đầu file.
type MerkleVerifier struct {
	cfg FullVerifyConfig
}

// NewMerkleVerifier tạo MerkleVerifier.
func NewMerkleVerifier(cfg FullVerifyConfig) *MerkleVerifier {
	if cfg.ChunkRows <= 0 {
		cfg.ChunkRows = DefaultChunkRows
	}
	if cfg.SourceEngine == "" {
		cfg.SourceEngine = model.EngineMySQL
	}
	if cfg.TargetEngine == "" {
		cfg.TargetEngine = model.EngineMySQL
	}
	if cfg.Meta == nil {
		cfg.Meta = cfg.Target
	}
	return &MerkleVerifier{cfg: cfg}
}

// Verify thực thi Verifier (chỉ mode full).
func (mv *MerkleVerifier) Verify(ctx context.Context, mode VerifyMode, t model.TableID) (VerifyReport, error) {
	if mode != VerifyModeFull {
		return VerifyReport{}, fmt.Errorf("MerkleVerifier only supports mode full, got %s", mode)
	}
	pkCol, err := FindSinglePKColumn(ctx, mv.srcDB(), mv.cfg.SourceEngine, t)
	if err != nil {
		return VerifyReport{}, err
	}
	rep, err := mv.VerifyTableFull(ctx, t, pkCol)
	if err != nil {
		return VerifyReport{}, err
	}
	return VerifyReport{Table: rep.Table, RangesChecked: rep.RangesChecked, RowsChecked: rep.RowsChecked,
		Mismatches: rep.Mismatches, UnverifiableHot: rep.UnverifiableHot}, nil
}

func (mv *MerkleVerifier) srcDB() *sql.DB {
	if mv.cfg.SourceReplica != nil {
		return mv.cfg.SourceReplica
	}
	return mv.cfg.Source
}

func (mv *MerkleVerifier) targetTable(t model.TableID) model.TableID {
	if mv.cfg.TargetName != nil {
		return mv.cfg.TargetName(t)
	}
	return t
}

// ph trả placeholder thứ n (1-based) theo engine.
func ph(engine model.Engine, n int) string {
	if engine == model.EnginePG {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

// keyPh là placeholder cho giá trị PK; PG ép bigint để so được cả khi cột PK là int4 mà biên chunk
// (±2^62) vượt int4.
func keyPh(engine model.Engine, n int) string {
	if engine == model.EnginePG {
		return fmt.Sprintf("$%d::bigint", n)
	}
	return "?"
}

func schemaOrDefault(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID) string {
	if t.Schema != "" {
		return t.Schema
	}
	if engine == model.EnginePG {
		return "public"
	}
	var s sql.NullString
	_ = db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&s)
	return s.String
}

// FindSinglePKColumn trả tên cột PK khi PK có đúng một cột.
func FindSinglePKColumn(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID) (string, error) {
	schema := schemaOrDefault(ctx, db, engine, t)
	var q string
	if engine == model.EnginePG {
		q = `SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
			 AND tc.table_name = kcu.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = $1 AND tc.table_name = $2
			ORDER BY kcu.ordinal_position`
	} else {
		q = `SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
			ORDER BY ORDINAL_POSITION`
	}
	rows, err := db.QueryContext(ctx, q, schema, t.Table)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(cols) != 1 {
		return "", fmt.Errorf("table %s has %d primary key columns, need exactly 1", t, len(cols))
	}
	return cols[0], nil
}

// EnsureVerifyTable tạo _strand_verify (tiến độ full verify) trên target; gọi lại nhiều lần không lỗi.
func EnsureVerifyTable(ctx context.Context, db *sql.DB, engine model.Engine) error {
	ddl := `CREATE TABLE IF NOT EXISTS _strand_verify (
			pipeline_id   VARCHAR(64) NOT NULL,
			table_name    VARCHAR(255) NOT NULL,
			chunk_id      INT NOT NULL,
			range_start   BIGINT NOT NULL,
			range_end     BIGINT NOT NULL,
			source_hash   VARCHAR(64) NOT NULL,
			target_hash   VARCHAR(64) NOT NULL,
			status        VARCHAR(16) NOT NULL,
			updated_at    TIMESTAMP(6) NOT NULL,
			PRIMARY KEY (pipeline_id, table_name, chunk_id)
		) ENGINE=InnoDB`
	if engine == model.EnginePG {
		ddl = `CREATE TABLE IF NOT EXISTS _strand_verify (
			pipeline_id   text NOT NULL,
			table_name    text NOT NULL,
			chunk_id      int NOT NULL,
			range_start   bigint NOT NULL,
			range_end     bigint NOT NULL,
			source_hash   text NOT NULL,
			target_hash   text NOT NULL,
			status        text NOT NULL,
			updated_at    timestamptz NOT NULL,
			PRIMARY KEY (pipeline_id, table_name, chunk_id)
		)`
	}
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("ensure _strand_verify: %w", err)
	}
	return nil
}

// QuoteIdent trích dẫn định danh theo engine.
func QuoteIdent(engine model.Engine, name string) string {
	if engine == model.EnginePG {
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// QuoteTable trích dẫn schema.table theo engine.
func QuoteTable(engine model.Engine, t model.TableID) string {
	if t.Schema == "" {
		return QuoteIdent(engine, t.Table)
	}
	return QuoteIdent(engine, t.Schema) + "." + QuoteIdent(engine, t.Table)
}

// GetTableColumns trả các cột của bảng theo thứ tự định nghĩa, cột PK đứng đầu.
func GetTableColumns(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID, pkCol string) ([]ColumnInfo, error) {
	schema := schemaOrDefault(ctx, db, engine, t)
	q := `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`
	if engine == model.EnginePG {
		q = `SELECT column_name, data_type FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`
	}
	rows, err := db.QueryContext(ctx, q, schema, t.Table)
	if err != nil {
		return nil, fmt.Errorf("columns of %s: %w", t, err)
	}
	defer rows.Close()
	var cols []ColumnInfo
	var pk *ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		if err := rows.Scan(&c.Name, &c.DataType); err != nil {
			return nil, err
		}
		c.DataType = strings.ToLower(c.DataType)
		if strings.EqualFold(c.Name, pkCol) {
			pk = &c
			continue
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if pk == nil {
		return nil, fmt.Errorf("table %s: primary key column %s not found", t, pkCol)
	}
	return append([]ColumnInfo{*pk}, cols...), nil
}

// matchTargetColumns trả cột target theo đúng thứ tự cột source (khớp tên không phân biệt hoa thường).
func matchTargetColumns(src, dst []ColumnInfo, dstTable model.TableID) ([]ColumnInfo, error) {
	byName := make(map[string]ColumnInfo, len(dst))
	for _, c := range dst {
		byName[strings.ToLower(c.Name)] = c
	}
	out := make([]ColumnInfo, len(src))
	for i, c := range src {
		d, ok := byName[strings.ToLower(c.Name)]
		if !ok {
			return nil, fmt.Errorf("column %s missing on target table %s", c.Name, dstTable)
		}
		out[i] = d
	}
	return out, nil
}

func isIntegerType(engine model.Engine, dt string) bool {
	if engine == model.EnginePG {
		return dt == "smallint" || dt == "integer" || dt == "bigint"
	}
	switch dt {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return true
	}
	return false
}

// DivideTableChunks chia bảng theo keyset walk trên PK số nguyên. Thêm hai chunk biên mở
// [MinInt64, min-1] và [max+1, MaxInt64] (phủ TOÀN BỘ int64) để phát hiện row thừa ở target ngoài
// [min, max]. Biên chỉ thêm khi min > MinInt64 / max < MaxInt64 nên min-1, max+1 không tràn.
// PK BIGINT UNSIGNED > MaxInt64 ở source: MIN/MAX không đọc được vào int64 → trả lỗi (không bỏ qua).
func DivideTableChunks(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID, pkCol string, chunkSize int) ([]ChunkRange, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkRows
	}
	qpk, qt := QuoteIdent(engine, pkCol), QuoteTable(engine, t)
	var mn, mx sql.NullInt64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qpk, qpk, qt)).Scan(&mn, &mx); err != nil {
		return nil, fmt.Errorf("min/max pk of %s: %w", t, err)
	}
	const infMin, infMax = int64(math.MinInt64), int64(math.MaxInt64)
	if !mn.Valid {
		// Source rỗng: một chunk phủ mọi khóa để bắt row thừa ở target.
		return []ChunkRange{{ID: 0, Start: infMin, End: infMax}}, nil
	}
	var chunks []ChunkRange
	add := func(lo, hi int64) { chunks = append(chunks, ChunkRange{ID: len(chunks), Start: lo, End: hi}) }
	if mn.Int64 > infMin {
		add(infMin, mn.Int64-1)
	}
	walk := fmt.Sprintf("SELECT MAX(%s) FROM (SELECT %s FROM %s WHERE %s >= %s ORDER BY %s LIMIT %d) x",
		qpk, qpk, qt, qpk, keyPh(engine, 1), qpk, chunkSize)
	next := mn.Int64
	for next <= mx.Int64 {
		var end sql.NullInt64
		if err := db.QueryRowContext(ctx, walk, next).Scan(&end); err != nil {
			return nil, fmt.Errorf("keyset walk on %s: %w", t, err)
		}
		if !end.Valid {
			break
		}
		add(next, end.Int64)
		if end.Int64 >= mx.Int64 || end.Int64 == math.MaxInt64 {
			break
		}
		next = end.Int64 + 1
	}
	if mx.Int64 < infMax {
		add(mx.Int64+1, infMax)
	}
	return chunks, nil
}

// stripDecimalSQL bỏ số 0 thừa sau dấu chấm và dấu chấm lẻ của chuỗi số thập phân v (10.50 -> 10.5,
// 10.00 -> 10). Biểu thức dùng được ở cả MySQL và PG.
func stripDecimalSQL(engine model.Engine, v string) string {
	if engine == model.EnginePG {
		return fmt.Sprintf("(CASE WHEN strpos(%s, '.') > 0 THEN rtrim(rtrim(%s, '0'), '.') ELSE %s END)", v, v, v)
	}
	return fmt.Sprintf("(CASE WHEN LOCATE('.', %s) > 0 THEN TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM %s)) ELSE %s END)", v, v, v)
}

// canonicalValueExpr trả biểu thức SQL cho text chính tắc của một cột: NULL khi giá trị NULL, không
// bao giờ chứa byte 0x00. Cùng dữ liệu cho cùng chuỗi ở MySQL và PG (giới hạn: xem đầu file).
func canonicalValueExpr(engine model.Engine, col ColumnInfo) string {
	i := QuoteIdent(engine, col.Name)
	if engine == model.EnginePG {
		switch col.DataType {
		case "smallint", "integer", "bigint":
			return i + "::text"
		case "numeric":
			return fmt.Sprintf("(CASE WHEN %s = 0 THEN '0' ELSE %s END)", i, stripDecimalSQL(engine, i+"::text"))
		case "real", "double precision":
			// Text của float8 là chữ số ngắn nhất round-trip; ::numeric giữ nguyên các chữ số đó.
			return fmt.Sprintf("(CASE WHEN %s = 0 THEN '0' WHEN abs(%s) < 1e35 THEN %s ELSE replace(%s::float8::text, 'e+', 'e') END)",
				i, i, stripDecimalSQL(engine, fmt.Sprintf("round(%s::float8::text::numeric, 30)::text", i)), i)
		case "boolean":
			// Không dùng ELSE: NULL phải ra NULL, không ra '0'.
			return fmt.Sprintf("(CASE WHEN %s THEN '1' WHEN NOT %s THEN '0' END)", i, i)
		case "date":
			return fmt.Sprintf("to_char(%s, 'YYYY-MM-DD')", i)
		case "timestamp without time zone":
			return fmt.Sprintf("to_char(%s, 'YYYY-MM-DD HH24:MI:SS.US')", i)
		case "timestamp with time zone":
			return fmt.Sprintf("to_char(%s AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')", i)
		case "time without time zone":
			return fmt.Sprintf("to_char(%s, 'HH24:MI:SS.US')", i)
		case "bytea":
			return fmt.Sprintf("encode(%s, 'hex')", i)
		default:
			return i + "::text"
		}
	}
	var e string
	switch col.DataType {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year":
		e = fmt.Sprintf("CAST(%s AS CHAR)", i)
	case "bit":
		e = fmt.Sprintf("CAST(%s + 0 AS CHAR)", i)
	case "decimal", "numeric":
		e = fmt.Sprintf("(CASE WHEN %s = 0 THEN '0' ELSE %s END)", i, stripDecimalSQL(engine, fmt.Sprintf("CAST(%s AS CHAR)", i)))
	case "float", "double", "real":
		// MySQL đổi double sang DECIMAL theo chữ số ngắn nhất round-trip, khớp với nhánh PG.
		e = fmt.Sprintf("(CASE WHEN %s = 0 THEN '0' WHEN ABS(%s) < 1e35 THEN %s ELSE REPLACE(CAST(CAST(%s AS DOUBLE) AS CHAR), 'e+', 'e') END)",
			i, i, stripDecimalSQL(engine, fmt.Sprintf("CAST(CAST(%s AS DECIMAL(65,30)) AS CHAR)", i)), i)
	case "date":
		e = fmt.Sprintf("DATE_FORMAT(%s, '%%Y-%%m-%%d')", i)
	case "datetime":
		e = fmt.Sprintf("DATE_FORMAT(%s, '%%Y-%%m-%%d %%H:%%i:%%s.%%f')", i)
	case "timestamp":
		// UNIX_TIMESTAMP của cột TIMESTAMP là giá trị UTC lưu trong bảng, không phụ thuộc time_zone
		// của session; cộng vào mốc 1970 dạng DATETIME để ra giờ UTC.
		e = fmt.Sprintf("DATE_FORMAT(DATE_ADD(TIMESTAMP '1970-01-01 00:00:00', INTERVAL CAST(ROUND(UNIX_TIMESTAMP(%s) * 1000000) AS SIGNED) MICROSECOND), '%%Y-%%m-%%d %%H:%%i:%%s.%%f')", i)
	case "time":
		e = fmt.Sprintf("TIME_FORMAT(%s, '%%H:%%i:%%s.%%f')", i)
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		e = fmt.Sprintf("LOWER(HEX(%s))", i)
	default:
		e = fmt.Sprintf("CAST(%s AS CHAR CHARACTER SET utf8mb4)", i)
	}
	return fmt.Sprintf("CONVERT(%s USING utf8mb4)", e)
}

// rowExpression trả biểu thức text chính tắc của cả row: mỗi cột là "độ_dài:giá_trị" (NULL là "N"),
// nối bằng '|'. Tiền tố độ dài làm chuỗi không mơ hồ kể cả khi giá trị chứa '|' hay ':'.
func rowExpression(engine model.Engine, cols []ColumnInfo) string {
	parts := make([]string, len(cols))
	for k, c := range cols {
		v := canonicalValueExpr(engine, c)
		if engine == model.EnginePG {
			parts[k] = fmt.Sprintf("COALESCE(char_length(%s)::text || ':' || %s, 'N')", v, v)
		} else {
			parts[k] = fmt.Sprintf("COALESCE(CONCAT(CHAR_LENGTH(%s), ':', %s), 'N')", v, v)
		}
	}
	if engine == model.EnginePG {
		return "concat_ws('|', " + strings.Join(parts, ", ") + ")"
	}
	return "CONCAT_WS('|', " + strings.Join(parts, ", ") + ")"
}

// HashChunk tính hash của chunk r ngay trong DB: "số_row:tổng 60 bit đầu MD5 của từng row chính tắc".
// Tổng không phụ thuộc thứ tự row và là số nguyên chính xác ở cả hai engine (DECIMAL / numeric).
func HashChunk(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID, pkCol string, r ChunkRange, cols []ColumnInfo) (string, error) {
	row := rowExpression(engine, cols)
	qpk := QuoteIdent(engine, pkCol)
	var q string
	if engine == model.EnginePG {
		q = fmt.Sprintf("SELECT count(*)::text, COALESCE(sum(('x' || substr(md5(%s), 1, 15))::bit(60)::bigint), 0)::text FROM %s WHERE %s BETWEEN $1::bigint AND $2::bigint",
			row, QuoteTable(engine, t), qpk)
	} else {
		q = fmt.Sprintf("SELECT CAST(COUNT(*) AS CHAR), CAST(COALESCE(SUM(CAST(CONV(SUBSTRING(MD5(%s), 1, 15), 16, 10) AS UNSIGNED)), 0) AS CHAR) FROM %s WHERE %s BETWEEN ? AND ?",
			row, QuoteTable(engine, t), qpk)
	}
	var cnt, sum string
	if err := db.QueryRowContext(ctx, q, r.Start, r.End).Scan(&cnt, &sum); err != nil {
		return "", fmt.Errorf("hash chunk %d of %s: %w", r.ID, t, err)
	}
	return cnt + ":" + sum, nil
}

// BuildMerkleTree dựng cây Merkle nhị phân từ các lá; nút cha = sha256(trái ":" phải), nút lẻ được nâng lên.
func BuildMerkleTree(leaves []*MerkleNode) *MerkleNode {
	if len(leaves) == 0 {
		return nil
	}
	cur := leaves
	for len(cur) > 1 {
		var next []*MerkleNode
		for k := 0; k < len(cur); k += 2 {
			if k+1 == len(cur) {
				next = append(next, cur[k])
				continue
			}
			h := sha256.Sum256([]byte(cur[k].Hash + ":" + cur[k+1].Hash))
			next = append(next, &MerkleNode{Hash: hex.EncodeToString(h[:]), Left: cur[k], Right: cur[k+1]})
		}
		cur = next
	}
	return cur[0]
}

// DiffMerkleTrees so hai cây (cùng hình dạng, cùng danh sách chunk) từ gốc, chỉ đi xuống nhánh khác
// hash; trả các lá (chunk) khác hash.
func DiffMerkleTrees(srcRoot, dstRoot *MerkleNode) []ChunkRange {
	var out []ChunkRange
	var dfs func(s, d *MerkleNode)
	dfs = func(s, d *MerkleNode) {
		if s == nil || d == nil || s.Hash == d.Hash {
			return
		}
		if s.IsLeaf || d.IsLeaf {
			out = append(out, s.Range)
			return
		}
		dfs(s.Left, d.Left)
		dfs(s.Right, d.Right)
	}
	dfs(srcRoot, dstRoot)
	return out
}

type cachedChunk struct {
	start, end int64
	src, dst   string
}

func (mv *MerkleVerifier) loadProgress(ctx context.Context, table string) (map[int]cachedChunk, error) {
	e := mv.cfg.TargetEngine
	q := fmt.Sprintf(`SELECT chunk_id, range_start, range_end, source_hash, target_hash FROM _strand_verify
		WHERE pipeline_id = %s AND table_name = %s`, ph(e, 1), ph(e, 2))
	rows, err := mv.cfg.Meta.QueryContext(ctx, q, mv.cfg.PipelineID, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]cachedChunk{}
	for rows.Next() {
		var id int
		var c cachedChunk
		if err := rows.Scan(&id, &c.start, &c.end, &c.src, &c.dst); err != nil {
			return nil, err
		}
		out[id] = c
	}
	return out, rows.Err()
}

func (mv *MerkleVerifier) clearProgress(ctx context.Context, table string) error {
	e := mv.cfg.TargetEngine
	_, err := mv.cfg.Meta.ExecContext(ctx, fmt.Sprintf("DELETE FROM _strand_verify WHERE pipeline_id = %s AND table_name = %s",
		ph(e, 1), ph(e, 2)), mv.cfg.PipelineID, table)
	return err
}

func (mv *MerkleVerifier) saveProgress(ctx context.Context, table string, r ChunkRange, srcHash, dstHash string) error {
	status := "matched"
	if srcHash != dstHash {
		status = "mismatched"
	}
	q := `INSERT INTO _strand_verify (pipeline_id, table_name, chunk_id, range_start, range_end, source_hash, target_hash, status, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(6))
		ON DUPLICATE KEY UPDATE range_start = VALUES(range_start), range_end = VALUES(range_end), source_hash = VALUES(source_hash),
			target_hash = VALUES(target_hash), status = VALUES(status), updated_at = NOW(6)`
	if mv.cfg.TargetEngine == model.EnginePG {
		q = `INSERT INTO _strand_verify (pipeline_id, table_name, chunk_id, range_start, range_end, source_hash, target_hash, status, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
		ON CONFLICT (pipeline_id, table_name, chunk_id) DO UPDATE SET range_start = EXCLUDED.range_start, range_end = EXCLUDED.range_end,
			source_hash = EXCLUDED.source_hash, target_hash = EXCLUDED.target_hash, status = EXCLUDED.status, updated_at = now()`
	}
	_, err := mv.cfg.Meta.ExecContext(ctx, q, mv.cfg.PipelineID, table, r.ID, r.Start, r.End, srcHash, dstHash, status)
	return err
}

// VerifyTableFull chạy full verify một bảng (xem chú thích đầu file).
func (mv *MerkleVerifier) VerifyTableFull(ctx context.Context, t model.TableID, pkCol string) (*FullVerifyReport, error) {
	src, dstTable := mv.srcDB(), mv.targetTable(t)
	if err := EnsureVerifyTable(ctx, mv.cfg.Meta, mv.cfg.TargetEngine); err != nil {
		return nil, err
	}
	srcCols, err := GetTableColumns(ctx, src, mv.cfg.SourceEngine, t, pkCol)
	if err != nil {
		return nil, err
	}
	if !isIntegerType(mv.cfg.SourceEngine, srcCols[0].DataType) {
		return nil, fmt.Errorf("table %s: full verify needs an integer primary key, %s is %s", t, pkCol, srcCols[0].DataType)
	}
	allDst, err := GetTableColumns(ctx, mv.cfg.Target, mv.cfg.TargetEngine, dstTable, pkCol)
	if err != nil {
		return nil, err
	}
	dstCols, err := matchTargetColumns(srcCols, allDst, dstTable)
	if err != nil {
		return nil, err
	}
	chunks, err := DivideTableChunks(ctx, src, mv.cfg.SourceEngine, t, pkCol, mv.cfg.ChunkRows)
	if err != nil {
		return nil, err
	}
	rep := &FullVerifyReport{Table: t.String(), TotalChunks: len(chunks)}
	tableKey := t.String()

	var cached map[int]cachedChunk
	if mv.cfg.Resume {
		if cached, err = mv.loadProgress(ctx, tableKey); err != nil {
			return nil, fmt.Errorf("load _strand_verify progress: %w", err)
		}
	} else if err := mv.clearProgress(ctx, tableKey); err != nil {
		return nil, fmt.Errorf("clear _strand_verify progress: %w", err)
	}

	var tick <-chan time.Time
	if mv.cfg.Rate > 0 {
		tk := time.NewTicker(time.Duration(float64(time.Second) / mv.cfg.Rate))
		defer tk.Stop()
		tick = tk.C
	}
	srcLeaves := make([]*MerkleNode, 0, len(chunks))
	dstLeaves := make([]*MerkleNode, 0, len(chunks))
	for _, c := range chunks {
		var sh, th string
		// Chunk đã hash KHỚP ở lượt trước bị ngắt (cùng ranh giới) thì dùng lại, không hash lại. Chunk
		// từng lệch luôn hash lại để báo đúng trạng thái hiện tại.
		if cc, ok := cached[c.ID]; ok && cc.start == c.Start && cc.end == c.End && cc.src != "" && cc.src == cc.dst {
			sh, th = cc.src, cc.dst
			rep.ResumedChunks++
		} else {
			if mv.cfg.StopAfter > 0 && rep.HashedChunks >= mv.cfg.StopAfter {
				return rep, fmt.Errorf("full verify of %s stopped after %d chunks: %w", t, rep.HashedChunks, ErrVerifyInterrupted)
			}
			if tick != nil {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-tick:
				}
			}
			if sh, err = HashChunk(ctx, src, mv.cfg.SourceEngine, t, pkCol, c, srcCols); err != nil {
				return nil, fmt.Errorf("source: %w", err)
			}
			if th, err = HashChunk(ctx, mv.cfg.Target, mv.cfg.TargetEngine, dstTable, pkCol, c, dstCols); err != nil {
				return nil, fmt.Errorf("target: %w", err)
			}
			rep.HashedChunks++
			if err := mv.saveProgress(ctx, tableKey, c, sh, th); err != nil {
				return nil, fmt.Errorf("save _strand_verify progress: %w", err)
			}
		}
		srcLeaves = append(srcLeaves, &MerkleNode{Hash: sh, Range: c, IsLeaf: true})
		dstLeaves = append(dstLeaves, &MerkleNode{Hash: th, Range: c, IsLeaf: true})
	}

	// Lượt đã hash đủ mọi chunk: xóa tiến độ. --resume chỉ để chạy tiếp một lượt BỊ NGẮT; nếu giữ lại,
	// lượt --resume sau đó tin hash cũ dù dữ liệu đã đổi và báo sạch sai (review R12).
	if err := mv.clearProgress(ctx, tableKey); err != nil {
		return nil, fmt.Errorf("clear _strand_verify progress: %w", err)
	}
	bad := DiffMerkleTrees(BuildMerkleTree(srcLeaves), BuildMerkleTree(dstLeaves))
	rep.MismatchedChunks = len(bad)
	if len(bad) == 0 {
		return rep, nil
	}
	ranges := make([][2]int64, len(bad))
	for k, c := range bad {
		ranges[k] = [2]int64{c.Start, c.End}
	}
	if mv.cfg.LiveVerifier != nil && mv.cfg.SourceEngine == model.EngineMySQL && mv.cfg.TargetEngine == model.EngineMySQL {
		lr, err := mv.cfg.LiveVerifier.VerifyRanges(ctx, t, pkCol, ranges)
		if err != nil {
			return nil, fmt.Errorf("row compare of %s: %w", t, err)
		}
		rep.RangesChecked, rep.RowsChecked = lr.RangesChecked, lr.RowsChecked
		rep.Mismatches, rep.UnverifiableHot = lr.Mismatches, lr.UnverifiableHot
	} else {
		for _, r := range ranges {
			s, err := readCanonicalRows(ctx, src, mv.cfg.SourceEngine, t, pkCol, srcCols, r)
			if err != nil {
				return nil, fmt.Errorf("source: %w", err)
			}
			d, err := readCanonicalRows(ctx, mv.cfg.Target, mv.cfg.TargetEngine, dstTable, pkCol, dstCols, r)
			if err != nil {
				return nil, fmt.Errorf("target: %w", err)
			}
			rep.RangesChecked++
			rep.RowsChecked += len(s)
			for k, sv := range s {
				if dv, ok := d[k]; !ok || dv != sv {
					rep.Mismatches = append(rep.Mismatches, k)
				}
			}
			for k := range d {
				if _, ok := s[k]; !ok {
					rep.Mismatches = append(rep.Mismatches, k)
				}
			}
		}
	}
	if m := mv.cfg.Metrics; m != nil {
		m.VerifyMismatchTotal.WithLabelValues(mv.cfg.PipelineID, tableKey).Add(float64(len(rep.Mismatches)))
		m.VerifyUnverifiableHotTotal.WithLabelValues(mv.cfg.PipelineID, tableKey).Add(float64(len(rep.UnverifiableHot)))
	}
	return rep, nil
}

// readCanonicalRows đọc PK -> row chính tắc của range r (cùng biểu thức dùng khi hash).
func readCanonicalRows(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID, pkCol string, cols []ColumnInfo, r [2]int64) (map[string]string, error) {
	q := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s BETWEEN %s AND %s",
		canonicalValueExpr(engine, cols[0]), rowExpression(engine, cols), QuoteTable(engine, t),
		QuoteIdent(engine, pkCol), keyPh(engine, 1), keyPh(engine, 2))
	rows, err := db.QueryContext(ctx, q, r[0], r[1])
	if err != nil {
		return nil, fmt.Errorf("read rows of %s: %w", t, err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var pk, row string
		if err := rows.Scan(&pk, &row); err != nil {
			return nil, err
		}
		out[pk] = row
	}
	return out, rows.Err()
}
