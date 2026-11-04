package main

// So sánh trạng thái cuối source/target theo kiểu streaming (thay cho việc dump toàn bộ bảng vào map):
// mỗi phía được đọc theo PK tăng dần bằng keyset pagination (WHERE pk > ? ORDER BY pk LIMIT n), hai phía
// đọc song song (mỗi phía một goroutine prefetch tối đa 1 chunk), rồi merge-compare như merge-join.
// Bộ nhớ là O(chunk) bất kể kích thước bảng: tối đa ~3 chunk mỗi phía (chunk đang so, chunk trong kênh,
// chunk đang đọc).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"strand/tests/checker"
)

const (
	// DefaultCompareChunkRows là số dòng mỗi lần đọc keyset khi so sánh trạng thái cuối.
	DefaultCompareChunkRows = 10000
	// DefaultMaxViolations là số vi phạm tối đa được giữ lại (vẫn đếm tổng số).
	DefaultMaxViolations = 20
)

// cmpRow là một dòng đã đọc: PK số nguyên và giá trị các cột (nil = NULL, còn lại là string).
type cmpRow struct {
	pk   int64
	vals []any
}

// cmpChunk là một trang dữ liệu đọc được từ một phía.
type cmpChunk struct {
	cols []string
	rows []cmpRow
}

// chunkFetcher đọc tối đa limit dòng có PK > after (hoặc từ đầu nếu first) theo PK tăng dần.
type chunkFetcher func(ctx context.Context, after int64, first bool, limit int) (cmpChunk, error)

// CompareResult là kết quả so sánh streaming của nhiều bảng.
type CompareResult struct {
	Equal           bool
	Violations      []string // tối đa maxViolations phần tử
	ViolationsTotal int64
	SourceRows      int64
	TargetRows      int64
}

type chunkMsg struct {
	chunk cmpChunk
	err   error
}

// rowStream đọc trước (prefetch) các chunk của một phía trong goroutine riêng.
type rowStream struct {
	ch   chan chunkMsg
	cur  cmpChunk
	idx  int
	done bool
	err  error
}

func startRowStream(ctx context.Context, fetch chunkFetcher, chunkRows int) *rowStream {
	s := &rowStream{ch: make(chan chunkMsg, 1)}
	go func() {
		defer close(s.ch)
		var after int64
		havePrev := false
		for {
			c, err := fetch(ctx, after, !havePrev, chunkRows)
			if err == nil {
				// Keyset pagination chỉ đúng khi PK tăng nghiêm ngặt.
				for _, r := range c.rows {
					if havePrev && r.pk <= after {
						err = fmt.Errorf("keyset pagination: pk %d is not greater than previous %d", r.pk, after)
						break
					}
					after = r.pk
					havePrev = true
				}
			}
			select {
			case s.ch <- chunkMsg{chunk: c, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil || len(c.rows) < chunkRows {
				return
			}
		}
	}()
	return s
}

// peek trả về dòng hiện tại (ok=false khi hết dữ liệu).
func (s *rowStream) peek(ctx context.Context) (*cmpRow, []string, bool, error) {
	for {
		if s.err != nil {
			return nil, nil, false, s.err
		}
		if s.idx < len(s.cur.rows) {
			return &s.cur.rows[s.idx], s.cur.cols, true, nil
		}
		if s.done {
			return nil, nil, false, nil
		}
		select {
		case msg, ok := <-s.ch:
			if !ok {
				s.done = true
				s.cur = cmpChunk{}
				s.idx = 0
				continue
			}
			if msg.err != nil {
				s.err = msg.err
				continue
			}
			s.cur = msg.chunk
			s.idx = 0
		case <-ctx.Done():
			s.err = ctx.Err()
		}
	}
}

func (s *rowStream) advance() { s.idx++ }

// compareTableStreaming so sánh một bảng bằng merge hai luồng đã sắp theo PK.
func compareTableStreaming(ctx context.Context, table string, srcFetch, dstFetch chunkFetcher, chunkRows, maxViolations int, res *CompareResult) error {
	if chunkRows <= 0 {
		chunkRows = DefaultCompareChunkRows
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // dừng goroutine prefetch nếu thoát sớm do lỗi

	src := startRowStream(ctx, srcFetch, chunkRows)
	dst := startRowStream(ctx, dstFetch, chunkRows)

	addViolation := func(msg string) {
		res.Equal = false
		res.ViolationsTotal++
		if len(res.Violations) < maxViolations {
			res.Violations = append(res.Violations, msg)
		}
	}

	for {
		s, sCols, sOK, err := src.peek(ctx)
		if err != nil {
			return fmt.Errorf("read source %s: %w", table, err)
		}
		t, tCols, tOK, err := dst.peek(ctx)
		if err != nil {
			return fmt.Errorf("read target %s: %w", table, err)
		}
		switch {
		case !sOK && !tOK:
			return nil
		case sOK && (!tOK || s.pk < t.pk):
			res.SourceRows++
			addViolation(fmt.Sprintf("bảng %s pk=%d tồn tại ở source nhưng thiếu ở target", table, s.pk))
			src.advance()
		case tOK && (!sOK || t.pk < s.pk):
			res.TargetRows++
			addViolation(fmt.Sprintf("bảng %s pk=%d tồn tại ở target nhưng không có ở source", table, t.pk))
			dst.advance()
		default:
			res.SourceRows++
			res.TargetRows++
			if diff := diffRow(table, s, sCols, t, tCols); diff != "" {
				addViolation(diff)
			}
			src.advance()
			dst.advance()
		}
	}
}

// diffRow so sánh hai dòng cùng PK theo tên cột; dùng cùng ngữ nghĩa với checker.RowState.Hash
// (giá trị dạng chuỗi, NULL khác chuỗi rỗng) và liệt kê các cột lệch để dễ chẩn đoán.
func diffRow(table string, s *cmpRow, sCols []string, t *cmpRow, tCols []string) string {
	if sameCols(sCols, tCols) { // đường nhanh: cùng thứ tự cột
		equal := true
		for i := range s.vals {
			if !valueEqual(s.vals[i], t.vals[i]) {
				equal = false
				break
			}
		}
		if equal {
			return ""
		}
	}
	tIdx := make(map[string]int, len(tCols))
	for i, c := range tCols {
		tIdx[c] = i
	}
	var diffs []string
	seen := 0
	for i, c := range sCols {
		j, ok := tIdx[c]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("%s: thiếu cột ở target", c))
			continue
		}
		seen++
		if !valueEqual(s.vals[i], t.vals[j]) {
			diffs = append(diffs, fmt.Sprintf("%s: source=%s target=%s", c, fmtVal(s.vals[i]), fmtVal(t.vals[j])))
		}
	}
	if seen != len(tCols) {
		diffs = append(diffs, "target có cột không có ở source")
	}
	if len(diffs) == 0 {
		return ""
	}
	sHash := rowHash(table, s, sCols)
	tHash := rowHash(table, t, tCols)
	return fmt.Sprintf("bảng %s pk=%d lệch hash (source=%s, target=%s): %s", table, s.pk, sHash, tHash, strings.Join(diffs, "; "))
}

func sameCols(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func valueEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.(string) == b.(string)
}

func fmtVal(v any) string {
	if v == nil {
		return "NULL"
	}
	s := v.(string)
	if len(s) > 64 {
		s = s[:64] + "…"
	}
	return strconv.Quote(s)
}

// rowHash tái dùng checker.RowState.Hash cho thông điệp vi phạm (chỉ gọi khi có lệch).
func rowHash(table string, r *cmpRow, cols []string) string {
	m := make(map[string]interface{}, len(cols))
	var ver int64
	for i, c := range cols {
		m[c] = r.vals[i]
		if c == "ver" {
			if s, ok := r.vals[i].(string); ok {
				if u, err := strconv.ParseUint(s, 10, 64); err == nil {
					ver = int64(u)
				}
			}
		}
	}
	rs := checker.RowState{Table: table, PK: strconv.FormatInt(r.pk, 10), Ver: ver, Values: m}
	h := rs.Hash()
	if len(h) > 12 {
		h = h[:12]
	}
	return h
}

// ---------------------------------------------------------------------------
// Nguồn dữ liệu MySQL: keyset pagination theo PK số nguyên một cột
// ---------------------------------------------------------------------------

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func splitTable(table string) (schema, name string, err error) {
	schema, name, ok := strings.Cut(table, ".")
	if !ok || schema == "" || name == "" {
		return "", "", fmt.Errorf("table %q must be schema.table", table)
	}
	return schema, name, nil
}

// lookupIntPK tìm cột PK duy nhất (kiểu số nguyên) của bảng.
func lookupIntPK(ctx context.Context, db *sql.DB, table string) (string, error) {
	schema, name, err := splitTable(table)
	if err != nil {
		return "", err
	}
	rows, err := db.QueryContext(ctx, `SELECT k.COLUMN_NAME, c.DATA_TYPE
		FROM information_schema.KEY_COLUMN_USAGE k
		JOIN information_schema.COLUMNS c
		  ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME
		WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY k.ORDINAL_POSITION`, schema, name)
	if err != nil {
		return "", fmt.Errorf("lookup PK of %s: %w", table, err)
	}
	defer rows.Close()
	var cols, types []string
	for rows.Next() {
		var c, t string
		if err := rows.Scan(&c, &t); err != nil {
			return "", err
		}
		cols = append(cols, c)
		types = append(types, strings.ToLower(t))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(cols) != 1 {
		return "", fmt.Errorf("table %s: streaming compare needs a single-column PK, got %v", table, cols)
	}
	switch types[0] {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
	default:
		return "", fmt.Errorf("table %s: streaming compare needs an integer PK, %s is %s", table, cols[0], types[0])
	}
	return cols[0], nil
}

// mysqlChunkFetcher tạo chunkFetcher dùng keyset pagination trên *sql.DB.
func mysqlChunkFetcher(db *sql.DB, table, pkCol string) (chunkFetcher, error) {
	schema, name, err := splitTable(table)
	if err != nil {
		return nil, err
	}
	from := quoteIdent(schema) + "." + quoteIdent(name)
	pk := quoteIdent(pkCol)
	qFirst := fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT ?", from, pk)
	qNext := fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT ?", from, pk, pk)

	return func(ctx context.Context, after int64, first bool, limit int) (cmpChunk, error) {
		var rows *sql.Rows
		var err error
		if first {
			rows, err = db.QueryContext(ctx, qFirst, limit)
		} else {
			rows, err = db.QueryContext(ctx, qNext, after, limit)
		}
		if err != nil {
			return cmpChunk{}, fmt.Errorf("query %s: %w", table, err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			return cmpChunk{}, err
		}
		pkIdx := -1
		for i, c := range cols {
			if c == pkCol {
				pkIdx = i
			}
		}
		if pkIdx < 0 {
			return cmpChunk{}, fmt.Errorf("table %s: PK column %s not in result", table, pkCol)
		}
		out := cmpChunk{cols: cols, rows: make([]cmpRow, 0, limit)}
		raw := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		for rows.Next() {
			if err := rows.Scan(ptrs...); err != nil {
				return cmpChunk{}, err
			}
			pkv, err := strconv.ParseInt(string(raw[pkIdx]), 10, 64)
			if err != nil {
				return cmpChunk{}, fmt.Errorf("table %s: parse PK %q: %w", table, raw[pkIdx], err)
			}
			vals := make([]any, len(cols))
			for i, b := range raw {
				if b != nil {
					vals[i] = string(b) // copy: RawBytes chỉ hợp lệ tới lần Next kế tiếp
				}
			}
			out.rows = append(out.rows, cmpRow{pk: pkv, vals: vals})
		}
		if err := rows.Err(); err != nil {
			return cmpChunk{}, err
		}
		return out, nil
	}, nil
}

// compareFinalState so sánh trạng thái cuối các bảng giữa source và target theo kiểu streaming.
func compareFinalState(ctx context.Context, srcDB, dstDB *sql.DB, tables []string, chunkRows, maxViolations int) (CompareResult, error) {
	res := CompareResult{Equal: true}
	if maxViolations <= 0 {
		maxViolations = DefaultMaxViolations
	}
	for _, tb := range tables {
		var (
			wg             sync.WaitGroup
			srcPK, dstPK   string
			srcErr, dstErr error
		)
		wg.Add(2)
		go func() { defer wg.Done(); srcPK, srcErr = lookupIntPK(ctx, srcDB, tb) }()
		go func() { defer wg.Done(); dstPK, dstErr = lookupIntPK(ctx, dstDB, tb) }()
		wg.Wait()
		if err := errors.Join(srcErr, dstErr); err != nil {
			return res, err
		}
		if srcPK != dstPK {
			return res, fmt.Errorf("table %s: PK differs (source %s, target %s)", tb, srcPK, dstPK)
		}
		sf, err := mysqlChunkFetcher(srcDB, tb, srcPK)
		if err != nil {
			return res, err
		}
		df, err := mysqlChunkFetcher(dstDB, tb, dstPK)
		if err != nil {
			return res, err
		}
		if err := compareTableStreaming(ctx, tb, sf, df, chunkRows, maxViolations, &res); err != nil {
			return res, err
		}
	}
	return res, nil
}
