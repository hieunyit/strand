package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"strand/internal/mysqlver"
)

// Ánh xạ collation MariaDB → MySQL 8.x cho collation mà đích không có.
//
//	Exact (so sánh và thứ tự y hệt nguồn, dùng được cho mọi cột kể cả PK/UNIQUE):
//	  utf8mb4_nopad_bin → utf8mb4_0900_bin   (cùng NO PAD, so theo code point; có test với DB thật)
//	Approximate (khác phép so sánh: PAD SPACE ↔ NO PAD, UCA 14.0.0 ↔ 9.0.0, độ nhạy dấu/hoa):
//	  chỉ dùng cho cột KHÔNG thuộc PRIMARY KEY / UNIQUE index, vì phép so sánh khác có thể gộp hai khóa
//	  khác nhau ở nguồn thành một ở đích (mất dữ liệu).
//	  X_nopad_ci/bin (general, unicode, unicode_520, latin1_*...) → X_ci/bin  (bỏ NO PAD; cùng trọng số)
//	  utf8mb4_uca1400_{ai_ci,as_ci,as_cs} → utf8mb4_0900_{ai_ci,as_ci,as_cs}; ai_cs → 0900_as_cs
//	  utf8mb3_uca1400_*ci → utf8mb3_unicode_ci; *cs → utf8mb3_bin
//
// Bảng này là nơi duy nhất cần sửa khi thêm ánh xạ.

// CollationMapping là kết quả ánh xạ một collation nguồn.
type CollationMapping struct {
	To    string // collation đích
	Exact bool   // true: tương đương hoàn toàn, dùng được cho khóa
}

var uca1400Re = regexp.MustCompile(`^(utf8mb4|utf8mb3|utf8)_uca1400_(?:[a-z0-9]+_)?(?:nopad_)?(ai|as)_(ci|cs)$`)

// MapMariaDBCollation trả collation đích cho collation nguồn src. targetHas báo đích có collation đó không.
// exactPeers=false (nguồn MariaDB < 10.5): cặp tương đương coi là gần đúng, vì nopad_bin của 10.3/10.4 coi 'a' và
// 'a\x00' là bằng nhau (lỗi đã sửa ở 10.5; e2e TestMDB_E2E_CollationExactEquivalence).
// ok=false: không có ánh xạ (và src không có trên đích). Collation đích đã có sẵn trả về chính nó (Exact).
func MapMariaDBCollation(src string, targetHas func(string) bool, exactPeers bool) (m CollationMapping, ok bool) {
	low := strings.ToLower(src)
	if targetHas(low) {
		return CollationMapping{To: src, Exact: true}, true
	}
	if peer, ok := mysqlver.ExactCollationPeer(low); ok && targetHas(peer) {
		return CollationMapping{To: peer, Exact: exactPeers}, true
	}
	var cands []string
	if g := uca1400Re.FindStringSubmatch(low); g != nil {
		cs, acc, cs2 := g[1], g[2], g[3]
		if cs == "utf8mb4" {
			if acc == "ai" && cs2 == "cs" {
				acc = "as" // MySQL không có ai_cs: giữ phân biệt hoa/thường (chặt hơn nguồn)
			}
			cands = []string{"utf8mb4_0900_" + acc + "_" + cs2}
		} else if cs2 == "ci" {
			cands = []string{"utf8mb3_unicode_ci"}
		} else {
			cands = []string{"utf8mb3_bin"}
		}
	} else if strings.Contains(low, "_nopad_") {
		cands = []string{strings.Replace(low, "_nopad_", "_", 1)}
	}
	for _, c := range cands {
		if targetHas(c) {
			return CollationMapping{To: c}, true
		}
	}
	return CollationMapping{}, false
}

// CollationPlan là kết quả phân tích collation của một bảng MariaDB.
type CollationPlan struct {
	Rewrite  map[string]string // collation nguồn (chữ thường) → collation đích, chỉ mục cần đổi tên
	Warnings []string
}

// PlanMariaDBCollations phân tích mọi collation của bảng schema.table trên nguồn: cột, mặc định bảng. Từ chối
// (exit 3) khi collation không có ánh xạ, hoặc cột thuộc PRIMARY KEY/UNIQUE index chỉ có ánh xạ gần đúng.
func PlanMariaDBCollations(ctx context.Context, src *sql.DB, targetHas func(string) bool, schema, table string) (*CollationPlan, error) {
	ver, err := mysqlver.Cached(ctx, src)
	if err != nil {
		return nil, err
	}
	exactPeers := ver.AtLeast(10, 5, 0)
	keyCols := map[string]bool{}
	kr, err := src.QueryContext(ctx, `SELECT COLUMN_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("read unique keys of %s.%s: %w", schema, table, err)
	}
	for kr.Next() {
		var c sql.NullString
		if err := kr.Scan(&c); err != nil {
			kr.Close()
			return nil, err
		}
		keyCols[strings.ToLower(c.String)] = true
	}
	kr.Close()
	if err := kr.Err(); err != nil {
		return nil, err
	}
	var cols []colColl
	cr, err := src.QueryContext(ctx, `SELECT COLUMN_NAME, COLLATION_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLLATION_NAME IS NOT NULL ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("read collations of %s.%s: %w", schema, table, err)
	}
	for cr.Next() {
		var c colColl
		if err := cr.Scan(&c.name, &c.coll); err != nil {
			cr.Close()
			return nil, err
		}
		c.key = keyCols[strings.ToLower(c.name)]
		cols = append(cols, c)
	}
	cr.Close()
	if err := cr.Err(); err != nil {
		return nil, err
	}
	var tableColl sql.NullString
	if err := src.QueryRowContext(ctx, `SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
		schema, table).Scan(&tableColl); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("read table collation of %s.%s: %w", schema, table, err)
	}
	return planCollations(schema+"."+table, targetHas, exactPeers, cols, tableColl.String)
}

// colColl: một cột có collation; key: thuộc PRIMARY KEY/UNIQUE.
type colColl struct {
	name, coll string
	key        bool
}

// planCollations: phần thuần (test không cần DB). tbl: "schema.table".
func planCollations(tbl string, targetHas func(string) bool, exactPeers bool, cols []colColl, tableColl string) (*CollationPlan, error) {
	p := &CollationPlan{Rewrite: map[string]string{}}
	var unmapped, keyBad []string
	note := func(what, c string, m CollationMapping) {
		if m.To != c {
			p.Rewrite[strings.ToLower(c)] = m.To
			if !m.Exact {
				p.Warnings = append(p.Warnings, fmt.Sprintf("%s %s: source collation %s mapped to %s (approximate: comparison and sort order differ from the source)", tbl, what, c, m.To))
			}
		}
	}
	for _, c := range cols {
		if targetHas(strings.ToLower(normalizeCollationName(c.coll))) {
			continue
		}
		m, ok := MapMariaDBCollation(normalizeCollationName(c.coll), targetHas, exactPeers)
		switch {
		case !ok:
			unmapped = append(unmapped, fmt.Sprintf("%s.%s (%s)", tbl, c.name, c.coll))
		case !m.Exact && c.key:
			keyBad = append(keyBad, fmt.Sprintf("%s.%s uses %s, nearest target collation %s compares differently", tbl, c.name, c.coll, m.To))
		default:
			note("column "+c.name, c.coll, m)
		}
	}
	if tableColl != "" && !targetHas(strings.ToLower(normalizeCollationName(tableColl))) {
		if m, ok := MapMariaDBCollation(normalizeCollationName(tableColl), targetHas, exactPeers); !ok {
			unmapped = append(unmapped, fmt.Sprintf("%s (table default %s)", tbl, tableColl))
		} else {
			note("table default", tableColl, m)
		}
	}
	if len(unmapped) > 0 {
		sort.Strings(unmapped)
		return nil, reject("source collation(s) not available on the target and without a mapping: %s; convert these columns/tables on the source to a collation MySQL has (e.g. utf8mb4_general_ci, utf8mb4_bin) or exclude the tables",
			strings.Join(unmapped, "; "))
	}
	if len(keyBad) > 0 {
		sort.Strings(keyBad)
		return nil, reject("column(s) in a PRIMARY KEY or UNIQUE index use a MariaDB collation without an exact MySQL equivalent: %s; a different comparison could merge distinct keys on the target. Change the column collation on the source (e.g. utf8mb4_general_ci, utf8mb4_bin, utf8mb4_nopad_bin) or exclude the table",
			strings.Join(keyBad, "; "))
	}
	sort.Strings(p.Warnings)
	return p, nil
}

// normalizeCollationName: "utf8_*" của MariaDB < 10.6 là "utf8mb3_*" trên MySQL 8.x.
func normalizeCollationName(c string) string {
	if len(c) >= 5 && strings.EqualFold(c[:5], "utf8_") {
		return "utf8mb3_" + c[5:]
	}
	return c
}

// TargetCollations đọc tên collation (chữ thường) của đích MySQL.
func TargetCollations(ctx context.Context, dst *sql.DB) (func(string) bool, error) {
	rows, err := dst.QueryContext(ctx, "SELECT COLLATION_NAME FROM information_schema.COLLATIONS")
	if err != nil {
		return nil, fmt.Errorf("read target collations: %w", err)
	}
	defer rows.Close()
	has := map[string]bool{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		has[strings.ToLower(c)] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return func(c string) bool { return has[strings.ToLower(c)] }, nil
}

// rewriteCollations đổi tên collation trong DDL (COLLATE x và COLLATE=x của bảng) theo rewrite. Hàm thuần.
func rewriteCollations(ddl string, rewrite map[string]string) (string, error) {
	if len(rewrite) == 0 {
		return ddl, nil
	}
	ts, err := lex(ddl)
	if err != nil {
		return "", err
	}
	type span struct {
		from, to int
		text     string
	}
	var edits []span
	for i := 0; i < len(ts); i++ {
		if !ts[i].keyword(ddl, "COLLATE") {
			continue
		}
		j := i + 1
		if j < len(ts) && ts[j].value == "=" {
			j++
		}
		if j >= len(ts) {
			break
		}
		if to, ok := rewrite[strings.ToLower(ts[j].value)]; ok {
			edits = append(edits, span{ts[j].start, ts[j].end, to})
		}
	}
	for k := len(edits) - 1; k >= 0; k-- {
		e := edits[k]
		ddl = ddl[:e.from] + e.text + ddl[e.to:]
	}
	return ddl, nil
}

// MapDatabaseCollation ánh xạ collation mặc định của database cho CREATE DATABASE trên đích; trả cảnh báo khi gần đúng.
// Không có ánh xạ thì giữ nguyên (CREATE DATABASE sẽ lỗi rõ ràng trên đích).
func MapDatabaseCollation(schema, coll string, targetHas func(string) bool, exactPeers bool) (string, []string) {
	n := normalizeCollationName(coll)
	if coll == "" || targetHas(strings.ToLower(n)) {
		return coll, nil
	}
	m, ok := MapMariaDBCollation(n, targetHas, exactPeers)
	if !ok {
		return coll, nil
	}
	if m.Exact {
		return m.To, nil
	}
	return m.To, []string{fmt.Sprintf("database %s: default collation %s mapped to %s (approximate)", schema, coll, m.To)}
}

// MariaDBCharsetDefault trả collation mặc định thật của charset trên nguồn MariaDB: bảng
// COLLATION_CHARACTER_SET_APPLICABILITY (11.x; COLLATIONS ở 11.x chỉ có tên chung không charset) hoặc
// COLLATIONS.IS_DEFAULT (10.x), rồi biến @@character_set_collations (11.2+) ghi đè. Không đọc được thì
// dùng <charset>_general_ci.
func MariaDBCharsetDefault(ctx context.Context, db *sql.DB, charset string) (string, error) {
	def := ""
	var c sql.NullString
	err := db.QueryRowContext(ctx, `SELECT FULL_COLLATION_NAME FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY
		WHERE CHARACTER_SET_NAME=? AND IS_DEFAULT='Yes'`, charset).Scan(&c)
	if err != nil {
		// 10.x: không có cột FULL_COLLATION_NAME.
		err = db.QueryRowContext(ctx, `SELECT COLLATION_NAME FROM information_schema.COLLATIONS
			WHERE CHARACTER_SET_NAME=? AND IS_DEFAULT='Yes'`, charset).Scan(&c)
	}
	if err == nil && c.Valid {
		def = c.String
	}
	var cc sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.character_set_collations").Scan(&cc); err == nil && cc.Valid {
		// "utf8mb4=utf8mb4_uca1400_ai_ci,utf8mb3=..."; collation có thể ghi dạng chung "uca1400_ai_ci".
		for _, kv := range strings.Split(cc.String, ",") {
			k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
			if ok && strings.EqualFold(k, charset) && v != "" {
				if !strings.HasPrefix(strings.ToLower(v), strings.ToLower(charset)+"_") {
					v = charset + "_" + v
				}
				def = v
			}
		}
	}
	if def == "" {
		def = charset + "_general_ci"
	}
	return def, nil
}
