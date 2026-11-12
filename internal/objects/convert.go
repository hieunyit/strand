package objects

import (
	"fmt"
	"strings"
)

// Options điều khiển việc chuyển định nghĩa.
type Options struct {
	Include, Exclude []string
	Rename           map[string]string // "schema.table" → "schema.table"
	KeepDefiner      bool
	// WithUsers: đọc cả user/quyền. SelfUser: user của Strand (bỏ qua). Auth: cách tạo mật khẩu.
	WithUsers bool
	SelfUser  string
	Auth      AuthMode
}

// kindKeyword: từ khóa nhận diện loại đối tượng trong header CREATE.
func kindKeyword(k Kind) string { return strings.ToUpper(string(k)) }

// header mô tả vị trí các phần của câu CREATE (chỉ số theo ts).
type header struct {
	kw       int // chỉ số token từ khóa loại (VIEW/PROCEDURE/...)
	nameFrom int // token đầu của tên đối tượng
	nameTo   int // token sau tên (exclusive)
	defFrom  int // [defFrom, defTo) là mệnh đề DEFINER=..., -1 nếu không có
	defTo    int
}

// parseHeader tìm từ khóa loại, tên đối tượng và DEFINER trong header của câu CREATE.
func parseHeader(ts []token, k Kind) (header, bool) {
	h := header{defFrom: -1, defTo: -1}
	s := sig(ts)
	want := kindKeyword(k)
	pos := -1
	for n, i := range s {
		if ts[i].isWord(want) {
			pos = n
			break
		}
		if n > 12 { // header luôn ngắn; tránh nhầm với từ trong thân
			break
		}
	}
	if pos < 0 {
		return h, false
	}
	// DEFINER=... nằm trước từ khóa loại.
	for n := 0; n < pos; n++ {
		if ts[s[n]].isWord("DEFINER") && n+1 < len(s) && ts[s[n+1]].isPunct("=") {
			h.defFrom = s[n]
			m := n + 2
			if m >= len(s) {
				return h, false
			}
			if ts[s[m]].isWord("CURRENT_USER") {
				if m+2 < len(s) && ts[s[m+1]].isPunct("(") && ts[s[m+2]].isPunct(")") {
					m += 2
				}
			} else if m+2 < len(s) && ts[s[m+1]].isPunct("@") {
				m += 2
			}
			h.defTo = s[m] + 1
			break
		}
	}
	h.kw = s[pos]
	m := pos + 1
	// bỏ IF NOT EXISTS
	if m+2 < len(s) && ts[s[m]].isWord("IF") && ts[s[m+1]].isWord("NOT") && ts[s[m+2]].isWord("EXISTS") {
		m += 3
	}
	if m >= len(s) {
		return h, false
	}
	h.nameFrom = s[m]
	if _, ok := ts[s[m]].name(); !ok {
		return h, false
	}
	h.nameTo = s[m] + 1
	if m+2 < len(s) && ts[s[m+1]].isPunct(".") {
		if _, ok := ts[s[m+2]].name(); ok {
			h.nameTo = s[m+2] + 1
		}
	}
	return h, true
}

// dropDefinerEdit trả edit xóa DEFINER=... cùng một khoảng trắng đi kèm.
func dropDefinerEdit(ts []token, h header) []edit {
	if h.defFrom < 0 {
		return nil
	}
	to := h.defTo
	if to < len(ts) && ts[to].kind == tkWS {
		to++
	}
	return []edit{{from: h.defFrom, to: to}}
}

// qualifiedRenames tìm cặp `schema`.`table` (hoặc không quote) khớp khóa rename, trả các edit thay bằng tên mới.
// Không đụng vào cặp đứng sau dấu '.' (đó là cột của bảng đã có tên đầy đủ).
func qualifiedRenames(ts []token, rename map[string]string, skip func(i int) bool) []edit {
	if len(rename) == 0 {
		return nil
	}
	var out []edit
	s := sig(ts)
	for n := 0; n+2 < len(s); n++ {
		a, b, c := ts[s[n]], ts[s[n+1]], ts[s[n+2]]
		if !b.isPunct(".") {
			continue
		}
		if n > 0 && ts[s[n-1]].isPunct(".") {
			continue
		}
		if skip != nil && skip(s[n]) {
			continue
		}
		sc, ok1 := a.name()
		tb, ok2 := c.name()
		if !ok1 || !ok2 {
			continue
		}
		if to, ok := rename[sc+"."+tb]; ok {
			if ts2, tt, ok := strings.Cut(to, "."); ok {
				out = append(out, edit{from: s[n], to: s[n+2] + 1, text: quoteIdent(ts2) + "." + quoteIdent(tt)})
			}
		}
	}
	return out
}

// bareRenameWarnings: tên bảng bị rename xuất hiện không kèm schema trong thân (không tự sửa được vì không
// biết đó là bảng hay cột/biến cùng tên).
func bareRenameWarnings(ts []token, schema string, rename map[string]string) []string {
	var out []string
	s := sig(ts)
	seen := map[string]bool{}
	for key, to := range rename {
		sc, tb, ok := strings.Cut(key, ".")
		if !ok || sc != schema || to == key {
			continue
		}
		for n, i := range s {
			nm, ok := ts[i].name()
			if !ok || nm != tb || seen[key] {
				continue
			}
			if n > 0 && ts[s[n-1]].isPunct(".") || n+1 < len(s) && ts[s[n+1]].isPunct(".") {
				continue
			}
			seen[key] = true
			out = append(out, fmt.Sprintf("refers to %q without a schema and %s is renamed to %s on the target: check the name inside the definition", tb, key, to))
		}
	}
	return out
}

// Chuyển một câu SHOW CREATE thành câu CREATE cho target.
// name đã là tên cuối (sau rename); schema là schema đích.
func convertCreate(create string, k Kind, targetSchema, targetName string, keepDefiner bool, rename map[string]string, srcSchema string) (stmt string, warns []string, err error) {
	ts := lex(create)
	h, ok := parseHeader(ts, k)
	if !ok {
		return "", nil, fmt.Errorf("cannot parse the CREATE %s statement", strings.ToUpper(string(k)))
	}
	var edits []edit
	if !keepDefiner {
		edits = append(edits, dropDefinerEdit(ts, h)...)
	}
	edits = append(edits, edit{from: h.nameFrom, to: h.nameTo, text: quoteIdent(targetSchema) + "." + quoteIdent(targetName)})
	// Rename qualified: bỏ phần header (tên đối tượng) khỏi tìm kiếm.
	edits = append(edits, qualifiedRenames(ts, rename, func(i int) bool { return i >= h.nameFrom && i < h.nameTo })...)
	if k != KindView {
		warns = append(warns, bareRenameWarnings(ts, srcSchema, rename)...)
	}
	return strings.TrimSpace(applyEdits(ts, edits)), warns, nil
}

// triggerTable trả token bảng sau "ON" trong header TRIGGER (chỉ số [from,to)).
func triggerTableSpan(ts []token) (from, to int, ok bool) {
	s := sig(ts)
	for n, i := range s {
		if n > 20 {
			break
		}
		if ts[i].isWord("ON") && n+1 < len(s) {
			if _, ok := ts[s[n+1]].name(); !ok {
				return 0, 0, false
			}
			from, to = s[n+1], s[n+1]+1
			if n+3 < len(s) && ts[s[n+2]].isPunct(".") {
				to = s[n+3] + 1
			}
			return from, to, true
		}
	}
	return 0, 0, false
}

// convertTrigger như convertCreate nhưng sửa cả tên bảng sau ON.
func convertTrigger(create, targetSchema, name, targetTable string, keepDefiner bool, rename map[string]string, srcSchema string) (string, []string, error) {
	ts := lex(create)
	h, ok := parseHeader(ts, KindTrigger)
	if !ok {
		return "", nil, fmt.Errorf("cannot parse the CREATE TRIGGER statement")
	}
	tf, tt, ok := triggerTableSpan(ts)
	if !ok {
		return "", nil, fmt.Errorf("cannot find the table of the trigger")
	}
	var edits []edit
	if !keepDefiner {
		edits = append(edits, dropDefinerEdit(ts, h)...)
	}
	edits = append(edits, edit{from: h.nameFrom, to: h.nameTo, text: quoteIdent(targetSchema) + "." + quoteIdent(name)})
	edits = append(edits, edit{from: tf, to: tt, text: quoteIdent(targetTable)})
	edits = append(edits, qualifiedRenames(ts, rename, func(i int) bool {
		return i >= h.nameFrom && i < h.nameTo || i >= tf && i < tt
	})...)
	return strings.TrimSpace(applyEdits(ts, edits)), bareRenameWarnings(ts, srcSchema, rename), nil
}

// eventDisabled đổi trạng thái event trong câu CREATE thành DISABLE (event trên target chỉ chạy sau cutover).
func eventDisabled(stmt string) (string, error) {
	ts := lex(stmt)
	s := sig(ts)
	doAt := -1
	for n, i := range s {
		if ts[i].isWord("DO") {
			doAt = n
			break
		}
	}
	if doAt < 0 {
		return "", fmt.Errorf("cannot find DO in the CREATE EVENT statement")
	}
	var edits []edit
	found := false
	for n := 0; n < doAt; n++ {
		i := s[n]
		if ts[i].isWord("ENABLE") || ts[i].isWord("DISABLE") {
			// DISABLE ON SLAVE|REPLICA → DISABLE
			to := i + 1
			if n+2 < doAt && ts[s[n+1]].isWord("ON") && (ts[s[n+2]].isWord("SLAVE") || ts[s[n+2]].isWord("REPLICA")) {
				to = s[n+2] + 1
			}
			edits = append(edits, edit{from: i, to: to, text: "DISABLE"})
			found = true
			break
		}
	}
	if !found {
		// Chèn DISABLE trước COMMENT (nếu có) hoặc DO.
		at := s[doAt]
		for n := 0; n < doAt; n++ {
			if ts[s[n]].isWord("COMMENT") {
				at = s[n]
				break
			}
		}
		edits = append(edits, edit{from: at, to: at, text: "DISABLE "})
	}
	return applyEdits(ts, edits), nil
}

// collapseWS gom mọi khoảng trắng ngoài chuỗi thành một dấu cách: dùng để so sánh định nghĩa.
func collapseWS(stmt string) string {
	var sb strings.Builder
	for _, t := range lex(stmt) {
		switch t.kind {
		case tkWS:
			sb.WriteByte(' ')
		case tkComment:
		default:
			sb.WriteString(t.text)
		}
	}
	return strings.TrimSpace(sb.String())
}

// Canonical đưa câu CREATE (của source đã chuyển, hoặc SHOW CREATE của target) về dạng so sánh được:
// bỏ DEFINER, bỏ schema trước tên đối tượng, bỏ trạng thái event, gom khoảng trắng.
func Canonical(stmt string, k Kind) string {
	ts := lex(stmt)
	h, ok := parseHeader(ts, k)
	if !ok {
		return collapseWS(stmt)
	}
	name := ""
	{
		// tên cuối của [schema.]name
		last := h.nameTo - 1
		if n, ok := ts[last].name(); ok {
			name = n
		}
	}
	edits := dropDefinerEdit(ts, h)
	edits = append(edits, edit{from: h.nameFrom, to: h.nameTo, text: quoteIdent(name)})
	out := applyEdits(ts, edits)
	if k == KindTrigger {
		ts2 := lex(out)
		if f, t, ok := triggerTableSpan(ts2); ok {
			if n, ok := ts2[t-1].name(); ok {
				out = applyEdits(ts2, []edit{{from: f, to: t, text: quoteIdent(n)}})
			}
		}
	}
	if k == KindEvent {
		if d, err := eventDisabled(out); err == nil {
			out = d
		}
	}
	return collapseWS(out)
}

// modernSQLModes: sql_mode MySQL 8.x còn nhận. Các mode khác của MariaDB (ORACLE, NO_AUTO_CREATE_USER, ...) bị bỏ và báo.
var modernSQLModes = map[string]bool{
	"ALLOW_INVALID_DATES": true, "ANSI_QUOTES": true, "ERROR_FOR_DIVISION_BY_ZERO": true, "HIGH_NOT_PRECEDENCE": true,
	"IGNORE_SPACE": true, "NO_AUTO_VALUE_ON_ZERO": true, "NO_BACKSLASH_ESCAPES": true, "NO_DIR_IN_CREATE": true,
	"NO_ENGINE_SUBSTITUTION": true, "NO_UNSIGNED_SUBTRACTION": true, "NO_ZERO_DATE": true, "NO_ZERO_IN_DATE": true,
	"ONLY_FULL_GROUP_BY": true, "PAD_CHAR_TO_FULL_LENGTH": true, "PIPES_AS_CONCAT": true, "REAL_AS_FLOAT": true,
	"STRICT_ALL_TABLES": true, "STRICT_TRANS_TABLES": true, "TIME_TRUNCATE_FRACTIONAL": true,
}

// FilterSQLMode giữ mode MySQL 8.x hiểu; trả thêm danh sách mode bị bỏ.
func FilterSQLMode(mode string) (kept string, dropped []string) {
	var keep []string
	for _, m := range strings.Split(mode, ",") {
		m = strings.TrimSpace(strings.ToUpper(m))
		if m == "" {
			continue
		}
		if modernSQLModes[m] {
			keep = append(keep, m)
		} else if m != "NO_AUTO_CREATE_USER" { // mặc định của MariaDB/5.x, vô hại khi bỏ
			dropped = append(dropped, m)
		}
	}
	return strings.Join(keep, ","), dropped
}

var mariaDBFuncs = map[string]bool{
	"NVL": true, "NVL2": true, "DECODE_ORACLE": true, "TO_CHAR": true, "ADD_MONTHS": true, "SYS_GUID": true,
	"JSON_DETAILED": true, "JSON_LOOSE": true, "JSON_EXISTS": true, "JSON_QUERY": true, "UUID_SHORT": true,
	"NATURAL_SORT_KEY": true, "COLUMN_CREATE": true, "COLUMN_GET": true, "COLUMN_JSON": true, "COLUMN_ADD": true,
	"COLUMN_DELETE": true, "COLUMN_EXISTS": true, "COLUMN_LIST": true, "NEXTVAL": true, "LASTVAL": true, "SETVAL": true,
	"WSREP_LAST_SEEN_GTID": true, "WSREP_SYNC_WAIT_UPTO_GTID": true, "REGEXP_REPLACE_ORACLE": true,
	"SUBSTR_ORACLE": true, "TRIM_ORACLE": true, "LPAD_ORACLE": true, "RPAD_ORACLE": true, "CHR": true, "ROWNUM": true,
	"INET6_ATON_MARIADB": true, "KDF": true, "ROW_NUMBER_ORACLE": true, "MEDIAN": true, "DEFAULT_ORACLE": true,
}

// DetectMariaDBOnly báo cú pháp riêng của MariaDB mà MySQL 8.x không hiểu. Chỉ xét token ngoài chuỗi/comment.
func DetectMariaDBOnly(stmt string, k Kind, sqlMode string) []string {
	ts := lex(stmt)
	s := sig(ts)
	found := map[string]bool{}
	for _, t := range ts {
		if t.kind == tkComment && (strings.HasPrefix(t.text, "/*M!") || strings.HasPrefix(t.text, "/*!M")) {
			found["MariaDB-only versioned comment "+firstN(t.text, 24)+" (MySQL ignores it)"] = true
		}
	}
	for n, i := range s {
		t := ts[i]
		if t.kind != tkWord {
			continue
		}
		next := func(d int) (token, bool) {
			if n+d < len(s) {
				return ts[s[n+d]], true
			}
			return token{}, false
		}
		up := strings.ToUpper(t.text)
		switch {
		case up == "BEGIN":
			if a, ok := next(1); ok && a.isWord("NOT") {
				if b, ok := next(2); ok && b.isWord("ATOMIC") {
					found["BEGIN NOT ATOMIC (MariaDB compound statement)"] = true
				}
			}
		case up == "PACKAGE" && n <= 6:
			found["PACKAGE (MariaDB package)"] = true
		case up == "AGGREGATE" && n <= 4:
			found["CREATE AGGREGATE FUNCTION (MariaDB stored aggregate)"] = true
		case up == "RETURNING":
			found["RETURNING clause"] = true
		case up == "NEXT":
			if a, ok := next(1); ok && a.isWord("VALUE") {
				if b, ok := next(2); ok && b.isWord("FOR") {
					found["NEXT VALUE FOR (sequence)"] = true
				}
			}
		case up == "PREVIOUS":
			if a, ok := next(1); ok && a.isWord("VALUE") {
				found["PREVIOUS VALUE FOR (sequence)"] = true
			}
		case up == "OR":
			if a, ok := next(1); ok && a.isWord("REPLACE") && k != KindView && n <= 3 {
				found["CREATE OR REPLACE (MySQL supports it only for views)"] = true
			}
		case up == "INET6" || up == "INET4":
			found[up+" type (MariaDB-only)"] = true
		case up == "UUID":
			if a, ok := next(1); !ok || !a.isPunct("(") {
				found["UUID type (MariaDB-only)"] = true
			}
		case up == "SYSTEM_TIME":
			found["SYSTEM_TIME (system-versioned table query)"] = true
		}
		if mariaDBFuncs[up] {
			if a, ok := next(1); ok && a.isPunct("(") {
				found[up+"() (MariaDB-only function)"] = true
			}
		}
	}
	if strings.Contains(strings.ToUpper(sqlMode), "ORACLE") {
		found["sql_mode contains ORACLE (Oracle-compatible routine)"] = true
	}
	var out []string
	for _, w := range sortedKeys(found) {
		out = append(out, "MariaDB-only syntax: "+w)
	}
	return out
}

func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// viewDeps tìm view khác mà thân view tham chiếu: cặp `schema`.`name` trùng view đã biết, hoặc tên trần
// (cùng schema) đứng ngay sau FROM / JOIN / dấu phẩy.
func viewDeps(create, schema string, views map[string]bool) []string {
	ts := lex(create)
	s := sig(ts)
	// chỉ xét phần sau AS đầu tiên (thân view)
	start := 0
	for n, i := range s {
		if ts[i].isWord("AS") {
			start = n + 1
			break
		}
	}
	deps := map[string]bool{}
	for n := start; n < len(s); n++ {
		t := ts[s[n]]
		nm, ok := t.name()
		if !ok {
			continue
		}
		if n+2 < len(s) && ts[s[n+1]].isPunct(".") && !(n > 0 && ts[s[n-1]].isPunct(".")) {
			if tb, ok := ts[s[n+2]].name(); ok && views[nm+"."+tb] {
				deps[nm+"."+tb] = true
			}
			continue
		}
		if n > 0 && ts[s[n-1]].isPunct(".") {
			continue
		}
		if t.kind == tkWord && n+1 < len(s) && ts[s[n+1]].isPunct("(") {
			continue
		}
		if n > 0 {
			p := ts[s[n-1]]
			if (p.isWord("FROM") || p.isWord("JOIN") || p.isPunct(",")) && views[schema+"."+nm] {
				deps[schema+"."+nm] = true
			}
		}
	}
	return sortedKeys(deps)
}
