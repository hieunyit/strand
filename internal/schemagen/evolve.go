package schemagen

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"strand/internal/model"
)

// P3-01 — schema evolution: so schema bảng ở source (SAU DDL) với bảng ở target, phân loại từng khác biệt
// thành ADDITIVE (tự áp được) hoặc DESTRUCTIVE (chờ người duyệt) và sinh DDL cho target.
// Nguyên tắc: không chắc thì DESTRUCTIVE (fail safe). File này thuần (không chạm DB) để test theo bảng;
// phần đọc catalog nằm ở evolve_db.go.

// ColumnDef mô tả một cột. Với đích MySQL, Type là COLUMN_TYPE của MySQL (chữ thường); với đích PG,
// Type là kiểu PG đã chuẩn hoá (source MySQL đã đi qua typemap).
type ColumnDef struct {
	Name     string
	Type     string
	Nullable bool
	// HasDefault: cột có DEFAULT tường minh (hoặc biểu thức mặc định).
	HasDefault bool
	// Unsupported khác rỗng: cột không thể thêm/sửa tự động (auto_increment, generated, default không dịch được...).
	Unsupported string
	Charset     string // chỉ MySQL, cột chuỗi
	Collation   string
	// Def: đoạn định nghĩa cột đầy đủ để ghép vào ADD/MODIFY COLUMN (chỉ phía source cần).
	Def string
}

// IndexDef mô tả một index (không gồm PRIMARY — PK nằm ở TableDef.PK).
type IndexDef struct {
	Name   string
	Unique bool
	// Sig: chữ ký so sánh độc lập tên (u|n + cột[(prefix)][ desc],...).
	Sig string
	// Cols: danh sách cột đã quote, dùng để sinh CREATE INDEX.
	Cols string
	// Manual khác rỗng: index không sinh được DDL tự động (prefix/expression/fulltext/tên quá dài...).
	Manual string
}

// TableDef là schema một bảng ở một phía.
type TableDef struct {
	Columns []ColumnDef
	PK      string // chữ ký PK ("" = không có)
	Indexes []IndexDef
}

// ChangeKind: loại thay đổi.
type ChangeKind string

const (
	AddColumn    ChangeKind = "add_column"
	DropColumn   ChangeKind = "drop_column"
	ModifyColumn ChangeKind = "modify_column"
	ReorderCols  ChangeKind = "reorder_columns"
	ChangePK     ChangeKind = "change_primary_key"
	AddIndex     ChangeKind = "add_index"
	DropIndex    ChangeKind = "drop_index"
	ChangeIndex  ChangeKind = "change_index"
)

// Change là một thay đổi đã phân loại. ManualReason khác rỗng = Strand không tự áp được.
type Change struct {
	Kind         ChangeKind
	Column       string
	Additive     bool
	Desc         string
	Stmts        []string
	ManualReason string
}

// Evolution là kết quả so sánh một bảng.
type Evolution struct{ Changes []Change }

// Empty: không có khác biệt nào.
func (e Evolution) Empty() bool { return len(e.Changes) == 0 }

// Additive: mọi thay đổi đều cộng thêm và có DDL tự động.
func (e Evolution) Additive() bool {
	for _, c := range e.Changes {
		if !c.Additive || c.ManualReason != "" {
			return false
		}
	}
	return true
}

// Classification: "additive" hoặc "destructive".
func (e Evolution) Classification() string {
	if e.Additive() {
		return "additive"
	}
	return "destructive"
}

// Statements: DDL đề xuất theo thứ tự; thay đổi phải áp tay xuất hiện dưới dạng dòng chú thích ManualPrefix.
func (e Evolution) Statements() []string {
	var out []string
	for _, c := range e.Changes {
		if c.ManualReason != "" {
			out = append(out, ManualPrefix+c.Desc+": "+c.ManualReason)
			continue
		}
		out = append(out, c.Stmts...)
	}
	return mergeAlters(out)
}

var (
	reAlterStmt   = regexp.MustCompile(`(?s)^ALTER TABLE (\S+) (.+)$`)
	reMyDropIndex = regexp.MustCompile("(?s)^DROP INDEX (`[^`]+`) ON (\\S+)$")
	reMyCreateIdx = regexp.MustCompile("(?s)^CREATE (UNIQUE |FULLTEXT |SPATIAL )?INDEX (`[^`]+`) ON (\\S+) (\\(.+\\))$")
)

// mergeAlters gộp các câu liên tiếp trên cùng một bảng thành MỘT câu ALTER TABLE nhiều mệnh đề, để target
// nhận đúng một ALTER cho một thay đổi schema (Gate 3, online DDL: "đích nhận đúng 1 ALTER tại barrier"),
// và để MySQL không copy bảng nhiều lần. MySQL: DROP INDEX … ON t / CREATE INDEX … ON t đổi thành mệnh đề
// của ALTER TABLE t. PG: CREATE/DROP INDEX là lệnh riêng, giữ nguyên; các ALTER TABLE liên tiếp vẫn gộp.
// Câu MANUAL và câu không nhận ra giữ nguyên và ngắt việc gộp, nên thứ tự áp không đổi.
func mergeAlters(stmts []string) []string {
	var out []string
	curTbl := ""
	var clauses []string
	flush := func() {
		if curTbl != "" {
			out = append(out, "ALTER TABLE "+curTbl+" "+strings.Join(clauses, ", "))
		}
		curTbl, clauses = "", nil
	}
	for _, st := range stmts {
		tbl, clause := "", ""
		if m := reAlterStmt.FindStringSubmatch(st); m != nil {
			tbl, clause = m[1], m[2]
		} else if m := reMyDropIndex.FindStringSubmatch(st); m != nil {
			tbl, clause = m[2], "DROP INDEX "+m[1]
		} else if m := reMyCreateIdx.FindStringSubmatch(st); m != nil {
			tbl, clause = m[3], "ADD "+m[1]+"INDEX "+m[2]+" "+m[4]
		}
		if tbl == "" {
			flush()
			out = append(out, st)
			continue
		}
		if tbl != curTbl {
			flush()
			curTbl = tbl
		}
		clauses = append(clauses, clause)
	}
	flush()
	return out
}

// Describe: mô tả ngắn từng thay đổi.
func (e Evolution) Describe() string {
	parts := make([]string, len(e.Changes))
	for i, c := range e.Changes {
		k := "destructive"
		if c.Additive && c.ManualReason == "" {
			k = "additive"
		}
		parts[i] = k + " " + c.Desc
	}
	return strings.Join(parts, "; ")
}

// ManualPrefix mở đầu dòng đề xuất mà Strand không tự áp được.
const ManualPrefix = "-- MANUAL: "

// IsManualStatement: câu đề xuất là chú thích phải áp tay.
func IsManualStatement(s string) bool { return strings.HasPrefix(s, ManualPrefix) }

// DiffOptions điều khiển so sánh.
type DiffOptions struct {
	PG     bool          // target PostgreSQL (Type là kiểu PG, ADD COLUMN chỉ nối cuối)
	Target model.TableID // tên bảng đích (đã áp tables.rename)
	// LenientWiderTarget: target PG đã rộng hơn (cùng họ kiểu) thì coi là khớp — schema PG do DBA tự tạo
	// thường dùng kiểu rộng hơn typemap; không bắt họ duyệt mỗi lần DDL.
	LenientWiderTarget bool
	// IgnoreExtraIndexes: index chỉ có ở target không bị coi là DROP INDEX (PG: DBA tự thêm index).
	IgnoreExtraIndexes bool
	// ForceManual khác rỗng: mọi thay đổi đều phải áp tay (nguồn MariaDB: DDL không dịch được an toàn).
	ForceManual string
}

// Diff so source (schema mới) với dst (schema target hiện tại) và trả danh sách thay đổi theo thứ tự áp:
// DROP INDEX, DROP COLUMN, ADD COLUMN, MODIFY, CREATE INDEX.
func Diff(src, dst TableDef, o DiffOptions) Evolution {
	var dropIdx, drops, adds, mods, idxs []Change
	tbl := name(o.Target, o.PG)
	q := func(s string) string { return quote(s, o.PG) }
	dstCols := map[string]ColumnDef{}
	for _, c := range dst.Columns {
		dstCols[strings.ToLower(c.Name)] = c
	}
	srcCols := map[string]ColumnDef{}
	for _, c := range src.Columns {
		srcCols[strings.ToLower(c.Name)] = c
	}
	manual := func(c *Change, why string) {
		if c.ManualReason == "" && why != "" {
			c.ManualReason = why
			c.Stmts = nil
		}
	}

	for _, c := range dst.Columns {
		if _, ok := srcCols[strings.ToLower(c.Name)]; !ok {
			drops = append(drops, Change{Kind: DropColumn, Column: c.Name, Desc: "drop column " + c.Name,
				Stmts: []string{"ALTER TABLE " + tbl + " DROP COLUMN " + q(c.Name)}})
		}
	}
	prev := ""
	for _, c := range src.Columns {
		d, ok := dstCols[strings.ToLower(c.Name)]
		if !ok {
			ch := Change{Kind: AddColumn, Column: c.Name, Desc: "add column " + c.Name + " " + c.Type,
				Additive: (c.Nullable || c.HasDefault) && c.Unsupported == ""}
			def := "ALTER TABLE " + tbl + " ADD COLUMN " + c.Def
			if !o.PG {
				// Giữ đúng vị trí cột như source: schemaDiff so cột theo thứ tự.
				if prev == "" {
					def += " FIRST"
				} else {
					def += " AFTER " + q(prev)
				}
			}
			ch.Stmts = []string{def}
			switch {
			case c.Unsupported != "":
				manual(&ch, c.Unsupported)
			case c.Def == "":
				ch.Additive = false
				manual(&ch, "no column definition available")
			case !ch.Additive:
				ch.Desc += " (NOT NULL without default)"
			}
			adds = append(adds, ch)
			prev = c.Name
			continue
		}
		prev = c.Name
		if m, ok := diffColumn(c, d, o, tbl, q); ok {
			mods = append(mods, m)
		}
	}
	// Thứ tự cột chung khác nhau (chỉ MySQL: PG luôn nối cuối, không so thứ tự).
	if !o.PG && len(adds) == 0 && len(drops) == 0 && !sameOrder(src.Columns, dst.Columns) {
		mods = append(mods, Change{Kind: ReorderCols, Desc: "column order differs", ManualReason: "reordering columns is not applied automatically"})
	}

	if src.PK != dst.PK {
		idxs = append(idxs, Change{Kind: ChangePK, Desc: fmt.Sprintf("primary key changes (%s -> %s)", orNone(dst.PK), orNone(src.PK)),
			ManualReason: "changing the primary key is never applied automatically"})
	}
	dstIdx := map[string]IndexDef{}
	for _, i := range dst.Indexes {
		dstIdx[i.Sig] = i
	}
	srcIdx := map[string]bool{}
	for _, i := range src.Indexes {
		srcIdx[i.Sig] = true
	}
	consumed := map[string]bool{}
	for _, i := range src.Indexes {
		if _, ok := dstIdx[i.Sig]; ok {
			continue
		}
		kw := "INDEX "
		if i.Unique {
			kw = "UNIQUE INDEX "
		}
		create := "CREATE " + kw + q(i.Name) + " ON " + tbl + " (" + i.Cols + ")"
		ch := Change{Kind: AddIndex, Desc: "add index " + i.Name, Additive: !i.Unique, Stmts: []string{create}}
		if i.Unique {
			ch.Desc = "add unique index " + i.Name
		}
		// Cùng tên nhưng khác định nghĩa: DROP + CREATE (phá hủy).
		for _, d := range dst.Indexes {
			if d.Name == i.Name && !srcIdx[d.Sig] {
				consumed[d.Sig] = true
				ch.Kind, ch.Additive, ch.Desc = ChangeIndex, false, "change index "+i.Name
				ch.Stmts = []string{dropIndexStmt(d.Name, tbl, o), create}
			}
		}
		if i.Manual != "" {
			ch.Additive = false
			manual(&ch, i.Manual)
		}
		idxs = append(idxs, ch)
	}
	if !o.IgnoreExtraIndexes {
		for _, d := range dst.Indexes {
			if srcIdx[d.Sig] || consumed[d.Sig] {
				continue
			}
			dropIdx = append(dropIdx, Change{Kind: DropIndex, Desc: "drop index " + d.Name, Stmts: []string{dropIndexStmt(d.Name, tbl, o)}})
		}
	}

	var all []Change
	all = append(all, dropIdx...)
	all = append(all, drops...)
	all = append(all, adds...)
	all = append(all, mods...)
	all = append(all, idxs...)
	if o.ForceManual != "" {
		for i := range all {
			all[i].Additive = false
			manual(&all[i], o.ForceManual)
		}
	}
	return Evolution{Changes: all}
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func dropIndexStmt(n, tbl string, o DiffOptions) string {
	if o.PG {
		return "DROP INDEX " + quote(o.Target.Schema, true) + "." + quote(n, true)
	}
	return "DROP INDEX " + quote(n, false) + " ON " + tbl
}

func sameOrder(a, b []ColumnDef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i].Name, b[i].Name) {
			return false
		}
	}
	return true
}

// diffColumn so một cột có ở cả hai phía (s = source mới, d = target hiện tại).
func diffColumn(s, d ColumnDef, o DiffOptions, tbl string, q func(string) string) (Change, bool) {
	r := typeRel(d.Type, s.Type, o.PG)
	if o.PG && o.LenientWiderTarget && r == relNarrow {
		r = relSame
	}
	charsetDiffers := !o.PG && targetCharsetTooNarrow(s.Charset, d.Charset)
	nullAdd := s.Nullable && !d.Nullable  // NOT NULL → NULL: nới
	nullDrop := !s.Nullable && d.Nullable // NULL → NOT NULL: siết
	if r == relSame && !charsetDiffers && !nullAdd && !nullDrop {
		return Change{}, false
	}
	ch := Change{Kind: ModifyColumn, Column: s.Name, Additive: true}
	var parts []string
	if r != relSame {
		parts = append(parts, fmt.Sprintf("type %s -> %s", d.Type, s.Type))
		if r != relWiden {
			ch.Additive = false
		}
	}
	if charsetDiffers {
		parts = append(parts, fmt.Sprintf("charset/collation %s/%s -> %s/%s", d.Charset, d.Collation, s.Charset, s.Collation))
		ch.Additive = false
	}
	if nullAdd {
		parts = append(parts, "NOT NULL -> NULL")
	}
	if nullDrop {
		parts = append(parts, "NULL -> NOT NULL")
		ch.Additive = false
	}
	ch.Desc = "modify column " + s.Name + " (" + strings.Join(parts, ", ") + ")"
	if s.Unsupported != "" {
		ch.Additive = false
		ch.ManualReason = s.Unsupported
		return ch, true
	}
	if o.PG {
		var st []string
		if r != relSame {
			st = append(st, "ALTER TABLE "+tbl+" ALTER COLUMN "+q(s.Name)+" TYPE "+s.Type+" USING "+q(s.Name)+"::"+s.Type)
		}
		if nullAdd {
			st = append(st, "ALTER TABLE "+tbl+" ALTER COLUMN "+q(s.Name)+" DROP NOT NULL")
		}
		if nullDrop {
			st = append(st, "ALTER TABLE "+tbl+" ALTER COLUMN "+q(s.Name)+" SET NOT NULL")
		}
		ch.Stmts = st
		return ch, true
	}
	if s.Def == "" {
		ch.Additive = false
		ch.ManualReason = "no column definition available"
		return ch, true
	}
	ch.Stmts = []string{"ALTER TABLE " + tbl + " MODIFY COLUMN " + s.Def}
	return ch, true
}

// targetCharsetTooNarrow: cột target không chứa được mọi giá trị của cột source vì khác charset. Target
// utf8mb4 chứa được mọi charset nên không cần đổi; chỉ khác collation cũng không tính. Lý do: không biết
// khác biệt có sẵn từ trước DDL hay không (nguồn 5.6/5.7 mặc định latin1/utf8mb4_general_ci, đích 8.0
// mặc định utf8mb4_0900_ai_ci), và trước P3-01 barrier DDL cũng không so collation. Đổi target về charset
// hẹp hơn để "khớp" source mới là sai hướng (mất dữ liệu).
func targetCharsetTooNarrow(src, dst string) bool {
	if src == "" || dst == "" {
		return false
	}
	s, d := normCharset(src), normCharset(dst)
	return s != d && d != "utf8mb4"
}

func normCharset(s string) string {
	s = strings.ToLower(s)
	if s == "utf8" || strings.HasPrefix(s, "utf8_") { // 5.7 gọi utf8, 8.0 gọi utf8mb3
		return "utf8mb3" + strings.TrimPrefix(s, "utf8")
	}
	return s
}

type rel int

const (
	relSame   rel = iota
	relWiden      // mới rộng hơn cũ, không mất dữ liệu
	relNarrow     // mới hẹp hơn cũ (cùng họ)
	relOther      // khác họ / không so được
)

type parsedType struct {
	base string
	args []int
	rest string // phần sau ngoặc (unsigned, zerofill, ...)
	ok   bool
}

var reParseType = regexp.MustCompile(`^([a-z]+)\s*(?:\(([^)]*)\))?\s*([a-z ]*)$`)

func parseType(t string) parsedType {
	t = strings.ToLower(strings.TrimSpace(t))
	m := reParseType.FindStringSubmatch(t)
	if m == nil {
		return parsedType{base: t}
	}
	p := parsedType{base: strings.TrimSpace(m[1]), rest: strings.TrimSpace(m[3]), ok: true}
	if m[2] != "" {
		for _, a := range strings.Split(m[2], ",") {
			n, err := strconv.Atoi(strings.TrimSpace(a))
			if err != nil {
				p.ok = false
				break
			}
			p.args = append(p.args, n)
		}
	}
	return p
}

var (
	mysqlInts = map[string]int{"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4, "integer": 4, "bigint": 5}
	pgInts    = map[string]int{"smallint": 1, "integer": 2, "bigint": 3}
	textRank  = map[string]int{"tinytext": 1, "text": 2, "mediumtext": 3, "longtext": 4}
	blobRank  = map[string]int{"tinyblob": 1, "blob": 2, "mediumblob": 3, "longblob": 4}
)

func cmpRank(a, b int) rel {
	switch {
	case a == b:
		return relSame
	case b > a:
		return relWiden
	}
	return relNarrow
}

// typeRel: quan hệ từ kiểu cũ (ở target) sang kiểu mới (ở source). Chỉ trả relWiden cho các nới rộng an toàn
// đã liệt kê: số nguyên → số nguyên rộng hơn cùng dấu, VARCHAR/VARBINARY dài hơn, DECIMAL tăng precision cùng
// scale, họ TEXT/BLOB lớn hơn (PG: smallint→integer→bigint→numeric(p,0), varchar(n)→varchar(m>n)/text, numeric).
func typeRel(oldT, newT string, pg bool) rel {
	o, n := parseType(oldT), parseType(newT)
	if !o.ok || !n.ok {
		if strings.EqualFold(strings.TrimSpace(oldT), strings.TrimSpace(newT)) {
			return relSame
		}
		return relOther
	}
	if !pg {
		// Độ rộng hiển thị số nguyên không phải khác biệt kiểu (5.7 'int(11)' = 8.0 'int').
		if _, ok := mysqlInts[o.base]; ok && !strings.Contains(o.rest, "zerofill") {
			o.args = nil
		}
		if _, ok := mysqlInts[n.base]; ok && !strings.Contains(n.rest, "zerofill") {
			n.args = nil
		}
		if o.base == "integer" {
			o.base = "int"
		}
		if n.base == "integer" {
			n.base = "int"
		}
	}
	if o.base == n.base && o.rest == n.rest && fmt.Sprint(o.args) == fmt.Sprint(n.args) {
		return relSame
	}
	if pg {
		if ro, ok := pgInts[o.base]; ok {
			if rn, ok := pgInts[n.base]; ok && o.rest == n.rest {
				return cmpRank(ro, rn)
			}
			if n.base == "numeric" && len(n.args) == 2 && n.args[1] == 0 {
				return relWiden
			}
			return relOther
		}
		if o.base == "numeric" && n.base == "numeric" && len(o.args) == 2 && len(n.args) == 2 && o.args[1] == n.args[1] {
			return cmpRank(o.args[0], n.args[0])
		}
		if o.base == "varchar" && n.base == "varchar" && len(o.args) == 1 && len(n.args) == 1 {
			return cmpRank(o.args[0], n.args[0])
		}
		if o.base == "varchar" && n.base == "text" {
			return relWiden
		}
		if o.base == "text" && n.base == "varchar" {
			return relNarrow
		}
		return relOther
	}
	if o.rest != n.rest {
		return relOther
	}
	if ro, ok := mysqlInts[o.base]; ok {
		if rn, ok := mysqlInts[n.base]; ok {
			if ro == rn && fmt.Sprint(o.args) != fmt.Sprint(n.args) {
				return relOther // zerofill: độ rộng có nghĩa
			}
			return cmpRank(ro, rn)
		}
		return relOther
	}
	if (o.base == "varchar" && n.base == "varchar" || o.base == "varbinary" && n.base == "varbinary") && len(o.args) == 1 && len(n.args) == 1 {
		return cmpRank(o.args[0], n.args[0])
	}
	if ro, ok := textRank[o.base]; ok {
		if rn, ok := textRank[n.base]; ok {
			return cmpRank(ro, rn)
		}
		return relOther
	}
	if ro, ok := blobRank[o.base]; ok {
		if rn, ok := blobRank[n.base]; ok {
			return cmpRank(ro, rn)
		}
		return relOther
	}
	if (o.base == "decimal" || o.base == "numeric") && (n.base == "decimal" || n.base == "numeric") {
		op, os := decimalArgs(o.args)
		np, ns := decimalArgs(n.args)
		if os != ns {
			return relOther
		}
		return cmpRank(op, np)
	}
	return relOther
}

func decimalArgs(a []int) (p, s int) {
	switch len(a) {
	case 0:
		return 10, 0
	case 1:
		return a[0], 0
	}
	return a[0], a[1]
}

// NormalizePGType chuẩn hoá tên kiểu của format_type() về dạng typemap dùng.
func NormalizePGType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	for _, r := range [][2]string{
		{"character varying", "varchar"}, {"character", "char"},
		{" without time zone", ""}, {" with time zone", "tz"},
	} {
		t = strings.ReplaceAll(t, r[0], r[1])
	}
	// "timestamp(3)tz" → "timestamptz(3)" như typemap.
	if i := strings.Index(t, ")tz"); i > 0 {
		j := strings.Index(t, "(")
		t = t[:j] + "tz" + t[j:i+1]
	}
	return t
}
