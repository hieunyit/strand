package pipeline

import (
	"regexp"
	"strings"

	"strand/internal/config"
	"strand/internal/model"
)

// P3-02 — nhận diện online DDL của gh-ost / pt-osc (design §7, Gate 3).
//
// Dấu hiệu theo tool (không chỉ theo tên): tên bảng tạm CHỈ được coi là bảng tạm khi bảng gốc <t> tồn tại
// cùng schema (thuộc pipeline lúc xử lý DDL, hoặc có mặt trong information_schema lúc khởi động).
//
//	gh-ost: _<t>_gho (bản sao có schema mới), _<t>_ghc (changelog), _<t>_del (bảng cũ / sentinel cut-over),
//	        cut-over: RENAME TABLE t TO _t_del, _t_gho TO t
//	pt-osc: _<t>_new (+ trigger pt_osc_<db>_<t>_ins/upd/del), cut-over: RENAME TABLE t TO _t_old, _t_new TO t
//
// Hành vi:
//   - mọi DDL chỉ tác động bảng tạm (CREATE/ALTER/DROP/TRUNCATE) và CREATE/DROP TRIGGER pt_osc_* → bỏ qua
//     (không drain, không tạo bảng tạm ở target, không dừng pipeline). Event row của bảng tạm đã bị router
//     bỏ vì bảng tạm không thuộc pipeline (p.table == nil) và không bao giờ được snapshot (loadSourceTables lọc);
//     phần row của bảng gốc trong cùng transaction (trigger pt-osc) được giữ nguyên.
//   - cut-over (đúng mẫu hai cặp, tên khớp bảng tạm của t) → coi như MỘT ALTER trên t, đi qua đường P3-01.
//   - chuỗi không khớp mẫu (bảng _gho bỏ dở, gh-ost bị hủy, RENAME lạ) → không phải cut-over: bảng tạm vẫn bị
//     bỏ qua, mọi thứ khác theo hành vi cũ (dừng nếu schema lệch). Không bao giờ mất row của bảng gốc.

type tempRole uint8

const (
	roleNew tempRole = 1 << iota // bản sao schema mới (_gho, _new) — sẽ được RENAME thành bảng gốc
	roleOld                      // bảng gốc cũ sau cut-over (_del, _old)
	roleAux                      // bảng phụ (_ghc)
)

type onlineDDL struct {
	suffix map[string]tempRole // hậu tố → vai trò, theo tool bật
	pt     bool                // pt-osc bật (trigger pt_osc_*)
	extra  []string            // extra_patterns, {table} = bảng gốc
}

// newOnlineDDL trả nil khi tắt (không tool, không extra pattern). cfg.Tools == nil: mặc định cả hai tool
// (config.Load đã điền; nhánh này cho cfg dựng tay).
func newOnlineDDL(c config.OnlineDDLConfig) *onlineDDL {
	tools := c.Tools
	if tools == nil {
		tools = []string{"gh-ost", "pt-osc"}
	}
	o := &onlineDDL{suffix: map[string]tempRole{}, extra: c.ExtraPatterns}
	for _, t := range tools {
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "gh-ost":
			o.suffix["gho"], o.suffix["ghc"], o.suffix["del"] = roleNew, roleAux, roleOld
		case "pt-osc":
			o.suffix["new"], o.suffix["old"] = roleNew, roleOld
			o.pt = true
		}
	}
	if len(o.suffix) == 0 && len(o.extra) == 0 {
		return nil
	}
	return o
}

var reTempName = regexp.MustCompile(`^_(.+)_([a-z]+)$`)
var reTimestampSuffix = regexp.MustCompile(`^(.+)_\d{14}$`)

// tempOf: name có phải bảng tạm của một bảng trong tables (cùng schema) không. Trả bảng gốc và vai trò.
// Hỗ trợ: tên đầy đủ, --timestamp-old-table của gh-ost (_t_<14 số>_del), tên bị cắt khi quá dài (gh-ost/pt-osc cắt
// <t> để vừa 64 ký tự: chỉ nhận khi phần giữa là tiền tố thật sự của đúng một bảng gốc và tên tạm ≥ 60 ký tự).
func (o *onlineDDL) tempOf(name string, tables []string) (base string, role tempRole, ok bool) {
	if o == nil {
		return "", 0, false
	}
	set := make(map[string]bool, len(tables))
	for _, t := range tables {
		set[t] = true
	}
	if m := reTempName.FindStringSubmatch(name); m != nil {
		if r, has := o.suffix[m[2]]; has {
			cands := []string{m[1]}
			if tm := reTimestampSuffix.FindStringSubmatch(m[1]); tm != nil && r == roleOld {
				cands = append(cands, tm[1])
			}
			for _, c := range cands {
				if set[c] && c != name {
					return c, r, true
				}
			}
			if len(name) >= 60 {
				found, n := "", 0
				for _, t := range tables {
					if len(t) > len(m[1]) && strings.HasPrefix(t, m[1]) {
						found, n = t, n+1
					}
				}
				if n == 1 {
					return found, r, true
				}
			}
		}
	}
	for _, pat := range o.extra {
		for _, t := range tables {
			if t != name && strings.Replace(pat, "{table}", t, 1) == name {
				return t, roleNew | roleOld | roleAux, true
			}
		}
	}
	return "", 0, false
}

type onlineKind int

const (
	onlineNone    onlineKind = iota // không phải online DDL: hành vi cũ
	onlineIgnore                    // chỉ tác động bảng tạm / trigger của tool: bỏ qua hoàn toàn
	onlineCutover                   // RENAME cut-over: một ALTER trên Base
)

type onlineResult struct {
	Kind    onlineKind
	Base    model.TableID // bảng gốc (Cutover, Ignore)
	OldName model.TableID // Cutover: bảng cũ sau rename (_t_del / _t_old)
	NewName model.TableID // Cutover: bảng mới trước rename (_t_gho / _t_new)
	Tool    string
}

var (
	reCommentBlock = regexp.MustCompile(`(?s)/\*[^!M].*?\*/|/\*\*/`)
	reAlterTable   = regexp.MustCompile(`(?is)^\s*alter\s+table\s+` + ddlIdent)
	reTruncate     = regexp.MustCompile(`(?is)^\s*truncate\s+(?:table\s+)?` + ddlIdent)
	reDropTable    = regexp.MustCompile(`(?is)^\s*drop\s+(?:temporary\s+)?tables?\s+(?:if\s+exists\s+)?(.+?)\s*(?:restrict|cascade)?\s*;?\s*$`)
	reDropTrigger  = regexp.MustCompile(`(?is)^\s*drop\s+trigger\s+(?:if\s+exists\s+)?` + ddlIdent)
	reCreateTrig   = regexp.MustCompile(`(?is)^\s*create\s+(?:definer\s*=\s*\S+\s+)?trigger\s+(?:if\s+not\s+exists\s+)?` + ddlIdent + `\s.*?\bon\s+` + ddlIdent)
	reIdentOnly    = regexp.MustCompile(`^\s*` + ddlIdent + `\s*$`)
)

// stripComments bỏ comment khối không thực thi (gh-ost chèn `/* gh-ost */` giữa các từ khóa); giữ /*! ... */.
func stripComments(s string) string {
	return strings.TrimSpace(reCommentBlock.ReplaceAllString(s, " "))
}

// classify phân loại một DDL. tables(schema) trả tên các bảng của pipeline trong schema (chỉ bảng gốc có
// thể có bảng tạm). defSchema: schema mặc định của câu lệnh.
func (o *onlineDDL) classify(ddl, defSchema string, tables func(schema string) []string) onlineResult {
	if o == nil {
		return onlineResult{}
	}
	q := stripComments(ddl)
	// temp trả bảng gốc nếu id là bảng tạm của một bảng pipeline cùng schema.
	temp := func(id model.TableID) (model.TableID, tempRole, bool) {
		b, r, ok := o.tempOf(id.Table, tables(id.Schema))
		return model.TableID{Schema: id.Schema, Table: b}, r, ok
	}
	if m := reRenameTable.FindStringSubmatch(q); m != nil {
		pairs := reRenamePair.FindAllStringSubmatch(m[1], -1)
		if len(pairs) != 2 {
			return onlineResult{}
		}
		from1, to1 := qualify(pairs[0][1], pairs[0][2], defSchema), qualify(pairs[0][3], pairs[0][4], defSchema)
		from2, to2 := qualify(pairs[1][1], pairs[1][2], defSchema), qualify(pairs[1][3], pairs[1][4], defSchema)
		// RENAME t TO _t_old, _t_new TO t
		if from1 != to2 || from1.Schema != to1.Schema || from1.Schema != from2.Schema {
			return onlineResult{}
		}
		tbl := from1
		bOld, rOld, okOld := temp(to1)
		bNew, rNew, okNew := temp(from2)
		if !okOld || !okNew || bOld != tbl || bNew != tbl || rOld&roleOld == 0 || rNew&roleNew == 0 {
			return onlineResult{}
		}
		tool := "gh-ost"
		if strings.HasSuffix(from2.Table, "_new") {
			tool = "pt-osc"
		} else if !strings.HasSuffix(from2.Table, "_gho") {
			tool = "custom"
		}
		return onlineResult{Kind: onlineCutover, Base: tbl, OldName: to1, NewName: from2, Tool: tool}
	}
	allTemp := func(ids ...model.TableID) (model.TableID, bool) {
		var base model.TableID
		for _, id := range ids {
			b, _, ok := temp(id)
			if !ok {
				return model.TableID{}, false
			}
			base = b
		}
		return base, len(ids) > 0
	}
	if m := reCreateTable.FindStringSubmatch(q); m != nil {
		if b, ok := allTemp(qualify(m[1], m[2], defSchema)); ok {
			return onlineResult{Kind: onlineIgnore, Base: b}
		}
		return onlineResult{}
	}
	for _, re := range []*regexp.Regexp{reAlterTable, reTruncate} {
		if m := re.FindStringSubmatch(q); m != nil {
			if b, ok := allTemp(qualify(m[1], m[2], defSchema)); ok {
				return onlineResult{Kind: onlineIgnore, Base: b}
			}
			return onlineResult{}
		}
	}
	if m := reDropTable.FindStringSubmatch(q); m != nil {
		var ids []model.TableID
		for _, part := range strings.Split(m[1], ",") {
			im := reIdentOnly.FindStringSubmatch(part)
			if im == nil {
				return onlineResult{}
			}
			ids = append(ids, qualify(im[1], im[2], defSchema))
		}
		if b, ok := allTemp(ids...); ok {
			return onlineResult{Kind: onlineIgnore, Base: b}
		}
		return onlineResult{}
	}
	if o.pt {
		// Trigger của pt-osc: pt_osc_<db>_<t>_(ins|upd|del) trên bảng gốc. Strand không replicate trigger.
		if m := reCreateTrig.FindStringSubmatch(q); m != nil {
			if strings.HasPrefix(unquoteIdent(trigName(m)), "pt_osc_") {
				on := qualify(m[3], m[4], defSchema)
				if containsStr(tables(on.Schema), on.Table) {
					return onlineResult{Kind: onlineIgnore, Base: on}
				}
			}
			return onlineResult{}
		}
		if m := reDropTrigger.FindStringSubmatch(q); m != nil {
			if strings.HasPrefix(unquoteIdent(trigName(m)), "pt_osc_") {
				return onlineResult{Kind: onlineIgnore}
			}
		}
	}
	return onlineResult{}
}

// trigName: tên trigger từ ddlIdent (nhóm 1 = schema hoặc tên, nhóm 2 = tên nếu có schema).
func trigName(m []string) string {
	if m[2] != "" {
		return m[2]
	}
	return m[1]
}

func containsStr(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
