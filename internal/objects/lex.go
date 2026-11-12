// Package objects chuyển view, procedure, function, trigger, event, user và quyền từ nguồn họ MySQL
// (MariaDB 10.6, MySQL 5.x/8.x) sang MySQL 8.x. Strand chỉ đồng bộ bảng; đây là phần còn lại.
package objects

import "strings"

type tokKind int

const (
	tkWS tokKind = iota
	tkComment
	tkIdent  // `quoted`
	tkWord   // từ khóa / định danh không quote / số
	tkString // '...' hoặc "..."
	tkPunct
)

type token struct {
	kind       tokKind
	text       string // nguyên văn
	val        string // tkIdent: tên đã bỏ quote
	start, end int
}

// isWord: token là từ (không quote) bằng k, không phân biệt hoa thường.
func (t token) isWord(k string) bool  { return t.kind == tkWord && strings.EqualFold(t.text, k) }
func (t token) isPunct(p string) bool { return t.kind == tkPunct && t.text == p }

// name trả tên định danh (backtick hoặc từ trần).
func (t token) name() (string, bool) {
	switch t.kind {
	case tkIdent:
		return t.val, true
	case tkWord:
		return t.text, true
	}
	return "", false
}

func isWordByte(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// lex tách câu lệnh SQL thành token, giữ nguyên văn để ghép lại đúng từng byte.
func lex(q string) []token {
	var ts []token
	i := 0
	for i < len(q) {
		c := q[i]
		start := i
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			for i < len(q) && (q[i] == ' ' || q[i] == '\t' || q[i] == '\n' || q[i] == '\r') {
				i++
			}
			ts = append(ts, token{kind: tkWS, text: q[start:i], start: start, end: i})
		case c == '`':
			i++
			var sb strings.Builder
			for i < len(q) {
				if q[i] == '`' {
					if i+1 < len(q) && q[i+1] == '`' {
						sb.WriteByte('`')
						i += 2
						continue
					}
					break
				}
				sb.WriteByte(q[i])
				i++
			}
			i++ // backtick đóng
			if i > len(q) {
				i = len(q)
			}
			ts = append(ts, token{kind: tkIdent, text: q[start:i], val: sb.String(), start: start, end: i})
		case c == '\'' || c == '"':
			i++
			for i < len(q) {
				if q[i] == '\\' {
					i += 2
					continue
				}
				if q[i] == c {
					if i+1 < len(q) && q[i+1] == c {
						i += 2
						continue
					}
					break
				}
				i++
			}
			i++
			if i > len(q) {
				i = len(q)
			}
			ts = append(ts, token{kind: tkString, text: q[start:i], start: start, end: i})
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			end := strings.Index(q[i+2:], "*/")
			if end < 0 {
				i = len(q)
			} else {
				i += 2 + end + 2
			}
			ts = append(ts, token{kind: tkComment, text: q[start:i], start: start, end: i})
		case c == '#' || (c == '-' && i+1 < len(q) && q[i+1] == '-' && (i+2 >= len(q) || q[i+2] == ' ' || q[i+2] == '\t' || q[i+2] == '\n')):
			for i < len(q) && q[i] != '\n' {
				i++
			}
			ts = append(ts, token{kind: tkComment, text: q[start:i], start: start, end: i})
		case isWordByte(c):
			for i < len(q) && isWordByte(q[i]) {
				i++
			}
			ts = append(ts, token{kind: tkWord, text: q[start:i], start: start, end: i})
		default:
			i++
			ts = append(ts, token{kind: tkPunct, text: q[start:i], start: start, end: i})
		}
	}
	return ts
}

// sig: chỉ số các token có nghĩa (bỏ khoảng trắng và comment).
func sig(ts []token) []int {
	var out []int
	for i, t := range ts {
		if t.kind != tkWS && t.kind != tkComment {
			out = append(out, i)
		}
	}
	return out
}

func join(ts []token) string {
	var sb strings.Builder
	for _, t := range ts {
		sb.WriteString(t.text)
	}
	return sb.String()
}

// quoteIdent bọc tên bằng backtick.
func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func quoteStr(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}

// edit là một thay thế theo khoảng token [from, to) bằng text.
type edit struct {
	from, to int
	text     string
}

// applyEdits áp các edit không chồng lấn lên danh sách token, trả chuỗi mới.
func applyEdits(ts []token, edits []edit) string {
	byFrom := map[int]edit{}
	for _, e := range edits {
		byFrom[e.from] = e
	}
	var sb strings.Builder
	for i := 0; i < len(ts); {
		if e, ok := byFrom[i]; ok {
			sb.WriteString(e.text)
			if e.to > i {
				i = e.to
			} else {
				sb.WriteString(ts[i].text)
				i++
			}
			continue
		}
		sb.WriteString(ts[i].text)
		i++
	}
	return sb.String()
}
