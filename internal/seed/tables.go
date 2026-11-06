package seed

import (
	"fmt"
	"strings"

	"strand/internal/model"
)

// spec: 02 §9 (seed.json "tables"); review N06
//
// Một cách hiểu DUY NHẤT cho trường "tables" của seed.json PG, dùng chung cho planner (kiểm cú pháp),
// `seed apply` (kiểm bảng đã có ở target) và `strand sync --start-at seed` (bảng nào đã có trong seed).
// Trước đây planner vật lý ghi ["*"] (cả cluster) còn sync so khớp tên chính xác → "*" không khớp bảng
// nào → mọi bảng bị snapshot logic lại sau khi đã restore vật lý đầy đủ (review N06).
//
// Quy ước:
//   - danh sách rỗng, hoặc có phần tử đúng bằng "*" → mọi bảng (mọi schema). Đây là thứ planner vật lý
//     (pgbackrest / walg) ghi khi không có --tables: restore vật lý chứa cả cluster.
//   - phần tử khác là pattern kiểu `pg_dump -t` (psql "Patterns"), vì `strand export` chuyển nguyên văn
//     --tables sang `pg_dump -t`: "schema.table"; chữ không nằm trong ngoặc kép bị hạ về chữ thường
//     (ASCII, như PG fold identifier); "..." giữ nguyên hoa/thường và làm `*`, `?`, `.` thành ký tự
//     thường; `""` trong ngoặc kép là một dấu `"`; `*` khớp chuỗi bất kỳ, `?` khớp một ký tự.
//   - pattern không có phần schema được hiểu là schema public (xấp xỉ search_path mặc định mà
//     `pg_dump -t` dùng cho tên không có schema).
//   - pattern sai cú pháp không khớp bảng nào (chiều an toàn: bảng đó sẽ được snapshot logic).

// patTok là một phần tử của pattern đã biên dịch.
type patTok struct {
	kind byte // 'l' ký tự thường, '*' chuỗi bất kỳ, '?' một ký tự
	r    rune
}

// TablePattern là một phần tử "tables" của seed.json đã biên dịch.
type TablePattern struct {
	all    bool // "*" đứng một mình: mọi bảng
	schema []patTok
	name   []patTok
}

// ParseTablePattern biên dịch một phần tử "tables" theo quy ước ở trên.
func ParseTablePattern(s string) (TablePattern, error) {
	s = strings.TrimSpace(s)
	if s == "*" {
		return TablePattern{all: true}, nil
	}
	if s == "" {
		return TablePattern{}, fmt.Errorf("empty table pattern")
	}
	var parts [][]patTok
	cur := []patTok{}
	inQuote := false
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		if inQuote {
			if c == '"' {
				if i+1 < len(rs) && rs[i+1] == '"' {
					cur = append(cur, patTok{kind: 'l', r: '"'})
					i++
					continue
				}
				inQuote = false
				continue
			}
			cur = append(cur, patTok{kind: 'l', r: c})
			continue
		}
		switch c {
		case '"':
			inQuote = true
		case '.':
			parts = append(parts, cur)
			cur = []patTok{}
		case '*':
			cur = append(cur, patTok{kind: '*'})
		case '?':
			cur = append(cur, patTok{kind: '?'})
		default:
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			cur = append(cur, patTok{kind: 'l', r: c})
		}
	}
	if inQuote {
		return TablePattern{}, fmt.Errorf("table pattern %q: unterminated quoted identifier", s)
	}
	parts = append(parts, cur)
	for _, p := range parts {
		if len(p) == 0 {
			return TablePattern{}, fmt.Errorf("table pattern %q: empty name part", s)
		}
	}
	switch len(parts) {
	case 1:
		return TablePattern{schema: litToks("public"), name: parts[0]}, nil
	case 2:
		return TablePattern{schema: parts[0], name: parts[1]}, nil
	default:
		return TablePattern{}, fmt.Errorf("table pattern %q: want [schema.]table", s)
	}
}

func litToks(s string) []patTok {
	out := make([]patTok, 0, len(s))
	for _, r := range s {
		out = append(out, patTok{kind: 'l', r: r})
	}
	return out
}

// Match cho biết bảng id (tên thật, phân biệt hoa/thường như trong catalog) khớp pattern.
func (p TablePattern) Match(id model.TableID) bool {
	if p.all {
		return true
	}
	schema := id.Schema
	if schema == "" {
		schema = "public"
	}
	return globMatch(p.schema, []rune(schema)) && globMatch(p.name, []rune(id.Table))
}

// globMatch khớp toàn bộ s với tokens (quy hoạch động, không đệ quy mũ).
func globMatch(toks []patTok, s []rune) bool {
	// dp[j] = toks[:i] khớp s[:j]
	dp := make([]bool, len(s)+1)
	dp[0] = true
	for _, t := range toks {
		next := make([]bool, len(s)+1)
		switch t.kind {
		case '*':
			acc := false
			for j := 0; j <= len(s); j++ {
				acc = acc || dp[j]
				next[j] = acc
			}
		default:
			for j := 1; j <= len(s); j++ {
				next[j] = dp[j-1] && (t.kind == '?' || s[j-1] == t.r)
			}
		}
		dp = next
	}
	return dp[len(s)]
}

// ValidateTablePatterns kiểm cú pháp mọi phần tử "tables" (planner gọi trước khi ghi seed.json).
func ValidateTablePatterns(tables []string) error {
	for _, t := range tables {
		if _, err := ParseTablePattern(t); err != nil {
			return err
		}
	}
	return nil
}

// SeedCovers cho biết bảng id có dữ liệu trong seed mô tả bởi "tables" của seed.json PG không
// (review N06). Rỗng hoặc có "*" → mọi bảng.
func SeedCovers(tables []string, id model.TableID) bool {
	if len(tables) == 0 {
		return true
	}
	for _, t := range tables {
		p, err := ParseTablePattern(t)
		if err != nil {
			continue
		}
		if p.Match(id) {
			return true
		}
	}
	return false
}
