package web

import (
	"bytes"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const maskStr = "********"

var (
	secretKeyRE = regexp.MustCompile(`(?i)(password|passwd|secret|token|credential|private|encryption_key|access_key|api_key)`)
	// Khóa chỉ là TÊN biến môi trường hoặc cờ, không phải bí mật.
	safeKeyRE = regexp.MustCompile(`(?i)(_env|_file)$`)
	// scheme://user:pass@host  → che pass
	urlPassRE = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.\-]*://[^:/@\s"']*:)([^@\s"']*)(@)`)
	// user:pass@tcp(host:3306)/db  (DSN go-sql-driver) → che pass
	dsnPassRE = regexp.MustCompile(`([^\s:@/"']+:)([^@\s"']*)(@(?:tcp|unix)\()`)
	// password = value / secret=value dạng key=value của keyword DSN PG hoặc log; giá trị được quét bằng scanKVValue.
	kvKeyRE = regexp.MustCompile(`(?i)\b(password|passwd|secret|token)([ \t]*=[ \t]*)`)
)

// MaskSecrets che mật khẩu trong chuỗi tự do (dòng log, DSN).
func MaskSecrets(s string) string {
	s = urlPassRE.ReplaceAllString(s, "${1}"+maskStr+"${3}")
	s = dsnPassRE.ReplaceAllString(s, "${1}"+maskStr+"${3}")
	s, _ = maskKVSecrets(s)
	return s
}

// scanKVValue trả vị trí kết thúc của giá trị key=value bắt đầu tại start. Theo quy tắc keyword DSN của
// libpq: giá trị trong '...' (hoặc "...") kết thúc ở dấu nháy đóng chưa bị escape bằng '\\' (không đóng thì
// che đến hết chuỗi: fail-closed); giá trị trần kết thúc ở khoảng trắng, '&' hoặc dấu nháy, và '\\x' escape
// ký tự kế tiếp (ví dụ "a\\ b").
func scanKVValue(s string, start int) int {
	if start >= len(s) {
		return start
	}
	if q := s[start]; q == '\'' || q == '"' {
		for i := start + 1; i < len(s); i++ {
			switch s[i] {
			case '\\':
				i++
			case q:
				return i + 1
			}
		}
		return len(s)
	}
	i := start
	for i < len(s) {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			i += 2
			continue
		}
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '&' || c == '"' || c == '\'' {
			break
		}
		i++
	}
	return i
}

// kvHit là một giá trị bí mật dạng key=value (val gồm cả dấu nháy nếu có).
type kvHit struct{ valStart, end int }

func findKVSecrets(s string) []kvHit {
	var hits []kvHit
	pos := 0
	for _, loc := range kvKeyRE.FindAllStringSubmatchIndex(s, -1) {
		if loc[0] < pos {
			continue // nằm trong giá trị đã quét
		}
		valStart := loc[1]
		end := scanKVValue(s, valStart)
		if end <= valStart {
			continue
		}
		hits = append(hits, kvHit{valStart, end})
		pos = end
	}
	return hits
}

func isMaskedKVValue(value string) bool {
	return value == maskStr || value == `"`+maskStr+`"` || value == `'`+maskStr+`'`
}

// maskKVSecrets che các giá trị key=value bí mật (giữ nguyên khoảng trắng quanh '='); trả chuỗi đã che và các
// giá trị gốc (gồm dấu nháy) theo thứ tự xuất hiện.
func maskKVSecrets(s string) (string, []string) {
	var b strings.Builder
	var secrets []string
	pos := 0
	for _, h := range findKVSecrets(s) {
		val := s[h.valStart:h.end]
		b.WriteString(s[pos:h.valStart])
		pos = h.end
		if isMaskedKVValue(val) {
			b.WriteString(val)
			continue
		}
		b.WriteString(maskStr)
		secrets = append(secrets, val)
	}
	b.WriteString(s[pos:])
	return b.String(), secrets
}

// MaskConfigYAML che mọi giá trị bí mật của file cấu hình pipeline: khóa mang tên password/secret/token/...,
// và mật khẩu nhúng trong DSN/URL. File được đọc THÔ (chưa thay ${VAR}), nên giá trị từ biến môi trường
// không bao giờ vào trang. Giữ nguyên chú thích và thứ tự khóa.
func MaskConfigYAML(raw []byte) (string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return "", err
	}
	maskNode(&doc, "")
	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return "", err
	}
	_ = enc.Close()
	return out.String(), nil
}

func maskNode(n *yaml.Node, key string) {
	switch n.Kind {
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range n.Content {
			maskNode(c, key)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			maskNode(n.Content[i+1], n.Content[i].Value)
		}
	case yaml.ScalarNode:
		if secretKeyRE.MatchString(key) && !safeKeyRE.MatchString(key) && n.Value != "" &&
			!strings.HasPrefix(strings.TrimSpace(n.Value), "${") {
			n.Value, n.Style, n.Tag = maskStr, yaml.DoubleQuotedStyle, "!!str"
			return
		}
		n.Value = MaskSecrets(n.Value)
	}
}
