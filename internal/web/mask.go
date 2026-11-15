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
	// password=xxx / secret=xxx dạng key=value trong DSN PG hoặc log
	kvPassRE = regexp.MustCompile(`(?i)\b(password|passwd|secret|token)=([^\s&"']+)`)
)

// MaskSecrets che mật khẩu trong chuỗi tự do (dòng log, DSN).
func MaskSecrets(s string) string {
	s = urlPassRE.ReplaceAllString(s, "${1}"+maskStr+"${3}")
	s = dsnPassRE.ReplaceAllString(s, "${1}"+maskStr+"${3}")
	s = kvPassRE.ReplaceAllString(s, "${1}="+maskStr)
	return s
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
		if secretKeyRE.MatchString(key) && !safeKeyRE.MatchString(key) && strings.TrimSpace(n.Value) != "" &&
			!strings.HasPrefix(strings.TrimSpace(n.Value), "${") {
			n.Value, n.Style, n.Tag = maskStr, yaml.DoubleQuotedStyle, "!!str"
			return
		}
		n.Value = MaskSecrets(n.Value)
	}
}
