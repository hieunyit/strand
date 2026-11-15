package web

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Che / khôi phục bí mật theo dòng: giữ nguyên định dạng, chú thích, dòng trống, flow-style của file cấu hình.

var (
	// key: value (block hoặc flow style); key đứng sau đầu dòng, khoảng trắng, '{' hoặc ','.
	kvLineRE = regexp.MustCompile(`(^|[\s{,])(["']?)([A-Za-z0-9_.\-]+)(["']?\s*:[ \t]+)("(?:[^"\\]|\\.)*"|'[^']*'|[^\s,{}\[\]#][^,}\]#]*)`)
	// khóa của một dòng (để ghép dòng đã sửa với dòng gốc)
	lineKeyRE = regexp.MustCompile(`^\s*(?:-\s+)?["']?([A-Za-z0-9_.\-]+)["']?\s*:`)
	// token che do MaskConfigText sinh ra hoặc do người dùng gõ lại
	maskTokRE = regexp.MustCompile(`["']?\*{8}["']?`)
)

func isSecretYAMLKey(k string) bool { return secretKeyRE.MatchString(k) && !safeKeyRE.MatchString(k) }

func unquoteScalar(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		return v[1 : len(v)-1]
	}
	return v
}

// maskLine che bí mật trong một dòng; trả dòng đã che và danh sách giá trị gốc bị thay (theo thứ tự lần che).
func maskLine(line string) (string, []string) {
	var secrets []string
	out := kvLineRE.ReplaceAllStringFunc(line, func(m string) string {
		sub := kvLineRE.FindStringSubmatch(m)
		key, val := sub[3], sub[5]
		if isSecretYAMLKey(key) {
			t := strings.TrimRight(val, " \t")
			u := unquoteScalar(t)
			if u != "" && !strings.HasPrefix(u, "${") && u != maskStr {
				secrets = append(secrets, t)
				return m[:len(m)-len(val)] + `"` + maskStr + `"` + val[len(t):]
			}
		}
		return m
	})
	for _, re := range []*regexp.Regexp{urlPassRE, dsnPassRE} {
		re := re
		out = re.ReplaceAllStringFunc(out, func(m string) string {
			sub := re.FindStringSubmatch(m)
			if sub[2] == maskStr || sub[2] == "" {
				return m
			}
			secrets = append(secrets, sub[2])
			return sub[1] + maskStr + sub[3]
		})
	}
	out = kvPassRE.ReplaceAllStringFunc(out, func(m string) string {
		sub := kvPassRE.FindStringSubmatch(m)
		if sub[2] == maskStr {
			return m
		}
		secrets = append(secrets, sub[2])
		return sub[1] + "=" + maskStr
	})
	return out, secrets
}

// MaskConfigText che giá trị bí mật của file cấu hình nhưng giữ NGUYÊN văn bản (định dạng, chú thích, dòng trống,
// flow-style): chỉ thay đúng giá trị bí mật. File được đọc THÔ (chưa thay ${VAR}). Trả lỗi nếu file không phải YAML
// hợp lệ hoặc bí mật nằm trong block scalar nhiều dòng (không che an toàn theo dòng được).
func MaskConfigText(raw []byte) (string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return "", err
	}
	lines := strings.Split(string(raw), "\n")
	for i, l := range lines {
		if m := lineKeyRE.FindStringSubmatch(l); m != nil && isSecretYAMLKey(m[1]) {
			if v := strings.TrimSpace(l[strings.Index(l, ":")+1:]); strings.HasPrefix(v, "|") || strings.HasPrefix(v, ">") {
				return "", fmt.Errorf("line %d: secret key %q uses a multi-line block scalar; cannot be masked safely", i+1, m[1])
			}
		}
		lines[i], _ = maskLine(l)
	}
	return strings.Join(lines, "\n"), nil
}

// hasMaskedSecret: dòng vẫn còn giá trị bí mật dạng '********' (sẽ bị ghi thành mật khẩu thật nếu không khôi phục).
func hasMaskedSecret(line string) bool {
	for _, sub := range kvLineRE.FindAllStringSubmatch(line, -1) {
		if isSecretYAMLKey(sub[3]) && unquoteScalar(strings.TrimRight(sub[5], " \t")) == maskStr {
			return true
		}
	}
	for _, re := range []*regexp.Regexp{urlPassRE, dsnPassRE} {
		for _, sub := range re.FindAllStringSubmatch(line, -1) {
			if sub[2] == maskStr {
				return true
			}
		}
	}
	for _, sub := range kvPassRE.FindAllStringSubmatch(line, -1) {
		if sub[2] == maskStr {
			return true
		}
	}
	return false
}

// UnmaskConfigText khôi phục bí mật gốc cho mọi dòng người dùng gửi lại với '********' (văn bản từ MaskConfigText),
// dựa trên file gốc, và giữ nguyên mọi chỉnh sửa/định dạng khác. Không bao giờ để '********' lọt vào file như một
// mật khẩu thật: nếu không khôi phục được thì trả lỗi.
func UnmaskConfigText(edited string, orig []byte) (string, error) {
	type origLine struct {
		secrets []string
		key     string
		used    bool
	}
	var cands []*origLine
	byMasked := map[string][]*origLine{}
	for _, l := range strings.Split(string(orig), "\n") {
		m, sec := maskLine(l)
		if len(sec) == 0 {
			continue
		}
		o := &origLine{secrets: sec}
		if km := lineKeyRE.FindStringSubmatch(l); km != nil {
			o.key = km[1]
		}
		cands = append(cands, o)
		byMasked[m] = append(byMasked[m], o)
	}
	restore := func(line string, secrets []string) string {
		i := 0
		return maskTokRE.ReplaceAllStringFunc(line, func(tok string) string {
			if i >= len(secrets) {
				return tok
			}
			i++
			return secrets[i-1] // giá trị gốc đã gồm dấu nháy (nếu có)
		})
	}
	lines := strings.Split(edited, "\n")
	for i, l := range lines {
		if !strings.Contains(l, maskStr) {
			continue
		}
		var pick *origLine
		for _, o := range byMasked[l] { // dòng không đổi: ghép đúng thứ tự xuất hiện
			if !o.used {
				pick = o
				break
			}
		}
		if pick == nil { // dòng đã sửa: ghép theo khóa
			if km := lineKeyRE.FindStringSubmatch(l); km != nil {
				for _, o := range cands {
					if !o.used && o.key == km[1] {
						pick = o
						break
					}
				}
			}
		}
		if pick != nil {
			pick.used = true
			lines[i] = restore(l, pick.secrets)
		}
		if hasMaskedSecret(lines[i]) {
			return "", fmt.Errorf("line %d: cannot restore the original secret for the masked value ******** (the line was not found in the current file); type the real value or revert the line", i+1)
		}
	}
	return strings.Join(lines, "\n"), nil
}
