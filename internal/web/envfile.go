package web

import (
	"fmt"
	"strings"
)

// Định dạng .env do web ghi: dòng `KEY=VALUE` thuần, đọc được bởi systemd EnvironmentFile
// (systemd.exec(5)), `set -a; . file` của sh và ReadEnvFile. Giá trị chỉ gồm ký tự an toàn thì ghi trần,
// còn lại đặt trong nháy kép và escape \ " $ ` (cùng quy tắc nháy kép của systemd và sh).

// envBareSafe: ký tự không cần nháy ở cả systemd lẫn sh.
func envBareSafe(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_@%+=:,./-", r)
}

// formatEnvLine trả `KEY=VALUE` (có xuống dòng cuối) đã quote đúng; lỗi 400 nếu giá trị có newline/CR/NUL.
func formatEnvLine(key, v string) (string, error) {
	if strings.ContainsAny(v, "\n\r\x00") {
		return "", BadRequest("value of %s must not contain a newline, carriage return or NUL character (not representable in a systemd EnvironmentFile)", key)
	}
	bare := v != ""
	for _, r := range v {
		if !envBareSafe(r) {
			bare = false
			break
		}
	}
	if bare {
		return fmt.Sprintf("%s=%s\n", key, v), nil
	}
	rep := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `$`, `\$`, "`", "\\`")
	return fmt.Sprintf("%s=\"%s\"\n", key, rep.Replace(v)), nil
}

// parseEnvValue giải mã vế phải của một dòng .env (đã TrimSpace).
func parseEnvValue(v string) string {
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return v[1 : len(v)-1]
	}
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		in := v[1 : len(v)-1]
		var b strings.Builder
		for i := 0; i < len(in); i++ {
			if in[i] == '\\' && i+1 < len(in) && strings.IndexByte("\\\"$`", in[i+1]) >= 0 {
				i++
			}
			b.WriteByte(in[i])
		}
		return b.String()
	}
	return v
}
