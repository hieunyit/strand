package breaker

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	// dsnPasswordRegex finds user:password@
	dsnPasswordRegex = regexp.MustCompile(`://([^:]+):([^@]+)@`)
	// mysqlPlainDSNRegex finds user:password@tcp(...)
	mysqlPlainDSNRegex = regexp.MustCompile(`^([^:]+):([^@]+)@`)

	// Regexes to sanitize SHOW ENGINE INNODB STATUS
	sqlQueryRegex    = regexp.MustCompile(`(?i)(SELECT|INSERT|UPDATE|DELETE|REPLACE|ALTER|CREATE|DROP|GRANT)\s+.*`)
	hexDumpRegex     = regexp.MustCompile(`(?i)[0-9a-f]{8,}`)
	passwordVarRegex = regexp.MustCompile(`(?i)(password|passwd|secret|token|key)\s*[:=]\s*\S+`)
)

// RedactDSN masks the password in a database DSN.
func RedactDSN(raw string) string {
	if raw == "" {
		return ""
	}
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err == nil && u.User != nil {
			if _, hasPass := u.User.Password(); hasPass {
				u.User = url.UserPassword(u.User.Username(), "REDACTED")
				return strings.ReplaceAll(u.String(), "REDACTED", "***")
			}
		}
		return dsnPasswordRegex.ReplaceAllString(raw, `://${1}:***@`)
	}
	return mysqlPlainDSNRegex.ReplaceAllString(raw, `${1}:***@`)
}

// SanitizeEngineStatus removes query text, hex dumps, and secret values from engine status text.
func SanitizeEngineStatus(raw string) string {
	lines := strings.Split(raw, "\n")
	sanitized := make([]string, 0, len(lines))

	inTransactions := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Detect transaction section
		if strings.EqualFold(trimmed, "TRANSACTIONS") {
			inTransactions = true
			sanitized = append(sanitized, line)
			continue
		}
		if inTransactions && strings.HasPrefix(trimmed, "FILE I/O") {
			inTransactions = false
		}

		// If line contains SQL statements
		if sqlQueryRegex.MatchString(trimmed) {
			line = "[QUERY TEXT REDACTED]"
		} else if inTransactions {
			if idx := strings.Index(line, "query id"); idx != -1 {
				line = line[:idx] + "query id [QUERY REDACTED]"
			}
		}

		// Mask potential passwords or secrets
		if passwordVarRegex.MatchString(line) {
			line = passwordVarRegex.ReplaceAllString(line, `$1: [REDACTED]`)
		}

		sanitized = append(sanitized, line)
	}

	return strings.Join(sanitized, "\n")
}
