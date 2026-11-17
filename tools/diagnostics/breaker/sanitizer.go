package breaker

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/go-sql-driver/mysql"
)

var (
	dsnURLPrefix = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`)
)

// RedactDSN masks the password in a database DSN.
func RedactDSN(raw string) string {
	if raw == "" {
		return ""
	}
	if dsnURLPrefix.MatchString(raw) {
		u, err := url.Parse(raw)
		if err == nil && u.User != nil {
			if _, hasPass := u.User.Password(); hasPass {
				u.User = url.UserPassword(u.User.Username(), "REDACTED")
				return strings.ReplaceAll(u.String(), "REDACTED", "***")
			}
		}
		// Unparseable input may still contain credentials.
		return "[REDACTED_DSN]"
	}
	if _, err := mysql.ParseDSN(raw); err != nil {
		return "[REDACTED_DSN]"
	}
	// Follow the driver's grammar: passwords can contain '@', ':' and '/';
	// username may be empty. The final slash and preceding final '@' delimit
	// the credentials, and the first colon delimits the password.
	if slash := strings.LastIndexByte(raw, '/'); slash >= 0 {
		if at := strings.LastIndexByte(raw[:slash], '@'); at >= 0 {
			if colon := strings.IndexByte(raw[:at], ':'); colon >= 0 {
				return raw[:colon+1] + "***" + raw[at:]
			}
		}
	}
	return raw
}

// Engine status includes arbitrary multiline queries and physical record dumps.
// Export only complete lines with known numeric diagnostic formats; searching
// for SQL keywords cannot identify continuation lines or sensitive row values.
var engineMetricLine = regexp.MustCompile(`^(?:` +
	`(?:Log sequence number|Log flushed up to|Pages flushed up to|Last checkpoint at)\s+[0-9]+|` +
	`[0-9]+ pending log flushes(?: \(fsync\))?, [0-9]+ pending chkp writes|` +
	`[0-9]+ pending normal aio reads, [0-9]+ pending aio writes|` +
	`OS WAIT ARRAY INFO: (?:reservation|signal) count [0-9]+|` +
	`RW-(?:shared|excl|sx) spins [0-9]+, rounds [0-9]+, OS waits [0-9]+|` +
	`Spin rounds per wait: [0-9.]+ RW-shared, [0-9.]+ RW-excl, [0-9.]+ RW-sx|` +
	`(?:Total large memory allocated|Dictionary memory allocated|Buffer pool size|Free buffers|Database pages|Old database pages|Modified db pages|Pending reads)\s+[0-9]+|` +
	`Pending writes: LRU [0-9]+, flush list [0-9]+, single page [0-9]+` +
	`)$`)

// SanitizeEngineStatus retains numeric pressure and contention diagnostics and
// omits arbitrary query, record, user, schema, and server-supplied free text.
func SanitizeEngineStatus(raw string) string {
	lines := strings.Split(raw, "\n")
	sanitized := []string{"[QUERY AND RECORD TEXT OMITTED]"}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch trimmed {
		case "BACKGROUND THREAD", "SEMAPHORES", "TRANSACTIONS", "FILE I/O", "LOG", "BUFFER POOL AND MEMORY", "ROW OPERATIONS":
			sanitized = append(sanitized, trimmed)
			// Preserve the section format used by the semaphore metric parser.
			sanitized = append(sanitized, "----------")
		default:
			if engineMetricLine.MatchString(trimmed) {
				sanitized = append(sanitized, trimmed)
			}
		}
	}
	return strings.Join(sanitized, "\n")
}
