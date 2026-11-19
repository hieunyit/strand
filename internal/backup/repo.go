package backup

import (
	"fmt"
	"regexp"
	"time"
)

// Repo is a named S3 repository. Credentials and the endpoint are never stored in the catalog: EnvPrefix
// names environment variables of the Strand process (<prefix>ENDPOINT, <prefix>REGION,
// <prefix>ACCESS_KEY, <prefix>SECRET_KEY, <prefix>SESSION_TOKEN, <prefix>ALLOW_INSECURE_LOCAL).
type Repo struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	EnvPrefix string          `json:"env_prefix"`
	Bucket    string          `json:"bucket"`
	Prefix    string          `json:"prefix,omitempty"`
	LockMode  string          `json:"lock_mode,omitempty"` // "", GOVERNANCE, COMPLIANCE (bucket must have Object Lock enabled)
	LockDays  int             `json:"lock_days,omitempty"`
	Retention RetentionPolicy `json:"retention"` // policy for backups in this repository that belong to no job
	CreatedAt time.Time       `json:"created_at,omitempty"`
	UpdatedAt time.Time       `json:"updated_at,omitempty"`
}

// DefaultEnvPrefix is the environment prefix of the default repository.
const DefaultEnvPrefix = "STRAND_BACKUP_S3_"

var envPrefixRE = regexp.MustCompile(`^STRAND_BACKUP_(S3|REPO_[A-Z0-9]{1,32})_$`)

// ValidateRepo checks a repository definition.
func ValidateRepo(r Repo) error {
	bad := func(s string) error { return fmt.Errorf("%w: %s", ErrInvalid, s) }
	if !validID(r.ID) {
		return bad("invalid repository id")
	}
	if !safeText(r.Name, 96) {
		return bad("repository name is required (at most 96 characters)")
	}
	if !envPrefixRE.MatchString(r.EnvPrefix) {
		return bad("env_prefix must be STRAND_BACKUP_S3_ or STRAND_BACKUP_REPO_<NAME>_")
	}
	if !safeBucket(r.Bucket) {
		return bad("invalid bucket")
	}
	if len(r.Prefix) > 512 || hasControl(r.Prefix) || !safePrefix(r.Prefix) {
		return bad("invalid prefix")
	}
	if r.LockMode != "" && r.LockMode != "GOVERNANCE" && r.LockMode != "COMPLIANCE" {
		return bad("lock_mode must be GOVERNANCE or COMPLIANCE")
	}
	if (r.LockMode == "") != (r.LockDays == 0) || r.LockDays < 0 || r.LockDays > 36500 {
		return bad("lock_mode and lock_days (1-36500) go together")
	}
	return r.Retention.Validate()
}

func safePrefix(p string) bool {
	if p == "" {
		return true
	}
	for _, part := range splitSlash(p) {
		if part == ".." || part == "." {
			return false
		}
	}
	return p[0] != '/'
}

func splitSlash(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '/' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

// SourceSummary aggregates the catalog per source for the dashboard and metrics.
type SourceSummary struct {
	Source       string     `json:"source"`
	Engine       string     `json:"engine"`
	JobID        string     `json:"job_id,omitempty"`
	LastSuccess  *Record    `json:"last_success,omitempty"`
	LastFailedAt *time.Time `json:"last_failed_at,omitempty"`
	Complete     int        `json:"complete"`
	TotalBytes   int64      `json:"total_bytes"`
	LogStart     *time.Time `json:"log_start,omitempty"`
	LogEnd       *time.Time `json:"log_end,omitempty"`
	LogSegments  int        `json:"log_segments"`
	RecentSizes  []int64    `json:"recent_sizes"` // last complete restore points, oldest first
}
