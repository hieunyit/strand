package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Repository kinds. Only S3-compatible storage (AWS, MinIO, Ceph) is implemented; the field exists so
// Azure Blob and GCS can be added without a schema change.
const (
	RepoKindS3    = "s3"
	RepoKindAzure = "azure"
	RepoKindGCS   = "gcs"
)

// Repository secret modes: env = endpoint and credentials come from STRAND_BACKUP_REPO_<NAME>_* in the
// environment of the Web service; stored = entered in the console and sealed with the Web secret key.
const (
	RepoSecretEnv    = "env"
	RepoSecretStored = "stored"
)

// Repo is a named S3 repository. In env mode (the default) the endpoint and credentials are never stored:
// EnvPrefix names environment variables of the Strand process (<prefix>ENDPOINT, <prefix>REGION,
// <prefix>ACCESS_KEY, <prefix>SECRET_KEY, <prefix>SESSION_TOKEN, <prefix>ALLOW_INSECURE_LOCAL). In stored
// mode the endpoint/region are columns and the access/secret key are sealed (store.Box, AAD = repository
// id); the Web service then hands them to the backup processes it starts through their environment under
// the derived prefix EffectivePrefix (never argv).
type Repo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Kind       string `json:"kind"` // s3 (default); azure and gcs are reserved
	SecretMode string `json:"secret_mode"`
	EnvPrefix  string `json:"env_prefix"` // env mode only
	Endpoint   string `json:"endpoint,omitempty"`
	Region     string `json:"region,omitempty"`
	// AllowInsecureLocal permits http:// to a loopback endpoint (development, MinIO on the same host).
	AllowInsecureLocal bool `json:"allow_insecure_local,omitempty"`
	// AccessKeySealed / SecretKeySealed: store.Box ciphertext (AAD = repository id). Never serialized.
	AccessKeySealed []byte `json:"-"`
	SecretKeySealed []byte `json:"-"`
	AccessKeySet    bool   `json:"access_key_set"`
	SecretKeySet    bool   `json:"secret_key_set"`
	// Last connectivity test (POST /api/v1/backup-repositories/{id}/test).
	LastTestAt     *time.Time      `json:"last_test_at,omitempty"`
	LastTestStatus string          `json:"last_test_status,omitempty"`
	LastTestDetail string          `json:"last_test_detail,omitempty"`
	LastTestSteps  []byte          `json:"-"` // JSON of the checklist
	Bucket         string          `json:"bucket"`
	Prefix         string          `json:"prefix,omitempty"`
	LockMode       string          `json:"lock_mode,omitempty"` // "", GOVERNANCE, COMPLIANCE (bucket must have Object Lock enabled)
	LockDays       int             `json:"lock_days,omitempty"`
	Retention      RetentionPolicy `json:"retention"` // policy for backups in this repository that belong to no job
	CreatedAt      time.Time       `json:"created_at,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at,omitempty"`
}

// DefaultEnvPrefix is the environment prefix of the default repository.
const DefaultEnvPrefix = "STRAND_BACKUP_S3_"

var envPrefixRE = regexp.MustCompile(`^STRAND_BACKUP_(S3|REPO_[A-Z0-9]{1,32})_$`)

// KindOrS3 returns Kind, defaulting to s3.
func (r Repo) KindOrS3() string {
	if r.Kind == "" {
		return RepoKindS3
	}
	return r.Kind
}

// Stored reports a repository whose endpoint and credentials live in the database.
func (r Repo) Stored() bool { return r.SecretMode == RepoSecretStored }

// Immutable reports a repository written under Object Lock COMPLIANCE retention (shown as a badge).
func (r Repo) Immutable() bool { return r.LockMode == "COMPLIANCE" }

// EffectivePrefix is the environment prefix the runner reads the repository settings from: EnvPrefix in
// env mode, else a prefix derived from the repository id (the Web service sets those variables, only in
// the environment of the processes it starts).
func (r Repo) EffectivePrefix() string {
	if r.EnvPrefix != "" {
		return r.EnvPrefix
	}
	h := sha256.Sum256([]byte("strand-backup-repo:" + r.ID))
	return "STRAND_BACKUP_REPO_X" + strings.ToUpper(hex.EncodeToString(h[:8])) + "_"
}

// EnvVars returns the NAME=value pairs for a stored repository (the plaintext keys are passed in by the
// caller, which opened the sealed values). The result goes to a child's environment only.
func (r Repo) EnvVars(accessKey, secretKey string) []string {
	p := r.EffectivePrefix()
	insecure := ""
	if r.AllowInsecureLocal {
		insecure = "true"
	}
	return []string{p + "ENDPOINT=" + r.Endpoint, p + "REGION=" + r.Region, p + "ACCESS_KEY=" + accessKey,
		p + "SECRET_KEY=" + secretKey, p + "SESSION_TOKEN=", p + "ALLOW_INSECURE_LOCAL=" + insecure}
}

var regionRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidEndpoint checks an S3 endpoint URL: http(s), a host, no credentials, query or fragment.
func ValidEndpoint(e string) bool {
	if e == "" || len(e) > 512 || hasControl(e) {
		return false
	}
	u, err := url.Parse(e)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	return true
}

// ValidateRepo checks a repository definition.
func ValidateRepo(r Repo) error {
	bad := func(s string) error { return fmt.Errorf("%w: %s", ErrInvalid, s) }
	if !validID(r.ID) {
		return bad("invalid repository id")
	}
	if !safeText(r.Name, 96) {
		return bad("repository name is required (at most 96 characters)")
	}
	if k := r.KindOrS3(); k != RepoKindS3 {
		if k == RepoKindAzure || k == RepoKindGCS {
			return bad("kind " + k + " is not supported yet (only s3)")
		}
		return bad("kind must be s3")
	}
	switch r.SecretMode {
	case "", RepoSecretEnv:
		if !envPrefixRE.MatchString(r.EnvPrefix) {
			return bad("env_prefix must be STRAND_BACKUP_S3_ or STRAND_BACKUP_REPO_<NAME>_")
		}
		if r.Endpoint != "" || r.Region != "" || r.AccessKeySealed != nil || r.SecretKeySealed != nil || r.AllowInsecureLocal {
			return bad("endpoint, region and keys are only stored with secret_mode stored (env mode reads them from the environment)")
		}
	case RepoSecretStored:
		if r.EnvPrefix != "" {
			return bad("env_prefix is not used with secret_mode stored")
		}
		if !ValidEndpoint(r.Endpoint) {
			return bad("endpoint must be an http(s) URL without credentials")
		}
		if r.Region != "" && !regionRE.MatchString(r.Region) {
			return bad("invalid region")
		}
		if len(r.AccessKeySealed) == 0 || len(r.SecretKeySealed) == 0 {
			return bad("access key and secret key are required with secret_mode stored")
		}
	default:
		return bad("secret_mode must be env or stored")
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
