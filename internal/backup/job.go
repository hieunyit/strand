package backup

import (
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type RunStatus string

const (
	RunStatusRunning     RunStatus = "running"
	RunStatusSucceeded   RunStatus = "succeeded"
	RunStatusFailed      RunStatus = "failed"
	RunStatusCancelled   RunStatus = "cancelled"
	RunStatusInterrupted RunStatus = "interrupted"
)

// Backup methods. "custom" runs an operator-provided wrapper script (artifact + seed manifest on disk);
// every other method is a built-in preset: Strand builds the native tool's argv itself (no shell), streams
// its output to the repository and records the method-specific recovery position.
const (
	MethodCustom       = "custom"
	MethodMySQLDump    = "mysqldump"
	MethodMariaDBDump  = "mariadb-dump"
	MethodMydumper     = "mydumper"
	MethodPGDump       = "pg_dump"
	MethodPGBaseBackup = "pg_basebackup"
	MethodMongodump    = "mongodump"
	// Continuous log archiving methods (catalog records of kind "log").
	MethodBinlog = "binlog"
	MethodWAL    = "wal"
	MethodOplog  = "oplog"
)

// IsUnsupportedMethod reports backup methods that were removed because they must run on the database host.
func IsUnsupportedMethod(method string) bool {
	return method == "xtrabackup" || method == "mariabackup"
}

// MethodEngine maps a built-in method to its engine ("" for custom/unknown).
func MethodEngine(method string) string {
	switch method {
	case MethodMySQLDump, MethodMariaDBDump, MethodMydumper, MethodBinlog:
		return EngineMySQL
	case MethodPGDump, MethodPGBaseBackup, MethodWAL:
		return EnginePostgres
	case MethodMongodump, MethodOplog:
		return EngineMongoDB
	}
	return ""
}

// MethodSupportsIncremental reports physical methods that can take incremental backups.
// Incremental chain creation is no longer supported (xtrabackup/mariabackup removed).
func MethodSupportsIncremental(method string) bool {
	return false
}

// MethodLogArchive returns the continuous log-archiving method paired with a base-backup method.
func MethodLogArchive(method string) string {
	switch MethodEngine(method) {
	case EngineMySQL:
		return MethodBinlog
	case EnginePostgres:
		return MethodWAL
	case EngineMongoDB:
		return MethodOplog
	}
	return ""
}

// ConnSpec says how a built-in method reaches the database. Secrets are never stored: PasswordEnv /
// URIEnv name environment variables of the Web service that hold them.
type ConnSpec struct {
	Host        string   `json:"host,omitempty"`
	Port        int      `json:"port,omitempty"`
	User        string   `json:"user,omitempty"`
	PasswordEnv string   `json:"password_env,omitempty"`
	Databases   []string `json:"databases,omitempty"`
	// URIEnv (MongoDB): environment variable holding a mongodb:// URI (with credentials).
	URIEnv string `json:"uri_env,omitempty"`
	// Datadir (xtrabackup/mariabackup): the MySQL data directory, readable on the Strand host.
	Datadir string `json:"datadir,omitempty"`
}

// JobOptions are the managed-backup settings of a job (stored as JSON).
type JobOptions struct {
	Method        string          `json:"method,omitempty"`
	Source        string          `json:"source,omitempty"` // source identity recorded in the catalog
	Conn          ConnSpec        `json:"conn"`
	Repository    string          `json:"repository,omitempty"`
	Retention     RetentionPolicy `json:"retention"`
	Compress      bool            `json:"compress,omitempty"`
	CompressLevel int             `json:"compress_level,omitempty"`
	Encrypt       bool            `json:"encrypt,omitempty"`
	BandwidthKBps int             `json:"bandwidth_kbps,omitempty"` // upload limit, 0 = unlimited
	// FullEvery: physical methods take a full backup every N runs and incrementals in between (0/1 = full).
	FullEvery int `json:"full_every,omitempty"`
	// LogArchive runs continuous log archiving (binlog/WAL/oplog) for point-in-time recovery.
	LogArchive      bool   `json:"log_archive,omitempty"`
	LogFlushSeconds int    `json:"log_flush_seconds,omitempty"` // force a log switch this often (RPO bound)
	ReplicateTo     string `json:"replicate_to,omitempty"`      // copy every new backup to this repository
	// VerifyEverySeconds schedules integrity checks of this job's backups (0 = off).
	VerifyEverySeconds int `json:"verify_every_seconds,omitempty"`
	// VerifySamplePercent re-reads this share of objects per check (0/100 = all).
	VerifySamplePercent int `json:"verify_sample_percent,omitempty"`
	// TestRestore restores the newest logical backup into a scratch database after each verification.
	TestRestore bool     `json:"test_restore,omitempty"`
	Scratch     ConnSpec `json:"scratch"`
}

// Job is a persisted recurring backup. Secrets are read from the Web process environment; SSH
// authentication is by key path or SSH agent.
type Job struct {
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	Command          string     `json:"command"`
	EverySeconds     int        `json:"every_seconds"`
	ArtifactTemplate string     `json:"artifact_template"`
	ManifestTemplate string     `json:"manifest_template"`
	Bucket           string     `json:"bucket"`
	Prefix           string     `json:"prefix,omitempty"`
	Enabled          bool       `json:"enabled"`
	TunnelSSH        string     `json:"tunnel_ssh,omitempty"`
	TunnelUser       string     `json:"tunnel_user,omitempty"`
	TunnelIdentity   string     `json:"tunnel_identity_file,omitempty"`
	TunnelKnownHosts string     `json:"tunnel_known_hosts,omitempty"`
	TunnelRemote     string     `json:"tunnel_remote,omitempty"`
	CleanupLocal     bool       `json:"cleanup_local"`
	NextRunAt        *time.Time `json:"next_run_at,omitempty"`
	UpdatedAt        time.Time  `json:"updated_at,omitempty"`
	RunStartedAt     *time.Time `json:"run_started_at,omitempty"`
	RunFinishedAt    *time.Time `json:"run_finished_at,omitempty"`
	LastRunStatus    RunStatus  `json:"last_run_status,omitempty"`
	// Retry policy: after a failed run, retry up to RetryMax times with exponential backoff starting at
	// RetryBackoffSeconds (capped by the interval) instead of waiting a full interval.
	RetryMax            int `json:"retry_max,omitempty"`
	RetryBackoffSeconds int `json:"retry_backoff_seconds,omitempty"`
	ConsecutiveFailures int `json:"consecutive_failures,omitempty"` // maintained by the store
	JobOptions
}

// IsCustom reports a wrapper-script job.
func (j Job) IsCustom() bool { return j.Method == "" || j.Method == MethodCustom }

// Engine returns the engine of a built-in method job.
func (j Job) Engine() string { return MethodEngine(j.Method) }

var (
	envNameRE  = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,127}$`)
	dbNameRE   = regexp.MustCompile(`^[A-Za-z0-9_$][A-Za-z0-9_$.-]{0,127}$`)
	hostRE     = regexp.MustCompile(`^[A-Za-z0-9._:\[\]-]{1,253}$`)
	userNameRE = regexp.MustCompile(`^[A-Za-z0-9_.@$-][A-Za-z0-9_.@$ -]{0,127}$`)
)

// ValidEnvName reports a safe environment variable name for PasswordEnv/URIEnv. Strand's own secrets
// can never be named (that would let a Web user exfiltrate them through a backup tool).
func ValidEnvName(s string) bool {
	return envNameRE.MatchString(s) && !strings.HasPrefix(s, "STRAND_WEB_") && !strings.HasPrefix(s, "STRAND_BACKUP_S3_") &&
		!strings.HasPrefix(s, "STRAND_BACKUP_REPO_") && !strings.HasPrefix(s, "STRAND_BACKUP_ENCRYPTION") &&
		!strings.HasPrefix(s, "STRAND_SECRET") && !strings.HasPrefix(s, "AWS_") && !strings.HasPrefix(s, "STRAND_BACKUP_NOTIFY")
}

func validateConn(label string, c ConnSpec, engine string, needDB bool) error {
	bad := func(s string) error { return fmt.Errorf("%w: %s %s", ErrInvalid, label, s) }
	if engine == EngineMongoDB {
		if c.URIEnv == "" && c.Host == "" {
			return bad("needs uri_env or host")
		}
	} else if c.Host == "" {
		return bad("host is required")
	}
	if c.Host != "" && (!hostRE.MatchString(c.Host) || strings.HasPrefix(c.Host, "-")) {
		return bad("host is invalid")
	}
	if c.Port < 0 || c.Port > 65535 {
		return bad("port is invalid")
	}
	if c.User != "" && (!userNameRE.MatchString(c.User) || strings.HasPrefix(c.User, "-")) {
		return bad("user is invalid")
	}
	for _, e := range []string{c.PasswordEnv, c.URIEnv} {
		if e != "" && !ValidEnvName(e) {
			return bad("password/URI environment variable name is invalid or reserved for Strand secrets")
		}
	}
	if len(c.Databases) > 256 {
		return bad("lists too many databases")
	}
	for _, d := range c.Databases {
		if !dbNameRE.MatchString(d) || strings.HasPrefix(d, "-") {
			return bad("database name " + fmt.Sprintf("%q", d) + " is invalid")
		}
	}
	if needDB && len(c.Databases) == 0 {
		return bad("needs at least one database")
	}
	if c.Datadir != "" && (!filepath.IsAbs(c.Datadir) || filepath.Clean(c.Datadir) != c.Datadir) {
		return bad("datadir must be a clean absolute path")
	}
	return nil
}

func ValidateJob(j Job) error {
	bad := func(s string) error { return fmt.Errorf("%w: %s", ErrInvalid, s) }
	if !validID(j.ID) {
		return bad("invalid backup job id")
	}
	if !safeText(j.Name, 96) {
		return bad("job name is required and must be at most 96 characters")
	}
	if j.EverySeconds < 60 || j.EverySeconds > 365*24*60*60 {
		return bad("interval must be from 60 seconds to 365 days")
	}
	if j.RetryMax < 0 || j.RetryMax > 20 {
		return bad("retry_max must be between 0 and 20")
	}
	if j.RetryBackoffSeconds < 0 || j.RetryBackoffSeconds > 86400 || (j.RetryMax > 0 && j.RetryBackoffSeconds < 10) {
		return bad("retry_backoff_seconds must be between 10 and 86400 when retries are enabled")
	}
	if j.IsCustom() {
		if !filepath.IsAbs(j.Command) || strings.ContainsAny(j.Command, "\x00\r\n") {
			return bad("command must be an absolute executable path")
		}
		for label, path := range map[string]string{"artifact": j.ArtifactTemplate, "manifest": j.ManifestTemplate} {
			if !filepath.IsAbs(path) || !strings.Contains(path, "{id}") || strings.ContainsAny(path, "\x00\r\n") {
				return bad(label + " path must be absolute and contain {id}")
			}
			// Local cleanup removes these paths recursively: no "..", "//" or "." segments, so the expanded
			// path can never climb out of the directory the operator wrote.
			if filepath.Clean(path) != path || strings.Contains(path, "/../") || strings.HasSuffix(path, "/..") {
				return bad(label + " path must be a clean absolute path without '..' segments")
			}
		}
		if j.LogArchive || j.FullEvery > 1 || j.TestRestore {
			return bad("log archiving and test restores need a built-in method")
		}
	} else {
		if IsUnsupportedMethod(j.Method) {
			return bad(fmt.Sprintf("method %q is unsupported (removed: physical backups reading the datadir on the DB host are not supported)", j.Method))
		}
		engine := MethodEngine(j.Method)
		if engine == "" || j.Method == MethodBinlog || j.Method == MethodWAL || j.Method == MethodOplog {
			return bad("unknown backup method " + fmt.Sprintf("%q", j.Method))
		}
		if j.Command != "" || j.ArtifactTemplate != "" || j.ManifestTemplate != "" {
			return bad("command, artifact and manifest templates are only used by the custom method")
		}
		if !safeText(j.Source, 256) {
			return bad("source identity is required for built-in methods")
		}
		needDB := j.Method == MethodPGDump
		if err := validateConn("connection", j.Conn, engine, needDB); err != nil {
			return err
		}
		if j.FullEvery < 0 || j.FullEvery > 1000 || j.FullEvery > 1 {
			return bad("incremental backup chains (full_every) are no longer supported")
		}
		if j.LogFlushSeconds != 0 && (j.LogFlushSeconds < 10 || j.LogFlushSeconds > 86400) {
			return bad("log_flush_seconds must be between 10 and 86400")
		}
		if j.TestRestore {
			if j.Method != MethodMySQLDump && j.Method != MethodMariaDBDump && j.Method != MethodPGDump {
				return bad("test restores support mysqldump, mariadb-dump and pg_dump")
			}
			if err := validateConn("scratch connection", j.Scratch, engine, false); err != nil {
				return err
			}
		}
	}
	if j.CompressLevel < 0 || j.CompressLevel > 4 {
		return bad("compress_level must be between 0 and 4")
	}
	if j.BandwidthKBps < 0 || j.BandwidthKBps > 100<<20 {
		return bad("bandwidth_kbps is out of range")
	}
	if j.VerifyEverySeconds != 0 && (j.VerifyEverySeconds < 300 || j.VerifyEverySeconds > 365*24*3600) {
		return bad("verify_every_seconds must be between 300 and one year")
	}
	if j.VerifySamplePercent < 0 || j.VerifySamplePercent > 100 {
		return bad("verify_sample_percent must be between 0 and 100")
	}
	if err := j.Retention.Validate(); err != nil {
		return err
	}
	for _, r := range []string{j.Repository, j.ReplicateTo} {
		if r != "" && !validID(r) {
			return bad("invalid repository id")
		}
	}
	if j.ReplicateTo != "" && j.ReplicateTo == j.Repository {
		return bad("replicate_to must name a different repository")
	}
	if (j.Repository == "" || j.Bucket != "") && !safeBucket(j.Bucket) {
		return bad("invalid object bucket (or choose a repository)")
	}
	if len(j.Prefix) > 512 || hasControl(j.Prefix) || strings.HasPrefix(j.Prefix, "/") {
		return bad("invalid object prefix")
	}
	for _, part := range strings.Split(strings.Trim(j.Prefix, "/"), "/") {
		if part == ".." {
			return bad("object prefix cannot contain parent path segments")
		}
	}
	tunnelSet := j.TunnelSSH != "" || j.TunnelUser != "" || j.TunnelRemote != "" || j.TunnelIdentity != "" || j.TunnelKnownHosts != ""
	if tunnelSet {
		if strings.TrimSpace(j.TunnelSSH) == "" || strings.TrimSpace(j.TunnelUser) == "" || strings.TrimSpace(j.TunnelRemote) == "" {
			return bad("SSH tunnel requires ssh host, user, and remote host:port")
		}
		if _, _, err := net.SplitHostPort(j.TunnelRemote); err != nil {
			return bad("SSH tunnel remote must be host:port")
		}
	}
	if j.TunnelIdentity != "" && !filepath.IsAbs(j.TunnelIdentity) {
		return bad("SSH identity file path must be absolute")
	}
	if j.TunnelKnownHosts != "" && !filepath.IsAbs(j.TunnelKnownHosts) {
		return bad("known_hosts path must be absolute")
	}
	if j.TunnelUser != "" && strings.ContainsAny(j.TunnelUser, "\x00\r\n") {
		return bad("invalid SSH username")
	}
	return nil
}

// RetryDelay returns the wait before retry number attempt (1-based) of a failed run, or 0 when the job
// should wait for its next regular interval.
func (j Job) RetryDelay(attempt int) time.Duration {
	if j.RetryMax <= 0 || attempt < 1 || attempt > j.RetryMax {
		return 0
	}
	d := time.Duration(j.RetryBackoffSeconds) * time.Second
	for i := 1; i < attempt && d < time.Duration(j.EverySeconds)*time.Second; i++ {
		d *= 2
	}
	if max := time.Duration(j.EverySeconds) * time.Second; d > max {
		d = max
	}
	return d
}

// ValidatePITR checks that continuous log archiving (log_archive) is possible for the job's method. It is
// separate from ValidateJob so that jobs stored before these rules existed can still be read, listed,
// stopped and run as plain base backups; only saving and starting the log archiver enforce it.
func ValidatePITR(j Job) error {
	bad := func(s string) error { return fmt.Errorf("%w: %s", ErrInvalid, s) }
	if !j.LogArchive || j.IsCustom() {
		return nil
	}
	if j.LogArchive {
		switch j.Method {
		case MethodMariaDBDump:
			return bad("MariaDB does not support binlog archiving")
		case MethodMySQLDump:
			if len(j.Conn.Databases) != 1 {
				return bad("PITR requires exactly one database")
			}
		case MethodPGBaseBackup:
			// ok
		case MethodPGDump:
			return bad("PITR needs pg_basebackup as the base backup")
		case MethodMongodump:
			// ok
		default:
			return bad(fmt.Sprintf("method %q does not support point-in-time recovery", j.Method))
		}
	}
	return nil
}

// ValidateJobForSave is ValidateJob plus the point-in-time-recovery rules; use it when a job is created
// or edited.
func ValidateJobForSave(j Job) error {
	if err := ValidateJob(j); err != nil {
		return err
	}
	return ValidatePITR(j)
}
