// Package store is the PostgreSQL metadata store of `strand web` (admin credentials, sessions, pipeline
// registry and desired state, job history, wizard state, audit log). Store has two implementations: PG (real)
// and Mem (tests); the contract suite in storetest runs against both.
package store

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"strand/internal/backup"
)

// Admin is the single admin account.
type Admin struct {
	PasswordHash string
	APITokenHash string
}

// Session is a login session; IDHash is sha256 hex of the cookie value.
type Session struct {
	IDHash   string
	CSRF     string
	IP       string
	Created  time.Time
	LastSeen time.Time
	UserID   int64
	Username string
}

type Role string

const (
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

func (r Role) Valid() bool {
	return r == RoleAdmin || r == RoleOperator || r == RoleViewer
}

// User is a web console user with an RBAC role.
type User struct {
	ID                 int64      `json:"id"`
	Username           string     `json:"username"`
	DisplayName        string     `json:"display_name"`
	PasswordHash       string     `json:"-"`
	Role               Role       `json:"role"`
	Disabled           bool       `json:"disabled"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	LastLoginAt        *time.Time `json:"last_login_at,omitempty"`
	MustChangePassword bool       `json:"must_change_password"`
	FailedLoginCount   int        `json:"failed_login_count"`
	LockedUntil        *time.Time `json:"locked_until,omitempty"`
	LastFailedAt       *time.Time `json:"last_failed_at,omitempty"`
}

// UserToken is a scoped API bearer token belonging to a user.
type UserToken struct {
	ID          int64      `json:"id"`
	UserID      int64      `json:"user_id"`
	Name        string     `json:"name"`
	TokenHash   string     `json:"-"`
	TokenPrefix string     `json:"token_prefix"`
	CreatedAt   time.Time  `json:"created_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
}

// Pipeline is a registry row of a pipeline created from the web.
type Pipeline struct {
	Name        string
	ConfigPath  string
	EnvPath     string
	SystemdUnit string
	LogFile     string
	PidFile     string
	// ConfigYAML is the source of truth of the pipeline config; HasConfig=false means "not stored".
	ConfigYAML string
	HasConfig  bool
	// EnvEnc is the AES-GCM sealed .env (nil when no secret key was configured).
	EnvEnc []byte
	// Origin: created (web) | web.yaml (declared) | scan (discovered by a scan glob). "" = created.
	Origin string
	// MissingSince: non-zero when a declared/scanned pipeline's web.yaml entry or file has gone.
	MissingSince time.Time
	// Conflicts: JSON list of on-disk edits set aside because the database copy won ("" = none).
	Conflicts string
	Created   time.Time
	Updated   time.Time
}

// Pipeline origins.
const (
	OriginCreated = "created"
	OriginFile    = "web.yaml"
	OriginScan    = "scan"
)

func (p *Pipeline) normalize() {
	if p.Origin == "" {
		p.Origin = OriginCreated
	}
	if p.Conflicts == "" {
		p.Conflicts = "[]"
	}
}

// Job is one job-history row; Data is the JSON of web.JobStatus.
type Job struct {
	ID       string
	Pipeline string
	Started  time.Time
	Running  bool
	Data     []byte
}

// AuditRow is one append-only audit row; Data is the full JSON entry.
type AuditRow struct {
	ID       int64
	Time     time.Time
	IP       string
	Action   string
	Pipeline string
	Result   string
	Data     []byte
}

// MetaKV is one web_meta key/value.
type MetaKV struct{ Key, Value string }

// Desired states.
const (
	DesiredRunning = "running"
	DesiredStopped = "stopped"
)

// ReasonObservedStop: the desired state was set to stopped because the active web observed a clean stop it
// did not initiate (`strand stop`, SIGTERM from an operator): such a pipeline is not autostarted again.
const ReasonObservedStop = "observed_stop"

// DesiredState is one pipeline_desired row. Reason is "" when set by a web action, ReasonObservedStop otherwise.
type DesiredState struct {
	Desired string
	Reason  string
	Updated time.Time
}

// Store is the metadata store contract.
type Store interface {
	Ping(ctx context.Context) error
	Close() error

	Admin(ctx context.Context) (Admin, bool, error)
	SetPasswordHash(ctx context.Context, hash string) error
	SetAPITokenHash(ctx context.Context, hash string) error

	// User management & RBAC
	GetUser(ctx context.Context, username string) (User, bool, error)
	GetUserByID(ctx context.Context, id int64) (User, bool, error)
	ListUsers(ctx context.Context) ([]User, error)
	CreateUser(ctx context.Context, u User) (User, error)
	UpdateUser(ctx context.Context, u User) error
	SetUserPassword(ctx context.Context, id int64, passwordHash string, mustChange bool) error
	RecordLoginSuccess(ctx context.Context, id int64, at time.Time) error
	RecordLoginFailure(ctx context.Context, id int64, count int, lockedUntil *time.Time, at time.Time) error
	ResetLoginFailures(ctx context.Context, id int64) error
	DeleteUser(ctx context.Context, id int64) error

	// User sessions
	PurgeUserSessions(ctx context.Context, userID int64) error
	PurgeOtherUserSessions(ctx context.Context, userID int64, keepIDHash string) error
	ListUserSessions(ctx context.Context, userID int64) ([]Session, error)

	// User tokens
	CreateUserToken(ctx context.Context, tok UserToken) (UserToken, error)
	ListUserTokens(ctx context.Context, userID int64) ([]UserToken, error)
	RevokeUserToken(ctx context.Context, id int64, userID int64) error
	AuthenticateToken(ctx context.Context, tokenHash string) (User, UserToken, bool, error)

	PutSession(ctx context.Context, s Session) error
	GetSession(ctx context.Context, idHash string) (Session, bool, error)
	TouchSession(ctx context.Context, idHash string, t time.Time) error
	DeleteSession(ctx context.Context, idHash string) error
	// PurgeSessions deletes sessions idle since before idleBefore or created before createdBefore.
	PurgeSessions(ctx context.Context, idleBefore, createdBefore time.Time) error
	CountSessions(ctx context.Context) (int, error)

	UpsertPipeline(ctx context.Context, p Pipeline) error
	GetPipeline(ctx context.Context, name string) (Pipeline, bool, error)
	ListPipelines(ctx context.Context) ([]Pipeline, error)
	// CreatePipeline inserts p only if no row has that name (discovery: never overwrites). Reports whether
	// it inserted.
	CreatePipeline(ctx context.Context, p Pipeline) (bool, error)
	// UpdatePipelineMeta updates only the registry metadata of an existing row (paths, unit, origin,
	// missing_since), never config_yaml/env_enc/conflicts: safe against a concurrent config edit.
	UpdatePipelineMeta(ctx context.Context, p Pipeline) error
	// SetPipelineConflicts replaces the conflicts JSON of a row.
	SetPipelineConflicts(ctx context.Context, name, conflicts string) error
	// DeletePipeline removes the registry row and its desired state (files are left alone).
	DeletePipeline(ctx context.Context, name string) error

	// SetDesired records a desired state chosen from the web (clears Reason).
	SetDesired(ctx context.Context, name, desired string) error
	// ObserveStop flips desired running -> stopped with Reason=ReasonObservedStop, atomically (compare-and-set:
	// a concurrent Start that set running again wins only if it commits later). Reports whether it changed.
	ObserveStop(ctx context.Context, name string) (bool, error)
	Desired(ctx context.Context) (map[string]string, error)
	DesiredStates(ctx context.Context) (map[string]DesiredState, error)

	UpsertJob(ctx context.Context, j Job) error
	ListJobs(ctx context.Context) ([]Job, error)
	DeleteJob(ctx context.Context, id string) error
	// AppendJobOutput stores data at byte offset off of the job's output. Idempotent: a chunk already stored
	// at off is kept (re-shipping after a restart is harmless). The job row must exist.
	AppendJobOutput(ctx context.Context, id string, off int64, data []byte) error
	// ReadJobOutput returns up to max bytes of the job output starting at off (shorter at the end).
	ReadJobOutput(ctx context.Context, id string, off int64, max int) ([]byte, error)
	// JobOutputSize is the number of contiguous bytes stored from offset 0.
	JobOutputSize(ctx context.Context, id string) (int64, error)

	GetWizard(ctx context.Context, kind, pipeline string) ([]byte, bool, error)
	PutWizard(ctx context.Context, kind, pipeline string, data []byte) error

	AppendAudit(ctx context.Context, r AuditRow) error
	// AppendAuditBatch inserts many rows in one transaction (import of audit.log). A non-nil progress is
	// upserted into web_meta in the same transaction, so an interrupted import can resume exactly where it
	// stopped (audit_log is append-only: duplicates could never be removed).
	AppendAuditBatch(ctx context.Context, rows []AuditRow, progress *MetaKV) error
	// ListAudit returns the newest n rows, newest first.
	ListAudit(ctx context.Context, n int) ([]AuditRow, error)

	MetaGet(ctx context.Context, key string) (string, bool, error)
	MetaSet(ctx context.Context, key, value string) error

	MetricsStore
	SettingsStore
	LeaseStore
	AuditArchiveStore
	ExportStore

	// Central backup catalog metadata; payloads stay in object storage.
	CreateBackup(ctx context.Context, r backup.Record) (backup.Record, bool, error)
	GetBackup(ctx context.Context, id string) (backup.Record, bool, error)
	ListBackups(ctx context.Context, f backup.Filter) ([]backup.Record, error)
	UpdateBackupStatus(ctx context.Context, id string, expected, next backup.Status, verifiedAt *time.Time) (backup.Record, bool, error)
	FinalizeBackup(ctx context.Context, id string, expected backup.Status, bytes int64, sha256, manifestURI string, verifiedAt time.Time) (backup.Record, bool, error)
	CreateBackupJob(ctx context.Context, j backup.Job) error
	GetBackupJob(ctx context.Context, id string) (backup.Job, bool, error)
	ListBackupJobs(ctx context.Context) ([]backup.Job, error)
	UpdateBackupJob(ctx context.Context, j backup.Job) (bool, error)
	SetBackupJobNextRun(ctx context.Context, id string, expectedUpdatedAt time.Time, nextRunAt time.Time) (bool, error)
	BeginBackupJobRun(ctx context.Context, id string, expectedUpdatedAt time.Time) (time.Time, bool, error)
	FinishBackupJobRun(ctx context.Context, id string, startedAt time.Time, status backup.RunStatus) (bool, error)
	RecoverBackupJobRun(ctx context.Context, id string, expectedUpdatedAt time.Time) (bool, error)
	DeleteBackupJob(ctx context.Context, id string) (bool, error)
	BackupProStore

	// Locked runs fn while holding an exclusive, cross-instance lock (advisory lock) named by key.
	Locked(ctx context.Context, key string, fn func(ctx context.Context) error) error
}

var (
	urlPwRE = regexp.MustCompile(`(?i)(postgres(?:ql)?://[^:/@\s]*:)[^@\s]*@`)
	kvPwRE  = regexp.MustCompile(`(?i)(\bpassword\s*=\s*)('[^']*'|\S+)`)
)

// Redact hides the password of a PostgreSQL DSN (URL or key=value form) in s. Safe on any text.
func Redact(s string) string {
	s = urlPwRE.ReplaceAllString(s, "${1}xxxxx@")
	return kvPwRE.ReplaceAllString(s, "${1}xxxxx")
}

// RedactError returns an error equal to err with the DSN password (and any password pattern) removed.
func RedactError(err error, dsn string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if u, perr := url.Parse(dsn); perr == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && pw != "" {
			msg = strings.ReplaceAll(msg, pw, "xxxxx")
		}
	}
	return errors.New(Redact(msg))
}
