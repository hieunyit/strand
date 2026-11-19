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
	EnvEnc  []byte
	Created time.Time
	Updated time.Time
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

// Desired states.
const (
	DesiredRunning = "running"
	DesiredStopped = "stopped"
)

// Store is the metadata store contract.
type Store interface {
	Ping(ctx context.Context) error
	Close() error

	Admin(ctx context.Context) (Admin, bool, error)
	SetPasswordHash(ctx context.Context, hash string) error
	SetAPITokenHash(ctx context.Context, hash string) error

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
	DeletePipeline(ctx context.Context, name string) error

	SetDesired(ctx context.Context, name, desired string) error
	Desired(ctx context.Context) (map[string]string, error)

	UpsertJob(ctx context.Context, j Job) error
	ListJobs(ctx context.Context) ([]Job, error)
	DeleteJob(ctx context.Context, id string) error

	GetWizard(ctx context.Context, kind, pipeline string) ([]byte, bool, error)
	PutWizard(ctx context.Context, kind, pipeline string, data []byte) error

	AppendAudit(ctx context.Context, r AuditRow) error
	// AppendAuditBatch inserts many rows in one transaction (import of audit.log).
	AppendAuditBatch(ctx context.Context, rows []AuditRow) error
	// ListAudit returns the newest n rows, newest first.
	ListAudit(ctx context.Context, n int) ([]AuditRow, error)

	MetaGet(ctx context.Context, key string) (string, bool, error)
	MetaSet(ctx context.Context, key, value string) error

	// Central backup catalog metadata; payloads stay in object storage.
	CreateBackup(ctx context.Context, r backup.Record) (backup.Record, bool, error)
	GetBackup(ctx context.Context, id string) (backup.Record, bool, error)
	ListBackups(ctx context.Context, f backup.Filter) ([]backup.Record, error)
	UpdateBackupStatus(ctx context.Context, id string, expected, next backup.Status, verifiedAt *time.Time) (backup.Record, bool, error)
	FinalizeBackup(ctx context.Context, id string, expected backup.Status, bytes int64, sha256, manifestURI string, verifiedAt time.Time) (backup.Record, bool, error)

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
