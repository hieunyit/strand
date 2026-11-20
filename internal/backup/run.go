package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// NewRunID generates a random run ID with prefix "brun-".
func NewRunID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "brun-" + hex.EncodeToString(b[:]), nil
}

// Run triggers.
const (
	TriggerSchedule = "schedule"
	TriggerManual   = "manual"
	TriggerRetry    = "retry"
	TriggerAPI      = "api"
)

// Run statuses for backup_runs table (spec 2.4: running|success|failed|cancelled|missed|partial).
// RunStatusRunning, RunStatusFailed, RunStatusCancelled are declared in job.go.
const (
	RunStatusSuccess = "success"
	RunStatusPartial = "partial"
)

// Normalized error codes per spec 2.4.
const (
	ErrCodeAuthFailed      = "auth_failed"
	ErrCodeNetwork         = "network"
	ErrCodePermission      = "permission"
	ErrCodeDiskFull        = "disk_full"
	ErrCodeRepoUnreachable = "repo_unreachable"
	ErrCodeToolMissing     = "tool_missing"
	ErrCodeToolFailed      = "tool_failed"
	ErrCodeTimeout         = "timeout"
	ErrCodeCancelled       = "cancelled"
)

// RemediationHints maps normalized error codes to actionable advice.
var RemediationHints = map[string]string{
	ErrCodeAuthFailed:      "Check database credentials or password environment variable.",
	ErrCodeNetwork:         "Check network connectivity and firewall rules to the database host.",
	ErrCodePermission:      "Grant required database privileges to the backup user (e.g. RELOAD, REPLICATION CLIENT, LOCK TABLES).",
	ErrCodeDiskFull:        "Free up disk space in the temporary staging directory.",
	ErrCodeRepoUnreachable: "Verify repository endpoint, credentials, bucket name, and network access to object storage.",
	ErrCodeToolMissing:     "Install the required native backup tool or set STRAND_BACKUP_TOOLS_DIR.",
	ErrCodeToolFailed:      "Native backup tool exited with error; check process logs for details.",
	ErrCodeTimeout:         "Backup exceeded timeout limit; check database load or increase timeout.",
	ErrCodeCancelled:       "Backup was cancelled by operator.",
}

// Run records the execution history of a backup job (persisted in backup_runs).
type Run struct {
	ID           string        `json:"id"`
	JobID        string        `json:"job_id"`
	Trigger      string        `json:"trigger"` // schedule|manual|retry|api
	StartedAt    time.Time     `json:"started_at"`
	FinishedAt   *time.Time    `json:"finished_at,omitempty"`
	Status       string        `json:"status"` // running|success|failed|cancelled|missed|partial
	BytesRead    int64         `json:"bytes_read"`
	BytesWritten int64         `json:"bytes_written"`
	Duration     time.Duration `json:"duration"` // duration of run
	BackupID     string        `json:"backup_id,omitempty"`
	ErrorCode    string        `json:"error_code,omitempty"`
	ErrorMessage string        `json:"error_message,omitempty"`
	ProcessID    string        `json:"process_id,omitempty"`
	Attempt      int           `json:"attempt"`
}

// Remediation returns the actionable remediation advice for this run's error code.
func (r Run) Remediation() string {
	if r.ErrorCode == "" {
		return ""
	}
	return RemediationHints[r.ErrorCode]
}

// RunNextCursor returns the cursor for the page after rows when the page is full, else "".
func RunNextCursor(rows []Run, limit int) string {
	if limit <= 0 || len(rows) < limit {
		return ""
	}
	last := rows[len(rows)-1]
	return Cursor{CreatedAt: last.StartedAt, ID: last.ID}.Encode()
}

// NormalizeErrorCode maps an execution error to a normalized spec 2.4 error code.
func NormalizeErrorCode(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "cancel") || errors.Is(err, context.Canceled):
		return ErrCodeCancelled
	case strings.Contains(msg, "deadline exceeded") || strings.Contains(msg, "timeout") || errors.Is(err, context.DeadlineExceeded):
		return ErrCodeTimeout
	case strings.Contains(msg, "access denied") || strings.Contains(msg, "authentication failed") || strings.Contains(msg, "password") || strings.Contains(msg, "bad auth") || strings.Contains(msg, "credentials"):
		return ErrCodeAuthFailed
	case strings.Contains(msg, "permission") || strings.Contains(msg, "privilege") || strings.Contains(msg, "super") || strings.Contains(msg, "denied"):
		return ErrCodePermission
	case strings.Contains(msg, "no space left on device") || strings.Contains(msg, "disk full") || strings.Contains(msg, "disk_full"):
		return ErrCodeDiskFull
	case strings.Contains(msg, "repository") || strings.Contains(msg, "bucket") || strings.Contains(msg, "s3") || strings.Contains(msg, "nosuchbucket") || strings.Contains(msg, "repo_unreachable"):
		return ErrCodeRepoUnreachable
	case strings.Contains(msg, "not found") || strings.Contains(msg, "lookpath") || strings.Contains(msg, "tool_missing") || strings.Contains(msg, "executable file not found"):
		return ErrCodeToolMissing
	case strings.Contains(msg, "connection refused") || strings.Contains(msg, "dial") || strings.Contains(msg, "no route to host") || strings.Contains(msg, "reset by peer") || strings.Contains(msg, "broken pipe") || strings.Contains(msg, "network"):
		return ErrCodeNetwork
	default:
		return ErrCodeToolFailed
	}
}

type runReportKey struct{}

// RunReport carries runtime metrics collected during backup execution.
type RunReport struct {
	BackupID     string
	BytesRead    int64
	BytesWritten int64
}

// ContextWithRunReport returns a context carrying a mutable RunReport.
func ContextWithRunReport(ctx context.Context) (context.Context, *RunReport) {
	rep := &RunReport{}
	return context.WithValue(ctx, runReportKey{}, rep), rep
}

// SetRunReport updates the RunReport stored in ctx if present.
func SetRunReport(ctx context.Context, rep *RunReport) {
	if rep == nil {
		return
	}
	if existing, ok := ctx.Value(runReportKey{}).(*RunReport); ok && existing != nil {
		*existing = *rep
	}
}

// GetRunReport returns the RunReport stored in ctx, or nil if none.
func GetRunReport(ctx context.Context) *RunReport {
	if rep, ok := ctx.Value(runReportKey{}).(*RunReport); ok {
		return rep
	}
	return nil
}
