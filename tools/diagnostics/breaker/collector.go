package breaker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// SelectedStatusCounters are the exact counters required for diagnosing InnoDB log/buffer/fsync/commit pressure.
var SelectedStatusCounters = []string{
	"Innodb_log_waits",
	"Innodb_buffer_pool_wait_free",
	"Innodb_data_fsyncs",
	"Innodb_os_log_fsyncs",
	"Innodb_os_log_written",
	"Innodb_data_writes",
	"Handler_commit",
}

var SelectedSafeVariables = []string{
	"version",
	"version_comment",
	"innodb_log_file_size",
	"innodb_log_buffer_size",
	"innodb_buffer_pool_size",
	"innodb_flush_log_at_trx_commit",
	"max_connections",
}

// DiagnosticCollector performs read-only database inspections without query text or secrets.
type DiagnosticCollector struct {
	db            *sql.DB
	redactedDSN   string
	mu            sync.Mutex
	lastCounters  *GlobalCounters
	counterResets int
}

// NewDiagnosticCollector creates a collector with an explicit DB handle and redacted DSN.
// It will NEVER auto-connect without an explicit DSN.
func NewDiagnosticCollector(db *sql.DB, rawDSN string) *DiagnosticCollector {
	return &DiagnosticCollector{
		db:          db,
		redactedDSN: RedactDSN(rawDSN),
	}
}

// RedactedDSN returns the safe, redacted connection string.
func (c *DiagnosticCollector) RedactedDSN() string {
	return c.redactedDSN
}

// Collect reads one diagnostic snapshot from the database.
func (c *DiagnosticCollector) Collect(ctx context.Context) (*DiagnosticSnapshot, error) {
	if c.db == nil {
		return nil, fmt.Errorf("collector: database connection is nil; explicit DSN required")
	}

	snap := &DiagnosticSnapshot{
		CollectedAt: time.Now().UTC(),
	}

	// 1. Version and Safe Variables
	vars, err := c.collectSafeVariables(ctx)
	if err != nil {
		return nil, fmt.Errorf("collect variables: %w", err)
	}
	snap.Variables = vars
	snap.ServerVersion = vars.Version

	// 2. Selected Status Counters
	counters, err := c.collectStatusCounters(ctx)
	if err != nil {
		return nil, fmt.Errorf("collect status counters: %w", err)
	}
	counters.Timestamp = snap.CollectedAt
	snap.StatusCounters = *counters

	// Calculate delta from previous sample if available
	c.mu.Lock()
	if c.lastCounters != nil {
		delta := CalculateCounterDelta(*c.lastCounters, *counters)
		if delta.ResetDetected {
			c.counterResets++
		}
		snap.StatusDelta = &delta
	}
	c.lastCounters = counters
	c.mu.Unlock()

	// 3. Active Waits (without query text or passwords)
	waits, err := c.collectActiveWaits(ctx)
	if err != nil {
		// Non-fatal if permission denied on processlist, but record empty
		waits = []ActiveWaitEntry{}
	}
	snap.ActiveWaits = waits

	// 4. SHOW ENGINE INNODB STATUS (sanitized)
	engineStatus, err := c.collectEngineStatus(ctx)
	if err == nil {
		snap.EngineStatus = *engineStatus
	}

	return snap, nil
}

// collectSafeVariables reads safe configuration parameters.
func (c *DiagnosticCollector) collectSafeVariables(ctx context.Context) (SafeVariables, error) {
	var vars SafeVariables

	quoted := make([]string, len(SelectedSafeVariables))
	for i, v := range SelectedSafeVariables {
		quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	query := "SHOW GLOBAL VARIABLES WHERE Variable_name IN (" + strings.Join(quoted, ", ") + ")"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return vars, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, val string
		if err := rows.Scan(&name, &val); err != nil {
			return vars, err
		}
		nameLower := strings.ToLower(name)
		switch nameLower {
		case "version":
			vars.Version = val
		case "version_comment":
			vars.VersionComment = val
		case "innodb_log_file_size":
			vars.InnodbLogFileSize, _ = strconv.ParseUint(val, 10, 64)
		case "innodb_log_buffer_size":
			vars.InnodbLogBufferSize, _ = strconv.ParseUint(val, 10, 64)
		case "innodb_buffer_pool_size":
			vars.InnodbBufferPoolSize, _ = strconv.ParseUint(val, 10, 64)
		case "innodb_flush_log_at_trx_commit":
			vars.InnodbFlushLogAtTrxCommit, _ = strconv.Atoi(val)
		case "max_connections":
			vars.MaxConnections, _ = strconv.ParseUint(val, 10, 64)
		}
	}
	return vars, rows.Err()
}

// collectStatusCounters reads the required InnoDB and commit counters.
func (c *DiagnosticCollector) collectStatusCounters(ctx context.Context) (*GlobalCounters, error) {
	quoted := make([]string, len(SelectedStatusCounters))
	for i, v := range SelectedStatusCounters {
		quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	query := "SHOW GLOBAL STATUS WHERE Variable_name IN (" + strings.Join(quoted, ", ") + ")"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := &GlobalCounters{
		RawStatus: make(map[string]uint64),
	}

	for rows.Next() {
		var name, val string
		if err := rows.Scan(&name, &val); err != nil {
			return nil, err
		}
		var num uint64
		fmt.Sscanf(strings.TrimSpace(val), "%d", &num)
		out.RawStatus[name] = num

		switch name {
		case "Innodb_log_waits":
			out.InnodbLogWaits = num
		case "Innodb_buffer_pool_wait_free":
			out.InnodbBufferPoolWaitFree = num
		case "Innodb_data_fsyncs":
			out.InnodbDataFsyncs = num
		case "Innodb_os_log_fsyncs":
			out.InnodbOsLogFsyncs = num
		case "Innodb_os_log_written":
			out.InnodbOsLogWritten = num
		case "Innodb_data_writes":
			out.InnodbDataWrites = num
		case "Handler_commit":
			out.HandlerCommit = num
		}
	}
	return out, rows.Err()
}

// collectActiveWaits queries processlist/waits without query text or passwords.
func (c *DiagnosticCollector) collectActiveWaits(ctx context.Context) ([]ActiveWaitEntry, error) {
	// Notice: explicitly do NOT select Info to avoid exposing query text or passwords.
	query := "SELECT id, user, host, db, command, time, state FROM information_schema.processlist WHERE command != 'Sleep' ORDER BY time DESC LIMIT 50"
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ActiveWaitEntry
	for rows.Next() {
		var entry ActiveWaitEntry
		var user, host, db, state sql.NullString
		if err := rows.Scan(&entry.ID, &user, &host, &db, &entry.Command, &entry.Time, &state); err != nil {
			return nil, err
		}
		entry.User = user.String
		// Redact host port/IP to avoid leakage if desired, or keep host
		entry.Host = host.String
		entry.DB = db.String
		entry.State = state.String
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// collectEngineStatus runs SHOW ENGINE INNODB STATUS and sanitizes it.
func (c *DiagnosticCollector) collectEngineStatus(ctx context.Context) (*SanitizedEngineStatus, error) {
	row := c.db.QueryRowContext(ctx, "SHOW ENGINE INNODB STATUS")
	var typ, name, statusText string
	if err := row.Scan(&typ, &name, &statusText); err != nil {
		return nil, err
	}
	return ParseAndSanitizeEngineStatus(statusText, time.Now().UTC()), nil
}

var (
	lsnRegex           = regexp.MustCompile(`Log sequence number\s+(\d+)`)
	logFlushedRegex    = regexp.MustCompile(`Log flushed up to\s+(\d+)`)
	pagesFlushedRegex  = regexp.MustCompile(`Pages flushed up to\s+(\d+)`)
	checkpointRegex    = regexp.MustCompile(`Last checkpoint at\s+(\d+)`)
	pendingFsyncsRegex = regexp.MustCompile(`(\d+)\s+pending log flushes \(fsync\),\s+(\d+)\s+pending chkp writes`)
	pendingIOsRegex    = regexp.MustCompile(`(\d+)\s+pending normal aio reads.*pending aio writes`)
	semaphoreRegex     = regexp.MustCompile(`(?s)SEMAPHORES\n-+\n(.*?)\n-+`)
)

// ParseAndSanitizeEngineStatus parses key metrics and completely sanitizes query text & passwords.
func ParseAndSanitizeEngineStatus(raw string, ts time.Time) *SanitizedEngineStatus {
	res := &SanitizedEngineStatus{
		Timestamp:        ts,
		RawSanitizedText: SanitizeEngineStatus(raw),
	}

	if m := lsnRegex.FindStringSubmatch(raw); len(m) > 1 {
		res.LogSequenceNumber, _ = strconv.ParseUint(m[1], 10, 64)
	}
	if m := logFlushedRegex.FindStringSubmatch(raw); len(m) > 1 {
		res.LogFlushedUpTo, _ = strconv.ParseUint(m[1], 10, 64)
	}
	if m := pagesFlushedRegex.FindStringSubmatch(raw); len(m) > 1 {
		res.PagesFlushedUpTo, _ = strconv.ParseUint(m[1], 10, 64)
	}
	if m := checkpointRegex.FindStringSubmatch(raw); len(m) > 1 {
		res.LastCheckpointAt, _ = strconv.ParseUint(m[1], 10, 64)
	}
	if m := pendingFsyncsRegex.FindStringSubmatch(raw); len(m) > 2 {
		res.LogPendingFsyncs, _ = strconv.ParseUint(m[1], 10, 64)
		res.PendingChkpWrites, _ = strconv.ParseUint(m[2], 10, 64)
	}
	if m := pendingIOsRegex.FindStringSubmatch(raw); len(m) > 1 {
		res.PendingBufferPoolIO, _ = strconv.ParseUint(m[1], 10, 64)
	}
	if m := semaphoreRegex.FindStringSubmatch(res.RawSanitizedText); len(m) > 1 {
		res.SemaphoresWaitInfo = strings.TrimSpace(m[1])
	}

	return res
}

// CalculateCounterDelta computes difference between previous and current counters, detecting resets.
func CalculateCounterDelta(prev, curr GlobalCounters) GlobalCounterDelta {
	delta := GlobalCounterDelta{
		ElapsedDuration: curr.Timestamp.Sub(prev.Timestamp),
	}

	calc := func(p, c uint64) (uint64, bool) {
		if c < p {
			// Counter reset detected (e.g. server reboot or FLUSH STATUS)
			return c, true
		}
		return c - p, false
	}

	var reset bool
	var r bool

	delta.DeltaInnodbLogWaits, r = calc(prev.InnodbLogWaits, curr.InnodbLogWaits)
	reset = reset || r
	delta.DeltaBufferPoolWaitFree, r = calc(prev.InnodbBufferPoolWaitFree, curr.InnodbBufferPoolWaitFree)
	reset = reset || r
	delta.DeltaInnodbDataFsyncs, r = calc(prev.InnodbDataFsyncs, curr.InnodbDataFsyncs)
	reset = reset || r
	delta.DeltaInnodbOsLogFsyncs, r = calc(prev.InnodbOsLogFsyncs, curr.InnodbOsLogFsyncs)
	reset = reset || r
	delta.DeltaInnodbOsLogWritten, r = calc(prev.InnodbOsLogWritten, curr.InnodbOsLogWritten)
	reset = reset || r
	delta.DeltaInnodbDataWrites, r = calc(prev.InnodbDataWrites, curr.InnodbDataWrites)
	reset = reset || r
	delta.DeltaHandlerCommit, r = calc(prev.HandlerCommit, curr.HandlerCommit)
	reset = reset || r

	delta.ResetDetected = reset
	return delta
}

// SaveProtectedSnapshot writes a diagnostic snapshot to disk with strict 0600 permissions.
func SaveProtectedSnapshot(snap *DiagnosticSnapshot, destPath string) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	return SaveProtectedArtifact(data, destPath)
}

// SaveProtectedArtifact replaces the destination atomically with a private file.
// Writing directly would preserve an existing permissive mode, expose partial
// contents, and follow a destination symlink or modify other hard links.
func SaveProtectedArtifact(data []byte, destPath string) error {
	f, err := os.CreateTemp(filepath.Dir(destPath), ".breaker-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), destPath)
}
