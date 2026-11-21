package runner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"strand/internal/backup"
)

// Native tool preflight. A built-in backup method runs a native client tool (mysqldump, pg_dump, ...) on the
// Strand host; a job whose tool is missing fails on every run. These helpers say which tools a method needs
// and whether they resolve (same lookup as Start: ResolveTool), so the Web and the CLI refuse such a job
// before anything is scheduled or recorded.

// Tool purposes.
const (
	ToolForBackup      = "backup"
	ToolForRestore     = "restore"
	ToolForTestRestore = "test-restore"
)

// KnownTools lists every native tool a built-in method may run, in display order.
var KnownTools = []string{"mysqldump", "mariadb-dump", "mysql", "mysqlbinlog", "mydumper", "pg_dump", "pg_restore",
	"pg_basebackup", "mongodump", "mongorestore"}

// ToolHint is the fix suggested when a native tool is missing.
const ToolHint = "install it in the Strand Web host/image or set " + EnvToolsDir

// BackupTools returns the native tools a backup with method runs. Custom (wrapper script) jobs and the
// continuous log methods (binlog, WAL, oplog: implemented in Go) need none.
func BackupTools(method string) []string {
	switch method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump, backup.MethodMydumper, backup.MethodPGDump,
		backup.MethodPGBaseBackup, backup.MethodMongodump:
		return []string{method} // the method is named after its tool
	}
	return nil
}

// RestoreTools returns the native tools restoring a backup of method runs; withLogs adds the tool replaying
// archived logs (point-in-time recovery). pg_basebackup restores are extracted in Go; WAL/oplog replay needs
// no client tool.
func RestoreTools(method string, withLogs bool) []string {
	switch method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump:
		if withLogs {
			return []string{"mysql", "mysqlbinlog"}
		}
		return []string{"mysql"}
	case backup.MethodPGDump:
		return []string{"pg_restore"}
	case backup.MethodMongodump:
		return []string{"mongorestore"}
	}
	return nil
}

// TestRestoreTools returns the native tools a test restore (verify --test-restore) of method runs.
func TestRestoreTools(method string) []string {
	switch method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump, backup.MethodPGDump:
		return RestoreTools(method, false)
	}
	return nil
}

// ToolNeed is one native tool needed for a purpose.
type ToolNeed struct {
	Tool    string `json:"tool"`
	Purpose string `json:"purpose"`
	Method  string `json:"method"`
}

// JobToolNeeds lists the native tools an enabled job runs: its backup tool, plus the test-restore tool
// when the job test-restores after verification.
func JobToolNeeds(j backup.Job) []ToolNeed {
	if j.IsCustom() {
		return nil
	}
	out := BackupToolNeeds(j.Method)
	if j.TestRestore {
		out = append(out, TestRestoreToolNeeds(j.Method)...)
	}
	return out
}

// TestRestoreToolNeeds lists the native tools a test restore of method runs.
func TestRestoreToolNeeds(method string) []ToolNeed {
	var out []ToolNeed
	for _, t := range TestRestoreTools(method) {
		out = append(out, ToolNeed{Tool: t, Purpose: ToolForTestRestore, Method: method})
	}
	return out
}

// BackupToolNeeds lists the native tools a backup with method runs.
func BackupToolNeeds(method string) []ToolNeed {
	var out []ToolNeed
	for _, t := range BackupTools(method) {
		out = append(out, ToolNeed{Tool: t, Purpose: ToolForBackup, Method: method})
	}
	return out
}

// RestoreToolNeeds lists the native tools restoring a backup of method runs.
func RestoreToolNeeds(method string, withLogs bool) []ToolNeed {
	var out []ToolNeed
	for _, t := range RestoreTools(method, withLogs) {
		out = append(out, ToolNeed{Tool: t, Purpose: ToolForRestore, Method: method})
	}
	return out
}

// MissingTool is a needed tool that does not resolve.
type MissingTool struct {
	ToolNeed
	Reason string `json:"reason,omitempty"`
}

// ToolMissingError reports needed native tools that are not available. Its message starts with the
// normalized error code (tool_missing) and names every missing tool and the fix.
type ToolMissingError struct {
	Missing []MissingTool
}

func (e *ToolMissingError) Error() string {
	var parts []string
	for _, m := range e.Missing {
		if m.Reason == "" {
			parts = append(parts, fmt.Sprintf("native tool %s not found (needed for %s of method %s)", m.Tool, m.Purpose, m.Method))
		} else {
			parts = append(parts, fmt.Sprintf("native tool %s is not usable (needed for %s of method %s): %s", m.Tool, m.Purpose, m.Method, m.Reason))
		}
	}
	return backup.ErrCodeToolMissing + ": " + strings.Join(parts, "; ") + "; " + ToolHint
}

// Tools returns the names of the missing tools.
func (e *ToolMissingError) Tools() []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range e.Missing {
		if !seen[m.Tool] {
			seen[m.Tool] = true
			out = append(out, m.Tool)
		}
	}
	return out
}

// CheckTools resolves every needed tool (ResolveTool: STRAND_BACKUP_TOOLS_DIR, STRAND_WEB_BACKUP_COMMAND_DIR,
// PATH, with the same permission checks as a run) and returns a *ToolMissingError naming those that do not.
func CheckTools(getenv Getenv, needs []ToolNeed) error {
	if getenv == nil {
		getenv = os.Getenv
	}
	var missing []MissingTool
	for _, n := range needs {
		if _, err := ResolveTool(getenv, n.Tool); err != nil {
			// Reason only when a file exists but may not be run (permissions): "not found" says the rest.
			reason := err.Error()
			if errors.Is(err, fs.ErrNotExist) || strings.Contains(reason, " not found ") {
				reason = ""
			}
			missing = append(missing, MissingTool{ToolNeed: n, Reason: reason})
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return &ToolMissingError{Missing: missing}
}

// CheckJobTools is CheckTools for the tools of a job.
func CheckJobTools(getenv Getenv, j backup.Job) error { return CheckTools(getenv, JobToolNeeds(j)) }

// ToolStatus describes one native tool on this host.
type ToolStatus struct {
	Name    string `json:"name"`
	Found   bool   `json:"found"`
	Path    string `json:"path,omitempty"`
	Version string `json:"version,omitempty"`
	Error   string `json:"error,omitempty"`
	// BackupMethods / RestoreMethods: the methods whose backups / restores (or test restores) run the tool.
	BackupMethods  []string `json:"backup_methods,omitempty"`
	RestoreMethods []string `json:"restore_methods,omitempty"`
}

// toolMethods lists the built-in base-backup methods.
var toolMethods = []string{backup.MethodMySQLDump, backup.MethodMariaDBDump, backup.MethodMydumper, backup.MethodPGDump,
	backup.MethodPGBaseBackup, backup.MethodMongodump}

// ToolVersionTTL is how long a probed version is reused (keyed by path, size and modification time).
var ToolVersionTTL = 5 * time.Minute

// toolProbeTimeout bounds one `tool --version` probe.
var toolProbeTimeout = 3 * time.Second

type versionEntry struct {
	key     string
	version string
	at      time.Time
}

var versionCache struct {
	mu sync.Mutex
	m  map[string]versionEntry
}

// toolVersion probes `<tool> --version` (no shell, short timeout, secrets stripped from the environment)
// and caches the first output line.
func toolVersion(ctx context.Context, getenv Getenv, name, path string) string {
	fi, err := os.Stat(path)
	if err != nil {
		return ""
	}
	key := fmt.Sprintf("%s|%d|%d", path, fi.Size(), fi.ModTime().UnixNano())
	versionCache.mu.Lock()
	if e, ok := versionCache.m[name]; ok && e.key == key && time.Since(e.at) < ToolVersionTTL {
		versionCache.mu.Unlock()
		return e.version
	}
	versionCache.mu.Unlock()
	pctx, cancel := context.WithTimeout(ctx, toolProbeTimeout)
	defer cancel()
	out, _ := RunCapture(pctx, getenv, name, "--version")
	ver := ""
	for _, line := range strings.Split(out, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			ver = line
			break
		}
	}
	if len(ver) > 200 {
		ver = ver[:200]
	}
	versionCache.mu.Lock()
	if versionCache.m == nil {
		versionCache.m = map[string]versionEntry{}
	}
	versionCache.m[name] = versionEntry{key: key, version: ver, at: time.Now()}
	versionCache.mu.Unlock()
	return ver
}

// ToolsStatus reports every known native tool: found or missing, its resolved path and version, and the
// methods that need it. Version probes run in parallel and are cached (ToolVersionTTL).
func ToolsStatus(ctx context.Context, getenv Getenv) []ToolStatus {
	if getenv == nil {
		getenv = os.Getenv
	}
	out := make([]ToolStatus, len(KnownTools))
	idx := map[string]int{}
	for i, name := range KnownTools {
		idx[name] = i
		out[i].Name = name
	}
	add := func(list *[]string, m string) {
		for _, x := range *list {
			if x == m {
				return
			}
		}
		*list = append(*list, m)
	}
	for _, m := range toolMethods {
		for _, t := range BackupTools(m) {
			add(&out[idx[t]].BackupMethods, m)
		}
		for _, t := range RestoreTools(m, true) {
			add(&out[idx[t]].RestoreMethods, m)
		}
	}
	var wg sync.WaitGroup
	for i := range out {
		p, err := ResolveTool(getenv, out[i].Name)
		if err != nil {
			out[i].Error = err.Error()
			continue
		}
		out[i].Found, out[i].Path = true, p
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out[i].Version = toolVersion(ctx, getenv, out[i].Name, out[i].Path)
		}(i)
	}
	wg.Wait()
	for i := range out {
		sort.Strings(out[i].BackupMethods)
		sort.Strings(out[i].RestoreMethods)
	}
	return out
}
