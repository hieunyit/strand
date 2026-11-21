package web

import (
	"errors"
	"net/http"
	"os"

	"strand/internal/backup"
	"strand/internal/backup/runner"
)

// Native tool preflight for built-in backup methods. The backup processes the Web starts inherit its
// environment, so the tools are resolved here exactly as a run resolves them (runner.ResolveTool:
// STRAND_BACKUP_TOOLS_DIR, STRAND_WEB_BACKUP_COMMAND_DIR, PATH). A job whose tool is missing is never
// scheduled or run: enabling, saving an enabled job, Run now, the boot-time resume and the reconcile loop
// all refuse it (HTTP 409, code tool_missing). Disabled jobs may still be saved, with a warning, so an
// administrator can prepare jobs before the tools are installed.

func init() { pageNames = append(pageNames, "backups_tools") }

func (s *Server) backupToolRoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backup-tools", s.auth(true, s.apiBackupTools))
	mux.Handle("GET /backups/tools", s.auth(false, s.pageBackupTools))
}

// toolGetenv is the environment the tools are resolved from (the backup processes inherit it).
func toolGetenv(k string) string { return os.Getenv(k) }

// jobToolError returns a *runner.ToolMissingError when a scheduled job's native tools (backup, and test
// restore when enabled) are missing; nil for custom jobs.
func jobToolError(j backup.Job) error {
	if j.IsCustom() {
		return nil
	}
	return runner.CheckJobTools(toolGetenv, j)
}

// runToolError is jobToolError for one backup run (Run now): only the backup tool is needed.
func runToolError(j backup.Job) error {
	if j.IsCustom() {
		return nil
	}
	return runner.CheckTools(toolGetenv, runner.BackupToolNeeds(j.Method))
}

func isToolMissing(err error) bool {
	var tm *runner.ToolMissingError
	return errors.As(err, &tm)
}

// writeToolMissing answers 409 {"error", "code": "tool_missing", "tools", "hint"} when err is a missing-tool
// error and reports whether it did.
func writeToolMissing(w http.ResponseWriter, err error) bool {
	var tm *runner.ToolMissingError
	if !errors.As(err, &tm) {
		return false
	}
	writeJSON(w, http.StatusConflict, map[string]any{"error": tm.Error(), "code": backup.ErrCodeToolMissing,
		"tools": tm.Tools(), "hint": runner.ToolHint})
	return true
}

// refuseJobWithoutTools applies the save rule: an enabled job whose tools are missing is refused (409); a
// disabled one is accepted with the problem added to warnings.
func refuseJobWithoutTools(w http.ResponseWriter, j backup.Job, warnings *[]string) bool {
	err := jobToolError(j)
	if err == nil {
		return false
	}
	if j.Enabled {
		return writeToolMissing(w, err)
	}
	*warnings = append(*warnings, err.Error())
	return false
}

// apiBackupTools: every native tool with found/missing, resolved path, version and the methods using it.
// Admin only (rbac.go readRules): paths and versions describe the host.
func (s *Server) apiBackupTools(w http.ResponseWriter, r *http.Request, _ *Session) {
	writeJSON(w, http.StatusOK, map[string]any{"tools": runner.ToolsStatus(r.Context(), toolGetenv),
		"tools_dir": toolGetenv(runner.EnvToolsDir), "hint": runner.ToolHint})
}

type backupToolsPageBody struct {
	Tools    []runner.ToolStatus
	ToolsDir string
	Missing  int
}

func (s *Server) pageBackupTools(w http.ResponseWriter, r *http.Request, sess *Session) {
	body := backupToolsPageBody{Tools: runner.ToolsStatus(r.Context(), toolGetenv), ToolsDir: toolGetenv(runner.EnvToolsDir)}
	for _, t := range body.Tools {
		if !t.Found && !t.Optional {
			body.Missing++
		}
	}
	crumbs := append(opsCrumbs("", ""), BreadcrumbItem{Label: "Backup tools"})
	s.render(w, "backups_tools", sess, pageData{Title: "Backup tools",
		Subtitle: "Native client tools the built-in backup methods run on this Web host",
		Active:   "backups-tools", Breadcrumbs: crumbs, Body: body})
}

// wizTool is the status of one tool for the job wizard.
type wizTool struct {
	Found   bool   `json:"found"`
	Version string `json:"version,omitempty"`
}

// wizardTools returns the tool status and the tools each method needs (backup / test restore).
func wizardTools(r *http.Request) (map[string]wizTool, map[string][]string, map[string][]string) {
	tools := map[string]wizTool{}
	for _, t := range runner.ToolsStatus(r.Context(), toolGetenv) {
		tools[t.Name] = wizTool{Found: t.Found, Version: t.Version}
	}
	bk, tr := map[string][]string{}, map[string][]string{}
	for _, m := range []string{backup.MethodMySQLDump, backup.MethodMariaDBDump, backup.MethodMydumper, backup.MethodPGDump,
		backup.MethodPGBaseBackup, backup.MethodMongodump} {
		bk[m] = runner.BackupTools(m)
		if t := runner.TestRestoreTools(m); len(t) > 0 {
			tr[m] = t
		}
	}
	return tools, bk, tr
}
