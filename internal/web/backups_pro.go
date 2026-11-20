package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strand/internal/web/store"
	"strconv"
	"strings"
	"sync"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/runner"
)

// Managed backups in the Web: dashboard, retention preview/prune, verification, repositories, copies,
// restores, legal hold, notifications and Prometheus metrics. Long operations run as `strand backup ...`
// job processes (visible with their log on the Jobs page); the maintenance loop runs on the ACTIVE
// instance only.

func (s *Server) backupProRoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backups/summary", s.auth(true, s.apiBackupSummary))
	mux.Handle("GET /api/v1/backups/{id}/checks", s.auth(true, s.apiBackupChecks))
	mux.Handle("GET /api/v1/backups/{id}/manifest", s.auth(true, s.apiBackupManifest))
	mux.Handle("POST /api/v1/backups/{id}/legal-hold", s.auth(true, s.apiBackupLegalHold))
	mux.Handle("POST /api/v1/backups/{id}/verify", s.auth(true, s.apiBackupVerifyOne))
	mux.Handle("POST /api/v1/backups/{id}/copy", s.auth(true, s.apiBackupCopy))
	mux.Handle("POST /api/v1/backup-restores", s.auth(true, s.apiBackupRestore))
	mux.Handle("POST /api/v1/backup-jobs/{id}/retention-preview", s.auth(true, s.apiBackupRetentionPreview))
	mux.Handle("POST /api/v1/backup-jobs/{id}/prune", s.auth(true, s.apiBackupJobOp("prune")))
	mux.Handle("POST /api/v1/backup-jobs/{id}/verify", s.auth(true, s.apiBackupJobOp("verify")))
	s.backupOpsRoutes(mux)
	s.backupRepoRoutes(mux)
	s.backupNotifyRoutes(mux)
	s.backupKeyRoutes(mux)
	s.backupRPORoutes(mux)
	mux.Handle("GET /metrics", s.auth(true, s.backupMetrics))
}

func (s *Server) needDB(w http.ResponseWriter) bool {
	if s.db == nil || s.backups == nil {
		writeErr(w, http.StatusServiceUnavailable, "managed backups require the metadata database")
		return false
	}
	return true
}

// auditBackupOp records a mutating backup request before it takes effect (fail-closed).
func (s *Server) auditBackupOp(w http.ResponseWriter, r *http.Request, action string, detail map[string]any) bool {
	if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), User: auditUser(r), Action: action, Result: "started", Detail: maskAuditSecrets(detail)}); err != nil {
		s.log.Error("audit backup operation", "action", action, "error", err)
		writeErr(w, http.StatusInternalServerError, "audit log unavailable; refusing the backup operation")
		return false
	}
	return true
}

// startBackupOpEnv starts `strand backup <args>` as a job process with extra environment variables (active instance only).
func (s *Server) startBackupOpEnv(kind, pipeline string, extraEnv []string, args ...string) (string, error) {
	if s.jobs == nil {
		return "", errors.New("job runner unavailable")
	}
	if !s.isActive() {
		return "", errors.New("this web instance is standby; backup operations run on the active instance")
	}
	full := []string{}
	if s.cfg.SourcePath != "" {
		full = append(full, "--config", s.cfg.SourcePath)
	}
	full = append(append(full, "backup"), args...)
	env := append(s.backupOpEnv(args), s.repoSecretEnv()...)
	env = append(env, extraEnv...)
	return s.jobs.Start(context.Background(), JobSpec{Pipeline: pipeline, Kind: kind, Args: full, Env: env})
}

// startBackupOp starts `strand backup <args>` as a job process (active instance only).
func (s *Server) startBackupOp(kind, pipeline string, args ...string) (string, error) {
	return s.startBackupOpEnv(kind, pipeline, nil, args...)
}

// backupOpEnv: the log archiver reads the source database, so for a job on a stored-secret connection it
// receives the decrypted secret in its environment (other operations never touch the source).
func (s *Server) backupOpEnv(args []string) []string {
	if len(args) < 3 || args[0] != "log-archive" || s.db == nil {
		return nil
	}
	for i := 1; i+1 < len(args); i++ {
		if args[i] == "--job-id" {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if j, ok, err := s.db.st.GetBackupJob(ctx, args[i+1]); err == nil && ok {
				return s.jobSecretEnv(j)
			}
		}
	}
	return nil
}

// startOwnedBackupOp starts a long-running backup process owned by this instance (stopped on step-down).
func (s *Server) startOwnedBackupOp(kind, pipeline string, args ...string) (string, error) {
	id, err := s.startBackupOp(kind, pipeline, args...)
	if err == nil {
		if st, ok := s.jobs.Get(id); ok && st.PID > 0 {
			s.ownedBackupProcs.Store(id, st.PID)
		}
	}
	return id, err
}

type recoveryView struct {
	backup.SourceSummary
	AgeSeconds  int64      `json:"age_seconds,omitempty"`
	RPOSeconds  int64      `json:"rpo_seconds,omitempty"` // configured interval of the job
	Stale       bool       `json:"stale"`                 // last success older than 2 intervals
	WindowStart *time.Time `json:"window_start,omitempty"`
	WindowEnd   *time.Time `json:"window_end,omitempty"`
}

func (s *Server) backupDashboard(ctx context.Context) ([]recoveryView, error) {
	sums, err := s.db.st.BackupSummaries(ctx)
	if err != nil {
		return nil, err
	}
	jobs, _ := s.db.st.ListBackupJobs(ctx)
	every := map[string]int{}
	for _, j := range jobs {
		every[j.ID] = j.EverySeconds
	}
	now := s.now().UTC()
	out := make([]recoveryView, 0, len(sums))
	for _, sm := range sums {
		v := recoveryView{SourceSummary: sm}
		if sm.LastSuccess != nil {
			at := sm.LastSuccess.CreatedAt
			if sm.LastSuccess.VerifiedAt != nil {
				at = *sm.LastSuccess.VerifiedAt
			}
			v.AgeSeconds = int64(now.Sub(at).Seconds())
			v.RPOSeconds = int64(every[sm.JobID])
			v.Stale = v.RPOSeconds > 0 && v.AgeSeconds > 2*v.RPOSeconds
			start := sm.LastSuccess.CreatedAt
			v.WindowStart, v.WindowEnd = &start, &start
		}
		if win, err := s.recoveryWindow(ctx, sm.Source); err == nil && win.Start != nil {
			v.WindowStart, v.WindowEnd = win.Start, win.End
		}
		if sm.LogEnd != nil && v.RPOSeconds == 0 {
			v.AgeSeconds = int64(now.Sub(*sm.LogEnd).Seconds())
		}
		out = append(out, v)
	}
	return out, nil
}

// recoveryWindow is the point-in-time window of a source (base backups + continuous logs).
func (s *Server) recoveryWindow(ctx context.Context, source string) (runner.Window, error) {
	rs, err := s.backups.ListAll(ctx, backup.Filter{Source: source, Status: backup.StatusComplete}, 200000)
	if err != nil {
		return runner.Window{}, err
	}
	return runner.RecoveryWindow(rs), nil
}

func (s *Server) apiBackupSummary(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	v, err := s.backupDashboard(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		return
	}
	checks, _ := s.backups.Checks(r.Context(), "", 50)
	writeJSON(w, http.StatusOK, map[string]any{"sources": v, "checks": checks})
}

func (s *Server) apiBackupChecks(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	cs, err := s.backups.Checks(r.Context(), r.PathValue("id"), 100)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checks": cs})
}

func decodeStrict(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON request")
		return false
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "request must contain exactly one JSON object")
		return false
	}
	return true
}

func (s *Server) apiBackupLegalHold(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	var req struct {
		Hold bool `json:"hold"`
	}
	if !decodeStrict(w, r, &req) {
		return
	}
	id := r.PathValue("id")
	if !s.auditBackupOp(w, r, "backup.legal_hold", map[string]any{"backup_id": id, "hold": req.Hold}) {
		return
	}
	rec, found, err := s.backups.SetLegalHold(r.Context(), id, req.Hold)
	switch {
	case err != nil:
		writeErr(w, http.StatusBadRequest, err.Error())
	case !found:
		writeErr(w, http.StatusNotFound, "backup not found")
	default:
		writeJSON(w, http.StatusOK, rec)
	}
}

func (s *Server) apiBackupManifest(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	rec, found, err := s.backups.Get(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="manifest-%s.json"`, id))
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(rec)
}

func (s *Server) apiBackupVerifyOne(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	if _, found, err := s.backups.Get(r.Context(), id); err != nil || !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return
	}
	if !s.auditBackupOp(w, r, "backup.verify", map[string]any{"backup_id": id}) {
		return
	}
	pid, err := s.startBackupOp("backup-verify", "backup-verify-"+id, "verify", "--id", id)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"process_id": pid})
}

func (s *Server) apiBackupCopy(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	var req struct {
		Repository string `json:"repository"`
	}
	if !decodeStrict(w, r, &req) {
		return
	}
	id := r.PathValue("id")
	if !backup.ValidID(req.Repository) {
		writeErr(w, http.StatusBadRequest, "repository is required")
		return
	}
	if _, ok, err := s.db.st.GetBackupRepo(r.Context(), req.Repository); err != nil || !ok {
		writeErr(w, http.StatusBadRequest, "repository not found")
		return
	}
	if !s.auditBackupOp(w, r, "backup.copy", map[string]any{"backup_id": id, "repository": req.Repository}) {
		return
	}
	pid, err := s.startBackupOp("backup-copy", "backup-copy-"+id, "copy", "--id", id, "--to-repository", req.Repository)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"process_id": pid})
}

// RestoreRequest is the restore wizard's request. The target is always NEW: a database name that must
// not exist (logical), or an empty directory (physical); the source database is never written.
type RestoreRequest struct {
	BackupID string `json:"backup_id,omitempty"`
	Source   string `json:"source,omitempty"`
	// TargetTime (RFC3339): point-in-time restore; the base backup is chosen automatically.
	TargetTime   string          `json:"target_time,omitempty"`
	ConnectionID string          `json:"connection_id,omitempty"`
	Target       backup.ConnSpec `json:"target"`
	TargetDB     string          `json:"target_database,omitempty"`
	TargetDir    string          `json:"target_dir,omitempty"`
	Threads      int             `json:"threads,omitempty"`
	Objects      []string        `json:"objects,omitempty"`
	Confirm      string          `json:"confirm"` // must equal the target database or directory
}

func (s *Server) apiBackupRestore(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	var req RestoreRequest
	if !decodeStrict(w, r, &req) {
		return
	}

	var backupEngine string
	var src string
	var backupRecord *backup.Record
	if req.BackupID != "" {
		if s.backups != nil {
			b, ok, err := s.backups.Get(r.Context(), req.BackupID)
			if err != nil || !ok {
				writeErr(w, http.StatusBadRequest, "backup not found")
				return
			}
			backupRecord = &b
			backupEngine = b.Engine
			src = b.Source
		}
	} else if req.Source != "" {
		src = req.Source
		if s.backups != nil {
			recs, _ := s.backups.List(r.Context(), backup.Filter{Source: req.Source, Limit: 1})
			if len(recs) > 0 {
				backupRecord = &recs[0]
				backupEngine = recs[0].Engine
			}
		}
	}

	if backupRecord != nil {
		method := backupRecord.Method
		if method == "" {
			method = backupRecord.Tool
		}
		switch method {
		case backup.MethodMySQLDump, backup.MethodMariaDBDump:
			if req.Threads > 1 {
				writeErr(w, http.StatusBadRequest, "parallel restore is not available for mysqldump backups")
				return
			}
			if len(req.Objects) > 0 && req.TargetTime != "" {
				writeErr(w, http.StatusBadRequest, "partial restore with point-in-time recovery is not supported for MySQL")
				return
			}
		case backup.MethodPGBaseBackup:
			if req.Threads > 0 {
				writeErr(w, http.StatusBadRequest, "parallel restore is not available for physical backups")
				return
			}
			if len(req.Objects) > 0 {
				writeErr(w, http.StatusBadRequest, "partial restore is not available for physical backups")
				return
			}
		case backup.MethodMongodump:
			if len(req.Objects) > 0 && (backupRecord.Details == nil || backupRecord.Details["databases"] == "") {
				writeErr(w, http.StatusBadRequest, "partial restore is not available for whole-deployment MongoDB backups")
				return
			}
		case backup.MethodPGDump:
			// supported
		default:
			if req.Threads > 0 {
				writeErr(w, http.StatusBadRequest, "parallel restore is not available for "+method+" backups")
				return
			}
			if len(req.Objects) > 0 {
				writeErr(w, http.StatusBadRequest, "partial restore is not available for "+method+" backups")
				return
			}
		}
	}
	if req.TargetDir != "" {
		if req.Threads > 0 {
			writeErr(w, http.StatusBadRequest, "parallel restore is not available for physical backups")
			return
		}
		if len(req.Objects) > 0 {
			writeErr(w, http.StatusBadRequest, "partial restore is not available for physical backups")
			return
		}
	}

	var extraEnv []string
	var tunnelArgs []string
	if req.ConnectionID != "" && s.db != nil && s.db.st != nil {
		if req.Target.Host != "" || req.Target.Port != 0 || req.Target.User != "" || req.Target.PasswordEnv != "" || req.Target.URIEnv != "" {
			writeErr(w, http.StatusBadRequest, "target fields are taken from the saved connection")
			return
		}
		conn, ok, err := s.db.st.GetBackupConnection(r.Context(), req.ConnectionID)
		if err != nil || !ok {
			writeErr(w, http.StatusBadRequest, "connection not found")
			return
		}
		if conn.Engine != "" && backupEngine != "" && conn.Engine != backupEngine {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("target connection engine %s does not match backup engine %s", conn.Engine, backupEngine))
			return
		}
		req.Target.Host = conn.Host
		req.Target.Port = conn.Port
		req.Target.User = conn.User
		if conn.SSHTunnel != nil && (conn.URISet || conn.URIEnv != "") {
			writeErr(w, http.StatusBadRequest, "ssh_tunnel cannot be used with a connection URI; use host/port/user")
			return
		}
		if conn.SSHTunnel != nil {
			sshAddr := conn.SSHTunnel.Host
			if conn.SSHTunnel.Port > 0 {
				sshAddr = net.JoinHostPort(conn.SSHTunnel.Host, strconv.Itoa(conn.SSHTunnel.Port))
			} else {
				sshAddr = net.JoinHostPort(conn.SSHTunnel.Host, "22")
			}
			tunnelArgs = append(tunnelArgs,
				"--tunnel-ssh", sshAddr,
				"--tunnel-user", conn.SSHTunnel.User,
				"--tunnel-known-hosts", conn.SSHTunnel.KnownHostsRef,
			)
			if conn.SSHTunnel.IdentityRef != "" {
				tunnelArgs = append(tunnelArgs, "--tunnel-identity-file", conn.SSHTunnel.IdentityRef)
			}
		}
		if conn.SecretMode == backup.SecretStored {
			pw, uri, err := s.connSecrets(conn)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			if pw != "" {
				envName := "STRAND_RESTORE_TARGET_PASSWORD"
				req.Target.PasswordEnv = envName
				extraEnv = append(extraEnv, envName+"="+pw, "MYSQL_PWD="+pw, "PGPASSWORD="+pw)
			}
			if uri != "" {
				envName := "STRAND_RESTORE_TARGET_URI"
				req.Target.URIEnv = envName
				extraEnv = append(extraEnv, envName+"="+uri)
			}
		} else if conn.SecretMode == backup.SecretEnv {
			req.Target.PasswordEnv = conn.PasswordEnv
			req.Target.URIEnv = conn.URIEnv
		}
	}

	if req.TargetTime != "" {
		tt, err := time.Parse(time.RFC3339, req.TargetTime)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "target_time must be RFC3339")
			return
		}
		if src == "" {
			writeErr(w, http.StatusBadRequest, "source could not be resolved for PITR restore")
			return
		}
		win, err := s.recoveryWindow(r.Context(), src)
		if err != nil || !win.Continuous || win.Start == nil || win.End == nil {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("source %s has no continuous PITR window", src))
			return
		}
		if tt.Before(*win.Start) || tt.After(*win.End) {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("target time %s is outside PITR recovery window [%s, %s]",
				tt.Format(time.RFC3339), win.Start.Format(time.RFC3339), win.End.Format(time.RFC3339)))
			return
		}
	}

	args, err := restoreArgs(req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	args = append(args, tunnelArgs...)
	auditDetail := map[string]any{
		"backup_id":       req.BackupID,
		"source":          req.Source,
		"target_time":     req.TargetTime,
		"target_host":     req.Target.Host,
		"target_database": req.TargetDB,
		"target_dir":      req.TargetDir,
		"threads":         req.Threads,
		"object_count":    len(req.Objects),
	}
	if len(req.Objects) > 0 && len(req.Objects) <= 20 {
		auditDetail["objects"] = req.Objects
	}
	if !s.auditBackupOp(w, r, "backup.restore", auditDetail) {
		return
	}
	pid, err := s.startBackupOpEnv("backup-restore", "backup-restore-"+runner.NewID("r"), extraEnv, args...)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"process_id": pid})
}

func restoreArgs(req RestoreRequest) ([]string, error) {
	if (req.BackupID == "") == (req.Source == "") {
		return nil, errors.New("choose a backup or a source")
	}
	if req.Source != "" && req.TargetTime == "" {
		return nil, errors.New("restoring a source needs a target time")
	}
	if req.TargetTime != "" {
		if _, err := time.Parse(time.RFC3339, req.TargetTime); err != nil {
			return nil, errors.New("target_time must be RFC3339")
		}
	}
	if (req.TargetDB == "") == (req.TargetDir == "") {
		return nil, errors.New("give exactly one of target_database (logical restore) and target_dir (physical restore)")
	}
	if want := req.TargetDB + req.TargetDir; req.Confirm != want {
		return nil, errors.New("type the target database or directory to confirm the restore")
	}
	if err := runner.ValidateRestoreThreads(req.Threads); err != nil {
		return nil, err
	}
	if err := runner.ValidateRestoreObjects(req.Objects); err != nil {
		return nil, err
	}
	if req.TargetDir != "" {
		if req.Threads > 0 {
			return nil, errors.New("parallel restore is not available for physical backups")
		}
		if len(req.Objects) > 0 {
			return nil, errors.New("partial restore is not available for physical backups")
		}
	}
	args := []string{"restore", "--yes"}
	if req.BackupID != "" {
		if !backup.ValidID(req.BackupID) {
			return nil, errors.New("invalid backup id")
		}
		args = append(args, "--id", req.BackupID)
	} else {
		args = append(args, "--source", req.Source)
	}
	if req.TargetTime != "" {
		args = append(args, "--to-time", req.TargetTime)
	}
	if req.Threads > 0 {
		args = append(args, "--threads", strconv.Itoa(req.Threads))
	}
	if len(req.Objects) > 0 {
		args = append(args, "--objects", strings.Join(req.Objects, ","))
	}
	if req.TargetDir != "" {
		args = append(args, "--target-dir", req.TargetDir)
	} else {
		probe := backup.Job{ID: "probe", Name: "probe", EverySeconds: 3600, Bucket: "probe-bucket",
			JobOptions: backup.JobOptions{Method: backup.MethodPGDump, Source: "probe", Conn: req.Target}}
		probe.Conn.Databases = []string{req.TargetDB}
		if err := backup.ValidateJob(probe); err != nil && !strings.Contains(err.Error(), "host") {
			return nil, fmt.Errorf("target: %v", err)
		} else if err != nil {
			return nil, errors.New("target host is required")
		}
		args = append(args, "--target-host", req.Target.Host, "--target-database", req.TargetDB)
		if req.Target.Port > 0 {
			args = append(args, "--target-port", strconv.Itoa(req.Target.Port))
		}
		if req.Target.User != "" {
			args = append(args, "--target-user", req.Target.User)
		}
		if req.Target.PasswordEnv != "" {
			args = append(args, "--target-password-env", req.Target.PasswordEnv)
		}
		if req.Target.URIEnv != "" {
			args = append(args, "--target-uri-env", req.Target.URIEnv)
		}
	}
	return args, nil
}

func (s *Server) apiBackupRetentionPreview(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	job, found, err := s.db.st.GetBackupJob(r.Context(), r.PathValue("id"))
	if err != nil || !found {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	// An optional policy in the body previews an edited (unsaved) policy.
	if r.ContentLength > 0 {
		var p backup.RetentionPolicy
		if !decodeStrict(w, r, &p) {
			return
		}
		if err := p.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		job.Retention = p
	}
	rs, err := s.backups.ListAll(r.Context(), backup.Filter{JobID: job.ID, Status: backup.StatusComplete}, 0)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		return
	}
	ds := backup.PlanRetention(rs, job.Retention, s.now())
	expire := 0
	for _, d := range ds {
		if !d.Keep {
			expire++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"policy": job.Retention, "decisions": ds, "expire": expire, "keep": len(ds) - expire})
}

func (s *Server) apiBackupJobOp(op string) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, _ *Session) {
		if !s.needDB(w) {
			return
		}
		id := r.PathValue("id")
		if _, found, err := s.db.st.GetBackupJob(r.Context(), id); err != nil || !found {
			writeErr(w, http.StatusNotFound, "backup job not found")
			return
		}
		if !s.auditBackupOp(w, r, "backup_job."+op, map[string]any{"job": id}) {
			return
		}
		pid, err := s.startBackupOp("backup-"+op, "backup-"+op+"-"+id, op, "--job-id", id)
		if err != nil {
			writeErr(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"process_id": pid})
	}
}

// --- maintenance (active instance) ---

var backupMaint struct {
	once     sync.Once
	interval time.Duration
}

func init() { backupMaint.interval = 30 * time.Second }

// startBackupMaintenance starts the loop once per process; each tick does nothing on a standby.
func (s *Server) startBackupMaintenance() {
	if s.db == nil {
		return
	}
	backupMaint.once.Do(func() {
		go func() {
			t := time.NewTicker(backupMaint.interval)
			defer t.Stop()
			for range t.C {
				if s.isActive() {
					ctx, cancel := context.WithTimeout(context.Background(), backupMaint.interval)
					s.backupMaintenanceTick(ctx)
					cancel()
				}
			}
		}()
	})
}

// backupMaintenanceTick: hourly retention per job, scheduled verification, notifications of finished
// and missed runs. Claims in the database make each action happen once across instances.
func (s *Server) backupMaintenanceTick(ctx context.Context) {
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return
	}
	now := s.now().UTC()
	notifyOn := s.notifyConfigured(ctx)
	for _, j := range jobs {
		if j.Enabled && j.LogArchive && !j.IsCustom() && s.jobs != nil {
			if err := backup.ValidatePITR(j); err != nil {
				s.log.Warn("skip log archiving: job validation failed", "job", j.ID, "reason", err)
				continue
			}
			running := false
			for _, p := range s.jobs.List("backup-logs-" + j.ID) {
				if p.Kind == "backup-logs" && p.Running {
					running = true
				}
			}
			if !running {
				if _, err := s.startOwnedBackupOp("backup-logs", "backup-logs-"+j.ID, "log-archive", "--job-id", j.ID); err != nil {
					s.log.Warn("start log archiving", "job", j.ID, "error", err)
				}
			}
		}
		if j.Retention.Enabled() {
			if ok, _ := s.db.st.ClaimBackupJobPrune(ctx, j.ID, now, time.Hour); ok {
				if _, err := s.startBackupOp("backup-prune", "backup-prune-"+j.ID, "prune", "--job-id", j.ID); err != nil {
					s.log.Warn("scheduled retention", "job", j.ID, "error", err)
				}
			}
		}
		if j.VerifyEverySeconds > 0 {
			if ok, _ := s.db.st.ClaimBackupJobVerify(ctx, j.ID, now, time.Duration(j.VerifyEverySeconds)*time.Second); ok {
				args := []string{"verify", "--job-id", j.ID}
				if j.TestRestore {
					args = append(args, "--test-restore")
				}
				if _, err := s.startBackupOp("backup-verify", "backup-verify-"+j.ID, args...); err != nil {
					s.log.Warn("scheduled verification", "job", j.ID, "error", err)
				}
			}
		}
		if !notifyOn {
			continue
		}
		if j.RunFinishedAt != nil && j.LastRunStatus != "" && j.LastRunStatus != backup.RunStatusRunning {
			if ok, _ := s.db.st.MarkBackupJobNotified(ctx, j.ID, *j.RunFinishedAt); ok {
				kind := "failure"
				switch j.LastRunStatus {
				case backup.RunStatusSucceeded:
					kind = "success"
				case backup.RunStatusMissed:
					kind = "missed" // not started within window_minutes (recorded by the scheduler)
				}
				if kind == "failure" {
					// A run that could not reach its repository is its own event (rules can route it separately).
					if runs, err := s.db.st.ListBackupRuns(ctx, store.BackupRunFilter{JobID: j.ID, Limit: 1}); err == nil && len(runs) == 1 && runs[0].ErrorCode == backup.ErrCodeRepoUnreachable {
						kind = backup.EventRepoUnreachable
					}
				}
				msg := fmt.Sprintf("backup job %q (%s) run %s at %s", j.Name, j.Source, j.LastRunStatus, j.RunFinishedAt.UTC().Format(time.RFC3339))
				if j.ConsecutiveFailures > 0 {
					msg += fmt.Sprintf("; %d consecutive failure(s)", j.ConsecutiveFailures)
				}
				go s.notifyBackupEvent(runner.Event{Kind: kind, Job: j.ID, JobName: j.Name, Source: j.Source, Message: msg, At: now}, j.PolicyID)
			}
		}
		if j.Enabled && j.NextRunAt != nil && j.LastRunStatus != backup.RunStatusRunning {
			grace := time.Duration(j.EverySeconds) * time.Second / 10
			if grace < 5*time.Minute {
				grace = 5 * time.Minute
			}
			if now.Sub(*j.NextRunAt) > grace {
				if ok, _ := s.db.st.MarkBackupJobMissed(ctx, j.ID, *j.NextRunAt); ok {
					go s.notifyBackupEvent(runner.Event{Kind: "missed", Job: j.ID, JobName: j.Name, Source: j.Source, At: now,
						Message: fmt.Sprintf("backup job %q missed its schedule (due %s)", j.Name, j.NextRunAt.UTC().Format(time.RFC3339))}, j.PolicyID)
				}
			}
		}
	}
	s.rpoTick(ctx, now)
	if notifyOn {
		checks, _ := s.backups.Checks(ctx, "", 20)
		for _, c := range checks {
			if c.Status == backup.CheckFailed && c.FinishedAt.After(now.Add(-backupMaint.interval*2)) {
				key := fmt.Sprintf("backup_check_notified:%d", c.ID)
				if _, seen, _ := s.db.st.MetaGet(ctx, key); !seen {
					_ = s.db.st.MetaSet(ctx, key, now.Format(time.RFC3339))
					ev := runner.Event{Kind: "verify_failed", Message: fmt.Sprintf("backup %s failed %s: %s", c.BackupID, c.Kind, c.Detail), At: now}
					policyID := ""
					if rec, found, _ := s.backups.Get(ctx, c.BackupID); found {
						ev.Job, ev.Source = rec.JobID, rec.Source
						policyID = s.jobPolicy(ctx, rec.JobID)
					}
					go s.notifyBackupEvent(ev, policyID)
				}
			}
		}
	}
}

// --- Prometheus metrics ---

func promLabel(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

func (s *Server) backupMetrics(w http.ResponseWriter, r *http.Request, _ *Session) {
	var b strings.Builder
	gauge := func(name, help string) { fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name) }
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if s.db == nil {
		_, _ = io.WriteString(w, "")
		return
	}
	ctx := r.Context()
	views, err := s.backupDashboard(ctx)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		return
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Source < views[j].Source })
	gauge("strand_backup_last_success_timestamp_seconds", "Completion time of the newest complete restore point per source.")
	for _, v := range views {
		if v.LastSuccess != nil && v.LastSuccess.VerifiedAt != nil {
			fmt.Fprintf(&b, "strand_backup_last_success_timestamp_seconds{source=\"%s\",engine=\"%s\"} %d\n", promLabel(v.Source), v.Engine, v.LastSuccess.VerifiedAt.Unix())
		}
	}
	gauge("strand_backup_last_size_bytes", "Stored size of the newest complete restore point per source.")
	for _, v := range views {
		if v.LastSuccess != nil {
			fmt.Fprintf(&b, "strand_backup_last_size_bytes{source=\"%s\"} %d\n", promLabel(v.Source), v.LastSuccess.Bytes)
		}
	}
	gauge("strand_backup_last_duration_seconds", "Duration of the newest complete restore point per source.")
	for _, v := range views {
		if v.LastSuccess != nil && v.LastSuccess.VerifiedAt != nil {
			fmt.Fprintf(&b, "strand_backup_last_duration_seconds{source=\"%s\"} %.3f\n", promLabel(v.Source), v.LastSuccess.VerifiedAt.Sub(v.LastSuccess.CreatedAt).Seconds())
		}
	}
	gauge("strand_backup_stored_bytes", "Bytes of complete backups and log segments per source.")
	for _, v := range views {
		fmt.Fprintf(&b, "strand_backup_stored_bytes{source=\"%s\"} %d\n", promLabel(v.Source), v.TotalBytes)
	}
	gauge("strand_backup_recovery_window_end_timestamp_seconds", "Latest recoverable point per source (base backup or archived log).")
	for _, v := range views {
		if v.WindowEnd != nil {
			fmt.Fprintf(&b, "strand_backup_recovery_window_end_timestamp_seconds{source=\"%s\"} %d\n", promLabel(v.Source), v.WindowEnd.Unix())
		}
	}
	jobs, _ := s.db.st.ListBackupJobs(ctx)
	gauge("strand_backup_job_consecutive_failures", "Consecutive failed runs of a backup job.")
	for _, j := range jobs {
		fmt.Fprintf(&b, "strand_backup_job_consecutive_failures{job=\"%s\",name=\"%s\"} %d\n", j.ID, promLabel(j.Name), j.ConsecutiveFailures)
	}
	gauge("strand_backup_job_last_run_success", "1 when the last run of the job succeeded, 0 otherwise.")
	for _, j := range jobs {
		ok := 0
		if j.LastRunStatus == backup.RunStatusSucceeded {
			ok = 1
		}
		fmt.Fprintf(&b, "strand_backup_job_last_run_success{job=\"%s\",name=\"%s\"} %d\n", j.ID, promLabel(j.Name), ok)
	}
	checks, _ := s.backups.Checks(ctx, "", 200)
	failed := 0
	for _, c := range checks {
		if c.Status == backup.CheckFailed {
			failed++
		}
	}
	gauge("strand_backup_recent_check_failures", "Failed verification/test-restore results among the latest 200 checks.")
	fmt.Fprintf(&b, "strand_backup_recent_check_failures %d\n", failed)
	s.writeRPOMetrics(ctx, &b, gauge)
	_, _ = io.WriteString(w, b.String())
}
