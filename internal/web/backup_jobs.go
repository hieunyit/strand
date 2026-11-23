package web

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/runner"
)

type backupJobView struct {
	Job        backup.Job   `json:"job"`
	Process    *JobStatus   `json:"process,omitempty"`
	Warning    string       `json:"warning,omitempty"`
	Last10Runs []backup.Run `json:"last_10_runs,omitempty"`
	// Display fields filled by enrichJobViews (backups_ui_p23.go).
	Warnings       []string `json:"warnings,omitempty"`
	ScheduleText   string   `json:"schedule_text,omitempty"`
	PolicyName     string   `json:"policy_name,omitempty"`
	ConnectionName string   `json:"connection_name,omitempty"`
	Legacy         bool     `json:"legacy_connection,omitempty"` // built-in job with an inline connection (convertible)
	// ToolError / ToolsMissing: the job's native tools are missing on this host, so it cannot be scheduled
	// or run (backup_tools.go).
	ToolError    string   `json:"tool_error,omitempty"`
	ToolsMissing []string `json:"tools_missing,omitempty"`
}

func backupJobPipeline(id string) string    { return "backup-" + id }
func backupJobRunPipeline(id string) string { return "run-" + id }

// reconcileDelay bounds the backoff between attempts to restart an enabled job's scheduler process.
var reconcileDelay = struct{ first, max time.Duration }{time.Second, 5 * time.Minute}

// reconcileBackupSchedule restarts the scheduler process of an enabled job after it exited unexpectedly or
// could not be started. It keeps retrying (capped exponential backoff) until the scheduler is running or
// the job is disabled/deleted: giving up after a short metadata-database outage would silently stop the
// job's backups until the next Web restart.
func (s *Server) reconcileBackupSchedule(pipeline string) {
	if s.closed.Load() || !s.isActive() {
		return
	}
	s.reconcileMu.Lock()
	if s.reconciling == nil {
		s.reconciling = make(map[string]*reconcileState)
	}
	if state, running := s.reconciling[pipeline]; running {
		state.again = true
		s.reconcileMu.Unlock()
		return
	}
	state := &reconcileState{}
	s.reconciling[pipeline] = state
	s.reconcileMu.Unlock()

	defer func() {
		s.reconcileMu.Lock()
		rerun := false
		if s.reconciling[pipeline] == state {
			delete(s.reconciling, pipeline)
			rerun = state.again && !s.closed.Load() && s.isActive()
		}
		s.reconcileMu.Unlock()
		if rerun {
			go s.reconcileBackupSchedule(pipeline)
		}
	}()
	id := strings.TrimPrefix(pipeline, "backup-")
	delay := reconcileDelay.first
	for attempt := 1; ; attempt++ {
		if s.closed.Load() || !s.isActive() {
			return // a standby runs no schedulers; the active instance (or a takeover) does
		}
		baseCtx := context.Background()
		if s.closeCtx != nil {
			baseCtx = s.closeCtx
		}
		ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
		job, found, err := s.db.st.GetBackupJob(ctx, id)
		cancel()
		if err != nil {
			s.log.Warn("recheck scheduled backup job", "job", id, "attempt", attempt, "error", err)
		} else if !found || !job.Enabled {
			return
		} else {
			baseJob := job
			baseJob.LogArchive = false
			if verr := s.validateBackupJob(baseJob); verr != nil {
				// Permanent (configuration) problem: retrying cannot help; the job page shows it on Start.
				s.log.Error("scheduled backup job refused", "job", id, "error", verr)
				return
			} else if terr := jobToolError(job); terr != nil {
				// Missing native tool: as permanent as a configuration problem until an administrator installs
				// it; the job shows "tool missing" and Resume re-checks.
				s.log.Error("scheduled backup job blocked: native tool missing", "job", id, "error", terr)
				return
			} else if _, err := s.startBackupJob(job); err == nil {
				s.reconcileMu.Lock()
				if state.again {
					state.again = false
					s.reconcileMu.Unlock()
					delay = reconcileDelay.first
					continue
				}
				s.reconcileMu.Unlock()
				return
			} else {
				s.log.Warn("restart scheduled backup job", "job", id, "attempt", attempt, "error", err)
			}
		}
		var closeDone <-chan struct{}
		if s.closeCtx != nil {
			closeDone = s.closeCtx.Done()
		}
		select {
		case <-closeDone:
			return
		case <-time.After(delay):
		}
		if delay *= 2; delay > reconcileDelay.max {
			delay = reconcileDelay.max
		}
	}
}

func randomBackupJobID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "bjob-" + hex.EncodeToString(b[:]), nil
}

// resumeBackupJobs starts the scheduler process of every enabled job. Only the ACTIVE instance (the lease
// holder) runs schedulers: it is called at startup when active and on takeover. The per-job database locks
// in `strand backup schedule` remain the safety net if two instances ever overlap.
func (s *Server) resumeBackupJobs(ctx context.Context) error {
	if s.db == nil || !s.isActive() {
		return nil
	}
	s.startBackupMaintenance()
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Enabled {
			baseJob := j
			baseJob.LogArchive = false
			if err := s.validateBackupJob(baseJob); err != nil {
				s.log.Error("scheduled backup job refused", "job", j.ID, "error", err)
				continue
			}
			if err := s.validateBackupJob(j); err != nil {
				s.log.Warn("scheduled backup job validation warning", "job", j.ID, "error", err)
			}
			if _, err := s.startBackupJob(j); err != nil {
				if isToolMissing(err) {
					s.log.Error("scheduled backup job blocked: native tool missing", "job", j.ID, "error", err)
					continue
				}
				s.log.Error("start scheduled backup job", "job", j.ID, "error", err)
				go s.reconcileBackupSchedule(backupJobPipeline(j.ID))
			}
		}
	}
	return nil
}

func (s *Server) backupJobProcess(j backup.Job) *JobStatus {
	if s.jobs == nil {
		return nil
	}
	processes := append(s.jobs.List(backupJobPipeline(j.ID)), s.jobs.List(backupJobRunPipeline(j.ID))...)
	sort.Slice(processes, func(i, k int) bool {
		if processes[i].Running != processes[k].Running {
			return processes[i].Running
		}
		return processes[i].Started.After(processes[k].Started)
	})
	for _, p := range processes {
		if p.Kind == "backup-schedule" || p.Kind == "backup-run" {
			cp := p
			return &cp
		}
	}
	return nil
}

func (s *Server) backupJobViews(ctx context.Context) ([]backupJobView, error) {
	if s.db == nil {
		return nil, errors.New("backup jobs require the metadata database")
	}
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]backupJobView, 0, len(jobs))
	for _, j := range jobs {
		var warn string
		if err := backup.ValidatePITR(j); err != nil {
			warn = err.Error()
		}
		out = append(out, backupJobView{Job: j, Process: s.backupJobProcess(j), Warning: warn})
	}
	s.enrichJobViews(ctx, out)
	return out, nil
}

// EnvBackupCommandDir names the only directory whose executables backup jobs may run. Backup jobs run as
// the Web service account, so without this boundary anyone holding the Web admin password or API token
// could run any program on the host (/bin/sh, curl, ...). Unset: managed backup jobs are disabled.
const EnvBackupCommandDir = "STRAND_WEB_BACKUP_COMMAND_DIR"

func backupCommandAllowed(command string) error {
	dir := strings.TrimSpace(os.Getenv(EnvBackupCommandDir))
	if dir == "" {
		return errors.New("managed backup jobs are disabled: set " + EnvBackupCommandDir + " to a root-owned directory of backup wrapper scripts")
	}
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return fmt.Errorf("%s is not accessible: %w", EnvBackupCommandDir, err)
	}
	if filepath.Dir(filepath.Clean(command)) != filepath.Clean(dir) && filepath.Dir(filepath.Clean(command)) != realDir {
		return fmt.Errorf("backup command must be an executable directly inside %s (%s)", EnvBackupCommandDir, dir)
	}
	for _, p := range []string{realDir, command} {
		fi, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("backup command is not accessible to the Web service: %w", err)
		}
		if fi.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("%s must not be group- or world-writable", p)
		}
	}
	return nil
}

func backupJobEncryptionError(j backup.Job) error {
	if jobEncryption(j).Blocked {
		return errors.New("this backup job requires Strand encryption; configure a valid active encryption key before enabling or running it")
	}
	return nil
}

func (s *Server) validateBackupJob(j backup.Job) error {
	if err := backup.ValidateJobForSave(j); err != nil {
		return err
	}
	if !j.IsCustom() {
		return nil // built-in methods: Strand builds the native tool's argv itself
	}
	if err := backupCommandAllowed(j.Command); err != nil {
		return err
	}
	fi, err := os.Lstat(j.Command)
	if err != nil {
		return fmt.Errorf("backup command is not accessible to the Web service: %w", err)
	}
	if !fi.Mode().IsRegular() || fi.Mode()&os.ModeSymlink != 0 || fi.Mode().Perm()&0111 == 0 {
		return errors.New("backup command must be a regular executable file, not a symlink")
	}
	return nil
}

func (s *Server) startBackupJob(j backup.Job) (string, error) {
	return s.startBackupProcess(j, false)
}

func (s *Server) startBackupProcess(j backup.Job, once bool) (string, error) {
	return s.startBackupProcessWithTrigger(j, once, "")
}

func (s *Server) startBackupProcessWithTrigger(j backup.Job, once bool, trigger string) (string, error) {
	if s.jobs == nil {
		return "", errors.New("job runner unavailable")
	}
	if !s.isActive() {
		return "", errors.New("this web instance is standby; backup jobs run on the active instance")
	}
	// Every scheduler start and Run now passes here: never start a process whose native tool is missing.
	toolErr := runToolError
	if !once {
		toolErr = jobToolError
	}
	if err := toolErr(j); err != nil {
		return "", err
	}
	if !once {
		for _, current := range s.jobs.List(backupJobPipeline(j.ID)) {
			if current.Kind == "backup-schedule" && current.Running {
				return current.ID, nil
			}
		}
	}
	args := make([]string, 0, 24)
	if s.cfg.SourcePath != "" {
		args = append(args, "--config", s.cfg.SourcePath)
	}
	if !j.IsCustom() {
		args = append(args, "backup", "schedule", "--job-id", j.ID)
		if once {
			args = append(args, "--once")
			if trigger != "" {
				args = append(args, "--trigger", trigger)
			} else {
				args = append(args, "--trigger", "manual")
			}
		}
		return s.startBackupJobProcess(j, once, args)
	}
	args = append(args, "backup", "schedule", "--every", (time.Duration(j.EverySeconds) * time.Second).String(),
		"--artifact", j.ArtifactTemplate, "--manifest", j.ManifestTemplate, "--bucket", j.Bucket, "--job-id", j.ID)
	if j.Compress {
		args = append(args, "--compress")
	}
	if j.Encrypt {
		args = append(args, "--encrypt")
	}
	if j.CleanupLocal {
		args = append(args, "--cleanup-local")
	}
	if once {
		args = append(args, "--once")
		if trigger != "" {
			args = append(args, "--trigger", trigger)
		} else {
			args = append(args, "--trigger", "manual")
		}
	}
	if j.Prefix != "" {
		args = append(args, "--prefix", j.Prefix)
	}
	if j.TunnelSSH != "" {
		args = append(args, "--tunnel-ssh", j.TunnelSSH, "--tunnel-user", j.TunnelUser, "--tunnel-remote", j.TunnelRemote)
		if j.TunnelIdentity != "" {
			args = append(args, "--tunnel-identity-file", j.TunnelIdentity)
		}
		if j.TunnelKnownHosts != "" {
			args = append(args, "--tunnel-known-hosts", j.TunnelKnownHosts)
		}
	}
	args = append(args, "--", j.Command)
	return s.startBackupJobProcess(j, once, args)
}

func (s *Server) startBackupJobProcess(j backup.Job, once bool, args []string) (string, error) {
	kind := "backup-schedule"
	if once {
		kind = "backup-run"
	}
	pipeline := backupJobPipeline(j.ID)
	if once {
		pipeline = backupJobRunPipeline(j.ID)
	}
	env := s.jobSecretEnv(j)
	if !j.IsCustom() {
		// Stored repository credentials travel in the child's environment only (never argv).
		env = append(env, s.repoSecretEnv()...)
	}
	id, err := s.jobs.Start(context.Background(), JobSpec{Pipeline: pipeline, Kind: kind, Args: args, Env: env})
	if err != nil {
		return "", err
	}
	s.trackBackupProcess(id)
	return id, nil
}

func (s *Server) trackBackupProcess(id string) {
	if st, ok := s.jobs.Get(id); ok && st.Running && st.PID > 0 {
		s.ownedBackupProcs.Store(id, st.PID)
		// The exit callback can run before registration.
		if current, ok := s.jobs.Get(id); !ok || !current.Running {
			s.ownedBackupProcs.Delete(id)
		}
	}
	// The lease may have been lost while starting the process.
	if !s.isActive() {
		s.stopBackupSchedulers()
	}
}

// stopBackupSchedulers uses this instance's launch records, since the shared
// runner history may already have been reloaded from the new active instance.
func (s *Server) stopBackupSchedulers() {
	s.ownedBackupProcs.Range(func(key, _ any) bool {
		id := key.(string)
		value, ok := s.ownedBackupProcs.LoadAndDelete(id)
		if !ok {
			return true
		}
		pid := value.(int)
		var err error
		if runner, ok := s.jobs.(*FileJobRunner); ok {
			err = runner.terminate(pid, id)
		} else if s.jobs != nil {
			err = s.jobs.Cancel(id)
		}
		if err != nil && !errors.Is(err, errJobNotRunning) && !errors.Is(err, syscall.ESRCH) {
			s.log.Warn("step-down: could not stop backup process", "job", id, "error", err)
		}
		return true
	})
}

func (s *Server) stopBackupJob(id string) error {
	if s.jobs == nil {
		return nil
	}
	// Run now is launched on its own pipeline so the UI can show it separately
	// from the scheduler. Stop, update, and delete must drain both processes;
	// otherwise a one-off invocation could keep writing with stale settings after
	// the job was changed or removed.
	processes := append(s.jobs.List(backupJobPipeline(id)), s.jobs.List(backupJobRunPipeline(id))...)
	for _, j := range processes {
		if !j.Running {
			continue
		}
		if err := s.jobs.Cancel(j.ID); err != nil {
			if errors.Is(err, errJobNotRunning) || errors.Is(err, errUnknownJob) {
				continue
			}
			return err
		}
		deadline := time.NewTimer(15 * time.Second)
		tick := time.NewTicker(100 * time.Millisecond)
		for {
			current, ok := s.jobs.Get(j.ID)
			if !ok || !current.Running {
				tick.Stop()
				deadline.Stop()
				break
			}
			select {
			case <-deadline.C:
				tick.Stop()
				return errors.New("backup job did not stop within 15 seconds")
			case <-tick.C:
			}
		}
	}
	return nil
}

// auditBackupJob records a mutating backup-job request before it takes effect; like pipeline actions, the
// request is refused when the audit log cannot be written.
func (s *Server) auditBackupJob(w http.ResponseWriter, r *http.Request, action, id string) bool {
	if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), User: auditUser(r), Action: action, Pipeline: id, Result: "started"}); err != nil {
		s.log.Error("audit backup job", "action", action, "error", err)
		writeErr(w, http.StatusInternalServerError, "audit log unavailable; refusing to change backup jobs")
		return false
	}
	return true
}

// auditBackupJobDetail is auditBackupJob with the job's settings (secrets never appear: a job holds only
// environment variable NAMES).
func (s *Server) auditBackupJobDetail(w http.ResponseWriter, r *http.Request, action, id string, detail map[string]any) bool {
	if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), User: auditUser(r), Action: action, Pipeline: id, Result: "started", Detail: maskAuditSecrets(detail)}); err != nil {
		s.log.Error("audit backup job", "action", action, "error", err)
		writeErr(w, http.StatusInternalServerError, "audit log unavailable; refusing to change backup jobs")
		return false
	}
	return true
}

func jobAuditDetail(j backup.Job) map[string]any {
	d := map[string]any{"name": j.Name, "method": j.Method, "enabled": j.Enabled, "schedule": j.EffectiveSchedule().Human()}
	if j.ConnectionID != "" {
		d["connection_id"] = j.ConnectionID
	}
	if j.PolicyID != "" {
		d["policy_id"] = j.PolicyID
		d["overrides"] = j.Overrides
	}
	if j.WindowMinutes > 0 {
		d["window_minutes"] = j.WindowMinutes
	}
	if j.JitterSeconds > 0 {
		d["jitter_seconds"] = j.JitterSeconds
	}
	return d
}

func (s *Server) apiBackupJobs(w http.ResponseWriter, r *http.Request, _ *Session) {
	views, err := s.backupJobViews(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": views})
}

type backupJobProtection struct {
	Mode      string `json:"mode"`
	Effective bool   `json:"effective"`
	KeyID     string `json:"key_id,omitempty"`
	Blocked   bool   `json:"blocked,omitempty"`
}

func jobEncryption(j backup.Job) backupJobProtection {
	protection := backupJobProtection{Mode: "off", Effective: j.Encrypt && !j.IsCustom()}
	if protection.Effective {
		protection.Mode = "on"
		if ring, err := backup.LoadKeyring(os.Getenv); err == nil && ring != nil && ring.Active != nil {
			protection.KeyID = ring.Active.ID
		} else {
			protection.Blocked = true
		}
	}
	if !j.IsCustom() && j.PolicyID != "" && !slices.Contains(j.Overrides, "encrypt") {
		protection.Mode = "inherit"
	}
	return protection
}

func (s *Server) apiBackupJobGet(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	j, found, err := s.db.st.GetBackupJob(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup job")
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	out := map[string]any{"job": j, "process": s.backupJobProcess(j)}
	out["encryption"] = jobEncryption(j)
	var tm *runner.ToolMissingError
	if err := jobToolError(j); errors.As(err, &tm) {
		out["tool_error"], out["tools_missing"] = tm.Error(), tm.Tools()
	}
	writeJSON(w, http.StatusOK, out)
}

// decodeBackupJob decodes a job body strictly and also reports which top-level fields the request carried
// (a policy-managed field the client omitted keeps following the policy).
func (s *Server) decodeBackupJob(w http.ResponseWriter, r *http.Request) (backup.Job, map[string]bool, string, bool) {
	var bodyJob struct {
		backup.Job
		EncryptionMode string `json:"encryption_mode"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid backup job JSON")
		return bodyJob.Job, nil, "", false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&bodyJob); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid backup job JSON")
		return bodyJob.Job, nil, "", false
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "backup job request must contain exactly one JSON object")
		return bodyJob.Job, nil, "", false
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(body, &raw)
	present := make(map[string]bool, len(raw))
	for k := range raw {
		present[k] = true
	}
	return bodyJob.Job, present, bodyJob.EncryptionMode, true
}

// resolveJobRefs materializes the connection and policy a job references into the job (host, secret
// variable names, schedule, retention...) and normalizes its schedule. Policy fields the request carried
// with a different value become overrides (unless the request lists "overrides" itself).
func (s *Server) resolveJobRefs(ctx context.Context, j *backup.Job, present map[string]bool, mode string) (warnings []string, err error) {
	if j.ConnectionID != "" {
		c, ok, err := s.db.st.GetBackupConnection(ctx, j.ConnectionID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: connection %q does not exist", backup.ErrInvalid, j.ConnectionID)
		}
		if *j, err = backup.ApplyConnection(*j, c); err != nil {
			return nil, err
		}
	}
	if j.ScratchConnectionID != "" && !j.TestRestore {
		j.ScratchConnectionID = "" // no test restore: the scratch server is not used
	}
	if j.ScratchConnectionID != "" {
		c, ok, err := s.db.st.GetBackupConnection(ctx, j.ScratchConnectionID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: scratch connection %q does not exist", backup.ErrInvalid, j.ScratchConnectionID)
		}
		if *j, err = backup.ApplyScratchConnection(*j, c); err != nil {
			return nil, err
		}
	}
	if mode == "inherit" && j.PolicyID == "" {
		return nil, errors.New("inherit encryption requires a backup policy")
	}
	if (present["encryption_mode"] || mode != "") && mode != "inherit" && mode != "on" && mode != "off" {
		return nil, errors.New("encryption_mode must be inherit, on or off")
	}
	if mode == "on" || mode == "off" {
		j.Encrypt = mode == "on"
	}
	if j.PolicyID != "" {
		p, ok, err := s.db.st.GetBackupPolicy(ctx, j.PolicyID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: policy %q does not exist", backup.ErrInvalid, j.PolicyID)
		}
		if !present["overrides"] {
			j.Overrides = backup.DeriveOverrides(*j, p, func(f string) bool { return present[f] })
		}
		if mode != "" {
			j.Overrides = slices.DeleteFunc(j.Overrides, func(field string) bool { return field == "encrypt" })
			if mode != "inherit" {
				j.Overrides = append(j.Overrides, "encrypt")
			}
		}
		*j, warnings = backup.ApplyPolicy(*j, p)
	}
	backup.NormalizeSchedule(j)
	return warnings, nil
}

// initialNextRun is when a (re)started job first runs: right away for interval jobs (existing behaviour),
// the schedule's next occurrence for daily/weekly/monthly/cron jobs.
func initialNextRun(j backup.Job, now time.Time) time.Time {
	now = now.UTC()
	if j.Schedule != nil && j.Schedule.Kind != backup.ScheduleInterval {
		if n, err := j.Schedule.Next(now, now); err == nil {
			return n
		}
	}
	return now
}

func (s *Server) apiBackupJobCreate(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	j, present, encMode, ok := s.decodeBackupJob(w, r)
	if !ok {
		return
	}
	warnings, err := s.resolveJobRefs(r.Context(), &j, present, encMode)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if j.ID == "" {
		id, err := randomBackupJobID()
		if err != nil {
			writeErr(w, 500, "could not create backup job id")
			return
		}
		j.ID = id
	}
	if j.Enabled {
		if err := backupJobEncryptionError(j); err != nil {
			writeErr(w, http.StatusConflict, err.Error())
			return
		}
		next := initialNextRun(j, s.now())
		j.NextRunAt = &next
	}
	if err := s.validateBackupJob(j); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if refuseJobWithoutTools(w, j, &warnings) {
		return
	}
	if !s.auditBackupJobDetail(w, r, "backup_job.create", j.ID, jobAuditDetail(j)) {
		return
	}
	if err := s.db.st.CreateBackupJob(r.Context(), j); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	var processID string
	if j.Enabled {
		processID, err = s.startBackupJob(j)
		if err != nil {
			_, _ = s.db.st.DeleteBackupJob(context.Background(), j.ID)
			if writeToolMissing(w, err) {
				return
			}
			writeErr(w, http.StatusBadGateway, "could not start backup job: "+err.Error())
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"job": j, "process_id": processID, "warnings": warnings, "encryption": jobEncryption(j)})
}

func (s *Server) apiBackupJobUpdate(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	j, present, encMode, ok := s.decodeBackupJob(w, r)
	if !ok {
		return
	}
	j.ID = r.PathValue("id")
	cur, found, err := s.db.st.GetBackupJob(r.Context(), j.ID)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup job")
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	if !present["encryption_mode"] && !present["encrypt"] && !present["overrides"] && cur.PolicyID == j.PolicyID {
		switch {
		case j.PolicyID == "":
			j.Encrypt = cur.Encrypt
		case slices.Contains(cur.Overrides, "encrypt"):
			encMode = jobEncryption(cur).Mode
		}
	}

	warnings, err := s.resolveJobRefs(r.Context(), &j, present, encMode)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if j.Enabled && (!cur.Enabled || !cur.Encrypt || cur.IsCustom()) {
		if err := backupJobEncryptionError(j); err != nil {
			writeErr(w, http.StatusConflict, err.Error())
			return
		}
	}
	if j.Enabled {
		next := initialNextRun(j, s.now())
		j.NextRunAt = &next
	} else {
		j.NextRunAt = nil
	}
	if err := s.validateBackupJob(j); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if refuseJobWithoutTools(w, j, &warnings) {
		return
	}
	detail := jobAuditDetail(j)
	detail["changes"] = changedFields(cur.JobOptions, j.JobOptions)
	if cur.EverySeconds != j.EverySeconds {
		detail["changes"].(map[string]any)["every_seconds"] = map[string]any{"from": cur.EverySeconds, "to": j.EverySeconds}
	}
	if cur.Name != j.Name {
		detail["changes"].(map[string]any)["name"] = map[string]any{"from": cur.Name, "to": j.Name}
	}
	if cur.Enabled != j.Enabled {
		detail["changes"].(map[string]any)["enabled"] = map[string]any{"from": cur.Enabled, "to": j.Enabled}
	}
	if !s.auditBackupJobDetail(w, r, "backup_job.update", j.ID, detail) {
		return
	}
	changed, err := s.db.st.UpdateBackupJob(r.Context(), j)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if !changed {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	// Publish the new revision before draining local work. Other replicas check
	// updated_at before a due run, including replicas waiting for the scheduler lock.
	if err := s.stopBackupJob(j.ID); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	var processID string
	if j.Enabled {
		processID, err = s.startBackupJob(j)
	}
	if err != nil {
		if writeToolMissing(w, err) {
			return
		}
		go s.reconcileBackupSchedule(backupJobPipeline(j.ID))
		writeErr(w, http.StatusBadGateway, "could not start backup job: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": j, "process_id": processID, "warnings": warnings, "encryption": jobEncryption(j)})
}

func (s *Server) apiBackupJobStart(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	id := r.PathValue("id")
	j, found, err := s.db.st.GetBackupJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup job")
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	if err := backupJobEncryptionError(j); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	base := j
	base.LogArchive = false // stored jobs with an unsupported PITR setting still take base backups
	if err := s.validateBackupJob(base); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if writeToolMissing(w, jobToolError(j)) {
		return
	}
	if !s.auditBackupJob(w, r, "backup_job.start", id) {
		return
	}
	j.Enabled = true
	next := initialNextRun(j, s.now())
	j.NextRunAt = &next
	changed, err := s.db.st.UpdateBackupJob(r.Context(), j)
	if err != nil {
		writeErr(w, 500, "could not enable backup job")
		return
	}
	if !changed {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	processID, err := s.startBackupJob(j)
	if err != nil {
		if writeToolMissing(w, err) {
			return
		}
		go s.reconcileBackupSchedule(backupJobPipeline(id))
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": j, "process_id": processID})
}

func (s *Server) apiBackupJobRun(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	id := r.PathValue("id")
	j, found, err := s.db.st.GetBackupJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup job")
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	if err := backupJobEncryptionError(j); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	base := j
	base.LogArchive = false // stored jobs with an unsupported PITR setting still take base backups
	if err := s.validateBackupJob(base); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if writeToolMissing(w, runToolError(j)) {
		return
	}
	if !s.auditBackupJob(w, r, "backup_job.run", id) {
		return
	}
	processID, err := s.startBackupProcessWithTrigger(j, true, "api")
	if err != nil {
		if writeToolMissing(w, err) {
			return
		}
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"job": j, "process_id": processID})
}

func (s *Server) apiBackupJobStop(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	id := r.PathValue("id")
	j, found, err := s.db.st.GetBackupJob(r.Context(), id)
	if err != nil || !found {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	if !s.auditBackupJob(w, r, "backup_job.stop", id) {
		return
	}
	j.Enabled = false
	j.NextRunAt = nil
	changed, err := s.db.st.UpdateBackupJob(r.Context(), j)
	if err != nil {
		writeErr(w, 500, "could not disable backup job")
		return
	}
	if !changed {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	if err := s.stopBackupJob(id); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": j})
}

func (s *Server) apiBackupJobDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	id := r.PathValue("id")
	if !s.auditBackupJob(w, r, "backup_job.delete", id) {
		return
	}
	deleted, err := s.db.st.DeleteBackupJob(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not delete backup job")
		return
	}
	if !deleted {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	if err := s.stopBackupJob(id); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// jobSecretEnv hands the stored secret of the job's connection to the backup process through its
// environment (never argv, never a file): STRAND_BACKUP_CONN_PASSWORD / STRAND_BACKUP_CONN_URI. Jobs
// without a stored-secret connection get none (and the variables are blanked, so an inherited value cannot
// leak into the wrong job).
func (s *Server) jobSecretEnv(j backup.Job) []string {
	if j.ConnectionID == "" || j.IsCustom() || s.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, ok, err := s.db.st.GetBackupConnection(ctx, j.ConnectionID)
	env := []string{backup.EnvConnPassword + "=", backup.EnvConnURI + "="}
	if err != nil || !ok || c.SecretMode != backup.SecretStored {
		return env
	}
	pw, uri, err := s.connSecrets(c)
	if err != nil {
		s.log.Error("decrypt stored connection secret", "connection", c.ID, "error", err.Error())
		return env
	}
	return []string{backup.EnvConnPassword + "=" + pw, backup.EnvConnURI + "=" + uri}
}

// scratchSecretEnv returns the environment carrying the stored password of the job's scratch connection (test
// restores), for the verify process only. Always sets the variable (empty when there is none) so a value inherited
// from the Web service's own environment never leaks into the child.
func (s *Server) scratchSecretEnv(j backup.Job) []string {
	env := []string{backup.EnvScratchPassword + "="}
	if j.ScratchConnectionID == "" || !j.TestRestore || s.db == nil {
		return env
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, ok, err := s.db.st.GetBackupConnection(ctx, j.ScratchConnectionID)
	if err != nil || !ok || c.SecretMode != backup.SecretStored || !c.PasswordSet {
		return env
	}
	pw, _, err := s.connSecrets(c)
	if err != nil {
		s.log.Error("decrypt stored scratch connection secret", "connection", c.ID, "error", err.Error())
		return env
	}
	return []string{backup.EnvScratchPassword + "=" + pw}
}

// refreshJobsUsing re-materializes every job that references the connection or policy `id` (after an edit
// of it), stores the changed ones and restarts their schedulers. It returns the ids it updated and the
// "id: reason" of those it could not.
func (s *Server) refreshJobsUsing(ctx context.Context, kind, id string) (refreshed, failed []string) {
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return nil, []string{"list jobs: " + err.Error()}
	}
	for _, j := range jobs {
		if (kind == "connection" && j.ConnectionID != id && j.ScratchConnectionID != id) || (kind == "policy" && j.PolicyID != id) {
			continue
		}
		nj := j
		if nj.ConnectionID != "" {
			if c, ok, err := s.db.st.GetBackupConnection(ctx, nj.ConnectionID); err == nil && ok {
				if nj, err = backup.ApplyConnection(nj, c); err != nil {
					failed = append(failed, j.ID+": "+err.Error())
					continue
				}
			}
		}
		if nj.ScratchConnectionID != "" && nj.TestRestore {
			if c, ok, err := s.db.st.GetBackupConnection(ctx, nj.ScratchConnectionID); err == nil && ok {
				if nj, err = backup.ApplyScratchConnection(nj, c); err != nil {
					failed = append(failed, j.ID+": "+err.Error())
					continue
				}
			}
		}
		if nj.PolicyID != "" {
			if p, ok, err := s.db.st.GetBackupPolicy(ctx, nj.PolicyID); err == nil && ok {
				nj, _ = backup.ApplyPolicy(nj, p)
			}
		}
		if reflect.DeepEqual(j.JobOptions, nj.JobOptions) && j.EverySeconds == nj.EverySeconds {
			continue // nothing the edit touches changes this job
		}
		if nj.Enabled {
			if nj.Schedule != nil && nj.Schedule.Kind != backup.ScheduleInterval {
				n := initialNextRun(nj, s.now())
				nj.NextRunAt = &n
			}
		} else {
			nj.NextRunAt = nil
		}
		if err := s.validateBackupJob(nj); err != nil {
			failed = append(failed, j.ID+": "+err.Error())
			continue
		}
		if changed, err := s.db.st.UpdateBackupJob(ctx, nj); err != nil || !changed {
			failed = append(failed, j.ID+": could not store the refreshed job")
			continue
		}
		refreshed = append(refreshed, j.ID)
		if err := s.stopBackupJob(j.ID); err != nil {
			failed = append(failed, j.ID+": "+err.Error())
			continue
		}
		if nj.Enabled {
			if _, err := s.startBackupJob(nj); err != nil {
				if isToolMissing(err) {
					failed = append(failed, j.ID+": "+err.Error())
					continue
				}
				go s.reconcileBackupSchedule(backupJobPipeline(j.ID))
			}
		}
	}
	return refreshed, failed
}
