package web

import (
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
	"sort"
	"strings"
	"syscall"
	"time"

	"strand/internal/backup"
)

type backupJobView struct {
	Job     backup.Job `json:"job"`
	Process *JobStatus `json:"process,omitempty"`
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
	id := strings.TrimPrefix(pipeline, "backup-")
	delay := reconcileDelay.first
	for attempt := 1; ; attempt++ {
		if !s.isActive() {
			return // a standby runs no schedulers; the active instance (or a takeover) does
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		job, found, err := s.db.st.GetBackupJob(ctx, id)
		cancel()
		if err != nil {
			s.log.Warn("recheck scheduled backup job", "job", id, "attempt", attempt, "error", err)
		} else if !found || !job.Enabled {
			return
		} else if verr := s.validateBackupJob(job); verr != nil {
			// Permanent (configuration) problem: retrying cannot help; the job page shows it on Start.
			s.log.Error("scheduled backup job refused", "job", id, "error", verr)
			return
		} else if _, err := s.startBackupJob(job); err == nil {
			return
		} else {
			s.log.Warn("restart scheduled backup job", "job", id, "attempt", attempt, "error", err)
		}
		time.Sleep(delay)
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
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Enabled {
			if err := s.validateBackupJob(j); err != nil {
				s.log.Error("scheduled backup job refused", "job", j.ID, "error", err)
				continue
			}
			if _, err := s.startBackupJob(j); err != nil {
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
		out = append(out, backupJobView{Job: j, Process: s.backupJobProcess(j)})
	}
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

func (s *Server) validateBackupJob(j backup.Job) error {
	if err := backup.ValidateJob(j); err != nil {
		return err
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
	if s.jobs == nil {
		return "", errors.New("job runner unavailable")
	}
	if !s.isActive() {
		return "", errors.New("this web instance is standby; backup jobs run on the active instance")
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
	args = append(args, "backup", "schedule", "--every", (time.Duration(j.EverySeconds) * time.Second).String(),
		"--artifact", j.ArtifactTemplate, "--manifest", j.ManifestTemplate, "--bucket", j.Bucket, "--job-id", j.ID)
	if j.CleanupLocal {
		args = append(args, "--cleanup-local")
	}
	if once {
		args = append(args, "--once")
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
	kind := "backup-schedule"
	if once {
		kind = "backup-run"
	}
	pipeline := backupJobPipeline(j.ID)
	if once {
		pipeline = backupJobRunPipeline(j.ID)
	}
	id, err := s.jobs.Start(context.Background(), JobSpec{Pipeline: pipeline, Kind: kind, Args: args})
	if err != nil {
		return "", err
	}
	// Ghi nhớ tiến trình do CHÍNH instance này khởi động: khi mất quyền active chỉ dừng đúng các tiến trình
	// này. Danh sách job dùng chung qua DB không dùng được cho việc đó: instance mới lên active đánh dấu
	// tiến trình của instance cũ là "không chạy" (dù vẫn sống) và có tiến trình mới cùng pipeline.
	if st, ok := s.jobs.Get(id); ok && st.PID > 0 {
		s.ownedBackupProcs.Store(id, st.PID)
	}
	return id, nil
}

// stopBackupSchedulers stops every backup scheduler and Run-now process of this instance (SIGTERM, then
// SIGKILL after the runner's grace period). Called when this instance stops being the active one, so the
// new active instance's schedulers take over; a run in progress is recorded as interrupted by the next
// scheduler and the job's next run is kept.
func (s *Server) stopBackupSchedulers() {
	grace := 10 * time.Second
	if jr, ok := s.jobs.(*FileJobRunner); ok && jr.KillGrace > 0 {
		grace = jr.KillGrace
	}
	s.ownedBackupProcs.Range(func(k, v any) bool {
		id, pid := k.(string), v.(int)
		s.ownedBackupProcs.Delete(id)
		if syscall.Kill(pid, 0) != nil {
			return true // đã thoát
		}
		if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
			_ = syscall.Kill(pid, syscall.SIGTERM)
		}
		go func() {
			deadline := time.Now().Add(grace)
			for time.Now().Before(deadline) {
				if syscall.Kill(pid, 0) != nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}()
		s.log.Info("step-down: stopped backup process started by this instance", "job", id, "pid", pid)
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
	if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: action, Pipeline: id, Result: "started"}); err != nil {
		s.log.Error("audit backup job", "action", action, "error", err)
		writeErr(w, http.StatusInternalServerError, "audit log unavailable; refusing to change backup jobs")
		return false
	}
	return true
}

func (s *Server) apiBackupJobs(w http.ResponseWriter, r *http.Request, _ *Session) {
	views, err := s.backupJobViews(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": views})
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
	writeJSON(w, http.StatusOK, map[string]any{"job": j, "process": s.backupJobProcess(j)})
}

func (s *Server) decodeBackupJob(w http.ResponseWriter, r *http.Request) (backup.Job, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	var j backup.Job
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&j); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid backup job JSON")
		return j, false
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "backup job request must contain exactly one JSON object")
		return j, false
	}
	return j, true
}

func (s *Server) apiBackupJobCreate(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	j, ok := s.decodeBackupJob(w, r)
	if !ok {
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
		next := s.now().UTC()
		j.NextRunAt = &next
	}
	if err := s.validateBackupJob(j); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.auditBackupJob(w, r, "backup_job.create", j.ID) {
		return
	}
	if err := s.db.st.CreateBackupJob(r.Context(), j); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	var processID string
	var err error
	if j.Enabled {
		processID, err = s.startBackupJob(j)
		if err != nil {
			_, _ = s.db.st.DeleteBackupJob(context.Background(), j.ID)
			writeErr(w, http.StatusBadGateway, "could not start backup job: "+err.Error())
			return
		}
	}
	writeJSON(w, http.StatusCreated, map[string]any{"job": j, "process_id": processID})
}

func (s *Server) apiBackupJobUpdate(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	j, ok := s.decodeBackupJob(w, r)
	if !ok {
		return
	}
	j.ID = r.PathValue("id")
	if j.Enabled {
		next := s.now().UTC()
		j.NextRunAt = &next
	} else {
		j.NextRunAt = nil
	}
	if err := s.validateBackupJob(j); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.auditBackupJob(w, r, "backup_job.update", j.ID) {
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
		go s.reconcileBackupSchedule(backupJobPipeline(j.ID))
		writeErr(w, http.StatusBadGateway, "could not start backup job: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": j, "process_id": processID})
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
	if err := s.validateBackupJob(j); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.auditBackupJob(w, r, "backup_job.start", id) {
		return
	}
	j.Enabled = true
	next := s.now().UTC()
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
	if err := s.validateBackupJob(j); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.auditBackupJob(w, r, "backup_job.run", id) {
		return
	}
	processID, err := s.startBackupProcess(j, true)
	if err != nil {
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
