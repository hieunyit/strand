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
	"sort"
	"strings"
	"time"

	"strand/internal/backup"
)

type backupJobView struct {
	Job     backup.Job `json:"job"`
	Process *JobStatus `json:"process,omitempty"`
}

func backupJobPipeline(id string) string    { return "backup-" + id }
func backupJobRunPipeline(id string) string { return "run-" + id }

func (s *Server) reconcileBackupSchedule(pipeline string) {
	const attempts = 6
	delay := time.Second
	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		id := strings.TrimPrefix(pipeline, "backup-")
		job, found, err := s.db.st.GetBackupJob(ctx, id)
		cancel()
		if err != nil {
			s.log.Warn("recheck scheduled backup job", "job", id, "error", err)
		} else if !found || !job.Enabled {
			return
		} else {
			if _, err := s.startBackupJob(job); err == nil {
				return
			} else {
				s.log.Warn("restart scheduled backup job", "job", id, "error", err)
			}
		}
		time.Sleep(delay)
		if delay < 30*time.Second {
			delay *= 2
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

func (s *Server) resumeBackupJobs(ctx context.Context) error {
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Enabled {
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

func (s *Server) validateBackupJob(j backup.Job) error {
	if err := backup.ValidateJob(j); err != nil {
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
	return id, nil
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
	if err := s.db.st.CreateBackupJob(r.Context(), j); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "backup_job.create", Result: "ok", Pipeline: j.ID}); err != nil {
		s.log.Error("audit backup job create", "error", err)
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
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "backup_job.start", Result: "ok", Pipeline: id})
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
	processID, err := s.startBackupProcess(j, true)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "backup_job.run", Result: "started", Pipeline: id})
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
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "backup_job.stop", Result: "ok", Pipeline: id})
	writeJSON(w, http.StatusOK, map[string]any{"job": j})
}

func (s *Server) apiBackupJobDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup jobs require the metadata database")
		return
	}
	id := r.PathValue("id")
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
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "backup_job.delete", Result: "ok", Pipeline: id})
	w.WriteHeader(http.StatusNoContent)
}
