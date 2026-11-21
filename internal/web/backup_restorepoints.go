package web

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// Restore point lifecycle in the Web: only complete restore points can be restored, verified, copied or put
// on legal hold (409 otherwise); records that are not complete (failed, expired, abandoned uploads) can be
// deleted with DELETE /api/v1/backups/{id}, which runs `strand backup delete` as a job process: the child
// receives the repository credentials in its environment like verify/prune/copy (the Web holds no S3
// client), removes the partial objects and then the catalog record.

// abandonedAfter: an in-progress record that no tracked process can own (a `strand backup push` from a
// shell) is considered abandoned when it is older than this.
const abandonedAfter = 24 * time.Hour

// catalogMsg strips the catalog's sentinel prefixes from an error for an API answer.
func catalogMsg(err error) string {
	msg := err.Error()
	for _, sentinel := range []error{backup.ErrNotComplete, backup.ErrInvalid, backup.ErrConflict, backup.ErrTransition} {
		msg = strings.TrimPrefix(msg, sentinel.Error()+": ")
	}
	return msg
}

// requireCompleteRecord answers 404 for an unknown id and 409 for a record that is not complete.
func (s *Server) requireCompleteRecord(w http.ResponseWriter, r *http.Request, id, verb string) bool {
	rec, found, err := s.backups.Get(r.Context(), id)
	if err != nil && errors.Is(err, backup.ErrInvalid) {
		writeErr(w, http.StatusBadRequest, "invalid backup id")
		return false
	}
	if err != nil || !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return false
	}
	if err := backup.RequireComplete(rec, verb); err != nil {
		writeErr(w, http.StatusConflict, catalogMsg(err))
		return false
	}
	return true
}

// backupWriterRunning reports whether the process that writes an in-progress record may still be alive:
// its job's run (any instance: the run state is in the database), the job's log archiver or the copy
// process on this instance. A record with no owner to check is presumed alive for abandonedAfter.
func (s *Server) backupWriterRunning(ctx context.Context, rec backup.Record) (bool, string) {
	running := func(pipeline string) bool {
		if s.jobs == nil {
			return false
		}
		for _, p := range s.jobs.List(pipeline) {
			if p.Running {
				return true
			}
		}
		return false
	}
	owned := false
	if rec.JobID != "" {
		owned = true
		if j, ok, err := s.db.st.GetBackupJob(ctx, rec.JobID); err != nil {
			return true, "its backup job could not be read"
		} else if ok && j.LastRunStatus == backup.RunStatusRunning {
			return true, "a run of backup job " + rec.JobID + " is still running"
		}
		if running(backupJobRunPipeline(rec.JobID)) {
			return true, "a run of backup job " + rec.JobID + " is still running"
		}
		if rec.KindOrFull() == backup.KindLog && running("backup-logs-"+rec.JobID) {
			return true, "the log archiver of backup job " + rec.JobID + " is still running"
		}
	}
	if rec.CopyOf != "" {
		owned = true
		if running("backup-copy-" + rec.CopyOf) {
			return true, "the copy of " + rec.CopyOf + " is still running"
		}
	}
	if !owned && s.now().Sub(rec.CreatedAt) < abandonedAfter {
		return true, "its uploader cannot be checked; it can be deleted " + abandonedAfter.String() + " after it started"
	}
	return false, ""
}

// apiBackupDelete deletes a restore point that is not complete (operator+): validation here gives a quick
// 409 with the reason; the `strand backup delete` process re-checks everything against the catalog.
func (s *Server) apiBackupDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	rec, found, err := s.backups.Get(r.Context(), id)
	if err != nil && errors.Is(err, backup.ErrInvalid) {
		writeErr(w, http.StatusBadRequest, "invalid backup id")
		return
	}
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return
	}
	if err := backup.DeletableErr(rec, s.now()); err != nil {
		writeErr(w, http.StatusConflict, catalogMsg(err))
		return
	}
	args := []string{"delete", "--id", id}
	if backup.InProgress(rec.Status) {
		if alive, why := s.backupWriterRunning(r.Context(), rec); alive {
			writeErr(w, http.StatusConflict, "restore point "+id+" is "+string(rec.Status)+": "+why)
			return
		}
		args = append(args, "--abandoned")
	}
	if !s.auditBackupOp(w, r, "backup.delete", map[string]any{"backup_id": id, "status": string(rec.Status), "source": rec.Source}) {
		return
	}
	pid, err := s.startBackupOp("backup-delete", "backup-delete-"+id, args...)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"process_id": pid})
}

// recordRun finds the job run that produced rec (runs record their backup id only on success, so a failed
// record is matched by time: the run started before the record was created and finished after it).
func (s *Server) recordRun(ctx context.Context, rec backup.Record) *backup.Run {
	if rec.JobID == "" || s.db == nil || s.db.st == nil {
		return nil
	}
	runs, err := s.db.st.ListBackupRuns(ctx, store.BackupRunFilter{JobID: rec.JobID, Limit: 100})
	if err != nil {
		return nil
	}
	for i := range runs {
		run := runs[i]
		if run.BackupID == rec.ID {
			return &run
		}
		if run.BackupID == "" && !run.StartedAt.After(rec.CreatedAt.Add(time.Second)) &&
			(run.FinishedAt == nil || !run.FinishedAt.Before(rec.CreatedAt.Add(-time.Second))) {
			return &run
		}
	}
	return nil
}
