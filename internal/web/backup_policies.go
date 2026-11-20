package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"time"

	"strand/internal/backup"
)

// Backup policies (schedule + retention + storage + protection + verification) and the schedule preview.
// Jobs store the effective values (materialized) and the list of fields they override; a policy edit
// re-materializes every job that follows it and restarts its scheduler.

const policySeededKey = "backup_policies_seeded"

func (s *Server) backupPolicyRoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backup-policies", s.auth(true, s.apiPolicyList))
	mux.Handle("POST /api/v1/backup-policies", s.auth(true, s.apiPolicySave(false)))
	mux.Handle("GET /api/v1/backup-policies/{id}", s.auth(true, s.apiPolicyGet))
	mux.Handle("PUT /api/v1/backup-policies/{id}", s.auth(true, s.apiPolicySave(true)))
	mux.Handle("DELETE /api/v1/backup-policies/{id}", s.auth(true, s.apiPolicyDelete))
	mux.Handle("POST /api/v1/backup-policies/{id}/preview", s.auth(true, s.apiPolicyPreview))
	mux.Handle("POST /api/v1/backup-schedules/preview", s.auth(true, s.apiSchedulePreview))
}

// seedBackupPolicies creates the built-in Gold/Silver/Bronze policies once (never again: an operator who
// edited or deleted them keeps that state). Runs under the cross-instance "policy-seed" lock.
func (s *Server) seedBackupPolicies(ctx context.Context) error {
	return s.db.st.Locked(ctx, "policy-seed", func(ctx context.Context) error {
		if _, done, err := s.db.st.MetaGet(ctx, policySeededKey); err != nil || done {
			return err
		}
		for _, p := range backup.BuiltinPolicies() {
			p.CreatedBy = "system"
			if _, err := s.db.st.CreateBackupPolicy(ctx, p); err != nil && !errors.Is(err, backup.ErrConflict) {
				return err
			}
		}
		return s.db.st.MetaSet(ctx, policySeededKey, time.Now().UTC().Format(time.RFC3339))
	})
}

func randomPolicyID() (string, error) {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "policy-" + hex.EncodeToString(b[:]), nil
}

func (s *Server) apiPolicyList(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	ps, err := s.db.st.ListBackupPolicies(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup policies")
		return
	}
	jobs, _ := s.db.st.ListBackupJobs(r.Context())
	counts := map[string]int{}
	for _, j := range jobs {
		if j.PolicyID != "" {
			counts[j.PolicyID]++
		}
	}
	type view struct {
		backup.Policy
		Jobs int `json:"job_count"`
	}
	out := make([]view, 0, len(ps))
	for _, p := range ps {
		out = append(out, view{p, counts[p.ID]})
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": out, "overridable_fields": backup.OverridableFields()})
}

func (s *Server) apiPolicyGet(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	p, ok, err := s.db.st.GetBackupPolicy(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup policy")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "backup policy not found")
		return
	}
	used, _ := s.connsUsedBy(r.Context(), "policy", p.ID)
	writeJSON(w, http.StatusOK, map[string]any{"policy": p, "used_by": used})
}

// policyChanges lists the changed fields (old, new) of a policy edit.
func policyChanges(old *backup.Policy, nw backup.Policy) map[string]any {
	out := map[string]any{}
	if old == nil {
		out["name"], out["schedule"] = nw.Name, nw.Schedule.Human()
		return out
	}
	ov, nv := reflect.ValueOf(*old), reflect.ValueOf(nw)
	t := ov.Type()
	for i := 0; i < t.NumField(); i++ {
		name := strings.Split(t.Field(i).Tag.Get("json"), ",")[0]
		if name == "" || name == "-" || name == "created_at" || name == "updated_at" || name == "created_by" || name == "builtin" {
			continue
		}
		if !reflect.DeepEqual(ov.Field(i).Interface(), nv.Field(i).Interface()) {
			out[name] = map[string]any{"from": ov.Field(i).Interface(), "to": nv.Field(i).Interface()}
		}
	}
	return out
}

func (s *Server) apiPolicySave(update bool) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, sess *Session) {
		if !s.needDB(w) {
			return
		}
		var p backup.Policy
		if !decodeStrict(w, r, &p) {
			return
		}
		var cur *backup.Policy
		if update {
			p.ID = r.PathValue("id")
			c, ok, err := s.db.st.GetBackupPolicy(r.Context(), p.ID)
			if err != nil {
				writeErr(w, http.StatusServiceUnavailable, "could not read backup policy")
				return
			}
			if !ok {
				writeErr(w, http.StatusNotFound, "backup policy not found")
				return
			}
			cur = &c
		} else if p.ID == "" {
			id, err := randomPolicyID()
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "could not create a policy id")
				return
			}
			p.ID = id
		}
		p.Builtin = false
		if cur != nil {
			p.Builtin = cur.Builtin
		}
		if err := p.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		action := "backup_policy.create"
		if update {
			action = "backup_policy.update"
		}
		detail := map[string]any{"policy": p.ID, "changes": policyChanges(cur, p)}
		var affected []policyImpact
		if update {
			var err error
			if affected, err = s.policyImpact(r.Context(), p); err != nil {
				writeErr(w, http.StatusServiceUnavailable, "could not read backup jobs")
				return
			}
			detail["affects_jobs"] = len(affected)
		}
		if !s.auditBackupOp(w, r, action, detail) {
			return
		}
		if !update {
			p.CreatedBy = sess.Username
			got, err := s.db.st.CreateBackupPolicy(r.Context(), p)
			switch {
			case errors.Is(err, backup.ErrConflict):
				writeErr(w, http.StatusConflict, "a policy with this id or name already exists")
			case errors.Is(err, backup.ErrInvalid):
				writeErr(w, http.StatusBadRequest, err.Error())
			case err != nil:
				writeErr(w, http.StatusInternalServerError, "could not create the backup policy")
			default:
				writeJSON(w, http.StatusCreated, map[string]any{"policy": got})
			}
			return
		}
		ok, err := s.db.st.UpdateBackupPolicy(r.Context(), p)
		switch {
		case errors.Is(err, backup.ErrConflict):
			writeErr(w, http.StatusConflict, "a policy with this name already exists")
			return
		case errors.Is(err, backup.ErrInvalid):
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		case err != nil:
			writeErr(w, http.StatusInternalServerError, "could not update the backup policy")
			return
		case !ok:
			writeErr(w, http.StatusNotFound, "backup policy not found")
			return
		}
		refreshed, failed := s.refreshJobsUsing(r.Context(), "policy", p.ID)
		got, _, _ := s.db.st.GetBackupPolicy(r.Context(), p.ID)
		followers := make([]string, 0, len(affected))
		for _, im := range affected {
			followers = append(followers, im.Job)
		}
		// affects_jobs: every job that follows the policy; refreshed_jobs: those whose settings changed
		// (and whose scheduler was restarted).
		writeJSON(w, http.StatusOK, map[string]any{"policy": got, "affects_jobs": len(followers), "affected_jobs": followers,
			"refreshed_jobs": refreshed, "refresh_failed": failed})
	}
}

func (s *Server) apiPolicyDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	if used, err := s.connsUsedBy(r.Context(), "policy", id); err == nil && len(used) > 0 {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "policy is used by backup jobs", "jobs": used})
		return
	}
	if !s.auditBackupOp(w, r, "backup_policy.delete", map[string]any{"policy": id}) {
		return
	}
	ok, err := s.db.st.DeleteBackupPolicy(r.Context(), id)
	var inuse *backup.InUseError
	switch {
	case errors.As(err, &inuse):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "policy is used by backup jobs", "jobs": inuse.Jobs})
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "could not delete the backup policy")
	case !ok:
		writeErr(w, http.StatusNotFound, "backup policy not found")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// policyImpact is what applying a proposed policy would change for one job that follows it.
type policyImpact struct {
	Job      string   `json:"job"`
	Name     string   `json:"name"`
	Changes  []string `json:"changes"`
	Warnings []string `json:"warnings,omitempty"`
}

// policyImpact lists the jobs following policy p and the fields that would change (a dry run: nothing is
// written). Jobs that override a field keep it, so they do not list it.
func (s *Server) policyImpact(ctx context.Context, p backup.Policy) ([]policyImpact, error) {
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return nil, err
	}
	out := []policyImpact{}
	for _, j := range jobs {
		if j.PolicyID != p.ID {
			continue
		}
		nj, warn := backup.ApplyPolicy(j, p)
		out = append(out, policyImpact{Job: j.ID, Name: j.Name, Changes: jobDiff(j, nj), Warnings: warn})
	}
	return out, nil
}

// jobDiff names the policy-managed fields whose value differs between two versions of a job.
func jobDiff(a, b backup.Job) []string {
	changes := []string{}
	for _, f := range backup.OverridableFields() {
		if !reflect.DeepEqual(jobField(a, f), jobField(b, f)) {
			changes = append(changes, f)
		}
	}
	return changes
}

func jobField(j backup.Job, f string) any {
	switch f {
	case "schedule":
		return j.EffectiveSchedule()
	case "window_minutes":
		return j.WindowMinutes
	case "jitter_seconds":
		return j.JitterSeconds
	case "retention":
		return j.Retention
	case "log_archive":
		return j.LogArchive
	case "log_flush_seconds":
		return j.LogFlushSeconds
	case "repository":
		return j.Repository
	case "replicate_to":
		return j.ReplicateTo
	case "compress":
		return j.Compress
	case "compress_level":
		return j.CompressLevel
	case "encrypt":
		return j.Encrypt
	case "verify_every_seconds":
		return j.VerifyEverySeconds
	case "verify_sample_percent":
		return j.VerifySamplePercent
	case "test_restore":
		return j.TestRestore
	case "retry_max":
		return j.RetryMax
	case "retry_backoff_seconds":
		return j.RetryBackoffSeconds
	}
	return nil
}

// apiPolicyPreview is the dry run of a policy edit: body = the proposed policy; response = how many jobs
// would be affected, what changes for each, and the policy schedule's next runs. Nothing is stored.
func (s *Server) apiPolicyPreview(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	cur, ok, err := s.db.st.GetBackupPolicy(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup policy")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "backup policy not found")
		return
	}
	var p backup.Policy
	if !decodeStrict(w, r, &p) {
		return
	}
	p.ID, p.Builtin = cur.ID, cur.Builtin
	if err := p.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	impact, err := s.policyImpact(r.Context(), p)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup jobs")
		return
	}
	runs, _ := schedulePreview(p.Schedule, s.now(), 5)
	writeJSON(w, http.StatusOK, map[string]any{"affects_jobs": len(impact), "jobs": impact,
		"changes": policyChanges(&cur, p), "next_runs": runs})
}

type previewRun struct {
	UTC   time.Time `json:"utc"`
	Local string    `json:"local"`
}

func schedulePreview(sc backup.Schedule, after time.Time, n int) ([]previewRun, error) {
	loc, err := sc.Location()
	if err != nil {
		return nil, err
	}
	times, err := sc.NextN(after, n)
	if err != nil {
		return nil, err
	}
	out := make([]previewRun, 0, len(times))
	for _, t := range times {
		out = append(out, previewRun{UTC: t.UTC(), Local: t.In(loc).Format(time.RFC3339)})
	}
	return out, nil
}

// apiSchedulePreview returns the next runs of a schedule in its time zone (the UI shows it under the
// schedule field). Body: the schedule object, optionally with "count" (1-20, default 5) and "after".
func (s *Server) apiSchedulePreview(w http.ResponseWriter, r *http.Request, _ *Session) {
	var req struct {
		backup.Schedule
		Count int        `json:"count"`
		After *time.Time `json:"after"`
	}
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.Count == 0 {
		req.Count = 5
	}
	if req.Count < 1 || req.Count > 20 {
		writeErr(w, http.StatusBadRequest, "count must be between 1 and 20")
		return
	}
	after := s.now()
	if req.After != nil {
		after = *req.After
	}
	if err := req.Schedule.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	runs, err := schedulePreview(req.Schedule, after, req.Count)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	tz := req.Timezone
	if tz == "" {
		tz = "UTC"
	}
	writeJSON(w, http.StatusOK, map[string]any{"timezone": tz, "human": req.Schedule.Human(), "runs": runs})
}
