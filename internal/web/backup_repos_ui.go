package web

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/objectstore"
	"strand/internal/web/store"
)

// Repositories managed from the console. Two modes: env (endpoint and credentials in STRAND_BACKUP_REPO_<NAME>_*
// of the Web service, the original behaviour) and stored (endpoint/region in the database, access key and
// secret key sealed with the Web secret key, AAD = repository id). Stored credentials are write-only: no
// response, page, audit entry, log line or argv ever carries them. The Web service decrypts them only to put
// them in the ENVIRONMENT of the `strand backup ...` processes it starts (repoSecretEnv).

func (s *Server) backupRepoRoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backup-repositories", s.auth(true, s.apiBackupRepos))
	mux.Handle("POST /api/v1/backup-repositories", s.auth(true, s.apiBackupRepoSave(false)))
	mux.Handle("GET /api/v1/backup-repositories/{id}", s.auth(true, s.apiBackupRepoGet))
	mux.Handle("PUT /api/v1/backup-repositories/{id}", s.auth(true, s.apiBackupRepoSave(true)))
	mux.Handle("DELETE /api/v1/backup-repositories/{id}", s.auth(true, s.apiBackupRepoDelete))
	mux.Handle("POST /api/v1/backup-repositories/{id}/test", s.auth(true, s.apiBackupRepoTest))
}

// repoRequest is the write body; access_key and secret_key are write-only (nil keeps the stored value).
type repoRequest struct {
	ID                 string                 `json:"id"`
	Name               string                 `json:"name"`
	Kind               string                 `json:"kind"`
	SecretMode         string                 `json:"secret_mode"`
	EnvPrefix          string                 `json:"env_prefix"`
	Endpoint           string                 `json:"endpoint"`
	Region             string                 `json:"region"`
	AllowInsecureLocal bool                   `json:"allow_insecure_local"`
	AccessKey          *string                `json:"access_key"`
	SecretKey          *string                `json:"secret_key"`
	Bucket             string                 `json:"bucket"`
	Prefix             string                 `json:"prefix"`
	LockMode           string                 `json:"lock_mode"`
	LockDays           int                    `json:"lock_days"`
	Retention          backup.RetentionPolicy `json:"retention"`
}

type repoView struct {
	backup.Repo
	Configured bool            `json:"configured"` // endpoint and credentials are available
	Immutable  bool            `json:"immutable"`  // Object Lock COMPLIANCE
	UsedBytes  int64           `json:"used_bytes"`
	Backups    int             `json:"backup_count"`
	Copies     int             `json:"copy_count"`
	TestSteps  json.RawMessage `json:"last_test_steps,omitempty"`
}

func (s *Server) repoView(rp backup.Repo, usage []store.RepoUsage) repoView {
	v := repoView{Repo: rp, Immutable: rp.Immutable()}
	if rp.Stored() {
		v.Configured = rp.AccessKeySet && rp.SecretKeySet && rp.Endpoint != ""
	} else {
		v.Configured = os.Getenv(rp.EnvPrefix+"ENDPOINT") != "" && os.Getenv(rp.EnvPrefix+"ACCESS_KEY") != ""
	}
	for _, u := range usage {
		if u.Repository == rp.ID {
			v.UsedBytes += u.Bytes
			v.Backups += u.Count
			v.Copies += u.Copies
		}
	}
	if len(rp.LastTestSteps) > 0 && json.Valid(rp.LastTestSteps) {
		v.TestSteps = json.RawMessage(rp.LastTestSteps)
	}
	return v
}

func (s *Server) apiBackupRepos(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	rs, err := s.db.st.ListBackupRepos(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read repositories")
		return
	}
	usage, _ := s.db.st.BackupRepoUsage(r.Context())
	out := make([]repoView, 0, len(rs))
	for _, rp := range rs {
		out = append(out, s.repoView(rp, usage))
	}
	writeJSON(w, http.StatusOK, map[string]any{"repositories": out, "secret_storage": s.db.box != nil})
}

// repoUsedBy lists the jobs and policies that name the repository (as storage or replication target).
func (s *Server) repoUsedBy(ctx context.Context, id string) (jobs, policies []string) {
	js, _ := s.db.st.ListBackupJobs(ctx)
	for _, j := range js {
		if j.Repository == id || j.ReplicateTo == id {
			jobs = append(jobs, j.ID)
		}
	}
	ps, _ := s.db.st.ListBackupPolicies(ctx)
	for _, p := range ps {
		if p.Repository == id || p.ReplicateTo == id {
			policies = append(policies, p.ID)
		}
	}
	sort.Strings(jobs)
	sort.Strings(policies)
	return
}

func (s *Server) apiBackupRepoGet(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	rp, ok, err := s.db.st.GetBackupRepo(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the repository")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	}
	usage, _ := s.db.st.BackupRepoUsage(r.Context())
	var bySource []store.RepoUsage
	for _, u := range usage {
		if u.Repository == rp.ID {
			bySource = append(bySource, u)
		}
	}
	jobs, policies := s.repoUsedBy(r.Context(), rp.ID)
	writeJSON(w, http.StatusOK, map[string]any{"repository": s.repoView(rp, usage), "usage_by_source": bySource,
		"used_by": map[string]any{"jobs": jobs, "policies": policies}})
}

func randomRepoID(name string) string {
	id := strings.ToLower(strings.Map(func(c rune) rune {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			return c
		}
		return '-'
	}, name))
	id = strings.Trim(id, "-_")
	if id == "" || len(id) > 100 {
		var b [8]byte
		_, _ = rand.Read(b[:])
		id = "repo-" + hex.EncodeToString(b[:])
	}
	return id
}

// repoFromRequest builds the repository to store. Request fields replace the definition; keys the request does
// not carry are kept from cur (update); keys are sealed under the repository id.
func (s *Server) repoFromRequest(req repoRequest, cur *backup.Repo) (backup.Repo, error) {
	rp := backup.Repo{ID: req.ID, Name: req.Name, Kind: req.Kind, SecretMode: req.SecretMode, EnvPrefix: req.EnvPrefix,
		Endpoint: req.Endpoint, Region: req.Region, AllowInsecureLocal: req.AllowInsecureLocal, Bucket: req.Bucket, Prefix: req.Prefix,
		LockMode: req.LockMode, LockDays: req.LockDays, Retention: req.Retention}
	if cur != nil {
		rp.ID = cur.ID
	}
	if rp.SecretMode == "" {
		switch {
		case req.EnvPrefix != "":
			rp.SecretMode = backup.RepoSecretEnv
		case req.Endpoint != "" || req.AccessKey != nil || req.SecretKey != nil:
			rp.SecretMode = backup.RepoSecretStored
		default:
			rp.SecretMode = backup.RepoSecretEnv
		}
	}
	if rp.SecretMode != backup.RepoSecretStored && (req.AccessKey != nil || req.SecretKey != nil) {
		return rp, errors.New("access_key and secret_key are only accepted with secret_mode stored (secret_mode env reads them from the Web environment)")
	}
	if rp.SecretMode == backup.RepoSecretStored {
		if cur != nil && cur.SecretMode == backup.RepoSecretStored {
			rp.AccessKeySealed, rp.SecretKeySealed = cur.AccessKeySealed, cur.SecretKeySealed
		}
		seal := func(v *string, dst *[]byte, what string) error {
			if v == nil {
				return nil
			}
			if *v == "" {
				*dst = nil
				return nil
			}
			if len(*v) > 1024 || strings.ContainsAny(*v, "\x00\r\n") {
				return errors.New(what + " is too long or contains a control character")
			}
			sealed, err := s.db.box.Seal([]byte(*v), rp.ID)
			if err != nil {
				return errors.New("could not seal the " + what)
			}
			*dst = sealed
			return nil
		}
		if err := seal(req.AccessKey, &rp.AccessKeySealed, "access key"); err != nil {
			return rp, err
		}
		if err := seal(req.SecretKey, &rp.SecretKeySealed, "secret key"); err != nil {
			return rp, err
		}
	}
	rp.AccessKeySet, rp.SecretKeySet = len(rp.AccessKeySealed) > 0, len(rp.SecretKeySealed) > 0
	return rp, nil
}

// repoChanges lists the changed non-secret fields (from, to) and flags key changes without values.
func repoChanges(old *backup.Repo, nw backup.Repo) map[string]any {
	if old == nil {
		out := map[string]any{"name": nw.Name, "kind": nw.KindOrS3(), "secret_mode": nw.SecretMode, "bucket": nw.Bucket, "lock_mode": nw.LockMode}
		if nw.Endpoint != "" {
			out["endpoint"] = nw.Endpoint
		}
		if nw.AccessKeySet {
			out["access_key"] = "set"
		}
		if nw.SecretKeySet {
			out["secret_key"] = "set"
		}
		return out
	}
	out := changedFields(old, nw)
	delete(out, "access_key_set")
	delete(out, "secret_key_set")
	for k, v := range map[string]string{"access_key": sealedChange(old.AccessKeySealed, nw.AccessKeySealed), "secret_key": sealedChange(old.SecretKeySealed, nw.SecretKeySealed)} {
		if v != "" {
			out[k] = v
		}
	}
	return out
}

func (s *Server) apiBackupRepoSave(update bool) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, _ *Session) {
		if !s.needDB(w) {
			return
		}
		var req repoRequest
		if !decodeStrict(w, r, &req) {
			return
		}
		var cur *backup.Repo
		if update {
			req.ID = r.PathValue("id")
			c, ok, err := s.db.st.GetBackupRepo(r.Context(), req.ID)
			if err != nil {
				writeErr(w, http.StatusServiceUnavailable, "could not read the repository")
				return
			}
			if !ok {
				writeErr(w, http.StatusNotFound, "repository not found")
				return
			}
			cur = &c
		} else if req.ID == "" {
			req.ID = randomRepoID(req.Name)
		}
		rp, err := s.repoFromRequest(req, cur)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := backup.ValidateRepo(rp); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		action := "backup_repository.create"
		if update {
			action = "backup_repository.update"
		}
		if !s.auditBackupOp(w, r, action, map[string]any{"repository": rp.ID, "changes": repoChanges(cur, rp)}) {
			return
		}
		var serr error
		ok := true
		if update {
			ok, serr = s.db.st.UpdateBackupRepo(r.Context(), rp)
		} else {
			serr = s.db.st.CreateBackupRepo(r.Context(), rp)
		}
		switch {
		case errors.Is(serr, backup.ErrInvalid):
			writeErr(w, http.StatusBadRequest, serr.Error())
		case serr != nil:
			writeErr(w, http.StatusConflict, serr.Error())
		case !ok:
			writeErr(w, http.StatusNotFound, "repository not found")
		default:
			got, _, _ := s.db.st.GetBackupRepo(r.Context(), rp.ID)
			usage, _ := s.db.st.BackupRepoUsage(r.Context())
			resp := map[string]any{"repository": s.repoView(got, usage)}
			if update && cur != nil && repoAccessChanged(*cur, rp) {
				resp["restarted_jobs"] = s.restartJobsUsingRepo(r.Context(), rp.ID)
			}
			writeJSON(w, http.StatusOK, resp)
		}
	}
}

// repoAccessChanged: the edit changes how a process reaches the repository (its environment is fixed when the
// process starts).
func repoAccessChanged(old, nw backup.Repo) bool {
	return old.SecretMode != nw.SecretMode || old.EnvPrefix != nw.EnvPrefix || old.Endpoint != nw.Endpoint || old.Region != nw.Region ||
		old.AllowInsecureLocal != nw.AllowInsecureLocal || old.Bucket != nw.Bucket || string(old.AccessKeySealed) != string(nw.AccessKeySealed) ||
		string(old.SecretKeySealed) != string(nw.SecretKeySealed)
}

// restartJobsUsingRepo restarts the schedulers of the enabled jobs that store to or replicate to the repository,
// so they pick up its new credentials (a scheduler's environment is fixed when it starts). It returns the ids
// restarted.
func (s *Server) restartJobsUsingRepo(ctx context.Context, id string) []string {
	jobs, _ := s.repoUsedBy(ctx, id)
	var restarted []string
	for _, jid := range jobs {
		j, ok, err := s.db.st.GetBackupJob(ctx, jid)
		if err != nil || !ok || !j.Enabled {
			continue
		}
		if err := s.stopBackupJob(jid); err != nil {
			s.log.Warn("restart scheduler after a repository edit", "job", jid, "error", err)
			continue
		}
		if _, err := s.startBackupJob(j); err != nil {
			go s.reconcileBackupSchedule(backupJobPipeline(jid))
		}
		restarted = append(restarted, jid)
	}
	return restarted
}

func (s *Server) apiBackupRepoDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	if jobs, policies := s.repoUsedBy(r.Context(), id); len(jobs)+len(policies) > 0 {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "repository is used by backup jobs or policies", "jobs": jobs, "policies": policies})
		return
	}
	if !s.auditBackupOp(w, r, "backup_repository.delete", map[string]any{"repository": id}) {
		return
	}
	ok, err := s.db.st.DeleteBackupRepo(r.Context(), id)
	switch {
	case err != nil:
		writeErr(w, http.StatusConflict, err.Error()) // e.g. "repository still holds N backups"
	case !ok:
		writeErr(w, http.StatusNotFound, "repository not found")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// repoKeys opens the stored access key and secret key of a repository.
func (s *Server) repoKeys(rp backup.Repo) (access, secret string, err error) {
	open := func(sealed []byte) (string, error) {
		if len(sealed) == 0 {
			return "", nil
		}
		b, err := s.db.box.Open(sealed, rp.ID)
		if err != nil {
			return "", errors.New("the stored repository credentials cannot be decrypted (wrong STRAND_WEB_SECRET_KEY?)")
		}
		return string(b), nil
	}
	if access, err = open(rp.AccessKeySealed); err != nil {
		return "", "", err
	}
	if secret, err = open(rp.SecretKeySealed); err != nil {
		return "", "", err
	}
	return access, secret, nil
}

// repoSecretEnv returns the NAME=value environment of every stored repository (plaintext credentials, for the
// environment of a backup process only). A repository whose credentials cannot be opened is skipped (logged
// without any value); its jobs then fail with "repo unreachable".
func (s *Server) repoSecretEnv() []string {
	if s.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := s.db.st.ListBackupRepos(ctx)
	if err != nil {
		return nil
	}
	var env []string
	for _, rp := range rs {
		if !rp.Stored() {
			continue
		}
		ak, sk, err := s.repoKeys(rp)
		if err != nil {
			s.log.Error("decrypt stored repository credentials", "repository", rp.ID, "error", err.Error())
			continue
		}
		env = append(env, rp.EnvVars(ak, sk)...)
	}
	return env
}

// ---- connectivity test ----

// RepoTestStep is one line of the checklist.
type RepoTestStep struct {
	Name      string `json:"name"`
	Status    string `json:"status"` // ok | failed | skipped
	Detail    string `json:"detail,omitempty"`
	LatencyMS int64  `json:"latency_ms"`
}

// RepoTestResult is the result of POST /api/v1/backup-repositories/{id}/test.
type RepoTestResult struct {
	OK                  bool           `json:"ok"`
	Status              string         `json:"status"` // ok | failed
	Steps               []RepoTestStep `json:"steps"`
	ObjectLockEnabled   *bool          `json:"object_lock_enabled,omitempty"`
	ImmutableConfigured bool           `json:"immutable_configured"`
	LatencyMS           int64          `json:"latency_ms"` // whole checklist
	At                  time.Time      `json:"at"`
}

// repoObjectStore builds the S3 client of a repository from its stored keys or its environment prefix.
func (s *Server) repoObjectStore(rp backup.Repo) (*objectstore.S3, error) {
	cfg := objectstore.Config{Bucket: rp.Bucket, MaxRetries: -1, HTTPClient: &http.Client{Timeout: 20 * time.Second}}
	if rp.Stored() {
		ak, sk, err := s.repoKeys(rp)
		if err != nil {
			return nil, err
		}
		cfg.Endpoint, cfg.Region, cfg.AccessKey, cfg.SecretKey, cfg.AllowInsecureLocal = rp.Endpoint, rp.Region, ak, sk, rp.AllowInsecureLocal
	} else {
		p := rp.EnvPrefix
		cfg.Endpoint, cfg.Region = strings.TrimSpace(os.Getenv(p+"ENDPOINT")), strings.TrimSpace(os.Getenv(p+"REGION"))
		cfg.AccessKey, cfg.SecretKey, cfg.SessionToken = os.Getenv(p+"ACCESS_KEY"), os.Getenv(p+"SECRET_KEY"), os.Getenv(p+"SESSION_TOKEN")
		v := strings.TrimSpace(os.Getenv(p + "ALLOW_INSECURE_LOCAL"))
		cfg.AllowInsecureLocal = strings.EqualFold(v, "true") || v == "1"
	}
	c, err := objectstore.New(cfg)
	if err != nil {
		return nil, errors.New("invalid repository settings: check the endpoint, region and credentials")
	}
	return c, nil
}

// testRepository runs the checklist: list, put, get (content compared), delete a small test object, then
// ask whether the bucket has Object Lock (required when the repository is configured immutable). No error
// text carries a credential (the S3 client redacts them).
func (s *Server) testRepository(ctx context.Context, rp backup.Repo) RepoTestResult {
	res := RepoTestResult{ImmutableConfigured: rp.LockMode != "", At: s.now().UTC()}
	begin := time.Now()
	step := func(name string, fn func() (string, error)) bool {
		t0 := time.Now()
		detail, err := fn()
		st := RepoTestStep{Name: name, Status: "ok", Detail: detail, LatencyMS: time.Since(t0).Milliseconds()}
		if err != nil {
			st.Status, st.Detail = "failed", store.Redact(err.Error())
		}
		res.Steps = append(res.Steps, st)
		return err == nil
	}
	skip := func(names ...string) {
		for _, n := range names {
			res.Steps = append(res.Steps, RepoTestStep{Name: n, Status: "skipped", Detail: "an earlier step failed"})
		}
	}
	finish := func() RepoTestResult {
		res.LatencyMS = time.Since(begin).Milliseconds()
		res.OK = true
		for _, st := range res.Steps {
			if st.Status == "failed" {
				res.OK = false
			}
		}
		res.Status = "ok"
		if !res.OK {
			res.Status = "failed"
		}
		return res
	}
	c, err := s.repoObjectStore(rp)
	if err != nil {
		res.Steps = []RepoTestStep{{Name: "configure", Status: "failed", Detail: err.Error()}}
		skip("list", "put", "get", "delete", "object_lock")
		return finish()
	}
	prefix := strings.Trim(rp.Prefix, "/")
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	key := strings.TrimPrefix(prefix+"/.strand-repo-test/"+hex.EncodeToString(nonce[:]), "/")
	payload := []byte("strand repository test " + hex.EncodeToString(nonce[:]))
	version := ""
	ok := step("list", func() (string, error) {
		objs, err := c.List(ctx, strings.TrimPrefix(prefix+"/.strand-repo-test/", "/"))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("bucket %s is reachable (%d stale test object(s))", rp.Bucket, len(objs)), nil
	})
	if ok {
		ok = step("put", func() (string, error) {
			info, err := c.UploadWith(ctx, key, bytes.NewReader(payload), int64(len(payload)), "", objectstore.PutOptions{})
			version = info.VersionID
			return fmt.Sprintf("wrote %d bytes", len(payload)), err
		})
	} else {
		skip("put")
	}
	if ok {
		ok = step("get", func() (string, error) {
			rc, _, err := c.Download(ctx, key)
			if err != nil {
				return "", err
			}
			defer rc.Close()
			var buf bytes.Buffer
			if _, err := buf.ReadFrom(rc); err != nil {
				return "", err
			}
			if !bytes.Equal(buf.Bytes(), payload) {
				return "", errors.New("the object read back differs from what was written")
			}
			return "read back and compared", nil
		})
	} else {
		skip("get")
	}
	if res.hasStep("put", "ok") {
		step("delete", func() (string, error) {
			if err := c.DeleteVersion(ctx, key, version); err != nil {
				return "", fmt.Errorf("could not delete the test object (a bucket default retention would explain it): %w", err)
			}
			return "test object deleted", nil
		})
	} else {
		skip("delete")
	}
	if res.hasStep("list", "ok") {
		step("object_lock", func() (string, error) {
			on, err := c.ObjectLockEnabled(ctx)
			if err != nil {
				return "", err
			}
			res.ObjectLockEnabled = &on
			switch {
			case on:
				return "Object Lock is enabled on the bucket", nil
			case rp.LockMode != "":
				return "", fmt.Errorf("the repository requires %s retention but the bucket has no Object Lock (it can only be enabled when a bucket is created)", rp.LockMode)
			}
			return "Object Lock is not enabled (the repository is not immutable)", nil
		})
	} else {
		skip("object_lock")
	}
	return finish()
}

func (r RepoTestResult) hasStep(name, status string) bool {
	for _, s := range r.Steps {
		if s.Name == name && s.Status == status {
			return true
		}
	}
	return false
}

// apiBackupRepoTest runs the checklist against the saved repository and records the outcome on it. 200 with
// the result even when a step failed (the failure is the answer).
func (s *Server) apiBackupRepoTest(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	rp, ok, err := s.db.st.GetBackupRepo(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the repository")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "repository not found")
		return
	}
	if !s.auditBackupOp(w, r, "backup_repository.test", map[string]any{"repository": rp.ID, "bucket": rp.Bucket}) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	res := s.testRepository(ctx, rp)
	detail := "all steps passed"
	for _, st := range res.Steps {
		if st.Status == "failed" {
			detail = st.Name + ": " + st.Detail
			break
		}
	}
	if len(detail) > 500 {
		detail = detail[:500]
	}
	steps, _ := json.Marshal(res.Steps)
	_, _ = s.db.st.SetBackupRepoTest(r.Context(), rp.ID, res.At, res.Status, detail, steps)
	writeJSON(w, http.StatusOK, res)
}
