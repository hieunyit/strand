package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func TestBackupPagesAndAPIsP1(t *testing.T) {
	st := store.NewMem()
	ctx := context.Background()

	// Seed repository
	_ = st.CreateBackupRepo(ctx, backup.Repo{
		ID:        "central-backups",
		Name:      "Central Backups",
		Bucket:    "central-backups",
		EnvPrefix: "STRAND_BACKUP_S3",
	})

	// Seed job
	job := backup.Job{
		ID:           "bjob-test-1",
		Name:         "mysql-primary-nightly",
		Enabled:      true,
		EverySeconds: 86400,
		Bucket:       "central-backups",
		JobOptions: backup.JobOptions{
			Source: "mysql-prod",
			Method: backup.MethodMySQLDump,
			Conn: backup.ConnSpec{
				Host:        "127.0.0.1",
				Port:        3306,
				User:        "backup",
				PasswordEnv: "MYSQL_PWD",
				Databases:   []string{"shop"},
			},
		},
	}
	if err := st.CreateBackupJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Seed completed backup in catalog
	now := time.Now().UTC().Truncate(time.Second)
	cat, _ := backup.NewCatalog(st)
	bkpRec := backup.Record{
		ID:          "bkp-p1-001",
		Source:      "mysql-prod",
		Engine:      "mysql",
		Tool:        "mysqldump",
		Bucket:      "central-backups",
		ObjectKey:   "p/bkp-p1-001/artifact",
		ManifestURI: "s3://central-backups/p/bkp-p1-001/manifest.json",
		JobID:       job.ID,
		Method:      backup.MethodMySQLDump,
		Kind:        backup.KindFull,
		Status:      backup.StatusQueued,
		CreatedAt:   now.Add(-2 * time.Hour),
	}
	_, _, err := cat.Create(ctx, bkpRec)
	if err != nil {
		t.Fatalf("create backup: %v", err)
	}
	_, _, _ = cat.Finalize(ctx, bkpRec.ID, backup.StatusComplete, 1024*1024, strings.Repeat("a", 64), bkpRec.ManifestURI, now.Add(-115*time.Minute))

	// Seed runs for the job
	run1 := backup.Run{
		ID:        "brun-111",
		JobID:     job.ID,
		Trigger:   backup.TriggerSchedule,
		StartedAt: now.Add(-2 * time.Hour),
		ProcessID: "proc-111",
		Status:    string(backup.RunStatusRunning),
	}
	if err := st.CreateBackupRun(ctx, run1); err != nil {
		t.Fatalf("start run 1: %v", err)
	}
	_, _, _ = st.FinishBackupRun(ctx, run1.ID, now.Add(-115*time.Minute), backup.RunStatusSuccess, 1024*1024, 1024*1024, 5*time.Minute, bkpRec.ID, "", "")

	run2 := backup.Run{
		ID:        "brun-222",
		JobID:     job.ID,
		Trigger:   backup.TriggerManual,
		StartedAt: now.Add(-1 * time.Hour),
		ProcessID: "proc-222",
		Status:    string(backup.RunStatusRunning),
	}
	if err := st.CreateBackupRun(ctx, run2); err != nil {
		t.Fatalf("start run 2: %v", err)
	}
	_, _, _ = st.FinishBackupRun(ctx, run2.ID, now.Add(-59*time.Minute), string(backup.RunStatusFailed), 100, 0, 1*time.Minute, "", backup.ErrCodeNetwork, "connection refused by target host")

	// Create test server environment
	e := dbEnv(t, st, nil, nil)
	waitIdle(t, e, "backup-"+job.ID)
	c, csrf := e.loginOK()
	h := map[string]string{"X-CSRF-Token": csrf, "Content-Type": "application/json"}

	// 1. Overview page (/backups)
	resp, body := e.do(c, "GET", "/backups", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/backups returned %d", resp.StatusCode)
	}
	if !strings.Contains(body, "Protected sources") || !strings.Contains(body, "24h success rate") || !strings.Contains(body, "Storage used") {
		t.Fatalf("/backups missing stat tiles: %s", body)
	}
	if !strings.Contains(body, "polyline class=\"line s1\"") {
		t.Fatalf("/backups missing 14-day stacked chart SVG: %s", body)
	}

	// 2. Jobs list page (/backups/jobs)
	resp, body = e.do(c, "GET", "/backups/jobs", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/backups/jobs returned %d", resp.StatusCode)
	}
	if !strings.Contains(body, "mysql-primary-nightly") {
		t.Fatalf("/backups/jobs missing job: %s", body)
	}
	if !strings.Contains(body, "bulkbar") || !strings.Contains(body, "filterbar") {
		t.Fatalf("/backups/jobs missing bulkbar or filterbar")
	}

	// Filter query strings
	resp, body = e.do(c, "GET", "/backups/jobs?q=mysql&engine=mysql", "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "mysql-primary-nightly") {
		t.Fatalf("/backups/jobs filter match failed: %d", resp.StatusCode)
	}
	resp, body = e.do(c, "GET", "/backups/jobs?engine=postgres", "", nil)
	if resp.StatusCode != http.StatusOK || strings.Contains(body, "mysql-primary-nightly") {
		t.Fatalf("/backups/jobs filter non-match failed: %d", resp.StatusCode)
	}

	// 3. Job detail page (/backups/jobs/{id})
	resp, body = e.do(c, "GET", "/backups/jobs/"+job.ID, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/backups/jobs/%s returned %d", job.ID, resp.StatusCode)
	}
	for _, tab := range []string{"Overview", "Runs", "Restore points", "Settings", "Activity"} {
		if !strings.Contains(body, tab) {
			t.Errorf("/backups/jobs/%s missing tab %q", job.ID, tab)
		}
	}
	// Tab switches
	for _, tabKey := range []string{"overview", "runs", "restore-points", "settings", "activity"} {
		resp, _ = e.do(c, "GET", "/backups/jobs/"+job.ID+"?tab="+tabKey, "", nil)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("job tab %s returned %d", tabKey, resp.StatusCode)
		}
	}

	// 4. Catalog page (/backups/catalog)
	resp, body = e.do(c, "GET", "/backups/catalog", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/backups/catalog returned %d", resp.StatusCode)
	}
	if !strings.Contains(body, bkpRec.ID) {
		t.Fatalf("/backups/catalog missing backup %s", bkpRec.ID)
	}

	// 5. Catalog detail page (/backups/catalog/{id})
	resp, body = e.do(c, "GET", "/backups/catalog/"+bkpRec.ID, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/backups/catalog/%s returned %d", bkpRec.ID, resp.StatusCode)
	}
	if !strings.Contains(body, bkpRec.ID) || !strings.Contains(body, "central-backups") {
		t.Fatalf("/backups/catalog/%s missing metadata: %s", bkpRec.ID, body)
	}

	// 6. Restore page (/backups/restore)
	resp, body = e.do(c, "GET", "/backups/restore", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/backups/restore returned %d", resp.StatusCode)
	}
	if !strings.Contains(body, "restore-form") {
		t.Fatalf("/backups/restore missing restore-form")
	}

	// 7. GET /api/v1/backup-jobs/{id}/runs
	resp, body = e.do(c, "GET", "/api/v1/backup-jobs/"+job.ID+"/runs", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("api runs returned %d: %s", resp.StatusCode, body)
	}
	var runsResp struct {
		Runs []struct {
			ID              string `json:"id"`
			Status          string `json:"status"`
			ErrorCode       string `json:"error_code"`
			RemediationHint string `json:"remediation_hint"`
		} `json:"runs"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(body), &runsResp); err != nil {
		t.Fatalf("unmarshal runs json: %v", err)
	}
	if len(runsResp.Runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runsResp.Runs))
	}
	foundFailed := false
	for _, r := range runsResp.Runs {
		if r.ErrorCode == backup.ErrCodeNetwork {
			foundFailed = true
			if r.RemediationHint == "" {
				t.Errorf("expected remediation hint for %s, got empty", r.ErrorCode)
			}
		}
	}
	if !foundFailed {
		t.Fatalf("failed run not found in API response")
	}

	// 8. POST /api/v1/backup-jobs/{id}/pause and resume
	resp, _ = e.do(c, "POST", "/api/v1/backup-jobs/"+job.ID+"/pause", "", h)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pause job failed: %d", resp.StatusCode)
	}
	jUpdated, _, _ := st.GetBackupJob(ctx, job.ID)
	if jUpdated.Enabled {
		t.Fatalf("expected job to be paused (Enabled=false)")
	}

	resp, _ = e.do(c, "POST", "/api/v1/backup-jobs/"+job.ID+"/resume", "", h)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume job failed: %d", resp.StatusCode)
	}
	jUpdated, _, _ = st.GetBackupJob(ctx, job.ID)
	if !jUpdated.Enabled {
		t.Fatalf("expected job to be resumed (Enabled=true)")
	}

	// 9. POST /api/v1/backup-runs/{id}/cancel
	// A) Non-existent run -> 404
	resp, _ = e.do(c, "POST", "/api/v1/backup-runs/nonexistent/cancel", "", h)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cancel non-existent run expected 404, got %d", resp.StatusCode)
	}

	// B) Non-running run -> 409
	resp, _ = e.do(c, "POST", "/api/v1/backup-runs/"+run1.ID+"/cancel", "", h)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("cancel completed run expected 409, got %d", resp.StatusCode)
	}

	// C) Running run but foreign/unowned process -> 409
	run3 := backup.Run{
		ID:        "brun-333",
		JobID:     job.ID,
		Trigger:   backup.TriggerManual,
		StartedAt: now,
		ProcessID: "foreign-proc-999",
		Status:    string(backup.RunStatusRunning),
	}
	if err := st.CreateBackupRun(ctx, run3); err != nil {
		t.Fatalf("start run 3: %v", err)
	}
	resp, body = e.do(c, "POST", "/api/v1/backup-runs/"+run3.ID+"/cancel", "", h)
	if resp.StatusCode != http.StatusConflict || !strings.Contains(body, "owned by another instance") {
		t.Fatalf("cancel foreign run expected 409, got %d %s", resp.StatusCode, body)
	}

	// D) Running run owned by this instance -> 200 and marked cancelled
	run3.ProcessID = "20261123T000000Z-deadbeef"
	const ownedPID = 4242
	e.srv.ownedBackupProcs.Store(run3.ProcessID, ownedPID)
	e.srv.SetJobRunner(&backupStopTestRunner{jobs: map[string]JobStatus{
		run3.ProcessID: {ID: run3.ProcessID, Pipeline: backupJobRunPipeline(job.ID), Running: true, PID: ownedPID},
	}})
	_, _ = st.UpdateBackupRun(ctx, run3)

	resp, body = e.do(c, "POST", "/api/v1/backup-runs/"+run3.ID+"/cancel", "", h)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel owned run failed: %d %s", resp.StatusCode, body)
	}
	run3Cancelled, _, _ := st.GetBackupRun(ctx, run3.ID)
	if run3Cancelled.Status != string(backup.RunStatusCancelled) {
		t.Fatalf("expected run status %s, got %s", backup.RunStatusCancelled, run3Cancelled.Status)
	}
	if run3Cancelled.ErrorCode != backup.ErrCodeCancelled {
		t.Fatalf("expected error code %s, got %s", backup.ErrCodeCancelled, run3Cancelled.ErrorCode)
	}
	// The reconciler restarts the killed scheduler; the cancelled run must not be immediately due again.
	if jc, _, _ := st.GetBackupJob(ctx, job.ID); jc.NextRunAt == nil || !jc.NextRunAt.After(e.srv.now()) {
		t.Fatalf("cancel left the job due now (next_run_at=%v): the restarted scheduler would rerun it", jc.NextRunAt)
	}
}

func TestBackupRBACViewerDenied(t *testing.T) {
	st := store.NewMem()
	ctx := context.Background()
	e := dbEnv(t, st, nil, nil)

	pw := "SecurePassword123!"
	h, err := HashPassword(pw)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}

	// Create a viewer user in the store
	now := time.Now().UTC()
	_, err = st.CreateUser(ctx, store.User{
		Username:     "alice_viewer",
		PasswordHash: h,
		Role:         store.RoleViewer,
		CreatedAt:    now,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Login as viewer
	c := e.client()
	e.do(c, "GET", "/login", "", nil)
	var lc string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}
	loginBody := `{"username":"alice_viewer","password":"` + pw + `"}`
	resp, respBody := e.do(c, "POST", "/api/v1/login", loginBody, map[string]string{
		csrfHeader:     lc,
		"Content-Type": "application/json",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login as viewer failed: %d %s", resp.StatusCode, respBody)
	}
	var parsed map[string]any
	_ = json.Unmarshal([]byte(respBody), &parsed)
	csrf, _ := parsed["csrf"].(string)
	hdr := map[string]string{"X-CSRF-Token": csrf, "Content-Type": "application/json"}

	// Verify viewer CAN view backup pages
	for _, path := range []string{"/backups", "/backups/jobs", "/backups/catalog", "/backups/restore"} {
		r, b := e.do(c, "GET", path, "", nil)
		if r.StatusCode != http.StatusOK {
			t.Errorf("viewer GET %s returned %d", path, r.StatusCode)
		}
		// Write buttons must NOT appear for viewer
		if strings.Contains(b, `data-action="new-job"`) || strings.Contains(b, `action="/api/v1/backup-jobs"`) {
			t.Errorf("viewer GET %s contained write action", path)
		}
	}

	// Verify viewer CANNOT write to operator/admin endpoints
	postRoutes := []string{
		"/api/v1/backup-runs/r-1/cancel",
		"/api/v1/backup-jobs/j-1/pause",
		"/api/v1/backup-jobs/j-1/resume",
		"/api/v1/backup-jobs/j-1/run",
	}
	for _, rt := range postRoutes {
		r, _ := e.do(c, "POST", rt, "{}", hdr)
		if r.StatusCode != http.StatusForbidden {
			t.Errorf("viewer POST %s expected 403 Forbidden, got %d", rt, r.StatusCode)
		}
	}
}
