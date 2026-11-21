package web

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// seedStatus creates a record and moves it to status (queued, uploading, failed or expired; complete via
// seedInRepo).
func seedStatus(t *testing.T, st store.Store, id, job string, at time.Time, status backup.Status) {
	t.Helper()
	ctx := context.Background()
	if status == backup.StatusComplete || status == backup.StatusExpired {
		seedInRepo(t, st, id, "", "mysql-prod", at)
		if status == backup.StatusExpired {
			cat, _ := backup.NewCatalog(st)
			if _, _, err := cat.Expire(ctx, id, at.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	cat, _ := backup.NewCatalog(st)
	r := backup.Record{ID: id, Source: "mysql-prod", Engine: "mysql", Tool: "mysqldump", Bucket: "central-backups", ObjectKey: "p/" + id,
		ManifestURI: "s3://central-backups/p/" + id + ".json", Status: backup.StatusQueued, CreatedAt: at, JobID: job, Method: backup.MethodMySQLDump}
	if _, _, err := cat.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if status == backup.StatusQueued {
		return
	}
	if _, _, err := cat.UpdateStatus(ctx, id, backup.StatusUploading, nil); err != nil {
		t.Fatal(err)
	}
	if status == backup.StatusFailed {
		if _, _, err := cat.UpdateStatus(ctx, id, backup.StatusFailed, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// Only complete restore points can be restored, verified, copied or put on legal hold: the API answers 409
// with the status named and starts nothing (before: a failed backup was accepted).
func TestIncompleteRestorePointsRefused(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	now := e.srv.now()
	seedStatus(t, e.st, "bkp-ok", "", now.Add(-3*time.Hour), backup.StatusComplete)
	seedStatus(t, e.st, "bkp-bad", "", now.Add(-2*time.Hour), backup.StatusFailed)
	seedStatus(t, e.st, "bkp-old", "", now.Add(-48*time.Hour), backup.StatusExpired)
	seedStatus(t, e.st, "bkp-up", "", now.Add(-time.Minute), backup.StatusUploading)
	repo := decode(t, a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Offsite", "http://127.0.0.1:1"), 200))["repository"].(map[string]any)["id"].(string)
	e.rec.take()

	for _, c := range []struct{ id, status string }{{"bkp-bad", "failed"}, {"bkp-old", "expired"}, {"bkp-up", "uploading"}} {
		want := func(verb string) string {
			return "restore point " + c.id + " is " + c.status + ": only complete restore points can be " + verb
		}
		out := a.must("POST", "/api/v1/backup-restores", map[string]any{"backup_id": c.id, "connection_id": "target",
			"target_database": "newdb", "confirm": "newdb"}, http.StatusConflict)
		if !strings.Contains(out, want("restored")) {
			t.Fatalf("restore %s: %s", c.id, out)
		}
		if out := a.must("POST", "/api/v1/backups/"+c.id+"/verify", nil, http.StatusConflict); !strings.Contains(out, want("verified")) {
			t.Fatalf("verify %s: %s", c.id, out)
		}
		if out := a.must("POST", "/api/v1/backups/"+c.id+"/copy", map[string]any{"repository": repo}, http.StatusConflict); !strings.Contains(out, want("copied")) {
			t.Fatalf("copy %s: %s", c.id, out)
		}
		if c.status != "expired" { // an expired record was already refused by the store
			if out := a.must("POST", "/api/v1/backups/"+c.id+"/legal-hold", map[string]any{"hold": true}, http.StatusConflict); !strings.Contains(out, want("put on legal hold")) {
				t.Fatalf("hold %s: %s", c.id, out)
			}
		}
	}
	if specs := e.rec.take(); len(specs) != 0 {
		t.Fatalf("processes started for incomplete restore points: %+v", specs)
	}
	for _, act := range []string{"backup.restore", "backup.verify", "backup.copy", "backup.legal_hold"} {
		if strings.Contains(e.auditText(), `"`+act+`"`) {
			t.Fatalf("refused %s was audited as started", act)
		}
	}
	// A complete restore point still works; releasing a hold is always allowed.
	a.must("POST", "/api/v1/backups/bkp-ok/verify", nil, http.StatusAccepted)
	a.must("POST", "/api/v1/backups/bkp-ok/copy", map[string]any{"repository": repo}, http.StatusAccepted)
	if _, _, err := e.st.SetBackupLegalHold(context.Background(), "bkp-bad", true); err != nil { // held before it failed
		t.Fatal(err)
	}
	a.must("POST", "/api/v1/backups/bkp-bad/legal-hold", map[string]any{"hold": false}, http.StatusOK)
	a.must("POST", "/api/v1/backups/missing/verify", nil, http.StatusNotFound)
}

// DELETE /api/v1/backups/{id}: operator+, only for records that are not complete, refused under legal hold,
// Object Lock or while the writer may still run; audited; runs `strand backup delete` with the repository
// credentials in the child's environment (never argv).
func TestDeleteRestorePointAPI(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	ctx := context.Background()
	now := e.srv.now()
	job := backup.Job{ID: "bjob-del", Name: "nightly", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-prod",
			Conn: backup.ConnSpec{Host: "db1", Port: 3306, User: "backup", PasswordEnv: "MYSQL_BACKUP_PASSWORD", Databases: []string{"shop"}}}}
	if err := e.st.CreateBackupJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	job, _, _ = e.st.GetBackupJob(ctx, job.ID)
	seedStatus(t, e.st, "bkp-ok", "", now.Add(-3*time.Hour), backup.StatusComplete)
	seedStatus(t, e.st, "bkp-bad", job.ID, now.Add(-2*time.Hour), backup.StatusFailed)
	seedStatus(t, e.st, "bkp-held", job.ID, now.Add(-2*time.Hour), backup.StatusFailed)
	seedStatus(t, e.st, "bkp-old", "", now.Add(-48*time.Hour), backup.StatusExpired)
	seedStatus(t, e.st, "bkp-run", job.ID, now.Add(-time.Minute), backup.StatusUploading)
	seedStatus(t, e.st, "bkp-push", "", now.Add(-time.Hour), backup.StatusQueued)
	if _, _, err := e.st.SetBackupLegalHold(ctx, "bkp-held", true); err != nil {
		t.Fatal(err)
	}
	a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Offsite", "http://127.0.0.1:1"), 200)
	e.rec.take()
	op, vw := e.roleClient(store.RoleOperator), e.roleClient(store.RoleViewer)

	vw.must("DELETE", "/api/v1/backups/bkp-bad", nil, http.StatusForbidden)
	for id, want := range map[string]string{
		"bkp-ok":   "restore point bkp-ok is complete: complete restore points are removed by the retention policy",
		"bkp-held": "restore point bkp-held is under legal hold",
		"bkp-push": "restore point bkp-push is queued: its uploader cannot be checked",
	} {
		if out := op.must("DELETE", "/api/v1/backups/"+id, nil, http.StatusConflict); !strings.Contains(out, want) {
			t.Fatalf("delete %s: %s", id, out)
		}
	}
	// The job is running: its uploading record is not abandoned.
	started, ok, err := e.st.BeginBackupJobRun(ctx, job.ID, job.UpdatedAt)
	if err != nil || !ok {
		t.Fatalf("begin run: %v %v", ok, err)
	}
	if out := op.must("DELETE", "/api/v1/backups/bkp-run", nil, http.StatusConflict); !strings.Contains(out, "a run of backup job bjob-del is still running") {
		t.Fatalf("delete running: %s", out)
	}
	op.must("DELETE", "/api/v1/backups/missing", nil, http.StatusNotFound)
	if specs := e.rec.take(); len(specs) != 0 {
		t.Fatalf("refused deletes started processes: %+v", specs)
	}

	// Failed and expired records: a `strand backup delete` process with the repository secrets in its env.
	op.must("DELETE", "/api/v1/backups/bkp-bad", nil, http.StatusAccepted)
	a.must("DELETE", "/api/v1/backups/bkp-old", nil, http.StatusAccepted)
	if _, err := e.st.FinishBackupJobRun(ctx, job.ID, started, backup.RunStatusFailed); err != nil {
		t.Fatal(err)
	}
	if j, _, _ := e.st.GetBackupJob(ctx, job.ID); j.LastRunStatus == backup.RunStatusRunning {
		t.Fatalf("run still marked running: %+v", j)
	}
	// The run is over: the uploading record is abandoned and deleted with --abandoned.
	op.must("DELETE", "/api/v1/backups/bkp-run", nil, http.StatusAccepted)
	specs := e.rec.take()
	if len(specs) != 3 {
		t.Fatalf("specs = %+v", specs)
	}
	wantArgs := []string{"backup delete --id bkp-bad", "backup delete --id bkp-old", "backup delete --id bkp-run --abandoned"}
	for i, sp := range specs {
		args := strings.Join(sp.Args, " ")
		if sp.Kind != "backup-delete" || !strings.HasSuffix(args, wantArgs[i]) || strings.Contains(args, canary) {
			t.Fatalf("spec %d = %s %q", i, sp.Kind, args)
		}
		if !strings.Contains(strings.Join(sp.Env, "\n"), "SK-"+canary) {
			t.Fatalf("spec %d lacks the stored repository secret in its environment", i)
		}
	}
	if n := strings.Count(e.auditText(), `"backup.delete"`); n != 3 {
		t.Fatalf("backup.delete audited %d times:\n%s", n, e.auditText())
	}
}

// The catalog list and detail pages show actions by status: complete -> Restore (and Verify/Copy/Legal hold
// on the detail page); failed -> View error + Delete; expired -> Delete; in progress -> none. The timeline
// never links a record that is not complete to the restore wizard, and the wizard refuses one in ?backup=.
func TestRestorePointPagesByStatus(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	ctx := context.Background()
	now := e.srv.now()
	job := backup.Job{ID: "bjob-pg", Name: "nightly", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-prod",
			Conn: backup.ConnSpec{Host: "db1", Port: 3306, User: "backup", PasswordEnv: "MYSQL_BACKUP_PASSWORD", Databases: []string{"shop"}}}}
	if err := e.st.CreateBackupJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	seedStatus(t, e.st, "bkp-ok", "", now.Add(-3*time.Hour), backup.StatusComplete)
	seedStatus(t, e.st, "bkp-bad", job.ID, now.Add(-2*time.Hour), backup.StatusFailed)
	seedStatus(t, e.st, "bkp-old", "", now.Add(-48*time.Hour), backup.StatusExpired)
	seedStatus(t, e.st, "bkp-up", "", now.Add(-time.Minute), backup.StatusUploading)
	fin := now.Add(-2*time.Hour + time.Minute)
	if err := e.st.CreateBackupRun(ctx, backup.Run{ID: "run-1", JobID: job.ID, StartedAt: now.Add(-2*time.Hour - time.Second), FinishedAt: &fin,
		Status: "failed", ErrorCode: "tool_failed", ErrorMessage: "mysqldump: Got error 2013: Lost connection"}); err != nil {
		t.Fatal(err)
	}

	page := func(path string) string {
		t.Helper()
		code, body := a.do("GET", path, nil)
		if code != 200 {
			t.Fatalf("GET %s = %d", path, code)
		}
		return body
	}
	row := func(body, id string) string {
		t.Helper()
		m := regexp.MustCompile(`(?s)<tr data-backup-id="` + id + `">.*?</tr>`).FindString(body)
		if m == "" {
			t.Fatalf("no row for %s", id)
		}
		return m
	}
	list := page("/backups/catalog")
	if r := row(list, "bkp-ok"); !strings.Contains(r, `/backups/restore?backup=bkp-ok`) || strings.Contains(r, `data-backup-action="delete"`) {
		t.Fatalf("complete row: %s", r)
	}
	if r := row(list, "bkp-bad"); strings.Contains(r, `/backups/restore?backup=`) || !strings.Contains(r, `data-rp-action="view-error"`) ||
		!strings.Contains(r, `data-backup-action="delete"`) || !strings.Contains(r, `data-confirm="Delete restore point bkp-bad`) {
		t.Fatalf("failed row: %s", r)
	}
	if r := row(list, "bkp-old"); strings.Contains(r, `/backups/restore?backup=`) || !strings.Contains(r, `data-backup-action="delete"`) || strings.Contains(r, "view-error") {
		t.Fatalf("expired row: %s", r)
	}
	if r := row(list, "bkp-up"); strings.Contains(r, `/backups/restore?backup=`) || strings.Contains(r, "data-backup-action") || !strings.Contains(r, `title="In progress"`) {
		t.Fatalf("uploading row: %s", r)
	}
	for _, id := range []string{"bkp-bad", "bkp-old", "bkp-up"} {
		if strings.Contains(list, `/backups/restore?backup=`+id) {
			t.Fatalf("catalog page (timeline) links %s to the restore wizard", id)
		}
	}

	detail := page("/backups/catalog/bkp-bad")
	if strings.Contains(detail, "/backups/restore?backup=bkp-bad") || strings.Contains(detail, `data-backup-action="verify"`) ||
		strings.Contains(detail, "copy-drawer") || !strings.Contains(detail, `id="backup-error-card"`) ||
		!strings.Contains(detail, "Lost connection") || !strings.Contains(detail, `data-backup-action="delete"`) ||
		!strings.Contains(detail, "/backups/jobs/bjob-pg?tab=runs") {
		t.Fatalf("failed detail page:\n%s", detail)
	}
	detail = page("/backups/catalog/bkp-up")
	if strings.Contains(detail, "/backups/restore?backup=bkp-up") || strings.Contains(detail, "data-backup-action") || !strings.Contains(detail, "still running") {
		t.Fatalf("uploading detail page:\n%s", detail)
	}
	detail = page("/backups/catalog/bkp-ok")
	for _, want := range []string{"/backups/restore?backup=bkp-ok", `data-backup-action="verify"`, "copy-drawer", `data-backup-action="legal-hold"`} {
		if !strings.Contains(detail, want) {
			t.Fatalf("complete detail lacks %s", want)
		}
	}
	if strings.Contains(detail, `data-backup-action="delete"`) || strings.Contains(detail, "backup-error-card") {
		t.Fatal("complete detail offers delete or an error")
	}

	// Restore wizard: a failed backup in ?backup= is reported and not preselected; only complete ones are listed.
	wiz := page("/backups/restore?backup=bkp-bad")
	if !strings.Contains(wiz, `id="restore-selected-error"`) || !strings.Contains(wiz, "restore point bkp-bad is failed: only complete restore points can be restored") ||
		strings.Contains(wiz, `value="bkp-bad"`) || strings.Contains(wiz, `<option value="bkp-bad"`) || !strings.Contains(wiz, `<option value="bkp-ok"`) {
		t.Fatalf("restore wizard with a failed backup:\n%s", wiz)
	}
	if wiz := page("/backups/restore?backup=bkp-ok"); strings.Contains(wiz, "restore-selected-error") || !strings.Contains(wiz, `value="bkp-ok"`) {
		t.Fatal("restore wizard rejects a complete backup")
	}

	// Vietnamese strings for the new actions.
	resp, _ := e.do(a.c, "POST", "/account/preferences", url.Values{"language": {LangVI}}.Encode(), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded", "X-CSRF-Token": a.hdr["X-CSRF-Token"]})
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("set language: %d", resp.StatusCode)
	}
	if r := row(page("/backups/catalog"), "bkp-bad"); !strings.Contains(r, "Xem lỗi") || !strings.Contains(r, ">Xóa<") || !strings.Contains(r, "Xóa điểm khôi phục bkp-bad") {
		t.Fatalf("vi failed row: %s", r)
	}
}
