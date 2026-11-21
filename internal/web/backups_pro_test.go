package web

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func seedComplete(t *testing.T, st store.Store, id, job string, at time.Time, kind string) {
	t.Helper()
	ctx := context.Background()
	cat, _ := backup.NewCatalog(st)
	r := backup.Record{ID: id, Source: "mysql-prod", Engine: "mysql", Tool: "mysqldump", Bucket: "central-backups",
		ObjectKey: "p/" + id, ManifestURI: "s3://central-backups/p/" + id + ".json", Status: backup.StatusQueued, CreatedAt: at,
		JobID: job, Method: backup.MethodMySQLDump, Kind: kind}
	if _, _, err := cat.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusUploading, nil)
	start := at
	if _, _, err := cat.UpdateMeta(ctx, id, "mysql", backup.Meta{Position: backup.Position{GTIDSet: "u:1-5"}, StartTime: &start,
		Format: backup.FormatSBK1, KeyID: "k1", Compression: "zstd"}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
	if _, _, err := cat.Finalize(ctx, id, backup.StatusVerifying, 2048, strings.Repeat("c", 64), r.ManifestURI, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func TestManagedBackupWebAPIs(t *testing.T) {
	st := store.NewMem()
	now := time.Now().UTC().Truncate(time.Second)
	job := backup.Job{ID: "bjob-web", Name: "mysql nightly", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-prod", Compress: true, Encrypt: true,
			Conn:      backup.ConnSpec{Host: "db1", Port: 3306, User: "backup", PasswordEnv: "MYSQL_BACKUP_PASSWORD", Databases: []string{"shop"}},
			Retention: backup.RetentionPolicy{KeepLast: 2}}}
	if err := st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		seedComplete(t, st, "bkp-w"+string(rune('a'+i)), job.ID, now.Add(-time.Duration(4-i)*time.Hour), backup.KindFull)
	}
	e := dbEnv(t, st, nil, nil)
	waitIdle(t, e, "backup-prune-bjob-web", "backup-bjob-web")
	c, csrf := e.loginOK()
	h := map[string]string{"X-CSRF-Token": csrf, "Content-Type": "application/json"}

	// Pages: the dashboard, the restore points (with their Restore links), the jobs, the restore wizard and the
	// repositories each have their own page; all CSP-safe.
	for path, wants := range map[string][]string{
		"/backups":              {"Backup dashboard", "mysql-prod", "<polyline"},
		"/backups/catalog":      {"mysql-prod", "/backups/restore?backup="},
		"/backups/jobs":         {"mysqldump"},
		"/backups/restore":      {"restore-form"},
		"/backups/repositories": {"bk-repo-form", "Retention (automatic deletion)", "Test connection"},
	} {
		resp, body := e.do(c, "GET", path, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
		for _, want := range wants {
			if !strings.Contains(body, want) {
				t.Fatalf("%s lacks %q", path, want)
			}
		}
		if strings.Contains(body, " style=") || strings.Contains(body, "<style") || strings.Contains(body, "<script>") || strings.Contains(body, "onclick=") {
			t.Fatalf("inline style/script on %s", path)
		}
	}

	// Summary API.
	resp, body := e.do(c, "GET", "/api/v1/backups/summary", "", nil)
	var sum struct {
		Sources []recoveryView `json:"sources"`
	}
	if resp.StatusCode != 200 || json.Unmarshal([]byte(body), &sum) != nil || len(sum.Sources) != 1 || sum.Sources[0].LastSuccess == nil ||
		sum.Sources[0].LastSuccess.ID != "bkp-wd" || sum.Sources[0].Complete != 4 || len(sum.Sources[0].RecentSizes) != 4 {
		t.Fatalf("summary %d %s", resp.StatusCode, body)
	}

	// Retention preview: saved policy keeps 2; an edited policy in the body is previewed without saving.
	resp, body = e.do(c, "POST", "/api/v1/backup-jobs/"+job.ID+"/retention-preview", "", h)
	var prev struct {
		Expire    int               `json:"expire"`
		Keep      int               `json:"keep"`
		Decisions []backup.Decision `json:"decisions"`
	}
	if resp.StatusCode != 200 || json.Unmarshal([]byte(body), &prev) != nil || prev.Keep != 2 || prev.Expire != 2 {
		t.Fatalf("preview %d %s", resp.StatusCode, body)
	}
	resp, body = e.do(c, "POST", "/api/v1/backup-jobs/"+job.ID+"/retention-preview", `{"keep_last":3}`, h)
	if json.Unmarshal([]byte(body), &prev) != nil || prev.Keep != 3 {
		t.Fatalf("edited preview %s", body)
	}
	if resp, _ := e.do(c, "POST", "/api/v1/backup-jobs/"+job.ID+"/retention-preview", `{"keep_last":-1}`, h); resp.StatusCode != 400 {
		t.Fatalf("negative policy accepted: %d", resp.StatusCode)
	}

	// Prune and verify start audited job processes running `strand backup ...`.
	resp, body = e.do(c, "POST", "/api/v1/backup-jobs/"+job.ID+"/prune", "", h)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("prune %d %s", resp.StatusCode, body)
	}
	if len(e.auditFor("backup_job.prune")) != 1 {
		t.Fatal("prune not audited")
	}

	// Legal hold.
	resp, body = e.do(c, "POST", "/api/v1/backups/bkp-wa/legal-hold", `{"hold":true}`, h)
	if resp.StatusCode != 200 || !strings.Contains(body, `"legal_hold": true`) || len(e.auditFor("backup.legal_hold")) != 1 {
		t.Fatalf("hold %d %s", resp.StatusCode, body)
	}
	resp, body = e.do(c, "POST", "/api/v1/backup-jobs/"+job.ID+"/retention-preview", "", h)
	if !strings.Contains(body, "legal hold") {
		t.Fatalf("preview must show the hold: %s", body)
	}

	// Repositories: env prefix restricted (Strand's own secrets can never be named); CRUD audited.
	if resp, _ := e.do(c, "POST", "/api/v1/backup-repositories", `{"name":"x","env_prefix":"STRAND_WEB_","bucket":"offsite-b"}`, h); resp.StatusCode != 400 {
		t.Fatalf("reserved prefix accepted: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "POST", "/api/v1/backup-repositories", `{"name":"Offsite","env_prefix":"STRAND_BACKUP_REPO_OFFSITE_","bucket":"offsite-b"}`, h); resp.StatusCode != 400 {
		t.Fatalf("new environment repository accepted: %d", resp.StatusCode)
	}
	resp, body = e.do(c, "POST", "/api/v1/backup-repositories", `{"name":"Offsite","endpoint":"https://s3.example.com","access_key":"ak","secret_key":"sk","bucket":"offsite-b","lock_mode":"COMPLIANCE","lock_days":30}`, h)
	if resp.StatusCode != 200 || !strings.Contains(body, `"id": "offsite"`) {
		t.Fatalf("repo create %d %s", resp.StatusCode, body)
	}
	resp, body = e.do(c, "GET", "/api/v1/backup-repositories", "", nil)
	if !strings.Contains(body, `"configured": true`) || !strings.Contains(body, "COMPLIANCE") {
		t.Fatalf("repos %s", body)
	}
	if resp, _ := e.do(c, "POST", "/api/v1/backups/bkp-wa/copy", `{"repository":"nope"}`, h); resp.StatusCode != 400 {
		t.Fatalf("copy to unknown repo: %d", resp.StatusCode)
	}

	// Restore wizard: confirmation and a single new target are required; secrets are env names only.
	bad := `{"backup_id":"bkp-wd","target":{"host":"db2"},"target_database":"shop_restored","confirm":"shop"}`
	if resp, _ := e.do(c, "POST", "/api/v1/backup-restores", bad, h); resp.StatusCode != 400 {
		t.Fatalf("unconfirmed restore: %d", resp.StatusCode)
	}
	both := `{"backup_id":"bkp-wd","target":{"host":"db2"},"target_database":"a","target_dir":"/x","confirm":"a/x"}`
	if resp, _ := e.do(c, "POST", "/api/v1/backup-restores", both, h); resp.StatusCode != 400 {
		t.Fatalf("two targets: %d", resp.StatusCode)
	}
	reserved := `{"source":"mysql-prod","target_time":"2026-11-20T10:00:00Z","target":{"host":"db2","password_env":"STRAND_WEB_SECRET_KEY"},"target_database":"r1","confirm":"r1"}`
	if resp, _ := e.do(c, "POST", "/api/v1/backup-restores", reserved, h); resp.StatusCode != 400 {
		t.Fatalf("reserved password env accepted: %d", resp.StatusCode)
	}
	// Seed a continuous PITR window for mysql-prod covering target_time 2026-11-20T10:00:00Z
	tt, _ := time.Parse(time.RFC3339, "2026-11-20T10:00:00Z")
	tBase := tt.Add(-2 * time.Hour)
	tLogStart := tt.Add(-3 * time.Hour)
	tLogEnd := tt.Add(2 * time.Hour)
	seedRichBackup(t, st, "bkp-pitr-base", "mysql-prod", "mysql", "mysqldump", backup.KindFull, tBase, backup.Position{GTIDSet: "u:1-100"}, false, nil, "", &tBase, nil, "", nil)
	seedRichBackup(t, st, "bkp-pitr-log", "mysql-prod", "mysql", "binlog", backup.KindLog, tLogEnd, backup.Position{}, false, nil, "", &tLogStart, &tLogEnd, "", map[string]string{"seq": "1"})

	resp, body = e.do(c, "POST", "/api/v1/backup-connections", `{"name":"restore-target","engine":"mysql","host":"db2","port":3307,"user":"restore","password":"restore-canary"}`, h)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("restore target connection %d %s", resp.StatusCode, body)
	}
	var tc struct {
		Connection struct {
			ID string `json:"id"`
		} `json:"connection"`
	}
	_ = json.Unmarshal([]byte(body), &tc)
	good := `{"source":"mysql-prod","target_time":"2026-11-20T10:00:00Z","connection_id":"` + tc.Connection.ID + `","target_database":"shop_pitr","confirm":"shop_pitr"}`
	resp, body = e.do(c, "POST", "/api/v1/backup-restores", good, h)
	if resp.StatusCode != http.StatusAccepted || len(e.auditFor("backup.restore")) != 1 {
		t.Fatalf("restore %d %s", resp.StatusCode, body)
	}
	var pid struct {
		ProcessID string `json:"process_id"`
	}
	_ = json.Unmarshal([]byte(body), &pid)
	waitFor(t, "restore process recorded", func() bool { _, ok := e.srv.jobs.Get(pid.ProcessID); return ok })
	js, _ := e.srv.jobs.Get(pid.ProcessID)
	waitIdle(t, e, js.Pipeline)
	// A second restore runs concurrently (own pipeline), e.g. MySQL and PostgreSQL at the same time.
	second := `{"source":"mysql-prod","target_time":"2026-11-20T10:00:00Z","connection_id":"` + tc.Connection.ID + `","target_database":"shop_pitr2","confirm":"shop_pitr2"}`
	resp2, body2 := e.do(c, "POST", "/api/v1/backup-restores", second, h)
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("concurrent restore refused: %d %s", resp2.StatusCode, body2)
	}
	var pid2 struct {
		ProcessID string `json:"process_id"`
	}
	_ = json.Unmarshal([]byte(body2), &pid2)
	if js2, ok := e.srv.jobs.Get(pid2.ProcessID); ok {
		waitIdle(t, e, js2.Pipeline)
	}
	args := strings.Join(js.Args, " ")
	if !strings.Contains(args, "backup restore --yes --source mysql-prod --to-time 2026-11-20T10:00:00Z") ||
		!strings.Contains(args, "--target-host db2 --target-database shop_pitr") || !strings.Contains(args, "--target-password-env STRAND_RESTORE_TARGET_PASSWORD") ||
		strings.Contains(args, "restore-canary") {
		t.Fatalf("restore argv %q", args)
	}

	// Metrics: Prometheus text with the per-source and per-job gauges; requires authentication.
	if resp, _ := e.do(e.client(), "GET", "/metrics", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous metrics %d", resp.StatusCode)
	}
	resp, body = e.do(c, "GET", "/metrics", "", nil)
	for _, want := range []string{`strand_backup_last_success_timestamp_seconds{source="mysql-prod",engine="mysql"}`,
		`strand_backup_last_size_bytes{source="mysql-prod"} 2048`, `strand_backup_job_consecutive_failures{job="bjob-web",name="mysql nightly"} 0`,
		"# TYPE strand_backup_recent_check_failures gauge"} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %q:\n%s", want, body)
		}
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
		t.Fatal("metrics content type")
	}
}

// The maintenance tick, on the active instance: hourly retention once per job across ticks (claim), a
// missed schedule notified exactly once, finished runs notified once.
func TestBackupMaintenanceTick(t *testing.T) {
	st := store.NewMem()
	job := backup.Job{ID: "bjob-m", Name: "pg", EverySeconds: 3600, Bucket: "central-backups", Enabled: true,
		JobOptions: backup.JobOptions{Method: backup.MethodPGDump, Source: "pg-prod",
			Conn:      backup.ConnSpec{Host: "db1", Databases: []string{"app"}},
			Retention: backup.RetentionPolicy{KeepDaily: 7}}}
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC) // before the test clock
	job.NextRunAt = &past
	var got []string
	srv := newHookServer(t, func(b string) { got = append(got, b) })
	t.Setenv("STRAND_BACKUP_NOTIFY_WEBHOOK", srv)
	e := dbEnv(t, st, nil, nil)
	waitIdle(t, e, "backup-prune-bjob-m")
	// Created after start-up: no scheduler process is started for it in this test.
	if err := st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	e.srv.backupMaintenanceTick(context.Background())
	e.srv.backupMaintenanceTick(context.Background())
	prunes := 0
	for _, p := range e.srv.jobs.List("backup-prune-" + job.ID) {
		if p.Kind == "backup-prune" {
			prunes++
		}
	}
	if prunes != 1 {
		t.Fatalf("retention started %d times, want 1", prunes)
	}
	waitFor(t, "missed-schedule notification", func() bool { return len(hookBodies()) >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := len(hookBodies()); n != 1 || !strings.Contains(hookBodies()[0], `"event":"missed"`) {
		t.Fatalf("notifications %v", hookBodies())
	}
	if len(e.auditFor("backup.notify")) == 0 {
		t.Fatal("notification not audited")
	}
	_ = got
}

var hookMu sync.Mutex
var hookGot []string

func hookBodies() []string {
	hookMu.Lock()
	defer hookMu.Unlock()
	return append([]string(nil), hookGot...)
}

func newHookServer(t *testing.T, _ func(string)) string {
	hookMu.Lock()
	hookGot = nil
	hookMu.Unlock()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		hookMu.Lock()
		hookGot = append(hookGot, string(b))
		hookMu.Unlock()
	}))
	t.Cleanup(s.Close)
	return s.URL
}

// waitIdle waits until the fake strand processes of the given pipelines have exited (they write into the
// test's state directory).
func waitIdle(t *testing.T, e *testEnv, pipelines ...string) {
	t.Cleanup(func() {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			busy := false
			for _, p := range pipelines {
				for _, j := range e.srv.jobs.List(p) {
					busy = busy || j.Running
				}
			}
			if !busy {
				time.Sleep(100 * time.Millisecond)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	})
}

func TestStoredUnsupportedBackupJobBehaviour(t *testing.T) {
	st := store.NewMem()
	jobXtra := backup.Job{
		ID:           "bjob-xtra",
		Name:         "old physical job",
		EverySeconds: 3600,
		Bucket:       "central-backups",
		Enabled:      true,
		JobOptions: backup.JobOptions{
			Method: "xtrabackup",
			Source: "mysql-prod",
			Conn: backup.ConnSpec{
				Host:        "db1",
				Port:        3306,
				User:        "backup",
				PasswordEnv: "MYSQL_PW",
				Datadir:     "/var/lib/mysql",
			},
		},
	}
	st.SeedBackupJob(jobXtra)
	jobMaria := backup.Job{
		ID:           "bjob-maria",
		Name:         "old mariabackup job",
		EverySeconds: 3600,
		Bucket:       "central-backups",
		Enabled:      true,
		JobOptions: backup.JobOptions{
			Method: "mariabackup",
			Source: "mariadb-prod",
			Conn: backup.ConnSpec{
				Host:        "db2",
				Port:        3306,
				User:        "backup",
				PasswordEnv: "MARIA_PW",
				Datadir:     "/var/lib/mysql",
			},
		},
	}
	st.SeedBackupJob(jobMaria)

	// 1. Starting the web server and resuming schedulers does NOT crash.
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	h := map[string]string{"X-CSRF-Token": csrf, "Content-Type": "application/json"}

	// 2. The backup jobs page shows it as "unsupported method (removed)".
	resp, body := e.do(c, "GET", "/backups/jobs", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GET /backups/jobs status %d", resp.StatusCode)
	}
	if !strings.Contains(body, "unsupported method (removed)") {
		t.Fatalf("expected UI to show 'unsupported method (removed)', got:\n%s", body)
	}

	// 3. Triggering 'Run now' is refused with a clear error.
	resp, body = e.do(c, "POST", "/api/v1/backup-jobs/"+jobXtra.ID+"/run", "", h)
	if resp.StatusCode != 400 {
		t.Fatalf("run status %d (expected 400)", resp.StatusCode)
	}
	if !strings.Contains(body, "unsupported") || !strings.Contains(body, "removed") {
		t.Fatalf("expected clear unsupported error, got %s", body)
	}

	// 4. Triggering 'Start' is refused with a clear error.
	resp, body = e.do(c, "POST", "/api/v1/backup-jobs/"+jobXtra.ID+"/start", "", h)
	if resp.StatusCode != 400 {
		t.Fatalf("start status %d (expected 400)", resp.StatusCode)
	}
	if !strings.Contains(body, "unsupported") || !strings.Contains(body, "removed") {
		t.Fatalf("expected clear unsupported error, got %s", body)
	}

	// 5. Editing the job to a supported method (mysqldump) succeeds.
	updateReq := `{
		"name": "migrated job",
		"method": "mysqldump",
		"every_seconds": 3600,
		"bucket": "central-backups",
		"source": "mysql-prod",
		"conn": {
			"host": "db1",
			"port": 3306,
			"user": "backup",
			"password_env": "MYSQL_PW",
			"databases": ["shop"]
		}
	}`
	resp, body = e.do(c, "PUT", "/api/v1/backup-jobs/"+jobXtra.ID, updateReq, h)
	if resp.StatusCode != 200 {
		t.Fatalf("update status %d %s", resp.StatusCode, body)
	}
	updated, found, err := st.GetBackupJob(context.Background(), jobXtra.ID)
	if err != nil || !found || updated.Method != "mysqldump" {
		t.Fatalf("expected method mysqldump, got %+v (err: %v)", updated, err)
	}

	// 6. Deleting the unsupported job succeeds.
	resp, body = e.do(c, "DELETE", "/api/v1/backup-jobs/"+jobMaria.ID, "", h)
	if resp.StatusCode != 204 {
		t.Fatalf("delete status %d %s", resp.StatusCode, body)
	}
	_, found, err = st.GetBackupJob(context.Background(), jobMaria.ID)
	if err != nil || found {
		t.Fatalf("expected job to be deleted, found=%v err=%v", found, err)
	}
}
