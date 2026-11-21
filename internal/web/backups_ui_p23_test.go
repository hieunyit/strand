package web

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
)

func TestRetentionWords(t *testing.T) {
	cases := []struct {
		r    backup.RetentionPolicy
		want string
	}{
		{backup.RetentionPolicy{}, "Keep every backup"},
		{backup.RetentionPolicy{KeepDaily: 7, KeepWeekly: 4, KeepMonthly: 12}, "Keep 7 daily, 4 weekly, 12 monthly backups"},
		{backup.RetentionPolicy{KeepLast: 3}, "Keep 3 most recent backups"},
		{backup.RetentionPolicy{KeepDaily: 14, KeepWithinDays: 2, MaxAgeDays: 400}, "Keep 14 daily backups and every backup of the last 2 days; expire anything older than 400 days"},
		{backup.RetentionPolicy{KeepWithinDays: 5}, "Keep every backup of the last 5 days"},
	}
	for _, c := range cases {
		if got := retentionWords(c.r); got != c.want {
			t.Errorf("retentionWords(%+v) = %q, want %q", c.r, got, c.want)
		}
	}
}

// pageChecks asserts the shared console skeleton and the CSP rules on a rendered page.
func pageChecks(t *testing.T, path, body string) {
	t.Helper()
	for _, want := range []string{`class="breadcrumb"`, `class="pagehead"`, `<h1>`, `class="card`} {
		if !strings.Contains(body, want) {
			t.Errorf("%s lacks %s", path, want)
		}
	}
	for _, bad := range []string{` style="`, `<style`, `onclick=`, `onload=`} {
		if strings.Contains(body, bad) {
			t.Errorf("%s contains %q (CSP)", path, bad)
		}
	}
}

func seedLegacyJob(t *testing.T, e *cpEnv, id, host string) {
	t.Helper()
	j := backup.Job{ID: id, Name: id, Enabled: false, EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "legacy-src",
			Conn: backup.ConnSpec{Host: host, Port: 3306, User: "backup", PasswordEnv: "LEGACY_DB_PW", Databases: []string{"shop"}}}}
	if err := e.st.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
}

func TestBackupConnectionsAndPoliciesPagesRender(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	id := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod-mysql"), 201))
	a.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 200)
	j := decode(t, a.must("POST", "/api/v1/backup-jobs", mysqlJobBody("uses-conn", id), 201))["job"].(map[string]any)["id"].(string)

	body := a.must("GET", "/backups/connections", nil, 200)
	pageChecks(t, "/backups/connections", body)
	for _, want := range []string{"prod-mysql", "New connection", `dialog class="drawer"`, `name="secret_mode"`, `value="stored"`, "Test connection", "data-table", "<caption", "db1.internal:3306"} {
		if !strings.Contains(body, want) {
			t.Errorf("connections page lacks %q", want)
		}
	}
	if strings.Contains(body, canary) {
		t.Fatal("connections page leaks the secret")
	}
	if b := a.must("GET", "/backups/connections?engine=postgres", nil, 200); strings.Contains(b, "prod-mysql") {
		t.Error("engine filter did not filter")
	}
	if b := a.must("GET", "/backups/connections?q=prod", nil, 200); !strings.Contains(b, "prod-mysql") {
		t.Error("search filter lost the connection")
	}

	det := a.must("GET", "/backups/connections/"+id, nil, 200)
	pageChecks(t, "connection detail", det)
	for _, want := range []string{`class="detail-layout"`, `class="tabs"`, "Used by", "stored encrypted", "Edit connection"} {
		if !strings.Contains(det, want) {
			t.Errorf("connection detail lacks %q", want)
		}
	}
	if strings.Contains(det, canary) {
		t.Fatal("connection detail leaks the secret")
	}
	used := a.must("GET", "/backups/connections/"+id+"?tab=used-by", nil, 200)
	if !strings.Contains(used, "/backups/jobs/"+j) || !strings.Contains(used, "uses-conn") {
		t.Errorf("Used by tab does not list the job: %s", used)
	}
	a.must("GET", "/backups/connections/nope", nil, 404)

	pol := a.must("GET", "/backups/policies", nil, 200)
	pageChecks(t, "/backups/policies", pol)
	for _, want := range []string{"Gold", "Silver", "Bronze", "Keep 14 daily, 8 weekly, 12 monthly backups", "New policy", "data-schedule", "data-retention-words", "Daily 01:00 UTC"} {
		if !strings.Contains(pol, want) {
			t.Errorf("policies page lacks %q", want)
		}
	}
	gold := a.must("GET", "/backups/policies/policy-gold", nil, 200)
	pageChecks(t, "policy detail", gold)
	for _, want := range []string{"Next 5 runs", "Used by", `data-policy-json=`, "Edit policy"} {
		if !strings.Contains(gold, want) {
			t.Errorf("policy detail lacks %q", want)
		}
	}
	a.must("GET", "/backups/policies/nope", nil, 404)
}

func TestBackupUIRBAC(t *testing.T) {
	e := newCPEnv(t)
	admin := e.admin()
	id := connID(t, admin.must("POST", "/api/v1/backup-connections", storedConn("c1"), 201))
	seedLegacyJob(t, e, "legacy-1", "db.legacy")
	viewer, operator := e.roleClient("viewer"), e.roleClient("operator")

	for _, p := range []string{"/backups/connections", "/backups/connections/" + id, "/backups/policies", "/backups/policies/policy-silver", "/backups/jobs"} {
		for name, c := range map[string]*apiClient{"viewer": viewer, "operator": operator} {
			b := c.must("GET", p, nil, 200)
			for _, forbidden := range []string{`dialog class="drawer"`, "New connection", "New policy", `href="/backups/jobs/new"`, "data-convert-job", `data-conn-action="delete"`, `data-policy-action="delete"`, "Edit connection", "Edit policy"} {
				if strings.Contains(b, forbidden) {
					t.Errorf("%s GET %s shows write control %q", name, p, forbidden)
				}
			}
		}
	}
	// The operator may test a connection (a probe), the viewer may not.
	if b := operator.must("GET", "/backups/connections", nil, 200); !strings.Contains(b, `data-conn-action="test"`) {
		t.Error("operator lacks the Test button")
	}
	if b := viewer.must("GET", "/backups/connections", nil, 200); strings.Contains(b, `data-conn-action="test"`) {
		t.Error("viewer sees the Test button")
	}
	operator.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 200)
	viewer.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 403)
	// The job wizard writes, so it is admin only.
	for _, p := range []string{"/backups/jobs/new", "/backups/jobs/legacy-1/edit"} {
		viewer.must("GET", p, nil, 403)
		operator.must("GET", p, nil, 403)
		admin.must("GET", p, nil, 200)
	}
	// Convert to connection is an admin write.
	viewer.must("POST", "/api/v1/backup-jobs/legacy-1/convert-connection", nil, 403)
	operator.must("POST", "/api/v1/backup-jobs/legacy-1/convert-connection", nil, 403)
	// The admin sees the controls.
	if b := admin.must("GET", "/backups/jobs", nil, 200); !strings.Contains(b, `href="/backups/jobs/new"`) || !strings.Contains(b, "data-convert-job") {
		t.Errorf("admin jobs list lacks New job or Convert: %s", b)
	}
	if b := operator.must("GET", "/backups/jobs", nil, 200); !strings.Contains(b, `data-job-action="run"`) || strings.Contains(b, `data-job-action="delete"`) {
		t.Error("operator controls on the jobs list are wrong")
	}
}

func TestConvertConnection(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	seedLegacyJob(t, e, "legacy-a", "db.legacy")
	seedLegacyJob(t, e, "legacy-b", "db.legacy")
	out := a.must("POST", "/api/v1/backup-jobs/legacy-a/convert-connection", nil, 200)
	res := decode(t, out)
	conn := res["connection"].(map[string]any)
	if res["reused"] != false || conn["secret_mode"] != "env" || conn["password_env"] != "LEGACY_DB_PW" || conn["host"] != "db.legacy" || conn["engine"] != "mysql" {
		t.Fatalf("convert = %s", out)
	}
	j, _, _ := e.st.GetBackupJob(context.Background(), "legacy-a")
	if j.ConnectionID != conn["id"] || len(j.Conn.Databases) != 1 || j.Conn.Host != "db.legacy" || j.Conn.PasswordEnv != "LEGACY_DB_PW" {
		t.Fatalf("job after convert = %+v", j)
	}
	// A second legacy job on the same source reuses the connection.
	res2 := decode(t, a.must("POST", "/api/v1/backup-jobs/legacy-b/convert-connection", nil, 200))
	if res2["reused"] != true || res2["connection"].(map[string]any)["id"] != conn["id"] {
		t.Fatalf("second convert = %v", res2)
	}
	a.must("POST", "/api/v1/backup-jobs/legacy-a/convert-connection", nil, 409) // already converted
	a.must("POST", "/api/v1/backup-jobs/nope/convert-connection", nil, 404)
	cs, _ := e.st.ListBackupConnections(context.Background())
	if len(cs) != 1 {
		t.Fatalf("connections = %d, want 1", len(cs))
	}
	if audit := e.auditText(); !strings.Contains(audit, "backup_job.convert_connection") || strings.Contains(audit, "LEGACY_DB_PW=") {
		t.Fatalf("audit = %s", audit)
	}
	// A custom job cannot be converted.
	cj := backup.Job{ID: "custom-1", Name: "custom-1", EverySeconds: 3600, Command: "/bin/true", ArtifactTemplate: "/a/{id}", ManifestTemplate: "/m/{id}", Bucket: "bucket-x"}
	if err := e.st.CreateBackupJob(context.Background(), cj); err != nil {
		t.Fatal(err)
	}
	a.must("POST", "/api/v1/backup-jobs/custom-1/convert-connection", nil, 400)
}

func TestWizardPagesAndData(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	t.Setenv(backup.EnvEncryptionKeys, "k1:"+key)
	e := newCPEnv(t)
	a := e.admin()
	connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("wiz-conn"), 201))
	seedLegacyJob(t, e, "legacy-w", "db.w")
	body := a.must("GET", "/backups/jobs/new", nil, 200)
	pageChecks(t, "/backups/jobs/new", body)
	for _, want := range []string{`ol class="stepper"`, `aria-current="step"`, `id="job-wizard-form"`, "Source", "Schedule and retention", "Storage and protection", "Verify and review",
		"Create and run first backup", "Create only", `id="wiz-summary"`, `id="wiz-review"`, "Advanced: custom script", "Test connection", `dialog class="drawer"`,
		"wiz-conn", "policy-silver", `&#34;keys_present&#34;:true`, `&#34;key_id&#34;:&#34;k1&#34;`, "Point-in-time recovery"} {
		if !strings.Contains(body, want) {
			t.Errorf("wizard lacks %q", want)
		}
	}
	if strings.Contains(body, canary) || strings.Contains(body, "password_sealed") {
		t.Fatal("wizard page leaks a secret")
	}
	edit := a.must("GET", "/backups/jobs/legacy-w/edit", nil, 200)
	pageChecks(t, "wizard edit", edit)
	for _, want := range []string{`data-edit="1"`, `data-job-id="legacy-w"`, "Save changes", "Save and run now", `&#34;legacy&#34;:true`, "Convert to connection"} {
		if !strings.Contains(edit, want) {
			t.Errorf("wizard edit lacks %q", want)
		}
	}
	a.must("GET", "/backups/jobs/nope/edit", nil, 404)
}

// The payload the wizard sends for a Gold job on a MySQL connection with two databases: PITR cannot apply, so
// the job is saved with PITR off and the warning is visible on the job pages.
func TestWizardPayloadWarningsAndMissed(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	id := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	_, _ = a.do("POST", "/api/v1/backup-repositories", map[string]any{"name": "central", "env_prefix": "STRAND_BACKUP_S3_", "bucket": "central-backups"})
	payload := map[string]any{"name": "gold-two-dbs", "enabled": false, "method": "mysqldump", "connection_id": id, "policy_id": "policy-gold",
		"conn": map[string]any{"databases": []string{"app", "billing"}}, "bucket": "central-backups"}
	out := a.must("POST", "/api/v1/backup-jobs", payload, 201)
	res := decode(t, out)
	warns := anyStrings(res["warnings"])
	if len(warns) == 0 || !strings.Contains(warns[0], "PITR") {
		t.Fatalf("no PITR warning: %s", out)
	}
	jid := res["job"].(map[string]any)["id"].(string)
	page := a.must("GET", "/backups/jobs/"+jid, nil, 200)
	if !strings.Contains(page, "Gold") || !strings.Contains(page, "requests PITR") || !strings.Contains(page, `href="/backups/policies/policy-gold"`) || !strings.Contains(page, `href="/backups/connections/`+id+`"`) {
		t.Errorf("job detail lacks policy/connection/warning: %s", page)
	}
	list := a.must("GET", "/backups/jobs", nil, 200)
	if !strings.Contains(list, "requests PITR") || !strings.Contains(list, "Daily 01:00 UTC") {
		t.Errorf("jobs list lacks warning or human schedule")
	}
	// A missed run shows as a warning-toned badge and filters.
	j, _, _ := e.st.GetBackupJob(context.Background(), jid)
	j.LastRunStatus = backup.RunStatusMissed
	if _, err := e.st.UpdateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if b := a.must("GET", "/backups/jobs?status=missed", nil, 200); !strings.Contains(b, "gold-two-dbs") || !strings.Contains(b, `st-missed`) {
		t.Errorf("missed filter/badge missing")
	}
	if b := a.must("GET", "/backups/jobs/"+jid, nil, 200); !strings.Contains(b, "Missed.") {
		t.Errorf("job detail lacks the missed notice")
	}
	if b := a.must("GET", "/backups/jobs?policy=policy-silver", nil, 200); strings.Contains(b, "gold-two-dbs") {
		t.Errorf("policy filter did not filter")
	}
}

// dumpPages writes rendered pages for the jsdom harnesses (STRAND_DUMP_UI_DIR); a no-op otherwise.
func dumpPage(t *testing.T, name, body string) {
	t.Helper()
	if dir := os.Getenv("STRAND_DUMP_UI_DIR"); dir != "" {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDumpUIPagesForHarness(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	e := newCPEnv(t)
	a := e.admin()
	ctx := context.Background()
	_ = e.st.CreateBackupRepo(ctx, backup.Repo{ID: "central", Name: "Central", Bucket: "central-backups", EnvPrefix: "STRAND_BACKUP_S3_"})
	_ = e.st.CreateBackupRepo(ctx, backup.Repo{ID: "vault", Name: "Vault", Bucket: "vault-backups", EnvPrefix: "STRAND_BACKUP_REPO_VAULT_", LockMode: "COMPLIANCE", LockDays: 30})
	ids := map[string]string{}
	for _, c := range []map[string]any{
		{"name": "shop-mysql", "engine": "mysql", "host": "mysql.internal", "secret_mode": "stored", "password": "shop-pw"},
		{"name": "legacy-mariadb", "engine": "mariadb", "host": "maria.internal", "secret_mode": "stored", "password": "maria-pw"},
		{"name": "ledger-pg", "engine": "postgres", "host": "pg.internal", "secret_mode": "stored", "password": "pg-pw"},
		{"name": "events-mongo", "engine": "mongodb", "host": "mongo.internal", "secret_mode": "stored"},
		{"name": "stored-mysql", "engine": "mysql", "host": "stored.internal", "secret_mode": "stored", "password": canary},
	} {
		ids[c["name"].(string)] = connID(t, a.must("POST", "/api/v1/backup-connections", c, 201))
	}
	a.must("POST", "/api/v1/backup-jobs", map[string]any{"name": "gold-shop", "method": "mysqldump", "bucket": "central-backups", "enabled": false,
		"connection_id": ids["shop-mysql"], "policy_id": "policy-gold", "repository": "central", "overrides": []string{"compress"}, "compress": false,
		"conn": map[string]any{"databases": []string{"app"}}}, 201)
	jobs, _ := e.st.ListBackupJobs(ctx)
	dumpPage(t, "wizard_nokeys.html", a.must("GET", "/backups/jobs/new", nil, 200))
	dumpPage(t, "wizard_edit.html", a.must("GET", "/backups/jobs/"+jobs[0].ID+"/edit", nil, 200))
	dumpPage(t, "connections.html", a.must("GET", "/backups/connections", nil, 200))
	dumpPage(t, "policy_detail.html", a.must("GET", "/backups/policies/policy-gold", nil, 200))
	dumpPage(t, "policies.html", a.must("GET", "/backups/policies", nil, 200))
	t.Setenv(backup.EnvEncryptionKeys, "k1:"+key)
	dumpPage(t, "wizard_keys.html", a.must("GET", "/backups/jobs/new", nil, 200))
	dumpPage(t, "jobs.html", a.must("GET", "/backups/jobs", nil, 200))
	dumpPage(t, "repositories.html", a.must("GET", "/backups/repositories", nil, 200))
	seedRichBackup(t, e.st, "bkp-dump-1", "shop", "mysql", "mysqldump", backup.KindFull, e.srv.now().UTC().Add(-time.Hour), backup.Position{}, false, nil, "", nil, nil, "", nil)
	dumpPage(t, "catalog_detail.html", a.must("GET", "/backups/catalog/bkp-dump-1", nil, 200))
	dumpPage(t, "catalog.html", a.must("GET", "/backups/catalog", nil, 200))
}
