package web

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/web/store"
)

// ---- helpers ----

// hookRecorder is a notification receiver: it records every request body and answers with status.
type hookRecorder struct {
	*httptest.Server
	mu     sync.Mutex
	bodies []string
	paths  []string
	status int
}

func newHook(t *testing.T, status int) *hookRecorder {
	h := &hookRecorder{status: status}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.bodies, h.paths = append(h.bodies, string(b)), append(h.paths, r.URL.Path)
		h.mu.Unlock()
		w.WriteHeader(h.status)
	}))
	t.Cleanup(h.Close)
	return h
}

func (h *hookRecorder) got() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.bodies...)
}

func (h *hookRecorder) waitFor(t *testing.T, n int) []string {
	t.Helper()
	waitFor(t, fmt.Sprintf("%d notification(s)", n), func() bool { return len(h.got()) >= n })
	time.Sleep(100 * time.Millisecond) // let a wrongly duplicated delivery show up
	return h.got()
}

// mailRecorder replaces smtp.SendMail.
type mailRecorder struct {
	mu   sync.Mutex
	msgs []string
	to   [][]string
	from []string
	auth []smtp.Auth
}

func (m *mailRecorder) send(_ string, a smtp.Auth, from string, to []string, msg []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs, m.to, m.from, m.auth = append(m.msgs, string(msg)), append(m.to, to), append(m.from, from), append(m.auth, a)
	return nil
}

func newP56Env(t *testing.T, opt func(o *Options)) *cpEnv {
	t.Helper()
	st := store.NewMem()
	e := dbEnv(t, st, nil, opt)
	rec := &specRecorder{backupStopTestRunner: backupStopTestRunner{jobs: map[string]JobStatus{}}}
	e.srv.SetJobRunner(rec)
	return &cpEnv{testEnv: e, st: st, rec: rec}
}

func seedInRepo(t *testing.T, st store.Store, id, repo, source string, at time.Time) {
	t.Helper()
	cat, _ := backup.NewCatalog(st)
	r := backup.Record{ID: id, Source: source, Engine: "mysql", Tool: "mysqldump", Bucket: "central-backups", ObjectKey: "p/" + id,
		ManifestURI: "s3://central-backups/p/" + id + ".json", Status: backup.StatusQueued, CreatedAt: at, Repository: repo, Method: backup.MethodMySQLDump}
	if _, _, err := cat.Create(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusUploading, nil)
	start := at
	if _, _, err := cat.UpdateMeta(ctx, id, "mysql", backup.Meta{Position: backup.Position{GTIDSet: "u:1-5"}, StartTime: &start, Format: backup.FormatSBK1, KeyID: "k1"}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
	if _, _, err := cat.Finalize(ctx, id, backup.StatusVerifying, 2048, strings.Repeat("c", 64), r.ManifestURI, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

func storedRepoBody(name, endpoint string) map[string]any {
	return map[string]any{"name": name, "secret_mode": "stored", "endpoint": endpoint, "region": "us-east-1", "bucket": "strand-test",
		"prefix": "strand", "allow_insecure_local": true, "access_key": "AK-" + canary, "secret_key": "SK-" + canary}
}

// ---- repositories ----

func TestRepoStoredLifecycle(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	out := a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Primary", "http://127.0.0.1:1"), 200)
	rp := decode(t, out)["repository"].(map[string]any)
	id := rp["id"].(string)
	if id != "primary" || rp["access_key_set"] != true || rp["secret_key_set"] != true || rp["configured"] != true || rp["secret_mode"] != "stored" || rp["kind"] != "s3" {
		t.Fatalf("created = %v", rp)
	}
	row, _, _ := e.st.GetBackupRepo(context.Background(), id)
	if pt, err := e.srv.db.box.Open(row.SecretKeySealed, id); err != nil || string(pt) != "SK-"+canary {
		t.Fatalf("secret key not sealed under the repository id: %v", err)
	}
	if _, err := e.srv.db.box.Open(row.SecretKeySealed, "other"); err == nil {
		t.Fatal("sealed key opens under another id")
	}
	// Validation.
	bad := storedRepoBody("Bad1", "http://127.0.0.1:1")
	delete(bad, "secret_key")
	a.must("POST", "/api/v1/backup-repositories", bad, 400)
	bad = storedRepoBody("Bad2", "ftp://x")
	a.must("POST", "/api/v1/backup-repositories", bad, 400)
	bad = storedRepoBody("Bad3", "http://127.0.0.1:1")
	bad["kind"] = "azure"
	a.must("POST", "/api/v1/backup-repositories", bad, 400)
	a.must("POST", "/api/v1/backup-repositories", map[string]any{"name": "Bad4", "secret_mode": "env", "env_prefix": "STRAND_BACKUP_REPO_B4_", "bucket": "bucket-b4", "secret_key": "x"}, 400)
	a.must("POST", "/api/v1/backup-repositories", map[string]any{"name": "Bad5", "env_prefix": "STRAND_WEB_", "bucket": "bucket-b5"}, 400)
	a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Primary", "http://127.0.0.1:1"), 409)
	// Update without keys keeps them; with a key rotates it; env mode drops them.
	upd := storedRepoBody("Primary", "http://127.0.0.1:1")
	delete(upd, "access_key")
	delete(upd, "secret_key")
	upd["region"] = "eu-west-1"
	a.must("PUT", "/api/v1/backup-repositories/"+id, upd, 200)
	row2, _, _ := e.st.GetBackupRepo(context.Background(), id)
	if string(row2.SecretKeySealed) != string(row.SecretKeySealed) || row2.Region != "eu-west-1" {
		t.Fatal("update without keys changed the stored key")
	}
	upd["secret_key"] = "SK2-" + canary
	a.must("PUT", "/api/v1/backup-repositories/"+id, upd, 200)
	row3, _, _ := e.st.GetBackupRepo(context.Background(), id)
	if pt, _ := e.srv.db.box.Open(row3.SecretKeySealed, id); string(pt) != "SK2-"+canary {
		t.Fatal("secret key not rotated")
	}
	audit := e.auditText()
	if !strings.Contains(audit, `"secret_key":"changed"`) || !strings.Contains(audit, `"region":{"from":"us-east-1","to":"eu-west-1"}`) || strings.Contains(audit, canary) {
		t.Fatalf("audit lacks the diff or leaks: %s", audit)
	}
	// Detail: usage and used_by.
	d := decode(t, a.must("GET", "/api/v1/backup-repositories/"+id, nil, 200))
	if d["usage_by_source"] != nil && len(d["usage_by_source"].([]any)) != 0 {
		t.Fatalf("detail = %v", d)
	}
	a.must("GET", "/api/v1/backup-repositories/nope", nil, 404)
}

func TestRepoDeleteBlockedAndUsage(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	body := storedRepoBody("Vault", "http://127.0.0.1:1")
	body["lock_mode"], body["lock_days"] = "COMPLIANCE", 30
	id := decode(t, a.must("POST", "/api/v1/backup-repositories", body, 200))["repository"].(map[string]any)["id"].(string)
	now := e.srv.now().UTC()
	seedInRepo(t, e.st, "bk-r1", id, "src-a", now.Add(-2*time.Hour))
	seedInRepo(t, e.st, "bk-r2", id, "src-a", now.Add(-time.Hour))
	seedInRepo(t, e.st, "bk-r3", id, "src-b", now.Add(-time.Hour))
	list := decode(t, a.must("GET", "/api/v1/backup-repositories", nil, 200))["repositories"].([]any)
	v := list[0].(map[string]any)
	if v["immutable"] != true || v["backup_count"].(float64) != 3 || v["used_bytes"].(float64) != 6144 {
		t.Fatalf("list view = %v", v)
	}
	det := decode(t, a.must("GET", "/api/v1/backup-repositories/"+id, nil, 200))
	if len(det["usage_by_source"].([]any)) != 2 {
		t.Fatalf("usage by source = %v", det["usage_by_source"])
	}
	// Blocked while it holds backups.
	code, out := a.do("DELETE", "/api/v1/backup-repositories/"+id, nil)
	if code != 409 || !strings.Contains(out, "3 backups") {
		t.Fatalf("delete with backups = %d %s", code, out)
	}
	for _, b := range []string{"bk-r1", "bk-r2", "bk-r3"} {
		if _, _, err := e.st.UpdateBackupStatus(context.Background(), b, backup.StatusComplete, backup.StatusExpired, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Blocked while a job names it.
	job := backup.Job{ID: "bjob-vault", Name: "uses vault", EverySeconds: 3600, Bucket: "central-backups", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump,
		Source: "src-a", Repository: id, Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
	if err := e.st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	code, out = a.do("DELETE", "/api/v1/backup-repositories/"+id, nil)
	if code != 409 || !strings.Contains(out, "bjob-vault") {
		t.Fatalf("delete used by a job = %d %s", code, out)
	}
	if _, err := e.st.DeleteBackupJob(context.Background(), "bjob-vault"); err != nil {
		t.Fatal(err)
	}
	a.must("DELETE", "/api/v1/backup-repositories/"+id, nil, 204)
	a.must("DELETE", "/api/v1/backup-repositories/"+id, nil, 404)
}

func TestRepoRBAC(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	id := decode(t, a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Rbac", "http://127.0.0.1:1"), 200))["repository"].(map[string]any)["id"].(string)
	op, vw := e.roleClient(store.RoleOperator), e.roleClient(store.RoleViewer)
	// Operator: may test (a probe), may not create/edit/delete.
	op.must("POST", "/api/v1/backup-repositories/"+id+"/test", nil, 200)
	op.must("POST", "/api/v1/backup-repositories", storedRepoBody("Nope", "http://127.0.0.1:1"), 403)
	op.must("PUT", "/api/v1/backup-repositories/"+id, storedRepoBody("Rbac", "http://127.0.0.1:1"), 403)
	op.must("DELETE", "/api/v1/backup-repositories/"+id, nil, 403)
	// Viewer: read only.
	vw.must("GET", "/api/v1/backup-repositories", nil, 200)
	vw.must("POST", "/api/v1/backup-repositories/"+id+"/test", nil, 403)
	vw.must("POST", "/api/v1/backup-repositories", storedRepoBody("Nope", "http://127.0.0.1:1"), 403)
	// Keys, channels, rules and the activity feed are admin reads.
	for _, p := range []string{"/api/v1/backup-keys", "/api/v1/backup-notify-channels", "/api/v1/backup-notify-rules", "/api/v1/backup-activity", "/backups/keys", "/backups/notifications", "/backups/activity"} {
		op.must("GET", p, nil, 403)
		vw.must("GET", p, nil, 403)
		a.must("GET", p, nil, 200)
	}
	// Channel and rule writes (including the test button) stay admin.
	op.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "x", "kind": "slack", "url": "https://h.example.com/x"}, 403)
	op.must("POST", "/api/v1/backup-notify-channels/env/test", nil, 403)
	op.must("POST", "/api/v1/backup-notify-rules", map[string]any{"event": "failure", "channel_id": "c"}, 403)
}

func TestRepoTestUnreachableChecklist(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	body := storedRepoBody("Down", "http://127.0.0.1:1")
	id := decode(t, a.must("POST", "/api/v1/backup-repositories", body, 200))["repository"].(map[string]any)["id"].(string)
	out := a.must("POST", "/api/v1/backup-repositories/"+id+"/test", nil, 200)
	if strings.Contains(out, canary) {
		t.Fatalf("test result leaks a credential: %s", out)
	}
	res := decode(t, out)
	steps := res["steps"].([]any)
	want := []string{"list", "put", "get", "delete", "object_lock"}
	if res["ok"] != false || res["status"] != "failed" || len(steps) != len(want) {
		t.Fatalf("result = %s", out)
	}
	for i, st := range steps {
		m := st.(map[string]any)
		if m["name"] != want[i] {
			t.Fatalf("step %d = %v", i, m)
		}
		if i == 0 && m["status"] != "failed" || i > 0 && m["status"] != "skipped" {
			t.Fatalf("step %d = %v", i, m)
		}
	}
	got, _, _ := e.st.GetBackupRepo(context.Background(), id)
	if got.LastTestStatus != "failed" || got.LastTestAt == nil || !strings.Contains(got.LastTestDetail, "list") {
		t.Fatalf("last test not recorded: %+v", got)
	}
	// The recorded checklist is part of the repository view.
	if !strings.Contains(a.must("GET", "/api/v1/backup-repositories/"+id, nil, 200), `"last_test_steps"`) {
		t.Fatal("checklist missing from the detail")
	}
	if len(e.auditFor("backup_repository.test")) != 1 {
		t.Fatal("test not audited")
	}
	a.must("POST", "/api/v1/backup-repositories/nope/test", nil, 404)
}

// Stored credentials reach the backup process through its environment only.
func TestStoredRepoCredentialsReachChildEnvNotArgv(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	id := decode(t, a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Envd", "http://127.0.0.1:9"), 200))["repository"].(map[string]any)["id"].(string)
	row, _, _ := e.st.GetBackupRepo(context.Background(), id)
	job := backup.Job{ID: "bjob-env", Name: "env job", EverySeconds: 3600, Bucket: "central-backups", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump,
		Source: "src", Repository: id, Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
	if err := e.st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	e.rec.take()
	a.must("POST", "/api/v1/backup-jobs/bjob-env/prune", nil, 202)
	specs := e.rec.take()
	if len(specs) != 1 {
		t.Fatalf("specs = %d", len(specs))
	}
	p := row.EffectivePrefix()
	env := strings.Join(specs[0].Env, "\n")
	for _, want := range []string{p + "ENDPOINT=http://127.0.0.1:9", p + "ACCESS_KEY=AK-" + canary, p + "SECRET_KEY=SK-" + canary, p + "ALLOW_INSECURE_LOCAL=true", p + "REGION=us-east-1"} {
		if !strings.Contains(env, want) {
			t.Fatalf("child environment lacks %q:\n%s", want, env)
		}
	}
	if strings.Contains(strings.Join(specs[0].Args, " "), canary) || strings.Contains(strings.Join(specs[0].DisplayArgs, " "), canary) {
		t.Fatal("credential in argv")
	}
	// The runner reads the repository through the derived prefix.
	getenv := func(k string) string {
		for _, kv := range specs[0].Env {
			if strings.HasPrefix(kv, k+"=") {
				return strings.TrimPrefix(kv, k+"=")
			}
		}
		return ""
	}
	r, err := runner.OpenRepository(getenv, &row, row.Bucket, "", 0, 0)
	if err != nil || r.Bucket != "strand-test" || r.Prefix != "strand" {
		t.Fatalf("runner could not open the stored repository: %v %+v", err, r)
	}
}

// ---- notification channels ----

func TestChannelsRulesAndDelivery(t *testing.T) {
	mail := &mailRecorder{}
	e := newP56Env(t, func(o *Options) { o.NotifySendMail = mail.send })
	a := e.admin()
	h1, h2 := newHook(t, 200), newHook(t, 200)
	mk := func(name, kind, url string) string {
		out := a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": name, "kind": kind, "url": url}, 201)
		if strings.Contains(out, "canary") || !strings.Contains(out, `"secret_set": true`) {
			t.Fatalf("channel response = %s", out)
		}
		return decode(t, out)["channel"].(map[string]any)["id"].(string)
	}
	c1 := mk("ops-slack", "slack", h1.URL+"/slack-secret-canary")
	c2 := mk("gold-hook", "webhook", h2.URL+"/hook-secret-canary")
	// Listing never shows a destination.
	list := a.must("GET", "/api/v1/backup-notify-channels", nil, 200)
	if strings.Contains(list, "canary") || strings.Contains(list, h1.URL) {
		t.Fatalf("list leaks a destination: %s", list)
	}
	// Send test: delivered with a slack-shaped body; the result is recorded.
	res := decode(t, a.must("POST", "/api/v1/backup-notify-channels/"+c1+"/test", nil, 200))
	if res["ok"] != true {
		t.Fatalf("test = %v", res)
	}
	bodies := h1.waitFor(t, 1)
	if !strings.Contains(bodies[0], `"text":"[strand backup] TEST:`) || h1.paths[0] != "/slack-secret-canary" {
		t.Fatalf("slack test delivery = %v %v", bodies, h1.paths)
	}
	ch, _, _ := e.st.GetNotifyChannel(context.Background(), c1)
	if ch.LastTestStatus != "ok" || ch.LastTestAt == nil {
		t.Fatalf("test result not recorded: %+v", ch)
	}
	// A failing receiver: the result says so, without the destination.
	bad := newHook(t, 500)
	c3 := mk("broken", "teams", bad.URL+"/teams-secret-canary")
	out := a.must("POST", "/api/v1/backup-notify-channels/"+c3+"/test", nil, 200)
	if !strings.Contains(out, `"ok": false`) || !strings.Contains(out, "HTTP 500") || strings.Contains(out, "canary") || strings.Contains(out, bad.URL) {
		t.Fatalf("failing channel result = %s", out)
	}
	if b := bad.got(); len(b) != 1 || !strings.Contains(b[0], "MessageCard") {
		t.Fatalf("teams payload = %v", b)
	}
	// Email via the SMTP seam: recipients, user auth, no password in the message.
	mailOut := a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "mail", "kind": "email", "smtp_addr": "smtp.example.com:587",
		"from": "strand@example.com", "to": []string{"dba@example.com", "ops@example.com"}, "user": "mailer", "password": "smtp-pass-canary"}, 201)
	c4 := decode(t, mailOut)["channel"].(map[string]any)["id"].(string)
	if strings.Contains(mailOut, "canary") {
		t.Fatalf("email channel response leaks: %s", mailOut)
	}
	a.must("POST", "/api/v1/backup-notify-channels/"+c4+"/test", nil, 200)
	if len(mail.msgs) != 1 || strings.Join(mail.to[0], ",") != "dba@example.com,ops@example.com" || mail.from[0] != "strand@example.com" || mail.auth[0] == nil ||
		strings.Contains(mail.msgs[0], "canary") || !strings.Contains(mail.msgs[0], "Subject: [strand backup] TEST") {
		t.Fatalf("mail = %+v", mail)
	}
	// Validation.
	a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "x", "kind": "slack"}, 400)
	a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "x", "kind": "slack", "url": "ftp://x"}, 400)
	a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "x", "kind": "slack", "url": "https://h.example.com/x", "to": []string{"a@b.c"}}, 400)
	a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"id": "env", "name": "x", "kind": "slack", "url": "https://h.example.com/x"}, 400)
	a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "ops-slack", "kind": "slack", "url": "https://h.example.com/x"}, 409)
	// Update without a url keeps the destination.
	a.must("PUT", "/api/v1/backup-notify-channels/"+c1, map[string]any{"name": "ops-slack-2", "kind": "slack"}, 200)
	a.must("POST", "/api/v1/backup-notify-channels/"+c1+"/test", nil, 200)
	if got := h1.waitFor(t, 2); len(got) != 2 || h1.paths[1] != "/slack-secret-canary" {
		t.Fatalf("destination lost on update: %v", h1.paths)
	}
	// Rules: event x scope -> channel.
	gold := backup.BuiltinPolicies()[0]
	rule := func(event, scope, id, ch string, want int) string {
		body := map[string]any{"event": event, "scope_kind": scope, "scope_id": id, "channel_id": ch}
		code, out := a.do("POST", "/api/v1/backup-notify-rules", body)
		if code != want {
			t.Fatalf("rule %v = %d %s", body, code, out)
		}
		if code != 201 {
			return ""
		}
		return decode(t, out)["rule"].(map[string]any)["id"].(string)
	}
	rule("failure", "all", "", c1, 201)
	rGold := rule("failure", "policy", gold.ID, c2, 201)
	rule("rpo_breach", "all", "", c4, 201)
	rule("failure", "all", "", c1, 409)                  // duplicate
	rule("failure", "policy", "no-such-policy", c1, 400) // scope must exist
	rule("failure", "all", "", "nope", 400)              // channel must exist
	rule("failure", "all", "", "env", 400)               // the environment channel is not a target
	rule("explode", "all", "", c1, 400)
	before1, before2 := len(h1.got()), len(h2.got())
	e.srv.notifyBackupEvent(runner.Event{Kind: "failure", Job: "j1", Message: "boom", At: e.srv.now()}, gold.ID)
	if n := len(h1.got()) - before1; n != 1 {
		t.Fatalf("scope-all channel got %d deliveries", n)
	}
	if n := len(h2.got()) - before2; n != 1 {
		t.Fatalf("policy-scoped channel got %d deliveries", n)
	}
	e.srv.notifyBackupEvent(runner.Event{Kind: "failure", Job: "j1", Message: "boom", At: e.srv.now()}, "policy-other")
	if len(h1.got())-before1 != 2 || len(h2.got())-before2 != 1 {
		t.Fatal("an event of another policy reached the policy-scoped channel")
	}
	e.srv.notifyBackupEvent(runner.Event{Kind: "success", Job: "j1", Message: "ok", At: e.srv.now()}, gold.ID)
	if len(h1.got())-before1 != 2 {
		t.Fatal("an event with no rule was delivered")
	}
	// Disabling a rule or a channel stops delivery; deleting a channel deletes its rules.
	a.must("PUT", "/api/v1/backup-notify-rules/"+rGold, map[string]any{"enabled": false}, 200)
	e.srv.notifyBackupEvent(runner.Event{Kind: "failure", Job: "j1", Message: "boom", At: e.srv.now()}, gold.ID)
	if len(h2.got())-before2 != 1 {
		t.Fatal("a disabled rule delivered")
	}
	a.must("PUT", "/api/v1/backup-notify-channels/"+c1, map[string]any{"name": "ops-slack-2", "kind": "slack", "enabled": false}, 200)
	n1 := len(h1.got())
	e.srv.notifyBackupEvent(runner.Event{Kind: "failure", Job: "j1", Message: "boom", At: e.srv.now()}, gold.ID)
	if len(h1.got()) != n1 {
		t.Fatal("a disabled channel delivered")
	}
	a.must("DELETE", "/api/v1/backup-notify-channels/"+c1, nil, 204)
	rules := decode(t, a.must("GET", "/api/v1/backup-notify-rules", nil, 200))["rules"].([]any)
	for _, r := range rules {
		if r.(map[string]any)["channel_id"] == c1 {
			t.Fatal("rule of a deleted channel survived")
		}
	}
	// Deliveries are audited without the destination.
	audit := e.auditText()
	if !strings.Contains(audit, `"action":"backup.notify"`) || strings.Contains(audit, "canary") {
		t.Fatalf("audit = %s", audit)
	}
}

func TestEnvChannelReadOnly(t *testing.T) {
	h := newHook(t, 200)
	t.Setenv("STRAND_BACKUP_NOTIFY_WEBHOOK", h.URL+"/env-hook-canary")
	t.Setenv("STRAND_BACKUP_NOTIFY_ON", "failure,rpo_breach")
	e := newP56Env(t, nil)
	a := e.admin()
	list := a.must("GET", "/api/v1/backup-notify-channels", nil, 200)
	if strings.Contains(list, "canary") {
		t.Fatalf("env destination leaked: %s", list)
	}
	chans := decode(t, list)["channels"].([]any)
	env := chans[0].(map[string]any)
	if env["id"] != "env" || env["read_only"] != true || env["source"] != "environment" || !strings.Contains(list, `"rpo_breach"`) || strings.Contains(strings.Split(list, `"events"`)[0], `"success"`) {
		t.Fatalf("env channel = %v", env)
	}
	a.must("PUT", "/api/v1/backup-notify-channels/env", map[string]any{"name": "x", "kind": "slack", "url": "https://h.example.com/x"}, 409)
	a.must("DELETE", "/api/v1/backup-notify-channels/env", nil, 409)
	a.must("POST", "/api/v1/backup-notify-channels/env/test", nil, 200)
	if b := h.waitFor(t, 1); !strings.Contains(b[0], `"event":"test"`) {
		t.Fatalf("env test = %v", b)
	}
	// STRAND_BACKUP_NOTIFY_ON still decides what the environment channel gets.
	e.srv.notifyBackupEvent(runner.Event{Kind: "success", Message: "x", At: e.srv.now()}, "")
	e.srv.notifyBackupEvent(runner.Event{Kind: "rpo_breach", Message: "y", At: e.srv.now()}, "")
	if b := h.waitFor(t, 2); len(b) != 2 || !strings.Contains(b[1], `"event":"rpo_breach"`) {
		t.Fatalf("env deliveries = %v", b)
	}
}

// The maintenance tick routes a missed schedule through a rule to a stored channel.
func TestMaintenanceTickUsesRules(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	h := newHook(t, 200)
	gold := backup.BuiltinPolicies()[0]
	ch := decode(t, a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "w", "kind": "webhook", "url": h.URL + "/w"}, 201))["channel"].(map[string]any)["id"].(string)
	a.must("POST", "/api/v1/backup-notify-rules", map[string]any{"event": "missed", "scope_kind": "policy", "scope_id": gold.ID, "channel_id": ch}, 201)
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	job := backup.Job{ID: "bjob-tick", Name: "tick", EverySeconds: 3600, Bucket: "central-backups", Enabled: true, NextRunAt: &past,
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "src-tick", PolicyID: gold.ID, Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
	if err := e.st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	e.srv.backupMaintenanceTick(context.Background())
	e.srv.backupMaintenanceTick(context.Background())
	b := h.waitFor(t, 1)
	if len(b) != 1 || !strings.Contains(b[0], `"event":"missed"`) || !strings.Contains(b[0], `"job":"bjob-tick"`) {
		t.Fatalf("deliveries = %v", b)
	}
}

// ---- keys ----

func TestKeysPageDataNeverHasKeyMaterial(t *testing.T) {
	keyA, keyB := strings.Repeat("a1", 32), strings.Repeat("b2", 32)
	t.Setenv("STRAND_BACKUP_ENCRYPTION_KEYS", "k-new:"+keyA+",k1:"+keyB)
	e := newP56Env(t, nil)
	now := e.srv.now().UTC()
	seedComplete(t, e.st, "bk-k1a", "j", now.Add(-3*time.Hour), backup.KindFull) // key k1
	seedComplete(t, e.st, "bk-k1b", "j", now.Add(-2*time.Hour), backup.KindFull) // key k1
	// A backup whose key is gone, and an unencrypted one.
	cat, _ := backup.NewCatalog(e.st)
	mk := func(id, key string) {
		r := backup.Record{ID: id, Source: "other", Engine: "mysql", Tool: "mysqldump", Bucket: "central-backups", ObjectKey: "p/" + id,
			ManifestURI: "s3://central-backups/p/" + id, Status: backup.StatusQueued, CreatedAt: now.Add(-time.Hour), Method: backup.MethodMySQLDump}
		ctx := context.Background()
		if _, _, err := cat.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusUploading, nil)
		m := backup.Meta{Position: backup.Position{GTIDSet: "u:1-5"}}
		if key != "" {
			m.Format, m.KeyID = backup.FormatSBK1, key
		}
		if _, _, err := cat.UpdateMeta(ctx, id, "mysql", m); err != nil {
			t.Fatal(err)
		}
		_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
		if _, _, err := cat.Finalize(ctx, id, backup.StatusVerifying, 100, strings.Repeat("c", 64), r.ManifestURI, now); err != nil {
			t.Fatal(err)
		}
	}
	mk("bk-lost", "k-retired")
	mk("bk-plain", "")
	a := e.admin()
	out := a.must("GET", "/api/v1/backup-keys", nil, 200)
	if strings.Contains(out, keyA) || strings.Contains(out, keyB) {
		t.Fatalf("key material in the response: %s", out)
	}
	var k keysInfo
	if err := json.Unmarshal([]byte(out), &k); err != nil {
		t.Fatal(err)
	}
	if !k.Configured || k.ActiveKeyID != "k-new" || len(k.Keys) != 2 || k.Keys[0].ID != "k-new" || !k.Keys[0].Active || k.Keys[1].ID != "k1" || k.Keys[1].Backups != 2 ||
		k.Keys[0].Fingerprint == "" || k.Keys[0].Fingerprint == k.Keys[1].Fingerprint {
		t.Fatalf("keys = %+v", k)
	}
	if len(k.Warnings) != 1 || k.Warnings[0].KeyID != "k-retired" || k.Warnings[0].Backups != 1 || k.Warnings[0].SampleIDs[0] != "bk-lost" {
		t.Fatalf("warnings = %+v", k.Warnings)
	}
	if k.Unencrypted.Backups != 1 {
		t.Fatalf("unencrypted = %+v", k.Unencrypted)
	}
	_, page := a.e.testEnv.do(a.c, "GET", "/backups/keys", "", nil)
	if strings.Contains(page, keyA) || strings.Contains(page, keyB) || !strings.Contains(page, "k-retired") || !strings.Contains(page, "write key") {
		t.Fatal("keys page")
	}
	// No keys configured: a notice, not an error.
	t.Setenv("STRAND_BACKUP_ENCRYPTION_KEYS", "")
	if out := a.must("GET", "/api/v1/backup-keys", nil, 200); !strings.Contains(out, `"configured": false`) || !strings.Contains(out, "notice") {
		t.Fatalf("no keys = %s", out)
	}
	// A broken key variable is reported without echoing it.
	t.Setenv("STRAND_BACKUP_ENCRYPTION_KEYS", "bad:not-a-key-"+canary)
	if out := a.must("GET", "/api/v1/backup-keys", nil, 200); strings.Contains(out, canary) || !strings.Contains(out, `"error"`) {
		t.Fatalf("broken key config = %s", out)
	}
}

// ---- RPO and compliance ----

func seedLogSegment(t *testing.T, st store.Store, id, source, job string, end time.Time) {
	t.Helper()
	cat, _ := backup.NewCatalog(st)
	ctx := context.Background()
	r := backup.Record{ID: id, Source: source, Engine: "postgres", Tool: "strand", Bucket: "central-backups", ObjectKey: "pg/wal/" + id,
		ManifestURI: "s3://central-backups/pg/wal/" + id, Status: backup.StatusQueued, CreatedAt: end, JobID: job, Method: backup.MethodWAL, Kind: backup.KindLog}
	if _, _, err := cat.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusUploading, nil)
	start := end.Add(-time.Minute)
	if _, _, err := cat.UpdateMeta(ctx, id, "postgres", backup.Meta{Position: backup.Position{LSN: "0/3000000"}, EndPosition: backup.Position{LSN: "0/4000000"}, StartTime: &start, EndTime: &end}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
	if _, _, err := cat.Finalize(ctx, id, backup.StatusVerifying, 10, strings.Repeat("d", 64), r.ManifestURI, end); err != nil {
		t.Fatal(err)
	}
}

func seedPITRBackup(t *testing.T, st store.Store, id, source, job string, at time.Time) {
	t.Helper()
	cat, _ := backup.NewCatalog(st)
	ctx := context.Background()
	r := backup.Record{ID: id, Source: source, Engine: "postgres", Tool: "pg_basebackup", Bucket: "central-backups", ObjectKey: "pg/" + id,
		ManifestURI: "s3://central-backups/pg/" + id, Status: backup.StatusQueued, CreatedAt: at, JobID: job, Method: backup.MethodPGBaseBackup, Kind: backup.KindFull}
	if _, _, err := cat.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusUploading, nil)
	start := at
	if _, _, err := cat.UpdateMeta(ctx, id, "postgres", backup.Meta{Position: backup.Position{LSN: "0/3000028"}, EndPosition: backup.Position{LSN: "0/3000100"}, StartTime: &start}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = cat.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
	if _, _, err := cat.Finalize(ctx, id, backup.StatusVerifying, 500, strings.Repeat("e", 64), r.ManifestURI, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
}

// rpoFixture: a PITR source (gold: RPO 5 min; last log segment 2 min old) and a plain source (silver: RPO
// 24 h; last backup 26 h old = breach).
func rpoFixture(t *testing.T, e *cpEnv) (pitrJob, plainJob backup.Job) {
	t.Helper()
	ctx := context.Background()
	now := e.srv.now().UTC()
	pitrJob = backup.Job{ID: "bjob-pg", Name: "pg gold", EverySeconds: 86400, Bucket: "central-backups", Enabled: true,
		JobOptions: backup.JobOptions{Method: backup.MethodPGBaseBackup, Source: "pg-prod", PolicyID: "policy-gold", LogArchive: true,
			Conn: backup.ConnSpec{Host: "db", User: "u", PasswordEnv: "PGPW"}}}
	plainJob = backup.Job{ID: "bjob-my", Name: "mysql silver", EverySeconds: 86400, Bucket: "central-backups", Enabled: true,
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-prod", PolicyID: "policy-silver",
			Conn: backup.ConnSpec{Host: "db2", Databases: []string{"app"}}}}
	for _, j := range []backup.Job{pitrJob, plainJob} {
		if err := e.st.CreateBackupJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	seedPITRBackup(t, e.st, "bk-pg-base", "pg-prod", "bjob-pg", now.Add(-20*time.Hour))
	seedLogSegment(t, e.st, "bk-pg-wal", "pg-prod", "bjob-pg", now.Add(-2*time.Minute))
	seedComplete(t, e.st, "bk-my-1", "bjob-my", now.Add(-26*time.Hour), backup.KindFull)
	return
}

func TestRPOAPIAndMetric(t *testing.T) {
	e := newP56Env(t, nil)
	rpoFixture(t, e)
	a := e.admin()
	out := a.must("GET", "/api/v1/backup-rpo", nil, 200)
	var resp struct {
		Sources []backup.RPOStatus `json:"sources"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil || len(resp.Sources) != 2 {
		t.Fatalf("rpo = %s", out)
	}
	my, pg := resp.Sources[0], resp.Sources[1]
	if pg.Source != "pg-prod" || !pg.PITR || pg.Basis != backup.RPOBasisLog || pg.CurrentSecs != 120 || pg.Breach || pg.TargetMinutes != 5 {
		t.Fatalf("pitr source = %+v", pg)
	}
	if my.Source != "mysql-prod" || my.PITR || my.Basis != backup.RPOBasisBackup || my.CurrentSecs != 26*3600 || !my.Breach || my.TargetMinutes != 1440 {
		t.Fatalf("plain source = %+v", my)
	}
	_, metrics := a.e.testEnv.do(a.c, "GET", "/metrics", "", nil)
	for _, want := range []string{"# TYPE strand_backup_rpo_seconds gauge", `strand_backup_rpo_seconds{source="pg-prod"} 120`,
		fmt.Sprintf(`strand_backup_rpo_seconds{source="mysql-prod"} %d`, 26*3600), `strand_backup_rpo_target_seconds{source="pg-prod"} 300`,
		`strand_backup_rpo_breach{source="mysql-prod"} 1`, `strand_backup_rpo_breach{source="pg-prod"} 0`} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("/metrics lacks %q:\n%s", want, metrics)
		}
	}
	// Viewers read the RPO too (no secrets in it).
	e.roleClient(store.RoleViewer).must("GET", "/api/v1/backup-rpo", nil, 200)
}

func TestRPOBreachNotifiedOncePerEpisode(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	h := newHook(t, 200)
	ch := decode(t, a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "w", "kind": "webhook", "url": h.URL + "/rpo"}, 201))["channel"].(map[string]any)["id"].(string)
	a.must("POST", "/api/v1/backup-notify-rules", map[string]any{"event": "rpo_breach", "scope_kind": "all", "channel_id": ch}, 201)
	rpoFixture(t, e)
	ctx := context.Background()
	e.srv.backupMaintenanceTick(ctx)
	e.srv.backupMaintenanceTick(ctx)
	b := h.waitFor(t, 1)
	if len(b) != 1 || !strings.Contains(b[0], `"event":"rpo_breach"`) || !strings.Contains(b[0], `"source":"mysql-prod"`) || !strings.Contains(b[0], `"job":"bjob-my"`) {
		t.Fatalf("breach notifications = %v", b)
	}
	if open, _ := e.st.ListBackupRPOBreaches(ctx); len(open) != 1 || open[0].Source != "mysql-prod" {
		t.Fatalf("open breaches = %+v", open)
	}
	// A new backup ends the episode; a later breach notifies again.
	seedComplete(t, e.st, "bk-my-2", "bjob-my", e.srv.now().UTC().Add(-time.Hour), backup.KindFull)
	e.srv.backupMaintenanceTick(ctx)
	if open, _ := e.st.ListBackupRPOBreaches(ctx); len(open) != 0 {
		t.Fatalf("recovered source still open: %+v", open)
	}
	e.clock.Advance(30 * time.Hour)
	e.srv.backupMaintenanceTick(ctx)
	// pg-prod (log 2 min old + 30 h) and mysql-prod are both over their targets now.
	if b := h.waitFor(t, 3); len(b) != 3 {
		t.Fatalf("after the second episode: %v", b)
	}
	// A paused job is reported but never pages.
	j, _, _ := e.st.GetBackupJob(ctx, "bjob-my")
	j.Enabled = false
	if _, err := e.st.UpdateBackupJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	_ = e.st.ClearBackupRPOBreach(ctx, "mysql-prod")
	n := len(h.got())
	e.srv.backupMaintenanceTick(ctx)
	time.Sleep(150 * time.Millisecond)
	for _, body := range h.got()[n:] {
		if strings.Contains(body, "mysql-prod") {
			t.Fatalf("a paused job paged: %s", body)
		}
	}
}

func TestComplianceReportAndCSV(t *testing.T) {
	e := newP56Env(t, nil)
	jobs := func() {}
	_ = jobs
	pitr, plain := rpoFixture(t, e)
	ctx := context.Background()
	now := e.srv.now().UTC()
	addRun := func(job, id, status string, ago time.Duration) {
		if err := e.st.CreateBackupRun(ctx, backup.Run{ID: id, JobID: job, Trigger: backup.TriggerSchedule, StartedAt: now.Add(-ago), Status: status}); err != nil {
			t.Fatal(err)
		}
	}
	// plain: 3 ok + 1 failed in the last 7 days, 1 ok 20 days ago (inside 30 d, outside 7 d), 1 cancelled (ignored).
	for i := 0; i < 3; i++ {
		addRun(plain.ID, fmt.Sprintf("r-ok-%d", i), backup.RunStatusSuccess, time.Duration(i+1)*24*time.Hour)
	}
	addRun(plain.ID, "r-bad", string(backup.RunStatusFailed), 36*time.Hour)
	addRun(plain.ID, "r-old", backup.RunStatusSuccess, 20*24*time.Hour)
	addRun(plain.ID, "r-cancel", string(backup.RunStatusCancelled), 2*time.Hour)
	addRun(pitr.ID, "p-ok", backup.RunStatusSuccess, 3*time.Hour)
	// A test-restore result for the plain source.
	if _, err := e.st.AddBackupCheck(ctx, backup.CheckResult{BackupID: "bk-my-1", Kind: "test_restore", Status: backup.CheckOK,
		StartedAt: now.Add(-3 * time.Hour), FinishedAt: now.Add(-2 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	a := e.admin()
	var rep complianceReport
	out := a.must("GET", "/api/v1/backup-reports/compliance", nil, 200)
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Sources != 2 || rep.RPOBreaching != 1 || rep.LastTestRestore == nil || rep.SuccessRate7d == nil || rep.SuccessRate30d == nil {
		t.Fatalf("summary = %s", out)
	}
	var my complianceRow
	for _, r := range rep.Rows {
		if r.Source == "mysql-prod" {
			my = r
		}
	}
	if my.Runs != 5 || my.Succeeded != 4 || my.Failed != 1 || *my.SuccessRate != 80 || *my.SuccessRate7d != 75 || *my.SuccessRate30d != 80 || !my.RPOBreach ||
		my.LastTestRestore != backup.CheckOK || my.LastTestRestoreAt == nil || my.RPOTargetMinutes != 1440 {
		t.Fatalf("mysql row = %+v", my)
	}
	// 7 d overall: plain 3 ok + 1 bad, pitr 1 ok -> 4/5 = 80 %.
	if *rep.SuccessRate7d != 80 {
		t.Fatalf("overall 7 d = %v", *rep.SuccessRate7d)
	}

	// CSV: UTF-8 BOM, Excel-friendly CRLF, parseable, one row per source.
	code, raw := a.do("GET", "/api/v1/backup-reports/compliance.csv?from=2026-11-01&to=2026-11-16", nil)
	if code != 200 || !strings.HasPrefix(raw, "\xEF\xBB\xBF") {
		t.Fatalf("csv = %d %q", code, raw[:min(len(raw), 40)])
	}
	rd := csv.NewReader(strings.NewReader(strings.TrimPrefix(raw, "\xEF\xBB\xBF")))
	recs, err := rd.ReadAll()
	if err != nil || len(recs) != 3 {
		t.Fatalf("csv parse: %v %d records\n%s", err, len(recs), raw)
	}
	col := map[string]int{}
	for i, h := range recs[0] {
		col[h] = i
	}
	for _, h := range []string{"source", "rpo_target_minutes", "current_rpo_minutes", "rpo_breach", "success_rate_7d_pct", "last_test_restore_at"} {
		if _, ok := col[h]; !ok {
			t.Fatalf("csv lacks column %q: %v", h, recs[0])
		}
	}
	var row []string
	for _, r := range recs[1:] {
		if r[col["source"]] == "mysql-prod" {
			row = r
		}
	}
	if row == nil || row[col["rpo_breach"]] != "yes" || row[col["current_rpo_minutes"]] != "1560.0" || row[col["rpo_target_minutes"]] != "1440" || row[col["success_rate_7d_pct"]] != "75.0" {
		t.Fatalf("mysql csv row = %v", row)
	}
	// Download headers, window validation and the audit trail of the export.
	resp, _ := e.testEnv.do(a.c, "GET", "/api/v1/backup-reports/compliance.csv", "", a.hdr)
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/csv") || !strings.Contains(resp.Header.Get("Content-Disposition"), "attachment") {
		t.Fatalf("headers = %v", resp.Header)
	}
	a.must("GET", "/api/v1/backup-reports/compliance.csv?from=2026-11-10&to=2026-11-01", nil, 400)
	a.must("GET", "/api/v1/backup-reports/compliance.csv?from=nope", nil, 400)
	if len(e.auditFor("backup.report_export")) == 0 {
		t.Fatal("report export not audited")
	}
}

func TestCSVInjectionGuard(t *testing.T) {
	for in, want := range map[string]string{"=1+1": "'=1+1", "+cmd": "'+cmd", "-2": "'-2", "@SUM(A1)": "'@SUM(A1)", "\tx": "'\tx", "\rx": "'\rx", "plain": "plain", "": "", "a=b": "a=b"} {
		if got := csvText(in); got != want {
			t.Errorf("csvText(%q) = %q, want %q", in, got, want)
		}
	}
	// End to end: a hostile source/job name is neutralised in the file.
	e := newP56Env(t, nil)
	job := backup.Job{ID: "bjob-evil", Name: `=HYPERLINK("http://evil","x")`, EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "+evil-source", Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
	if err := e.st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	a := e.admin()
	_, raw := a.do("GET", "/api/v1/backup-reports/compliance.csv", nil)
	recs, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(raw, "\xEF\xBB\xBF"))).ReadAll()
	if err != nil || len(recs) != 2 {
		t.Fatalf("csv = %v %v", err, recs)
	}
	for i, c := range recs[1] {
		if c != "" && strings.ContainsRune("=+-@", rune(c[0])) {
			t.Fatalf("cell %d (%s) starts with a formula character: %q", i, recs[0][i], c)
		}
	}
	if recs[1][0] != "'+evil-source" || !strings.HasPrefix(recs[1][2], "'=HYPERLINK") {
		t.Fatalf("row = %v", recs[1])
	}
}

// ---- audit ----

func TestBackupWritesAreAuditedWithUserAndDiff(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	ctx := context.Background()
	h := newHook(t, 200)
	last := func() (string, map[string]any) {
		ls := e.auditLines()
		l := ls[len(ls)-1]
		return l["action"].(string), l
	}
	expect := func(action string) map[string]any {
		t.Helper()
		var found map[string]any
		for _, l := range e.auditLines() {
			if l["action"] == action {
				found = l
			}
		}
		if found == nil {
			t.Fatalf("no audit entry %q", action)
		}
		if found["user"] != "admin" {
			t.Fatalf("%s: audit entry has no acting user: %v", action, found)
		}
		return found
	}
	_ = last
	// connection
	cid := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("audited"), 201))
	expect("backup_connection.create")
	up := storedConn("audited")
	up["host"] = "db9.internal"
	delete(up, "password")
	a.must("PUT", "/api/v1/backup-connections/"+cid, up, 200)
	det := expect("backup_connection.update")["detail"].(map[string]any)["changes"].(map[string]any)
	if det["host"] == nil {
		t.Fatalf("connection update lacks the changed field: %v", det)
	}
	a.must("POST", "/api/v1/backup-connections/"+cid+"/test", nil, 200)
	expect("backup_connection.test")
	// policy
	pol := backup.BuiltinPolicies()[1]
	pol.ID, pol.Name, pol.Builtin = "pol-audit", "audit policy", false
	a.must("POST", "/api/v1/backup-policies", pol, 201)
	expect("backup_policy.create")
	pol.RPOMinutes = 90
	a.must("PUT", "/api/v1/backup-policies/pol-audit", pol, 200)
	if ch := expect("backup_policy.update")["detail"].(map[string]any)["changes"]; ch == nil || !strings.Contains(fmt.Sprint(ch), "rpo_minutes") {
		t.Fatalf("policy update diff = %v", ch)
	}
	// job
	jb := mysqlJobBody("audit-job", cid)
	jid := decode(t, a.must("POST", "/api/v1/backup-jobs", jb, 201))["job"].(map[string]any)["id"].(string)
	expect("backup_job.create")
	jb["every_seconds"] = 7200
	jb["name"] = "audit-job-renamed"
	a.must("PUT", "/api/v1/backup-jobs/"+jid, jb, 200)
	jc := expect("backup_job.update")["detail"].(map[string]any)["changes"].(map[string]any)
	if jc["name"] == nil || jc["every_seconds"] == nil {
		t.Fatalf("job update diff = %v", jc)
	}
	a.must("POST", "/api/v1/backup-jobs/"+jid+"/run", nil, 202)
	expect("backup_job.run")
	a.must("POST", "/api/v1/backup-jobs/"+jid+"/start", nil, 200)
	expect("backup_job.start")
	a.must("POST", "/api/v1/backup-jobs/"+jid+"/stop", nil, 200)
	expect("backup_job.stop")
	a.must("POST", "/api/v1/backup-jobs/"+jid+"/prune", nil, 202)
	expect("backup_job.prune")
	a.must("POST", "/api/v1/backup-jobs/"+jid+"/verify", nil, 202)
	expect("backup_job.verify")
	// backups: legal hold, verify, copy, restore
	seedComplete(t, e.st, "bk-aud", jid, e.srv.now().UTC().Add(-time.Hour), backup.KindFull)
	a.must("POST", "/api/v1/backups/bk-aud/legal-hold", map[string]any{"hold": true}, 200)
	expect("backup.legal_hold")
	a.must("POST", "/api/v1/backups/bk-aud/verify", nil, 202)
	expect("backup.verify")
	rid := decode(t, a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Audit Repo", "http://127.0.0.1:1"), 200))["repository"].(map[string]any)["id"].(string)
	expect("backup_repository.create")
	a.must("POST", "/api/v1/backups/bk-aud/copy", map[string]any{"repository": rid}, 202)
	expect("backup.copy")
	a.must("POST", "/api/v1/backup-restores", map[string]any{"backup_id": "bk-aud", "connection_id": restoreConn(t, e.st, "mysql"), "target_database": "r1", "confirm": "r1"}, 202)
	expect("backup.restore")
	upr := storedRepoBody("Audit Repo", "http://127.0.0.1:1")
	delete(upr, "access_key")
	delete(upr, "secret_key")
	upr["bucket"] = "other-bucket"
	a.must("PUT", "/api/v1/backup-repositories/"+rid, upr, 200)
	if rc := expect("backup_repository.update")["detail"].(map[string]any)["changes"].(map[string]any); rc["bucket"] == nil {
		t.Fatalf("repo diff = %v", rc)
	}
	a.must("POST", "/api/v1/backup-repositories/"+rid+"/test", nil, 200)
	expect("backup_repository.test")
	// channels and rules
	chID := decode(t, a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "aud", "kind": "webhook", "url": h.URL + "/x-canary"}, 201))["channel"].(map[string]any)["id"].(string)
	expect("backup_notify_channel.create")
	a.must("PUT", "/api/v1/backup-notify-channels/"+chID, map[string]any{"name": "aud2", "kind": "webhook", "url": h.URL + "/y-canary"}, 200)
	if cc := expect("backup_notify_channel.update")["detail"].(map[string]any)["changes"].(map[string]any); cc["secret"] != "changed" || cc["name"] == nil {
		t.Fatalf("channel diff = %v", cc)
	}
	a.must("POST", "/api/v1/backup-notify-channels/"+chID+"/test", nil, 200)
	expect("backup_notify_channel.test")
	ruleID := decode(t, a.must("POST", "/api/v1/backup-notify-rules", map[string]any{"event": "failure", "scope_kind": "all", "channel_id": chID}, 201))["rule"].(map[string]any)["id"].(string)
	expect("backup_notify_rule.create")
	a.must("PUT", "/api/v1/backup-notify-rules/"+ruleID, map[string]any{"enabled": false}, 200)
	expect("backup_notify_rule.update")
	a.must("DELETE", "/api/v1/backup-notify-rules/"+ruleID, nil, 204)
	expect("backup_notify_rule.delete")
	a.must("DELETE", "/api/v1/backup-notify-channels/"+chID, nil, 204)
	expect("backup_notify_channel.delete")
	// deletes
	a.must("DELETE", "/api/v1/backup-jobs/"+jid, nil, 204)
	expect("backup_job.delete")
	a.must("DELETE", "/api/v1/backup-policies/pol-audit", nil, 204)
	expect("backup_policy.delete")
	a.must("DELETE", "/api/v1/backup-connections/"+cid, nil, 204)
	expect("backup_connection.delete")
	// Cancel of a running run.
	cancelRun(t, e, a)
	expect("backup_run.cancel")
	if txt := e.auditText(); strings.Contains(txt, canary) || strings.Contains(txt, "-canary") {
		t.Fatalf("a secret reached the audit log")
	}
	// The DB copy of the log carries the same entries (the activity feed reads it).
	rows, _ := e.st.ListAuditFiltered(ctx, store.AuditFilter{ActionPrefix: "backup", Limit: 500})
	if len(rows) < 30 {
		t.Fatalf("only %d backup audit rows in the database", len(rows))
	}
}

// cancelRun starts a real child process, records a running run owned by this instance and cancels it.
func cancelRun(t *testing.T, e *cpEnv, a *apiClient) {
	t.Helper()
	ctx := context.Background()
	job := backup.Job{ID: "bjob-cancel", Name: "to cancel", EverySeconds: 3600, Bucket: "central-backups", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump,
		Source: "src-cancel", Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
	if err := e.st.CreateBackupJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	runner, _, dir := newRunner(t)
	hold := filepath.Join(dir, "hold")
	if err := os.WriteFile(hold, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(hold) })
	e.srv.SetJobRunner(runner)
	id, err := runner.Start(ctx, JobSpec{Pipeline: backupJobRunPipeline(job.ID), Kind: "backup-run"})
	if err != nil {
		t.Fatal(err)
	}
	e.srv.trackBackupProcess(id)
	if err := e.st.CreateBackupRun(ctx, backup.Run{ID: "brun-cancel", JobID: job.ID, Trigger: backup.TriggerManual, StartedAt: e.srv.now().UTC(),
		Status: string(backup.RunStatusRunning), ProcessID: id}); err != nil {
		t.Fatal(err)
	}
	a.must("POST", "/api/v1/backup-runs/brun-cancel/cancel", nil, 200)
}

func TestBackupActivityFeed(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	for i := 0; i < 5; i++ {
		a.must("POST", "/api/v1/backup-repositories", storedRepoBody(fmt.Sprintf("Feed %d", i), "http://127.0.0.1:1"), 200)
	}
	_ = os.Remove // keep os imported for helpers
	// Non-backup audit noise must not appear.
	_ = e.srv.audit.Log(AuditEntry{Action: "pipeline.start", User: "admin", Result: "ok"})
	var page1 struct {
		Activity []activityItem `json:"activity"`
		Next     string         `json:"next_cursor"`
	}
	out := a.must("GET", "/api/v1/backup-activity?limit=2", nil, 200)
	if err := json.Unmarshal([]byte(out), &page1); err != nil || len(page1.Activity) != 2 || page1.Next == "" {
		t.Fatalf("page 1 = %s", out)
	}
	seen := map[int64]bool{}
	var all []activityItem
	next := ""
	for i := 0; i < 20; i++ {
		var pg struct {
			Activity []activityItem `json:"activity"`
			Next     string         `json:"next_cursor"`
		}
		url := "/api/v1/backup-activity?limit=2"
		if next != "" {
			url += "&cursor=" + next
		}
		if err := json.Unmarshal([]byte(a.must("GET", url, nil, 200)), &pg); err != nil {
			t.Fatal(err)
		}
		for _, it := range pg.Activity {
			if seen[it.ID] {
				t.Fatalf("row %d listed twice", it.ID)
			}
			seen[it.ID] = true
			all = append(all, it)
		}
		if next = pg.Next; next == "" || len(pg.Activity) == 0 {
			break
		}
	}
	creates := 0
	for _, it := range all {
		if !strings.HasPrefix(it.Action, "backup") {
			t.Fatalf("non-backup row in the feed: %+v", it)
		}
		if it.Action == "backup_repository.create" {
			creates++
			if it.User != "admin" || it.Detail["repository"] == nil {
				t.Fatalf("create row = %+v", it)
			}
		}
		if strings.Contains(fmt.Sprint(it.Detail), canary) {
			t.Fatalf("secret in the feed: %+v", it)
		}
	}
	if creates != 5 {
		t.Fatalf("feed has %d repository creates, want 5 (rows %d)", creates, len(all))
	}
	// Filters.
	var f struct {
		Activity []activityItem `json:"activity"`
	}
	_ = json.Unmarshal([]byte(a.must("GET", "/api/v1/backup-activity?action=backup_repository.create&user=admin", nil, 200)), &f)
	if len(f.Activity) != 5 {
		t.Fatalf("filtered = %d", len(f.Activity))
	}
	_ = json.Unmarshal([]byte(a.must("GET", "/api/v1/backup-activity?user=nobody", nil, 200)), &f)
	if len(f.Activity) != 0 {
		t.Fatal("user filter")
	}
	a.must("GET", "/api/v1/backup-activity?action=pipeline", nil, 400)
	a.must("GET", "/api/v1/backup-activity?limit=0", nil, 400)
	a.must("GET", "/api/v1/backup-activity?cursor=x", nil, 400)
	// The page renders (CSP-safe) and pages with a cursor.
	_, page := a.e.testEnv.do(a.c, "GET", "/backups/activity", "", nil)
	if !strings.Contains(page, "backup_repository.create") || strings.Contains(page, `style="`) {
		t.Fatal("activity page")
	}
}

func TestMaskAuditSecrets(t *testing.T) {
	got := maskAuditSecrets(map[string]any{"password": "p", "password_env": "PW", "secret_key": "s", "url": "https://h", "name": "n",
		"nested": map[string]any{"token": "t", "x": []any{map[string]any{"uri": "u"}}}, "secret": "changed"})
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), `"p"`) || strings.Contains(string(b), "https://h") || strings.Contains(string(b), `"t"`) || strings.Contains(string(b), `"u"`) ||
		!strings.Contains(string(b), `"password_env":"PW"`) || !strings.Contains(string(b), `"secret":"changed"`) || !strings.Contains(string(b), `"name":"n"`) {
		t.Fatalf("masked = %s", b)
	}
	old := backup.Repo{ID: "r", Name: "n", Bucket: "bucket-a", AccessKeySealed: []byte("x")}
	nw := old
	nw.Bucket, nw.AccessKeySealed = "bucket-b", []byte("y")
	ch := repoChanges(&old, nw)
	if ch["bucket"] == nil || ch["access_key"] != "changed" {
		t.Fatalf("changes = %v", ch)
	}
	_ = time.Now
}

func TestOpsPagesRenderAndFollowTheDesignRules(t *testing.T) {
	t.Setenv("STRAND_BACKUP_ENCRYPTION_KEYS", "k-now:"+strings.Repeat("c3", 32))
	t.Setenv("STRAND_BACKUP_NOTIFY_WEBHOOK", "https://env.example.com/hook")
	e := newP56Env(t, nil)
	a := e.admin()
	rid := decode(t, a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Pages", "http://127.0.0.1:1"), 200))["repository"].(map[string]any)["id"].(string)
	a.must("POST", "/api/v1/backup-repositories/"+rid+"/test", nil, 200)
	seedInRepo(t, e.st, "bk-pg1", rid, "src-pages", e.srv.now().UTC().Add(-time.Hour))
	h := newHook(t, 200)
	ch := decode(t, a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "pg-hook", "kind": "webhook", "url": h.URL}, 201))["channel"].(map[string]any)["id"].(string)
	a.must("POST", "/api/v1/backup-notify-rules", map[string]any{"event": "failure", "scope_kind": "all", "channel_id": ch}, 201)
	for _, p := range []string{"/backups/repositories", "/backups/repositories/" + rid, "/backups/notifications", "/backups/keys", "/backups/activity"} {
		resp, body := a.e.testEnv.do(a.c, "GET", p, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("%s = %d", p, resp.StatusCode)
		}
		for _, bad := range []string{` style="`, "<style", "onclick=", "onload=", "<script>"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s contains %q (CSP)", p, bad)
			}
		}
		if !strings.Contains(body, `class="breadcrumb"`) || !strings.Contains(body, `class="pagehead"`) || !strings.Contains(body, "<h1>") {
			t.Errorf("%s lacks breadcrumb/pagehead/h1", p)
		}
		if regexp.MustCompile(`<div class="card[ "]`).MatchString(body) {
			t.Errorf("%s uses div.card", p)
		}
		for _, tb := range regexp.MustCompile(`<table[^>]*>`).FindAllString(body, -1) {
			if !strings.Contains(tb, "data-table") {
				t.Errorf("%s: table without data-table: %s", p, tb)
			}
		}
		if strings.Count(body, "<table") != strings.Count(body, "<caption") {
			t.Errorf("%s: every table needs a caption", p)
		}
		for _, b := range regexp.MustCompile(`<button[^>]*>`).FindAllString(body, -1) {
			if !regexp.MustCompile(`class="[^"]*(btn|iconbtn|tab|chip|segbtn|sortbtn)`).MatchString(b) {
				t.Errorf("%s: button without a design-system class: %s", p, b)
			}
		}
	}
	_, repos := a.e.testEnv.do(a.c, "GET", "/backups/repositories", "", nil)
	if !strings.Contains(repos, "secret-bucket") && !strings.Contains(repos, "strand-test") || !strings.Contains(repos, "stored") || !strings.Contains(repos, `id="bk-repo-new"`) {
		t.Fatalf("repositories page: %s", repos)
	}
	_, notify := a.e.testEnv.do(a.c, "GET", "/backups/notifications", "", nil)
	if !strings.Contains(notify, "from environment") || !strings.Contains(notify, "pg-hook") || strings.Contains(notify, "env.example.com") {
		t.Fatal("notifications page: env channel must be listed read-only without its destination")
	}
	_, detail := a.e.testEnv.do(a.c, "GET", "/backups/repositories/"+rid, "", nil)
	if !strings.Contains(detail, "Connection test") || !strings.Contains(detail, "list") || !strings.Contains(detail, "src-pages") {
		t.Fatal("repository detail lacks the checklist or usage")
	}
	// Roles: operators and viewers see repositories but no write controls; admin-only pages are refused.
	for _, role := range []store.Role{store.RoleOperator, store.RoleViewer} {
		c := e.roleClient(role)
		code, body := c.do("GET", "/backups/repositories", nil)
		if code != 200 || strings.Contains(body, `id="bk-repo-new"`) {
			t.Fatalf("%s: repositories page = %d, new button present: %v", role, code, strings.Contains(body, `id="bk-repo-new"`))
		}
		for _, p := range []string{"/backups/notifications", "/backups/keys", "/backups/activity"} {
			if code, _ := c.do("GET", p, nil); code != 403 {
				t.Fatalf("%s: %s = %d, want 403", role, p, code)
			}
		}
	}
	a.must("GET", "/backups/repositories/nope", nil, 404)
}

// Editing a repository's credentials restarts the schedulers that use it: their environment is fixed at start.
func TestRepoCredentialEditRestartsSchedulers(t *testing.T) {
	e := newP56Env(t, nil)
	a := e.admin()
	id := decode(t, a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Rot", "http://127.0.0.1:9"), 200))["repository"].(map[string]any)["id"].(string)
	job := backup.Job{ID: "bjob-rot", Name: "rot", EverySeconds: 3600, Bucket: "central-backups", Enabled: true, JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump,
		Source: "src-rot", Repository: id, Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
	if err := e.st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	e.rec.take()
	upd := storedRepoBody("Rot", "http://127.0.0.1:9")
	upd["secret_key"] = "ROTATED-" + canary
	delete(upd, "access_key")
	out := a.must("PUT", "/api/v1/backup-repositories/"+id, upd, 200)
	if !strings.Contains(out, `"restarted_jobs"`) || !strings.Contains(out, "bjob-rot") {
		t.Fatalf("response = %s", out)
	}
	found := false
	for _, sp := range e.rec.take() {
		if sp.Kind == "backup-schedule" && strings.Contains(strings.Join(sp.Env, "\n"), "SECRET_KEY=ROTATED-"+canary) {
			found = true
		}
	}
	if !found {
		t.Fatal("no scheduler was started with the rotated key")
	}
	// A rename alone does not touch the schedulers.
	upd2 := storedRepoBody("Rot renamed", "http://127.0.0.1:9")
	delete(upd2, "access_key")
	delete(upd2, "secret_key")
	if out := a.must("PUT", "/api/v1/backup-repositories/"+id, upd2, 200); strings.Contains(out, "restarted_jobs") {
		t.Fatalf("a rename restarted schedulers: %s", out)
	}
}
