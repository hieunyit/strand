package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/web/store"
	"strings"
	"testing"
	"time"
)

func TestAuditUnmaskReordered(t *testing.T) {
	orig := []byte("source:\n  password: source-secret\ntarget:\n  password: target-secret\n")
	edited := "target:\n  password: \"********\"\nsource:\n  password: \"********\"\n"
	got, err := UnmaskConfigText(edited, orig)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "target:\n  password: target-secret") {
		t.Errorf("passwords assigned by occurrence rather than YAML path: %s", got)
	}
}
func TestAuditEnvSavePersistsRegistration(t *testing.T) {
	e := newEnv(t, nil)
	p := e.srv.reg.Get("orders")
	if _, err := e.srv.saveEnvContent(context.Background(), p, map[string]string{"SRC_PW": "test-secret"}, "test"); err != nil {
		t.Fatal(err)
	}
	if e.srv.reg.Get("orders").EnvFile == "" {
		t.Error("saveEnvContent returns success but Refresh loses new env_file")
	}
}
func TestAuditSecretsInAudit(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	code, _ := e.act(c, csrf, "orders", "env-save", map[string]string{"entries": `{"SRC_PW":"audit-canary-secret"}`})
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	raw, err := os.ReadFile(e.srv.audit.Path())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "audit-canary-secret") {
		t.Error("plaintext password saved into audit log")
	}
}
func TestAuditConfigDiffLeaksSecret(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	raw, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, []byte("\n# comment\npassword: diff-canary-secret\n")...)
	e.writePipelineFile("orders", p.ConfigPath, string(raw), 0o600)
	masked, err := MaskConfigText(raw)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.srv.validateConfigContent(context.Background(), p, masked+"# next\n")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Diff, "diff-canary-secret") {
		t.Error("config diff discloses originally masked password")
	}
}
func TestAuditLimiterMemory(t *testing.T) {
	now := time.Now()
	l := NewLoginLimiter(func() time.Time { return now })
	l.Fail("192.0.2.1")
	now = now.Add(24 * time.Hour)
	l.Fail("192.0.2.2")
	if _, ok := l.m["192.0.2.1"]; ok {
		t.Error("expired failed IP is never removed")
	}
}

func TestConfigDiffBoundsWork(t *testing.T) {
	got := unifiedDiff("old", "new", strings.Repeat("old\n", 10000), strings.Repeat("new\n", 10000))
	if !strings.Contains(got, "diff omitted") {
		t.Fatal("large diff must use bounded fallback")
	}
}

func TestEnvDynamicArgsRedacted(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	code, _ := e.act(c, csrf, "orders", "env-save", map[string]string{"CUSTOM": "dynamic-canary-secret"})
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	raw, err := os.ReadFile(e.srv.audit.Path())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "dynamic-canary-secret") {
		t.Fatal("dynamic env value leaked")
	}
}

type rejectPipelineWrites struct{ store.Store }

func (s rejectPipelineWrites) UpsertPipeline(context.Context, store.Pipeline) error {
	return errors.New("test database unavailable")
}

func (s rejectPipelineWrites) UpdatePipelineEnv(context.Context, string, string, []byte) (bool, error) {
	return false, errors.New("test database unavailable")
}

func (s rejectPipelineWrites) UpdatePipelineConfigCompareAndSwap(context.Context, string, bool, string, string) (bool, error) {
	return false, errors.New("test database unavailable")
}

func TestEditPersistenceFailureRollsBack(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	orig, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMem()
	if err := st.UpsertPipeline(t.Context(), store.Pipeline{Name: p.Name, ConfigPath: p.ConfigPath, ConfigYAML: string(orig), HasConfig: true}); err != nil {
		t.Fatal(err)
	}
	db, err := newMetaDB(rejectPipelineWrites{st}, "0123456789abcdef0123456789abcdef", e.srv.log)
	if err != nil {
		t.Fatal(err)
	}
	e.srv.db = db
	if _, err := e.srv.saveConfigContent(t.Context(), p, string(orig)+"\n# updated\n", false, false, "test"); err == nil {
		t.Fatal("config save succeeded despite database failure")
	}
	got, _ := os.ReadFile(p.ConfigPath)
	if string(got) != string(orig) {
		t.Fatal("config not rolled back")
	}
	envPath := e.srv.envPathFor(p)
	if err := os.Remove(envPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"CUSTOM": "value"}, "test"); err == nil {
		t.Fatal("env save succeeded despite database failure")
	}
	if _, err := os.Stat(envPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new env was not removed: %v", err)
	}
}

func TestEnvRegistrationSurvivesRegistryRestart(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"CUSTOM": "value"}, "test"); err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry(e.srv.cfg, e.cmd)
	reg.db = e.srv.db
	reg.Refresh(t.Context())
	if got := reg.Get(p.Name); got == nil || got.EnvFile != e.srv.envPathFor(p) {
		t.Fatalf("env path lost after restart: %+v", got)
	}
}

func TestEnvSavePersistsNewDBPath(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	orig, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewMem()
	if err := st.UpsertPipeline(t.Context(), store.Pipeline{Name: p.Name, ConfigPath: p.ConfigPath, ConfigYAML: string(orig), HasConfig: true}); err != nil {
		t.Fatal(err)
	}
	db, err := newMetaDB(st, "0123456789abcdef0123456789abcdef", e.srv.log)
	if err != nil {
		t.Fatal(err)
	}
	e.srv.db = db
	if _, err := e.srv.saveEnvContent(t.Context(), p, map[string]string{"CUSTOM": "value"}, "test"); err != nil {
		t.Fatal(err)
	}
	row, _, err := st.GetPipeline(t.Context(), p.Name)
	if err != nil {
		t.Fatal(err)
	}
	if row.EnvPath == "" || len(row.EnvEnc) == 0 {
		t.Fatal("new env path/content not stored")
	}
}

func TestEnvRejectsInvalidNames(t *testing.T) {
	for _, key := range []string{"BAD\nINJECTED", "BAD=KEY", "$BAD", "1BAD", "", "A-B"} {
		if _, err := formatEnvLine(key, "value"); err == nil {
			t.Errorf("accepted invalid key %q", key)
		}
	}
	for _, key := range []string{"GOOD", "_GOOD", "GOOD_1"} {
		if _, err := formatEnvLine(key, "value"); err != nil {
			t.Errorf("rejected valid key %q: %v", key, err)
		}
	}
}

// A stored backup-connection secret exists in plaintext only inside the Web process while it is handed to a
// backup process through that process's environment: it must not appear in any API response, HTML page,
// metrics output, audit entry, log line or process argv, and never in the clear in the database.
func TestStoredConnectionSecretAppearsNowhere(t *testing.T) {
	e := newCPEnv(t)
	var logs strings.Builder
	e.srv.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a := e.admin()
	var seen []string
	rec := func(s string) string { seen = append(seen, s); return s }
	id := connID(t, rec(a.must("POST", "/api/v1/backup-connections", storedConn("secret-conn"), 201)))
	rec(a.must("GET", "/api/v1/backup-connections", nil, 200))
	rec(a.must("GET", "/api/v1/backup-connections/"+id, nil, 200))
	rec(a.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 200))
	rec(a.must("POST", "/api/v1/backup-connections/test", storedConn("secret-conn"), 200))
	upd := storedConn("secret-conn")
	upd["password"] = canary + "-2"
	rec(a.must("PUT", "/api/v1/backup-connections/"+id, upd, 200))
	b := mysqlJobBody("secret-job", id)
	b["enabled"] = true
	rec(a.must("POST", "/api/v1/backup-jobs", b, 201))
	rec(a.must("GET", "/api/v1/backup-jobs", nil, 200))
	rec(a.must("GET", "/api/v1/backup-policies", nil, 200))
	for _, page := range []string{"/backups", "/audit", "/", "/jobs"} {
		_, body := a.e.testEnv.do(a.c, "GET", page, "", nil)
		rec(body)
	}
	_, metrics := a.e.testEnv.do(a.c, "GET", "/metrics", "", nil)
	rec(metrics)
	sealed, _, _ := e.st.GetBackupConnection(context.Background(), id)
	b64 := base64.StdEncoding.EncodeToString(sealed.PasswordSealed)
	for i, s := range seen {
		if strings.Contains(s, canary) || strings.Contains(s, b64) || strings.Contains(s, base64.RawStdEncoding.EncodeToString(sealed.PasswordSealed)) {
			t.Fatalf("surface %d leaks the connection secret", i)
		}
	}
	for _, sp := range e.rec.take() {
		if strings.Contains(strings.Join(sp.Args, "\x00"), canary) || strings.Contains(strings.Join(sp.DisplayArgs, "\x00"), canary) {
			t.Fatalf("secret in argv of %s", sp.Kind)
		}
	}
	if strings.Contains(e.auditText(), canary) {
		t.Fatal("secret in the audit log")
	}
	if strings.Contains(logs.String(), canary) {
		t.Fatalf("secret in the log: %s", logs.String())
	}
	row, _, _ := e.st.GetBackupConnection(context.Background(), id)
	if strings.Contains(string(row.PasswordSealed), canary) || len(row.PasswordSealed) == 0 {
		t.Fatal("secret not sealed in the database row")
	}
	// The Go value serializes without any secret field, so no future handler can leak it by accident.
	raw, _ := json.Marshal(row)
	if strings.Contains(string(raw), "ealed") || strings.Contains(string(raw), canary) || !strings.Contains(string(raw), `"password_set":true`) {
		t.Fatalf("connection JSON = %s", raw)
	}
}

// Stored repository credentials and notification destinations exist in plaintext only inside the Web process
// (and, for repository keys, the ENVIRONMENT of the backup processes it starts). They must not appear in any
// API response, HTML page, metrics output, CSV, audit entry, log line or process argv, never in the clear in the
// database, and the Go values must serialize without any secret field.
func TestStoredRepoAndChannelSecretsAppearNowhere(t *testing.T) {
	const akCanary, skCanary, urlCanary, smtpCanary = "AKIA-repo-canary-31337", "SK-repo-canary-31337", "hook-url-canary-31337", "smtp-pass-canary-31337"
	hook := newHook(t, 200)
	mail := &mailRecorder{}
	e := newP56Env(t, func(o *Options) { o.NotifySendMail = mail.send })
	var logs strings.Builder
	e.srv.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a := e.admin()
	var seen []string
	rec := func(s string) string { seen = append(seen, s); return s }

	repo := map[string]any{"name": "secret-repo", "secret_mode": "stored", "endpoint": "http://127.0.0.1:1", "region": "us-east-1", "bucket": "secret-bucket",
		"allow_insecure_local": true, "access_key": akCanary, "secret_key": skCanary}
	rid := decode(t, rec(a.must("POST", "/api/v1/backup-repositories", repo, 200)))["repository"].(map[string]any)["id"].(string)
	rec(a.must("GET", "/api/v1/backup-repositories", nil, 200))
	rec(a.must("GET", "/api/v1/backup-repositories/"+rid, nil, 200))
	rec(a.must("POST", "/api/v1/backup-repositories/"+rid+"/test", nil, 200))
	repo["secret_key"] = skCanary + "-2"
	rec(a.must("PUT", "/api/v1/backup-repositories/"+rid, repo, 200))
	rec(a.must("POST", "/api/v1/backup-repositories/"+rid+"/test", nil, 200))

	cid := decode(t, rec(a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "secret-hook", "kind": "webhook", "url": hook.URL + "/" + urlCanary}, 201)))["channel"].(map[string]any)["id"].(string)
	mid := decode(t, rec(a.must("POST", "/api/v1/backup-notify-channels", map[string]any{"name": "secret-mail", "kind": "email", "smtp_addr": "smtp.example.com:587",
		"from": "strand@example.com", "to": []string{"ops@example.com"}, "user": "mailer", "password": smtpCanary}, 201)))["channel"].(map[string]any)["id"].(string)
	rec(a.must("GET", "/api/v1/backup-notify-channels", nil, 200))
	rec(a.must("GET", "/api/v1/backup-notify-channels/"+cid, nil, 200))
	rec(a.must("POST", "/api/v1/backup-notify-channels/"+cid+"/test", nil, 200))
	rec(a.must("POST", "/api/v1/backup-notify-channels/"+mid+"/test", nil, 200))
	rec(a.must("PUT", "/api/v1/backup-notify-channels/"+cid, map[string]any{"name": "secret-hook", "kind": "webhook", "url": hook.URL + "/" + urlCanary + "-2"}, 200))
	rec(a.must("POST", "/api/v1/backup-notify-rules", map[string]any{"event": "failure", "scope_kind": "all", "channel_id": cid}, 201))
	e.srv.notifyBackupEvent(runner.Event{Kind: "failure", Message: "x", At: e.srv.now()}, "")
	// A job on the repository, a child process that carries the repository environment, and every read surface.
	job := backup.Job{ID: "bjob-sec", Name: "sec job", EverySeconds: 3600, Bucket: "central-backups", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump,
		Source: "src-sec", Repository: rid, Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
	if err := e.st.CreateBackupJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	rec(a.must("POST", "/api/v1/backup-jobs/bjob-sec/prune", nil, 202))
	rec(a.must("GET", "/api/v1/backup-jobs", nil, 200))
	for _, p := range []string{"/api/v1/backup-keys", "/api/v1/backup-rpo", "/api/v1/backup-reports/compliance", "/api/v1/backup-reports/compliance.csv",
		"/api/v1/backup-activity", "/api/v1/backup-activity?limit=200", "/api/v1/audit", "/metrics", "/backups", "/backups/repositories", "/backups/repositories/" + rid,
		"/backups/notifications", "/backups/keys", "/backups/activity", "/backups/jobs", "/audit", "/"} {
		_, body := a.e.testEnv.do(a.c, "GET", p, "", nil)
		rec(body)
	}
	row, _, _ := e.st.GetBackupRepo(context.Background(), rid)
	ch, _, _ := e.st.GetNotifyChannel(context.Background(), cid)
	mch, _, _ := e.st.GetNotifyChannel(context.Background(), mid)
	forms := []string{base64.StdEncoding.EncodeToString(row.AccessKeySealed), base64.StdEncoding.EncodeToString(row.SecretKeySealed),
		base64.RawStdEncoding.EncodeToString(row.SecretKeySealed), base64.StdEncoding.EncodeToString(ch.SecretSealed), base64.StdEncoding.EncodeToString(mch.SecretSealed)}
	for i, s := range seen {
		for _, c := range []string{akCanary, skCanary, urlCanary, smtpCanary} {
			if strings.Contains(s, c) {
				t.Fatalf("surface %d leaks %q", i, c)
			}
		}
		for _, f := range forms {
			if strings.Contains(s, f) {
				t.Fatalf("surface %d carries a sealed value", i)
			}
		}
		if strings.Contains(s, "_sealed") || strings.Contains(s, "Sealed") {
			t.Fatalf("surface %d exposes a sealed secret field", i)
		}
	}
	children := e.rec.take()
	childEnv := false
	for _, sp := range children {
		joined := strings.Join(sp.Args, "\x00") + strings.Join(sp.DisplayArgs, "\x00")
		for _, c := range []string{akCanary, skCanary, urlCanary, smtpCanary} {
			if strings.Contains(joined, c) {
				t.Fatalf("secret in argv of %s", sp.Kind)
			}
		}
		if strings.Contains(strings.Join(sp.Env, "\n"), skCanary+"-2") {
			childEnv = true
		}
	}
	if !childEnv {
		t.Fatal("the stored repository key never reached a child environment (the runner would not be able to use it)")
	}
	if txt := e.auditText(); strings.Contains(txt, akCanary) || strings.Contains(txt, skCanary) || strings.Contains(txt, urlCanary) || strings.Contains(txt, smtpCanary) {
		t.Fatal("secret in the audit log")
	}
	for _, c := range []string{akCanary, skCanary, urlCanary, smtpCanary} {
		if strings.Contains(logs.String(), c) {
			t.Fatalf("secret %q in the log: %s", c, logs.String())
		}
	}
	// Never in the clear in the database; the sealed values open only under their own id.
	for name, b := range map[string][]byte{"access key": row.AccessKeySealed, "secret key": row.SecretKeySealed, "webhook url": ch.SecretSealed, "smtp password": mch.SecretSealed} {
		if len(b) == 0 || strings.Contains(string(b), "canary") {
			t.Fatalf("%s is not sealed in the row", name)
		}
	}
	if _, err := e.srv.db.box.Open(row.SecretKeySealed, cid); err == nil {
		t.Fatal("a repository key opens under a channel id")
	}
	// The Go values serialize without any secret field, so no future handler can leak one by accident.
	for _, v := range []any{row, ch, mch} {
		raw, _ := json.Marshal(v)
		if strings.Contains(string(raw), "ealed") || strings.Contains(string(raw), "canary") || !strings.Contains(string(raw), "_set\":true") {
			t.Fatalf("JSON = %s", raw)
		}
	}
}
