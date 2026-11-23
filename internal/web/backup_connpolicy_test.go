package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/conncheck"
	"strand/internal/web/store"
)

const canary = "conn-canary-s3cr3t-9f2a"

type fakeChecker struct {
	mu     sync.Mutex
	got    []conncheck.Target
	result conncheck.Result
}

func (f *fakeChecker) Check(_ context.Context, t conncheck.Target) conncheck.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, t)
	r := f.result
	r.Engine = t.Engine
	return r
}

func (f *fakeChecker) last() conncheck.Target {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.got[len(f.got)-1]
}

// specRecorder records the JobSpec of every started process (argv + environment).
type specRecorder struct {
	backupStopTestRunner
	mu    sync.Mutex
	specs []JobSpec
}

func (r *specRecorder) Start(_ context.Context, spec JobSpec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.specs = append(r.specs, spec)
	return "proc-" + spec.Pipeline, nil
}

func (r *specRecorder) take() []JobSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.specs
	r.specs = nil
	return out
}

type cpEnv struct {
	*testEnv
	st   *store.Mem
	fake *fakeChecker
	rec  *specRecorder
}

func newCPEnv(t *testing.T) *cpEnv {
	t.Helper()
	st := store.NewMem()
	fake := &fakeChecker{result: conncheck.Result{OK: true, Version: "8.4.0", MissingPrivileges: []string{"RELOAD"}, Databases: []string{"app", "billing"},
		GTIDMode: "ON", PITRAllowed: true, PITRMethods: []string{"mysqldump"}}}
	e := dbEnv(t, st, nil, func(o *Options) { o.ConnChecker = fake })
	rec := &specRecorder{backupStopTestRunner: backupStopTestRunner{jobs: map[string]JobStatus{}}}
	e.srv.SetJobRunner(rec)
	return &cpEnv{testEnv: e, st: st, fake: fake, rec: rec}
}

type apiClient struct {
	e   *cpEnv
	c   *http.Client
	hdr map[string]string
}

func (e *cpEnv) admin() *apiClient {
	c, csrf := e.loginOK()
	return &apiClient{e: e, c: c, hdr: map[string]string{"X-CSRF-Token": csrf, "Content-Type": "application/json"}}
}

// roleClient authenticates with an API token of a new user with the role.
func (e *cpEnv) roleClient(role store.Role) *apiClient {
	h, _ := HashPassword("SecurePassword123!")
	u, err := e.srv.db.st.CreateUser(context.Background(), store.User{Username: "u_" + string(role), PasswordHash: h, Role: role})
	if err != nil {
		e.t.Fatal(err)
	}
	tok := NewAPIToken()
	if _, err := e.srv.db.st.CreateUserToken(context.Background(), store.UserToken{UserID: u.ID, Name: "t", TokenHash: hashKey(tok)}); err != nil {
		e.t.Fatal(err)
	}
	return &apiClient{e: e, c: e.client(), hdr: map[string]string{"Authorization": "Bearer " + tok, "Content-Type": "application/json"}}
}

func (a *apiClient) do(method, path string, body any) (int, string) {
	a.e.t.Helper()
	var b string
	switch v := body.(type) {
	case nil:
	case string:
		b = v
	default:
		raw, _ := json.Marshal(v)
		b = string(raw)
	}
	resp, out := a.e.testEnv.do(a.c, method, path, b, a.hdr)
	return resp.StatusCode, out
}

func (a *apiClient) must(method, path string, body any, want int) string {
	a.e.t.Helper()
	code, out := a.do(method, path, body)
	if code != want {
		a.e.t.Fatalf("%s %s = %d, want %d: %s", method, path, code, want, out)
	}
	return out
}

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("not JSON: %v: %s", err, s)
	}
	return m
}

func storedConn(name string) map[string]any {
	return map[string]any{"name": name, "engine": "mysql", "host": "db1.internal", "port": 3306, "user": "backup",
		"secret_mode": "stored", "password": canary, "tls_mode": "require"}
}

func (e *cpEnv) auditText() string {
	b, _ := os.ReadFile(e.srv.audit.Path())
	return string(b)
}

func containsStr(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func anyStrings(v any) []string {
	var out []string
	l, _ := v.([]any)
	for _, x := range l {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func connID(t *testing.T, out string) string {
	return decode(t, out)["connection"].(map[string]any)["id"].(string)
}

func TestConnectionStoredSecretNeverLeaves(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	out := a.must("POST", "/api/v1/backup-connections", storedConn("prod-mysql"), 201)
	conn := decode(t, out)["connection"].(map[string]any)
	id := conn["id"].(string)
	if conn["password_set"] != true || conn["secret_mode"] != "stored" {
		t.Fatalf("created = %v", conn)
	}
	row, _, _ := e.st.GetBackupConnection(context.Background(), id)
	if len(row.PasswordSealed) == 0 || strings.Contains(string(row.PasswordSealed), canary) {
		t.Fatal("secret not sealed in the store")
	}
	pt, err := e.srv.db.box.Open(row.PasswordSealed, id)
	if err != nil || string(pt) != canary {
		t.Fatalf("sealed value does not open under its id: %v", err)
	}
	if _, err := e.srv.db.box.Open(row.PasswordSealed, "another-id"); err == nil {
		t.Fatal("sealed value opens under another id")
	}
	surfaces := []string{out, a.must("GET", "/api/v1/backup-connections", nil, 200), a.must("GET", "/api/v1/backup-connections/"+id, nil, 200)}
	upd := storedConn("prod-mysql")
	delete(upd, "password")
	surfaces = append(surfaces, a.must("PUT", "/api/v1/backup-connections/"+id, upd, 200))
	surfaces = append(surfaces, a.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 200))
	for i, sfc := range surfaces {
		if strings.Contains(sfc, canary) || strings.Contains(sfc, string(row.PasswordSealed)) || strings.Contains(sfc, base64.StdEncoding.EncodeToString(row.PasswordSealed)) || strings.Contains(strings.ToLower(sfc), "sealed") {
			t.Fatalf("response %d leaks the secret: %s", i, sfc)
		}
	}
	if !strings.Contains(surfaces[2], `"password_set": true`) {
		t.Fatalf("GET lacks password_set: %s", surfaces[2])
	}
	row2, _, _ := e.st.GetBackupConnection(context.Background(), id)
	if string(row2.PasswordSealed) != string(row.PasswordSealed) {
		t.Fatal("update without password changed the stored secret")
	}
	upd["password"] = canary + "-new"
	a.must("PUT", "/api/v1/backup-connections/"+id, upd, 200)
	row3, _, _ := e.st.GetBackupConnection(context.Background(), id)
	if pt, _ := e.srv.db.box.Open(row3.PasswordSealed, id); string(pt) != canary+"-new" {
		t.Fatal("password not replaced")
	}
	// A stored connection can no longer be switched to reading its password from an environment variable.
	envMode := map[string]any{"name": "prod-mysql", "engine": "mysql", "host": "db1.internal", "secret_mode": "env", "password_env": "PROD_DB_PW"}
	a.must("PUT", "/api/v1/backup-connections/"+id, envMode, 400)
	row4, _, _ := e.st.GetBackupConnection(context.Background(), id)
	if row4.SecretMode != backup.SecretStored || !row4.PasswordSet || row4.PasswordEnv != "" {
		t.Fatalf("refused switch to env mode changed the connection: %+v", row4)
	}
	envMode["password"] = "x"
	a.must("PUT", "/api/v1/backup-connections/"+id, envMode, 400)
	audit := e.auditText()
	if !strings.Contains(audit, "backup_connection.create") || !strings.Contains(audit, "backup_connection.update") ||
		!strings.Contains(audit, `"password":"changed"`) || strings.Contains(audit, canary) {
		t.Fatalf("audit = %s", audit)
	}
}

func TestConnectionValidationAndConflicts(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "x", "engine": "oracle", "secret_mode": "env"}, 400)
	a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "x", "engine": "mysql", "host": "h", "secret_mode": "env", "password_env": "STRAND_WEB_SECRET_KEY"}, 400)
	a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "x", "engine": "mysql", "host": "h", "secret_mode": "env", "password": "p"}, 400)
	a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "x", "engine": "mysql", "host": "h", "bogus": 1}, 400)
	a.must("POST", "/api/v1/backup-connections", storedConn("dup"), 201)
	a.must("POST", "/api/v1/backup-connections", storedConn("dup"), 409)
	a.must("PUT", "/api/v1/backup-connections/nope", storedConn("n"), 404)
	a.must("GET", "/api/v1/backup-connections/nope", nil, 404)
	a.must("DELETE", "/api/v1/backup-connections/nope", nil, 404)
}

func mysqlJobBody(name, conn string) map[string]any {
	return map[string]any{"name": name, "method": "mysqldump", "bucket": "central-backups", "enabled": false, "every_seconds": 3600,
		"connection_id": conn, "conn": map[string]any{"databases": []string{"app"}}}
}

func TestConnectionDeleteBlockedByJobs(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	id := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	j1 := decode(t, a.must("POST", "/api/v1/backup-jobs", mysqlJobBody("j-one", id), 201))["job"].(map[string]any)["id"].(string)
	j2 := decode(t, a.must("POST", "/api/v1/backup-jobs", mysqlJobBody("j-two", id), 201))["job"].(map[string]any)["id"].(string)
	code, out := a.do("DELETE", "/api/v1/backup-connections/"+id, nil)
	if code != 409 || !strings.Contains(out, j1) || !strings.Contains(out, j2) {
		t.Fatalf("delete in use = %d %s", code, out)
	}
	var body struct {
		Jobs []string `json:"jobs"`
	}
	_ = json.Unmarshal([]byte(out), &body)
	if len(body.Jobs) != 2 {
		t.Fatalf("jobs list = %v", body.Jobs)
	}
	if _, ok, _ := e.st.GetBackupConnection(context.Background(), id); !ok {
		t.Fatal("connection deleted despite 409")
	}
	a.must("DELETE", "/api/v1/backup-jobs/"+j1, nil, 204)
	a.must("DELETE", "/api/v1/backup-jobs/"+j2, nil, 204)
	a.must("DELETE", "/api/v1/backup-connections/"+id, nil, 204)
}

func TestJobWithConnectionResolvesAndPassesSecretByEnvOnly(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	id := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	body := mysqlJobBody("with-conn", id)
	body["enabled"] = true
	out := a.must("POST", "/api/v1/backup-jobs", body, 201)
	job := decode(t, out)["job"].(map[string]any)
	conn := job["conn"].(map[string]any)
	if conn["host"] != "db1.internal" || conn["user"] != "backup" || conn["password_env"] != backup.EnvConnPassword || job["source"] != "prod" {
		t.Fatalf("job conn = %v", conn)
	}
	if strings.Contains(out, canary) {
		t.Fatal("job response carries the secret")
	}
	specs := e.rec.take()
	if len(specs) != 1 {
		t.Fatalf("started %d processes", len(specs))
	}
	if strings.Contains(strings.Join(specs[0].Args, " "), canary) || strings.Contains(strings.Join(specs[0].DisplayArgs, " "), canary) {
		t.Fatalf("secret in argv: %v", specs[0].Args)
	}
	if !containsStr(specs[0].Env, backup.EnvConnPassword+"="+canary) {
		t.Fatalf("secret not handed over by environment: %v", specs[0].Env)
	}
	jobID := job["id"].(string)
	a.must("POST", "/api/v1/backup-jobs/"+jobID+"/run", nil, 202)
	run := e.rec.take()
	if len(run) != 1 || !containsStr(run[0].Env, backup.EnvConnPassword+"="+canary) {
		t.Fatalf("run-now env = %v", run)
	}
	pg := mysqlJobBody("pg-on-mysql", id)
	pg["method"] = "pg_dump"
	a.must("POST", "/api/v1/backup-jobs", pg, 400)
	a.must("POST", "/api/v1/backup-jobs", mysqlJobBody("bad-ref", "no-such-conn"), 400)
	// New env-mode connections are refused; an existing (legacy) one keeps working without leaking a value.
	a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "env-conn", "engine": "mysql", "host": "h2",
		"secret_mode": "env", "password_env": "H2_PW"}, 400)
	legacy, err := e.st.CreateBackupConnection(context.Background(), backup.Connection{ID: "conn-legacy-h2", Name: "env-conn", Engine: backup.ConnMySQL,
		Host: "h2", SecretMode: backup.SecretEnv, PasswordEnv: "H2_PW"})
	if err != nil {
		t.Fatal(err)
	}
	envID := legacy.ID
	e.rec.take()
	b2 := mysqlJobBody("env-job", envID)
	b2["enabled"] = true
	a.must("POST", "/api/v1/backup-jobs", b2, 201)
	for _, sp := range e.rec.take() {
		for _, kv := range sp.Env {
			if strings.HasPrefix(kv, backup.EnvConnPassword+"=") && kv != backup.EnvConnPassword+"=" {
				t.Fatalf("env-mode connection leaked a value: %q", kv)
			}
		}
	}
}

func TestLegacyInlineJobUnchangedByConnections(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	body := map[string]any{"name": "legacy", "method": "mysqldump", "bucket": "central-backups", "enabled": true, "every_seconds": 3600,
		"source": "legacy-src", "conn": map[string]any{"host": "old-host", "user": "u", "password_env": "OLD_PW", "databases": []string{"app"}}}
	out := a.must("POST", "/api/v1/backup-jobs", body, 201)
	job := decode(t, out)["job"].(map[string]any)
	if job["conn"].(map[string]any)["host"] != "old-host" || job["connection_id"] != nil || job["policy_id"] != nil || job["schedule"] != nil {
		t.Fatalf("legacy job altered: %v", job)
	}
	specs := e.rec.take()
	if len(specs) != 1 || len(specs[0].Env) != 0 {
		t.Fatalf("legacy job env = %v", specs)
	}
}

func TestConnectionTestEndpoints(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	out := a.must("POST", "/api/v1/backup-connections/test", map[string]any{"engine": "mysql", "host": "db1", "user": "u",
		"secret_mode": "stored", "password": canary}, 200)
	res := decode(t, out)
	if res["ok"] != true || res["pitr_allowed"] != true || res["version"] != "8.4.0" || res["gtid_mode"] != "ON" {
		t.Fatalf("result = %s", out)
	}
	if tgt := e.fake.last(); tgt.Password != canary || tgt.Host != "db1" || tgt.Engine != "mysql" {
		t.Fatalf("checker target = %+v", tgt)
	}
	if strings.Contains(out, canary) || strings.Contains(e.auditText(), canary) {
		t.Fatal("test leaked the password")
	}
	a.must("POST", "/api/v1/backup-connections/test", map[string]any{"engine": "nope"}, 400)
	id := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	a.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 200)
	if tgt := e.fake.last(); tgt.Password != canary || tgt.Host != "db1.internal" || tgt.TLSMode != "require" {
		t.Fatalf("saved target = %+v", tgt)
	}
	row, _, _ := e.st.GetBackupConnection(context.Background(), id)
	if row.LastTestAt == nil || row.LastTestStatus != "warning" || !strings.Contains(row.LastTestDetail, "RELOAD") {
		t.Fatalf("last test = %v %q %q", row.LastTestAt, row.LastTestStatus, row.LastTestDetail)
	}
	a.must("POST", "/api/v1/backup-connections/test", map[string]any{"id": id, "name": "prod", "engine": "mysql", "host": "proposed.internal",
		"secret_mode": "stored"}, 200)
	if tgt := e.fake.last(); tgt.Host != "proposed.internal" || tgt.Password != "" || tgt.URI != "" {
		t.Fatal("unsaved edit must not reuse credentials from the saved connection")
	}
	e.fake.mu.Lock()
	e.fake.result = conncheck.Result{OK: false, Error: "connect: access denied", MissingPrivileges: []string{}, Databases: []string{}}
	e.fake.mu.Unlock()
	a.must("POST", "/api/v1/backup-connections/"+id+"/test", nil, 200)
	row, _, _ = e.st.GetBackupConnection(context.Background(), id)
	if row.LastTestStatus != "failed" || !strings.Contains(row.LastTestDetail, "access denied") {
		t.Fatalf("failed test recorded as %q %q", row.LastTestStatus, row.LastTestDetail)
	}
	a.must("POST", "/api/v1/backup-connections/missing/test", nil, 404)
	// An unsaved env-mode probe is refused; a legacy env-mode connection still tests with its variable.
	t.Setenv("CP_TEST_PW", "from-env-pw")
	a.must("POST", "/api/v1/backup-connections/test", map[string]any{"engine": "postgres", "host": "pg", "user": "u", "secret_mode": "env", "password_env": "CP_TEST_PW"}, 400)
	if _, err := e.st.CreateBackupConnection(context.Background(), backup.Connection{ID: "conn-legacy-pg", Name: "legacy-pg", Engine: backup.ConnPostgres,
		Host: "pg", User: "u", SecretMode: backup.SecretEnv, PasswordEnv: "CP_TEST_PW"}); err != nil {
		t.Fatal(err)
	}
	a.must("POST", "/api/v1/backup-connections/conn-legacy-pg/test", nil, 200)
	if e.fake.last().Password != "from-env-pw" {
		t.Fatalf("env-mode password = %q", e.fake.last().Password)
	}
}

func TestConnectionAndPolicyRBAC(t *testing.T) {
	e := newCPEnv(t)
	admin := e.admin()
	id := connID(t, admin.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	pid := decode(t, admin.must("POST", "/api/v1/backup-policies", map[string]any{"name": "mine", "schedule": map[string]any{"kind": "daily", "at": "03:00"}}, 201))["policy"].(map[string]any)["id"].(string)
	oper, viewer := e.roleClient(store.RoleOperator), e.roleClient(store.RoleViewer)
	conn := storedConn("other")
	pol := map[string]any{"name": "p2", "schedule": map[string]any{"kind": "daily", "at": "03:00"}}
	probe := map[string]any{"engine": "mysql", "host": "h", "secret_mode": "stored"}
	for _, c := range []struct {
		who  string
		cl   *apiClient
		m, p string
		body any
		want int
	}{
		{"operator", oper, "POST", "/api/v1/backup-connections", conn, 403},
		{"operator", oper, "PUT", "/api/v1/backup-connections/" + id, conn, 403},
		{"operator", oper, "DELETE", "/api/v1/backup-connections/" + id, nil, 403},
		{"operator", oper, "POST", "/api/v1/backup-policies", pol, 403},
		{"operator", oper, "PUT", "/api/v1/backup-policies/" + pid, pol, 403},
		{"operator", oper, "DELETE", "/api/v1/backup-policies/" + pid, nil, 403},
		{"operator", oper, "POST", "/api/v1/backup-policies/" + pid + "/preview", pol, 403},
		{"operator", oper, "POST", "/api/v1/backup-connections/test", probe, 200},
		{"operator", oper, "POST", "/api/v1/backup-connections/" + id + "/test", nil, 200},
		{"operator", oper, "GET", "/api/v1/backup-connections", nil, 200},
		{"viewer", viewer, "POST", "/api/v1/backup-connections/test", probe, 403},
		{"viewer", viewer, "POST", "/api/v1/backup-connections/" + id + "/test", nil, 403},
		{"viewer", viewer, "POST", "/api/v1/backup-connections", conn, 403},
		{"viewer", viewer, "DELETE", "/api/v1/backup-policies/" + pid, nil, 403},
		{"viewer", viewer, "GET", "/api/v1/backup-connections", nil, 200},
		{"viewer", viewer, "GET", "/api/v1/backup-policies", nil, 200},
	} {
		if code, out := c.cl.do(c.m, c.p, c.body); code != c.want {
			t.Errorf("%s %s %s = %d, want %d: %s", c.who, c.m, c.p, code, c.want, out)
		}
	}
}

func TestBuiltinPoliciesSeededOnce(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	list := decode(t, a.must("GET", "/api/v1/backup-policies", nil, 200))["policies"].([]any)
	names := map[string]bool{}
	for _, p := range list {
		pm := p.(map[string]any)
		names[pm["name"].(string)] = pm["builtin"] == true
	}
	if len(list) != 3 || !names["Gold"] || !names["Silver"] || !names["Bronze"] {
		t.Fatalf("seeded = %v", names)
	}
	gold := decode(t, a.must("GET", "/api/v1/backup-policies/policy-gold", nil, 200))["policy"].(map[string]any)
	ret := gold["retention"].(map[string]any)
	if gold["log_archive"] != true || ret["keep_daily"].(float64) != 14 || ret["keep_weekly"].(float64) != 8 || ret["keep_monthly"].(float64) != 12 || gold["rpo_minutes"].(float64) != 5 {
		t.Fatalf("gold = %v", gold)
	}
	gold["description"] = "edited gold"
	a.must("PUT", "/api/v1/backup-policies/policy-gold", gold, 200)
	a.must("DELETE", "/api/v1/backup-policies/policy-bronze", nil, 204)
	e2 := e.restart()
	a2 := (&cpEnv{testEnv: e2, st: e.st}).admin()
	list = decode(t, a2.must("GET", "/api/v1/backup-policies", nil, 200))["policies"].([]any)
	if len(list) != 2 {
		t.Fatalf("after restart %d policies, want 2 (seeding is once-only)", len(list))
	}
	g2 := decode(t, a2.must("GET", "/api/v1/backup-policies/policy-gold", nil, 200))["policy"].(map[string]any)
	if g2["description"] != "edited gold" {
		t.Fatal("edit lost on restart")
	}
}

func policyJobBody(name, conn, policy string, extra map[string]any) map[string]any {
	b := map[string]any{"name": name, "method": "mysqldump", "bucket": "central-backups", "enabled": false,
		"connection_id": conn, "policy_id": policy, "conn": map[string]any{"databases": []string{"app"}}}
	for k, v := range extra {
		b[k] = v
	}
	return b
}

func TestJobFollowsPolicyAndOverrides(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	cid := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	out := a.must("POST", "/api/v1/backup-jobs", policyJobBody("gold-job", cid, "policy-gold", nil), 201)
	res := decode(t, out)
	job := res["job"].(map[string]any)
	sched := job["schedule"].(map[string]any)
	if job["policy_id"] != "policy-gold" || job["log_archive"] != true || sched["kind"] != "daily" || sched["at"] != "01:00" ||
		job["every_seconds"].(float64) != 86400 || job["window_minutes"].(float64) != 120 || job["overrides"] != nil {
		t.Fatalf("gold job = %v", job)
	}
	if job["retention"].(map[string]any)["keep_monthly"].(float64) != 12 {
		t.Fatalf("retention = %v", job["retention"])
	}
	warnings, _ := res["warnings"].([]any)
	if len(warnings) != 1 || !strings.Contains(warnings[0].(string), "test-restore") {
		t.Fatalf("warnings = %v", warnings)
	}
	two := policyJobBody("gold-two-db", cid, "policy-gold", map[string]any{"conn": map[string]any{"databases": []string{"a", "b"}}})
	res = decode(t, a.must("POST", "/api/v1/backup-jobs", two, 201))
	if res["job"].(map[string]any)["log_archive"] == true || !strings.Contains(strings.Join(anyStrings(res["warnings"]), " "), "exactly one database") {
		t.Fatalf("multi-db gold = %v", res)
	}
	ov := policyJobBody("silver-ov", cid, "policy-silver", map[string]any{"retention": map[string]any{"keep_daily": 30}})
	res = decode(t, a.must("POST", "/api/v1/backup-jobs", ov, 201))
	ovJob := res["job"].(map[string]any)
	if ovs := anyStrings(ovJob["overrides"]); len(ovs) != 1 || ovs[0] != "retention" || ovJob["retention"].(map[string]any)["keep_daily"].(float64) != 30 {
		t.Fatalf("override job = %v", ovJob)
	}
	follow := decode(t, a.must("POST", "/api/v1/backup-jobs", policyJobBody("silver-follow", cid, "policy-silver", nil), 201))["job"].(map[string]any)

	silver := decode(t, a.must("GET", "/api/v1/backup-policies/policy-silver", nil, 200))["policy"].(map[string]any)
	silver["retention"] = map[string]any{"keep_daily": 10, "keep_weekly": 6}
	silver["schedule"] = map[string]any{"kind": "daily", "at": "04:30", "timezone": "Asia/Ho_Chi_Minh"}
	pv := decode(t, a.must("POST", "/api/v1/backup-policies/policy-silver/preview", silver, 200))
	if pv["affects_jobs"].(float64) != 2 || len(pv["next_runs"].([]any)) != 5 {
		t.Fatalf("preview = %v", pv)
	}
	before, _, _ := e.st.GetBackupJob(context.Background(), follow["id"].(string))
	if before.Retention.KeepDaily != 7 {
		t.Fatal("preview wrote to the job")
	}
	for _, im := range pv["jobs"].([]any) {
		m := im.(map[string]any)
		ch := strings.Join(anyStrings(m["changes"]), ",")
		if m["job"] == ovJob["id"] && strings.Contains(ch, "retention") {
			t.Fatalf("overridden field listed as changing: %v", m)
		}
		if m["job"] == follow["id"] && (!strings.Contains(ch, "retention") || !strings.Contains(ch, "schedule")) {
			t.Fatalf("follower impact = %v", m)
		}
	}
	put := decode(t, a.must("PUT", "/api/v1/backup-policies/policy-silver", silver, 200))
	if put["affects_jobs"].(float64) != 2 {
		t.Fatalf("PUT = %v", put)
	}
	f2, _, _ := e.st.GetBackupJob(context.Background(), follow["id"].(string))
	o2, _, _ := e.st.GetBackupJob(context.Background(), ovJob["id"].(string))
	if f2.Retention.KeepDaily != 10 || f2.Retention.KeepWeekly != 6 || f2.Schedule.At != "04:30" || f2.Schedule.Timezone != "Asia/Ho_Chi_Minh" ||
		!f2.UpdatedAt.After(before.UpdatedAt) {
		t.Fatalf("follower after policy edit = %+v", f2)
	}
	if o2.Retention.KeepDaily != 30 || o2.Schedule.At != "04:30" {
		t.Fatalf("override job after policy edit: retention=%+v schedule=%+v", o2.Retention, o2.Schedule)
	}
	code, body := a.do("DELETE", "/api/v1/backup-policies/policy-silver", nil)
	if code != 409 || !strings.Contains(body, follow["id"].(string)) || !strings.Contains(body, ovJob["id"].(string)) {
		t.Fatalf("delete policy in use = %d %s", code, body)
	}
	audit := e.auditText()
	if !strings.Contains(audit, "backup_policy.update") || !strings.Contains(audit, "affects_jobs") || !strings.Contains(audit, "keep_daily") {
		t.Fatalf("policy update audit missing details: %s", audit)
	}
}

func TestPolicyUpdateRestartsFollowingSchedulers(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	cid := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	a.must("POST", "/api/v1/backup-jobs", policyJobBody("running", cid, "policy-silver", map[string]any{"enabled": true}), 201)
	e.rec.take()
	silver := decode(t, a.must("GET", "/api/v1/backup-policies/policy-silver", nil, 200))["policy"].(map[string]any)
	silver["window_minutes"] = 30
	a.must("PUT", "/api/v1/backup-policies/policy-silver", silver, 200)
	specs := e.rec.take()
	if len(specs) != 1 || specs[0].Kind != "backup-schedule" || !containsStr(specs[0].Env, backup.EnvConnPassword+"="+canary) {
		t.Fatalf("restart after policy edit = %+v", specs)
	}
	silver["description"] = "only a description"
	put := decode(t, a.must("PUT", "/api/v1/backup-policies/policy-silver", silver, 200))
	if len(e.rec.take()) != 0 || put["affects_jobs"].(float64) != 1 {
		t.Fatalf("no-op edit restarted schedulers or miscounted: %v", put)
	}
}

func TestConnectionEditRefreshesJobsAndRestarts(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	cid := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	b := mysqlJobBody("uses", cid)
	b["enabled"] = true
	jobID := decode(t, a.must("POST", "/api/v1/backup-jobs", b, 201))["job"].(map[string]any)["id"].(string)
	e.rec.take()
	upd := storedConn("prod")
	upd["host"] = "db2.internal"
	upd["password"] = canary + "-rotated"
	out := a.must("PUT", "/api/v1/backup-connections/"+cid, upd, 200)
	if strings.Contains(out, canary) {
		t.Fatal("update response leaks the secret")
	}
	j, _, _ := e.st.GetBackupJob(context.Background(), jobID)
	if j.Conn.Host != "db2.internal" {
		t.Fatalf("job not refreshed: %+v", j.Conn)
	}
	specs := e.rec.take()
	if len(specs) != 1 || !containsStr(specs[0].Env, backup.EnvConnPassword+"="+canary+"-rotated") {
		t.Fatalf("restart did not carry the rotated secret: %+v", specs)
	}
	bad := storedConn("prod")
	bad["engine"] = "postgres"
	a.must("PUT", "/api/v1/backup-connections/"+cid, bad, 409)
}

func TestPolicyValidation(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	good := map[string]any{"name": "custom", "schedule": map[string]any{"kind": "cron", "cron": "0 3 * * 1-5", "timezone": "Europe/Paris"}, "window_minutes": 60}
	a.must("POST", "/api/v1/backup-policies", good, 201)
	a.must("POST", "/api/v1/backup-policies", good, 409)
	for name, p := range map[string]map[string]any{
		"bad cron":     {"name": "a", "schedule": map[string]any{"kind": "cron", "cron": "99 * * * *"}},
		"bad tz":       {"name": "b", "schedule": map[string]any{"kind": "daily", "at": "01:00", "timezone": "Mars/Base"}},
		"bad window":   {"name": "c", "schedule": map[string]any{"kind": "daily", "at": "01:00"}, "window_minutes": -5},
		"same replica": {"name": "d", "schedule": map[string]any{"kind": "daily", "at": "01:00"}, "repository": "r1", "replicate_to": "r1"},
		"unknown":      {"name": "e", "schedule": map[string]any{"kind": "daily", "at": "01:00"}, "bogus": true},
	} {
		if code, out := a.do("POST", "/api/v1/backup-policies", p); code != 400 {
			t.Errorf("%s accepted: %d %s", name, code, out)
		}
	}
	a.must("DELETE", "/api/v1/backup-policies/nope", nil, 404)
	a.must("POST", "/api/v1/backup-policies/nope/preview", good, 404)
}

func TestSchedulePreview(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	out := decode(t, a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "daily", "at": "01:00",
		"timezone": "Asia/Ho_Chi_Minh", "after": "2026-03-10T12:00:00Z"}, 200))
	runs := out["runs"].([]any)
	if len(runs) != 5 || out["timezone"] != "Asia/Ho_Chi_Minh" {
		t.Fatalf("preview = %v", out)
	}
	for i, r := range runs {
		m := r.(map[string]any)
		want := time.Date(2026, 3, 10+i, 18, 0, 0, 0, time.UTC)
		if m["utc"] != want.Format(time.RFC3339) {
			t.Fatalf("run %d utc = %v, want %s", i, m["utc"], want.Format(time.RFC3339))
		}
		if !strings.HasSuffix(m["local"].(string), "+07:00") || !strings.Contains(m["local"].(string), "T01:00:00") {
			t.Fatalf("run %d local = %v", i, m["local"])
		}
	}
	w := decode(t, a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "weekly", "at": "02:00", "days": []int{0, 3},
		"after": "2026-03-10T00:00:00Z", "count": 3}, 200))["runs"].([]any)
	got := []string{w[0].(map[string]any)["utc"].(string), w[1].(map[string]any)["utc"].(string), w[2].(map[string]any)["utc"].(string)}
	if got[0] != "2026-03-11T02:00:00Z" || got[1] != "2026-03-15T02:00:00Z" || got[2] != "2026-03-18T02:00:00Z" {
		t.Fatalf("weekly = %v", got)
	}
	cr := decode(t, a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "cron", "cron": "*/20 * * * *", "after": "2026-03-10T00:05:00Z"}, 200))["runs"].([]any)
	if cr[0].(map[string]any)["utc"] != "2026-03-10T00:20:00Z" {
		t.Fatalf("cron = %v", cr[0])
	}
	iv := decode(t, a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "interval", "every_seconds": 3600, "after": "2026-03-10T00:00:00Z"}, 200))["runs"].([]any)
	if iv[4].(map[string]any)["utc"] != "2026-03-10T05:00:00Z" {
		t.Fatalf("interval = %v", iv[4])
	}
	a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "cron", "cron": "nope"}, 400)
	a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "daily", "at": "01:00", "count": 21}, 400)
	a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "daily", "at": "01:00", "timezone": "Nowhere/X"}, 400)
	a.must("POST", "/api/v1/backup-schedules/preview", map[string]any{"kind": "interval"}, 400)
}

func TestJobScheduleFieldsStoredAndNextRun(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	cid := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	b := mysqlJobBody("cron-job", cid)
	delete(b, "every_seconds")
	b["enabled"] = true
	b["schedule"] = map[string]any{"kind": "cron", "cron": "30 2 * * *", "timezone": "UTC"}
	b["window_minutes"], b["jitter_seconds"] = 45, 20
	job := decode(t, a.must("POST", "/api/v1/backup-jobs", b, 201))["job"].(map[string]any)
	got, _, _ := e.st.GetBackupJob(context.Background(), job["id"].(string))
	if got.Schedule == nil || got.Schedule.Cron != "30 2 * * *" || got.WindowMinutes != 45 || got.JitterSeconds != 20 || got.EverySeconds != 86400 {
		t.Fatalf("stored = %+v schedule=%+v", got, got.Schedule)
	}
	want, _ := got.Schedule.Next(e.clock.Now(), time.Time{})
	if got.NextRunAt == nil || !got.NextRunAt.Equal(want) {
		t.Fatalf("next run = %v, want %s", got.NextRunAt, want)
	}
	leg := mysqlJobBody("interval-job", cid)
	leg["enabled"] = true
	lj := decode(t, a.must("POST", "/api/v1/backup-jobs", leg, 201))["job"].(map[string]any)
	g2, _, _ := e.st.GetBackupJob(context.Background(), lj["id"].(string))
	if g2.NextRunAt == nil || g2.NextRunAt.After(e.clock.Now().Add(time.Second)) {
		t.Fatalf("interval job next run = %v", g2.NextRunAt)
	}
	a.must("POST", "/api/v1/backup-jobs", map[string]any{"name": "badsched", "method": "mysqldump", "bucket": "central-backups", "every_seconds": 3600,
		"conn": map[string]any{"host": "h", "databases": []string{"a"}}, "source": "s", "schedule": map[string]any{"kind": "cron", "cron": "x"}}, 400)
	a.must("POST", "/api/v1/backup-jobs", map[string]any{"name": "badwin", "method": "mysqldump", "bucket": "central-backups", "every_seconds": 3600,
		"conn": map[string]any{"host": "h", "databases": []string{"a"}}, "source": "s", "window_minutes": -1}, 400)
}

func TestConnectionSecretsSurviveKeyRotation(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	id := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	repoResult := decode(t, a.must("POST", "/api/v1/backup-repositories", storedRepoBody("Primary", "http://127.0.0.1:1"), 200))
	repoID := repoResult["repository"].(map[string]any)["id"].(string)
	notifyURL := "https://hooks.example.com/services/" + canary
	channelResult := decode(t, a.must("POST", "/api/v1/backup-notify-channels", map[string]any{
		"name": "alerts", "kind": "slack", "url": notifyURL,
	}, 201))
	channelID := channelResult["channel"].(map[string]any)["id"].(string)
	oldKey := e.srv.db.secret
	e.srv.releaseLease()
	c2 := *e.srv.cfg
	srv, err := New(Options{Config: &c2, Version: "test", Commander: e.cmd, Now: e.clock.Now, Store: e.st,
		SecretKey: "rotated-secret-key-0123456789abcdef", PreviousSecretKey: oldKey})
	if err != nil {
		t.Fatal(err)
	}
	row, _, _ := e.st.GetBackupConnection(context.Background(), id)
	pt, err := srv.db.box.Open(row.PasswordSealed, id)
	if err != nil || string(pt) != canary {
		t.Fatalf("stored secret not re-encrypted with the new key: %v", err)
	}
	repo, _, err := e.st.GetBackupRepo(t.Context(), repoID)
	if err != nil {
		t.Fatal(err)
	}
	channel, _, err := e.st.GetNotifyChannel(t.Context(), channelID)
	if err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		id, want string
		sealed   []byte
	}{
		"access key": {repoID, "AK-" + canary, repo.AccessKeySealed},
		"secret key": {repoID, "SK-" + canary, repo.SecretKeySealed},
		"webhook":    {channelID, notifyURL, channel.SecretSealed},
	} {
		pt, err := srv.db.box.Open(test.sealed, test.id)
		if err != nil || string(pt) != test.want {
			t.Errorf("%s was not re-encrypted with the new key: %v", name, err)
		}
	}

}
