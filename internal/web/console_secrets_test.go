package web

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// restoreConn returns the id of a saved (stored-secret, password-less) connection of the engine, creating it once:
// restores into a database always go to a saved connection.
func restoreConn(t *testing.T, st store.Store, engine string) string {
	t.Helper()
	id := "rc-" + engine
	_, err := st.CreateBackupConnection(context.Background(), backup.Connection{ID: id, Name: id, Engine: engine,
		Host: engine + "-restore.internal", SecretMode: backup.SecretStored})
	if err != nil && !errors.Is(err, backup.ErrConflict) {
		if _, ok, _ := st.GetBackupConnection(context.Background(), id); !ok {
			t.Fatal(err)
		}
	}
	return id
}

// Secrets are entered in the console and stored sealed: no repository, connection or restore target can be
// created that reads a password or key from an environment variable of the Web service. Existing (legacy)
// env-mode records keep working and can be converted.
func TestConsoleSecretsAreStoredNotEnvironment(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	ctx := context.Background()

	// Repositories: env mode refused; no secret_mode = stored; the keys are required.
	a.must("POST", "/api/v1/backup-repositories", map[string]any{"name": "envrepo", "secret_mode": "env",
		"env_prefix": "STRAND_BACKUP_REPO_OFFSITE_", "bucket": "bkt-one"}, 400)
	a.must("POST", "/api/v1/backup-repositories", map[string]any{"name": "nokeys", "endpoint": "https://s3.example.com", "bucket": "bkt-one"}, 400)
	body := decode(t, a.must("POST", "/api/v1/backup-repositories", map[string]any{"name": "offsite", "endpoint": "https://s3.example.com",
		"region": "eu-west-1", "access_key": "AKIA-canary", "secret_key": "sk-canary", "bucket": "bkt-one", "prefix": "strand/prod",
		"retention": map[string]any{"max_age_days": 30, "keep_last": 3}}, 200))
	rp := body["repository"].(map[string]any)
	if rp["secret_mode"] != "stored" || rp["access_key_set"] != true || rp["prefix"] != "strand/prod" {
		t.Fatalf("repository = %v", rp)
	}
	if ret := rp["retention"].(map[string]any); ret["max_age_days"] != float64(30) || ret["keep_last"] != float64(3) {
		t.Fatalf("retention not stored: %v", ret)
	}
	if strings.Contains(a.must("GET", "/api/v1/backup-repositories/"+rp["id"].(string), nil, 200), "canary") {
		t.Fatal("repository keys came back from the API")
	}
	// A legacy env repository stays editable as such, and converts to stored when the keys are entered.
	if err := e.st.CreateBackupRepo(ctx, backup.Repo{ID: "legacy-repo", Name: "legacy", SecretMode: backup.RepoSecretEnv,
		EnvPrefix: "STRAND_BACKUP_REPO_LEGACY_", Bucket: "bkt-two"}); err != nil {
		t.Fatal(err)
	}
	a.must("PUT", "/api/v1/backup-repositories/legacy-repo", map[string]any{"name": "legacy", "secret_mode": "env",
		"env_prefix": "STRAND_BACKUP_REPO_LEGACY_", "bucket": "bkt-two"}, 200)
	a.must("PUT", "/api/v1/backup-repositories/legacy-repo", map[string]any{"name": "legacy", "endpoint": "https://s3.example.com",
		"access_key": "ak2", "secret_key": "sk2", "bucket": "bkt-two"}, 200)
	if got, _, _ := e.st.GetBackupRepo(ctx, "legacy-repo"); !got.Stored() || got.EnvPrefix != "" || !got.SecretKeySet {
		t.Fatalf("legacy repository not converted: %+v", got)
	}

	// Connections: env mode refused; default is stored.
	a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "c-env", "engine": "mysql", "host": "h",
		"secret_mode": "env", "password_env": "SOME_PW"}, 400)
	c := decode(t, a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "c-default", "engine": "mysql", "host": "h",
		"password": "pw-canary"}, 201))
	if conn := c["connection"].(map[string]any); conn["secret_mode"] != "stored" || conn["password_set"] != true {
		t.Fatalf("connection = %v", conn)
	}

	// Restores into a database need a saved connection.
	seedRichBackup(t, e.st, "bkp-sec-1", "mysql-app", "mysql", "mysqldump", backup.KindFull, e.srv.now().UTC().Add(-time.Hour), backup.Position{}, false, nil, "", nil, nil, "", nil)
	code, out := a.do("POST", "/api/v1/backup-restores", map[string]any{"backup_id": "bkp-sec-1",
		"target": map[string]any{"host": "db2", "password_env": "RESTORE_PW"}, "target_database": "r1", "confirm": "r1"})
	if code != http.StatusBadRequest || !strings.Contains(out, "connection") {
		t.Fatalf("restore without a saved connection = %d %s", code, out)
	}
	a.must("POST", "/api/v1/backup-restores", map[string]any{"backup_id": "bkp-sec-1", "connection_id": restoreConn(t, e.st, "mysql"),
		"target_database": "r1", "confirm": "r1"}, 202)
}

// The scratch server of test restores is a saved connection: its stored password reaches only the verify
// process, in STRAND_BACKUP_SCRATCH_PASSWORD, never argv.
func TestScratchConnectionStoredPasswordReachesVerifyOnly(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	src := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	scr := connID(t, a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "scratch", "engine": "mysql",
		"host": "scratch.internal", "user": "restorer", "password": "scratch-canary"}, 201))
	b := mysqlJobBody("with-test-restore", src)
	b["test_restore"] = true
	b["scratch_connection_id"] = scr
	b["verify_every_seconds"] = 3600
	job := decode(t, a.must("POST", "/api/v1/backup-jobs", b, 201))["job"].(map[string]any)
	sc := job["scratch"].(map[string]any)
	if job["scratch_connection_id"] != scr || sc["host"] != "scratch.internal" || sc["user"] != "restorer" || sc["password_env"] != backup.EnvScratchPassword {
		t.Fatalf("scratch not resolved from the connection: %v", job)
	}
	jid := job["id"].(string)
	e.rec.take()
	a.must("POST", "/api/v1/backup-jobs/"+jid+"/verify", nil, 202)
	for _, sp := range e.rec.take() {
		for _, arg := range sp.Args {
			if strings.Contains(arg, "scratch-canary") {
				t.Fatalf("scratch password in argv: %v", sp.Args)
			}
		}
	}
	// A deleted-in-use or mismatched scratch connection is refused.
	pg := decode(t, a.must("POST", "/api/v1/backup-connections", map[string]any{"name": "pg", "engine": "postgres", "host": "pg"}, 201))["connection"].(map[string]any)["id"].(string)
	b2 := mysqlJobBody("bad-scratch", src)
	b2["test_restore"] = true
	b2["scratch_connection_id"] = pg
	a.must("POST", "/api/v1/backup-jobs", b2, 400)
	a.must("DELETE", "/api/v1/backup-connections/"+scr, nil, 409)
	if env := e.srv.scratchSecretEnv(mustJob(t, e, jid)); len(env) != 1 || env[0] != backup.EnvScratchPassword+"=scratch-canary" {
		t.Fatal("scratch secret env does not carry the stored password")
	}
}

func mustJob(t *testing.T, e *cpEnv, id string) backup.Job {
	t.Helper()
	j, ok, err := e.st.GetBackupJob(context.Background(), id)
	if err != nil || !ok {
		t.Fatalf("job %s: %v", id, err)
	}
	return j
}
