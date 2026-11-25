package runner

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// The native tool inherits no connection secret of Strand's environment: neither the STRAND_* secret
// variables (source, scratch, restore target, ...) nor the legacy variable named by conn.password_env.
// It gets its own password only explicitly (MYSQL_PWD).
func TestNativeToolInheritsNoConnectionSecrets(t *testing.T) {
	secrets := map[string]string{
		backup.EnvConnPassword:           "conn-pw-7f3a",
		backup.EnvConnURI:                "mongodb://u:conn-uri-pw-91c2@db/",
		backup.EnvScratchPassword:        "scratch-pw-5d1e",
		"STRAND_RESTORE_TARGET_PASSWORD": "target-pw-2b8c",
		"STRAND_RESTORE_TARGET_URI":      "mongodb://u:target-uri-pw-6e4d@db/",
		"LEGACY_SOURCE_PW":               "legacy-pw-3c9f",
	}
	for k, v := range secrets {
		t.Setenv(k, v)
	}
	s3 := &versionedS3{locks: map[string]string{}, bodies: map[string][]byte{}}
	srv := httptest.NewServer(s3)
	defer srv.Close()
	tools := t.TempDir()
	script := "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'mysqldump  Ver 8.0.36 for Linux'; exit 0; fi\nenv\n"
	if err := os.WriteFile(filepath.Join(tools, "mysqldump"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	e := testEnv(t, map[string]string{EnvToolsDir: tools, "STRAND_BACKUP_TMPDIR": t.TempDir(),
		"STRAND_BACKUP_S3_ENDPOINT": srv.URL, "STRAND_BACKUP_S3_ACCESS_KEY": "test", "STRAND_BACKUP_S3_SECRET_KEY": "test",
		"STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true", "LEGACY_SOURCE_PW": secrets["LEGACY_SOURCE_PW"]}, store.NewMem())
	e.NoReadBack = true
	job := backup.Job{ID: "bjob-env", Name: "env", Bucket: "bkt", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "src",
		Conn: backup.ConnSpec{Host: "127.0.0.1", Port: 1, User: "root", PasswordEnv: "LEGACY_SOURCE_PW"}}}
	_, _ = e.RunJob(context.Background(), job)
	if len(s3.bodies) != 1 {
		t.Fatalf("uploaded objects = %d, want the tool output", len(s3.bodies))
	}
	var out string
	for _, b := range s3.bodies {
		out = string(b)
	}
	if !strings.Contains(out, "MYSQL_PWD="+secrets["LEGACY_SOURCE_PW"]) {
		t.Fatal("the tool did not get its own password through MYSQL_PWD")
	}
	for _, line := range strings.Split(out, "\n") {
		name, _, _ := strings.Cut(line, "=")
		if name == "MYSQL_PWD" {
			continue
		}
		for k, v := range secrets {
			if strings.Contains(line, v) {
				t.Errorf("tool inherited the secret of %s (as %s)", k, name)
			}
		}
	}
}
