package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The native wrapper runs with the Web/scheduler environment; Strand's own secrets must not reach it.
func TestBackupWrapperDoesNotInheritStrandSecrets(t *testing.T) {
	t.Setenv("STRAND_WEB_DATABASE_DSN", "postgres://strand:dsn-secret@db/meta")
	t.Setenv("STRAND_WEB_SECRET_KEY", "web-secret-key-value")
	t.Setenv("STRAND_BACKUP_S3_SECRET_KEY", "s3-secret-value")
	t.Setenv("MYSQL_PWD", "source-db-password") // the wrapper's own credential stays
	script := filepath.Join(t.TempDir(), "w.sh")
	os.WriteFile(script, []byte("#!/bin/sh\nenv\n"), 0o755)
	var out bytes.Buffer
	env := append(wrapperEnv(os.Environ()), "STRAND_BACKUP_ID=x")
	if err := runBackupCommand(context.Background(), []string{script}, env, &out, &out); err != nil {
		t.Fatal(err)
	}
	for _, leaked := range []string{"dsn-secret", "web-secret-key-value", "s3-secret-value"} {
		if strings.Contains(out.String(), leaked) {
			t.Fatalf("wrapper environment contains %q", leaked)
		}
	}
	if !strings.Contains(out.String(), "MYSQL_PWD=source-db-password") || !strings.Contains(out.String(), "STRAND_BACKUP_ID=x") {
		t.Fatalf("wrapper lost its own variables: %s", out.String())
	}
}
