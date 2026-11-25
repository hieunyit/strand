package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// failManifestVersion fails the catalog update that records the manifest sidecar's version, i.e. a
// failure after the sidecar was uploaded as complete.
type failManifestVersion struct{ *store.Mem }

func (f failManifestVersion) UpdateBackupMeta(ctx context.Context, id string, m backup.Meta) (backup.Record, bool, error) {
	if m.Details["manifest_version"] != "" {
		return backup.Record{}, false, errors.New("catalog unavailable")
	}
	return f.Mem.UpdateBackupMeta(ctx, id, m)
}

// A record that fails after its manifest sidecar was uploaded must not leave a "complete" manifest in the
// repository: the sidecar is deleted, or (when Object Lock refuses that) superseded by one marked failed.
func TestFailedFinalizeWithdrawsCompleteManifest(t *testing.T) {
	for _, locked := range []bool{false, true} {
		s3 := &versionedS3{locks: map[string]string{}, bodies: map[string][]byte{}, sums: map[string]string{}}
		if locked {
			s3.deleteStatus = http.StatusForbidden
		}
		srv := httptest.NewServer(s3)
		tools := t.TempDir()
		script := "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'mysqldump  Ver 8.0.36 for Linux'; exit 0; fi\necho 'CREATE TABLE t (id int);'\n"
		if err := os.WriteFile(filepath.Join(tools, "mysqldump"), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		e := testEnv(t, map[string]string{EnvToolsDir: tools, "STRAND_BACKUP_TMPDIR": t.TempDir(),
			"STRAND_BACKUP_S3_ENDPOINT": srv.URL, "STRAND_BACKUP_S3_ACCESS_KEY": "test", "STRAND_BACKUP_S3_SECRET_KEY": "test",
			"STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true"}, failManifestVersion{store.NewMem()})
		e.NoReadBack = true
		job := backup.Job{ID: "bjob-m", Name: "m", Bucket: "bkt", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump,
			Source: "src", Conn: backup.ConnSpec{Host: "127.0.0.1", Port: 1}}}
		_, err := e.RunJob(context.Background(), job)
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), "catalog unavailable") {
			t.Fatalf("locked=%v: RunJob err = %v, want the catalog failure", locked, err)
		}
		var manifestKey string
		for k := range s3.bodies {
			if strings.HasSuffix(k, "/manifest.json") {
				manifestKey = k
			}
		}
		if manifestKey == "" {
			t.Fatalf("locked=%v: no manifest was uploaded", locked)
		}
		deleted := false
		for _, d := range s3.deletes {
			deleted = deleted || strings.HasPrefix(d, manifestKey+"?")
		}
		var m backup.Record
		if err := json.Unmarshal(s3.bodies[manifestKey], &m); err != nil {
			t.Fatal(err)
		}
		switch {
		case !locked && !deleted:
			t.Fatalf("manifest of the failed record was not deleted (status %s)", m.Status)
		case locked && m.Status != backup.StatusFailed:
			t.Fatalf("locked manifest of the failed record still says %s", m.Status)
		}
	}
}
