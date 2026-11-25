package runner

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// versionedS3 is a tiny versioned, Object-Lock-aware bucket: every PUT creates version v<n>.
type versionedS3 struct {
	mu      sync.Mutex
	n       int
	locks   map[string]string // key -> lock mode of the last PUT
	bodies  map[string][]byte // key -> body of the last PUT
	sums    map[string]string // key -> SHA-256 metadata of the last PUT
	// deleteStatus, when set, is the answer to DELETE (e.g. 403 for a version under Object Lock).
	deleteStatus int
	deletes []string          // "key?versionId=..." of each DELETE
}

func (s *versionedS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Has("uploads"):
		_, _ = io.WriteString(w, "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>")
	case r.Method == http.MethodPut:
		s.n++
		s.locks[key] = r.Header.Get("X-Amz-Object-Lock-Mode")
		if s.bodies != nil {
			s.bodies[key] = body
		}
		if s.sums != nil {
			s.sums[key] = r.Header.Get("X-Amz-Meta-Strand-Sha256")
		}
		w.Header().Set("ETag", `"e"`)
		w.Header().Set("X-Amz-Version-Id", "v"+string(rune('0'+s.n)))
	case r.Method == http.MethodDelete:
		s.deletes = append(s.deletes, key+"?versionId="+q.Get("versionId"))
		if s.deleteStatus != 0 {
			w.WriteHeader(s.deleteStatus)
			_, _ = io.WriteString(w, "<Error><Code>AccessDenied</Code><Message>object is WORM protected</Message></Error>")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodHead && s.bodies != nil && s.bodies[key] != nil:
		w.Header().Set("Content-Length", strconv.Itoa(len(s.bodies[key])))
		w.Header().Set("X-Amz-Meta-Strand-Sha256", s.sums[key])
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// failFullMeta fails the metadata update that RunJob makes after the tool finished (the one carrying
// start/end times): a catalog hiccup after the upload succeeded.
type failFullMeta struct{ *store.Mem }

func (f failFullMeta) UpdateBackupMeta(ctx context.Context, id string, m backup.Meta) (backup.Record, bool, error) {
	if m.StartTime != nil {
		return backup.Record{}, false, errors.New("catalog unavailable")
	}
	return f.Mem.UpdateBackupMeta(ctx, id, m)
}

// A run that fails after its payload was uploaded to an Object Lock repository must leave a failed record
// that carries the object version and lock retention: deleting it is refused while the lock holds, and
// afterwards removes that exact version (not only a delete marker over the locked version).
func TestFailedRunAfterUploadKeepsVersionAndLock(t *testing.T) {
	s3 := &versionedS3{locks: map[string]string{}}
	srv := httptest.NewServer(s3)
	defer srv.Close()
	tools := t.TempDir()
	script := "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'mysqldump  Ver 8.0.36 for Linux'; exit 0; fi\necho 'CREATE TABLE t (id int);'\n"
	if err := os.WriteFile(filepath.Join(tools, "mysqldump"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	e := testEnv(t, map[string]string{EnvToolsDir: tools, "STRAND_BACKUP_TMPDIR": t.TempDir(),
		"TESTREPO_ENDPOINT": srv.URL, "TESTREPO_ACCESS_KEY": "test", "TESTREPO_SECRET_KEY": "test",
		"TESTREPO_ALLOW_INSECURE_LOCAL": "true"}, failFullMeta{store.NewMem()})
	e.NoReadBack = true
	e.Repo = func(context.Context, string) (backup.Repo, bool, error) {
		return backup.Repo{ID: "repo1", Bucket: "locked", EnvPrefix: "TESTREPO_", LockMode: "GOVERNANCE", LockDays: 1}, true, nil
	}
	job := backup.Job{ID: "bjob-v", Name: "v", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "src",
		Repository: "repo1", Conn: backup.ConnSpec{Host: "127.0.0.1", Port: 1}}}
	ctx := context.Background()
	if _, err := e.RunJob(ctx, job); err == nil || !strings.Contains(err.Error(), "catalog unavailable") {
		t.Fatalf("RunJob err = %v, want the catalog failure", err)
	}
	rs, err := e.Catalog.List(ctx, backup.Filter{})
	if err != nil || len(rs) != 1 {
		t.Fatalf("records = %+v, %v", rs, err)
	}
	rec := rs[0]
	if rec.Status != backup.StatusFailed {
		t.Fatalf("status = %s, want failed", rec.Status)
	}
	if s3.locks[rec.ObjectKey] != "GOVERNANCE" {
		t.Fatalf("payload was not uploaded under Object Lock: %v", s3.locks)
	}
	if rec.VersionID != "v1" || rec.LockMode != "GOVERNANCE" || rec.RetainUntil == nil {
		t.Fatalf("failed record lost the uploaded version/lock: version=%q lock=%q retain=%v", rec.VersionID, rec.LockMode, rec.RetainUntil)
	}
	if _, err := e.DeleteRecord(ctx, rec); err == nil || !strings.Contains(err.Error(), "object-locked") {
		t.Fatalf("delete of a locked failed record = %v, want refused", err)
	}
	if len(s3.deletes) != 0 {
		t.Fatalf("objects deleted under lock: %v", s3.deletes)
	}
	e.Now = func() time.Time { return rec.RetainUntil.Add(time.Minute) }
	if _, err := e.DeleteRecord(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if want := rec.ObjectKey + "?versionId=v1"; len(s3.deletes) == 0 || s3.deletes[0] != want {
		t.Fatalf("deletes = %v, want the payload version %s first", s3.deletes, want)
	}
}
