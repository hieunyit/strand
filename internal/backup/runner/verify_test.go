package runner

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// With log archiving the newest record of a job is a log segment. Sampled verification must still
// fully read the newest base backup (the newest restore point), not just that log segment.
func TestVerifySampleAlwaysFullyReadsNewestBase(t *testing.T) {
	payload := []byte("payload bytes")
	digest := fmt.Sprintf("%x", sha256.Sum256(payload))
	var mu sync.Mutex
	gets := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			gets[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]]++
			mu.Unlock()
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(payload)
		}
	}))
	defer server.Close()
	env := testEnv(t, map[string]string{
		"STRAND_BACKUP_S3_ENDPOINT":             server.URL,
		"STRAND_BACKUP_S3_ACCESS_KEY":           "test",
		"STRAND_BACKUP_S3_SECRET_KEY":           "test",
		"STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true",
	}, store.NewMem())
	now := time.Now().UTC()
	rec := func(id string, kind string, at time.Time) backup.Record {
		return backup.Record{ID: id, Kind: kind, Engine: backup.EngineMySQL, Status: backup.StatusComplete, CreatedAt: at,
			Bucket: "backup-fixture", ObjectKey: id, SHA256: digest, Bytes: int64(len(payload))}
	}
	// Newest first, as the catalog lists them.
	rs := []backup.Record{
		rec("log3", backup.KindLog, now),
		rec("log2", backup.KindLog, now.Add(-time.Minute)),
		rec("base2", backup.KindFull, now.Add(-2*time.Minute)),
		rec("log1", backup.KindLog, now.Add(-3*time.Minute)),
		rec("base1", backup.KindFull, now.Add(-4*time.Minute)),
	}
	for i := 0; i < 5; i++ {
		rep := env.verifyRecords(t.Context(), rs, VerifyOptions{SamplePercent: 1})
		if len(rep.Failed) != 0 {
			t.Fatalf("verify failed: %+v", rep)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if gets["base2"] != 5 {
		t.Fatalf("the newest base backup was fully read %d of 5 times (reads %v)", gets["base2"], gets)
	}
}
