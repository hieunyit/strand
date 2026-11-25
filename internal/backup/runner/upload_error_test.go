package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"strand/internal/backup"
)

// noiseReader yields n incompressible bytes, then err (io.EOF when nil).
type noiseReader struct {
	n   int64
	err error
	x   uint64
}

func (z *noiseReader) Read(p []byte) (int, error) {
	if z.n <= 0 {
		if z.err != nil {
			return 0, z.err
		}
		return 0, io.EOF
	}
	if int64(len(p)) > z.n {
		p = p[:z.n]
	}
	for i := range p {
		z.x = z.x*6364136223846793005 + 1442695040888963407
		p[i] = byte(z.x >> 56)
	}
	z.n -= int64(len(p))
	return len(p), nil
}

// refusingS3 accepts multipart initiation and refuses every part upload with 403 AccessDenied.
func refusingS3(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>up1</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut && q.Has("partNumber"):
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>")
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func uploadTestRepo(t *testing.T, endpoint string) *Repository {
	t.Helper()
	vars := map[string]string{"STRAND_BACKUP_S3_ENDPOINT": endpoint, "STRAND_BACKUP_S3_ACCESS_KEY": "test",
		"STRAND_BACKUP_S3_SECRET_KEY": "test", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true", "STRAND_BACKUP_TMPDIR": t.TempDir()}
	repo, err := OpenRepository(envOf(vars), nil, "bkt", "", 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

// An object store that refuses a part of a large (multipart) stream must surface its own error and be
// classified as a repository failure, not as "upload ended" / tool_failed.
func TestStorePayloadReportsRepositoryErrorOfLargeStream(t *testing.T) {
	repo := uploadTestRepo(t, refusingS3(t).URL)
	for _, enc := range []backup.EncodeOptions{{}, {Compress: true}} {
		src := &noiseReader{n: 256 << 20}
		_, err := repo.StorePayload(context.Background(), "src/bkp-1/dump.sql", src, enc, 0, 0)
		if err == nil {
			t.Fatal("upload succeeded against a refusing repository")
		}
		if !strings.Contains(err.Error(), "AccessDenied") {
			t.Fatalf("compress=%v: error hides the S3 error: %v", enc.Compress, err)
		}
		if code := backup.NormalizeErrorCode(err); code != backup.ErrCodeRepoUnreachable {
			t.Fatalf("compress=%v: code = %s, want %s (err %v)", enc.Compress, code, backup.ErrCodeRepoUnreachable, err)
		}
	}
}

// A failing native tool stays a tool failure even though its error also aborts the upload.
func TestStorePayloadKeepsToolFailure(t *testing.T) {
	var parts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>up1</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut && q.Has("partNumber"):
			parts++
			w.Header().Set("ETag", fmt.Sprintf(`"p%d"`, parts))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	repo := uploadTestRepo(t, srv.URL)
	toolErr := errors.New("pg_dump failed: exit status 1: pg_dump: error: query failed")
	_, err := repo.StorePayload(context.Background(), "src/bkp-2/dump.pgdump", &noiseReader{n: 12 << 20, err: toolErr}, backup.EncodeOptions{}, 0, 0)
	if !errors.Is(err, toolErr) {
		t.Fatalf("err = %v, want the tool error", err)
	}
	var re *backup.RepositoryError
	if errors.As(err, &re) {
		t.Fatalf("tool failure reported as a repository error: %v", err)
	}
	if code := backup.NormalizeErrorCode(err); code != backup.ErrCodeToolFailed {
		t.Fatalf("code = %s, want %s", code, backup.ErrCodeToolFailed)
	}
}
