package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"strand/internal/backup"
	"strand/internal/objectstore"
)

// refusingUploader reads part of the stream, then fails the way an object store refusing a part does.
type refusingUploader struct{}

func (refusingUploader) Upload(ctx context.Context, key string, src io.Reader, size int64, sha string) (objectstore.ObjectInfo, error) {
	if _, err := io.CopyN(io.Discard, src, 1<<20); err != nil {
		return objectstore.ObjectInfo{}, err
	}
	return objectstore.ObjectInfo{}, errors.New("objectstore: upload part 1 failed (403 AccessDenied): Access Denied")
}

type endlessReader struct{ x uint64 }

func (r *endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		r.x = r.x*6364136223846793005 + 1442695040888963407
		p[i] = byte(r.x >> 56)
	}
	return len(p), nil
}

// backup push --compress: the object store's error, not "upload ended", is what the run reports.
func TestPushPayloadReportsRepositoryError(t *testing.T) {
	saved := pushEncoding
	t.Cleanup(func() { pushEncoding = saved })
	pushEncoding.compress, pushEncoding.encrypt = true, false
	_, _, err := uploadPushPayload(context.Background(), refusingUploader{}, "k", &endlessReader{}, -1)
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("err = %v, want the object store error", err)
	}
	if code := backup.NormalizeErrorCode(err); code != backup.ErrCodeRepoUnreachable {
		t.Fatalf("code = %s, want %s", code, backup.ErrCodeRepoUnreachable)
	}
}
