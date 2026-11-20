package objectstore

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"strand/internal/testutil"
)

// TestMinIOIntegration exercises the client against a real S3-compatible server: SigV4 signing of
// escaped keys, parallel multipart upload of an unknown-size stream, verified download, overwrite
// detection, and reclaiming an orphaned multipart upload.
func TestMinIOIntegration(t *testing.T) {
	endpoint := testutil.MinIOEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := New(Config{Endpoint: endpoint, Bucket: "strand-backups", AccessKey: "minioadmin", SecretKey: "minioadmin",
		AllowInsecureLocal: true, PartSize: minPartSize, Concurrency: 3, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.request(ctx, http.MethodPut, "", nil, nil, nil, 0)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("create bucket: %v %v", resp, err)
	}
	closeResponse(resp)

	data := randomBytes(3*int(minPartSize) + 4321)
	key := "prod/mysql/bkp 1/artifact+ü.tar"
	info, err := c.Upload(ctx, key, onlyReader{bytes.NewReader(data)}, -1, "")
	if err != nil {
		t.Fatalf("multipart upload: %v", err)
	}
	if info.Size != int64(len(data)) || info.SHA256 != sumHex(data) || !strings.HasSuffix(info.ETag, "-4") {
		t.Fatalf("info = %+v", info)
	}
	head, err := c.Head(ctx, key)
	if err != nil || head.Size != int64(len(data)) {
		t.Fatalf("head = %+v err %v", head, err)
	}
	rc, _, err := c.DownloadVerified(ctx, key, info.Size, info.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("verified download: err=%v equal=%v", err, bytes.Equal(got, data))
	}

	// The object is replaced behind the catalog's back with same-size content: verification must fail.
	tampered := append([]byte(nil), data...)
	tampered[len(tampered)-1] ^= 0x55
	if _, err := c.Upload(ctx, key, bytes.NewReader(tampered), int64(len(tampered)), ""); err != nil {
		t.Fatal(err)
	}
	rc, _, err = c.DownloadVerified(ctx, key, info.Size, info.SHA256)
	if err == nil {
		_, err = io.Copy(io.Discard, rc)
		rc.Close()
	}
	if err == nil {
		t.Fatal("a replaced object must not pass verification against the catalog digest")
	}

	// An uploader killed mid-transfer leaves an open multipart upload; it is reclaimed by key.
	orphan := "prod/mysql/bkp-2/artifact"
	if _, err := c.initiateMultipart(ctx, orphan, "", PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.initiateMultipart(ctx, "prod/mysql/bkp-3/artifact", "", PutOptions{}); err != nil {
		t.Fatal(err)
	}
	n, err := c.AbortStaleUploads(ctx, orphan)
	if err != nil || n != 1 {
		t.Fatalf("abort stale: n=%d err=%v", n, err)
	}
	if n, err := c.AbortStaleUploads(ctx, orphan); err != nil || n != 0 {
		t.Fatalf("second abort: n=%d err=%v", n, err)
	}
	if n, err := c.AbortStaleUploads(ctx, "prod/mysql/bkp-3/artifact"); err != nil || n != 1 {
		t.Fatalf("other key must still have its upload: n=%d err=%v", n, err)
	}

	// Bad credentials: a clear error that contains neither secret.
	bad, _ := New(Config{Endpoint: endpoint, Bucket: "strand-backups", AccessKey: "minioadmin", SecretKey: "wrong-secret-value", AllowInsecureLocal: true, MaxRetries: -1})
	if _, err := bad.Head(ctx, key); err == nil || strings.Contains(err.Error(), "wrong-secret-value") {
		t.Fatalf("bad credentials err = %v", err)
	}
}
