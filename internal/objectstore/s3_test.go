package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	testAccess = "AKIAEXAMPLEACCESS"
	testSecret = "s3cr3t-very-private-key"
	testToken  = "session-token-very-private"
)

func newTestClient(t *testing.T, endpoint string, mut func(*Config)) *S3 {
	t.Helper()
	cfg := Config{Endpoint: endpoint, Bucket: "bkt", AccessKey: testAccess, SecretKey: testSecret, SessionToken: testToken,
		AllowInsecureLocal: true, PartSize: minPartSize, TempDir: t.TempDir(), RetryBaseDelay: time.Millisecond}
	if mut != nil {
		mut(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	r := rand.NewChaCha8([32]byte{1, 2, 3})
	_, _ = r.Read(b)
	return b
}

func sumHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// onlyReader hides Len/Seek so Upload sees an unknown-size stream.
type onlyReader struct{ r io.Reader }

func (o onlyReader) Read(p []byte) (int, error) { return o.r.Read(p) }

func TestUploadRetriesTransientFailures(t *testing.T) {
	f, srv := newFakeS3(t)
	f.failHook = func(op string, n int) int {
		if (op == "part" && n == 2) || (op == "initiate" && n == 1) || (op == http503Op && n == 1) {
			return 503
		}
		return 0
	}
	c := newTestClient(t, srv.URL, nil)
	data := randomBytes(3*int(minPartSize) + 123)
	info, err := c.Upload(context.Background(), "a/artifact", onlyReader{bytes.NewReader(data)}, -1, "")
	if err != nil {
		t.Fatalf("upload with one transient 503 per stage must succeed: %v", err)
	}
	if info.Size != int64(len(data)) || info.SHA256 != sumHex(data) {
		t.Fatalf("info = %+v", info)
	}
	if !bytes.Equal(f.objects["a/artifact"], data) {
		t.Fatal("stored object differs from source")
	}
	if f.openUploads() != 0 {
		t.Fatal("multipart upload left open")
	}
}

const http503Op = "PUT"

func TestUploadGivesUpAfterMaxRetriesAndAborts(t *testing.T) {
	f, srv := newFakeS3(t)
	f.failHook = func(op string, n int) int {
		if op == "part" {
			return 500
		}
		return 0
	}
	c := newTestClient(t, srv.URL, func(c *Config) { c.MaxRetries = 2 })
	_, err := c.Upload(context.Background(), "k", bytes.NewReader(randomBytes(2*int(minPartSize))), 2*minPartSize, "")
	if err == nil {
		t.Fatal("expected failure")
	}
	if f.openUploads() != 0 || len(f.aborted) == 0 {
		t.Fatalf("failed upload must be aborted (open=%d aborted=%v)", f.openUploads(), f.aborted)
	}
}

func TestCompleteRetriesErrorIn200Body(t *testing.T) {
	f, srv := newFakeS3(t)
	f.failHook = func(op string, n int) int {
		if op == "complete" && n == 1 {
			return 200 // S3 may answer 200 OK with an <Error> body; the SDKs retry it.
		}
		return 0
	}
	c := newTestClient(t, srv.URL, nil)
	data := randomBytes(int(singlePutLimit) + 10)
	if _, err := c.Upload(context.Background(), "k", bytes.NewReader(data), int64(len(data)), ""); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if f.count("complete") != 2 || !bytes.Equal(f.objects["k"], data) {
		t.Fatalf("complete calls = %d", f.count("complete"))
	}
}

func TestParallelPartsAreBoundedAndCleanedUp(t *testing.T) {
	f, srv := newFakeS3(t)
	f.partWait = 400 * time.Millisecond
	tmp := t.TempDir()
	c := newTestClient(t, srv.URL, func(c *Config) { c.Concurrency = 3; c.TempDir = tmp })
	data := randomBytes(8 * int(minPartSize))
	start := time.Now()
	info, err := c.Upload(context.Background(), "k", onlyReader{bytes.NewReader(data)}, -1, sumHex(data))
	if err != nil {
		t.Fatal(err)
	}
	if got := f.maxInfl.Load(); got < 2 || got > 3 {
		t.Fatalf("max parts in flight = %d, want 2..3 (concurrency 3)", got)
	}
	if !bytes.Equal(f.objects["k"], data) || info.SHA256 != sumHex(data) {
		t.Fatal("object content/digest mismatch after parallel upload")
	}
	ents, _ := os.ReadDir(tmp)
	if len(ents) != 0 {
		t.Fatalf("temporary part files left behind: %d", len(ents))
	}
	t.Logf("8 parts with 400ms server latency took %v", time.Since(start))
}

func TestCancelledUploadAbortsMultipart(t *testing.T) {
	f, srv := newFakeS3(t)
	f.partWait = 50 * time.Millisecond
	tmp := t.TempDir()
	c := newTestClient(t, srv.URL, func(c *Config) { c.TempDir = tmp })
	ctx, cancel := context.WithCancel(context.Background())
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(randomBytes(3 * int(minPartSize)))
		cancel()
		time.Sleep(50 * time.Millisecond)
		_ = pw.CloseWithError(errors.New("producer stopped"))
	}()
	_, err := c.Upload(ctx, "k", pr, -1, "")
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if f.openUploads() != 0 || len(f.aborted) != 1 {
		t.Fatalf("cancelled upload must be aborted (open=%d aborted=%v)", f.openUploads(), f.aborted)
	}
	if ents, _ := os.ReadDir(tmp); len(ents) != 0 {
		t.Fatalf("temporary part files left behind: %d", len(ents))
	}
}

func TestAbortStaleUploadsOnlyExactKey(t *testing.T) {
	f, srv := newFakeS3(t)
	c := newTestClient(t, srv.URL, nil)
	ctx := context.Background()
	for _, k := range []string{"p/id1/artifact", "p/id1/artifact", "p/id1/artifact.tar", "p/id2/artifact"} {
		if _, err := c.initiateMultipart(ctx, k, ""); err != nil {
			t.Fatal(err)
		}
	}
	n, err := c.AbortStaleUploads(ctx, "p/id1/artifact")
	if err != nil || n != 2 {
		t.Fatalf("aborted %d err %v, want 2", n, err)
	}
	if f.openUploads() != 2 {
		t.Fatalf("other keys' uploads must survive, open = %d", f.openUploads())
	}
}

func TestUnknownSizePartSizeGrows(t *testing.T) {
	base := int64(64 << 20)
	var total int64
	for n := 1; n <= maxParts; n++ {
		p := unknownSizePartSize(base, n)
		if p < base || p > maxPartSize {
			t.Fatalf("part %d size %d out of range", n, p)
		}
		total += p
	}
	if unknownSizePartSize(base, 1000) != base || unknownSizePartSize(base, 1001) != 2*base {
		t.Fatal("part size must double every 1000 parts")
	}
	if total < 6<<40 {
		t.Fatalf("unknown-size capacity %d bytes, want >= 6 TiB", total)
	}
}

func TestCredentialsNeverInErrors(t *testing.T) {
	_, srv := newFakeS3(t)
	c := newTestClient(t, srv.URL, nil)
	// A misbehaving endpoint echoing credentials back in its error message.
	echo := strings.NewReader("<Error><Code>AccessDenied</Code><Message>bad key " + testAccess + " secret " + testSecret + " token " + testToken + "</Message></Error>")
	err := c.responseError("PUT object", &http.Response{StatusCode: 403, Body: io.NopCloser(echo)})
	for _, s := range []string{testAccess, testSecret, testToken} {
		if strings.Contains(err.Error(), s) {
			t.Fatalf("error leaks credential: %v", err)
		}
	}
	_, err = New(Config{Endpoint: "http://user:" + testSecret + "@[::1", Bucket: "b", AccessKey: "a", SecretKey: "s"})
	if err == nil || strings.Contains(err.Error(), testSecret) {
		t.Fatalf("endpoint error leaks userinfo: %v", err)
	}
	// Network errors carry the URL only, never the signing material.
	c2 := newTestClient(t, "http://127.0.0.1:1", func(c *Config) { c.MaxRetries = -1 })
	_, err = c2.Head(context.Background(), "k")
	if err == nil || strings.Contains(err.Error(), testSecret) || strings.Contains(err.Error(), testToken) {
		t.Fatalf("network error = %v", err)
	}
}

func TestDownloadVerifiedDetectsCorruption(t *testing.T) {
	f, srv := newFakeS3(t)
	c := newTestClient(t, srv.URL, nil)
	ctx := context.Background()
	data := randomBytes(2*int(minPartSize) + 7)
	info, err := c.Upload(ctx, "k", bytes.NewReader(data), int64(len(data)), "")
	if err != nil {
		t.Fatal(err)
	}
	// Intact object verifies.
	rc, _, err := c.DownloadVerified(ctx, "k", info.Size, info.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("intact download: err=%v", err)
	}
	rc.Close()
	// Same-size bit flip: only the digest can catch it, and only at EOF.
	f.mu.Lock()
	f.objects["k"][100] ^= 0xff
	f.mu.Unlock()
	rc, _, err = c.DownloadVerified(ctx, "k", info.Size, info.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, rc); err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatalf("corrupted object must fail verification, err = %v", err)
	}
	rc.Close()
	// Size mismatch is rejected before streaming.
	if _, _, err := c.DownloadVerified(ctx, "k", info.Size+1, info.SHA256); err == nil {
		t.Fatal("size mismatch must be rejected")
	}
}

func TestSinglePutStoresDigestMetadata(t *testing.T) {
	f, srv := newFakeS3(t)
	c := newTestClient(t, srv.URL, nil)
	data := []byte("small backup")
	if _, err := c.Upload(context.Background(), "s", bytes.NewReader(data), int64(len(data)), ""); err != nil {
		t.Fatal(err)
	}
	if got := f.meta["s"].Get("X-Amz-Meta-Strand-Sha256"); got != sumHex(data) {
		t.Fatalf("sha metadata = %q", got)
	}
	f.objects["s"][0] ^= 1
	rc, _, err := c.Download(context.Background(), "s")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if _, err := io.Copy(io.Discard, rc); err == nil {
		t.Fatal("download of a tampered single-PUT object must fail against its stored digest")
	}
}
