package objectstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"strand/internal/testutil"
)

// Object Lock against a real MinIO bucket created with lock enabled: retention headers are applied to
// single and multipart uploads (Content-MD5 is required), the version id is returned, a locked version
// cannot be deleted, and an unlocked one can.
func TestMinIOObjectLock(t *testing.T) {
	endpoint := testutil.MinIOEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := New(Config{Endpoint: endpoint, Bucket: "locked-backups", AccessKey: "minioadmin", SecretKey: "minioadmin",
		AllowInsecureLocal: true, PartSize: minPartSize, Concurrency: 2, TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateBucketWithObjectLock(ctx); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	for _, size := range []int{1000, 2*int(minPartSize) + 77} {
		data := randomBytes(size)
		key := "locked/" + strings.Repeat("x", size%7+1)
		info, err := c.UploadWith(ctx, key, onlyReader{bytes.NewReader(data)}, -1, "", PutOptions{LockMode: "COMPLIANCE", RetainUntil: until})
		if err != nil {
			t.Fatalf("locked upload of %d bytes: %v", size, err)
		}
		if info.VersionID == "" {
			t.Fatalf("versioned bucket returned no version id: %+v", info)
		}
		head, err := c.HeadVersion(ctx, key, info.VersionID)
		if err != nil || head.LockMode != "COMPLIANCE" || head.RetainUntil == nil || !head.RetainUntil.Equal(until) {
			t.Fatalf("head = %+v err %v", head, err)
		}
		if err := c.DeleteVersion(ctx, key, info.VersionID); err == nil {
			t.Fatal("a COMPLIANCE-locked version was deleted")
		}
		rc, _, err := c.DownloadVerified(ctx, key, info.Size, info.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatal("locked object content differs")
		}
	}
	info, err := c.UploadWith(ctx, "unlocked/a", bytes.NewReader([]byte("x")), 1, "", PutOptions{})
	if err != nil || info.VersionID == "" {
		t.Fatalf("unlocked: %+v %v", info, err)
	}
	if err := c.DeleteVersion(ctx, "unlocked/a", info.VersionID); err != nil {
		t.Fatalf("unlocked version delete: %v", err)
	}
	if _, err := c.HeadVersion(ctx, "unlocked/a", info.VersionID); err == nil {
		t.Fatal("deleted version still exists")
	}
	if _, err := c.UploadWith(ctx, "bad", bytes.NewReader(nil), 0, "", PutOptions{LockMode: "COMPLIANCE", RetainUntil: time.Now().Add(-time.Hour)}); err == nil {
		t.Fatal("retain-until in the past accepted")
	}
}

func TestRateLimitedUploadReader(t *testing.T) {
	data := make([]byte, 300<<10)
	start := time.Now()
	r := newRateLimitedReader(context.Background(), bytes.NewReader(data), 1<<20) // 1 MiB/s
	n, err := io.Copy(io.Discard, r)
	if err != nil || n != int64(len(data)) {
		t.Fatal(err)
	}
	if el := time.Since(start); el < 250*time.Millisecond || el > 2*time.Second {
		t.Fatalf("300 KiB at 1 MiB/s took %v", el)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := io.Copy(io.Discard, newRateLimitedReader(ctx, bytes.NewReader(data), 1024)); err == nil {
		t.Fatal("cancelled limiter kept reading")
	}
}

func TestCleanStaleParts(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, age time.Duration) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		mt := time.Now().Add(-age)
		_ = os.Chtimes(p, mt, mt)
	}
	write("strand-s3-part-"+strconv.Itoa(os.Getpid())+"-123", 0) // ours, alive
	write("strand-s3-part-999999999-456", 0)                     // dead pid
	write("strand-s3-part-789", 48*time.Hour)                    // legacy, old
	write("strand-s3-part-790", time.Minute)                     // legacy, recent
	write("unrelated", 48*time.Hour)                             //
	n, err := CleanStaleParts(dir)
	if err != nil || n != 2 {
		t.Fatalf("removed %d err %v", n, err)
	}
	left, _ := os.ReadDir(dir)
	if len(left) != 3 {
		t.Fatalf("left %d", len(left))
	}
}
