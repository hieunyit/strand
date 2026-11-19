package objectstore

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// PutOptions are per-upload settings.
type PutOptions struct {
	// LockMode/RetainUntil apply S3 Object Lock retention (the bucket must have Object Lock enabled).
	// COMPLIANCE cannot be shortened or removed by anyone; GOVERNANCE can by principals with
	// s3:BypassGovernanceRetention (Strand never bypasses it).
	LockMode    string
	RetainUntil time.Time
	// BytesPerSecond limits the read rate of the upload source (0 = unlimited).
	BytesPerSecond int64
}

func (o PutOptions) validate() error {
	switch o.LockMode {
	case "":
		if !o.RetainUntil.IsZero() {
			return errors.New("objectstore: retain-until requires a lock mode")
		}
	case "GOVERNANCE", "COMPLIANCE":
		if !o.RetainUntil.After(time.Now()) {
			return errors.New("objectstore: object lock retain-until must be in the future")
		}
	default:
		return errors.New("objectstore: lock mode must be GOVERNANCE or COMPLIANCE")
	}
	if o.BytesPerSecond < 0 {
		return errors.New("objectstore: negative bandwidth limit")
	}
	return nil
}

func (o PutOptions) apply(h http.Header) {
	if o.LockMode != "" {
		h.Set("X-Amz-Object-Lock-Mode", o.LockMode)
		h.Set("X-Amz-Object-Lock-Retain-Until-Date", o.RetainUntil.UTC().Format("2006-01-02T15:04:05.000Z"))
	}
}

func contentMD5(b []byte) string {
	sum := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(sum[:])
}

// DeleteVersion permanently deletes one version of an object (versioned / Object Lock buckets). It fails
// while the version is under retention; a plain Delete would only add a delete marker.
func (s *S3) DeleteVersion(ctx context.Context, key, versionID string) error {
	if versionID == "" {
		return s.Delete(ctx, key)
	}
	if err := validateKey(key); err != nil {
		return err
	}
	resp, err := s.request(ctx, http.MethodDelete, key, url.Values{"versionId": {versionID}}, nil, nil, 0)
	if err != nil {
		return err
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return s.responseError("DELETE object version", resp)
	}
	return nil
}

// HeadVersion is Head of a specific object version.
func (s *S3) HeadVersion(ctx context.Context, key, versionID string) (ObjectInfo, error) {
	if versionID == "" {
		return s.Head(ctx, key)
	}
	resp, err := s.request(ctx, http.MethodHead, key, url.Values{"versionId": {versionID}}, nil, nil, 0)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		return ObjectInfo{}, s.responseError("HEAD object version", resp)
	}
	return infoFromHeaders(key, resp.Header), nil
}

// CreateBucketWithObjectLock creates the configured bucket with Object Lock (and versioning) enabled
// (operator/test tooling; Object Lock can only be enabled when a bucket is created).
func (s *S3) CreateBucketWithObjectLock(ctx context.Context) error {
	resp, err := s.request(ctx, http.MethodPut, "", nil, http.Header{"X-Amz-Bucket-Object-Lock-Enabled": {"true"}}, nil, 0)
	if err != nil {
		return err
	}
	if !isSuccess(resp.StatusCode) {
		return s.responseError("create bucket with object lock", resp)
	}
	closeResponse(resp)
	return nil
}

// Bucket returns the configured bucket.
func (s *S3) Bucket() string { return s.cfg.Bucket }

// rateLimitedReader paces reads to an average of bps bytes per second.
type rateLimitedReader struct {
	ctx   context.Context
	r     io.Reader
	bps   int64
	start time.Time
	n     int64
}

func newRateLimitedReader(ctx context.Context, r io.Reader, bps int64) io.Reader {
	return &rateLimitedReader{ctx: ctx, r: r, bps: bps}
}

func (l *rateLimitedReader) Read(p []byte) (int, error) {
	if l.start.IsZero() {
		l.start = time.Now()
	}
	if max := int(l.bps / 4); max > 0 && len(p) > max {
		p = p[:max] // small slices keep the pacing smooth
	}
	n, err := l.r.Read(p)
	l.n += int64(n)
	due := l.start.Add(time.Duration(float64(l.n) / float64(l.bps) * float64(time.Second)))
	if wait := time.Until(due); wait > 0 {
		t := time.NewTimer(wait)
		select {
		case <-l.ctx.Done():
			t.Stop()
			return n, l.ctx.Err()
		case <-t.C:
		}
	}
	return n, err
}

// stagedPartPattern names temporary part files with the owning PID so a later process can tell parts of
// a killed uploader (dead PID) from parts of a running one.
func stagedPartPattern() string { return "strand-s3-part-" + strconv.Itoa(os.Getpid()) + "-*" }

// CleanStaleParts removes staged multipart files left in dir by uploaders that no longer run (killed with
// SIGKILL, host crash). Files of live processes are kept. Returns the number of files removed.
func CleanStaleParts(dir string) (int, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, e := range ents {
		name := e.Name()
		if !e.Type().IsRegular() || !strings.HasPrefix(name, "strand-s3-part-") {
			continue
		}
		rest := strings.TrimPrefix(name, "strand-s3-part-")
		pidText, _, ok := strings.Cut(rest, "-")
		pid, perr := strconv.Atoi(pidText)
		if ok && perr == nil && pid > 0 {
			if err := syscall.Kill(pid, 0); err == nil || errors.Is(err, syscall.EPERM) {
				continue // owner still alive
			}
		} else if info, err := e.Info(); err != nil || time.Since(info.ModTime()) < 24*time.Hour {
			continue // legacy name without PID: only when clearly abandoned
		}
		if err := os.Remove(filepath.Join(dir, name)); err == nil {
			removed++
		} else if !errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("remove stale staged part: %w", err)
		}
	}
	return removed, nil
}
