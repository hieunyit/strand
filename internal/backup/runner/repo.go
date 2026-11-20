// Package runner executes managed backups: built-in backup methods streamed to an S3 repository with
// optional compression/encryption, retention enforcement, verification, copies and restores. It sits
// between the pure catalog model (internal/backup) and the S3 client (internal/objectstore).
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/objectstore"
)

// Getenv reads the process environment (replaceable in tests).
type Getenv func(string) string

// Repository is an opened S3 repository.
type Repository struct {
	ID       string // "" = default environment repository
	Store    *objectstore.S3
	Bucket   string
	Prefix   string
	LockMode string
	LockDays int
}

// OpenRepository opens repo (nil = the default STRAND_BACKUP_S3_* repository with the given bucket and
// prefix). Errors never echo environment values.
func OpenRepository(getenv Getenv, repo *backup.Repo, bucket, prefix string, partMiB, concurrency int) (*Repository, error) {
	envPrefix := backup.DefaultEnvPrefix
	r := &Repository{Bucket: bucket, Prefix: strings.Trim(prefix, "/")}
	if repo != nil {
		envPrefix, r.ID, r.Bucket, r.LockMode, r.LockDays = repo.EffectivePrefix(), repo.ID, repo.Bucket, repo.LockMode, repo.LockDays
		if repo.Prefix != "" {
			r.Prefix = strings.Trim(strings.Trim(repo.Prefix, "/")+"/"+r.Prefix, "/")
		}
	}
	if partMiB == 0 {
		partMiB = 64
	}
	if concurrency == 0 {
		concurrency = 4
	}
	insecure := strings.TrimSpace(getenv(envPrefix + "ALLOW_INSECURE_LOCAL"))
	tmp := getenv("STRAND_BACKUP_TMPDIR")
	if _, err := objectstore.CleanStaleParts(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("clean stale staged upload parts: %w", err)
	}
	obj, err := objectstore.New(objectstore.Config{
		Endpoint: strings.TrimSpace(getenv(envPrefix + "ENDPOINT")), Bucket: r.Bucket,
		Region:    strings.TrimSpace(getenv(envPrefix + "REGION")),
		AccessKey: getenv(envPrefix + "ACCESS_KEY"), SecretKey: getenv(envPrefix + "SECRET_KEY"),
		SessionToken:       getenv(envPrefix + "SESSION_TOKEN"),
		AllowInsecureLocal: strings.EqualFold(insecure, "true") || insecure == "1",
		PartSize:           int64(partMiB) << 20, Concurrency: concurrency, TempDir: tmp,
	})
	if err != nil {
		return nil, fmt.Errorf("configure backup repository %q: invalid settings; check %sENDPOINT, %sREGION and the credential variables", r.ID, envPrefix, envPrefix)
	}
	r.Store = obj
	return r, nil
}

// Key joins the repository prefix and parts.
func (r *Repository) Key(parts ...string) string {
	all := make([]string, 0, len(parts)+1)
	if r.Prefix != "" {
		all = append(all, r.Prefix)
	}
	for _, p := range parts {
		if p = strings.Trim(p, "/"); p != "" {
			all = append(all, p)
		}
	}
	return strings.Join(all, "/")
}

// URI returns s3://bucket/key.
func (r *Repository) URI(key string) string { return "s3://" + r.Bucket + "/" + key }

// putOptions returns the Object Lock / bandwidth options for a new object.
func (r *Repository) putOptions(lockDays int, bandwidthKBps int, now time.Time) objectstore.PutOptions {
	o := objectstore.PutOptions{BytesPerSecond: int64(bandwidthKBps) * 1024}
	if r.LockMode != "" {
		days := r.LockDays
		if lockDays > 0 {
			days = lockDays
		}
		o.LockMode, o.RetainUntil = r.LockMode, now.Add(time.Duration(days)*24*time.Hour).UTC().Truncate(time.Second)
	}
	return o
}

// PayloadResult describes a stored payload.
type PayloadResult struct {
	Object objectstore.ObjectInfo
	Plain  backup.StreamStats
	Format string
	Lock   objectstore.PutOptions
}

// StorePayload streams src through the encoder (compression/encryption) into key. src is read to EOF; an
// error from src (e.g. the backup tool failed) aborts the upload.
func (r *Repository) StorePayload(ctx context.Context, key string, src io.Reader, enc backup.EncodeOptions, lockDays, bandwidthKBps int) (PayloadResult, error) {
	put := r.putOptions(lockDays, bandwidthKBps, time.Now())
	pr, pw := io.Pipe()
	// The encoder writes its header immediately: create it on the writer side of the pipe.
	var encoder *backup.Encoder
	copyErr := make(chan error, 1)
	go func() {
		var err error
		if encoder, err = backup.NewEncoder(pw, enc); err != nil {
			_ = pw.CloseWithError(err)
			copyErr <- err
			return
		}
		_, err = io.Copy(encoder, src)
		if err == nil {
			err = encoder.Close()
		}
		_ = pw.CloseWithError(err)
		copyErr <- err
	}()
	info, upErr := r.Store.UploadWith(ctx, key, pr, -1, "", put)
	_ = pr.CloseWithError(errors.New("upload ended"))
	srcErr := <-copyErr
	if srcErr != nil {
		return PayloadResult{}, srcErr
	}
	if upErr != nil {
		return PayloadResult{}, upErr
	}
	return PayloadResult{Object: info, Plain: encoder.Stats(), Format: enc.Format(), Lock: put}, nil
}

// OpenPayload opens a cataloged payload and returns its verified plaintext: the stored bytes are checked
// against the catalog size/SHA-256 and the plaintext against plain_sha256 (when recorded). Errors are
// returned by the final Read, so consumers that read to EOF never accept corrupted data.
func (r *Repository) OpenPayload(ctx context.Context, rec backup.Record, keys *backup.Keyring) (io.ReadCloser, error) {
	body, _, err := r.Store.DownloadVerified(ctx, rec.ObjectKey, rec.Bytes, rec.SHA256)
	if err != nil {
		return nil, err
	}
	dec, err := backup.NewDecoder(body, backup.DecodeOptions{Format: rec.Format, Keys: keys, PlainSHA256: rec.PlainSHA256, PlainBytes: rec.PlainBytes})
	if err != nil {
		body.Close()
		return nil, err
	}
	return &payloadReader{Decoder: dec, body: body}, nil
}

type payloadReader struct {
	*backup.Decoder
	body io.ReadCloser
}

func (p *payloadReader) Close() error {
	_ = p.Decoder.Close()
	return p.body.Close()
}

// PutJSON stores a small JSON document (manifest sidecar) with the repository's lock settings.
func (r *Repository) PutJSON(ctx context.Context, key string, v any, lockDays int) (objectstore.ObjectInfo, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return objectstore.ObjectInfo{}, err
	}
	return r.Store.UploadWith(ctx, key, strings.NewReader(string(b)), int64(len(b)), "", r.putOptions(lockDays, 0, time.Now()))
}

// Delete removes an object version (or the object when unversioned).
func (r *Repository) Delete(ctx context.Context, key, versionID string) error {
	return r.Store.DeleteVersion(ctx, key, versionID)
}
