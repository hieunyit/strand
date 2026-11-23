package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"strand/internal/backup"
)

// Copy replicates a complete backup to another repository: the stored bytes are copied unchanged
// (encrypted backups stay encrypted with the same key), verified against the catalog SHA-256 while
// downloading, re-verified after upload, and cataloged as a new record with copy_of = the original.
func (e *Env) Copy(ctx context.Context, src backup.Record, toRepo string) (out backup.Record, err error) {
	if src.Status != backup.StatusComplete {
		return backup.Record{}, fmt.Errorf("backup %s is %s; only complete backups can be copied", src.ID, src.Status)
	}
	if toRepo == "" || toRepo == src.Repository {
		return backup.Record{}, errors.New("the destination must be a different repository")
	}
	from, err := e.OpenRecordRepository(ctx, src)
	if err != nil {
		return backup.Record{}, err
	}
	dst, err := e.OpenJobRepository(ctx, toRepo, "", "")
	if err != nil {
		return backup.Record{}, err
	}
	id := NewID("cpy")
	rel := strings.TrimPrefix(src.ObjectKey, from.Prefix+"/")
	key := dst.Key("copies", id, rel[strings.LastIndex(rel, "/")+1:])
	rec := src
	rec.ID, rec.Bucket, rec.ObjectKey, rec.Repository, rec.CopyOf = id, dst.Bucket, key, dst.ID, src.ID
	rec.ManifestURI = dst.URI(dst.Key("copies", id, "manifest.json"))
	rec.Status, rec.CreatedAt, rec.VerifiedAt, rec.SHA256, rec.Bytes = backup.StatusQueued, e.now(), nil, "", 0
	rec.CheckedAt, rec.CheckStatus, rec.CheckDetail, rec.ExpiredAt = nil, "", "", nil
	rec.LegalHold = false
	meta := backup.MetaOf(src)
	meta.LockMode, meta.RetainUntil, meta.VersionID = "", nil, ""
	if meta.Details != nil {
		d := map[string]string{}
		for k, v := range meta.Details {
			if k != "manifest_version" {
				d[k] = v
			}
		}
		meta.Details = d
	}
	rec.ApplyMeta(meta)
	defer func() {
		err = e.audit(context.WithoutCancel(ctx), "backup.copy", err, map[string]any{"backup_id": src.ID, "copy_id": id, "repository": toRepo})
	}()
	if _, _, err := e.Catalog.Create(ctx, rec); err != nil {
		return backup.Record{}, err
	}
	fail := func(cause error) (backup.Record, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, _, _ = e.Catalog.UpdateStatus(fctx, id, backup.StatusFailed, nil)
		return backup.Record{}, cause
	}
	if _, _, err := e.Catalog.UpdateStatus(ctx, id, backup.StatusUploading, nil); err != nil {
		return fail(err)
	}
	body, _, err := from.Store.DownloadVerifiedVersion(ctx, src.ObjectKey, src.VersionID, src.Bytes, src.SHA256)
	if err != nil {
		return fail(err)
	}
	put := dst.putOptions(0, 0, time.Now())
	info, err := dst.Store.UploadWith(ctx, key, body, src.Bytes, src.SHA256, put)
	body.Close()
	if err != nil {
		return fail(fmt.Errorf("upload copy: %w", err))
	}
	meta.VersionID = info.VersionID
	if put.LockMode != "" {
		t := put.RetainUntil
		meta.LockMode, meta.RetainUntil = put.LockMode, &t
	}
	if rec, _, err = e.Catalog.UpdateMeta(ctx, id, rec.Engine, meta); err != nil {
		return fail(err)
	}
	return e.finalize(ctx, dst, rec, info.Size, info.SHA256, 0, fail)
}
