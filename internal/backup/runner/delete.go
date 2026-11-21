package runner

import (
	"context"
	"fmt"
	"strings"

	"strand/internal/backup"
)

// DeleteResult reports what DeleteRecord removed.
type DeleteResult struct {
	ID             string   `json:"id"`
	Status         string   `json:"status"`
	DeletedObjects []string `json:"deleted_objects"`
	AbortedUploads int      `json:"aborted_uploads"`
}

// DeleteRecord removes a restore point that is not complete (failed, expired, or an abandoned in-progress
// record the caller already moved to failed): the objects it uploaded in its repository (payload, any
// unfinished multipart upload of the payload, the manifest sidecar), then its catalog record. Complete
// restore points are refused (retention removes them), and so are records under legal hold or an
// unexpired Object Lock retention. Like retention, objects go first: a crash in between leaves a record
// whose objects are gone, and deleting it again finishes the job; the opposite order could leak objects.
// An expired record's objects were already deleted by retention, so only its record is removed.
func (e *Env) DeleteRecord(ctx context.Context, r backup.Record) (res DeleteResult, err error) {
	res = DeleteResult{ID: r.ID, Status: string(r.Status), DeletedObjects: []string{}}
	defer func() {
		err = e.audit(context.WithoutCancel(ctx), "backup.delete", err, map[string]any{"backup_id": r.ID, "source": r.Source,
			"status": string(r.Status), "object": "s3://" + r.Bucket + "/" + r.ObjectKey, "deleted_objects": res.DeletedObjects})
	}()
	if backup.InProgress(r.Status) {
		return res, fmt.Errorf("restore point %s is %s: mark it failed before deleting it", r.ID, r.Status)
	}
	if err := backup.DeletableErr(r, e.now()); err != nil {
		return res, err
	}
	if r.Status != backup.StatusExpired {
		repo, err := e.OpenRecordRepository(ctx, r)
		if err != nil {
			return res, err
		}
		n, err := repo.Store.AbortStaleUploads(ctx, r.ObjectKey)
		res.AbortedUploads = n
		if err != nil {
			return res, fmt.Errorf("abort unfinished upload: %w", err)
		}
		if err := repo.Delete(ctx, r.ObjectKey, r.VersionID); err != nil {
			return res, fmt.Errorf("delete payload: %w", err)
		}
		res.DeletedObjects = append(res.DeletedObjects, repo.URI(r.ObjectKey))
		if strings.HasPrefix(r.ManifestURI, "s3://"+r.Bucket+"/") {
			mk := strings.TrimPrefix(r.ManifestURI, "s3://"+r.Bucket+"/")
			if mk != r.ObjectKey {
				if err := repo.Delete(ctx, mk, r.Details["manifest_version"]); err != nil {
					return res, fmt.Errorf("delete manifest: %w", err)
				}
				res.DeletedObjects = append(res.DeletedObjects, repo.URI(mk))
			}
		}
	}
	if _, _, err := e.Catalog.Delete(ctx, r.ID, e.now()); err != nil {
		return res, err
	}
	e.logf("restore point %s (%s) deleted", r.ID, r.Status)
	return res, nil
}
