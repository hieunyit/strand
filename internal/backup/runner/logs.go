package runner

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"strand/internal/backup"
)

// logSegment is one archived log unit (WAL segment, binlog file, oplog slice).
type logSegment struct {
	ID     string
	Name   string // object name under <source>/
	Data   io.Reader
	Method string
	Seq    uint64 // archive order (contiguity is checked on it for binlogs)
	Meta   backup.Meta
}

// latestOriginalLog locates the archive cursor. Copies can be created at any
// time, so their creation time cannot decide where ingestion should resume.
func (e *Env) latestOriginalLog(ctx context.Context, jobID string) (*backup.Record, error) {
	filter := backup.Filter{JobID: jobID, Kind: backup.KindLog, Status: backup.StatusComplete, Limit: backup.MaxListLimit}
	for {
		page, err := e.Catalog.List(ctx, filter)
		if err != nil {
			return nil, err
		}
		for _, record := range page {
			if record.CopyOf == "" {
				return &record, nil
			}
		}
		if len(page) < filter.Limit {
			return nil, nil
		}
		last := page[len(page)-1]
		filter.After = &backup.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
}

// storeLog uploads and catalogs a log segment. A segment already cataloged complete is skipped
// (restarts re-send the tail of the stream).
func (e *Env) storeLog(ctx context.Context, job backup.Job, repo *Repository, enc backup.EncodeOptions, seg logSegment) (backup.Record, error) {
	if old, ok, err := e.Catalog.Get(ctx, seg.ID); err != nil {
		return backup.Record{}, err
	} else if ok && old.Status == backup.StatusComplete {
		return old, nil
	} else if ok {
		// A failed or interrupted earlier attempt: retry under the same id.
		if old.Status == backup.StatusUploading || old.Status == backup.StatusVerifying {
			_, _, _ = e.Catalog.UpdateStatus(ctx, seg.ID, backup.StatusFailed, nil)
		}
	}
	key := repo.Key(safeSegment(job.Source), "archives", safeSegment(job.ID), seg.Name)
	if enc.Format() == backup.FormatSBK1 {
		key += ".sbk"
	}
	rec := backup.Record{ID: seg.ID, Source: job.Source, Engine: job.Engine(), Tool: "strand", Bucket: repo.Bucket, ObjectKey: key,
		ManifestURI: repo.URI(key), Status: backup.StatusQueued, CreatedAt: e.now(), JobID: job.ID, Repository: repo.ID,
		Method: seg.Method, Kind: backup.KindLog}
	if _, _, err := e.Catalog.Create(ctx, rec); err != nil {
		return backup.Record{}, err
	}
	fail := func(cause error) (backup.Record, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, _, _ = e.Catalog.UpdateStatus(fctx, seg.ID, backup.StatusFailed, nil)
		return backup.Record{}, cause
	}
	if _, _, err := e.Catalog.UpdateStatus(ctx, seg.ID, backup.StatusUploading, nil); err != nil {
		return fail(err)
	}
	if _, err := repo.Store.AbortStaleUploads(ctx, key); err != nil {
		return fail(err)
	}
	res, err := repo.StorePayload(ctx, key, seg.Data, enc, job.Retention.LockDays, job.BandwidthKBps)
	if err != nil {
		return fail(err)
	}
	m := seg.Meta
	if m.Details == nil {
		m.Details = map[string]string{}
	}
	m.Details["seq"] = strconv.FormatUint(seg.Seq, 10)
	m.Format, m.PlainBytes, m.PlainSHA256, m.VersionID = res.Format, res.Plain.PlainBytes, res.Plain.PlainSHA256, res.Object.VersionID
	if enc.Compress {
		m.Compression = backup.CompressionZstd
	}
	if enc.Key != nil {
		m.KeyID = enc.Key.ID
	}
	if res.Lock.LockMode != "" {
		t := res.Lock.RetainUntil
		m.LockMode, m.RetainUntil = res.Lock.LockMode, &t
	}
	if rec, _, err = e.Catalog.UpdateMeta(ctx, seg.ID, rec.Engine, m); err != nil {
		return fail(fmt.Errorf("record log metadata: %w", err))
	}
	return e.finalize(ctx, repo, rec, res.Object.Size, res.Object.SHA256, job.Retention.LockDays, fail)
}
