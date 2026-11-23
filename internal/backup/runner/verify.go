package runner

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
)

// VerifyOptions controls an integrity check.
type VerifyOptions struct {
	// SamplePercent of payloads are fully re-read (decrypted, decompressed, plaintext SHA-256 compared);
	// the others get a HEAD check (existence, size, stored digest metadata). 0 or 100 = all.
	SamplePercent int
}

// VerifyReport summarizes a check of one job or one backup.
type VerifyReport struct {
	Checked   int      `json:"checked"`
	FullReads int      `json:"full_reads"`
	Failed    []string `json:"failed"`
	Problems  []string `json:"problems"`
	Bytes     int64    `json:"bytes_read"`
}

// OK reports a clean result.
func (r VerifyReport) OK() bool { return len(r.Failed) == 0 && len(r.Problems) == 0 }

// VerifyBackup checks one backup and records the result in the catalog.
func (e *Env) VerifyBackup(ctx context.Context, rec backup.Record, full bool) error {
	started := e.now()
	var read int64
	err := e.checkPayload(ctx, rec, full, &read)
	status, detail := backup.CheckOK, "head"
	if full {
		detail = "full read: stored and plaintext SHA-256 match"
	}
	if err != nil {
		status, detail = backup.CheckFailed, err.Error()
	}
	if _, aerr := e.Catalog.AddCheck(ctx, backup.CheckResult{BackupID: rec.ID, Kind: "verify", Status: status, StartedAt: started,
		FinishedAt: e.now(), Detail: detail, Metrics: map[string]string{"bytes_read": strconv.FormatInt(read, 10), "full": strconv.FormatBool(full)}}); aerr != nil {
		return aerr
	}
	return err
}

func (e *Env) checkPayload(ctx context.Context, rec backup.Record, full bool, read *int64) error {
	repo, err := e.OpenRecordRepository(ctx, rec)
	if err != nil {
		return err
	}
	head, err := repo.Store.HeadVersion(ctx, rec.ObjectKey, rec.VersionID)
	if err != nil {
		return fmt.Errorf("payload missing or unreadable: %w", err)
	}
	if head.Size != rec.Bytes {
		return fmt.Errorf("payload size %d differs from the catalog (%d)", head.Size, rec.Bytes)
	}
	if head.SHA256 != "" && !strings.EqualFold(head.SHA256, rec.SHA256) {
		return fmt.Errorf("payload SHA-256 metadata differs from the catalog")
	}
	if rec.LockMode != "" && rec.RetainUntil != nil && head.RetainUntil != nil && head.RetainUntil.Before(*rec.RetainUntil) {
		return fmt.Errorf("object lock retention is shorter than recorded")
	}
	if !full {
		return nil
	}
	body, err := repo.OpenPayload(ctx, rec, e.Keys)
	if err != nil {
		return err
	}
	defer body.Close()
	n, err := io.Copy(io.Discard, body)
	*read += n
	return err
}

// VerifyJob checks every complete backup of a job: payload integrity (sampled full reads), incremental
// chain completeness, and continuity of archived logs (no gaps between consecutive segments).
func (e *Env) VerifyJob(ctx context.Context, job backup.Job, opt VerifyOptions) (VerifyReport, error) {
	rs, err := e.Catalog.ListAll(ctx, backup.Filter{JobID: job.ID, Status: backup.StatusComplete}, 0)
	if err != nil {
		return VerifyReport{}, err
	}
	report := e.verifyRecords(ctx, rs, opt)
	return report, ctx.Err()
}

func (e *Env) verifyRecords(ctx context.Context, rs []backup.Record, opt VerifyOptions) VerifyReport {
	rep := VerifyReport{Failed: []string{}, Problems: []string{}}
	byID := map[string]backup.Record{}
	for _, r := range rs {
		byID[r.ID] = r
	}
	pct := opt.SamplePercent
	if pct <= 0 || pct > 100 {
		pct = 100
	}
	for i, r := range rs {
		if ctx.Err() != nil {
			break
		}
		// The newest restore point is always fully read; others by sample.
		full := pct == 100 || i == 0 || rand.IntN(100) < pct
		var n int64
		started := e.now()
		err := e.checkPayload(ctx, r, full, &n)
		rep.Checked++
		rep.Bytes += n
		if full {
			rep.FullReads++
		}
		status, detail := backup.CheckOK, "head"
		if full {
			detail = "full read"
		}
		if err != nil {
			status, detail = backup.CheckFailed, err.Error()
			rep.Failed = append(rep.Failed, r.ID+": "+err.Error())
		}
		_, _ = e.Catalog.AddCheck(ctx, backup.CheckResult{BackupID: r.ID, Kind: "verify", Status: status, StartedAt: started,
			FinishedAt: e.now(), Detail: detail, Metrics: map[string]string{"bytes_read": strconv.FormatInt(n, 10)}})
		if r.KindOrFull() == backup.KindIncremental {
			if _, err := restoreChain(r, byID); err != nil {
				rep.Problems = append(rep.Problems, err.Error())
			}
		}
	}
	rep.Problems = append(rep.Problems, LogGaps(rs)...)
	return rep
}

// LogGaps reports discontinuities between consecutive archived log segments of each source: a WAL
// segment whose start LSN is not the previous end, binlog archive sequence holes, or oplog slices that do
// not overlap/abut.
func LogGaps(rs []backup.Record) []string {
	byLineage := map[archiveLineage][]backup.Record{}
	for _, r := range rs {
		if r.KindOrFull() == backup.KindLog && r.Status == backup.StatusComplete && r.CopyOf == "" {
			key := archiveLineageOf(r)
			byLineage[key] = append(byLineage[key], r)
		}
	}
	var out []string
	for lineage, logs := range byLineage {
		sort.SliceStable(logs, func(i, j int) bool { return logSeq(logs[i]) < logSeq(logs[j]) })
		for i := 1; i < len(logs); i++ {
			prev, cur := logs[i-1], logs[i]
			if !Contiguous(prev, cur) {
				out = append(out, fmt.Sprintf("%s: log gap between %s and %s", lineage.source, prev.ID, cur.ID))
			}
		}
	}
	sort.Strings(out)
	return out
}

// logSeq orders log segments: the archive sequence recorded at upload (details "seq").
func logSeq(r backup.Record) string {
	if s := r.Details["seq"]; s != "" {
		return fmt.Sprintf("%020s", s)
	}
	return r.CreatedAt.UTC().Format(time.RFC3339Nano)
}

// Contiguous reports whether cur directly follows prev.
func Contiguous(prev, cur backup.Record) bool {
	if prev.Engine != cur.Engine || prev.Source != cur.Source || prev.Method != cur.Method || prev.JobID != cur.JobID {
		return false
	}
	switch prev.Method {
	case backup.MethodWAL:
		return prev.EndPosition.LSN != "" && prev.EndPosition.LSN == cur.Position.LSN &&
			prev.Details["source_identity"] != "" && prev.Details["source_identity"] == cur.Details["source_identity"] &&
			prev.Details["timeline"] != "" && prev.Details["timeline"] == cur.Details["timeline"]
	case backup.MethodBinlog:
		a, _ := strconv.ParseInt(prev.Details["seq"], 10, 64)
		b, _ := strconv.ParseInt(cur.Details["seq"], 10, 64)
		return a > 0 && b == a+1 &&
			prev.Details["source_identity"] != "" && prev.Details["source_identity"] == cur.Details["source_identity"] &&
			nextBinlogFile(prev, cur.Position.BinlogFile) &&
			cur.Position.BinlogPos == 4 && cur.EndPosition.BinlogPos > cur.Position.BinlogPos
	case backup.MethodOplog:
		return prev.Details["source_identity"] != "" && prev.Details["source_identity"] == cur.Details["source_identity"] &&
			prev.Details["oplog_boundary"] == "exclusive-v1" && cur.Details["oplog_boundary"] == "exclusive-v1" &&
			prev.EndPosition.OplogTS != "" && prev.EndPosition.OplogTS == cur.Position.OplogTS
	}
	return true
}

// When MySQL shuts down cleanly, STOP closes the current file and the next
// startup increments its sequence without a ROTATE event in the old file.
func nextBinlogFile(prev backup.Record, next string) bool {
	if next == "" {
		return false
	}
	if successor := prev.Details["next_file"]; successor != "" {
		return successor == next
	}
	if prev.Details["closed_with_stop"] != "true" {
		return false
	}
	n := strings.LastIndexByte(prev.EndPosition.BinlogFile, '.')
	m := strings.LastIndexByte(next, '.')
	if n < 0 || m < 0 || prev.EndPosition.BinlogFile[:n] != next[:m] {
		return false
	}
	a, aErr := strconv.ParseUint(prev.EndPosition.BinlogFile[n+1:], 10, 32)
	b, bErr := strconv.ParseUint(next[m+1:], 10, 32)
	return aErr == nil && bErr == nil && b == a+1
}
