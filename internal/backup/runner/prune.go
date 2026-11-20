package runner

import (
	"context"
	"fmt"
	"strings"

	"strand/internal/backup"
)

// PruneResult reports one retention run over one group.
type PruneResult struct {
	Group     string            `json:"group"`
	DryRun    bool              `json:"dry_run"`
	Decisions []backup.Decision `json:"decisions"`
	Expired   []string          `json:"expired"`
	Errors    []string          `json:"errors,omitempty"`
}

// PruneJob applies the job's retention policy to every backup of the job (including its log segments).
func (e *Env) PruneJob(ctx context.Context, job backup.Job, dryRun bool) (PruneResult, error) {
	rs, err := e.Catalog.ListAll(ctx, backup.Filter{JobID: job.ID, Status: backup.StatusComplete}, 0)
	if err != nil {
		return PruneResult{}, err
	}
	return e.prune(ctx, "job:"+job.ID, rs, job.Retention, dryRun), nil
}

// PruneRepository applies a repository's default policy to its backups that belong to no job, per source.
func (e *Env) PruneRepository(ctx context.Context, repo backup.Repo, dryRun bool) ([]PruneResult, error) {
	rs, err := e.Catalog.ListAll(ctx, backup.Filter{Repository: repo.ID, Status: backup.StatusComplete}, 0)
	if err != nil {
		return nil, err
	}
	groups := map[string][]backup.Record{}
	var order []string
	for _, r := range rs {
		if r.JobID != "" {
			continue
		}
		g := backup.RetentionGroup(r)
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], r)
	}
	var out []PruneResult
	for _, g := range order {
		out = append(out, e.prune(ctx, g, groups[g], repo.Retention, dryRun))
	}
	return out, nil
}

func (e *Env) prune(ctx context.Context, group string, rs []backup.Record, p backup.RetentionPolicy, dryRun bool) PruneResult {
	res := PruneResult{Group: group, DryRun: dryRun, Decisions: backup.PlanRetention(rs, p, e.now()), Expired: []string{}}
	if dryRun {
		return res
	}
	byID := map[string]backup.Record{}
	for _, r := range rs {
		byID[r.ID] = r
	}
	for _, d := range res.Decisions {
		if d.Keep {
			continue
		}
		if err := e.expire(ctx, byID[d.ID], strings.Join(d.Reasons, "; ")); err != nil {
			res.Errors = append(res.Errors, d.ID+": "+err.Error())
			continue
		}
		res.Expired = append(res.Expired, d.ID)
	}
	return res
}

// expire deletes a backup's objects, then marks it expired. Objects go first: a crash in between leaves
// a "complete" record whose objects are missing, which verification reports and the next prune retries;
// the opposite order could leak objects forever.
func (e *Env) expire(ctx context.Context, r backup.Record, reason string) (err error) {
	defer func() {
		err = e.audit(context.WithoutCancel(ctx), "backup.expire", err, map[string]any{"backup_id": r.ID, "source": r.Source,
			"object": "s3://" + r.Bucket + "/" + r.ObjectKey, "reason": reason})
	}()
	if r.LegalHold {
		return fmt.Errorf("legal hold")
	}
	if r.RetainUntil != nil && r.RetainUntil.After(e.now()) {
		return fmt.Errorf("object-locked until %s", r.RetainUntil.UTC())
	}
	repo, err := e.OpenRecordRepository(ctx, r)
	if err != nil {
		return err
	}
	if err := repo.Delete(ctx, r.ObjectKey, r.VersionID); err != nil {
		return fmt.Errorf("delete payload: %w", err)
	}
	if strings.HasPrefix(r.ManifestURI, "s3://"+r.Bucket+"/") {
		mk := strings.TrimPrefix(r.ManifestURI, "s3://"+r.Bucket+"/")
		if mk != r.ObjectKey {
			if err := repo.Delete(ctx, mk, r.Details["manifest_version"]); err != nil {
				return fmt.Errorf("delete manifest: %w", err)
			}
		}
	}
	_, _, err = e.Catalog.Expire(ctx, r.ID, e.now())
	return err
}
