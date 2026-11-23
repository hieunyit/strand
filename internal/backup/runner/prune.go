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
	rs, err := e.pruneRecords(ctx, backup.Filter{JobID: job.ID}, dryRun)
	if err != nil {
		return PruneResult{}, err
	}
	return e.prune(ctx, "job:"+job.ID, rs, job.Retention, dryRun), nil
}

// PruneRepository applies a repository's default policy to its backups that belong to no job, per source.
func (e *Env) PruneRepository(ctx context.Context, repo backup.Repo, dryRun bool) ([]PruneResult, error) {
	rs, err := e.pruneRecords(ctx, backup.Filter{Repository: repo.ID}, dryRun)
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

func (e *Env) pruneRecords(ctx context.Context, filter backup.Filter, dryRun bool) ([]backup.Record, error) {
	filter.Status = backup.StatusComplete
	records, err := e.Catalog.ListAll(ctx, filter, 0)
	if err != nil || dryRun {
		return records, err
	}
	filter.Status = backup.StatusDeleting
	pending, err := e.Catalog.ListAll(ctx, filter, 0)
	return append(records, pending...), err
}

func (e *Env) prune(ctx context.Context, group string, rs []backup.Record, p backup.RetentionPolicy, dryRun bool) PruneResult {
	var complete []backup.Record
	byID := map[string]backup.Record{}
	res := PruneResult{Group: group, DryRun: dryRun, Expired: []string{}}
	for _, r := range rs {
		byID[r.ID] = r
		if r.Status == backup.StatusComplete {
			complete = append(complete, r)
		}
	}
	res.Decisions = backup.PlanRetention(complete, p, e.now())
	if dryRun {
		return res
	}
	for _, r := range byID {
		if r.Status == backup.StatusDeleting {
			res.Decisions = append(res.Decisions, backup.Decision{ID: r.ID, Reasons: []string{"resume retention deletion"}})
		}
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

// expire claims a backup before touching storage. A subsequent prune resumes
// the claim if deletion or expiration was interrupted.
func (e *Env) expire(ctx context.Context, r backup.Record, reason string) (err error) {
	defer func() {
		err = e.audit(context.WithoutCancel(ctx), "backup.expire", err, map[string]any{"backup_id": r.ID, "source": r.Source,
			"object": "s3://" + r.Bucket + "/" + r.ObjectKey, "reason": reason})
	}()
	repo, err := e.OpenRecordRepository(ctx, r)
	if err != nil {
		return err
	}
	claimed, ok, err := e.Catalog.ClaimDeletion(ctx, r.ID, e.now())
	if err != nil || !ok {
		return err
	}
	r = claimed
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
