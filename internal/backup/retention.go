package backup

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// RetentionPolicy is a grandfather-father-son policy in the style of restic/Kopia "forget" and
// pgBackRest repo retention. Each keep rule independently selects restore points (full and incremental
// backups); a backup is kept when any rule selects it. MaxAgeDays is a hard cap applied after the rules.
// The zero policy keeps everything.
type RetentionPolicy struct {
	KeepLast       int `json:"keep_last,omitempty"`
	KeepHourly     int `json:"keep_hourly,omitempty"`
	KeepDaily      int `json:"keep_daily,omitempty"`
	KeepWeekly     int `json:"keep_weekly,omitempty"`
	KeepMonthly    int `json:"keep_monthly,omitempty"`
	KeepYearly     int `json:"keep_yearly,omitempty"`
	KeepWithinDays int `json:"keep_within_days,omitempty"` // keep every restore point newer than this
	MaxAgeDays     int `json:"max_age_days,omitempty"`     // expire restore points older than this
	// LockDays: with an Object Lock repository, new objects are locked for this many days (0 = repository
	// default). Retention never deletes a locked object; it waits until the lock expires.
	LockDays int `json:"lock_days,omitempty"`
}

// Enabled reports whether the policy expires anything.
func (p RetentionPolicy) Enabled() bool {
	return p.KeepLast > 0 || p.KeepHourly > 0 || p.KeepDaily > 0 || p.KeepWeekly > 0 || p.KeepMonthly > 0 ||
		p.KeepYearly > 0 || p.KeepWithinDays > 0 || p.MaxAgeDays > 0
}

// Validate bounds the policy values.
func (p RetentionPolicy) Validate() error {
	for name, v := range map[string]int{"keep_last": p.KeepLast, "keep_hourly": p.KeepHourly, "keep_daily": p.KeepDaily,
		"keep_weekly": p.KeepWeekly, "keep_monthly": p.KeepMonthly, "keep_yearly": p.KeepYearly,
		"keep_within_days": p.KeepWithinDays, "max_age_days": p.MaxAgeDays, "lock_days": p.LockDays} {
		if v < 0 || v > 100000 {
			return fmt.Errorf("%w: retention %s must be between 0 and 100000", ErrInvalid, name)
		}
	}
	if p.MaxAgeDays > 0 && p.KeepWithinDays > p.MaxAgeDays {
		return fmt.Errorf("%w: retention keep_within_days cannot exceed max_age_days", ErrInvalid)
	}
	return nil
}

// Decision is the retention verdict for one record.
type Decision struct {
	ID      string   `json:"id"`
	Kind    string   `json:"kind"`
	Created string   `json:"created_at"`
	Keep    bool     `json:"keep"`
	Reasons []string `json:"reasons"`
	// Blocked: the policy would expire the record but legal hold or an object lock prevents it.
	Blocked bool `json:"blocked,omitempty"`
}

// PlanRetention applies p to the complete records of one retention group (one job, or one source in one
// repository) and returns a decision per record, newest first.
//
// The records are first split into lineages that are planned independently (see retentionLineage):
// originals among originals, and replicas (CopyOf set) among the replicas in the same target repository.
// A replica is created later than its original (it is cataloged when the copy runs), so planning both
// together would let a fresh replica take a keep slot and expire the original it was made from; a
// replica is also not a PITR base. Every guarantee below holds per lineage:
//   - the newest complete full backup is always kept (a policy can never leave zero restore points);
//   - an incremental that is kept keeps its whole parent chain;
//   - log segments (binlog/WAL/oplog) are kept from the oldest kept base backup onward so every kept base
//     stays point-in-time recoverable; older segments expire;
//   - legal hold and S3 Object Lock retention (RetainUntil in the future) block expiry.
//
// Verification results: a restore point whose last check failed (CheckStatus == CheckFailed) is not
// a usable restore point, so it never fills a keep slot (keep_last, hourly/daily/..., within) and is not
// the "newest full" the safety net protects; the keep rules select among the restore points that are not
// known to be bad (verified OK or not checked yet). A failed-check restore point is kept only while it is
// newer than the oldest kept good restore point (it may be a false alarm and costs no good slot) and
// expires once it is older than all of them. When a lineage has no good restore point at all, the
// failed-check ones are planned as ordinary restore points rather than deleting everything.
func PlanRetention(records []Record, p RetentionPolicy, now time.Time) []Decision {
	lineages := map[string][]Record{}
	var order []string
	byID := map[string]Record{}
	for _, r := range records {
		if r.Status != StatusComplete {
			continue
		}
		k := retentionLineage(r)
		if _, ok := lineages[k]; !ok {
			order = append(order, k)
		}
		lineages[k] = append(lineages[k], r)
		byID[r.ID] = r
	}
	out := []Decision{}
	for _, k := range order {
		out = append(out, planLineage(lineages[k], p, now)...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := byID[out[i].ID], byID[out[j].ID]
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.After(b.CreatedAt)
		}
		return a.ID > b.ID
	})
	return out
}

// retentionLineage is the set of records one retention plan covers: the originals of the group, or the
// replicas of the group stored in one target repository.
func retentionLineage(r Record) string {
	if r.CopyOf != "" {
		return "copy:" + r.Repository + "|" + r.Bucket
	}
	return "original"
}

func planLineage(recs []Record, p RetentionPolicy, now time.Time) []Decision {
	sort.SliceStable(recs, func(i, j int) bool {
		if !recs[i].CreatedAt.Equal(recs[j].CreatedAt) {
			return recs[i].CreatedAt.After(recs[j].CreatedAt)
		}
		return recs[i].ID > recs[j].ID
	})
	reasons := map[string][]string{}
	add := func(id, why string) { reasons[id] = append(reasons[id], why) }
	byID := map[string]Record{}
	// A replica keeps the ParentID of its original; within a replica lineage the parent is the replica
	// of that original.
	replicaOf := map[string]string{}
	var points, good, bad []Record
	for _, r := range recs {
		byID[r.ID] = r
		if r.CopyOf != "" {
			replicaOf[r.CopyOf] = r.ID
		}
		if r.KindOrFull() != KindLog {
			points = append(points, r)
			if r.CheckStatus == CheckFailed {
				bad = append(bad, r)
			} else {
				good = append(good, r)
			}
		}
	}
	if len(good) == 0 {
		good, bad = points, nil
	}
	if !p.Enabled() {
		out := make([]Decision, 0, len(recs))
		for _, r := range recs {
			out = append(out, decision(r, true, []string{"no retention policy"}, false))
		}
		return out
	}
	now = now.UTC()
	if p.KeepLast > 0 {
		for i, r := range good {
			if i < p.KeepLast {
				add(r.ID, fmt.Sprintf("keep-last %d", p.KeepLast))
			}
		}
	}
	bucketRule := func(n int, name string, key func(time.Time) string) {
		if n <= 0 {
			return
		}
		seen := map[string]bool{}
		for _, r := range good {
			k := key(r.CreatedAt.UTC())
			if seen[k] {
				continue
			}
			if len(seen) >= n {
				break
			}
			seen[k] = true
			add(r.ID, fmt.Sprintf("%s %s", name, k))
		}
	}
	bucketRule(p.KeepHourly, "hourly", func(t time.Time) string { return t.Format("2006-01-02T15") })
	bucketRule(p.KeepDaily, "daily", func(t time.Time) string { return t.Format("2006-01-02") })
	bucketRule(p.KeepWeekly, "weekly", func(t time.Time) string {
		y, w := t.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", y, w)
	})
	bucketRule(p.KeepMonthly, "monthly", func(t time.Time) string { return t.Format("2006-01") })
	bucketRule(p.KeepYearly, "yearly", func(t time.Time) string { return t.Format("2006") })
	if p.KeepWithinDays > 0 {
		cut := now.Add(-time.Duration(p.KeepWithinDays) * 24 * time.Hour)
		for _, r := range good {
			if !r.CreatedAt.Before(cut) {
				add(r.ID, fmt.Sprintf("within %dd", p.KeepWithinDays))
			}
		}
	}
	onlyMaxAge := p.KeepLast == 0 && p.KeepHourly == 0 && p.KeepDaily == 0 && p.KeepWeekly == 0 &&
		p.KeepMonthly == 0 && p.KeepYearly == 0 && p.KeepWithinDays == 0
	var maxAgeCut time.Time
	if p.MaxAgeDays > 0 {
		maxAgeCut = now.Add(-time.Duration(p.MaxAgeDays) * 24 * time.Hour)
		for _, r := range good {
			if r.CreatedAt.Before(maxAgeCut) {
				delete(reasons, r.ID)
			} else if onlyMaxAge {
				add(r.ID, fmt.Sprintf("younger than max age %dd", p.MaxAgeDays))
			}
		}
	}
	// Safety net: never expire the newest good full backup (the newest full of any check status when no
	// good full exists).
	newestFull := func(rs []Record) (Record, bool) {
		for _, r := range rs {
			if r.KindOrFull() == KindFull {
				return r, true
			}
		}
		return Record{}, false
	}
	nf, ok := newestFull(good)
	if !ok {
		nf, ok = newestFull(points)
	}
	if ok && len(reasons[nf.ID]) == 0 {
		add(nf.ID, "newest full backup (always kept)")
	}
	// Failed-check restore points never displace a good one: kept while newer than the oldest kept good
	// restore point, expired after.
	var oldestGood *time.Time
	for _, r := range good {
		if len(reasons[r.ID]) > 0 && (oldestGood == nil || r.CreatedAt.Before(*oldestGood)) {
			t := r.CreatedAt
			oldestGood = &t
		}
	}
	for _, r := range bad {
		if len(reasons[r.ID]) > 0 || oldestGood == nil || !r.CreatedAt.After(*oldestGood) {
			continue
		}
		if p.MaxAgeDays > 0 && r.CreatedAt.Before(maxAgeCut) {
			continue
		}
		add(r.ID, "check failed; newer than the oldest kept verified restore point")
	}
	parentOf := func(id string) (Record, bool) {
		if pr, ok := byID[id]; ok {
			return pr, true
		}
		if rid, ok := replicaOf[id]; ok {
			return byID[rid], true
		}
		return Record{}, false
	}
	// Chains: a kept incremental keeps every ancestor.
	for _, r := range points {
		if len(reasons[r.ID]) == 0 {
			continue
		}
		for parent, guard := r.ParentID, 0; parent != "" && guard < 10000; guard++ {
			pr, ok := parentOf(parent)
			if !ok {
				break
			}
			if !containsPrefix(reasons[pr.ID], "parent of ") {
				add(pr.ID, "parent of "+r.ID)
			}
			parent = pr.ParentID
		}
	}
	// Logs: needed from the oldest kept base onward.
	var oldestKept *time.Time
	for _, r := range points {
		if len(reasons[r.ID]) > 0 {
			t := r.consistentTime()
			if oldestKept == nil || t.Before(*oldestKept) {
				oldestKept = &t
			}
		}
	}
	for _, r := range recs {
		if r.KindOrFull() != KindLog {
			continue
		}
		end := r.CreatedAt
		if r.EndTime != nil {
			end = *r.EndTime
		}
		if oldestKept != nil && !end.Before(*oldestKept) {
			add(r.ID, "log needed for point-in-time recovery of kept backups")
		}
	}
	out := make([]Decision, 0, len(recs))
	for _, r := range recs {
		why := reasons[r.ID]
		keep := len(why) > 0
		blocked := false
		if !keep {
			if r.LegalHold {
				keep, blocked, why = true, true, []string{"legal hold"}
			} else if r.RetainUntil != nil && r.RetainUntil.After(now) {
				keep, blocked, why = true, true, []string{"object lock until " + r.RetainUntil.UTC().Format(time.RFC3339)}
			} else {
				why = []string{"not selected by any keep rule"}
				if r.KindOrFull() != KindLog && r.CheckStatus == CheckFailed && len(bad) > 0 {
					why = []string{"check failed; older than every kept verified restore point"}
				}
				if p.MaxAgeDays > 0 && r.CreatedAt.Before(maxAgeCut) {
					why = []string{fmt.Sprintf("older than max age %dd", p.MaxAgeDays)}
				}
			}
		}
		out = append(out, decision(r, keep, why, blocked))
	}
	return out
}

func decision(r Record, keep bool, why []string, blocked bool) Decision {
	return Decision{ID: r.ID, Kind: r.KindOrFull(), Created: r.CreatedAt.UTC().Format(time.RFC3339), Keep: keep, Reasons: why, Blocked: blocked}
}

func containsPrefix(list []string, prefix string) bool {
	for _, s := range list {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

// consistentTime is the time the backup's data is consistent to (its recovery point).
func (r Record) consistentTime() time.Time {
	if r.StartTime != nil {
		return *r.StartTime
	}
	return r.CreatedAt
}

// RetentionGroup is the key retention policies are evaluated over: one managed job, else one source in
// one repository.
func RetentionGroup(r Record) string {
	if r.JobID != "" {
		return "job:" + r.JobID
	}
	return "src:" + r.Repository + "|" + r.Bucket + "|" + r.Source
}
