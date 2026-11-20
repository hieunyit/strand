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
// repository) and returns a decision per record, newest first. Guarantees:
//   - the newest complete full backup is always kept (a policy can never leave zero restore points);
//   - an incremental that is kept keeps its whole parent chain;
//   - log segments (binlog/WAL/oplog) are kept from the oldest kept base backup onward so every kept base
//     stays point-in-time recoverable; older segments expire;
//   - legal hold and S3 Object Lock retention (RetainUntil in the future) block expiry.
func PlanRetention(records []Record, p RetentionPolicy, now time.Time) []Decision {
	recs := make([]Record, 0, len(records))
	for _, r := range records {
		if r.Status == StatusComplete {
			recs = append(recs, r)
		}
	}
	sort.SliceStable(recs, func(i, j int) bool {
		if !recs[i].CreatedAt.Equal(recs[j].CreatedAt) {
			return recs[i].CreatedAt.After(recs[j].CreatedAt)
		}
		return recs[i].ID > recs[j].ID
	})
	reasons := map[string][]string{}
	add := func(id, why string) { reasons[id] = append(reasons[id], why) }
	byID := map[string]Record{}
	var points []Record
	for _, r := range recs {
		byID[r.ID] = r
		if r.KindOrFull() != KindLog {
			points = append(points, r)
		}
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
		for i, r := range points {
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
		for _, r := range points {
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
		for _, r := range points {
			if !r.CreatedAt.Before(cut) {
				add(r.ID, fmt.Sprintf("within %dd", p.KeepWithinDays))
			}
		}
	}
	onlyMaxAge := p.KeepLast == 0 && p.KeepHourly == 0 && p.KeepDaily == 0 && p.KeepWeekly == 0 &&
		p.KeepMonthly == 0 && p.KeepYearly == 0 && p.KeepWithinDays == 0
	if p.MaxAgeDays > 0 {
		cut := now.Add(-time.Duration(p.MaxAgeDays) * 24 * time.Hour)
		for _, r := range points {
			if r.CreatedAt.Before(cut) {
				delete(reasons, r.ID)
			} else if onlyMaxAge {
				add(r.ID, fmt.Sprintf("younger than max age %dd", p.MaxAgeDays))
			}
		}
	}
	// Safety net: never expire the newest full backup.
	for _, r := range points {
		if r.KindOrFull() == KindFull {
			if len(reasons[r.ID]) == 0 {
				add(r.ID, "newest full backup (always kept)")
			}
			break
		}
	}
	// Chains: a kept incremental keeps every ancestor.
	for _, r := range points {
		if len(reasons[r.ID]) == 0 {
			continue
		}
		for parent, guard := r.ParentID, 0; parent != "" && guard < 10000; guard++ {
			pr, ok := byID[parent]
			if !ok {
				break
			}
			if !containsPrefix(reasons[parent], "parent of ") {
				add(parent, "parent of "+r.ID)
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
				if p.MaxAgeDays > 0 && r.CreatedAt.Before(now.Add(-time.Duration(p.MaxAgeDays)*24*time.Hour)) {
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
