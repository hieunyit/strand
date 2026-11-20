package backup

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func rec(id string, at time.Time, kind, parent string) Record {
	return Record{ID: id, Status: StatusComplete, CreatedAt: at, Kind: kind, ParentID: parent}
}

func kept(ds []Decision) []string {
	var out []string
	for _, d := range ds {
		if d.Keep {
			out = append(out, d.ID)
		}
	}
	sort.Strings(out)
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 90 daily backups at 01:00 UTC, fake clock at day 90 12:00.
func dailySeries(now time.Time, n int) []Record {
	var rs []Record
	for i := 0; i < n; i++ {
		at := time.Date(now.Year(), now.Month(), now.Day(), 1, 0, 0, 0, time.UTC).AddDate(0, 0, -i)
		rs = append(rs, rec(fmt.Sprintf("d%03d", i), at, KindFull, ""))
	}
	return rs
}

func TestRetentionGFS(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC) // Friday
	rs := dailySeries(now, 90)
	ds := PlanRetention(rs, RetentionPolicy{KeepDaily: 7, KeepWeekly: 4, KeepMonthly: 3}, now)
	got := kept(ds)
	// 7 daily (d000-d006), weekly = newest of each ISO week: d000 (W47), d005 (Sun Nov 15, W46), d012, d019;
	// monthly = newest of Nov (d000), Oct (d020 = Oct 31), Sep (d051 = Sep 30).
	want := []string{"d000", "d001", "d002", "d003", "d004", "d005", "d006", "d012", "d019", "d020", "d051"}
	if !eq(got, want) {
		t.Fatalf("kept %v\nwant %v", got, want)
	}
	for _, d := range ds {
		if d.ID == "d051" && (len(d.Reasons) != 1 || d.Reasons[0] != "monthly 2026-09") {
			t.Fatalf("reasons %v", d.Reasons)
		}
		if d.ID == "d080" && (d.Keep || d.Reasons[0] != "not selected by any keep rule") {
			t.Fatalf("d080 = %+v", d)
		}
	}
	// keep-last and keep-within are independent and union.
	got = kept(PlanRetention(rs, RetentionPolicy{KeepLast: 2, KeepWithinDays: 3}, now))
	if !eq(got, []string{"d000", "d001", "d002"}) {
		t.Fatalf("last+within = %v", got)
	}
	// Zero policy keeps everything.
	if got := kept(PlanRetention(rs, RetentionPolicy{}, now)); len(got) != 90 {
		t.Fatalf("zero policy kept %d", len(got))
	}
}

func TestRetentionMaxAgeHoldLockAndSafety(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	rs := dailySeries(now, 40)
	rs[30].LegalHold = true                                    // d030
	until := now.Add(24 * time.Hour)                           //
	rs[35].RetainUntil, rs[35].LockMode = &until, "COMPLIANCE" // d035
	ds := PlanRetention(rs, RetentionPolicy{KeepMonthly: 12, MaxAgeDays: 14}, now)
	got := kept(ds)
	// monthly keeps d000 (Nov) and d020 (Oct 31) but d020 is older than max age: dropped. Hold/lock block.
	if !eq(got, []string{"d000", "d030", "d035"}) {
		t.Fatalf("kept %v", got)
	}
	for _, d := range ds {
		switch d.ID {
		case "d030":
			if !d.Blocked || d.Reasons[0] != "legal hold" {
				t.Fatalf("hold %+v", d)
			}
		case "d035":
			if !d.Blocked {
				t.Fatalf("lock %+v", d)
			}
		case "d020":
			if d.Keep || d.Reasons[0] != "older than max age 14d" {
				t.Fatalf("max age %+v", d)
			}
		}
	}
	// Max age alone keeps everything younger.
	got = kept(PlanRetention(rs[:20], RetentionPolicy{MaxAgeDays: 5}, now))
	if !eq(got, []string{"d000", "d001", "d002", "d003", "d004"}) {
		t.Fatalf("max age only = %v", got)
	}
	// Safety: a policy that would select nothing keeps the newest full backup.
	old := []Record{rec("a", now.AddDate(0, 0, -100), KindFull, ""), rec("b", now.AddDate(0, 0, -90), KindFull, "")}
	if got := kept(PlanRetention(old, RetentionPolicy{MaxAgeDays: 30}, now)); !eq(got, []string{"b"}) {
		t.Fatalf("safety = %v", got)
	}
	// Non-complete records are not part of the plan.
	failed := rec("f", now, KindFull, "")
	failed.Status = StatusFailed
	if ds := PlanRetention([]Record{failed}, RetentionPolicy{KeepLast: 1}, now); len(ds) != 0 {
		t.Fatalf("failed record planned: %+v", ds)
	}
}

func TestRetentionChainsAndLogs(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	h := func(n int) time.Time { return now.Add(-time.Duration(n) * time.Hour) }
	rs := []Record{
		rec("full1", h(100), KindFull, ""),
		rec("inc1a", h(90), KindIncremental, "full1"),
		rec("inc1b", h(80), KindIncremental, "inc1a"),
		rec("full2", h(50), KindFull, ""),
		rec("inc2a", h(40), KindIncremental, "full2"),
		rec("inc2b", h(30), KindIncremental, "inc2a"),
		rec("log1", h(99), KindLog, ""),
		rec("log2", h(60), KindLog, ""),
		rec("log3", h(45), KindLog, ""),
		rec("log4", h(10), KindLog, ""),
	}
	end := h(49)
	rs[8].EndTime = &end // log3 ends after full2 started: needed
	got := kept(PlanRetention(rs, RetentionPolicy{KeepLast: 1}, now))
	// keep-last 1 = inc2b; its chain keeps inc2a and full2; logs from full2 onward.
	if !eq(got, []string{"full2", "inc2a", "inc2b", "log3", "log4"}) {
		t.Fatalf("chain = %v", got)
	}
	got = kept(PlanRetention(rs, RetentionPolicy{KeepLast: 4}, now))
	// inc1b is the 4th newest restore point: its whole chain stays, and every log after full1.
	if !eq(got, []string{"full1", "full2", "inc1a", "inc1b", "inc2a", "inc2b", "log1", "log2", "log3", "log4"}) {
		t.Fatalf("chain 4 = %v", got)
	}
}
