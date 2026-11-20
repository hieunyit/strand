package backup

import (
	"errors"
	"testing"
	"time"
)

func at(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestScheduleDailyTimezone(t *testing.T) {
	s := Schedule{Kind: ScheduleDaily, At: "01:00", Timezone: "Asia/Ho_Chi_Minh"} // UTC+7
	got, err := s.NextN(at(t, "2026-03-10T12:00:00Z"), 5)
	if err != nil {
		t.Fatal(err)
	}
	// 01:00 +07 = 18:00Z of the previous day; first after 12:00Z on the 10th is 18:00Z on the 10th.
	for i, g := range got {
		want := at(t, "2026-03-10T18:00:00Z").AddDate(0, 0, i)
		if !g.Equal(want) {
			t.Fatalf("run %d = %s, want %s", i, g, want)
		}
	}
	// Strictly after: exactly at the run time yields the next day.
	n, _ := s.Next(at(t, "2026-03-10T18:00:00Z"), time.Time{})
	if !n.Equal(at(t, "2026-03-11T18:00:00Z")) {
		t.Fatalf("strictly-after = %s", n)
	}
}

func TestScheduleWeeklyMonthly(t *testing.T) {
	w := Schedule{Kind: ScheduleWeekly, At: "02:00", Days: []int{0}, Timezone: "UTC"}
	n, _ := w.Next(at(t, "2026-03-10T00:00:00Z"), time.Time{}) // Tuesday
	if !n.Equal(at(t, "2026-03-15T02:00:00Z")) || n.Weekday() != time.Sunday {
		t.Fatalf("weekly = %s", n)
	}
	m := Schedule{Kind: ScheduleMonthly, At: "03:30", Days: []int{31}}
	got, err := m.NextN(at(t, "2026-01-31T04:00:00Z"), 3)
	if err != nil {
		t.Fatal(err)
	}
	// day 31 clamps to the last day of short months
	want := []string{"2026-02-28T03:30:00Z", "2026-03-31T03:30:00Z", "2026-04-30T03:30:00Z"}
	for i := range want {
		if !got[i].Equal(at(t, want[i])) {
			t.Fatalf("monthly %d = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestScheduleInterval(t *testing.T) {
	s := Schedule{Kind: ScheduleInterval, EverySeconds: 3600}
	n, err := s.Next(at(t, "2026-01-01T00:10:00Z"), at(t, "2026-01-01T00:00:00Z"))
	if err != nil || !n.Equal(at(t, "2026-01-01T01:00:00Z")) {
		t.Fatalf("interval = %s %v", n, err)
	}
	if _, err := (Schedule{Kind: ScheduleInterval}).Next(time.Now(), time.Time{}); err == nil {
		t.Fatal("interval without every_seconds accepted")
	}
}

func TestCronParseAndNext(t *testing.T) {
	cases := []struct{ expr, after, want string }{
		{"*/15 * * * *", "2026-03-10T10:07:00Z", "2026-03-10T10:15:00Z"},
		{"0 1 * * *", "2026-03-10T10:07:00Z", "2026-03-11T01:00:00Z"},
		{"30 2 * * mon-fri", "2026-03-13T03:00:00Z", "2026-03-16T02:30:00Z"}, // Fri -> Mon
		{"0 0 1 jan *", "2026-03-10T00:00:00Z", "2027-01-01T00:00:00Z"},
		{"0 12 * * 7", "2026-03-10T00:00:00Z", "2026-03-15T12:00:00Z"}, // 7 = Sunday
		{"0,30 9-10 * * *", "2026-03-10T09:30:00Z", "2026-03-10T10:00:00Z"},
		{"5/20 0 * * *", "2026-03-10T00:00:00Z", "2026-03-10T00:05:00Z"},
		// dom and dow both restricted: either matches (Vixie)
		{"0 0 13 * fri", "2026-03-10T00:00:00Z", "2026-03-13T00:00:00Z"},
	}
	for _, c := range cases {
		cr, err := ParseCron(c.expr)
		if err != nil {
			t.Fatalf("%q: %v", c.expr, err)
		}
		got, err := cr.Next(at(t, c.after), time.UTC)
		if err != nil || !got.Equal(at(t, c.want)) {
			t.Fatalf("%q after %s = %s (%v), want %s", c.expr, c.after, got, err, c.want)
		}
	}
	for _, bad := range []string{"", "* * * *", "* * * * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "*/0 * * * *", "5-1 * * * *",
		"a * * * *", "-1 * * * *", "@daily", "* * * * 8"} {
		if _, err := ParseCron(bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%q accepted (err=%v)", bad, err)
		}
	}
	// impossible date never fires but terminates
	cr, _ := ParseCron("0 0 31 2 *")
	if _, err := cr.Next(at(t, "2026-01-01T00:00:00Z"), time.UTC); err == nil {
		t.Fatal("Feb 31 fired")
	}
}

func TestCronTimezoneDST(t *testing.T) {
	// America/New_York: 2026-03-08 02:30 does not exist; the run lands after the gap, never before `after`.
	s := Schedule{Kind: ScheduleDaily, At: "02:30", Timezone: "America/New_York"}
	got, err := s.NextN(at(t, "2026-03-07T12:00:00Z"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if !got[0].Equal(at(t, "2026-03-08T06:30:00Z")) {
		t.Fatalf("gap day = %s", got[0])
	}
	for i := 1; i < len(got); i++ {
		if !got[i].After(got[i-1]) {
			t.Fatalf("not increasing: %v", got)
		}
	}
}

func TestScheduleValidate(t *testing.T) {
	bad := []Schedule{
		{Kind: "hourly"}, {Kind: ScheduleDaily, At: "25:00"}, {Kind: ScheduleDaily, At: "1:00"},
		{Kind: ScheduleDaily, At: "01:00", Timezone: "Mars/Base"}, {Kind: ScheduleWeekly, At: "01:00"},
		{Kind: ScheduleWeekly, At: "01:00", Days: []int{7}}, {Kind: ScheduleMonthly, At: "01:00", Days: []int{0}},
		{Kind: ScheduleCron, Cron: "x"}, {Kind: ScheduleInterval, EverySeconds: 10}, {Kind: ScheduleDaily, At: "01:00", Days: []int{1}},
	}
	for _, s := range bad {
		if err := s.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%+v accepted (%v)", s, err)
		}
	}
	good := []Schedule{{Kind: ScheduleInterval, EverySeconds: 60}, {Kind: ScheduleDaily, At: "01:00", Timezone: "Asia/Ho_Chi_Minh"},
		{Kind: ScheduleWeekly, At: "02:00", Days: []int{0, 6}}, {Kind: ScheduleMonthly, At: "00:00", Days: []int{1, 15}}, {Kind: ScheduleCron, Cron: "0 1 * * *"}}
	for _, s := range good {
		if err := s.Validate(); err != nil {
			t.Fatalf("%+v: %v", s, err)
		}
	}
}
