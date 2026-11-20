package backup

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // zones resolve even on images without /usr/share/zoneinfo
)

// Schedule kinds.
const (
	ScheduleInterval = "interval"
	ScheduleDaily    = "daily"
	ScheduleWeekly   = "weekly"
	ScheduleMonthly  = "monthly"
	ScheduleCron     = "cron"
)

// Schedule says when a job runs. Interval uses EverySeconds (the job's legacy every_seconds); daily,
// weekly and monthly use At ("HH:MM") in Timezone; weekly Days are weekdays 0-6 (0 = Sunday), monthly Days
// are days of the month 1-31 (a day past the end of a short month runs on the month's last day); cron is a
// standard 5-field expression (minute hour day-of-month month day-of-week) evaluated in Timezone.
type Schedule struct {
	Kind         string `json:"kind"`
	EverySeconds int    `json:"every_seconds,omitempty"`
	At           string `json:"at,omitempty"`
	Days         []int  `json:"days,omitempty"`
	Cron         string `json:"cron,omitempty"`
	Timezone     string `json:"timezone,omitempty"`
}

// maxScheduleSearchDays bounds the look-ahead of Next (a cron such as "0 0 31 2 *" never matches).
const maxScheduleSearchDays = 366 * 8

// Location returns the schedule's time zone (UTC when empty).
func (s Schedule) Location() (*time.Location, error) {
	if s.Timezone == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return nil, fmt.Errorf("%w: unknown timezone %q", ErrInvalid, s.Timezone)
	}
	return loc, nil
}

func parseClock(at string) (h, m int, err error) {
	parts := strings.Split(at, ":")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		return 0, 0, fmt.Errorf("%w: schedule time must be HH:MM", ErrInvalid)
	}
	h, e1 := strconv.Atoi(parts[0])
	m, e2 := strconv.Atoi(parts[1])
	if e1 != nil || e2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, 0, fmt.Errorf("%w: schedule time must be HH:MM between 00:00 and 23:59", ErrInvalid)
	}
	return h, m, nil
}

// Validate checks the schedule. For interval, everyFallback (the job's every_seconds) is used when the
// schedule carries none.
func (s Schedule) Validate() error {
	bad := func(m string) error { return fmt.Errorf("%w: schedule %s", ErrInvalid, m) }
	if _, err := s.Location(); err != nil {
		return err
	}
	switch s.Kind {
	case ScheduleInterval:
		if s.EverySeconds != 0 && (s.EverySeconds < 60 || s.EverySeconds > 365*24*3600) {
			return bad("interval must be from 60 seconds to 365 days")
		}
	case ScheduleDaily, ScheduleWeekly, ScheduleMonthly:
		if _, _, err := parseClock(s.At); err != nil {
			return err
		}
		if s.Kind == ScheduleDaily && len(s.Days) > 0 {
			return bad("daily takes no days")
		}
		if s.Kind == ScheduleWeekly {
			if len(s.Days) == 0 {
				return bad("weekly needs at least one weekday (0-6, 0 = Sunday)")
			}
			for _, d := range s.Days {
				if d < 0 || d > 6 {
					return bad("weekly days must be 0-6 (0 = Sunday)")
				}
			}
		}
		if s.Kind == ScheduleMonthly {
			if len(s.Days) == 0 {
				return bad("monthly needs at least one day of the month (1-31)")
			}
			for _, d := range s.Days {
				if d < 1 || d > 31 {
					return bad("monthly days must be 1-31")
				}
			}
		}
		if len(s.Days) > 31 {
			return bad("lists too many days")
		}
	case ScheduleCron:
		if _, err := ParseCron(s.Cron); err != nil {
			return err
		}
	default:
		return bad("kind must be interval, daily, weekly, monthly or cron")
	}
	return nil
}

// Next returns the first run strictly after `after`. last is the previous run's start (interval kind:
// next = last + every; zero last = after + every).
func (s Schedule) Next(after, last time.Time) (time.Time, error) {
	if err := s.Validate(); err != nil {
		return time.Time{}, err
	}
	loc, _ := s.Location()
	switch s.Kind {
	case ScheduleInterval:
		every := time.Duration(s.EverySeconds) * time.Second
		if every <= 0 {
			return time.Time{}, fmt.Errorf("%w: schedule interval needs every_seconds", ErrInvalid)
		}
		base := last
		if base.IsZero() {
			base = after
		}
		n := base.Add(every)
		if !n.After(after) {
			n = after.Add(every)
		}
		return n.UTC(), nil
	case ScheduleCron:
		c, _ := ParseCron(s.Cron)
		return c.Next(after, loc)
	}
	h, m, _ := parseClock(s.At)
	days := map[int]bool{}
	for _, d := range s.Days {
		days[d] = true
	}
	start := after.In(loc)
	for i := 0; i < maxScheduleSearchDays; i++ {
		d := time.Date(start.Year(), start.Month(), start.Day()+i, 0, 0, 0, 0, loc)
		ok := true
		switch s.Kind {
		case ScheduleWeekly:
			ok = days[int(d.Weekday())]
		case ScheduleMonthly:
			last := daysIn(d.Year(), d.Month())
			ok = days[d.Day()] || (d.Day() == last && hasAbove(s.Days, last))
		}
		if !ok {
			continue
		}
		cand := time.Date(d.Year(), d.Month(), d.Day(), h, m, 0, 0, loc)
		if cand.After(after) {
			return cand.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("%w: schedule never fires", ErrInvalid)
}

// NextN returns the next n runs after `after`.
func (s Schedule) NextN(after time.Time, n int) ([]time.Time, error) {
	out := make([]time.Time, 0, n)
	cur := after
	for len(out) < n {
		nx, err := s.Next(cur, cur)
		if err != nil {
			return nil, err
		}
		out = append(out, nx)
		cur = nx
	}
	return out, nil
}

func daysIn(y int, m time.Month) int { return time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day() }

func hasAbove(days []int, n int) bool {
	for _, d := range days {
		if d > n {
			return true
		}
	}
	return false
}

// Human describes the schedule for UIs ("Daily 01:00 Asia/Ho_Chi_Minh").
func (s Schedule) Human() string {
	tz := s.Timezone
	if tz == "" {
		tz = "UTC"
	}
	switch s.Kind {
	case ScheduleInterval:
		return "Every " + (time.Duration(s.EverySeconds) * time.Second).String()
	case ScheduleDaily:
		return "Daily " + s.At + " " + tz
	case ScheduleWeekly:
		names := []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
		d := append([]int(nil), s.Days...)
		sort.Ints(d)
		var p []string
		for _, x := range d {
			if x >= 0 && x <= 6 {
				p = append(p, names[x])
			}
		}
		return "Weekly " + strings.Join(p, ",") + " " + s.At + " " + tz
	case ScheduleMonthly:
		d := append([]int(nil), s.Days...)
		sort.Ints(d)
		var p []string
		for _, x := range d {
			p = append(p, strconv.Itoa(x))
		}
		return "Monthly day " + strings.Join(p, ",") + " " + s.At + " " + tz
	case ScheduleCron:
		return "Cron " + s.Cron + " " + tz
	}
	return s.Kind
}

// ---- cron ----

// Cron is a parsed 5-field expression. Day-of-month and day-of-week combine like Vixie cron: when both are
// restricted a day matches if EITHER matches; when one is "*" only the other applies.
type Cron struct {
	min, hour, dom, month, dow uint64
	domStar, dowStar           bool
}

var (
	monthNames = map[string]int{"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
	dowNames   = map[string]int{"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}
)

// ParseCron parses "m h dom mon dow": lists (a,b), ranges (a-b), steps (*/n, a-b/n, a/n), month and weekday
// names, weekday 7 = Sunday. No @macros, no seconds, no L/W/#.
func ParseCron(expr string) (Cron, error) {
	f := strings.Fields(expr)
	bad := func(m string) (Cron, error) { return Cron{}, fmt.Errorf("%w: cron %s", ErrInvalid, m) }
	if len(f) != 5 {
		return bad("must have 5 fields: minute hour day-of-month month day-of-week")
	}
	var c Cron
	var err error
	if c.min, _, err = cronField(f[0], 0, 59, nil); err != nil {
		return bad("minute: " + err.Error())
	}
	if c.hour, _, err = cronField(f[1], 0, 23, nil); err != nil {
		return bad("hour: " + err.Error())
	}
	if c.dom, c.domStar, err = cronField(f[2], 1, 31, nil); err != nil {
		return bad("day-of-month: " + err.Error())
	}
	if c.month, _, err = cronField(f[3], 1, 12, monthNames); err != nil {
		return bad("month: " + err.Error())
	}
	if c.dow, c.dowStar, err = cronField(f[4], 0, 7, dowNames); err != nil {
		return bad("day-of-week: " + err.Error())
	}
	if c.dow&(1<<7) != 0 { // 7 = Sunday
		c.dow = c.dow&^(1<<7) | 1
	}
	return c, nil
}

func cronField(s string, lo, hi int, names map[string]int) (mask uint64, star bool, err error) {
	if s == "" {
		return 0, false, fmt.Errorf("empty")
	}
	star = s == "*"
	num := func(t string) (int, error) {
		if v, ok := names[strings.ToLower(t)]; ok {
			return v, nil
		}
		v, err := strconv.Atoi(t)
		if err != nil || v < lo || v > hi || strings.HasPrefix(t, "+") || strings.HasPrefix(t, "-") {
			return 0, fmt.Errorf("%q out of range %d-%d", t, lo, hi)
		}
		return v, nil
	}
	for _, part := range strings.Split(s, ",") {
		rng, stepS, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			step, err = strconv.Atoi(stepS)
			if err != nil || step < 1 || step > hi {
				return 0, false, fmt.Errorf("bad step %q", stepS)
			}
		}
		a, b := lo, hi
		if rng != "*" {
			from, to, isRange := strings.Cut(rng, "-")
			if a, err = num(from); err != nil {
				return 0, false, err
			}
			b = a
			if isRange {
				if b, err = num(to); err != nil {
					return 0, false, err
				}
			} else if hasStep {
				b = hi // "a/n" = a-hi/n
			}
			if a > b {
				return 0, false, fmt.Errorf("range %q is reversed", rng)
			}
		}
		for v := a; v <= b; v += step {
			mask |= 1 << uint(v)
		}
	}
	return mask, star, nil
}

func (c Cron) dayMatches(t time.Time) bool {
	if c.month&(1<<uint(t.Month())) == 0 {
		return false
	}
	dom := c.dom&(1<<uint(t.Day())) != 0
	dow := c.dow&(1<<uint(t.Weekday())) != 0
	switch {
	case c.domStar && c.dowStar:
		return true
	case c.domStar:
		return dow
	case c.dowStar:
		return dom
	}
	return dom || dow
}

// Next returns the first matching minute strictly after `after`, in loc.
func (c Cron) Next(after time.Time, loc *time.Location) (time.Time, error) {
	start := after.In(loc)
	for i := 0; i < maxScheduleSearchDays; i++ {
		d := time.Date(start.Year(), start.Month(), start.Day()+i, 0, 0, 0, 0, loc)
		if !c.dayMatches(d) {
			continue
		}
		for h := 0; h < 24; h++ {
			if c.hour&(1<<uint(h)) == 0 {
				continue
			}
			for m := 0; m < 60; m++ {
				if c.min&(1<<uint(m)) == 0 {
					continue
				}
				cand := time.Date(d.Year(), d.Month(), d.Day(), h, m, 0, 0, loc)
				if cand.After(after) {
					return cand.UTC(), nil
				}
			}
		}
	}
	return time.Time{}, fmt.Errorf("%w: cron expression never fires", ErrInvalid)
}
