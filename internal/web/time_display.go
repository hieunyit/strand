package web

import "time"

// Web timestamps and calendar boundaries use UTC+7 regardless of the server or browser timezone.
var webTimeZone = time.FixedZone("UTC+7", 7*60*60)

func formatWebTime(t time.Time, layout string) string {
	if t.IsZero() {
		return "-"
	}
	return t.In(webTimeZone).Format(layout)
}

func webDay(t time.Time) time.Time {
	y, m, d := t.In(webTimeZone).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, webTimeZone)
}

func formatWebLogTime(raw string) string {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return raw
	}
	return formatWebTime(t, "2006-01-02 15:04:05 MST")
}
