package mongocdc

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Production log: a line every 10 s per pipeline (also when nothing happened) and events_per_sec as the
// average since the stream opened (7 -> 2 over 18 minutes at a steady load). Lines now come every minute
// while events arrive, every 10 minutes when idle, and events_per_sec is the rate since the last line.
func TestStreamProgressLogInterval(t *testing.T) {
	var buf bytes.Buffer
	r := &runner{cfg: Config{Pipeline: "p", Logger: slog.New(slog.NewTextHandler(&buf, nil))}}
	start := time.Now().Add(-30 * time.Minute)
	st := &streamStats{start: start, lastLog: time.Now().Add(-15 * time.Second), events: 100}
	r.logStreamProgress(st, false)
	if buf.Len() != 0 {
		t.Fatalf("logged 15 s after the previous line: %q", buf.String())
	}
	st.lastLog = time.Now().Add(-2 * time.Minute)
	r.logStreamProgress(st, false)
	line := buf.String()
	if !strings.Contains(line, "events=100") || !strings.Contains(line, "events_since_last=100") || !strings.Contains(line, "events_per_sec=0.8") {
		t.Fatalf("rate over the last interval expected (100 events / 120 s = 0.8): %q", line)
	}
	// No new event: no line after a minute, a heartbeat after 10 minutes.
	buf.Reset()
	st.lastLog = time.Now().Add(-5 * time.Minute)
	r.logStreamProgress(st, false)
	if buf.Len() != 0 {
		t.Fatalf("idle stream logged after 5 min: %q", buf.String())
	}
	st.lastLog = time.Now().Add(-11 * time.Minute)
	r.logStreamProgress(st, false)
	if !strings.Contains(buf.String(), "events_per_sec=0 ") || !strings.Contains(buf.String(), "events_since_last=0") {
		t.Fatalf("idle heartbeat expected: %q", buf.String())
	}
}
