package web

import "strings"

// The single state -> tone map of the console (SPEC 3.1 "stateClass"). Every badge, dot, status strip and
// reason line takes its colour from the tone returned here; templates call stateClass, Go code calls
// stateTone. app.css carries one rule per tone listing the same keys (TestStateMapMatchesCSS keeps the two
// in step), so a status never gets two colours on two pages.
const (
	ToneSuccess = "success"
	ToneInfo    = "info"
	ToneWarning = "warning"
	ToneDanger  = "danger"
	ToneNeutral = "neutral"
)

// stateTones: lower-case state -> tone. Pipeline states, process-job states, backup run/job states and the
// per-table snapshot states (tbl-*) all live here.
var stateTones = map[string]string{
	// success
	"success": ToneSuccess, "ok": ToneSuccess, "complete": ToneSuccess, "completed": ToneSuccess,
	"synced": ToneSuccess, "done": ToneSuccess, "healthy": ToneSuccess, "verified": ToneSuccess, "passed": ToneSuccess,
	"tbl-done": ToneSuccess,
	// info
	"running": ToneInfo, "pending": ToneInfo, "starting": ToneInfo, "bootstrapping": ToneInfo, "queued": ToneInfo,
	"info": ToneInfo, "tbl-running": ToneInfo,
	// warning
	"warning": ToneWarning, "warn": ToneWarning, "stale": ToneWarning, "partial": ToneWarning, "missed": ToneWarning,
	"cancelling": ToneWarning, "interrupted": ToneWarning,
	// danger
	"failed": ToneDanger, "error": ToneDanger, "rpo_breach": ToneDanger, "unreachable": ToneDanger, "danger": ToneDanger,
	// neutral
	"paused": ToneNeutral, "disabled": ToneNeutral, "expired": ToneNeutral, "stopped": ToneNeutral,
	"unknown": ToneNeutral, "cancelled": ToneNeutral, "neutral": ToneNeutral, "tbl-pending": ToneNeutral,
}

func stateKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.NewReplacer(" ", "-").Replace(s)
}

// stateTone returns success | info | warning | danger | neutral (neutral for anything unknown).
func stateTone(s string) string {
	if t, ok := stateTones[stateKey(s)]; ok {
		return t
	}
	return ToneNeutral
}

// stateClass is the CSS class of a state ("st-synced", "st-failed", ...). Unknown states fall back to
// "st-neutral". The text of a badge is always the state itself, never colour alone (WCAG 1.4.1).
func stateClass(s string) string {
	k := stateKey(s)
	if _, ok := stateTones[k]; ok {
		return "st-" + k
	}
	return "st-neutral"
}
