package backup

import (
	"testing"
	"time"
)

func tp(t time.Time) *time.Time { return &t }

func TestComputeRPO(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		in     RPOInput
		secs   int64
		basis  string
		breach bool
	}{
		{"non-PITR uses the last backup", RPOInput{Source: "s", LastBackup: tp(now.Add(-3 * time.Hour)), TargetMinutes: 1440}, 3 * 3600, RPOBasisBackup, false},
		{"non-PITR ignores log segments", RPOInput{Source: "s", LastBackup: tp(now.Add(-3 * time.Hour)), LastLogEnd: tp(now.Add(-time.Minute)), TargetMinutes: 60}, 3 * 3600, RPOBasisBackup, true},
		{"PITR uses the last log segment", RPOInput{Source: "s", PITR: true, LastBackup: tp(now.Add(-20 * time.Hour)), LastLogEnd: tp(now.Add(-2 * time.Minute)), TargetMinutes: 5}, 120, RPOBasisLog, false},
		{"PITR breach when the archive stalls", RPOInput{Source: "s", PITR: true, LastBackup: tp(now.Add(-20 * time.Hour)), LastLogEnd: tp(now.Add(-7 * time.Minute)), TargetMinutes: 5}, 420, RPOBasisLog, true},
		{"PITR without a segment yet falls back to the backup", RPOInput{Source: "s", PITR: true, LastBackup: tp(now.Add(-30 * time.Minute)), TargetMinutes: 5}, 1800, RPOBasisBackup, true},
		{"a newer backup beats an older log segment", RPOInput{Source: "s", PITR: true, LastBackup: tp(now.Add(-time.Minute)), LastLogEnd: tp(now.Add(-time.Hour)), TargetMinutes: 5}, 60, RPOBasisBackup, false},
		{"exactly at the target is not a breach", RPOInput{Source: "s", LastBackup: tp(now.Add(-5 * time.Minute)), TargetMinutes: 5}, 300, RPOBasisBackup, false},
		{"no target never breaches", RPOInput{Source: "s", LastBackup: tp(now.Add(-100 * time.Hour))}, 100 * 3600, RPOBasisBackup, false},
		{"no restore point: no RPO, no breach", RPOInput{Source: "s", PITR: true, TargetMinutes: 5}, -1, RPOBasisNone, false},
		{"a point in the future clamps to zero", RPOInput{Source: "s", LastBackup: tp(now.Add(time.Minute)), TargetMinutes: 5}, 0, RPOBasisBackup, false},
	}
	for _, c := range cases {
		got := ComputeRPO(now, c.in)
		if got.CurrentSecs != c.secs || got.Basis != c.basis || got.Breach != c.breach {
			t.Errorf("%s: got secs=%d basis=%s breach=%v, want %d %s %v", c.name, got.CurrentSecs, got.Basis, got.Breach, c.secs, c.basis, c.breach)
		}
	}
}

func TestNotifyRuleMatches(t *testing.T) {
	r := func(scope, id string) NotifyRule {
		return NotifyRule{ID: "r", Event: EventFailure, ScopeKind: scope, ScopeID: id, ChannelID: "c", Enabled: true}
	}
	if !r(ScopeAll, "").Matches(EventFailure, "j", "p") || r(ScopeAll, "").Matches(EventSuccess, "j", "p") {
		t.Fatal("scope all must match its event only")
	}
	if !r(ScopePolicy, "p1").Matches(EventFailure, "j", "p1") || r(ScopePolicy, "p1").Matches(EventFailure, "j", "p2") || r(ScopePolicy, "p1").Matches(EventFailure, "j", "") {
		t.Fatal("policy scope")
	}
	if !r(ScopeJob, "j1").Matches(EventFailure, "j1", "") || r(ScopeJob, "j1").Matches(EventFailure, "j2", "p") || r(ScopeJob, "j1").Matches(EventFailure, "", "p") {
		t.Fatal("job scope")
	}
	off := r(ScopeAll, "")
	off.Enabled = false
	if off.Matches(EventFailure, "j", "p") {
		t.Fatal("a disabled rule matched")
	}
}

func TestNotifyChannelValidation(t *testing.T) {
	ok := NotifyChannel{ID: "c1", Name: "n", Kind: ChannelSlack, SecretSealed: []byte("x")}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []NotifyChannel{
		{ID: "c1", Name: "n", Kind: ChannelSlack},                                         // no destination
		{ID: "c1", Name: "n", Kind: "sms", SecretSealed: []byte("x")},                     // kind
		{ID: "c1", Name: "n", Kind: ChannelWebhook, SecretSealed: []byte("x"), User: "u"}, // email field on a webhook
		{ID: "c1", Name: "n", Kind: ChannelEmail, SMTPAddr: "smtp:25", From: "a@b.c", To: []string{"not an address"}},
		{ID: "c1", Name: "n", Kind: ChannelEmail, SMTPAddr: "smtp:25", From: "a@b.c\r\nBcc: x@y.z", To: []string{"a@b.c"}},
		{ID: "c1", Name: "n", Kind: ChannelEmail, SMTPAddr: "smtp", From: "a@b.c", To: []string{"a@b.c"}}, // no port
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			t.Errorf("case %d accepted: %+v", i, c)
		}
	}
	if ValidWebhookURL("ftp://x") || ValidWebhookURL("") || !ValidWebhookURL("https://hooks.example.com/x") {
		t.Fatal("ValidWebhookURL")
	}
}
