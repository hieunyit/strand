package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func runBackupRepoStored(t *testing.T, s store.Store) {
	ctx := context.Background()
	box, _ := store.NewBox("0123456789abcdef0123456789abcdef")
	ak, _ := box.Seal([]byte("AKIA-canary"), "stored-r")
	sk, _ := box.Seal([]byte("sk-canary"), "stored-r")
	r := backup.Repo{ID: "stored-r", Name: "Stored", Kind: backup.RepoKindS3, SecretMode: backup.RepoSecretStored, Endpoint: "https://s3.example.com",
		Region: "eu-west-1", Bucket: "stored-bucket", Prefix: "strand", AccessKeySealed: ak, SecretKeySealed: sk,
		LockMode: "COMPLIANCE", LockDays: 30}
	must(t, s.CreateBackupRepo(ctx, r))
	got, ok, err := s.GetBackupRepo(ctx, "stored-r")
	if err != nil || !ok {
		t.Fatalf("get: %v %v", ok, err)
	}
	if got.SecretMode != backup.RepoSecretStored || got.Endpoint != r.Endpoint || got.Region != "eu-west-1" || !got.AccessKeySet || !got.SecretKeySet ||
		!bytes.Equal(got.AccessKeySealed, ak) || !bytes.Equal(got.SecretKeySealed, sk) || got.EnvPrefix != "" || !got.Immutable() || got.KindOrS3() != "s3" {
		t.Fatalf("stored repo = %+v", got)
	}
	if pt, err := box.Open(got.SecretKeySealed, "stored-r"); err != nil || string(pt) != "sk-canary" {
		t.Fatalf("open: %q %v", pt, err)
	}
	if b, _ := json.Marshal(got); strings.Contains(string(b), "ealed") || bytes.Contains(b, sk) || strings.Contains(string(b), "canary") {
		t.Fatalf("repo JSON leaks secrets: %s", b)
	}
	// Validation: stored needs keys and an endpoint, env mode rejects them, only s3 is implemented.
	bad := r
	bad.ID, bad.Name, bad.SecretKeySealed = "bad1", "bad1", nil
	if err := s.CreateBackupRepo(ctx, bad); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("stored without secret key: %v", err)
	}
	bad = r
	bad.ID, bad.Name, bad.Kind = "bad2", "bad2", backup.RepoKindAzure
	if err := s.CreateBackupRepo(ctx, bad); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("azure is not implemented: %v", err)
	}
	bad = r
	bad.ID, bad.Name, bad.Endpoint = "bad3", "bad3", "ftp://x"
	if err := s.CreateBackupRepo(ctx, bad); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("bad endpoint: %v", err)
	}
	envBad := backup.Repo{ID: "bad4", Name: "bad4", EnvPrefix: "STRAND_BACKUP_REPO_X_", Bucket: "bucket-x", Endpoint: "https://x.example.com"}
	if err := s.CreateBackupRepo(ctx, envBad); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("env mode must not carry an endpoint: %v", err)
	}
	// The effective prefix is a valid env prefix and differs per repository.
	if p := got.EffectivePrefix(); !strings.HasPrefix(p, "STRAND_BACKUP_REPO_X") || p == (backup.Repo{ID: "other"}).EffectivePrefix() {
		t.Fatalf("prefix = %q", p)
	}
	// Test result persists across reads and survives an update.
	at := time.Now().UTC().Truncate(time.Millisecond)
	if ok, err := s.SetBackupRepoTest(ctx, "stored-r", at, "ok", "all steps passed", []byte(`[{"name":"list","status":"ok"}]`)); !ok || err != nil {
		t.Fatalf("set test: %v %v", ok, err)
	}
	got.Region = "eu-central-1"
	if ok, err := s.UpdateBackupRepo(ctx, got); !ok || err != nil {
		t.Fatalf("update: %v %v", ok, err)
	}
	got2, _, _ := s.GetBackupRepo(ctx, "stored-r")
	if got2.Region != "eu-central-1" || got2.LastTestStatus != "ok" || got2.LastTestAt == nil || !strings.Contains(string(got2.LastTestSteps), "list") || !bytes.Equal(got2.SecretKeySealed, sk) {
		t.Fatalf("after update = %+v", got2)
	}
	// Switching back to env mode clears the stored fields.
	back := got2
	back.SecretMode, back.EnvPrefix, back.Endpoint, back.Region, back.AccessKeySealed, back.SecretKeySealed = backup.RepoSecretEnv, "STRAND_BACKUP_REPO_STORED_", "", "", nil, nil
	if ok, err := s.UpdateBackupRepo(ctx, back); !ok || err != nil {
		t.Fatalf("switch to env: %v %v", ok, err)
	}
	if g, _, _ := s.GetBackupRepo(ctx, "stored-r"); g.AccessKeySet || g.SecretKeySet || g.SecretMode != backup.RepoSecretEnv {
		t.Fatalf("env repo = %+v", g)
	}
	if ok, err := s.DeleteBackupRepo(ctx, "stored-r"); !ok || err != nil {
		t.Fatalf("delete: %v %v", ok, err)
	}
}

func runBackupNotify(t *testing.T, s store.Store) {
	ctx := context.Background()
	box, _ := store.NewBox("0123456789abcdef0123456789abcdef")
	sealed, _ := box.Seal([]byte("https://hooks.example.com/T000/secret"), "ch-1")
	c := backup.NotifyChannel{ID: "ch-1", Name: "ops-slack", Kind: backup.ChannelSlack, Enabled: true, SecretSealed: sealed, CreatedBy: "admin"}
	got, err := s.CreateNotifyChannel(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if !got.SecretSet || got.Source != "stored" || got.CreatedAt.IsZero() || !bytes.Equal(got.SecretSealed, sealed) {
		t.Fatalf("created = %+v", got)
	}
	if b, _ := json.Marshal(got); strings.Contains(string(b), "ealed") || strings.Contains(string(b), "secret") && !strings.Contains(string(b), `"secret_set":true`) {
		t.Fatalf("channel JSON = %s", b)
	}
	if _, err := s.CreateNotifyChannel(ctx, c); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := s.CreateNotifyChannel(ctx, backup.NotifyChannel{ID: "ch-bad", Name: "x", Kind: backup.ChannelEmail, Enabled: true}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("email without settings: %v", err)
	}
	mail := backup.NotifyChannel{ID: "ch-2", Name: "mail", Kind: backup.ChannelEmail, Enabled: true, SMTPAddr: "smtp.example.com:587",
		From: "strand@example.com", To: []string{"a@example.com", "b@example.com"}}
	m2, err := s.CreateNotifyChannel(ctx, mail)
	if err != nil || len(m2.To) != 2 || m2.SecretSet {
		t.Fatalf("mail = %+v %v", m2, err)
	}
	list, _ := s.ListNotifyChannels(ctx)
	if len(list) != 2 || list[0].Name != "mail" {
		t.Fatalf("list = %+v", list)
	}
	at := time.Now().UTC().Truncate(time.Millisecond)
	if ok, err := s.SetNotifyChannelTest(ctx, "ch-1", at, "ok", "HTTP 200"); !ok || err != nil {
		t.Fatalf("test: %v %v", ok, err)
	}
	upd := got
	upd.Enabled = false
	if ok, err := s.UpdateNotifyChannel(ctx, upd); !ok || err != nil {
		t.Fatalf("update: %v %v", ok, err)
	}
	cur, _, _ := s.GetNotifyChannel(ctx, "ch-1")
	if cur.Enabled || cur.LastTestStatus != "ok" || cur.LastTestAt == nil || !bytes.Equal(cur.SecretSealed, sealed) {
		t.Fatalf("updated = %+v", cur)
	}
	clash := cur
	clash.Name = "mail"
	if _, err := s.UpdateNotifyChannel(ctx, clash); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("rename clash: %v", err)
	}
	// Rules.
	r := backup.NotifyRule{ID: "rule-1", Event: backup.EventFailure, ScopeKind: backup.ScopeAll, ChannelID: "ch-1", Enabled: true}
	if _, err := s.CreateNotifyRule(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateNotifyRule(ctx, backup.NotifyRule{ID: "rule-1b", Event: backup.EventFailure, ScopeKind: backup.ScopeAll, ChannelID: "ch-1", Enabled: true}); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("duplicate rule: %v", err)
	}
	if _, err := s.CreateNotifyRule(ctx, backup.NotifyRule{ID: "rule-x", Event: backup.EventFailure, ScopeKind: backup.ScopeAll, ChannelID: "nope", Enabled: true}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("rule to a missing channel: %v", err)
	}
	if _, err := s.CreateNotifyRule(ctx, backup.NotifyRule{ID: "rule-y", Event: "boom", ScopeKind: backup.ScopeAll, ChannelID: "ch-1"}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("bad event: %v", err)
	}
	if _, err := s.CreateNotifyRule(ctx, backup.NotifyRule{ID: "rule-z", Event: backup.EventSuccess, ScopeKind: backup.ScopeAll, ChannelID: backup.EnvChannelID}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("env channel as target: %v", err)
	}
	must2 := func(r backup.NotifyRule) {
		if _, err := s.CreateNotifyRule(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	must2(backup.NotifyRule{ID: "rule-2", Event: backup.EventRPOBreach, ScopeKind: backup.ScopePolicy, ScopeID: "policy-gold", ChannelID: "ch-2", Enabled: true})
	must2(backup.NotifyRule{ID: "rule-3", Event: backup.EventMissed, ScopeKind: backup.ScopeJob, ScopeID: "bjob-1", ChannelID: "ch-2", Enabled: true})
	rules, _ := s.ListNotifyRules(ctx)
	if len(rules) != 3 {
		t.Fatalf("rules = %+v", rules)
	}
	if ok, err := s.SetNotifyRuleEnabled(ctx, "rule-3", false); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if g, ok, _ := s.GetNotifyRule(ctx, "rule-3"); !ok || g.Enabled {
		t.Fatalf("rule-3 = %+v", g)
	}
	// Deleting a channel removes the rules that target it.
	if ok, err := s.DeleteNotifyChannel(ctx, "ch-2"); !ok || err != nil {
		t.Fatalf("delete channel: %v %v", ok, err)
	}
	rules, _ = s.ListNotifyRules(ctx)
	if len(rules) != 1 || rules[0].ID != "rule-1" {
		t.Fatalf("rules after channel delete = %+v", rules)
	}
	if ok, _ := s.DeleteNotifyRule(ctx, "rule-1"); !ok {
		t.Fatal("delete rule")
	}
	if ok, _ := s.DeleteNotifyRule(ctx, "rule-1"); ok {
		t.Fatal("second delete reported success")
	}
}

func runBackupRPOState(t *testing.T, s store.Store) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	if ok, err := s.ClaimBackupRPOBreach(ctx, "src", now, time.Hour); !ok || err != nil {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	if ok, _ := s.ClaimBackupRPOBreach(ctx, "src", now.Add(10*time.Minute), time.Hour); ok {
		t.Fatal("claimed again inside the re-notify interval")
	}
	if ok, _ := s.ClaimBackupRPOBreach(ctx, "src", now.Add(2*time.Hour), time.Hour); !ok {
		t.Fatal("not re-claimed after the interval")
	}
	if l, _ := s.ListBackupRPOBreaches(ctx); len(l) != 1 || l[0].Source != "src" || !l[0].Since.Equal(now) {
		t.Fatalf("breaches = %+v", l)
	}
	must(t, s.ClearBackupRPOBreach(ctx, "src"))
	if l, _ := s.ListBackupRPOBreaches(ctx); len(l) != 0 {
		t.Fatalf("breaches after clear = %+v", l)
	}
	if ok, _ := s.ClaimBackupRPOBreach(ctx, "src", now.Add(3*time.Hour), time.Hour); !ok {
		t.Fatal("a new episode must notify")
	}
}

func runBackupUsage(t *testing.T, s store.Store) {
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	mk := func(id, src, repo, key string, at time.Time) {
		completeBackup(t, s, backup.Record{ID: id, Source: src, Engine: "mysql", Tool: "mysqldump", Bucket: "central-backups", ObjectKey: "u/" + id,
			Status: backup.StatusQueued, CreatedAt: at, Repository: repo, Method: backup.MethodMySQLDump},
			backup.Meta{Format: backup.FormatSBK1, KeyID: key})
	}
	mk("u-1", "src-a", "r1", "k1", base)
	mk("u-2", "src-a", "r1", "k1", base.Add(time.Minute))
	mk("u-3", "src-b", "r1", "k2", base.Add(2*time.Minute))
	mk("u-4", "src-b", "", "", base.Add(3*time.Minute))
	use, err := s.BackupRepoUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]store.RepoUsage{}
	for _, u := range use {
		got[u.Repository+"/"+u.Source] = u
	}
	if len(got) != 3 || got["r1/src-a"].Count != 2 || got["r1/src-a"].Bytes != 2468 || got["r1/src-b"].Count != 1 || got["/src-b"].Count != 1 {
		t.Fatalf("repo usage = %+v", use)
	}
	ku, err := s.BackupKeyUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]store.KeyUsage{}
	for _, k := range ku {
		byKey[k.KeyID] = k
	}
	if len(byKey) != 3 || byKey["k1"].Count != 2 || byKey["k2"].Count != 1 || byKey[""].Count != 1 ||
		len(byKey["k1"].SampleIDs) != 2 || byKey["k1"].SampleIDs[0] != "u-2" {
		t.Fatalf("key usage = %+v", ku)
	}
	// An expired backup no longer counts.
	if _, _, err := s.UpdateBackupStatus(ctx, "u-1", backup.StatusComplete, backup.StatusExpired, nil); err != nil {
		t.Fatal(err)
	}
	use, _ = s.BackupRepoUsage(ctx)
	for _, u := range use {
		if u.Repository == "r1" && u.Source == "src-a" && u.Count != 1 {
			t.Fatalf("expired backup counted: %+v", u)
		}
	}
}

func runBackupRunCounts(t *testing.T, s store.Store) {
	ctx := context.Background()
	must(t, s.CreateBackupJob(ctx, cfgJob("bjob-rc", "rc")))
	now := time.Now().UTC().Truncate(time.Millisecond)
	add := func(id, status string, ago time.Duration) {
		must(t, s.CreateBackupRun(ctx, backup.Run{ID: id, JobID: "bjob-rc", Trigger: backup.TriggerSchedule, StartedAt: now.Add(-ago), Status: status}))
	}
	add("rc-1", backup.RunStatusSuccess, 1*time.Hour)
	add("rc-2", backup.RunStatusSuccess, 2*time.Hour)
	add("rc-3", string(backup.RunStatusFailed), 3*time.Hour)
	add("rc-4", backup.RunStatusSuccess, 10*24*time.Hour)
	cs, err := s.BackupRunCounts(ctx, now.Add(-7*24*time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]int{}
	for _, c := range cs {
		m[c.JobID+"/"+c.Status] = c.N
	}
	if len(m) != 2 || m["bjob-rc/success"] != 2 || m["bjob-rc/failed"] != 1 {
		t.Fatalf("counts = %+v", cs)
	}
}

func runAuditFiltered(t *testing.T, s store.Store) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	for i, a := range []string{"login", "backup_job.create", "pipeline.start", "backup.restore", "backup_repository.update", "backup.verify"} {
		must(t, s.AppendAudit(ctx, store.AuditRow{Time: now.Add(time.Duration(i) * time.Second), Action: a, Result: "ok", Data: []byte(`{"action":"` + a + `"}`)}))
	}
	page1, err := s.ListAuditFiltered(ctx, store.AuditFilter{ActionPrefix: "backup", Limit: 2})
	if err != nil || len(page1) != 2 || page1[0].Action != "backup.verify" || page1[1].Action != "backup_repository.update" {
		t.Fatalf("page 1 = %+v %v", page1, err)
	}
	page2, err := s.ListAuditFiltered(ctx, store.AuditFilter{ActionPrefix: "backup", Limit: 10, BeforeID: page1[1].ID})
	if err != nil || len(page2) != 2 || page2[0].Action != "backup.restore" || page2[1].Action != "backup_job.create" {
		t.Fatalf("page 2 = %+v %v", page2, err)
	}
	all, _ := s.ListAuditFiltered(ctx, store.AuditFilter{Limit: 100})
	if len(all) != 6 {
		t.Fatalf("unfiltered = %d", len(all))
	}
	until := now.Add(2 * time.Second)
	if w, _ := s.ListAuditFiltered(ctx, store.AuditFilter{Until: &until, Limit: 100}); len(w) != 2 {
		t.Fatalf("until = %+v", w)
	}
}
