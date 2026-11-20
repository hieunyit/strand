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

func cfgJob(id, name string) backup.Job {
	return backup.Job{ID: id, Name: name, EverySeconds: 3600, Enabled: true, Bucket: "central-backups", JobOptions: backup.JobOptions{
		Method: backup.MethodMySQLDump, Source: "s-" + id, Conn: backup.ConnSpec{Host: "h", Databases: []string{"app"}}}}
}

func runBackupConnections(t *testing.T, s store.Store) {
	ctx := context.Background()
	box, _ := store.NewBox("0123456789abcdef0123456789abcdef")
	sealed, err := box.Seal([]byte("s3cr3t-pw"), "conn-1")
	must(t, err)
	c := backup.Connection{ID: "conn-1", Name: "prod-mysql", Engine: backup.ConnMySQL, Host: "db1", Port: 3306, User: "backup",
		SecretMode: backup.SecretStored, PasswordSealed: sealed, TLSMode: "require", Labels: map[string]string{"env": "prod"},
		SSHTunnel: &backup.SSHTunnel{Host: "bastion", Port: 22, User: "ops", IdentityRef: "/etc/strand/id_ed25519"}, CreatedBy: "admin"}
	got, err := s.CreateBackupConnection(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if !got.PasswordSet || got.URISet || got.CreatedAt.IsZero() || got.Labels["env"] != "prod" || got.SSHTunnel == nil || got.SSHTunnel.Host != "bastion" {
		t.Fatalf("created = %+v", got)
	}
	if !bytes.Equal(got.PasswordSealed, sealed) {
		t.Fatal("sealed secret did not round-trip")
	}
	if pt, err := box.Open(got.PasswordSealed, "conn-1"); err != nil || string(pt) != "s3cr3t-pw" {
		t.Fatalf("open: %q %v", pt, err)
	}
	if _, err := box.Open(got.PasswordSealed, "other"); err == nil {
		t.Fatal("sealed secret opens under another id (AAD not bound)")
	}
	if b, _ := json.Marshal(got); strings.Contains(string(b), "ealed") || bytes.Contains(b, sealed) {
		t.Fatalf("connection JSON leaks sealed bytes: %s", b)
	}
	if _, err := s.CreateBackupConnection(ctx, c); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("duplicate id: %v", err)
	}
	dup := c
	dup.ID = "conn-2"
	if _, err := s.CreateBackupConnection(ctx, dup); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := s.CreateBackupConnection(ctx, backup.Connection{ID: "bad", Name: "x", Engine: "oracle", SecretMode: backup.SecretEnv}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("invalid engine: %v", err)
	}
	// env-mode connection
	env := backup.Connection{ID: "conn-env", Name: "stage-pg", Engine: backup.ConnPostgres, Host: "pg", SecretMode: backup.SecretEnv, PasswordEnv: "PG_PW"}
	if _, err := s.CreateBackupConnection(ctx, env); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListBackupConnections(ctx)
	if err != nil || len(list) != 2 || list[0].Name != "prod-mysql" {
		t.Fatalf("list = %+v %v", list, err)
	}
	// update replaces definition and secret, keeps created_at and the last test
	at := time.Now().UTC().Truncate(time.Millisecond)
	if ok, err := s.SetBackupConnectionTest(ctx, "conn-1", at, "ok", "MySQL 8.4"); err != nil || !ok {
		t.Fatalf("set test: %v %v", ok, err)
	}
	upd := got
	upd.Host = "db2"
	sealed2, _ := box.Seal([]byte("new"), "conn-1")
	upd.PasswordSealed = sealed2
	if ok, err := s.UpdateBackupConnection(ctx, upd); err != nil || !ok {
		t.Fatalf("update: %v %v", ok, err)
	}
	cur, _, _ := s.GetBackupConnection(ctx, "conn-1")
	if cur.Host != "db2" || !bytes.Equal(cur.PasswordSealed, sealed2) || cur.LastTestStatus != "ok" || cur.LastTestAt == nil || !cur.CreatedAt.Equal(got.CreatedAt) || !cur.UpdatedAt.After(got.UpdatedAt) {
		t.Fatalf("updated = %+v", cur)
	}
	clash := cur
	clash.Name = "stage-pg"
	if _, err := s.UpdateBackupConnection(ctx, clash); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("rename clash: %v", err)
	}
	if ok, _ := s.UpdateBackupConnection(ctx, backup.Connection{ID: "nope", Name: "n", Engine: backup.ConnMySQL, Host: "h", SecretMode: backup.SecretEnv}); ok {
		t.Fatal("update of a missing connection reported success")
	}
	// delete is refused while a job uses it, with the job list
	j := cfgJob("bjob-c1", "uses-conn")
	j.ConnectionID = "conn-1"
	must(t, s.CreateBackupJob(ctx, j))
	j2 := cfgJob("bjob-c2", "uses-conn-2")
	j2.ConnectionID = "conn-1"
	must(t, s.CreateBackupJob(ctx, j2))
	_, err = s.DeleteBackupConnection(ctx, "conn-1")
	var inuse *backup.InUseError
	if !errors.Is(err, backup.ErrInUse) || !errors.As(err, &inuse) || strings.Join(inuse.Jobs, ",") != "bjob-c1,bjob-c2" {
		t.Fatalf("delete in use = %v", err)
	}
	if _, ok, _ := s.GetBackupConnection(ctx, "conn-1"); !ok {
		t.Fatal("connection deleted despite jobs")
	}
	for _, id := range []string{"bjob-c1", "bjob-c2"} {
		if ok, err := s.DeleteBackupJob(ctx, id); err != nil || !ok {
			t.Fatal(err)
		}
	}
	if ok, err := s.DeleteBackupConnection(ctx, "conn-1"); err != nil || !ok {
		t.Fatalf("delete: %v %v", ok, err)
	}
	if ok, _ := s.DeleteBackupConnection(ctx, "conn-1"); ok {
		t.Fatal("second delete reported success")
	}
}

func runBackupPolicies(t *testing.T, s store.Store) {
	ctx := context.Background()
	built := backup.BuiltinPolicies()
	for _, p := range built {
		if _, err := s.CreateBackupPolicy(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.ListBackupPolicies(ctx)
	if err != nil || len(list) != 3 {
		t.Fatalf("list = %d %v", len(list), err)
	}
	gold, ok, _ := s.GetBackupPolicy(ctx, "policy-gold")
	if !ok || !gold.Builtin || gold.Retention.KeepMonthly != 12 || !gold.LogArchive || gold.Schedule.At != "01:00" || gold.CreatedAt.IsZero() {
		t.Fatalf("gold = %+v", gold)
	}
	br, _, _ := s.GetBackupPolicy(ctx, "policy-bronze")
	if br.Schedule.Kind != backup.ScheduleWeekly || len(br.Schedule.Days) != 1 || br.Schedule.Days[0] != 0 {
		t.Fatalf("bronze schedule = %+v", br.Schedule)
	}
	if _, err := s.CreateBackupPolicy(ctx, built[0]); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := s.CreateBackupPolicy(ctx, backup.Policy{ID: "p-bad", Name: "bad", Schedule: backup.Schedule{Kind: "cron", Cron: "nope"}}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("invalid: %v", err)
	}
	// editable, builtin flag and created_at preserved
	gold.Retention.KeepDaily = 21
	gold.Description = "edited"
	gold.Builtin = false
	if ok, err := s.UpdateBackupPolicy(ctx, gold); err != nil || !ok {
		t.Fatalf("update: %v %v", ok, err)
	}
	g2, _, _ := s.GetBackupPolicy(ctx, "policy-gold")
	if g2.Retention.KeepDaily != 21 || g2.Description != "edited" || !g2.Builtin || !g2.CreatedAt.Equal(gold.CreatedAt) || !g2.UpdatedAt.After(gold.UpdatedAt) {
		t.Fatalf("after update = %+v", g2)
	}
	clash := g2
	clash.Name = "Silver"
	if _, err := s.UpdateBackupPolicy(ctx, clash); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("rename clash: %v", err)
	}
	j := cfgJob("bjob-p1", "uses-policy")
	j.PolicyID = "policy-silver"
	must(t, s.CreateBackupJob(ctx, j))
	_, err = s.DeleteBackupPolicy(ctx, "policy-silver")
	var inuse *backup.InUseError
	if !errors.Is(err, backup.ErrInUse) || !errors.As(err, &inuse) || len(inuse.Jobs) != 1 || inuse.Jobs[0] != "bjob-p1" {
		t.Fatalf("delete in use = %v", err)
	}
	if ok, err := s.DeleteBackupPolicy(ctx, "policy-bronze"); err != nil || !ok {
		t.Fatalf("delete unused: %v %v", ok, err)
	}
}

func runBackupJobSchedule(t *testing.T, s store.Store) {
	ctx := context.Background()
	j := cfgJob("bjob-s1", "scheduled")
	j.Schedule = &backup.Schedule{Kind: backup.ScheduleWeekly, At: "02:00", Days: []int{0, 3}, Timezone: "Asia/Ho_Chi_Minh"}
	j.EverySeconds = j.Schedule.NominalSeconds()
	j.WindowMinutes, j.JitterSeconds = 90, 30
	j.PolicyID, j.ConnectionID, j.Overrides = "policy-x", "conn-x", []string{"retention"}
	must(t, s.CreateBackupJob(ctx, j))
	got, ok, err := s.GetBackupJob(ctx, j.ID)
	if err != nil || !ok || got.Schedule == nil || got.Schedule.Kind != backup.ScheduleWeekly || len(got.Schedule.Days) != 2 ||
		got.Schedule.Timezone != "Asia/Ho_Chi_Minh" || got.WindowMinutes != 90 || got.JitterSeconds != 30 ||
		got.PolicyID != "policy-x" || got.ConnectionID != "conn-x" || len(got.Overrides) != 1 {
		t.Fatalf("round trip = %+v %v", got, err)
	}
	// Recording a missed run: status missed, next run moved, notification bookkeeping untouched.
	next := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
	if ok, _ := s.RecordBackupJobMissed(ctx, j.ID, got.UpdatedAt.Add(-time.Second), next); ok {
		t.Fatal("stale revision recorded a missed run")
	}
	if ok, err := s.RecordBackupJobMissed(ctx, j.ID, got.UpdatedAt, next); err != nil || !ok {
		t.Fatalf("record missed: %v %v", ok, err)
	}
	m, _, _ := s.GetBackupJob(ctx, j.ID)
	if m.LastRunStatus != backup.RunStatusMissed || m.RunFinishedAt == nil || m.NextRunAt == nil || !m.NextRunAt.Equal(next) || m.ConsecutiveFailures != 0 {
		t.Fatalf("missed job = %+v", m)
	}
	// A missed job can run again.
	started, ok, err := s.BeginBackupJobRun(ctx, j.ID, m.UpdatedAt)
	if err != nil || !ok {
		t.Fatalf("begin after missed: %v %v", ok, err)
	}
	if ok, _ := s.RecordBackupJobMissed(ctx, j.ID, m.UpdatedAt, next); ok {
		t.Fatal("a running job was recorded as missed")
	}
	if ok, err := s.FinishBackupJobRun(ctx, j.ID, started, backup.RunStatusSucceeded); err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
}

func runBackupSlots(t *testing.T, s store.Store) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	req := func(job, host string) store.SlotRequest {
		return store.SlotRequest{JobID: job, Host: host, MaxTotal: 2, MaxPerHost: 1, TTL: time.Minute}
	}
	acq := func(job, host string, at time.Time) bool {
		ok, err := s.AcquireBackupSlot(ctx, req(job, host), at)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !acq("a", "h1", now) {
		t.Fatal("first slot refused")
	}
	if !acq("a", "h1", now) {
		t.Fatal("re-acquire by the same job must be idempotent")
	}
	if acq("b", "h1", now) {
		t.Fatal("second job on the same host got a slot (max_per_host=1)")
	}
	if !acq("c", "h2", now) {
		t.Fatal("job on another host refused")
	}
	if acq("d", "h3", now) {
		t.Fatal("third concurrent job got a slot (max=2)")
	}
	must(t, s.ReleaseBackupSlot(ctx, "a"))
	if !acq("d", "h3", now) {
		t.Fatal("released slot not reusable")
	}
	// Expiry: an un-renewed slot frees after the TTL; a renewed one does not.
	later := now.Add(2 * time.Minute)
	must(t, s.RenewBackupSlot(ctx, "c", later.Add(-10*time.Second)))
	if !acq("e", "h4", later) { // d expired, c renewed -> 1 used
		t.Fatal("expired slot was not freed")
	}
	if acq("f", "h5", later) {
		t.Fatal("renewed slot was freed")
	}
	// No limits (0) means unlimited.
	ok, err := s.AcquireBackupSlot(ctx, store.SlotRequest{JobID: "g", Host: "h1", TTL: time.Minute}, later)
	if err != nil || !ok {
		t.Fatalf("unlimited: %v %v", ok, err)
	}
}
