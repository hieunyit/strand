package web

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// The Backup overview warned about a missing encryption key by checking the Web's own secret key
// (STRAND_WEB_SECRET_KEY), which is always set in a database build, so the warning never appeared even
// when STRAND_BACKUP_ENCRYPTION_KEYS was empty and every new backup was written unencrypted.
func TestBackupOverviewWarnsWithoutBackupEncryptionKey(t *testing.T) {
	const banner = "No backup encryption key configured"
	t.Setenv(backup.EnvEncryptionKeys, "")
	t.Setenv(backup.EnvEncryptionKeysFile, "")
	e := dbEnv(t, store.NewMem(), nil, nil)
	c, _ := e.loginOK()
	resp, body := e.do(c, "GET", "/backups", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, banner) {
		t.Fatalf("no backup key configured but no warning (status %d)", resp.StatusCode)
	}
	if strings.Contains(body, "placeholder (P6)") || strings.Contains(body, "SLA tracking in P6") {
		t.Fatal("overview still shows the P6 placeholder instead of the RPO count")
	}

	t.Setenv(backup.EnvEncryptionKeys, "k1:"+base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))))
	e2 := dbEnv(t, store.NewMem(), nil, nil)
	c2, _ := e2.loginOK()
	if _, body := e2.do(c2, "GET", "/backups", "", nil); strings.Contains(body, banner) {
		t.Fatal("warning shown although a backup encryption key is configured")
	}
}

// The compliance report had only a JSON API and a CSV export; the page shows the same range for every role.
func TestBackupReportsPage(t *testing.T) {
	e := dbEnv(t, store.NewMem(), nil, nil)
	c, _ := e.loginOK()
	resp, body := e.do(c, "GET", "/backups/reports?from=2026-10-01&to=2026-10-31", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("reports page: %d", resp.StatusCode)
	}
	for _, want := range []string{`class="pagehead"`, `class="breadcrumb"`, "Compliance report",
		`href="/api/v1/backup-reports/compliance.csv?from=2026-10-01&amp;to=2026-10-31"`, `value="2026-10-01"`, `value="2026-10-31"`} {
		if !strings.Contains(body, want) {
			t.Errorf("reports page lacks %q", want)
		}
	}
	if _, body := e.do(c, "GET", "/backups/reports?from=2026-11-01&to=2026-10-01", "", nil); !strings.Contains(body, "showing the last 30 days") {
		t.Error("an inverted range is not reported")
	}
}

// A run still in progress (or cancelled) is neither a success nor a failure: it used to lower the 24-hour
// success rate and appear as a failed bar in the 14-day chart.
func TestBackupSuccessRateIgnoresUnfinishedRuns(t *testing.T) {
	st := store.NewMem()
	ctx := context.Background()
	now := time.Now().UTC()
	job := backup.Job{ID: "j1", Name: "j1", Command: "/opt/b/run", EverySeconds: 3600, ArtifactTemplate: "/var/backups/{id}.sql",
		ManifestTemplate: "/var/backups/{id}.json", Bucket: "central-backups"}
	if err := st.CreateBackupJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	for i, status := range []string{backup.RunStatusSuccess, string(backup.RunStatusRunning), string(backup.RunStatusCancelled)} {
		if err := st.CreateBackupRun(ctx, backup.Run{ID: fmt.Sprintf("brun-%d", i), JobID: "j1", Trigger: backup.TriggerSchedule,
			StartedAt: now.Add(-time.Duration(i+1) * time.Minute), Status: status}); err != nil {
			t.Fatal(err)
		}
	}
	e := dbEnv(t, st, nil, nil)
	c, _ := e.loginOK()
	_, body := e.do(c, "GET", "/backups", "", nil)
	i := strings.Index(body, "24h success rate")
	if i < 0 {
		t.Fatal("success rate tile missing")
	}
	if tile := body[i : i+200]; !strings.Contains(tile, ">100%<") {
		t.Fatalf("one success and two unfinished runs should be 100%%: %s", tile)
	}
}
