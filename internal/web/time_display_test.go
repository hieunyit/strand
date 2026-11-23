package web

import (
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
)

func TestBackupCatalogUsesUTCPlusSevenCalendarDays(t *testing.T) {
	e := newCPEnv(t)
	cutoff := time.Date(2026, 11, 22, 17, 0, 0, 0, time.UTC)
	seedRichBackup(t, e.st, "bkp-before-utc7", "mysql-app", "mysql", "mysqldump",
		backup.KindFull, cutoff.Add(-time.Second), backup.Position{}, false, nil, "", nil, nil, "", nil)
	seedRichBackup(t, e.st, "bkp-at-utc7", "mysql-app", "mysql", "mysqldump",
		backup.KindFull, cutoff, backup.Position{}, false, nil, "", nil, nil, "", nil)

	body := e.admin().must("GET", "/backups/catalog?from=2026-11-23&to=2026-11-23", nil, 200)
	if !strings.Contains(body, "bkp-at-utc7") || strings.Contains(body, "bkp-before-utc7") {
		t.Fatal("catalog filters did not use midnight in UTC+7")
	}
	if !strings.Contains(body, "2026-11-23 00:00:00 UTC&#43;7") {
		t.Fatal("catalog did not render the UTC+7 timestamp")
	}
}
