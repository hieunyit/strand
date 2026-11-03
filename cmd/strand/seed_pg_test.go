package main

import (
	"strings"
	"testing"

	"strand/internal/seed"
)

// pgRestoreCommands chỉ in hướng dẫn (Strand không tự chạy pgbackrest/wal-g), có PITR tới đúng P + pause.
func TestPGRestoreCommands(t *testing.T) {
	m := &seed.SeedManifest{Kind: "pgbackrest", BackupRef: "pgbackrest://main/20261101-000000F",
		StartPosition: seed.StartPosition{LSN: "0/9000060"}}
	all := strings.Join(restoreCommands(m), "\n")
	for _, want := range []string{"--stanza=main", "--set=20261101-000000F", "--type=standby",
		"recovery_target_lsn = '0/9000060'", "recovery_target_action = 'pause'", "pg_last_wal_replay_lsn() = P"} {
		if !strings.Contains(all, want) {
			t.Fatalf("missing %q in:\n%s", want, all)
		}
	}
	m = &seed.SeedManifest{Kind: "walg", BackupRef: "wal-g://base_000000010000000000000005", StartPosition: seed.StartPosition{LSN: "0/9000060"}}
	if all = strings.Join(restoreCommands(m), "\n"); !strings.Contains(all, "backup-fetch $PGDATA base_000000010000000000000005") {
		t.Fatalf("walg commands:\n%s", all)
	}
	if !isPGKind("strand-export") || isPGKind("xtrabackup") {
		t.Fatal("isPGKind")
	}
}
