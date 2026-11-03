package config_test

import (
	"testing"

	"strand/internal/config"
)

// Khóa source PG (P2): slot, publication, create_slot, failover_slot, sequence_sync_interval.
func TestLoadYAMLStrict_PGSourceKeys(t *testing.T) {
	cfg, err := config.LoadYAMLStrict([]byte(`
version: 1
name: pg-sync
source:
  kind: postgres
  dsn: "postgres://strand:pw@db1:5432/app"
  slot: strand_pg_sync
  publication: strand_pub
  create_slot: true
  failover_slot: true
  sequence_sync_interval: 2s
target: { kind: postgres, dsn: "postgres://strand:pw@db2:5432/app" }
tables: { include: ["public.*"] }
`))
	if err != nil {
		t.Fatalf("expected valid config: %v", err)
	}
	s := cfg.Source
	if s.Slot != "strand_pg_sync" || s.Publication != "strand_pub" || !s.CreateSlot || !s.FailoverSlot || s.SequenceSyncInterval != "2s" {
		t.Fatalf("PG source keys not parsed: %+v", s)
	}
}
