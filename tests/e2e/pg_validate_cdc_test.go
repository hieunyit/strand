package e2e

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestPGValidateChecksCDCPrerequisites(t *testing.T) {
	src, dst, start := setupPGPair(t, "validate_slot", "validate_pub")
	src.exec(t, "SELECT pg_create_physical_replication_slot('validate_physical')", "CREATE PUBLICATION validate_partial FOR TABLE app.marker", "CREATE ROLE validate_norepl LOGIN PASSWORD 'validate-test-password'")
	cases := []struct {
		name, extra string
		ok          bool
		contains    string
	}{
		{"valid", ", slot: validate_slot, publication: validate_pub", true, ""},
		{"missing_publication", ", slot: validate_slot, publication: absent_pub", false, "publication"},
		{"missing_table", ", slot: validate_slot, publication: validate_partial", false, "publication"},
		{"physical_slot", ", slot: validate_physical, publication: validate_pub", false, "slot"},
		{"missing_slot", ", slot: absent_slot, publication: validate_pub", false, "slot"},
		{"create_slot", ", slot: absent_slot, publication: validate_pub, create_slot: true", true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := strandCfg{Name: "validate-cdc", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN, SourceExtra: tc.extra, Include: []string{"app.*"}, Dir: testDir(t)}
			out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
			if tc.ok && code != 0 {
				t.Fatalf("expected validation success, code=%d: %s", code, out)
			}
			if !tc.ok && (code == 0 || !strings.Contains(strings.ToLower(out), tc.contains)) {
				t.Fatalf("expected %s failure, code=%d: %s", tc.contains, code, out)
			}
		})
	}
	t.Run("role_without_replication", func(t *testing.T) {
		u, err := url.Parse(src.DSN)
		if err != nil {
			t.Fatal(err)
		}
		u.User = url.UserPassword("validate_norepl", "validate-test-password")
		cfg := strandCfg{Name: "validate-cdc-role", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: u.String(), TargetDSN: dst.DSN, SourceExtra: ", slot: validate_slot, publication: validate_pub", Include: []string{"app.*"}, Dir: testDir(t)}
		out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
		if code == 0 || !strings.Contains(out, "REPLICATION") {
			t.Fatalf("role without replication should fail, code=%d: %s", code, out)
		}
	})
	// Slot đang được pipeline dùng vẫn hợp lệ; chỉ cảnh báo vì không xác định owner.
	cfg := strandCfg{Name: "validate-cdc-active", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN, SourceExtra: ", slot: validate_slot, publication: validate_pub", Include: []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	path := cfg.write(t)
	p := startStrand(t, path, "--start-at", start)
	t.Cleanup(func() {
		if !p.exited() {
			p.kill9()
		}
	})
	waitSynced(t, &cfg, p, time.Minute)
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 0 || !strings.Contains(out, "is active") {
		t.Fatalf("active slot should warn, code=%d: %s", code, out)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("pipeline exit=%d: %s", code, p.tail())
	}
	var count int
	if err := src.DB.QueryRow("SELECT count(*) FROM pg_replication_slots WHERE slot_name='absent_slot'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("validate created slot: %d %v", count, err)
	}
}
