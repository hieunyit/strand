package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

func TestPostSeedVerificationScopeAtCatchUp(t *testing.T) {
	src, dst := startPG(t, "source"), startPG(t, "target")
	for _, db := range []*pgNode{src, dst} {
		db.exec(t, "CREATE SCHEMA app", klPGDDL, "ALTER TABLE app.kl REPLICA IDENTITY FULL",
			"INSERT INTO app.kl(a,b) VALUES (1,'dup'),(1,'dup')",
			"CREATE TABLE app.control(id int PRIMARY KEY, v text)", "INSERT INTO app.control VALUES (1,'a')")
	}
	for _, tc := range []struct {
		name    string
		include []string
		methods []string
	}{
		{"keyed", []string{"app.control"}, []string{"sample-and-changed-keys"}},
		{"mixed", []string{"app.control", "app.kl"}, []string{"sample-and-changed-keys", "count-and-checksum"}},
		{"keyless", []string{"app.kl"}, []string{"count-and-checksum"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub, slot := "pub_"+tc.name, "slot_"+tc.name
			src.exec(t, "CREATE PUBLICATION "+pub+" FOR TABLE "+strings.Join(tc.include, ", "))
			var start string
			if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', false, true)", slot).Scan(&start); err != nil {
				t.Fatal(err)
			}
			cfg := strandCfg{Name: "scope_" + tc.name, SourceKind: "postgres", TargetKind: "postgres",
				SourceDSN: src.DSN, TargetDSN: dst.DSN, SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
				Include: tc.include, Dir: testDir(t), Bootstrap: "start_at", TablesExtra: klKeylessCfg, PostSeedWindow: "3s"}
			path := cfg.write(t)
			p := startStrand(t, path, "--start-at", start)
			defer func() {
				if !p.exited() {
					p.kill9()
				}
			}()
			check := func() *admin.VerificationEvidence {
				t.Helper()
				client := admin.NewClient(cfg.Socket)
				var top *admin.TopInfo
				var err error
				deadline := time.Now().Add(time.Minute)
				for time.Now().Before(deadline) && !p.exited() {
					top, err = client.Top(t.Context())
					if err == nil && top.State == "synced" && top.CDCState == "caught_up" && top.Verification != nil {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				if err != nil || top == nil || top.State != "synced" || top.CDCState != "caught_up" ||
					top.Verification == nil || top.Verification.Status != "checked" || len(top.Verification.Tables) != len(tc.include) {
					t.Fatalf("top=%+v err=%v\n%s", top, err, p.tail())
				}
				for i, row := range top.Verification.Tables {
					if row.Table != tc.include[i] || row.Status != "checked" || row.Method != tc.methods[i] || row.Method == "count-and-checksum" && row.Position == "" {
						t.Fatalf("unexpected table evidence: %+v", row)
					}
				}
				return top.Verification
			}
			original := check()
			p.kill9()
			p = startStrand(t, path)
			if got := check(); !reflect.DeepEqual(got, original) {
				t.Fatalf("restart changed evidence: %+v -> %+v", original, got)
			}
			if out, code := runStrandCLI(t, time.Minute, "verify", "-c", path, "--mode", "full", "--require-complete"); code != 0 {
				t.Fatalf("strict full: exit %d\n%s", code, out)
			}
			if tc.name == "mixed" {
				dst.exec(t, "UPDATE app.kl SET b='drift' WHERE ctid=(SELECT ctid FROM app.kl LIMIT 1)")
				if out, code := runStrandCLI(t, time.Minute, "verify", "-c", path, "--mode", "full", "--require-complete"); code != 5 || !strings.Contains(out, "verify_mismatch") {
					t.Fatalf("mixed drift: exit %d\n%s", code, out)
				}
				dst.exec(t, "UPDATE app.kl SET b='dup'")
			}
			if code := p.stop(t); code != 0 {
				t.Fatalf("stop exit %d\n%s", code, p.tail())
			}
		})
	}
	// Keep the target static. A repeatable volatile row policy changes the
	// visible source multiset between S1 and S2 on every attempt.
	src.exec(t, "CREATE ROLE scope_reader", "GRANT USAGE ON SCHEMA app TO scope_reader",
		"CREATE SEQUENCE app.vis", "GRANT USAGE ON SEQUENCE app.vis TO scope_reader",
		"CREATE TABLE app.varying(v int)", "INSERT INTO app.varying VALUES (1)",
		"ALTER TABLE app.varying ENABLE ROW LEVEL SECURITY", "GRANT SELECT ON app.varying TO scope_reader",
		"CREATE POLICY changing ON app.varying USING (nextval('app.vis') % 2 = 1)")
	dst.exec(t, "CREATE TABLE app.varying(v int)", "INSERT INTO app.varying VALUES (1)")
	cfg := strandCfg{Name: "scope_changing", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN + "&options=-crole%3Dscope_reader", TargetDSN: dst.DSN, Include: []string{"app.varying"}, Dir: testDir(t), Bootstrap: "start_at", TablesExtra: klKeylessCfg}
	if out, code := runStrandCLI(t, time.Minute, "verify", "-c", cfg.write(t), "--mode", "full", "--require-complete"); code != 5 || !strings.Contains(out, "verify_incomplete") || !strings.Contains(out, "attempts=5") {
		t.Fatalf("unstable source: exit %d\n%s", code, out)
	}
	cfg.Include = []string{"app.control", "app.absent_*"}
	if out, code := runStrandCLI(t, time.Minute, "verify", "-c", cfg.write(t), "--mode", "full", "--require-complete"); code != 5 || !strings.Contains(out, "app.absent_*") {
		t.Fatalf("uncovered scope: exit %d\n%s", code, out)
	}

	// Use the persisted scope shape written by snapshot add. The YAML
	// alone would allow this drift to pass.
	cfg.SourceDSN = src.DSN
	cfg.Include = []string{"app.control"}
	stateDir := filepath.Join(cfg.Dir, "state")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	state := []byte(`{"pipeline_id":"scope_changing","added_tables":["app.kl"]}`)
	if err := os.WriteFile(filepath.Join(stateDir, "pipeline-state.json"), state, 0600); err != nil {
		t.Fatal(err)
	}
	dst.exec(t, "DELETE FROM app.kl WHERE ctid=(SELECT ctid FROM app.kl LIMIT 1)")
	path := cfg.write(t)
	if out, code := runStrandCLI(t, time.Minute, "verify", "-c", path, "--mode", "full", "--require-complete"); code != 5 || !strings.Contains(out, "verify_mismatch") || !strings.Contains(out, "app.kl") {
		t.Fatalf("drift in added table: exit %d\n%s", code, out)
	}
	if out, code := runStrandCLI(t, time.Minute, "verify", "-c", path, "--mode", "full", "--require-complete", "--replica-dsn", src.DSN); code != 2 || !strings.Contains(out, "invalid_verify_gate") {
		t.Fatalf("strict verify with a replica: exit %d\n%s", code, out)
	}
}
