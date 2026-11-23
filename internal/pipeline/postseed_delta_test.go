package pipeline

import (
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
	"strand/internal/config"
	"strand/internal/model"
)

// review R21: post-seed verify không được tắt ghi khóa khi verify.delta: on; khi delta tắt thì vẫn
// tắt như trước.
func TestPostSeedKeepsDeltaTracking(t *testing.T) {
	id := model.TableID{Schema: "db", Table: "t"}
	for _, tc := range []struct {
		delta  string
		wantOn bool
	}{{"on", true}, {"ON", true}, {"", false}, {"off", false}} {
		cfg := &config.Config{}
		cfg.Verify.Delta = tc.delta
		p := &Pipeline{cfg: cfg, touched: newTouchedKeys(100)}
		p.touched.enable(touchPostSeed, true)
		if strings.EqualFold(tc.delta, "on") {
			p.touched.enable(touchDelta, true) // như Run khi verify.delta: on
		}
		p.touched.add(id, []model.Value{int64(1)})
		keys, _, _ := p.takePostSeedKeys()
		if len(keys[id]) != 1 {
			t.Fatalf("delta=%q: post-seed took %v, want key 1", tc.delta, keys)
		}
		// Khóa CDC chạm sau post-seed: verify delta phải thấy nếu delta on.
		p.touched.add(id, []model.Value{int64(2)})
		later := p.touched.take(touchDelta).keys
		if got := len(later[id]) > 0 && later[id][len(later[id])-1] == 2; got != tc.wantOn {
			t.Fatalf("delta=%q: tracking after post-seed = %v, want %v", tc.delta, got, tc.wantOn)
		}
	}
}

func TestVerificationEvidenceInvalidatedByOfflineSchemaChange(t *testing.T) {
	state, err := openLocalState(t.TempDir(), "pipeline")
	if err != nil {
		t.Fatal(err)
	}
	id := model.TableID{Schema: "app", Table: "items"}
	p := &Pipeline{cfg: &config.Config{}, state: state,
		tables: map[model.TableID]*tableMeta{id: {Columns: []string{"id"}, PK: []string{"id"}, ColTypes: map[string]string{"id": "int"}}}}
	result := &admin.VerificationEvidence{Status: "checked", Tables: []admin.VerificationTable{{
		Table: id.String(), Method: "sample-and-changed-keys", Status: "checked",
	}}}
	if _, err := state.recordVerification(result, p.verificationFingerprint(), false, state.verificationVersion()); err != nil {
		t.Fatal(err)
	}
	if got := p.currentVerification(); got.Status != "checked" {
		t.Fatalf("matching schema: %+v", got)
	}
	// Same config and table name, but metadata from an offline ALTER.
	p.tables[id] = &tableMeta{Columns: []string{"id", "payload"}, PK: []string{"id"}, ColTypes: map[string]string{"id": "int", "payload": "text"}}
	if got := p.currentVerification(); got.Status != "not_checked" || len(got.Tables) != 1 || got.Tables[0].Table != id.String() {
		t.Fatalf("changed schema reused prior evidence: %+v", got)
	}
}

func TestKeylessDirtyTrackingPerConsumerAndRetry(t *testing.T) {
	id := model.TableID{Schema: "app", Table: "keyless"}
	touched := newTouchedKeys(100)
	touched.enable(touchDelta, true)
	touched.enable(touchPostSeed, true)
	touched.addKeyless(id)
	postSeed := touched.take(touchPostSeed)
	delta := touched.take(touchDelta)
	if !postSeed.dirtyKeyless[id] || !delta.dirtyKeyless[id] {
		t.Fatalf("keyless change is missing from one consumer")
	}
	if _, err := deltaVerifyResult(delta); err == nil || !strings.Contains(err.Error(), id.String()) {
		t.Fatalf("delta did not report changed keyless table: %v", err)
	}
	touched.restore(touchDelta, delta)
	if !touched.take(touchDelta).dirtyKeyless[id] || touched.take(touchPostSeed).dirtyKeyless[id] {
		t.Fatalf("retry did not preserve the keyless change for only the delta consumer")
	}
}

func TestPostSeedEvidenceRevisionInvalidatesAcrossScopeChanges(t *testing.T) {
	dir := t.TempDir()
	state, err := openLocalState(dir, "migration")
	if err != nil {
		t.Fatal(err)
	}
	evidence := &admin.VerificationEvidence{Status: "checked", CheckedAt: time.Now().UTC(),
		Tables: []admin.VerificationTable{{Table: "app.legacy", Status: "checked", Position: "0/FA"}}}
	version := state.verificationVersion()
	if _, err := state.recordVerification(evidence, "config", false, version); err != nil {
		t.Fatal(err)
	}
	state, err = openLocalState(dir, "migration")
	if err != nil {
		t.Fatal(err)
	}
	if got := state.verification("config"); got == nil || got.Tables[0].Position != "0/FA" {
		t.Fatalf("historical watermark lost: %+v", got)
	}
	if got := state.verification("changed"); got != nil {
		t.Fatalf("configuration change preserved the evidence: %+v", got)
	}
	version = state.verificationVersion()
	if err := state.AddTable("app.new"); err != nil {
		t.Fatal(err)
	}
	if got := state.verification("config"); got != nil {
		t.Fatalf("table addition preserved the evidence: %+v", got)
	}
	if got, err := state.recordVerification(evidence, "config", false, version); err != nil || got.Status != "incomplete" {
		t.Fatalf("in-flight verification published stale evidence: %+v %v", got, err)
	}
}
