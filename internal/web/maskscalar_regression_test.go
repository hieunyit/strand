package web

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Exact regression tests for scalar-based masking and unmasking.

// 1. Compact JSON reorder source/target:
// Scalar mapping path identity ensures target.password restores target-secret
// and source.password restores source-secret even when reordered on a single compact line.
func TestRegression_CompactReorderSourceTarget(t *testing.T) {
	orig := []byte(`{"source":{"password":"source-canary-secret"},"target":{"password":"target-canary-secret"}}`)
	masked, err := MaskConfigText(orig)
	if err != nil {
		t.Fatalf("MaskConfigText: %v", err)
	}
	if strings.Contains(masked, "source-canary-secret") || strings.Contains(masked, "target-canary-secret") {
		t.Fatalf("secrets leaked in masked JSON: %s", masked)
	}

	// Reorder target before source
	reordered := `{"target":{"password":"********"},"source":{"password":"********"}}`
	restored, err := UnmaskConfigText(reordered, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText reordered: %v", err)
	}

	var parsed struct {
		Source struct {
			Password string `yaml:"password"`
		} `yaml:"source"`
		Target struct {
			Password string `yaml:"password"`
		} `yaml:"target"`
	}
	if err := yaml.Unmarshal([]byte(restored), &parsed); err != nil {
		t.Fatalf("unmarshal restored: %v", err)
	}
	if parsed.Source.Password != "source-canary-secret" {
		t.Errorf("source password got %q, want %q", parsed.Source.Password, "source-canary-secret")
	}
	if parsed.Target.Password != "target-canary-secret" {
		t.Errorf("target password got %q, want %q", parsed.Target.Password, "target-canary-secret")
	}
	// Check exact text output preserves JSON format
	want := `{"target":{"password":"target-canary-secret"},"source":{"password":"source-canary-secret"}}`
	if restored != want {
		t.Errorf("text mismatch:\ngot:  %s\nwant: %s", restored, want)
	}
}

// 2. One real edited + one placeholder on same line:
// Editing one field with a real password while keeping another field masked on the same line
// must preserve the deliberate edited password and restore the original secret for the masked field.
func TestRegression_OneRealEditedOnePlaceholderSameLine(t *testing.T) {
	orig := []byte(`{"source":{"password":"orig-src-pass"},"target":{"password":"orig-tgt-pass"}}`)

	// Case 2a: Edit source with a real password, leave target as placeholder
	editedA := `{"source":{"password":"my-new-deliberate-pass"},"target":{"password":"********"}}`
	restoredA, err := UnmaskConfigText(editedA, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText (edit source): %v", err)
	}
	wantA := `{"source":{"password":"my-new-deliberate-pass"},"target":{"password":"orig-tgt-pass"}}`
	if restoredA != wantA {
		t.Errorf("Case 2a mismatch:\ngot:  %s\nwant: %s", restoredA, wantA)
	}

	// Case 2b: Leave source as placeholder, edit target with a real password
	editedB := `{"source":{"password":"********"},"target":{"password":"my-new-target-pass"}}`
	restoredB, err := UnmaskConfigText(editedB, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText (edit target): %v", err)
	}
	wantB := `{"source":{"password":"orig-src-pass"},"target":{"password":"my-new-target-pass"}}`
	if restoredB != wantB {
		t.Errorf("Case 2b mismatch:\ngot:  %s\nwant: %s", restoredB, wantB)
	}
}

// 3. Line split / join (block <-> flow conversions):
// Restoring works across conversions from block YAML to single-line flow/JSON and vice-versa.
func TestRegression_LineSplitJoin(t *testing.T) {
	blockOrig := []byte("source:\n  password: block-source-pass\ntarget:\n  password: block-target-pass\n")

	// Joined into single-line flow:
	flowEdited := `{"source":{"password":"********"},"target":{"password":"********"}}`
	restoredFlow, err := UnmaskConfigText(flowEdited, blockOrig)
	if err != nil {
		t.Fatalf("Unmask block->flow: %v", err)
	}
	wantFlow := `{"source":{"password":"block-source-pass"},"target":{"password":"block-target-pass"}}`
	if restoredFlow != wantFlow {
		t.Errorf("block->flow mismatch:\ngot:  %s\nwant: %s", restoredFlow, wantFlow)
	}

	flowOrig := []byte(`{"source":{"password":"flow-source-pass"},"target":{"password":"flow-target-pass"}}`)
	// Split into multiline block YAML:
	blockEdited := "target:\n  password: \"********\"\nsource:\n  password: \"********\"\n"
	restoredBlock, err := UnmaskConfigText(blockEdited, flowOrig)
	if err != nil {
		t.Fatalf("Unmask flow->block: %v", err)
	}
	if !strings.Contains(restoredBlock, "target:\n  password: \"flow-target-pass\"") &&
		!strings.Contains(restoredBlock, "target:\n  password: flow-target-pass") {
		t.Errorf("flow->block target missing: %s", restoredBlock)
	}
	if !strings.Contains(restoredBlock, "source:\n  password: \"flow-source-pass\"") &&
		!strings.Contains(restoredBlock, "source:\n  password: flow-source-pass") {
		t.Errorf("flow->block source missing: %s", restoredBlock)
	}
}

// 4. Sequence indexes:
// Scalars inside sequences are identified by sequence index paths (e.g. endpoints[0], endpoints[1]).
func TestRegression_SequenceIndexes(t *testing.T) {
	orig := []byte(`
endpoints:
  - name: primary
    auth:
      token: token-zero
  - name: replica
    auth:
      token: token-one
`)
	masked, err := MaskConfigText(orig)
	if err != nil {
		t.Fatalf("MaskConfigText: %v", err)
	}
	if strings.Contains(masked, "token-zero") || strings.Contains(masked, "token-one") {
		t.Fatalf("token leaked: %s", masked)
	}

	// Unmask round-trip
	restored, err := UnmaskConfigText(masked, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText sequence: %v", err)
	}
	if restored != string(orig) {
		t.Errorf("sequence round-trip mismatch:\ngot:  %s\nwant: %s", restored, orig)
	}

	// Mix: edit index 0 with real token, leave index 1 as placeholder
	edited := strings.Replace(masked, `token: "********"`, `token: "deliberate-token-zero"`, 1)
	restoredMixed, err := UnmaskConfigText(edited, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText mixed sequence: %v", err)
	}
	if !strings.Contains(restoredMixed, `token: "deliberate-token-zero"`) {
		t.Errorf("deliberate token not preserved: %s", restoredMixed)
	}
	if !strings.Contains(restoredMixed, `token: token-one`) {
		t.Errorf("token-one not restored: %s", restoredMixed)
	}
}

// 5. Embedded DSNs:
// Binds scalar path and ordinal inside that scalar, handles quoted/escaped keyword/URL/MySQL DSNs,
// and rejects ambiguous count changes.
func TestRegression_EmbeddedDSNs(t *testing.T) {
	orig := []byte(`
connections:
  pg_url: "postgres://admin:pg-pass-secret@db.internal:5432/orders?sslmode=require"
  mysql_dsn: "user:mysql-pass-secret@tcp(mysql.internal:3306)/orders"
  pg_kv: "host=db.internal port=5432 user=app password='kv pass secret' dbname=orders"
  pg_escaped: "host=db.internal user=app password=\"esc\\\"pass\" dbname=orders"
`)
	masked, err := MaskConfigText(orig)
	if err != nil {
		t.Fatalf("MaskConfigText: %v", err)
	}
	for _, leak := range []string{"pg-pass-secret", "mysql-pass-secret", "kv pass secret", "esc\\\"pass"} {
		if strings.Contains(masked, leak) {
			t.Fatalf("leaked secret %q in masked: %s", leak, masked)
		}
	}

	// Editing host in pg_url while keeping placeholder restores the original secret
	editedHost := strings.Replace(masked, "db.internal", "new-db.internal", -1)
	editedHost = strings.Replace(editedHost, "mysql.internal", "new-mysql.internal", -1)
	restored, err := UnmaskConfigText(editedHost, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText with edited host: %v", err)
	}
	if !strings.Contains(restored, "postgres://admin:pg-pass-secret@new-db.internal:5432/orders") {
		t.Errorf("pg_url host edit failed:\n%s", restored)
	}
	if !strings.Contains(restored, "user:mysql-pass-secret@tcp(new-mysql.internal:3306)/orders") {
		t.Errorf("mysql_dsn host edit failed:\n%s", restored)
	}
	if !strings.Contains(restored, "password='kv pass secret'") {
		t.Errorf("pg_kv quoted password not restored:\n%s", restored)
	}
	if !strings.Contains(restored, `password=\"esc\\\"pass\"`) {
		t.Errorf("pg_escaped password not restored:\n%s", restored)
	}

	// Multi-secret scalar count change rejection (ambiguous count change):
	multiOrig := []byte("multi: \"host=h1 password=p1 host=h2 password=p2\"\n")
	multiMasked, err := MaskConfigText(multiOrig)
	if err != nil {
		t.Fatalf("MaskConfigText multi: %v", err)
	}
	// User removes or edits one placeholder, leaving only 1 placeholder when 2 were expected
	ambiguousEdit := strings.Replace(multiMasked, "password=********", "password=real1", 1)
	if _, err := UnmaskConfigText(ambiguousEdit, multiOrig); err == nil {
		t.Fatal("ambiguous count change in multi-secret scalar must fail closed")
	}
}

// 6. Original plain secret containing comma/quotes moved into flow:
// Secret strings inserted into different YAML/JSON syntax must be safely quoted/encoded,
// validate resulting YAML and semantic secret values.
func TestRegression_PlainSecretWithCommaOrQuotesMovedIntoFlow(t *testing.T) {
	// In block YAML, "hello, world" is a valid plain unquoted scalar
	orig := []byte("source:\n  password: hello, world\n")
	if _, err := MaskConfigText(orig); err != nil {
		t.Fatalf("MaskConfigText: %v", err)
	}

	// Move into flow style where raw comma would break mapping syntax
	flowEdited := `source: {password: "********"}`
	restoredFlow, err := UnmaskConfigText(flowEdited, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText flow: %v", err)
	}

	// Must be safely quoted so YAML remains valid
	var parsed struct {
		Source struct {
			Password string `yaml:"password"`
		} `yaml:"source"`
	}
	if err := yaml.Unmarshal([]byte(restoredFlow), &parsed); err != nil {
		t.Fatalf("restored flow YAML invalid: %v\ncontent: %s", err, restoredFlow)
	}
	if parsed.Source.Password != "hello, world" {
		t.Fatalf("semantic password mismatch: got %q, want %q", parsed.Source.Password, "hello, world")
	}

	// Test plain secret with quotes moved into compact JSON
	origQuotes := []byte("source:\n  password: foo\"bar'baz\n")
	jsonEdited := `{"source":{"password":"********"}}`
	restoredJSON, err := UnmaskConfigText(jsonEdited, origQuotes)
	if err != nil {
		t.Fatalf("UnmaskConfigText JSON: %v", err)
	}
	var parsedJSON struct {
		Source struct {
			Password string `yaml:"password"`
		} `yaml:"source"`
	}
	if err := yaml.Unmarshal([]byte(restoredJSON), &parsedJSON); err != nil {
		t.Fatalf("restored JSON YAML invalid: %v\ncontent: %s", err, restoredJSON)
	}
	if parsedJSON.Source.Password != "foo\"bar'baz" {
		t.Fatalf("semantic quote password mismatch: got %q, want %q", parsedJSON.Source.Password, "foo\"bar'baz")
	}
}

// 7. Unmatched placeholder and alias fail closed:
// Placeholders with unknown paths, secret aliases, or unexpected star tokens must fail closed.
func TestRegression_UnmatchedPlaceholderAndAliasFailClosed(t *testing.T) {
	orig := []byte("source:\n  password: s-secret\n")

	// 7a. Unmatched path
	unmatchedEdited := "source:\n  password: \"********\"\nextra:\n  password: \"********\"\n"
	if _, err := UnmaskConfigText(unmatchedEdited, orig); err == nil {
		t.Fatal("unmatched path placeholder must fail closed")
	}

	// 7b. Unmatched non-secret field containing placeholder
	unmatchedNonSecret := "source:\n  password: \"********\"\n  name: \"********\"\n"
	if _, err := UnmaskConfigText(unmatchedNonSecret, orig); err == nil {
		t.Fatal("unmatched placeholder in non-secret field must fail closed")
	}

	// 7c. Secret alias in orig fails closed
	aliasOrig := []byte("common: &pw secret\nsource: {password: *pw}\n")
	if _, err := MaskConfigText(aliasOrig); err == nil {
		t.Fatal("secret alias in MaskConfigText must fail closed")
	}

	// 7d. Secret alias in edited fails closed
	aliasEdited := "common: &pw \"********\"\nsource: {password: *pw}\n"
	if _, err := UnmaskConfigText(aliasEdited, orig); err == nil {
		t.Fatal("secret alias in UnmaskConfigText must fail closed")
	}
}

// 8. Arbitrary stars in comments preserved:
// Don't replace arbitrary stars in comments, and comments containing stars do not break unmasking.
func TestRegression_StarsInCommentsPreserved(t *testing.T) {
	orig := []byte("# Note: old password was ******** (or ****************)\nsource:\n  password: real-secret-123\n")
	masked, err := MaskConfigText(orig)
	if err != nil {
		t.Fatalf("MaskConfigText: %v", err)
	}
	if !strings.Contains(masked, "# Note: old password was ******** (or ****************)") {
		t.Fatalf("stars in comment were modified during masking: %s", masked)
	}

	restored, err := UnmaskConfigText(masked, orig)
	if err != nil {
		t.Fatalf("UnmaskConfigText: %v", err)
	}
	if restored != string(orig) {
		t.Fatalf("round trip with comment stars failed:\ngot:  %s\nwant: %s", restored, orig)
	}
}
