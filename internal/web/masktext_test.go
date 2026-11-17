package web

import (
	"strings"
	"testing"
)

func TestMaskUnmaskConfigTextRoundTrip(t *testing.T) {
	orig := "a: 1\n\n" +
		"source: { kind: mysql, password: 'p w', user: u }  # c\n" +
		"dsn: \"postgres://u:pw1@h:5432/d?sslmode=prefer\"\n" +
		"t: { password: x1, dsn: \"mysql://a:b2@h/d\" }\n" +
		"password_env: SRC_PW\n" +
		"pw: ${DST_PW}\n"
	masked, err := MaskConfigText([]byte(orig))
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"p w", "pw1", "x1", "b2"} {
		if strings.Contains(masked, leak) {
			t.Errorf("leak %q:\n%s", leak, masked)
		}
	}
	for _, keep := range []string{"password_env: SRC_PW", "${DST_PW}", "# c", "\n\n"} {
		if !strings.Contains(masked, keep) {
			t.Errorf("lost %q:\n%s", keep, masked)
		}
	}
	back, err := UnmaskConfigText(masked, []byte(orig))
	if err != nil || back != orig {
		t.Fatalf("round trip lỗi (err=%v):\n%s", err, back)
	}
	// Sửa host trong DSN nhưng giữ mật khẩu che: vẫn khôi phục được theo khóa.
	edited := strings.Replace(masked, "h:5432", "newhost:5432", 1)
	back, err = UnmaskConfigText(edited, []byte(orig))
	if err != nil || !strings.Contains(back, "postgres://u:pw1@newhost:5432/d") {
		t.Fatalf("sửa host + giữ mask: err=%v\n%s", err, back)
	}
}

func TestMaskConfigTextBlockScalarSecretRejected(t *testing.T) {
	if _, err := MaskConfigText([]byte("password: |\n  line1\n  line2\n")); err == nil {
		t.Fatal("block scalar bí mật phải bị từ chối thay vì rò rỉ")
	}
}

func TestMaskConfigTextCompactJSONAndFlowYAML(t *testing.T) {
	for _, raw := range []string{
		`{"source":{"password":"json-canary"},"target":{"password":"target-canary"}}`,
		`source: {password: 'flow canary', nested: {api_token: compact-token}}`,
	} {
		masked, err := MaskConfigText([]byte(raw))
		if err != nil {
			t.Fatalf("MaskConfigText(%q): %v", raw, err)
		}
		for _, secret := range []string{"json-canary", "target-canary", "flow canary", "compact-token"} {
			if strings.Contains(masked, secret) {
				t.Errorf("secret %q leaked from %q as %q", secret, raw, masked)
			}
		}
		if !strings.Contains(masked, `"********"`) && !strings.Contains(masked, `password: "********"`) && !strings.Contains(masked, `api_token: "********"`) {
			t.Errorf("no placeholder in masked %q", masked)
		}
		back, err := UnmaskConfigText(masked, []byte(raw))
		if err != nil || back != raw {
			t.Errorf("round trip: got %q err %v, want %q", back, err, raw)
		}
	}
}

func TestUnmaskRejectsCompactUnmatchedMaskedPassword(t *testing.T) {
	orig := []byte(`{"source":{"password":"original"}}`)
	back, err := UnmaskConfigText(`{"source":{"password":"********"}}`, orig)
	if err != nil || back != `{"source":{"password":"original"}}` {
		t.Fatalf("matching compact placeholder was not restored: back=%q err=%v", back, err)
	}
	if _, err := UnmaskConfigText(`{"target":{"password":"********"}}`, orig); err == nil {
		t.Fatal("unmatched compact placeholder must not be persisted")
	}
}

func TestMaskConfigTextSecretAliasRejected(t *testing.T) {
	raw := "common: &pw topsecret\nsource: {password: *pw}\n"
	if _, err := MaskConfigText([]byte(raw)); err == nil {
		t.Fatal("secret alias must fail closed because its anchor may expose the scalar")
	}
}

func TestMaskUnmaskWhitespaceOnlyPassword(t *testing.T) {
	orig := []byte("source:\n  password: '   '\n")
	masked, err := MaskConfigText(orig)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(masked, "password: '   '") || !strings.Contains(masked, `password: "********"`) {
		t.Fatalf("whitespace-only password was not masked: %q", masked)
	}
	back, err := UnmaskConfigText(masked, orig)
	if err != nil || back != string(orig) {
		t.Fatalf("whitespace-only password round trip: got %q err=%v, want %q", back, err, orig)
	}
	yamlMasked, err := MaskConfigYAML(orig)
	if err != nil || strings.Contains(yamlMasked, "password: '   '") || !strings.Contains(yamlMasked, "********") {
		t.Fatalf("MaskConfigYAML did not mask whitespace-only password: %q err=%v", yamlMasked, err)
	}
}

func TestUnmaskReorderedCompactJSON(t *testing.T) {
	orig := []byte(`{"source":{"password":"source-secret"},"target":{"password":"target-secret"}}`)
	masked, err := MaskConfigText(orig)
	if err != nil {
		t.Fatal(err)
	}
	reordered := strings.Replace(masked, `"source":{"password":"********"},"target":{"password":"********"}`, `"target":{"password":"********"},"source":{"password":"********"}`, 1)
	if reordered == masked {
		t.Fatalf("test did not reorder masked JSON: %s", masked)
	}
	got, err := UnmaskConfigText(reordered, orig)
	if err != nil {
		t.Fatalf("reordered compact placeholders should restore by scalar path: %v", err)
	}
	want := `{"target":{"password":"target-secret"},"source":{"password":"source-secret"}}`
	if got != want {
		t.Fatalf("reordered compact JSON restored wrongly:\ngot:  %s\nwant: %s", got, want)
	}
}
