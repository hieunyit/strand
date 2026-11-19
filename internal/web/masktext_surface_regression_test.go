package web

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMaskConfigTextMultilineEmbeddedSecretsFailClosed(t *testing.T) {
	for name, raw := range map[string]string{
		"folded URL":          "source:\n  dsn: >-\n    postgres://u:multiline-canary@h/db\n",
		"literal keyword":     "source:\n  dsn: |\n    host=h\n    password=multiline-canary\n",
		"double quoted URL":   "source:\n  dsn: \"postgres://u:multiline-canary@h/db\n    ?sslmode=disable\"\n",
		"single quoted MySQL": "source:\n  dsn: 'u:multiline-canary@tcp(h:3306)/db\n    ?parseTime=true'\n",
		"plain keyword":       "source:\n  dsn: host=h\n    password=multiline-canary dbname=db\n",
	} {
		t.Run(name, func(t *testing.T) {
			masked, err := MaskConfigText([]byte(raw))
			if err == nil || masked != "" {
				t.Fatalf("expected closed failure for multiline secret, got masked=%q err=%v", masked, err)
			}
			if strings.Contains(err.Error(), "multiline-canary") {
				t.Fatal("mask error exposes the secret")
			}
		})
	}
	plain := "description: >-\n  ordinary multiline\n  description\n"
	if masked, err := MaskConfigText([]byte(plain)); err != nil || masked != plain {
		t.Fatalf("nonsecret multiline changed: %q, %v", masked, err)
	}
}

func TestMaskConfigTextDiscoversDecodedEmbeddedSecret(t *testing.T) {
	orig := []byte(`source: {dsn: "postgres://u:\u0063anary@h/db"}`)
	masked, err := MaskConfigText(orig)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(masked, "anary") || !strings.Contains(masked, "********") {
		t.Fatalf("encoded credential was not masked: %q", masked)
	}
	restored, err := UnmaskConfigText(masked, orig)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodedSurfaceDSN(t, restored); got != "postgres://u:canary@h/db" {
		t.Fatalf("restored decoded DSN = %q", got)
	}
}

func decodedSurfaceDSN(t *testing.T, text string) string {
	t.Helper()
	var config struct {
		Source struct {
			DSN string `yaml:"dsn"`
		} `yaml:"source"`
	}
	if err := yaml.Unmarshal([]byte(text), &config); err != nil {
		t.Fatal(err)
	}
	return config.Source.DSN
}

func TestUnmaskConfigTextEmbeddedQuoteConversionPreservesCredential(t *testing.T) {
	for _, tc := range []struct {
		name, orig, edited, want string
	}{
		{
			name:   "double to single MySQL",
			orig:   `source: {dsn: "user:pa\\ss@tcp(oldhost:3306)/db"}`,
			edited: `source: {dsn: 'user:********@tcp(newhost:3306)/db'}`,
			want:   `user:pa\ss@tcp(newhost:3306)/db`,
		},
		{
			name:   "single to JSON MySQL",
			orig:   `source: {dsn: 'user:pa\ss@tcp(oldhost:3306)/db'}`,
			edited: `{"source":{"dsn":"user:********@tcp(newhost:3306)/db"}}`,
			want:   `user:pa\ss@tcp(newhost:3306)/db`,
		},
		{
			name:   "double to single keyword with escapes",
			orig:   `source: {dsn: "host=oldhost password='pa\\ss\\'word' dbname=db"}`,
			edited: `source: {dsn: 'host=newhost password=******** dbname=db'}`,
			want:   `host=newhost password='pa\ss\'word' dbname=db`,
		},
		{
			name:   "plain to double keyword",
			orig:   "source:\n  dsn: host=oldhost password=pa\\ss dbname=db\n",
			edited: `source: {dsn: "host=newhost password=******** dbname=db"}`,
			want:   `host=newhost password=pa\ss dbname=db`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restored, err := UnmaskConfigText(tc.edited, []byte(tc.orig))
			if err != nil {
				t.Fatal(err)
			}
			if got := decodedSurfaceDSN(t, restored); got != tc.want {
				t.Fatalf("decoded DSN = %q, want %q", got, tc.want)
			}
		})
	}
}
