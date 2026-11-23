package web

import (
	"net/http"
	"strings"
	"testing"

	"strand/internal/config"
)

func TestNewPipelineSSHTunnelConfig(t *testing.T) {
	e := newEnv(t, nil)
	client, _ := e.loginOK()
	resp, body := e.do(client, "GET", "/pipelines/new", "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `name="source_ssh_host"`) || !strings.Contains(body, `name="target_ssh_host"`) {
		t.Fatalf("SSH fields missing from new pipeline page (status %d)", resp.StatusCode)
	}
	opts := NewPipelineOpts{
		Name: "tunneled", From: "mysql", To: "postgres", Database: "app",
		SourceSSHTunnel: &config.SSHTunnelConfig{Host: "source.example", User: "strand"},
		TargetSSHTunnel: &config.SSHTunnelConfig{Host: "target.example", User: "strand", Port: 2222},
	}
	raw, err := buildNewPipelineYAML(opts, t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadYAMLStrictWithEnv([]byte(raw), map[string]string{"SRC_PW": "secret", "DST_PW": "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.SSHTunnel == nil || cfg.Source.SSHTunnel.Host != "source.example" ||
		cfg.Target.SSHTunnel == nil || cfg.Target.SSHTunnel.Port != 2222 {
		t.Fatalf("unexpected SSH config: source=%+v target=%+v", cfg.Source.SSHTunnel, cfg.Target.SSHTunnel)
	}
}
