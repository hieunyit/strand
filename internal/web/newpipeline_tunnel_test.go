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
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `name="source_ssh_profile"`) || !strings.Contains(body, `name="target_ssh_profile"`) ||
		strings.Contains(body, `name="source_ssh_host"`) || strings.Contains(body, `name="target_ssh_host"`) {
		t.Fatalf("unexpected SSH profile controls on new pipeline page (status %d)", resp.StatusCode)
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
	cfg, err := config.LoadYAMLStrictWithEnv([]byte(raw), map[string]string{pipelineSourcePassword: "secret", pipelineTargetDSN: "postgres://strand:secret@127.0.0.1/app"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Source.SSHTunnel == nil || cfg.Source.SSHTunnel.Host != "source.example" ||
		cfg.Target.SSHTunnel == nil || cfg.Target.SSHTunnel.Port != 2222 {
		t.Fatalf("unexpected SSH config: source=%+v target=%+v", cfg.Source.SSHTunnel, cfg.Target.SSHTunnel)
	}
	opts.SourceSSHTunnel = &config.SSHTunnelConfig{Profile: "shared-bastion"}
	opts.TargetSSHTunnel = &config.SSHTunnelConfig{Profile: "shared-bastion"}
	raw, err = buildNewPipelineYAML(opts, t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err = config.LoadYAMLStrictWithEnv([]byte(raw), map[string]string{pipelineSourcePassword: "secret", pipelineTargetDSN: "postgres://strand:secret@127.0.0.1/app"})
	if err != nil || cfg.Source.SSHTunnel.Profile != "shared-bastion" || cfg.Target.SSHTunnel.Profile != "shared-bastion" {
		t.Fatalf("could not load shared profile references from generated YAML: %v", err)
	}

}
