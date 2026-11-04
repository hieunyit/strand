package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/exitcode"
)

func TestBootstrapCreateSchema(t *testing.T) {
	for _, mode := range []string{"", "auto", "off", "AUTO", "bad"} {
		t.Run(mode, func(t *testing.T) {
			yaml := sampleSpecYAML
			if mode != "" {
				yaml += "\n"
				yaml = replaceBootstrap(yaml, mode)
			}
			c, err := config.LoadYAMLStrict([]byte(yaml))
			if mode == "bad" {
				if err == nil || exitcode.GetExitCode(err) != exitcode.InvalidSyntax {
					t.Fatalf("want invalid syntax: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Bootstrap.CreateSchemaAuto() != (mode != "off") {
				t.Fatalf("mode=%q auto=%v", mode, c.Bootstrap.CreateSchemaAuto())
			}
		})
	}
}
func replaceBootstrap(s, mode string) string {
	return strings.Replace(s, "bootstrap:", "bootstrap:\n  create_schema: "+mode, 1)
}
func TestBootstrapCreateSchemaEnvAndJSONSchema(t *testing.T) {
	p := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(p, []byte(sampleSpecYAML), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("STRAND_BOOTSTRAP_CREATE_SCHEMA", "off")
	c, err := config.LoadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Bootstrap.CreateSchemaAuto() {
		t.Fatal("env override ignored")
	}
	root := loadSchema(t)
	for _, mode := range []string{"auto", "off", "invalid"} {
		errs := schemaCheck(root, root, yamlToAny(t, replaceBootstrap(sampleSpecYAML, mode)), "$")
		if (len(errs) > 0) != (mode == "invalid") {
			t.Fatalf("schema mode=%s errors=%v", mode, errs)
		}
	}
}
