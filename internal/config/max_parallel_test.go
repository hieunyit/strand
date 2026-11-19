package config_test

import (
	"strings"
	"testing"

	"strand/internal/config"
)

// bootstrap.logical.max_parallel: 1..32 (vắng = 1); đã thi hành nên không còn trong UnimplementedKeys.
func TestMaxParallelValidation(t *testing.T) {
	base := `version: 1
name: mp
source: { kind: mysql, dsn: "root@tcp(127.0.0.1:3306)/db" }
target: { kind: mysql, dsn: "root@tcp(127.0.0.1:3307)/db" }
tables: { include: ["app.*"] }
resources: { spool: { path: /tmp/x } }
`
	for _, n := range []string{"1", "4", "32"} {
		cfg, err := config.LoadYAMLStrict([]byte(base + "bootstrap: { logical: { max_parallel: " + n + " } }\n"))
		if err != nil {
			t.Fatalf("max_parallel %s: %v", n, err)
		}
		for _, k := range cfg.UnimplementedKeys() {
			if strings.Contains(k, "max_parallel") {
				t.Fatalf("max_parallel still reported unimplemented")
			}
		}
	}
	for _, n := range []string{"33", "-1"} {
		if _, err := config.LoadYAMLStrict([]byte(base + "bootstrap: { logical: { max_parallel: " + n + " } }\n")); err == nil ||
			!strings.Contains(err.Error(), "max_parallel") {
			t.Fatalf("max_parallel %s accepted (err %v)", n, err)
		}
	}
}
