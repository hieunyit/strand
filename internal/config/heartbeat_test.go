package config_test

import (
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/exitcode"
)

func TestHeartbeatIntervalConfig(t *testing.T) {
	load := func(kind, extra string) (*config.Config, error) {
		dsn := map[string]string{"postgres": "postgres://u@h/db", "mysql": "root@tcp(127.0.0.1:3306)/app"}[kind]
		y := "version: 1\nname: hb\nsource: { kind: " + kind + ", dsn: \"" + dsn + "\"" + extra + " }\n" +
			"target: { kind: postgres, dsn: \"postgres://u@h2/db\" }\ntables: { include: [\"app.*\"] }\n"
		return config.LoadYAMLStrict([]byte(y))
	}
	for _, ok := range []string{"", ", heartbeat_interval: 30s", ", heartbeat_interval: off", ", heartbeat_interval: \"0\""} {
		if _, err := load("postgres", ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	cfg, _ := load("postgres", ", heartbeat_interval: off")
	if !cfg.Source.HeartbeatOff() {
		t.Error("off must disable the heartbeat")
	}
	for _, c := range []struct{ kind, extra, want string }{
		{"postgres", ", heartbeat_interval: 10", "invalid source.heartbeat_interval"},
		{"postgres", ", heartbeat_interval: 500ms", "at least 1s"},
		{"mysql", ", heartbeat_interval: 10s", "only applies to a PostgreSQL source"},
	} {
		_, err := load(c.kind, c.extra)
		if err == nil || exitcode.GetExitCode(err) != exitcode.InvalidSyntax || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s %q: want %q, got %v", c.kind, c.extra, c.want, err)
		}
	}
}
