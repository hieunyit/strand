package config

import (
	"reflect"
	"strings"
	"testing"
)

const mongoMultiYAML = `
version: 1
name: m2
source:
  kind: mongodb
  dsn: "mongodb://s1:27017/?replicaSet=rs0"
  databases: [app, billing, logs]
target:
  kind: mongodb
  dsn: "mongodb://t1:27017/?replicaSet=rs1"
  databases: { billing: billing_copy }
tables:
  include: ["app.*", "billing.invoices", "logs.*"]
  exclude: ["*.tmp"]
`

// source.databases lists several databases; target.databases renames some of them.
func TestMongoDBSeveralDatabases(t *testing.T) {
	cfg, err := LoadYAMLStrict([]byte(mongoMultiYAML))
	if err != nil {
		t.Fatal(err)
	}
	want := []MongoDatabase{{"app", "app"}, {"billing", "billing_copy"}, {"logs", "logs"}}
	if got := cfg.MongoDatabases(); !reflect.DeepEqual(got, want) {
		t.Fatalf("databases %+v, want %+v", got, want)
	}
	single, err := LoadYAMLStrict([]byte(mongoYAML))
	if err == nil {
		if got := single.MongoDatabases(); len(got) != 1 || got[0] != (MongoDatabase{"app", "app"}) {
			t.Fatalf("single database: %+v", got)
		}
	}
	for name, c := range map[string]struct{ from, to, msg string }{
		"database and databases":     {"  databases: [app, billing, logs]", "  database: app\n  databases: [app, billing, logs]", "not both"},
		"target.database with list":  {"  databases: { billing: billing_copy }", "  database: x", "target.databases"},
		"duplicate source":           {"[app, billing, logs]", "[app, billing, app]", "twice"},
		"two into one target":        {"{ billing: billing_copy }", "{ billing: app }", "both be copied"},
		"map unknown database":       {"{ billing: billing_copy }", "{ other: x }", "not in source.databases"},
		"target is another source":   {"{ billing: billing_copy }", "{ billing: logs2 }\n  # x", ""},
		"system database":            {"[app, billing, logs]", "[app, billing, logs, admin]", "system database"},
		"bad name":                   {"[app, billing, logs]", `[app, billing, "lo.gs"]`, "not allow"},
		"pattern selects none":       {`"logs.*"]`, `"logs.*", "other.x"]`, "selects none"},
		"database without include":   {`, "logs.*"]`, `]`, `no tables.include pattern selects source database "logs"`},
		"exclude selects none":       {`["*.tmp"]`, `["zz.tmp"]`, "selects none"},
		"pattern without collection": {`"billing.invoices"`, `"billing."`, "database.collection"},
	} {
		y := strings.Replace(mongoMultiYAML, c.from, c.to, 1)
		if y == mongoMultiYAML {
			t.Fatalf("%s: mutation did not apply", name)
		}
		_, err := LoadYAMLStrict([]byte(y))
		if name == "target is another source" {
			// billing -> logs2 is fine; billing -> logs (a source database) is not.
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			y = strings.Replace(mongoMultiYAML, "{ billing: billing_copy }", "{ billing: logs, logs: logs_copy }", 1)
			if _, err = LoadYAMLStrict([]byte(y)); err == nil || !strings.Contains(err.Error(), "itself a source database") {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), c.msg) {
			t.Errorf("%s: got %v, want an error containing %q", name, err, c.msg)
		}
	}
	sql := `
version: 1
name: s
source: { kind: mysql, dsn: "u:p@tcp(h:3306)/", server_id: 7, databases: [a] }
target: { kind: mysql, dsn: "u:p@tcp(t:3306)/" }
tables: { include: ["a.*"] }
`
	if _, err := LoadYAMLStrict([]byte(sql)); err == nil || !strings.Contains(err.Error(), "only used by MongoDB") {
		t.Errorf("source.databases on a MySQL pipeline: %v", err)
	}
}
