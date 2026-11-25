package web

import (
	"net/url"
	"reflect"
	"strings"
	"testing"

	"strand/internal/config"
)

// Several MongoDB databases chosen in the form become source.databases, each copied
// to a database of the same name, with one "<db>.*" include per database by default.
func TestNewPipelineMongoDBSeveralDatabases(t *testing.T) {
	e, c, csrf := npEnv(t)
	form := url.Values{
		"name": {"mongo-multi"}, "from": {"mongodb"}, "to": {"mongodb"},
		"source_uri": {"mongodb://s:pw@10.0.0.31:27017/?replicaSet=rs0"}, "target_uri": {"mongodb://t:pw@10.0.0.32:27017/?replicaSet=rs1"},
		"source_database": {"shop, billing"}, "max_parallel": {"6"},
	}
	code, _, y, out := createNPForm(t, e, c, csrf, form)
	if code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	for _, want := range []string{"databases:\n    - shop\n    - billing", "- shop.*", "- billing.*"} {
		if !strings.Contains(y, want) {
			t.Errorf("YAML lacks %q:\n%s", want, y)
		}
	}
	if strings.Contains(y, "  database:") {
		t.Errorf("YAML has a single database key:\n%s", y)
	}
	cfg, err := config.LoadYAMLStrictWithEnv([]byte(y), map[string]string{"SRC_URI": "mongodb://s/?replicaSet=rs0", "DST_URI": "mongodb://t/?replicaSet=rs1"})
	if err != nil {
		t.Fatalf("stored config does not load: %v\n%s", err, y)
	}
	if cfg.Bootstrap.Logical.MaxParallel != 6 {
		t.Fatalf("max_parallel %d:\n%s", cfg.Bootstrap.Logical.MaxParallel, y)
	}
	if got := cfg.MongoDatabases(); !reflect.DeepEqual(got, []config.MongoDatabase{{Source: "shop", Target: "shop"}, {Source: "billing", Target: "billing"}}) {
		t.Fatalf("databases %+v", got)
	}

	for name, mut := range map[string]url.Values{
		"target with several": {"name": {"mongo-m2"}, "target_mongo_database": {"x"}},
		"listed twice":        {"name": {"mongo-m3"}, "source_database": {"shop, shop"}},
		"bad name":            {"name": {"mongo-m4"}, "source_database": {"shop, bi.lling"}},
		"include misses one":  {"name": {"mongo-m5"}, "include": {"shop.*"}},
		"parallel too high":   {"name": {"mongo-m6"}, "max_parallel": {"33"}},
	} {
		f := url.Values{}
		for k, v := range form {
			f[k] = v
		}
		for k, v := range mut {
			f[k] = v
		}
		if code, _, _, out := createNPForm(t, e, c, csrf, f); code == 200 {
			t.Errorf("%s: accepted: %s", name, out)
		}
	}
}
