package web

import (
	"net/url"
	"strings"
	"testing"

	"strand/internal/config"
)

// MongoDB -> MongoDB from the new-pipeline form: the URIs are secrets (only in the encrypted credential record, never in the
// YAML), scope is "db.collection" patterns, tuning keys land under mongodb:, and the stored config parses.
func TestNewPipelineMongoDBKind(t *testing.T) {
	e, c, csrf := npEnv(t)
	srcURI := "mongodb://strand:MongoSrcSecret@10.0.0.31:27017/?replicaSet=rs0"
	code, _, y, out := createNPForm(t, e, c, csrf, url.Values{
		"name": {"mongo1"}, "from": {"mongodb"}, "to": {"mongodb"},
		"source_uri": {srcURI}, "target_uri": {"mongodb://strand:MongoDstSecret@10.0.0.32:27017/?replicaSet=rs1"},
		"source_database": {"shop"}, "target_mongo_database": {"shop_copy"},
		"include": {"shop.orders, shop.customers"}, "exclude": {"shop.tmp_*"},
		"snapshot_batch": {"500"}, "stream_batch": {"4000"}, "lease_ttl": {"60s"},
	})
	if code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	for _, leak := range []string{"MongoSrcSecret", "MongoDstSecret"} {
		if strings.Contains(y, leak) {
			t.Fatalf("URI secret in YAML:\n%s", y)
		}
	}
	for _, want := range []string{"kind: mongodb", `uri: "${SRC_URI}"`, `uri: "${DST_URI}"`, "database: shop_copy",
		"- shop.orders", "- shop.tmp_*", "snapshot_batch: 500", "stream_batch: 4000", "lease_ttl: 60s"} {
		if !strings.Contains(y, want) {
			t.Errorf("YAML lacks %q:\n%s", want, y)
		}
	}
	row, ok, _ := e.srv.db.st.GetPipeline(t.Context(), "mongo1")
	if !ok || !row.HasConfig || len(row.EnvEnc) == 0 || strings.Contains(string(row.EnvEnc), "MongoSrcSecret") {
		t.Fatalf("pipeline row: ok=%v has=%v env=%d", ok, row.HasConfig, len(row.EnvEnc))
	}
	plain, err := e.srv.db.box.Open(row.EnvEnc, "mongo1")
	if err != nil || !strings.Contains(string(plain), "SRC_URI=") || !strings.Contains(string(plain), "MongoSrcSecret") {
		t.Fatalf("encrypted credential record: %v %q", err, plain)
	}
	// Full strict load with the pipeline env: the URI becomes the source DSN.
	env := map[string]string{}
	lines, _ := ParseEnvData(strings.NewReader(string(plain)))
	for _, l := range lines {
		k, v, _ := strings.Cut(l, "=")
		env[k] = v
	}
	cfg, err := config.LoadYAMLStrictWithEnv([]byte(row.ConfigYAML), env)
	if err != nil {
		t.Fatalf("stored config does not load: %v", err)
	}
	if cfg.Source.DSN != srcURI || cfg.MongoDB.StreamBatch != 4000 || cfg.Target.Database != "shop_copy" {
		t.Fatalf("loaded %+v / %+v", cfg.Source, cfg.MongoDB)
	}
	if p := e.srv.reg.Get("mongo1"); p == nil || p.CfgErr != "" || p.Pair() != "mongodb → mongodb" {
		t.Fatalf("registry: %+v", p)
	}
	if row.EnvPath != "" {
		t.Fatal("MongoDB credentials should be stored only in the metadata database")
	}

	// Tuning card: MongoDB keys only; SQL keys are refused for this kind (and vice versa).
	resp, body := e.do(c, "GET", "/api/v1/pipelines/mongo1/tuning", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `"stream_batch"`) || !strings.Contains(body, `"4000"`) || strings.Contains(body, `"iops_soft"`) {
		t.Fatalf("mongo tuning view: %d %s", resp.StatusCode, body)
	}
	hdr := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}
	if resp, body := e.do(c, "POST", "/api/v1/pipelines/mongo1/tuning", `{"stream_batch": 3000, "lease_ttl": "90s"}`, hdr); resp.StatusCode != 200 {
		t.Fatalf("mongo tuning save: %d %s", resp.StatusCode, body)
	}
	if row, _, _ := e.srv.db.st.GetPipeline(t.Context(), "mongo1"); !strings.Contains(row.ConfigYAML, "stream_batch: 3000") || !strings.Contains(row.ConfigYAML, "lease_ttl: 90s") {
		t.Fatalf("tuning not stored in the database:\n%s", row.ConfigYAML)
	}
	if resp, body := e.do(c, "POST", "/api/v1/pipelines/mongo1/tuning", `{"iops_soft": 300}`, hdr); resp.StatusCode != 400 || !strings.Contains(body, "pipeline kind") {
		t.Fatalf("SQL key on a mongo pipeline: %d %s", resp.StatusCode, body)
	}
	if resp, body := e.do(c, "POST", "/api/v1/pipelines/orders/tuning", `{"stream_batch": 300}`, hdr); resp.StatusCode != 400 || !strings.Contains(body, "pipeline kind") {
		t.Fatalf("mongo key on a SQL pipeline: %d %s", resp.StatusCode, body)
	}

	// Invalid pairs and inputs are refused with a clear message.
	for _, tc := range []struct {
		v    url.Values
		want string
	}{
		{url.Values{"to": {"postgres"}}, "MongoDB can only be copied to MongoDB"},
		{url.Values{"source_uri": {"http://x"}}, "mongodb://"},
		{url.Values{"include": {"orders"}}, "database.collection"},
		{url.Values{"lease_ttl": {"1s"}}, "lease_ttl"},
	} {
		v := url.Values{"name": {"mongo-bad"}, "from": {"mongodb"}, "to": {"mongodb"}, "source_uri": {srcURI},
			"target_uri": {srcURI}, "source_database": {"shop"}, "target_mongo_database": {"shop"}}
		for k, x := range tc.v {
			v[k] = x
		}
		if code, _, _, out := createNPForm(t, e, c, csrf, v); code != 400 || !strings.Contains(out, tc.want) {
			t.Errorf("%v: %d %s (want 400 %q)", tc.v, code, out, tc.want)
		}
	}
}
