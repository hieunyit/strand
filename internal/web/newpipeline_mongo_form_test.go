package web

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"strand/internal/backup/conncheck"
	"strand/internal/config"
	"strand/internal/web/store"
)

// The console asks for host(s), port, user, password, replica set, auth database and TLS instead of a URI; the
// server builds the URI (user and password escaped) and keeps it only in the encrypted .env.
func TestBuildMongoURI(t *testing.T) {
	for _, tc := range []struct {
		e    mongoEndpoint
		want string
	}{
		{mongoEndpoint{Hosts: "db1", Port: 27017, User: "strand", Password: "p@ss:w/rd?#", ReplicaSet: "rs0"},
			"mongodb://strand:p%40ss%3Aw%2Frd%3F%23@db1:27017/?authSource=admin&replicaSet=rs0"},
		{mongoEndpoint{Hosts: "m1:27018, m2 ,10.0.0.3:27019", Port: 27020, User: "u", Password: "x", AuthDB: "users", TLS: true},
			"mongodb://u:x@m1:27018,m2:27020,10.0.0.3:27019/?authSource=users&tls=true"},
		{mongoEndpoint{Hosts: "[::1]:27017"}, "mongodb://[::1]:27017/"},
		{mongoEndpoint{Hosts: "db1"}, "mongodb://db1:27017/"},
	} {
		got, err := buildMongoURI("source", tc.e)
		if err != nil || got != tc.want {
			t.Errorf("%+v: %q %v, want %q", tc.e, got, err, tc.want)
		}
	}
	for _, tc := range []struct {
		e    mongoEndpoint
		want string
	}{
		{mongoEndpoint{}, "source_host: required"},
		{mongoEndpoint{Hosts: "db1/evil"}, "source_host"},
		{mongoEndpoint{Hosts: "user@db1"}, "source_host"},
		{mongoEndpoint{Hosts: "db1:99999"}, "source_host"},
		{mongoEndpoint{Hosts: "db1", Port: 70000}, "source_port"},
		{mongoEndpoint{Hosts: "db1", Password: "x"}, "source_user"},
		{mongoEndpoint{Hosts: "db1", User: "u", ReplicaSet: "rs0&ssl=false"}, "source_replica_set"},
		{mongoEndpoint{Hosts: "db1", User: "u", AuthDB: "a/b"}, "source_auth_db"},
	} {
		if _, err := buildMongoURI("source", tc.e); err == nil || !strings.HasPrefix(err.Error(), tc.want) {
			t.Errorf("%+v: %v, want %q", tc.e, err, tc.want)
		}
	}
}

func TestNewPipelineMongoFromHostFields(t *testing.T) {
	e, c, csrf := npEnv(t)
	const srcPw, dstPw = "Src&Pw@1:/x", "DstPw#2"
	code, _, y, out := createNPForm(t, e, c, csrf, url.Values{
		"name": {"mongo-hosts"}, "from": {"mongodb"}, "to": {"mongodb"},
		"source_host": {"m1:27017, m2:27018"}, "source_port": {"27017"}, "source_user": {"strand"}, "source_password": {srcPw},
		"source_replica_set": {"rs0"}, "source_auth_db": {"admin"}, "source_tls": {"true"},
		"target_host": {"t1"}, "target_port": {"27030"}, "target_user": {"writer"}, "target_password": {dstPw},
		"target_auth_db": {"admin"}, "source_database": {"shop"},
	})
	if code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	for _, leak := range []string{srcPw, dstPw, "Src&Pw"} {
		if strings.Contains(y, leak) {
			t.Fatalf("password in YAML:\n%s", y)
		}
	}
	row, ok, _ := e.srv.db.st.GetPipeline(t.Context(), "mongo-hosts")
	if !ok {
		t.Fatal("pipeline not stored")
	}
	plain, err := e.srv.db.box.Open(row.EnvEnc, "mongo-hosts")
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	lines, _ := ParseEnvData(strings.NewReader(string(plain)))
	for _, l := range lines {
		k, v, _ := strings.Cut(l, "=")
		env[k] = v
	}
	wantSrc := "mongodb://strand:Src&Pw%401%3A%2Fx@m1:27017,m2:27018/?authSource=admin&replicaSet=rs0&tls=true"
	wantDst := "mongodb://writer:DstPw%232@t1:27030/?authSource=admin"
	if env["SRC_URI"] != wantSrc || env["DST_URI"] != wantDst {
		t.Fatalf("env URIs:\n SRC_URI=%q\n DST_URI=%q", env["SRC_URI"], env["DST_URI"])
	}
	cfg, err := config.LoadYAMLStrictWithEnv([]byte(row.ConfigYAML), env)
	if err != nil {
		t.Fatalf("stored config does not load: %v", err)
	}
	// Target database left empty: same name as the source database.
	if cfg.Source.DSN != wantSrc || cfg.Source.Database != "shop" || cfg.Target.Database != "shop" {
		t.Fatalf("loaded source %+v target %+v", cfg.Source, cfg.Target)
	}

	// No database chosen, a bad host: refused, and the error names the form field.
	for _, tc := range []struct {
		v     url.Values
		field string
	}{
		{url.Values{"source_database": {""}}, "source_database"},
		{url.Values{"source_host": {"m1/x"}}, "source_host"},
		{url.Values{"target_host": {""}}, "target_host"},
		{url.Values{"target_replica_set": {"rs 1"}}, "target_replica_set"},
	} {
		v := url.Values{"name": {"mongo-bad"}, "from": {"mongodb"}, "to": {"mongodb"}, "source_host": {"m1"}, "source_user": {"u"},
			"target_host": {"t1"}, "target_user": {"u"}, "source_database": {"shop"}}
		for k, x := range tc.v {
			v[k] = x
		}
		resp, body := e.do(c, "POST", "/api/v1/pipelines", v.Encode(),
			map[string]string{csrfHeader: csrf, "Content-Type": "application/x-www-form-urlencoded"})
		if resp.StatusCode != 400 || !strings.Contains(body, `"`+tc.field+`":`) {
			t.Errorf("%v: %d %s (want 400 on field %s)", tc.v, resp.StatusCode, body, tc.field)
		}
	}
}

// The console's JSON body: checkboxes arrive as booleans (overwrite_target used to arrive as the string "true",
// which the JSON decoder refused).
func TestNewPipelineMongoJSONBooleans(t *testing.T) {
	e, c, csrf := npEnv(t)
	body := `{"name":"mongo-json","from":"mongodb","to":"mongodb","source_host":"m1","source_port":27017,"source_user":"u",
		"source_password":"p","source_tls":true,"target_host":"t1","target_port":27017,"target_user":"u","target_password":"p",
		"source_database":"shop","overwrite_target":true}`
	resp, out := e.do(c, "POST", "/api/v1/pipelines", body, map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	code, _, y, _ := decodeNP(t, e, resp.StatusCode, out)
	if code != 200 || !strings.Contains(y, "overwrite_target: true") {
		t.Fatalf("create: %d %s\n%s", code, out, y)
	}
}

// Test connection and List databases take the same fields; the checker gets the built URI.
func TestMongoConnTestAndDatabaseListFromFields(t *testing.T) {
	st := store.NewMem()
	fake := &fakeChecker{result: conncheck.Result{OK: true, Version: "4.4.29", MissingPrivileges: []string{}, Databases: []string{"orders", "shop"}}}
	e := dbEnv(t, st, nil, func(o *Options) { o.ConnChecker = fake })
	c, csrf := e.loginOK()
	hdr := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}
	const pw = "conn-canary-mongo-77"
	resp, out := e.do(c, "POST", "/api/v1/pipeline-connections/test",
		`{"role":"source","engine":"mongodb","host":"m1,m2:27018","port":27017,"user":"strand","password":"`+pw+`","replica_set":"rs0","auth_db":"admin","tls":true}`, hdr)
	if resp.StatusCode != 200 || strings.Contains(out, pw) {
		t.Fatalf("conn test: %d %s", resp.StatusCode, out)
	}
	want := "mongodb://strand:" + pw + "@m1:27017,m2:27018/?authSource=admin&replicaSet=rs0&tls=true"
	if got := fake.last(); got.URI != want {
		t.Fatalf("checker URI %q, want %q", got.URI, want)
	}

	for _, tc := range []struct{ body, want string }{
		{`{"from":"mongodb","host":"m1","port":27017,"user":"strand","password":"` + pw + `","replica_set":"rs0"}`, `["orders","shop"]`},
		{`{"from":"postgres","host":"pg1","port":5432,"user":"strand","password":"` + pw + `"}`, `["orders","shop"]`},
	} {
		resp, out := e.do(c, "POST", "/api/v1/source/databases", tc.body, hdr)
		if resp.StatusCode != http.StatusOK || !strings.Contains(strings.Join(strings.Fields(out), ""), tc.want) || strings.Contains(out, pw) {
			t.Errorf("%s: %d %s", tc.body, resp.StatusCode, out)
		}
	}
	if got := fake.last(); got.Engine != "postgres" || got.Host != "pg1" || got.Password != pw {
		t.Errorf("postgres checker target %+v", got)
	}
	fake.mu.Lock()
	fake.result = conncheck.Result{OK: false, Error: "auth failed"}
	fake.mu.Unlock()
	if resp, out := e.do(c, "POST", "/api/v1/source/databases", `{"from":"mongodb","host":"m1","user":"u","password":"x"}`, hdr); resp.StatusCode != http.StatusBadGateway {
		t.Errorf("failed connection: %d %s", resp.StatusCode, out)
	}
}

// The form: correct MongoDB versions, host/port/user/password fields instead of a URI, no "app" default.
func TestNewPipelineFormMongoFields(t *testing.T) {
	e, c, _ := npEnv(t)
	resp, page := e.do(c, "GET", "/pipelines/new", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GET /pipelines/new: %d", resp.StatusCode)
	}
	if strings.Contains(page, "MongoDB 6+") || strings.Count(page, "MongoDB 4.4 – 8.0 (replica set)") != 2 {
		t.Error("MongoDB version label: want 4.4 – 8.0 (the minimum the sync checks), twice")
	}
	for _, gone := range []string{`name="source_uri"`, `name="target_uri"`, `value="app"`} {
		if strings.Contains(page, gone) {
			t.Errorf("form still has %s", gone)
		}
	}
	for _, want := range []string{`data-mongodb="27017"`, `name="source_replica_set"`, `name="source_auth_db"`, `name="source_tls"`,
		`name="target_replica_set"`, `name="target_auth_db"`, `name="target_tls"`, `data-list-databases="np-mongo-src-db"`,
		`data-list-databases="np-pg-database"`} {
		if !strings.Contains(page, want) {
			t.Errorf("form lacks %s", want)
		}
	}
}

// Browsers compile pattern="" with the RegExp v flag, where a bare "-" at the end of a character class is a
// syntax error: the input then throws on every check (it did on the New pipeline name and the user name).
func TestTemplatePatternsCompileWithVFlag(t *testing.T) {
	re := regexp.MustCompile(`pattern="([^"]*)"`)
	bad := regexp.MustCompile(`[^\\]-\]`)
	entries, err := os.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join("templates", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			n++
			if bad.MatchString(m[1]) {
				t.Errorf("%s: pattern %q has an unescaped '-' before ']' (invalid with the v flag)", e.Name(), m[1])
			}
		}
	}
	if n == 0 {
		t.Fatal("no pattern attributes found")
	}
}
