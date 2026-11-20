package backup

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestBuiltinPoliciesValidAndMatchSpec(t *testing.T) {
	ps := BuiltinPolicies()
	if len(ps) != 3 {
		t.Fatalf("%d builtins", len(ps))
	}
	for _, p := range ps {
		if err := p.Validate(); err != nil {
			t.Fatalf("%s: %v", p.Name, err)
		}
	}
	g, s, b := ps[0], ps[1], ps[2]
	if g.Name != "Gold" || !g.LogArchive || g.RPOMinutes != 5 || g.LogFlushSeconds != 300 || g.Retention.KeepDaily != 14 ||
		g.Retention.KeepWeekly != 8 || g.Retention.KeepMonthly != 12 || g.Schedule.At != "01:00" || !g.TestRestore {
		t.Fatalf("gold = %+v", g)
	}
	if s.LogArchive || s.Retention.KeepDaily != 7 || s.Retention.KeepWeekly != 4 || s.Schedule.Kind != ScheduleDaily {
		t.Fatalf("silver = %+v", s)
	}
	if b.Schedule.Kind != ScheduleWeekly || b.Schedule.Days[0] != 0 || b.Schedule.At != "02:00" || b.Retention.KeepWeekly != 4 || b.LogArchive {
		t.Fatalf("bronze = %+v", b)
	}
}

func mysqlJob(dbs ...string) Job {
	return Job{ID: "j1", Name: "j", EverySeconds: 3600, JobOptions: JobOptions{Method: MethodMySQLDump, Source: "s", Repository: "r1",
		Conn: ConnSpec{Host: "h", Databases: dbs}}}
}

func TestApplyPolicyFollowsAndOverrides(t *testing.T) {
	gold := BuiltinPolicies()[0]
	j, warn := ApplyPolicy(mysqlJob("app"), gold)
	if !j.LogArchive || j.Retention.KeepMonthly != 12 || j.Schedule == nil || j.Schedule.Kind != ScheduleDaily || j.PolicyID != gold.ID {
		t.Fatalf("applied = %+v", j)
	}
	if j.EverySeconds != 86400 {
		t.Fatalf("every_seconds = %d, want nominal 86400", j.EverySeconds)
	}
	// test_restore needs a scratch connection: switched off with a warning
	if j.TestRestore || len(warn) != 1 || !strings.Contains(warn[0], "test-restore") {
		t.Fatalf("warn=%v testrestore=%v", warn, j.TestRestore)
	}
	if err := ValidateJobForSave(j); err != nil {
		t.Fatal(err)
	}
	// An override keeps the job's value across later policy edits.
	j.Overrides = []string{"retention"}
	j.Retention = RetentionPolicy{KeepDaily: 3}
	gold.Retention = RetentionPolicy{KeepDaily: 99}
	gold.RPOMinutes = 1
	j2, _ := ApplyPolicy(j, gold)
	if j2.Retention.KeepDaily != 3 {
		t.Fatalf("override lost: %+v", j2.Retention)
	}
	j.Overrides = nil
	j3, _ := ApplyPolicy(j, gold)
	if j3.Retention.KeepDaily != 99 {
		t.Fatalf("not following the policy: %+v", j3.Retention)
	}
}

func TestApplyPolicyDisablesUnsupportedPITR(t *testing.T) {
	gold := BuiltinPolicies()[0]
	for name, j := range map[string]Job{
		"mysql two databases": mysqlJob("a", "b"),
		"mariadb":             func() Job { j := mysqlJob("a"); j.Method = MethodMariaDBDump; return j }(),
		"pg_dump":             func() Job { j := mysqlJob("a"); j.Method = MethodPGDump; return j }(),
	} {
		out, warn := ApplyPolicy(j, gold)
		if out.LogArchive || len(warn) == 0 {
			t.Fatalf("%s: log_archive=%v warn=%v", name, out.LogArchive, warn)
		}
		if err := ValidateJobForSave(out); err != nil {
			t.Fatalf("%s: stored job must stay valid: %v", name, err)
		}
	}
	// ... but an explicit override of an unsupported setting is rejected on save.
	j := func() Job {
		j := mysqlJob("a")
		j.Method = MethodMariaDBDump
		j.Overrides = []string{"log_archive"}
		j.LogArchive = true
		return j
	}()
	out, _ := ApplyPolicy(j, BuiltinPolicies()[1])
	if err := ValidateJobForSave(out); !errors.Is(err, ErrInvalid) {
		t.Fatalf("overridden unsupported PITR accepted: %v", err)
	}
}

func TestDeriveOverrides(t *testing.T) {
	silver := BuiltinPolicies()[1]
	j, _ := ApplyPolicy(mysqlJob("a"), silver)
	j.Retention.KeepDaily = 30
	j.Compress = silver.Compress
	all := func(string) bool { return true }
	got := DeriveOverrides(j, silver, all)
	if len(got) != 1 || got[0] != "retention" {
		t.Fatalf("overrides = %v", got)
	}
	// repository is not managed by a policy without one: never an override
	for _, f := range DeriveOverrides(j, silver, all) {
		if f == "repository" {
			t.Fatal("unmanaged field reported")
		}
	}
	if got := DeriveOverrides(j, silver, func(f string) bool { return f == "compress" }); len(got) != 0 {
		t.Fatalf("absent fields reported: %v", got)
	}
}

func TestNominalSeconds(t *testing.T) {
	for s, want := range map[string]int{"0 * * * *": 3600, "*/5 * * * *": 300, "0 1 * * *": 86400} {
		if got := (Schedule{Kind: ScheduleCron, Cron: s}).NominalSeconds(); got != want {
			t.Fatalf("%s = %d, want %d", s, got, want)
		}
	}
	if got := (Schedule{Kind: ScheduleWeekly, At: "02:00", Days: []int{0}}).NominalSeconds(); got != 7*86400 {
		t.Fatalf("weekly = %d", got)
	}
}

func TestPITRAssessmentMatchesValidatePITR(t *testing.T) {
	ok, methods, _ := PITRAssessment(ConnMySQL, PITRCaps{GTIDOn: true, BinlogOn: true})
	if !ok || len(methods) != 1 || methods[0] != MethodMySQLDump {
		t.Fatalf("mysql gtid: %v %v", ok, methods)
	}
	if ok, _, r := PITRAssessment(ConnMySQL, PITRCaps{GTIDOn: true}); ok || !strings.Contains(r, "log_bin") {
		t.Fatalf("mysql no binlog: %v %q", ok, r)
	}
	if ok, _, r := PITRAssessment(ConnMySQL, PITRCaps{}); ok || !strings.Contains(r, "gtid_mode") {
		t.Fatalf("mysql no gtid: %v %q", ok, r)
	}
	if ok, _, r := PITRAssessment(ConnMariaDB, PITRCaps{GTIDOn: true}); ok || !strings.Contains(r, "MariaDB does not support") {
		t.Fatalf("mariadb: %v %q", ok, r)
	}
	ok, methods, _ = PITRAssessment(ConnPostgres, PITRCaps{WALLevelOK: true, ReplicationRole: true})
	if !ok || len(methods) != 1 || methods[0] != MethodPGBaseBackup {
		t.Fatalf("pg: %v %v", ok, methods)
	}
	if ok, _, r := PITRAssessment(ConnPostgres, PITRCaps{WALLevelOK: false, ReplicationRole: true}); ok || !strings.Contains(r, "wal_level") {
		t.Fatalf("pg wal_level: %v %q", ok, r)
	}
	if ok, _, r := PITRAssessment(ConnPostgres, PITRCaps{WALLevelOK: true}); ok || !strings.Contains(r, "REPLICATION") {
		t.Fatalf("pg role: %v %q", ok, r)
	}
	if ok, _, r := PITRAssessment(ConnMongoDB, PITRCaps{}); ok || !strings.Contains(r, "replica set") {
		t.Fatalf("mongo: %v %q", ok, r)
	}
	if ok, _, _ := PITRAssessment(ConnMongoDB, PITRCaps{ReplicaSet: true}); !ok {
		t.Fatal("mongo rs")
	}
}

func TestApplyConnection(t *testing.T) {
	c := Connection{ID: "c1", Name: "prod", Engine: ConnMySQL, Host: "db", User: "bk", SecretMode: SecretStored, PasswordSet: true}
	j, err := ApplyConnection(mysqlJob("app"), c)
	if err != nil || j.Conn.Host != "db" || j.Conn.Port != 3306 || j.Conn.PasswordEnv != EnvConnPassword || j.Conn.Databases[0] != "app" {
		t.Fatalf("%+v %v", j.Conn, err)
	}
	if err := ValidateJob(j); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyConnection(mysqlJob("a"), Connection{Name: "pg", Engine: ConnPostgres, Host: "h", SecretMode: SecretEnv}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("engine mismatch accepted: %v", err)
	}
	c2 := Connection{ID: "c2", Name: "m", Engine: ConnMariaDB, Host: "h", SecretMode: SecretEnv, PasswordEnv: "DB_PW"}
	jm := mysqlJob("a")
	jm.Method = MethodMariaDBDump
	if j, err = ApplyConnection(jm, c2); err != nil || j.Conn.PasswordEnv != "DB_PW" {
		t.Fatalf("mariadb conn: %+v %v", j.Conn, err)
	}
}

func TestConnectionValidate(t *testing.T) {
	ok := Connection{ID: "c1", Name: "x", Engine: ConnPostgres, Host: "h", SecretMode: SecretEnv}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*Connection){
		"engine":        func(c *Connection) { c.Engine = "oracle" },
		"mode":          func(c *Connection) { c.SecretMode = "vault" },
		"reserved env":  func(c *Connection) { c.PasswordEnv = "STRAND_WEB_SECRET_KEY" },
		"sealed in env": func(c *Connection) { c.PasswordSealed = []byte("x") },
		"stored + env":  func(c *Connection) { c.SecretMode = SecretStored; c.PasswordEnv = "X" },
		"tls":           func(c *Connection) { c.TLSMode = "maybe" },
		"host":          func(c *Connection) { c.Host = "-oProxyCommand=x" },
		"tunnel path":   func(c *Connection) { c.SSHTunnel = &SSHTunnel{Host: "b", User: "u", IdentityRef: "rel/key"} },
		"id":            func(c *Connection) { c.ID = "a b" },
	} {
		c := ok
		mut(&c)
		if err := c.Validate(); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s accepted: %v", name, err)
		}
	}
}

func TestConnectionNeverSerializesSecrets(t *testing.T) {
	c := Connection{ID: "c", Name: "n", Engine: ConnMySQL, SecretMode: SecretStored, PasswordSealed: []byte("SEALED-BYTES"), PasswordSet: true}
	b, _ := jsonMarshal(c)
	if strings.Contains(string(b), "SEALED") || strings.Contains(strings.ToLower(string(b)), "sealed") || !strings.Contains(string(b), `"password_set":true`) {
		t.Fatalf("json = %s", b)
	}
}

func TestJobValidateScheduleFields(t *testing.T) {
	j := mysqlJob("a")
	j.WindowMinutes = -1
	if err := ValidateJob(j); !errors.Is(err, ErrInvalid) {
		t.Fatal("negative window accepted")
	}
	j = mysqlJob("a")
	j.JitterSeconds = 4000
	if err := ValidateJob(j); !errors.Is(err, ErrInvalid) {
		t.Fatal("huge jitter accepted")
	}
	j = mysqlJob("a")
	j.Overrides = []string{"nope"}
	if err := ValidateJob(j); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown override accepted")
	}
	j = mysqlJob("a")
	j.Schedule = &Schedule{Kind: ScheduleCron, Cron: "bad"}
	if err := ValidateJob(j); !errors.Is(err, ErrInvalid) {
		t.Fatal("bad schedule accepted")
	}
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
