package validate

import (
	"strings"
	"testing"

	"strand/internal/findings"
)

func TestParseGrantsMySQLAndMariaDB(t *testing.T) {
	g := ParseGrants([]string{
		"GRANT USAGE ON *.* TO `u`@`%`",
		"GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO `u`@`%` IDENTIFIED BY PASSWORD '*ABC TO X'",
		"GRANT SELECT, LOCK TABLES ON `app\\_db`.* TO `u`@`%`",
		"GRANT SELECT (`a`, `b`), INSERT ON `other`.`t1` TO `u`@`%`",
		"GRANT ALL PRIVILEGES ON `meta`.* TO `u`@`%` WITH GRANT OPTION",
		"GRANT EXECUTE ON PROCEDURE `app_db`.`p` TO `u`@`%`",
	})
	checks := []struct {
		ok   bool
		desc string
	}{
		{g.HasGlobal("REPLICATION SLAVE"), "global REPLICATION SLAVE"},
		{g.HasGlobal("REPLICATION CLIENT"), "BINLOG MONITOR is REPLICATION CLIENT"},
		{!g.HasGlobal("RELOAD"), "no RELOAD"},
		{g.HasSchema("SELECT", "app_db"), "escaped underscore matches literally"},
		{!g.HasSchema("SELECT", "appXdb"), "escaped underscore is not a wildcard"},
		{g.HasSchema("LOCK TABLES", "APP_DB"), "schema names compare case-insensitively"},
		{g.HasTable("INSERT", "other", "t1"), "table-level INSERT"},
		{!g.HasTable("SELECT", "other", "t1"), "column-level SELECT is not table SELECT"},
		{g.HasSchema("CREATE", "meta"), "ALL PRIVILEGES on a schema"},
		{!g.HasRoles, "no roles"},
	}
	for _, c := range checks {
		if !c.ok {
			t.Error(c.desc)
		}
	}
	wild := ParseGrants([]string{"GRANT SELECT ON `app%`.* TO `u`@`%`", "GRANT `reader`@`%` TO `u`@`%`"})
	if !wild.HasSchema("SELECT", "app_eu") || wild.HasSchema("SELECT", "billing") || !wild.HasRoles {
		t.Error("wildcard schema grant or role detection")
	}
	all := ParseGrants([]string{"GRANT ALL PRIVILEGES ON *.* TO `root`@`%`"})
	if !all.HasTable("DELETE", "x", "y") || !all.HasGlobal("RELOAD") {
		t.Error("ALL PRIVILEGES ON *.* grants everything")
	}
}

func TestReportPrivilegesGroupsMissingGrants(t *testing.T) {
	g := ParseGrants([]string{"GRANT SELECT ON `app`.* TO `u`@`%`", "GRANT SELECT ON `b`.`t` TO `u`@`%`"})
	g.User = quoteAccount("u@%")
	c := &findings.Collector{}
	ReportPrivileges(g, "source", []PrivilegeNeed{
		{Priv: "REPLICATION SLAVE", Why: "binlog", Severity: findings.Error},
		{Priv: "REPLICATION CLIENT", Display: "BINLOG MONITOR", Why: "position", Severity: findings.Error},
		{Priv: "SELECT", Schema: "app", Why: "snapshot", Severity: findings.Error},
		{Priv: "LOCK TABLES", Schema: "app", Why: "snapshot", Severity: findings.Error},
		{Priv: "SELECT", Schema: "b", Why: "snapshot", Severity: findings.Error}, // every table in scope granted
		{Priv: "PROCESS", Why: "diagnostics", Severity: findings.Warning},
	}, map[string][]string{"b": {"t"}}, c)
	var fixes []string
	for _, f := range c.Findings() {
		if f.Code != "source_privilege_missing" {
			t.Errorf("code %s", f.Code)
		}
		fixes = append(fixes, string(f.Severity)+" "+f.Fix)
	}
	want := []string{
		"error GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO 'u'@'%';",
		"error GRANT LOCK TABLES ON `app`.* TO 'u'@'%';",
		"warning GRANT PROCESS ON *.* TO 'u'@'%';",
	}
	if strings.Join(fixes, "\n") != strings.Join(want, "\n") {
		t.Fatalf("fixes:\n%s\nwant:\n%s", strings.Join(fixes, "\n"), strings.Join(want, "\n"))
	}

	// Privileges that may come from a role are not proven missing: warning only.
	g.HasRoles = true
	c = &findings.Collector{}
	ReportPrivileges(g, "target", []PrivilegeNeed{{Priv: "INSERT", Schema: "app", Why: "apply", Severity: findings.Error}}, nil, c)
	if f := c.Findings(); len(f) != 1 || f[0].Severity != findings.Warning || f[0].Code != "target_privilege_missing" {
		t.Fatalf("role case: %+v", f)
	}
}
