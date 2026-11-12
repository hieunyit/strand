package objects

import (
	"strings"
	"testing"
)

func TestConvertCreate_DefinerDroppedOrKept(t *testing.T) {
	src := "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` AS select `app`.`acct`.`code` AS `code` from `app`.`acct`"
	got, _, err := convertCreate(src, KindView, "app", "v", false, nil, "app")
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE ALGORITHM=UNDEFINED SQL SECURITY DEFINER VIEW `app`.`v` AS select `app`.`acct`.`code` AS `code` from `app`.`acct`"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	kept, _, _ := convertCreate(src, KindView, "app", "v", true, nil, "app")
	if !strings.Contains(kept, "DEFINER=`root`@`localhost`") {
		t.Fatalf("--keep-definer mất DEFINER: %s", kept)
	}
	// các dạng DEFINER khác
	for _, d := range []string{"DEFINER=CURRENT_USER", "DEFINER=CURRENT_USER()", "DEFINER=`role`", "DEFINER='u'@'%'", "DEFINER=root@localhost"} {
		s := "CREATE " + d + " PROCEDURE `p`() BEGIN SELECT 1; END"
		g, _, err := convertCreate(s, KindProcedure, "app", "p", false, nil, "app")
		if err != nil || g != "CREATE PROCEDURE `app`.`p`() BEGIN SELECT 1; END" {
			t.Fatalf("%s → %q (%v)", d, g, err)
		}
	}
}

func TestConvertCreate_RenameQualified(t *testing.T) {
	rename := map[string]string{"app.acct": "app2.account", "app.old": "app.new"}
	src := "CREATE DEFINER=`r`@`%` FUNCTION `f`(x int) RETURNS int BEGIN DECLARE n INT; SELECT COUNT(*) INTO n FROM `app`.`acct` a JOIN app.old o ON a.id=o.id WHERE a.`acct`=1; SELECT 'app.acct' INTO @s; RETURN n; END"
	got, _, err := convertCreate(src, KindFunction, "app", "f", false, rename, "app")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"FROM `app2`.`account` a", "JOIN `app`.`new` o", "'app.acct'", "a.`acct`=1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("thiếu %q trong %s", want, got)
		}
	}
	// tên bảng rename không kèm schema: không sửa được → cảnh báo
	_, w, _ := convertCreate("CREATE PROCEDURE `p`() UPDATE old SET a=1", KindProcedure, "app", "p", false, rename, "app")
	if len(w) != 1 || !strings.Contains(w[0], "without a schema") {
		t.Fatalf("cảnh báo rename trần: %v", w)
	}
}

func TestConvertTrigger_RenameTable(t *testing.T) {
	src := "CREATE DEFINER=`root`@`%` TRIGGER `trg` AFTER INSERT ON `acct` FOR EACH ROW INSERT INTO `app`.`audit` VALUES (NEW.code)"
	got, _, err := convertTrigger(src, "app2", "trg", "account", false, map[string]string{"app.audit": "app2.audit_log"}, "app")
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE TRIGGER `app2`.`trg` AFTER INSERT ON `account` FOR EACH ROW INSERT INTO `app2`.`audit_log` VALUES (NEW.code)"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestEventDisabled(t *testing.T) {
	cases := map[string]string{
		"CREATE EVENT `e` ON SCHEDULE EVERY 1 DAY ON COMPLETION NOT PRESERVE ENABLE DO UPDATE t SET a='ENABLE'": "ON COMPLETION NOT PRESERVE DISABLE DO UPDATE t SET a='ENABLE'",
		"CREATE EVENT `e` ON SCHEDULE EVERY 1 DAY DISABLE ON SLAVE COMMENT 'x' DO SELECT 1":                     "DAY DISABLE COMMENT 'x' DO SELECT 1",
		"CREATE EVENT `e` ON SCHEDULE EVERY 1 DAY COMMENT 'c' DO SELECT 1":                                      "DAY DISABLE COMMENT 'c' DO SELECT 1",
		"CREATE EVENT `e` ON SCHEDULE EVERY 1 DAY DO SELECT 1":                                                  "DAY DISABLE DO SELECT 1",
	}
	for in, want := range cases {
		got, err := eventDisabled(in)
		if err != nil || !strings.Contains(got, want) {
			t.Fatalf("%s\n got %q want contains %q (%v)", in, got, want, err)
		}
	}
}

func TestCanonical_StatusAndDefinerAndQualifierIgnored(t *testing.T) {
	a := "CREATE DEFINER=`root`@`%` EVENT `app`.`e` ON SCHEDULE EVERY 1 DAY ENABLE DO\n SELECT 1"
	b := "CREATE EVENT `e` ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1"
	if Canonical(a, KindEvent) != Canonical(b, KindEvent) {
		t.Fatalf("%q != %q", Canonical(a, KindEvent), Canonical(b, KindEvent))
	}
	c := "CREATE DEFINER=`x`@`y` TRIGGER `app`.`t` BEFORE INSERT ON `app`.`acct` FOR EACH ROW SET NEW.a = 1"
	d := "CREATE TRIGGER `t` BEFORE INSERT ON `acct` FOR EACH ROW SET NEW.a = 1"
	if Canonical(c, KindTrigger) != Canonical(d, KindTrigger) {
		t.Fatalf("%q != %q", Canonical(c, KindTrigger), Canonical(d, KindTrigger))
	}
	if Canonical(c, KindTrigger) == Canonical(d+"2", KindTrigger) {
		t.Fatal("thân khác nhau mà canonical bằng nhau")
	}
}

func TestDetectMariaDBOnly(t *testing.T) {
	cases := []struct {
		stmt, mode, want string
		kind             Kind
	}{
		{"CREATE PROCEDURE p() BEGIN NOT ATOMIC SELECT 1; END", "", "BEGIN NOT ATOMIC", KindProcedure},
		{"CREATE FUNCTION f() RETURNS int RETURN NEXTVAL(s)", "", "NEXTVAL", KindFunction},
		{"CREATE FUNCTION f() RETURNS int RETURN (SELECT NEXT VALUE FOR s)", "", "NEXT VALUE FOR", KindFunction},
		{"CREATE PROCEDURE p() DELETE FROM t RETURNING id", "", "RETURNING", KindProcedure},
		{"CREATE OR REPLACE PROCEDURE p() SELECT 1", "", "CREATE OR REPLACE", KindProcedure},
		{"CREATE PROCEDURE p() SELECT 1", "PIPES_AS_CONCAT,ORACLE", "ORACLE", KindProcedure},
		{"CREATE PACKAGE pk AS PROCEDURE a(); END", "", "PACKAGE", KindProcedure},
		{"CREATE AGGREGATE FUNCTION f() RETURNS INT BEGIN END", "", "AGGREGATE", KindFunction},
		{"CREATE PROCEDURE p() SELECT NVL(a, 1) FROM t", "", "NVL()", KindProcedure},
		{"CREATE PROCEDURE p() BEGIN DECLARE a INET6; END", "", "INET6", KindProcedure},
		{"CREATE PROCEDURE p() /*M!100100 SELECT 1 */ SELECT 2", "", "versioned comment", KindProcedure},
	}
	for _, c := range cases {
		w := DetectMariaDBOnly(c.stmt, c.kind, c.mode)
		if len(w) == 0 || !strings.Contains(strings.Join(w, "|"), c.want) {
			t.Errorf("%q: cảnh báo %v, muốn chứa %q", c.stmt, w, c.want)
		}
	}
	// Không báo oan: từ khóa trong chuỗi/identifier/comment thường, view hợp lệ với CREATE OR REPLACE.
	for _, s := range []string{
		"CREATE PROCEDURE p() SELECT 'BEGIN NOT ATOMIC RETURNING NEXTVAL(x)' -- RETURNING",
		"CREATE PROCEDURE p() SELECT `returning`, uuid() FROM t /* nextval(x) */",
		"CREATE PROCEDURE p() BEGIN SELECT 1; END",
	} {
		if w := DetectMariaDBOnly(s, KindProcedure, "STRICT_TRANS_TABLES"); len(w) != 0 {
			t.Errorf("báo oan %q: %v", s, w)
		}
	}
	if w := DetectMariaDBOnly("CREATE OR REPLACE VIEW v AS SELECT 1", KindView, ""); len(w) != 0 {
		t.Errorf("CREATE OR REPLACE VIEW hợp lệ ở MySQL: %v", w)
	}
}

func TestOrderViews_DependenciesFirst(t *testing.T) {
	views := map[string]string{
		"app.c": "CREATE VIEW `c` AS select * from `app`.`b`",
		"app.b": "CREATE VIEW `b` AS select x from `app`.`a` join app.t on 1",
		"app.a": "CREATE VIEW `a` AS select 1 AS x from `app`.`t`",
		"app.d": "CREATE VIEW `d` AS select * from a", // tên trần cùng schema
	}
	set := map[string]bool{}
	for k := range views {
		set[k] = true
	}
	var objs []Object
	for k, v := range views {
		s, n, _ := strings.Cut(k, ".")
		objs = append(objs, Object{Kind: KindView, Schema: s, Name: n, Deps: viewDeps(v, s, set)})
	}
	got, cyc := OrderViews(objs)
	if len(cyc) != 0 {
		t.Fatalf("vòng: %v", cyc)
	}
	pos := map[string]int{}
	for i, o := range got {
		pos[o.Name] = i
	}
	if !(pos["a"] < pos["b"] && pos["b"] < pos["c"] && pos["a"] < pos["d"]) {
		t.Fatalf("thứ tự sai: %v", pos)
	}
}

func TestOrderViews_CycleDoesNotLoop(t *testing.T) {
	objs := []Object{{Kind: KindView, Schema: "s", Name: "a", Deps: []string{"s.b"}}, {Kind: KindView, Schema: "s", Name: "b", Deps: []string{"s.a"}}}
	got, cyc := OrderViews(objs)
	if len(got) != 2 || len(cyc) == 0 {
		t.Fatalf("got=%v cyc=%v", got, cyc)
	}
}

func TestFilterSQLMode(t *testing.T) {
	k, d := FilterSQLMode("STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,ORACLE,NO_ENGINE_SUBSTITUTION")
	if k != "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION" || strings.Join(d, ",") != "ORACLE" {
		t.Fatalf("kept=%q dropped=%v", k, d)
	}
}

func TestMapPrivilege(t *testing.T) {
	for in, want := range map[string]string{
		"BINLOG MONITOR": "REPLICATION CLIENT", "slave monitor": "REPLICATION CLIENT", "REPLICA MONITOR": "REPLICATION CLIENT",
		"SELECT": "SELECT", "BINLOG ADMIN": "BINLOG_ADMIN", "SET USER": "SET_USER_ID", "CREATE  VIEW": "CREATE VIEW",
	} {
		m, _, ok := MapPrivilege(in)
		if !ok || m != want {
			t.Errorf("%s → %q ok=%v, want %q", in, m, ok, want)
		}
	}
	for _, in := range []string{"READ_ONLY ADMIN", "DELETE HISTORY", "FEDERATED ADMIN", "SOMETHING NEW"} {
		if m, note, ok := MapPrivilege(in); ok || m != "" || note == "" {
			t.Errorf("%s phải bị bỏ kèm ghi chú: %q %q %v", in, m, note, ok)
		}
	}
}

func TestParseGrant(t *testing.T) {
	g, ok := parseGrant("GRANT SELECT, INSERT (a, b), DELETE HISTORY ON `app`.`acct` TO `u`@`%` WITH GRANT OPTION")
	if !ok || g.global || g.schema != "app" || g.object != "acct" || !g.withGrant || len(g.privs) != 3 || g.privs[1].cols != "(a, b)" || g.privs[2].name != "DELETE HISTORY" {
		t.Fatalf("%+v ok=%v", g, ok)
	}
	g, ok = parseGrant("GRANT USAGE ON *.* TO `u`@`%` IDENTIFIED BY PASSWORD '*ABC'")
	if !ok || !g.global {
		t.Fatalf("%+v", g)
	}
	g, ok = parseGrant("GRANT EXECUTE ON PROCEDURE `app`.`p` TO `u`@`%`")
	if !ok || g.objType != "PROCEDURE" || g.object != "p" {
		t.Fatalf("%+v", g)
	}
	g, ok = parseGrant("GRANT `app_ro`, `x` TO `u`@`%` WITH ADMIN OPTION")
	if !ok || !g.roleGrant || len(g.roles) != 2 || g.roles[0] != "app_ro" || !g.adminOpt {
		t.Fatalf("%+v", g)
	}
	g, ok = parseGrant("GRANT ALL PRIVILEGES ON `my\\_db`.* TO `u`@`%`")
	if !ok || g.schema != `my\_db` {
		t.Fatalf("%+v", g)
	}
	if !schemaPatternRe(`my\_db`).MatchString("my_db") || schemaPatternRe(`my\_db`).MatchString("myxdb") || !schemaPatternRe("app%").MatchString("app2") {
		t.Fatal("schemaPatternRe")
	}
}

func TestSplitAccount(t *testing.T) {
	if u, h := splitAccount("'a''b'@'10.%'"); u != "a'b" || h != "10.%" {
		t.Fatalf("%q %q", u, h)
	}
	if u, h := splitAccount("`role`"); u != "role" || h != "%" {
		t.Fatalf("%q %q", u, h)
	}
}

func TestExport_FilesAndDelimiter(t *testing.T) {
	set := &Set{Objects: []Object{
		{Kind: KindProcedure, Schema: "app", Name: "p", TargetSchema: "app", TargetName: "p", Stmt: "CREATE PROCEDURE `app`.`p`() BEGIN SELECT 1; END", Warnings: []string{"x"}},
		{Kind: KindTrigger, Schema: "app", Name: "t", TargetSchema: "app", TargetName: "t", Stmt: "CREATE TRIGGER `app`.`t` BEFORE INSERT ON `acct` FOR EACH ROW SET NEW.a=';;'"},
		{Kind: KindEvent, Schema: "app", Name: "e", TargetSchema: "app", TargetName: "e", Stmt: "CREATE EVENT `app`.`e` ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1", EnableStmt: "ALTER EVENT `app`.`e` ENABLE"},
	}, Users: &Users{Items: []UserItem{{Account: "'u'@'%'", Stmts: []string{"CREATE USER IF NOT EXISTS 'u'@'%'"}}}}}
	dir := t.TempDir()
	if _, err := Export(set, dir, true); err != nil {
		t.Fatal(err)
	}
	read := func(n string) string {
		b, err := readFile(dir, n)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	if s := read(FileViewsRoutines); !strings.Contains(s, "DELIMITER ;;") || !strings.Contains(s, "END\n;;") || !strings.Contains(s, "-- WARNING: x") {
		t.Fatalf("%s", s)
	}
	if s := read(FileTriggers); !strings.Contains(s, "DELIMITER $$") { // thân có ';;' nên đổi dấu kết thúc
		t.Fatalf("%s", s)
	}
	if s := read(FileEventsOn); !strings.Contains(s, "ALTER EVENT `app`.`e` ENABLE") {
		t.Fatalf("%s", s)
	}
	if s := read(FileUsers); !strings.Contains(s, "mysql_native_password=ON") {
		t.Fatalf("%s", s)
	}
	m, err := LoadManifest(dir)
	if err != nil || len(m.Objects) != 3 {
		t.Fatalf("%v %v", m, err)
	}
}
