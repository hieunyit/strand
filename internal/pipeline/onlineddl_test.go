package pipeline

import (
	"testing"

	"strand/internal/config"
	"strand/internal/model"
)

func tablesFn(names ...string) func(string) []string {
	return func(schema string) []string {
		if schema != "app" {
			return nil
		}
		return names
	}
}

func TestOnlineDDLClassify(t *testing.T) {
	long := "a_very_long_table_name_that_is_close_to_the_sixty_four_char_limit" // 65? trimmed below
	long = long[:62]
	trunc := "_" + long[:55] + "_gho"
	o := newOnlineDDL(config.OnlineDDLConfig{})
	cases := []struct {
		name   string
		ddl    string
		tables []string
		kind   onlineKind
		old    string
		neu    string
	}{
		{"gho create like", "CREATE TABLE `app`.`_t_gho` LIKE `app`.`t`", []string{"t"}, onlineIgnore, "", ""},
		{"gho create comment", "create /* gh-ost */ table `app`.`_t_ghc` (id int, hint varchar(64)) engine=InnoDB", []string{"t"}, onlineIgnore, "", ""},
		{"gho alter", "alter /* gh-ost */ table `app`.`_t_gho` add column `c` int", []string{"t"}, onlineIgnore, "", ""},
		{"gho drop", "drop /* gh-ost */ table if exists `app`.`_t_ghc`", []string{"t"}, onlineIgnore, "", ""},
		{"drop list", "DROP TABLE IF EXISTS `_t_gho`, `_t_ghc`", []string{"t"}, onlineIgnore, "", ""},
		{"drop list with real table", "DROP TABLE `_t_gho`, `t`", []string{"t"}, onlineNone, "", ""},
		{"sentinel del", "create /* gh-ost */ table `app`.`_t_del` (id int primary key) engine=InnoDB comment='magic-cut-over'", []string{"t"}, onlineIgnore, "", ""},
		{"pt new create", "CREATE TABLE `app`.`_t_new` (\n id int\n)", []string{"t"}, onlineIgnore, "", ""},
		{"pt new alter schema default", "ALTER TABLE `_t_new` ADD COLUMN c int", []string{"t"}, onlineIgnore, "", ""},
		{"pt trigger", "CREATE TRIGGER `pt_osc_app_t_ins` AFTER INSERT ON `app`.`t` FOR EACH ROW REPLACE INTO `app`.`_t_new` (`id`) VALUES (NEW.`id`)", []string{"t"}, onlineIgnore, "", ""},
		{"pt trigger definer", "CREATE DEFINER=`root`@`%` TRIGGER `pt_osc_app_t_upd` AFTER UPDATE ON `app`.`t` FOR EACH ROW BEGIN END", []string{"t"}, onlineIgnore, "", ""},
		{"pt drop trigger", "DROP TRIGGER IF EXISTS `app`.`pt_osc_app_t_del`", []string{"t"}, onlineIgnore, "", ""},
		{"user trigger untouched", "CREATE TRIGGER mytrig AFTER INSERT ON t FOR EACH ROW BEGIN END", []string{"t"}, onlineNone, "", ""},
		{"gh-ost cutover", "rename /* gh-ost */ table `app`.`t` to `app`.`_t_del`, `app`.`_t_gho` to `app`.`t`", []string{"t"}, onlineCutover, "_t_del", "_t_gho"},
		{"gh-ost cutover unqualified", "RENAME TABLE t TO _t_del, _t_gho TO t", []string{"t"}, onlineCutover, "_t_del", "_t_gho"},
		{"gh-ost timestamp old", "RENAME TABLE t TO _t_20260101120000_del, _t_gho TO t", []string{"t"}, onlineCutover, "_t_20260101120000_del", "_t_gho"},
		{"pt cutover", "RENAME TABLE `app`.`t` TO `app`.`_t_old`, `app`.`_t_new` TO `app`.`t`", []string{"t"}, onlineCutover, "_t_old", "_t_new"},
		{"pt cutover spaced dots", "RENAME TABLE app . t TO app . _t_old , app . _t_new TO app . t", []string{"t"}, onlineCutover, "_t_old", "_t_new"},
		{"cutover wrong order", "RENAME TABLE _t_gho TO t, t TO _t_del", []string{"t"}, onlineNone, "", ""},
		{"cutover other base", "RENAME TABLE t TO _t_del, _u_gho TO t", []string{"t", "u"}, onlineNone, "", ""},
		{"cutover cross schema", "RENAME TABLE app.t TO app.t_del, other._t_gho TO app.t", []string{"t"}, onlineNone, "", ""},
		{"plain rename", "RENAME TABLE t TO t2", []string{"t"}, onlineNone, "", ""},
		{"abandoned gho rename", "RENAME TABLE _t_gho TO t", []string{"t"}, onlineNone, "", ""},
		{"temp-looking but no base", "CREATE TABLE `_ghost_gho` (id int)", []string{"t"}, onlineNone, "", ""},
		{"real alter", "ALTER TABLE t ADD COLUMN c int", []string{"t"}, onlineNone, "", ""},
		{"truncated gho", "CREATE TABLE `" + trunc + "` LIKE `" + long + "`", []string{long}, onlineIgnore, "", ""},
		{"truncated cutover", "RENAME TABLE `" + long + "` TO `_" + long[:55] + "_del`, `" + trunc + "` TO `" + long + "`", []string{long}, onlineCutover, "_" + long[:55] + "_del", trunc},
	}
	for _, c := range cases {
		got := o.classify(c.ddl, "app", tablesFn(c.tables...))
		if got.Kind != c.kind {
			t.Errorf("%s: kind = %d, want %d (%+v)", c.name, got.Kind, c.kind, got)
			continue
		}
		if c.kind == onlineCutover && (got.OldName.Table != c.old || got.NewName.Table != c.neu || got.Base.Table != c.tables[0] || got.Base.Schema != "app") {
			t.Errorf("%s: got %+v, want old=%s new=%s", c.name, got, c.old, c.neu)
		}
	}
}

func TestOnlineDDLToolsAndExtraPatterns(t *testing.T) {
	ghOnly := newOnlineDDL(config.OnlineDDLConfig{Tools: []string{"gh-ost"}})
	if got := ghOnly.classify("CREATE TABLE _t_new (id int)", "app", tablesFn("t")); got.Kind != onlineNone {
		t.Fatalf("pt-osc table recognised with only gh-ost enabled: %+v", got)
	}
	if got := ghOnly.classify("CREATE TABLE _t_gho (id int)", "app", tablesFn("t")); got.Kind != onlineIgnore {
		t.Fatalf("gh-ost table not recognised: %+v", got)
	}
	if got := ghOnly.classify("CREATE TRIGGER pt_osc_app_t_ins AFTER INSERT ON t FOR EACH ROW BEGIN END", "app", tablesFn("t")); got.Kind != onlineNone {
		t.Fatalf("pt-osc trigger recognised with only gh-ost: %+v", got)
	}
	if newOnlineDDL(config.OnlineDDLConfig{Tools: []string{}}) != nil {
		t.Fatal("tools: [] must disable recognition")
	}
	var off *onlineDDL
	if got := off.classify("CREATE TABLE _t_gho (id int)", "app", tablesFn("t")); got.Kind != onlineNone {
		t.Fatalf("disabled matcher recognised: %+v", got)
	}
	ex := newOnlineDDL(config.OnlineDDLConfig{Tools: []string{}, ExtraPatterns: []string{"_{table}_tmp"}})
	if got := ex.classify("CREATE TABLE _t_tmp LIKE t", "app", tablesFn("t")); got.Kind != onlineIgnore {
		t.Fatalf("extra pattern not recognised: %+v", got)
	}
	if got := ex.classify("RENAME TABLE t TO t_x, _t_tmp TO t", "app", tablesFn("t")); got.Kind != onlineNone {
		t.Fatalf("non-temp old name accepted as cutover: %+v", got)
	}
	if got := ex.classify("RENAME TABLE t TO _t_tmp_bak, _t_tmp TO t", "app", tablesFn("t")); got.Kind != onlineNone {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestLoadTablesSkipsOnlineDDLTempTablesPattern(t *testing.T) {
	o := newOnlineDDL(config.OnlineDDLConfig{})
	names := []string{"t", "_t_gho", "_t_ghc", "_t_new", "_orphan_gho", "_t_old"}
	var kept []string
	for _, n := range names {
		if _, _, temp := o.tempOf(n, names); !temp {
			kept = append(kept, n)
		}
	}
	if len(kept) != 2 || kept[0] != "t" || kept[1] != "_orphan_gho" {
		t.Fatalf("kept = %v", kept)
	}
	_ = model.TableID{}
}
