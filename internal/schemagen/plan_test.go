package schemagen

import (
	"strings"
	"testing"

	"strand/internal/model"
)

func TestMySQLDDLQuotedRenamePartitionAndCounter(t *testing.T) {
	src := model.TableID{Schema: "app", Table: "child"}
	dst := model.TableID{Schema: "Dst`DB", Table: "Kid"}
	input := "CREATE TABLE `child` (\n `id` bigint NOT NULL AUTO_INCREMENT,\n `text` varchar(30) DEFAULT 'AUTO_INCREMENT=987 )',\n PRIMARY KEY (`id`),\n CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `app`.`parent` (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=123 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n/*!50100 PARTITION BY HASH (`id`) PARTITIONS 4 */"
	got, err := mysqlDDL(input, src, dst, map[string]string{"app.parent": "Other.Parent"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CREATE TABLE `Dst``DB`.`Kid`", "NOT NULL AUTO_INCREMENT", "'AUTO_INCREMENT=987 )'", "REFERENCES `Other`.`Parent`", "PARTITIONS 4", "COLLATE=utf8mb4_bin"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q: %s", want, got)
		}
	}
	if strings.Contains(got, "AUTO_INCREMENT=123") {
		t.Fatal(got)
	}
}
func TestMySQLDDLDoesNotRewriteLiterals(t *testing.T) {
	src := model.TableID{Schema: "app", Table: "x"}
	q := "CREATE TABLE `x` (`id` int PRIMARY KEY, `v` varchar(100) DEFAULT 'REFERENCES `app`.`x` AUTO_INCREMENT=123 (') ENGINE=InnoDB AUTO_INCREMENT=321"
	got, err := mysqlDDL(q, src, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "DEFAULT 'REFERENCES `app`.`x` AUTO_INCREMENT=123 ('") || strings.Contains(got, "AUTO_INCREMENT=321") {
		t.Fatal(got)
	}
}
func TestPGReferencesRenameQuoted(t *testing.T) {
	got, err := pgReferences(`FOREIGN KEY ("Parent") REFERENCES "Old"."Pa""rent"(id) ON DELETE CASCADE`, model.TableID{Schema: "Old", Table: "Kid"}, model.TableID{Schema: "New", Table: "Kid"}, map[string]string{`Old.Pa"rent`: `Dst.Pa"rent`})
	if err != nil {
		t.Fatal(err)
	}
	if got != `FOREIGN KEY ("Parent") REFERENCES "Dst"."Pa""rent"(id) ON DELETE CASCADE` {
		t.Fatal(got)
	}
}

func TestDDLQuotedKeywordColumnsAreNotRewritten(t *testing.T) {
	id := model.TableID{Schema: "a", Table: "t"}
	dst := model.TableID{Schema: "b", Table: "u"}
	q := "CREATE TABLE `t` (`id` int PRIMARY KEY, `CONSTRAINT` int, `REFERENCES` int, `x` varchar(1) DEFAULT '(') ENGINE=InnoDB AUTO_INCREMENT=321"
	got, err := mysqlDDL(q, id, dst, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "`CONSTRAINT` int, `REFERENCES` int") || strings.Contains(got, "AUTO_INCREMENT=321") {
		t.Fatal(got)
	}
	def := `FOREIGN KEY ("REFERENCES") REFERENCES "a"."parent"("id")`
	got, err = pgReferences(def, id, dst, map[string]string{"a.parent": "b.parent"})
	if err != nil {
		t.Fatal(err)
	}
	if got != `FOREIGN KEY ("REFERENCES") REFERENCES "b"."parent"("id")` {
		t.Fatal(got)
	}
}
