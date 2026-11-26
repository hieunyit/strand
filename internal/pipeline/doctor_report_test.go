package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"strand/internal/findings"
	"strand/internal/model"
)

// compareMySQLForeignKeys with a collector reports every mismatch; without one
// (Start, validate) it keeps stopping at the first, with the same message.
func TestCompareMySQLForeignKeysCollectsEveryProblem(t *testing.T) {
	source := sql.OpenDB(wiringConnector{answer: func(q string) ([]string, []driver.Value, error) {
		switch {
		case strings.HasPrefix(q, "SHOW CREATE TABLE") && strings.Contains(q, "`a`"):
			return []string{"t", "ddl"}, []driver.Value{"a", "CREATE TABLE `a` (\n  CONSTRAINT `fk_a` FOREIGN KEY (`p_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE\n)"}, nil
		case strings.HasPrefix(q, "SHOW CREATE TABLE") && strings.Contains(q, "`b`"):
			return []string{"t", "ddl"}, []driver.Value{"b", "CREATE TABLE `b` (\n  CONSTRAINT `fk_b` FOREIGN KEY (`p_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE\n)"}, nil
		case strings.HasPrefix(q, "SHOW CREATE TABLE"):
			return []string{"t", "ddl"}, []driver.Value{"parent", "CREATE TABLE `parent` (`id` int PRIMARY KEY)"}, nil
		case strings.Contains(q, "COLLATION_NAME"):
			return []string{"c"}, []driver.Value{""}, nil
		}
		return nil, nil, errors.New("unexpected source query: " + q)
	}})
	defer source.Close()
	target := sql.OpenDB(wiringConnector{answer: func(q string) ([]string, []driver.Value, error) {
		switch {
		case strings.Contains(q, "REFERENTIAL_CONSTRAINTS"): // one target FK, with another rule than the source
			return []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"},
				[]driver.Value{"app", "a", "app", "parent", "p_id", "id", "SET NULL", "RESTRICT", "fk_a", "", ""}, nil
		case strings.Contains(q, "COUNT(*) FROM information_schema.TABLES"):
			return []string{"n"}, []driver.Value{int64(1)}, nil
		}
		return nil, nil, errors.New("unexpected target query: " + q)
	}})
	defer target.Close()
	app := func(n string) model.TableID { return model.TableID{Schema: "app", Table: n} }
	scope := map[model.TableID]bool{app("a"): true, app("b"): true, app("parent"): true}
	same := func(id model.TableID) model.TableID { return id }
	never := func(model.TableID) bool { return false }

	_, err := compareMySQLForeignKeys(context.Background(), source, target, scope, same, never, false, nil)
	if err == nil || !strings.Contains(err.Error(), "unexpected cascading foreign key fk_a") || strings.Contains(err.Error(), "fk_b") {
		t.Fatalf("fail-fast error = %v, want only the first problem", err)
	}

	c := &findings.Collector{}
	if _, err := compareMySQLForeignKeys(context.Background(), source, target, scope, same, never, false, c); err != nil {
		t.Fatal(err)
	}
	if !c.Has("target_fk_unexpected", "app.a.fk_a") || !c.Has("target_fk_missing", "app.a.fk_a") || !c.Has("target_fk_missing", "app.b.fk_b") {
		t.Fatalf("findings = %+v", c.Findings())
	}
	for _, f := range c.Findings() {
		if f.Code == "target_fk_missing" && !strings.HasPrefix(f.Fix, "ALTER TABLE `app`.") {
			t.Errorf("missing FK fix should be the ALTER TABLE statement: %+v", f)
		}
	}
}
