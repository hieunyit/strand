package validate

import (
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/rowkey"
)

func TestUniqueIndexCoversMySQLSubsetAndPGExact(t *testing.T) {
	idx := map[string][]string{"uk_a": {"a"}, "uk_ab": {"a", "b"}}
	key := []string{"a", "b"}
	if n, ok := uniqueIndexCovers(idx, key, false); !ok || n != "uk_ab" {
		t.Fatalf("MySQL: exact source identity is required, got %q %v", n, ok)
	}
	if n, ok := uniqueIndexCovers(idx, key, true); !ok || n != "uk_ab" {
		t.Fatalf("PG needs the exact column set for ON CONFLICT, got %q %v", n, ok)
	}
	if _, ok := uniqueIndexCovers(map[string][]string{"uk_a": {"a"}}, key, false); ok {
		t.Fatal("MySQL subset unique cannot represent all source rows")
	}
	if _, ok := uniqueIndexCovers(map[string][]string{"uk_a": {"a"}}, key, true); ok {
		t.Fatal("PG: a subset index cannot be the ON CONFLICT arbiter")
	}
	if _, ok := uniqueIndexCovers(map[string][]string{"uk_ac": {"a", "c"}}, key, false); ok {
		t.Fatal("index with a column outside the key does not guarantee key uniqueness")
	}
	if _, ok := uniqueIndexCovers(nil, key, false); ok {
		t.Fatal("no index")
	}
}

func TestTargetKeyErrorMessage(t *testing.T) {
	src := model.TableID{Schema: "app", Table: "t"}
	k := rowkey.Key{Name: "uk_email", Columns: []string{"email"}}
	err := TargetKeyError(src, src, k, model.EngineMySQL)
	var ee *exitcode.Error
	if !errors.As(err, &ee) || ee.Code != exitcode.ErrMissingPK || ee.ExitCode != exitcode.Rejected {
		t.Fatalf("want missing_pk exit 3, got %v", err)
	}
	if !strings.Contains(err.Error(), "ADD UNIQUE KEY (`email`)") || !strings.Contains(err.Error(), "duplicate rows") {
		t.Fatal(err.Error())
	}
	pg := TargetKeyError(src, src, k, model.EnginePG)
	if !strings.Contains(pg.Error(), `CREATE UNIQUE INDEX ON "app"."t" ("email")`) {
		t.Fatal(pg.Error())
	}
}

func TestMissingKeysErrorListsEveryTable(t *testing.T) {
	if missingKeysError(nil) != nil {
		t.Fatal("no missing tables, no error")
	}
	a := rowkey.Suggest(rowkey.SuggestInput{Flavor: "mariadb", Major: 10, Minor: 6, Table: model.TableID{Schema: "d", Table: "a"}})
	b := rowkey.Suggest(rowkey.SuggestInput{Flavor: "postgres", Table: model.TableID{Schema: "d", Table: "b"}, PGIndex: "ix"})
	err := missingKeysError([]string{a, b})
	var ee *exitcode.Error
	if !errors.As(err, &ee) || ee.Code != exitcode.ErrMissingPK || ee.ExitCode != 3 {
		t.Fatal(err)
	}
	for _, w := range []string{"2 table(s)", "INVISIBLE", "REPLICA IDENTITY USING INDEX \"ix\""} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("missing %q in %s", w, err)
		}
	}
}

func TestIndexContainsIdentity(t *testing.T) {
	for _, tc := range []struct {
		cols []string
		safe bool
	}{
		{[]string{"a"}, false}, {[]string{"b"}, false}, {[]string{"c"}, false},
		{[]string{"b", "a"}, true}, {[]string{"c", "a", "b"}, true},
	} {
		if got := indexContainsIdentity(tc.cols, []string{"a", "b"}); got != tc.safe {
			t.Fatalf("%v safe=%v want %v", tc.cols, got, tc.safe)
		}
	}
}
