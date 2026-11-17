package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strand/internal/config"
	"strand/internal/model"
	"strand/internal/rowkey"
	"strings"
	"testing"
)

func TestMySQLFunctionalUniqueMetadataSkipsWholeIdentity(t *testing.T) {
	for _, exprFirst := range []bool{false, true} {
		idx := map[string][]string{}
		prefix, expression := map[string]bool{}, map[string]bool{}
		plain := sql.NullString{String: "a", Valid: true}
		add := func(c sql.NullString) {
			appendMySQLUniqueColumn(idx, prefix, expression, "functional", c, sql.NullInt64{})
		}
		if exprFirst {
			add(sql.NullString{})
			add(plain)
		} else {
			add(plain)
			add(sql.NullString{})
		}
		appendMySQLUniqueColumn(idx, prefix, expression, "fallback", plain, sql.NullInt64{})
		if len(idx["functional"]) != 0 || !prefix["functional"] || !expression["functional"] {
			t.Fatalf("partial expression identity accepted: %#v", idx)
		}
		if len(idx["fallback"]) != 1 || idx["fallback"][0] != "a" || prefix["fallback"] {
			t.Fatalf("usable fallback lost: %#v", idx)
		}
	}
}

func TestKeylessCitextFailsClosed(t *testing.T) {
	if rowkey.KeylessColumnProblem("citext", true, false) == "" {
		t.Fatal("citext equality must be rejected")
	}
	if rowkey.KeylessColumnProblem("text", true, false) != "" {
		t.Fatal("ordinary text must remain supported")
	}
	p := &Pipeline{srcEngine: model.EnginePG, cfg: &config.Config{}}
	p.cfg.Tables.Keyless = "full_row"
	m := klMeta([]string{"v"}, map[string]string{"v": "citext"})
	m.ReplIdent = "f"
	p.markKeyless(map[model.TableID]*tableMeta{m.ID: m})
	if m.Keyless || m.KeylessWhy == "" {
		t.Fatalf("citext source enabled: %+v", m)
	}
}

func TestKeylessTargetCitextDomainFailsClosed(t *testing.T) {
	for _, citext := range []bool{false, true} {
		queries := 0
		db := sql.OpenDB(wiringConnector{answer: func(q string) ([]string, []driver.Value, error) {
			queries++
			if strings.Contains(q, "WITH RECURSIVE column_types") {
				if !strings.Contains(q, "typbasetype") || !strings.Contains(q, "typelem") || !strings.Contains(q, "typcategory='A'") || !strings.Contains(q, "typname='citext'") {
					t.Fatalf("domain base equality not checked: %s", q)
				}
				if citext {
					return []string{"attname"}, []driver.Value{"v"}, nil
				}
				return []string{"attname"}, nil, nil
			}
			if strings.Contains(q, "NOT co.collisdeterministic") {
				return []string{"attname", "collname"}, nil, nil
			}
			return nil, nil, fmt.Errorf("unexpected query %s", q)
		}})
		p := &Pipeline{dst: db, dstEngine: model.EnginePG, filter: &tableFilter{}}
		id := model.TableID{Schema: "public", Table: "keyless"}
		err := p.checkKeylessTargetCollations(context.Background(), id, &tableMeta{Keyless: true})
		db.Close()
		if citext && (err == nil || !strings.Contains(err.Error(), "citext")) {
			t.Fatalf("unsafe equality accepted: %v", err)
		}
		if !citext && (err != nil || queries != 2) {
			t.Fatalf("deterministic text rejected: %v (%d queries)", err, queries)
		}
	}
}
