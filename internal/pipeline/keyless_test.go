package pipeline

import (
	"strings"
	"testing"

	"strand/internal/config"
	"strand/internal/model"
)

func klMeta(cols []string, types map[string]string) *tableMeta {
	return &tableMeta{ID: model.TableID{Schema: "d", Table: "t"}, Columns: cols, ColTypes: types}
}

func TestMarkKeyless(t *testing.T) {
	id := model.TableID{Schema: "d", Table: "t"}
	mk := func(on bool, allowFloat bool, engine model.Engine, m *tableMeta) *tableMeta {
		p := &Pipeline{cfg: &config.Config{}, srcEngine: engine}
		if on {
			p.cfg.Tables.Keyless = "full_row"
		}
		p.cfg.Tables.KeylessAllowFloat = allowFloat
		p.markKeyless(map[model.TableID]*tableMeta{id: m})
		return m
	}
	plain := func() *tableMeta { return klMeta([]string{"a", "b"}, map[string]string{"a": "int", "b": "varchar"}) }

	if m := mk(false, false, model.EngineMySQL, plain()); m.Keyless || len(m.PK) != 0 {
		t.Fatal("default off: a table without key stays keyless-less (missing_pk)")
	}
	m := mk(true, false, model.EngineMySQL, plain())
	if !m.Keyless || strings.Join(m.PK, ",") != "a,b" || m.PKNumeric || m.PKCodec != nil {
		t.Fatalf("keyless meta: %+v", m)
	}
	if keyLabel(m) != "full row (keyless)" {
		t.Fatal(keyLabel(m))
	}
	// A table with a key is never made keyless.
	withKey := plain()
	withKey.PK, withKey.KeyName = []string{"a"}, "PRIMARY"
	if m := mk(true, false, model.EngineMySQL, withKey); m.Keyless {
		t.Fatal("tables with a key are untouched")
	}
	// Float / JSON policy.
	fl := klMeta([]string{"a", "f"}, map[string]string{"a": "int", "f": "double"})
	if m := mk(true, false, model.EngineMySQL, fl); m.Keyless || !strings.Contains(m.KeylessWhy, "floating-point") {
		t.Fatalf("float refused by default: %+v", m)
	}
	fl = klMeta([]string{"a", "f"}, map[string]string{"a": "int", "f": "double"})
	if m := mk(true, true, model.EngineMySQL, fl); !m.Keyless {
		t.Fatal("keyless_allow_float")
	}
	js := klMeta([]string{"a", "j"}, map[string]string{"a": "int", "j": "json"})
	if m := mk(true, true, model.EngineMySQL, js); m.Keyless || !strings.Contains(m.KeylessWhy, "json") {
		t.Fatalf("json refused: %+v", m)
	}
	// PostgreSQL source needs REPLICA IDENTITY FULL.
	pg := plain()
	pg.ReplIdent = "d"
	if m := mk(true, false, model.EnginePG, pg); m.Keyless || !strings.Contains(m.KeylessWhy, "REPLICA IDENTITY FULL") {
		t.Fatalf("PG default identity refused: %+v", m)
	}
	pg = plain()
	pg.ReplIdent = "f"
	if m := mk(true, false, model.EnginePG, pg); !m.Keyless {
		t.Fatal("PG FULL accepted")
	}
}

func TestSetWriterKeyAndPendingError(t *testing.T) {
	m := klMeta([]string{"a"}, nil)
	m.Keyless, m.PK = true, []string{"a"}
	p := &Pipeline{cfg: &config.Config{}, srcEngine: model.EnginePG}
	err := p.keylessPendingError(nil, m.ID, m)
	if err == nil || !strings.Contains(err.Error(), "seed") || !strings.Contains(err.Error(), "missing_pk") {
		t.Fatalf("pending keyless table must be refused with missing_pk: %v", err)
	}
}
