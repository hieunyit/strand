package validate

import (
	"strings"
	"testing"
)

func TestKeylessDecide(t *testing.T) {
	seeded := func(string) bool { return true }
	cols := []string{"a", "b"}
	ok := map[string]string{"a": "int", "b": "varchar"}
	if (KeylessOpts{}).Decide("d.t", cols, ok, false, "") != "" {
		t.Fatal("keyless off: no decision to make (missing_pk is reported by the caller)")
	}
	if w := (KeylessOpts{On: true}).Decide("d.t", cols, ok, false, ""); !strings.Contains(w, "seed") {
		t.Fatalf("not seeded must be refused: %q", w)
	}
	if w := (KeylessOpts{On: true, Seeded: func(string) bool { return false }}).Decide("d.t", cols, ok, false, ""); !strings.Contains(w, "logical snapshot") {
		t.Fatalf("logical bootstrap must be refused: %q", w)
	}
	if w := (KeylessOpts{On: true, Seeded: seeded}).Decide("d.t", cols, ok, false, ""); w != "" {
		t.Fatalf("seeded + plain types must pass: %q", w)
	}
	fl := map[string]string{"a": "int", "b": "double"}
	if w := (KeylessOpts{On: true, Seeded: seeded}).Decide("d.t", cols, fl, false, ""); !strings.Contains(w, "floating-point") {
		t.Fatalf("float must be refused by default: %q", w)
	}
	if w := (KeylessOpts{On: true, Seeded: seeded, AllowFloat: true}).Decide("d.t", cols, fl, false, ""); w != "" {
		t.Fatalf("keyless_allow_float: %q", w)
	}
	js := map[string]string{"a": "int", "b": "json"}
	if w := (KeylessOpts{On: true, Seeded: seeded, AllowFloat: true}).Decide("d.t", cols, js, false, ""); !strings.Contains(w, "json") {
		t.Fatalf("json must always be refused: %q", w)
	}
	// PostgreSQL: REPLICA IDENTITY FULL is required.
	pg := map[string]string{"a": "int4", "b": "text"}
	for _, ri := range []string{"d", "n", "i"} {
		if w := (KeylessOpts{On: true, Seeded: seeded}).Decide("d.t", cols, pg, true, ri); !strings.Contains(w, "REPLICA IDENTITY FULL") {
			t.Fatalf("PG replica identity %q must be refused: %q", ri, w)
		}
	}
	if w := (KeylessOpts{On: true, Seeded: seeded}).Decide("d.t", cols, pg, true, "f"); w != "" {
		t.Fatalf("PG FULL: %q", w)
	}
	if w := (KeylessOpts{On: true, Seeded: seeded}).Decide("d.t", cols, map[string]string{"a": "int4", "b": "point"}, true, "f"); !strings.Contains(w, "point") {
		t.Fatalf("PG geometric type: %q", w)
	}
}
