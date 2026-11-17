package rowkey

import (
	"strings"
	"testing"
)

func TestKeylessColumnRejectsCitext(t *testing.T) {
	for _, name := range []string{"citext", " CITEXT ", "_citext", "citext[]", "citext[][]"} {
		if why := KeylessColumnProblem(name, true, false); !strings.Contains(why, "case-insensitive") {
			t.Fatalf("unsafe equality %q accepted: %q", name, why)
		}
	}
	if why := KeylessColumnProblem("text", true, false); why != "" {
		t.Fatal(why)
	}
	if why := KeylessColumnProblem("citext", true, true); why == "" {
		t.Fatal("allow_float must not permit unsafe citext equality")
	}
}
