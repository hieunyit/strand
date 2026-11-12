package promote

import (
	"errors"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/mysqlver"
)

func TestChooseFenceMode(t *testing.T) {
	cases := map[string]FenceMode{
		"5.5.62-log": FenceFTWRL, "5.6.51-log": FenceFTWRL, "5.7.7": FenceFTWRL,
		"5.7.8": FenceSuperReadOnly, "5.7.44-log": FenceSuperReadOnly, "8.0.36": FenceSuperReadOnly, "8.4.0": FenceSuperReadOnly,
	}
	for raw, want := range cases {
		v, err := mysqlver.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := ChooseFenceMode(v); got != want {
			t.Errorf("%s: got %v want %v", raw, got, want)
		}
	}
}

func TestSessionFilters(t *testing.T) {
	all := []SessionInfo{{1, "app", "h1"}, {2, "root", "h2"}, {3, "system user", ""}, {4, "strand", "h3"}, {5, "app", "h4"}, {6, "dba", "h5"}}
	if got := appSessions(all, []string{"app"}); len(got) != 2 || got[0].ID != 1 || got[1].ID != 5 {
		t.Errorf("appSessions = %+v", got)
	}
	if got := appSessions(all, nil); len(got) != 0 {
		t.Errorf("no app users must kill nothing, got %+v", got)
	}
	got := superSessions(all, map[string]bool{"root": true, "strand": true, "dba": true, "system user": true}, "strand")
	if len(got) != 2 || got[0].User != "root" || got[1].User != "dba" {
		t.Errorf("superSessions = %+v", got)
	}
}

func key(t *testing.T, seq uint32, off uint64) uint64 {
	k, err := checkpoint.BinlogKey(seq, off)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestPosCaughtUp(t *testing.T) {
	free := func(v bool, err error) func() (bool, error) { return func() (bool, error) { return v, err } }
	never := func() (bool, error) { t.Fatal("txnFree must not be consulted"); return false, nil }
	head := key(t, 3, 500)
	cases := []struct {
		name string
		lwd  uint64
		fn   func() (bool, error)
		want bool
		err  bool
	}{
		{"empty set is never caught up", 0, never, false, false},
		{"lwd == head", head, never, true, false},
		{"lwd beyond head (later file)", key(t, 4, 4), never, true, false},
		{"behind with txn events pending", key(t, 3, 300), free(false, nil), false, false},
		// head có Rotate/Format_desc sau Txn cuối: không phải Txn nên đã kịp dù lwd < head
		{"behind head only by non-txn events (rotate)", key(t, 2, 900), free(true, nil), true, false},
		{"error propagates", key(t, 3, 300), free(false, errors.New("boom")), false, true},
	}
	for _, c := range cases {
		got, err := PosCaughtUp(c.lwd, head, c.fn)
		if got != c.want || (err != nil) != c.err {
			t.Errorf("%s: got (%v, %v) want (%v, err=%v)", c.name, got, err, c.want, c.err)
		}
	}
}
