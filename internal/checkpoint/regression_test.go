package checkpoint

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"testing"

	"strand/internal/model"
)

const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

func my(g string) model.Pos { return model.Pos{Engine: model.EngineMySQL, GTID: g} }

// invariant: CP-2
// MySQL 8.4 prints tagged GTID sets as "uuid:1-5:tag:1-3" (intervals after a tag
// belong to uuid:tag). The old parser produced prefix "uuid:1-5:tag" and lost both
// ranges, so already-applied Txn were applied again after restart.
func TestCP2_TaggedGTIDSetNativeFormat(t *testing.T) {
	s, err := ParseAppliedSet(model.EngineMySQL, u+":1-5:mytag:1-3:other:7")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range []string{u + ":4", u + ":mytag:2", u + ":other:7"} {
		if !s.Contains(my(g)) {
			t.Fatalf("set must contain %s; String()=%q", g, s.(*MySQLAppliedSet).String())
		}
	}
	for _, g := range []string{u + ":6", u + ":mytag:4", u + ":other:1"} {
		if s.Contains(my(g)) {
			t.Fatalf("set must not contain %s", g)
		}
	}
	// Round trip.
	back, err := ParseAppliedSet(model.EngineMySQL, s.(*MySQLAppliedSet).String())
	if err != nil || !back.Equal(s) {
		t.Fatalf("round trip failed: %v %q", err, s.(*MySQLAppliedSet).String())
	}
}

// invariant: CP-2
// Server UUIDs and tags are case-insensitive (seed.json example uses upper case,
// the binlog stream lower case). They must compare equal.
func TestCP2_GTIDUUIDCaseInsensitive(t *testing.T) {
	s, err := ParseAppliedSet(model.EngineMySQL, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100")
	if err != nil {
		t.Fatal(err)
	}
	if !s.Contains(my(u + ":50")) {
		t.Fatalf("upper-case set must contain lower-case GTID")
	}
	s.Add(my("3E11FA47-71CA-11E1-9E33-C80AA9429562:MyTag:3"))
	if !s.Contains(my(u + ":mytag:3")) {
		t.Fatalf("tags must be case-insensitive")
	}
}

// invariant: CP-1, CP-2
// Corrupt checkpoint text must be an error, never a silently smaller/larger set.
func TestCP1_ParseAppliedSetRejectsGarbage(t *testing.T) {
	bad := map[model.Engine][]string{
		model.EngineMySQL: {u + ":1-x", u + ":5-3", u + ":0", u + ":tag", u + "::1", ":1-5"},
		model.EnginePG: {
			`{"lwd":"0/10","high":"0/100","holes":["ZZZ"]}`,
			`{"lwd":"0/10","high":"nope","holes":[]}`,
			`0/10,garbage`,
		},
	}
	for eng, inputs := range bad {
		for _, in := range inputs {
			if _, err := ParseAppliedSet(eng, in); err == nil {
				t.Errorf("%s: expected error for %q", eng, in)
			}
		}
	}
}

// Set operations used by seed SD-2: purged \ P must be covered by the archive.
func TestMySQLAppliedSetSubtractAndSubset(t *testing.T) {
	p, _ := ParseMySQLGTIDSet(u + ":1-1000")
	purged, _ := ParseMySQLGTIDSet(u + ":1-5000")
	archive, _ := ParseMySQLGTIDSet(u + ":1-2000")
	missing := purged.Clone().(*MySQLAppliedSet)
	missing.Subtract(p)
	if missing.String() != u+":1001-5000" {
		t.Fatalf("subtract = %q", missing.String())
	}
	if missing.IsSubsetOf(archive) {
		t.Fatalf("1001-5000 is not covered by archive 1-2000")
	}
	archive2, _ := ParseMySQLGTIDSet(u + ":900-6000")
	if !missing.IsSubsetOf(archive2) {
		t.Fatalf("1001-5000 is covered by archive 900-6000")
	}
}
