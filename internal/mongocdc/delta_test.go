package mongocdc

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func mustRaw(t *testing.T, d any) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestApplyUpdate(t *testing.T) {
	pre := mustRaw(t, bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "arr", Value: bson.A{1, 2, 3}},
		{Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "1", Value: "one"}}}})
	cases := []struct {
		name string
		d    updateDesc
		want bson.D
		err  error
	}{
		{"set existing keeps position, new appended", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "z", Value: 9}, {Key: "a", Value: 2}})},
			bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 2}, {Key: "arr", Value: bson.A{1, 2, 3}}, {Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "1", Value: "one"}}}, {Key: "z", Value: 9}}, nil},
		{"array index past end pads null", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "arr.5", Value: 6}})},
			bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "arr", Value: bson.A{1, 2, 3, nil, nil, 6}}, {Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "1", Value: "one"}}}}, nil},
		{"numeric key in document", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "n.1", Value: "uno"}})},
			bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "arr", Value: bson.A{1, 2, 3}}, {Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "1", Value: "uno"}}}}, nil},
		{"truncate, unset, unset array elem", updateDesc{TruncatedArrays: []truncation{{"arr", 2}}, RemovedFields: []string{"a", "arr.0", "missing.deep"}},
			bson.D{{Key: "_id", Value: 1}, {Key: "arr", Value: bson.A{nil, 2}}, {Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "1", Value: "one"}}}}, nil},
		{"4.4 missing parents created", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "p.q.r", Value: 1}})},
			bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "arr", Value: bson.A{1, 2, 3}}, {Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "1", Value: "one"}}}, {Key: "p", Value: bson.D{{Key: "q", Value: bson.D{{Key: "r", Value: 1}}}}}}, nil},
		{"5.0+ missing parent is a dotted name", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "p.q", Value: 1}}), noCreate: true}, nil, errNeedLookup},
		{"field on array", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "arr.x", Value: 1}})}, nil, errNeedLookup},
		{"traverse scalar", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "a.b", Value: 1}})}, nil, errNeedLookup},
		{"truncate longer than array", updateDesc{TruncatedArrays: []truncation{{"arr", 7}}}, nil, errNeedLookup},
		{"disambiguated dotted name", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "n.x", Value: 5}}),
			DisambiguatedPaths: mustRaw(t, bson.D{{Key: "n.x", Value: bson.A{"n.x"}}}), exactPaths: true},
			bson.D{{Key: "_id", Value: 1}, {Key: "a", Value: 1}, {Key: "arr", Value: bson.A{1, 2, 3}}, {Key: "n", Value: bson.D{{Key: "x", Value: 1}, {Key: "1", Value: "one"}}}, {Key: "n.x", Value: 5}}, nil},
		{"disambiguated index on document", updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "n.1", Value: 5}}),
			DisambiguatedPaths: mustRaw(t, bson.D{{Key: "n.1", Value: bson.A{"n", int32(1)}}}), exactPaths: true}, nil, errNeedLookup},
	}
	for _, c := range cases {
		got, err := applyUpdate(pre, &c.d)
		if c.err != nil {
			if !errors.Is(err, c.err) {
				t.Errorf("%s: want %v, got %v (%s)", c.name, c.err, err, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if want := mustRaw(t, c.want); !bytes.Equal(got, want) {
			t.Errorf("%s:\n got %s\nwant %s", c.name, got, want)
		}
	}
	// A document with a dotted field name is ambiguous without disambiguatedPaths.
	dotted := mustRaw(t, bson.D{{Key: "_id", Value: 1}, {Key: "a.b", Value: 1}, {Key: "a", Value: bson.D{}}})
	if _, err := applyUpdate(dotted, &updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "a.b", Value: 2}})}); !errors.Is(err, errNeedLookup) {
		t.Fatalf("ambiguous dotted path applied: %v", err)
	}
}

// Delta events fold into images already in the batch; a pending delta waits for the
// target image; non-canonical _ids are looked up.
func TestBatchDeltaFolding(t *testing.T) {
	b := newBatch()
	doc := mustRaw(t, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: 1}})
	b.add("c", rawID(t, int32(1)), doc)
	b.addDelta("c", rawID(t, int32(1)), &updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "v", Value: 2}})})
	b.addDelta("c", rawID(t, int32(2)), &updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "v", Value: 3}})})
	b.add("c", rawID(t, int32(3)), nil)
	b.addDelta("c", rawID(t, int32(3)), &updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "v", Value: 3}})})
	dec, _ := bson.ParseDecimal128("4")
	b.addDelta("c", rawID(t, dec), &updateDesc{UpdatedFields: mustRaw(t, bson.D{{Key: "v", Value: 3}})})
	k := func(v any) *docOp { key, _ := idKey(rawID(t, v)); return b.ops["c\x00"+key] }
	if op := k(int32(1)); !bytes.Equal(op.doc, mustRaw(t, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: 2}})) {
		t.Fatalf("in-batch delta not folded: %s", op.doc)
	}
	if op := k(int32(2)); len(op.deltas) != 1 || op.lookup {
		t.Fatalf("pending delta: %+v", op)
	}
	if op := k(int32(3)); !op.lookup {
		t.Fatalf("update after delete must be looked up: %+v", op)
	}
	if op := k(dec); !op.lookup {
		t.Fatalf("non-canonical _id must be looked up: %+v", op)
	}
}

func TestResolveUpdateMode(t *testing.T) {
	for wire, want := range map[int32]string{9: UpdateModeDelta, 13: UpdateModeLookup, 17: UpdateModeLookup, 18: UpdateModeDelta, 25: UpdateModeDelta} {
		if got := resolveUpdateMode("", wire); got != want {
			t.Errorf("wire %d: %s, want %s", wire, got, want)
		}
	}
	if resolveUpdateMode(UpdateModeDelta, 13) != UpdateModeDelta || resolveUpdateMode(UpdateModeLookup, 25) != UpdateModeLookup {
		t.Fatal("explicit mode not honored")
	}
}

// A lease holder on this host whose process is gone can be taken over at once; a live
// one, one on another host / PID namespace, or one without identity cannot.
func TestProvablyGone(t *testing.T) {
	self := selfOwnerInfo()
	if self.BootID == "" || self.PIDStart == "" {
		t.Fatal("process identity unavailable (needs Linux /proc)")
	}
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	dead := self
	dead.PID, dead.PIDStart = cmd.Process.Pid, "1"
	if !provablyGone(dead, self) {
		t.Fatal("dead process on this host not detected")
	}
	alive := self
	alive.PID, alive.PIDStart = os.Getppid(), processStart(os.Getppid())
	if provablyGone(alive, self) {
		t.Fatal("live process reported gone")
	}
	reused := alive
	reused.PIDStart = "123" // PID reused by another process
	if !provablyGone(reused, self) {
		t.Fatal("reused PID not detected")
	}
	for name, mut := range map[string]func(*ownerInfo){
		"other host": func(o *ownerInfo) { o.Host += "x" }, "other boot": func(o *ownerInfo) { o.BootID += "x" },
		"other pid namespace": func(o *ownerInfo) { o.PidNS += "x" }, "no identity": func(o *ownerInfo) { *o = ownerInfo{} },
	} {
		o := dead
		mut(&o)
		if provablyGone(o, self) {
			t.Errorf("%s: takeover allowed", name)
		}
	}
}

// Regression: target pre-images must be read with readConcern local on the primary; a
// majority read could return the image from before the previous (acknowledged) batch.
func TestTargetReadsAreLocal(t *testing.T) {
	var opts options.DatabaseOptions
	for _, f := range targetDBOptions().List() {
		_ = f(&opts)
	}
	if opts.ReadConcern == nil || opts.ReadConcern.Level != "local" || opts.ReadPreference == nil || opts.ReadPreference.Mode() != readpref.PrimaryMode {
		t.Fatalf("target reads: %+v %+v", opts.ReadConcern, opts.ReadPreference)
	}
}
