package mongocdc

import (
	"errors"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func rawID(t *testing.T, v any) bson.RawValue {
	t.Helper()
	typ, data, err := bson.MarshalValue(v)
	if err != nil {
		t.Fatal(err)
	}
	return bson.RawValue{Type: typ, Value: data}
}

// Several events for one document collapse to the last; numerically equal _ids of
// different BSON types are the same document (unique _id index) and collapse too.
func TestBatchCollapsesEqualIDs(t *testing.T) {
	b := newBatch()
	doc := bson.Raw{5, 0, 0, 0, 0}
	b.add("c", rawID(t, int32(1)), doc)
	b.add("c", rawID(t, 1.0), nil) // same document via a double _id
	b.add("c", rawID(t, int64(1)), doc)
	b.add("c", rawID(t, float64(1<<60)), nil)
	b.add("c", rawID(t, int64(1<<60)), doc)
	b.add("d", rawID(t, "x"), doc)
	b.add("c", rawID(t, int32(2)), nil)
	w, colls := b.writes()
	if strings.Join(colls, ",") != "c,d" || b.events != 7 {
		t.Fatalf("colls %v events %d", colls, b.events)
	}
	c := w["c"]
	if !c.fast || len(c.ids) != 3 || len(c.docs) != 2 {
		t.Fatalf("fast=%v ids=%d docs=%d", c.fast, len(c.ids), len(c.docs))
	}
}

// _ids that cannot be canonicalized (documents, Decimal128) use ordered per-document
// writes in order of last occurrence.
func TestBatchSlowPathKeepsOrder(t *testing.T) {
	b := newBatch()
	doc := bson.Raw{5, 0, 0, 0, 0}
	dec, _ := bson.ParseDecimal128("1")
	b.add("c", rawID(t, int32(1)), doc)
	b.add("c", rawID(t, dec), nil)
	b.add("c", rawID(t, bson.D{{Key: "a", Value: 1}}), doc)
	w, _ := b.writes()
	c := w["c"]
	if c.fast || len(c.models) != 3 {
		t.Fatalf("fast=%v models=%d", c.fast, len(c.models))
	}
	if _, ok := c.models[1].(*mongo.DeleteOneModel); !ok {
		t.Fatalf("order not kept: %T", c.models[1])
	}
}

func TestRedactError(t *testing.T) {
	uri := "mongodb://app:p%40ss%3Aw0rd@db1:27017/?replicaSet=rs0"
	err := redactError(errors.New("dial "+uri+" failed; auth p@ss:w0rd and p%40ss%3Aw0rd for app:p%40ss%3Aw0rd"), uri)
	if strings.Contains(err.Error(), "w0rd") {
		t.Fatalf("password leaked: %s", err)
	}
	sentinel := errors.New("x")
	if !errors.Is(redactError(errors.Join(sentinel, errors.New(uri)), uri), sentinel) {
		t.Fatal("redaction must keep the error chain")
	}
}

func TestCheckTopologyRefusesShardedAndStandalone(t *testing.T) {
	if err := checkTopology("source", helloReply{Msg: "isdbgrid", MaxWireVersion: 21}); err == nil || !strings.Contains(err.Error(), "sharded") {
		t.Fatalf("mongos accepted: %v", err)
	}
	if err := checkTopology("source", helloReply{MaxWireVersion: 21}); err == nil {
		t.Fatal("standalone accepted")
	}
	if err := checkTopology("target", helloReply{SetName: "rs0", MaxWireVersion: 8}); err == nil {
		t.Fatal("4.2 accepted")
	}
	if err := checkTopology("target", helloReply{SetName: "rs0", MaxWireVersion: 21}); err != nil {
		t.Fatal(err)
	}
}

// Two replica sets both named rs0 with member localhost:27017 (container defaults) are
// different deployments; only the same primary process is "self".
func TestSameDeployment(t *testing.T) {
	a := helloReply{SetName: "rs0", Hosts: []string{"localhost:27017"}}
	b := a
	a.TopologyVersion.ProcessID = bson.NewObjectID()
	b.TopologyVersion.ProcessID = bson.NewObjectID()
	if sameDeployment(a, b, "mongodb://h1:1", "mongodb://h2:2") {
		t.Fatal("distinct processes reported as the same deployment")
	}
	if !sameDeployment(a, a, "mongodb://h1:1", "mongodb://h2:2") || !sameDeployment(a, b, "mongodb://h:1", "mongodb://h:1") {
		t.Fatal("same deployment not detected")
	}
}

func TestValidateConfig(t *testing.T) {
	ok := Config{Pipeline: "p", SourceURI: "mongodb://a", TargetURI: "mongodb://b", SourceDB: "app", TargetDB: "app2", Include: []string{"app.*"}}
	if err := validateConfig(ok); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*Config){
		"other db pattern": func(c *Config) { c.Include = []string{"other.*"} },
		"no dot":           func(c *Config) { c.Include = []string{"app"} },
		"bad glob":         func(c *Config) { c.Include = []string{"app.["} },
		"state db":         func(c *Config) { c.TargetDB = stateDatabase },
		"admin":            func(c *Config) { c.SourceDB = "admin"; c.Include = []string{"admin.*"} },
		"no include":       func(c *Config) { c.Include = nil },
	} {
		c := ok
		mut(&c)
		if validateConfig(c) == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
