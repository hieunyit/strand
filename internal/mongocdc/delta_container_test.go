package mongocdc

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"strand/internal/testutil"
)

// Every update kind MongoDB can express (operators on dotted paths and array indexes,
// $unset, $pop/$push/$pull/$slice, $rename, pipeline updates that truncate arrays,
// replace fields, reorder fields) is applied to the pre-image with applyUpdate and
// compared byte for byte with the real post-image. applyUpdate must either produce the
// exact post-image or ask for a lookup; a silently different image is a failure.
func TestContainer_UpdateDescriptionsExact(t *testing.T) {
	rs := testutil.MongoReplicaSet(t)
	c := mustClient(t, rs.URI)
	ctx := context.Background()
	var h helloReply
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h); err != nil {
		t.Fatal(err)
	}
	coll := c.Database("delta").Collection("d")
	cfg := Config{MaxAwait: 100 * time.Millisecond}
	cs, err := c.Database("delta").Watch(ctx, mongo.Pipeline{}, streamOptions(cfg, h.MaxWireVersion))
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close(ctx)
	seed := testSeed(t)
	rng := rand.New(rand.NewSource(seed))
	const docs = 20
	pre := map[int]bson.Raw{}
	for i := 0; i < docs; i++ {
		d := deltaDoc(rng, i)
		if _, err := coll.InsertOne(ctx, d); err != nil {
			t.Fatal(err)
		}
		raw, _ := bson.Marshal(d)
		pre[i] = raw
	}
	next := func() streamEvent {
		t.Helper()
		for cs.Next(ctx) {
			var ev streamEvent
			if err := bson.Unmarshal(cs.Current, &ev); err != nil {
				t.Fatal(err)
			}
			if ev.OperationType != "update" && ev.OperationType != "replace" {
				continue // inserts of the setup, expanded DDL events (create)
			}
			return ev
		}
		t.Fatalf("change stream: %v", cs.Err())
		return streamEvent{}
	}
	updates := deltaUpdates(h.MaxWireVersion)
	exact, lookups, noops := 0, 0, 0
	byKind := map[string][2]int{}
	n := envInt("STRAND_MONGO_DELTA_OPS", 3000)
	for i := 0; i < n; i++ {
		u := updates[rng.Intn(len(updates))]
		// Field names containing '.' (5.0+ $setField) live in documents 0-2 only: before
		// 6.1 such documents are always looked up, which would hide everything else.
		id := 3 + rng.Intn(docs-3)
		if dottedUpdates[u.name] {
			id = rng.Intn(3)
		}
		res, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, u.update(rng))
		if err != nil {
			t.Fatalf("%s: %v", u.name, err)
		}
		if res.ModifiedCount == 0 {
			noops++
			continue
		}
		ev := next()
		var post bson.Raw
		if post, err = coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Raw(); err != nil {
			t.Fatal(err)
		}
		post = append(bson.Raw(nil), post...)
		k := byKind[u.name]
		switch {
		case ev.OperationType == "replace":
			// The server logged a full replacement: a full image, nothing to apply.
			exact++
		case ev.UpdateDescription == nil:
			t.Fatalf("%s: update event without description: %s", u.name, cs.Current)
		default:
			ev.UpdateDescription.exactPaths = h.MaxWireVersion >= wireVersion61
			ev.UpdateDescription.noCreate = h.MaxWireVersion >= wireVersion50
			got, err := applyUpdate(pre[id], ev.UpdateDescription)
			switch {
			case errors.Is(err, errNeedLookup):
				lookups++
				k[1]++
			case err != nil:
				t.Fatalf("%s: %v", u.name, err)
			case !bytes.Equal(got, post):
				t.Fatalf("%s (op %d, SEED=%d): applyUpdate produced a different image\npre:  %s\ndesc: %s\ngot:  %s\nwant: %s", u.name, i, seed, pre[id], bsonString(ev.UpdateDescription), got, post)
			default:
				exact++
				k[0]++
			}
		}
		byKind[u.name] = k
		pre[id] = post
	}
	t.Logf("server wire %d: %d updates: %d exact, %d lookup fallbacks, %d no-ops", h.MaxWireVersion, n, exact, lookups, noops)
	for name, k := range byKind {
		t.Logf("  %-22s exact=%d lookup=%d", name, k[0], k[1])
	}
	if lookups > (n-noops)/4 {
		t.Fatalf("too many lookup fallbacks (%d of %d): delta mode would not pay off", lookups, n)
	}
}

func bsonString(d *updateDesc) string {
	b, _ := bson.MarshalExtJSON(d, false, false)
	return string(b)
}

func deltaDoc(r *rand.Rand, id int) bson.D {
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "a", Value: int32(r.Intn(100))},
		{Key: "s", Value: "x"},
		{Key: "arr", Value: bson.A{1, 2, 3, bson.D{{Key: "k", Value: 1}}}},
		{Key: "nested", Value: bson.D{{Key: "x", Value: 1}, {Key: "y", Value: bson.D{{Key: "z", Value: bson.A{1, 2}}}}, {Key: "1", Value: "one"}}},
		{Key: "objarr", Value: bson.A{bson.D{{Key: "n", Value: 1}}, bson.D{{Key: "n", Value: 2}}}},
	}
}

type deltaUpdate struct {
	name   string
	update func(r *rand.Rand) any
}

// Updates that create or touch field names containing '.'.
var dottedUpdates = map[string]bool{"setField dotted name": true, "set after dotted": true, "set top (dotted doc)": true}

func deltaUpdates(wire int32) []deltaUpdate {
	set := func(kv ...any) bson.D {
		d := bson.D{}
		for i := 0; i < len(kv); i += 2 {
			d = append(d, bson.E{Key: kv[i].(string), Value: kv[i+1]})
		}
		return d
	}
	u := []deltaUpdate{
		{"set top", func(r *rand.Rand) any { return set("$set", set("a", r.Intn(1000))) }},
		{"set new fields", func(r *rand.Rand) any {
			return set("$set", set("zz"+strconv.Itoa(r.Intn(3)), 1, "b"+strconv.Itoa(r.Intn(3)), 2, "m"+strconv.Itoa(r.Intn(3)), 3))
		}},
		{"set dotted", func(r *rand.Rand) any { return set("$set", set("nested.y.z.1", r.Intn(9), "nested.x", r.Intn(9))) }},
		{"set deep new", func(r *rand.Rand) any { return set("$set", set("nested.new.deep."+strconv.Itoa(r.Intn(3)), r.Intn(9))) }},
		{"set numeric key", func(r *rand.Rand) any { return set("$set", set("nested.1", r.Intn(9))) }},
		{"set array index", func(r *rand.Rand) any { return set("$set", set("arr."+strconv.Itoa(r.Intn(6)), r.Intn(9))) }},
		{"set array past end", func(r *rand.Rand) any { return set("$set", set("arr."+strconv.Itoa(5+r.Intn(4)), "far")) }},
		{"set in array doc", func(r *rand.Rand) any { return set("$set", set("objarr.1.n", r.Intn(9), "objarr.0.new", true)) }},
		{"unset", func(r *rand.Rand) any {
			return set("$unset", set([]string{"s", "nested.x", "zz0", "b1", "nested.new"}[r.Intn(5)], ""))
		}},
		{"unset array elem", func(r *rand.Rand) any { return set("$unset", set("arr.0", "")) }},
		{"inc", func(r *rand.Rand) any { return set("$inc", set("a", 1, "counter", int64(2))) }},
		{"push", func(r *rand.Rand) any { return set("$push", set("arr", r.Intn(9))) }},
		{"push slice", func(r *rand.Rand) any { return set("$push", set("arr", set("$each", bson.A{7, 8}, "$slice", -4))) }},
		{"pop last", func(r *rand.Rand) any { return set("$pop", set("arr", 1)) }},
		{"pop first", func(r *rand.Rand) any { return set("$pop", set("arr", -1)) }},
		{"pull", func(r *rand.Rand) any { return set("$pull", set("arr", r.Intn(9))) }},
		{"addToSet", func(r *rand.Rand) any { return set("$addToSet", set("arr", r.Intn(12))) }},
		{"rename", func(r *rand.Rand) any {
			if r.Intn(2) == 0 {
				return set("$rename", set("s", "s2"))
			}
			return set("$rename", set("s2", "s"))
		}},
		{"replace subdoc", func(r *rand.Rand) any {
			return set("$set", set("nested", set("x", r.Intn(9), "1", "one", "y", set("z", bson.A{1, 2}))))
		}},
		{"set whole array", func(r *rand.Rand) any { return set("$set", set("arr", bson.A{1, 2, 3, set("k", r.Intn(3))})) }},
		{"pipeline truncate", func(r *rand.Rand) any {
			return mongo.Pipeline{set("$set", set("arr", set("$slice", bson.A{"$arr", 1 + r.Intn(2)})))}
		}},
		{"pipeline set+unset", func(r *rand.Rand) any {
			return mongo.Pipeline{set("$set", set("p"+strconv.Itoa(r.Intn(3)), r.Intn(9), "a", set("$add", bson.A{"$a", 1}))), set("$unset", bson.A{"zz1"})}
		}},
		{"pipeline reorder", func(r *rand.Rand) any {
			// Moves "a" behind the other fields (and may change it).
			return mongo.Pipeline{set("$replaceWith", set("$mergeObjects", bson.A{"$$ROOT", set("a", set("$add", bson.A{"$a", r.Intn(2)}))})),
				set("$unset", bson.A{"a"}), set("$set", set("a", r.Intn(3)))}
		}},
		{"pipeline concat", func(r *rand.Rand) any {
			return mongo.Pipeline{set("$set", set("objarr", set("$concatArrays", bson.A{"$objarr", bson.A{set("n", r.Intn(9))}})))}
		}},
	}
	// 5.0+: $setField allows field names with '.'. Before 6.1 (no disambiguatedPaths)
	// they are indistinguishable from nested paths when the parent document exists:
	// auto mode uses lookup there, so only 6.1+ is exercised with them.
	if wire >= wireVersion61 {
		u = append(u, deltaUpdate{"setField dotted name", func(r *rand.Rand) any {
			return mongo.Pipeline{set("$replaceWith", set("$setField", set("field", "dot.ted", "input", "$$ROOT", "value", r.Intn(9))))}
		}}, deltaUpdate{"set after dotted", func(r *rand.Rand) any { return set("$set", set("dot.ted", r.Intn(9))) }},
			deltaUpdate{"set top (dotted doc)", func(r *rand.Rand) any { return set("$set", set("a", r.Intn(1000))) }})
	}
	return u
}
