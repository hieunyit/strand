package mongocdc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/testutil"
)

func init() {
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

type pair struct {
	src, dst       *testutil.MongoRS
	srcC, dstC     *mongo.Client
	srcURI, dstURI string
}

func newPair(t *testing.T) *pair {
	t.Helper()
	var p pair
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.src = testutil.MongoReplicaSet(t) }()
	go func() { defer wg.Done(); p.dst = testutil.MongoReplicaSet(t) }()
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	p.srcURI, p.dstURI = p.src.URI, p.dst.URI
	p.srcC = mustClient(t, p.srcURI)
	p.dstC = mustClient(t, p.dstURI)
	return &p
}

func mustClient(t testing.TB, uri string) *mongo.Client {
	t.Helper()
	c, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Disconnect(context.Background()) })
	return c
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func testSeed(t *testing.T) int64 {
	if v, err := strconv.ParseInt(os.Getenv("SEED"), 10, 64); err == nil {
		t.Logf("SEED=%d", v)
		return v
	}
	v := time.Now().UnixNano()
	t.Logf("SEED=%d (set SEED to replay)", v)
	return v
}

func quietLogger(t testing.TB) *slog.Logger {
	if os.Getenv("STRAND_TEST_VERBOSE") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (p *pair) cfg(t testing.TB, pipeline, db string) Config {
	return Config{
		Pipeline: pipeline, SourceURI: p.srcURI, TargetURI: p.dstURI, SourceDB: db, TargetDB: db,
		Include: []string{db + ".*"}, Logger: quietLogger(t),
		LeaseDuration: 3 * time.Second, CheckpointEvery: 200 * time.Millisecond, MaxAwait: 100 * time.Millisecond,
	}
}

type running struct {
	cancel context.CancelFunc
	done   chan error
}

func start(cfg Config) *running {
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- Run(ctx, cfg) }()
	return r
}

func (r *running) stop(t testing.TB) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("sync stopped with error: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("sync did not stop")
	}
}

// failIfExited fails when the sync goroutine ended unexpectedly.
func (r *running) failIfExited(t testing.TB) {
	t.Helper()
	select {
	case err := <-r.done:
		t.Fatalf("sync exited early: %v", err)
	default:
	}
}

// dbDigest returns collection -> (_id bytes -> sha256(document bytes)). Documents are
// compared byte for byte: field order and every BSON type must survive.
func dbDigest(ctx context.Context, db *mongo.Database) (map[string]map[string][32]byte, error) {
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string][32]byte)
	for _, name := range names {
		if strings.HasPrefix(name, "system.") {
			continue
		}
		cur, err := db.Collection(name).Find(ctx, bson.D{})
		if err != nil {
			return nil, err
		}
		m := make(map[string][32]byte)
		for cur.Next(ctx) {
			id := cur.Current.Lookup("_id")
			key := string([]byte{byte(id.Type)}) + string(id.Value)
			if _, dup := m[key]; dup {
				_ = cur.Close(ctx)
				return nil, fmt.Errorf("%s: duplicate _id %s", name, id)
			}
			m[key] = sha256.Sum256(cur.Current)
		}
		if err := cur.Err(); err != nil {
			return nil, err
		}
		_ = cur.Close(ctx)
		out[name] = m
	}
	return out, nil
}

func diffDigest(a, b map[string]map[string][32]byte) string {
	var diffs []string
	names := map[string]bool{}
	for n := range a {
		names[n] = true
	}
	for n := range b {
		names[n] = true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	for _, n := range sorted {
		ca, oka := a[n]
		cb, okb := b[n]
		if !oka || !okb {
			diffs = append(diffs, fmt.Sprintf("collection %s: source=%v target=%v", n, oka, okb))
			continue
		}
		missing, extra, changed := 0, 0, 0
		for k, h := range ca {
			if hb, ok := cb[k]; !ok {
				missing++
			} else if hb != h {
				changed++
			}
		}
		for k := range cb {
			if _, ok := ca[k]; !ok {
				extra++
			}
		}
		if missing+extra+changed > 0 {
			diffs = append(diffs, fmt.Sprintf("%s: src=%d dst=%d missing=%d extra=%d changed=%d", n, len(ca), len(cb), missing, extra, changed))
		}
	}
	return strings.Join(diffs, "; ")
}

func countDocs(d map[string]map[string][32]byte) int {
	n := 0
	for _, c := range d {
		n += len(c)
	}
	return n
}

// waitIdentical polls until source and target databases are byte-identical.
func (p *pair) waitIdentical(t testing.TB, db string, timeout time.Duration, alive ...*running) int {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		for _, r := range alive {
			r.failIfExited(t)
		}
		a, err := dbDigest(ctx, p.srcC.Database(db))
		if err != nil {
			t.Fatal(err)
		}
		b, err := dbDigest(ctx, p.dstC.Database(db))
		if err != nil {
			t.Fatal(err)
		}
		last = diffDigest(a, b)
		if last == "" {
			return countDocs(a)
		}
		if time.Now().After(deadline) {
			t.Fatalf("target did not converge within %s: %s", timeout, last)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// richDoc builds a document exercising BSON type fidelity.
func richDoc(r *rand.Rand, id any, v int) bson.D {
	dec, _ := bson.ParseDecimal128(fmt.Sprintf("%d.%03d", v, r.Intn(1000)))
	oid := bson.NewObjectID()
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "v", Value: int32(v)},
		{Key: "i64", Value: int64(v) << 33},
		{Key: "f", Value: float64(v) / 7},
		{Key: "nan", Value: math.NaN()},
		{Key: "negzero", Value: math.Copysign(0, -1)},
		{Key: "dec", Value: dec},
		{Key: "date", Value: bson.NewDateTimeFromTime(time.UnixMilli(int64(v) * 1000003).UTC())},
		{Key: "oid", Value: oid},
		{Key: "bin", Value: bson.Binary{Subtype: 0x00, Data: []byte{byte(v), 0, 1, 2}}},
		{Key: "uuid", Value: bson.Binary{Subtype: 0x04, Data: bytes.Repeat([]byte{byte(v)}, 16)}},
		{Key: "md5", Value: bson.Binary{Subtype: 0x05, Data: bytes.Repeat([]byte{7}, 16)}},
		{Key: "user", Value: bson.Binary{Subtype: 0x80, Data: []byte("custom")}},
		{Key: "ts", Value: bson.Timestamp{T: uint32(v), I: 3}},
		{Key: "re", Value: bson.Regex{Pattern: "^a.*", Options: "i"}},
		{Key: "js", Value: bson.JavaScript("function(){return 1}")},
		{Key: "min", Value: bson.MinKey{}},
		{Key: "max", Value: bson.MaxKey{}},
		{Key: "null", Value: nil},
		{Key: "s", Value: strings.Repeat("ż", r.Intn(40))},
		{Key: "nested", Value: bson.D{{Key: "z", Value: 1}, {Key: "a", Value: bson.A{bson.A{1, "x", bson.A{}}, bson.D{{Key: "k", Value: v}}}}}},
		{Key: "arr", Value: bson.A{1, 2, 3, int64(4), "five"}},
	}
}

// mixedIDs returns _id values of many BSON types (sorted by MongoDB across type brackets).
func mixedID(i int) any {
	switch i % 7 {
	case 0:
		return int32(i)
	case 1:
		return fmt.Sprintf("s%06d", i)
	case 2:
		return int64(i) * 1000
	case 3:
		d, _ := bson.ParseDecimal128(fmt.Sprintf("%d.5", i))
		return d
	case 4:
		return bson.D{{Key: "b", Value: i}, {Key: "a", Value: "x"}}
	case 5:
		return bson.Binary{Subtype: 0x04, Data: []byte(fmt.Sprintf("%016d", i))}
	default:
		var oid bson.ObjectID
		copy(oid[:], fmt.Sprintf("%012d", i))
		return oid
	}
}

// loadGen applies random insert/update/replace/delete traffic until stopped.
type loadGen struct {
	stop chan struct{}
	wg   sync.WaitGroup
	ops  atomic.Int64
	errs atomic.Int64
}

func startLoad(t *testing.T, db *mongo.Database, colls []string, keyspace int, workers int, seed int64, idFn func(int) any) *loadGen {
	g := &loadGen{stop: make(chan struct{})}
	for w := 0; w < workers; w++ {
		g.wg.Add(1)
		go func(s int64) {
			defer g.wg.Done()
			r := rand.New(rand.NewSource(s))
			ctx := context.Background()
			for {
				select {
				case <-g.stop:
					return
				default:
				}
				c := db.Collection(colls[r.Intn(len(colls))])
				k := r.Intn(keyspace)
				id := idFn(k)
				var err error
				switch op := r.Intn(10); {
				case op < 3:
					_, err = c.InsertOne(ctx, richDoc(r, id, r.Intn(1e6)))
					if mongo.IsDuplicateKeyError(err) {
						err = nil
					}
				case op < 6:
					var upd bson.D
					switch r.Intn(5) {
					case 0:
						upd = bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: int32(r.Intn(1e6))}, {Key: "nested.a.1.k", Value: r.Intn(100)}}}}
					case 1:
						upd = bson.D{{Key: "$unset", Value: bson.D{{Key: "f", Value: ""}}}, {Key: "$inc", Value: bson.D{{Key: "i64", Value: int64(1)}}}}
					case 2:
						upd = bson.D{{Key: "$push", Value: bson.D{{Key: "arr", Value: bson.D{{Key: "$each", Value: bson.A{r.Intn(9), "p"}}}}}}}
					case 3:
						upd = bson.D{{Key: "$pop", Value: bson.D{{Key: "arr", Value: 1}}}}
					default:
						upd = bson.D{{Key: "$set", Value: bson.D{{Key: "arr", Value: bson.A{r.Intn(3)}}, {Key: "newfield." + strconv.Itoa(r.Intn(3)), Value: true}}}}
					}
					_, err = c.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, upd)
				case op < 8:
					doc := richDoc(r, id, r.Intn(1e6))
					_, err = c.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, doc[1:], options.Replace().SetUpsert(r.Intn(2) == 0))
				default:
					_, err = c.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
				}
				if err != nil {
					g.errs.Add(1)
				} else {
					g.ops.Add(1)
				}
			}
		}(seed + int64(w))
	}
	return g
}

func (g *loadGen) halt(t testing.TB) int64 {
	close(g.stop)
	g.wg.Wait()
	if e := g.errs.Load(); e > 0 {
		t.Logf("load generator: %d failed ops (ignored)", e)
	}
	return g.ops.Load()
}

func seedDocs(t testing.TB, db *mongo.Database, coll string, n int, seed int64, idFn func(int) any) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	docs := make([]any, 0, 1000)
	for i := 0; i < n; i++ {
		docs = append(docs, richDoc(r, idFn(i), i))
		if len(docs) == cap(docs) || i == n-1 {
			if _, err := db.Collection(coll).InsertMany(ctx, docs); err != nil {
				t.Fatal(err)
			}
			docs = docs[:0]
		}
	}
}

func intID(i int) any { return int32(i) }

// testutilPairWithOplog starts a source with the minimum oplog size (990 MB) and a target.
func testutilPairWithOplog(t *testing.T) (string, string) {
	var src, dst *testutil.MongoRS
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); src = testutil.MongoReplicaSet(t, "--oplogSize", "990") }()
	go func() { defer wg.Done(); dst = testutil.MongoReplicaSet(t) }()
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	return src.URI, dst.URI
}
