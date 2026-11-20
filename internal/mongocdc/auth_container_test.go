package mongocdc

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"strand/internal/testutil"
)

// Least-privilege roles documented in docs/mongodb-cdc.md.
func sourceRole(db string) bson.A {
	return bson.A{
		bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: db}, {Key: "collection", Value: ""}}},
			{Key: "actions", Value: bson.A{"find", "changeStream", "listCollections", "listIndexes", "collStats"}}},
		bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: "local"}, {Key: "collection", Value: "oplog.rs"}}},
			{Key: "actions", Value: bson.A{"find", "collStats"}}},
	}
}

func targetRole(db string) bson.A {
	return bson.A{
		bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: db}, {Key: "collection", Value: ""}}},
			{Key: "actions", Value: bson.A{"find", "insert", "update", "remove", "createCollection", "dropCollection", "renameCollectionSameDB",
				"listCollections", "createIndex", "dropIndex", "listIndexes", "collMod", "bypassDocumentValidation"}}},
		bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: stateDatabase}, {Key: "collection", Value: ""}}},
			{Key: "actions", Value: bson.A{"find", "insert", "update", "remove", "createCollection", "listCollections", "createIndex"}}},
	}
}

func createRoleUser(t *testing.T, c *mongo.Client, role string, privs bson.A, user, pwd string) {
	t.Helper()
	ctx := context.Background()
	admin := c.Database("admin")
	if err := admin.RunCommand(ctx, bson.D{{Key: "createRole", Value: role}, {Key: "privileges", Value: privs}, {Key: "roles", Value: bson.A{}}}).Err(); err != nil {
		t.Fatalf("createRole %s: %v", role, err)
	}
	if pwd == "" {
		return
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "createUser", Value: user}, {Key: "pwd", Value: pwd}, {Key: "roles", Value: bson.A{role}},
		{Key: "mechanisms", Value: bson.A{"SCRAM-SHA-256"}}}).Err(); err != nil {
		t.Fatalf("createUser %s: %v", user, err)
	}
}

// SCRAM-SHA-256 over TLS (self-signed CA) with least-privilege custom roles on both
// sides, x.509 client-certificate authentication on the source, preflight privilege
// checks (granted and missing), and a full snapshot + stream run.
func TestContainer_AuthTLSLeastPrivilege(t *testing.T) {
	src := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 1, Auth: true, TLS: true})
	dst := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 1, Auth: true, TLS: true})
	sc, dc := src.Client(), dst.Client()
	ctx := context.Background()
	db := "secure"
	createRoleUser(t, sc, "strandSource", sourceRole(db), "cdc_src", "s3cr3t:/@pw")
	createRoleUser(t, dc, "strandTarget", targetRole(db+"_r"), "cdc_dst", "t4rg3t#pw")
	createRoleUser(t, dc, "tooWeak", bson.A{bson.D{{Key: "resource", Value: bson.D{{Key: "db", Value: db + "_r"}, {Key: "collection", Value: ""}}},
		{Key: "actions", Value: bson.A{"find", "insert"}}}}, "weak", "w34kpw")
	// x.509 user on the source: subject of the generated client certificate.
	if err := sc.Database("$external").RunCommand(ctx, bson.D{{Key: "createUser", Value: src.ClientSubject},
		{Key: "roles", Value: bson.A{bson.D{{Key: "role", Value: "strandSource"}, {Key: "db", Value: "admin"}}}}}).Err(); err != nil {
		t.Fatalf("create x.509 user: %v", err)
	}
	for i, c := range []string{"a", "b"} {
		seedDocs(t, sc.Database(db), c, 2000, int64(i), intID)
	}
	scram := func(u, pw string, c *testutil.MongoCluster) string {
		return strings.Replace(c.URI(u, pw), "authSource=admin", "authSource=admin&authMechanism=SCRAM-SHA-256", 1)
	}
	q := url.Values{}
	q.Set("replicaSet", src.SetName)
	q.Set("tls", "true")
	q.Set("tlsCAFile", src.CAFile)
	q.Set("tlsCertificateKeyFile", src.ClientCertFile)
	q.Set("authMechanism", "MONGODB-X509")
	x509URI := "mongodb://" + src.Hosts() + "/?" + q.Encode()

	cfg := Config{Pipeline: "sec", SourceURI: x509URI, TargetURI: scram("cdc_dst", "t4rg3t#pw", dst), SourceDB: db, TargetDB: db + "_r",
		Include: []string{db + ".*"}, Logger: quietLogger(t), ReplicateIndexes: true,
		LeaseDuration: 5 * time.Second, CheckpointEvery: 200 * time.Millisecond, MaxAwait: 100 * time.Millisecond}
	checks, _, err := Preflight(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range checks {
		t.Logf("[%s] %s: %s", c.Status, c.Name, c.Detail)
		if c.Status == CheckFail || (strings.Contains(c.Name, "privileges") && c.Status != CheckOK) {
			t.Fatalf("preflight with least-privilege roles: %s %s", c.Name, c.Detail)
		}
		if strings.Contains(c.Detail, "t4rg3t") {
			t.Fatal("password leaked in preflight output")
		}
	}
	weak := cfg
	weak.TargetURI = scram("weak", "w34kpw", dst)
	checks, _, err = Preflight(ctx, weak)
	if err != nil {
		t.Fatal(err)
	}
	failed := false
	for _, c := range checks {
		if c.Name == "target privileges" && c.Status == CheckFail && strings.Contains(c.Detail, "changeStream") == false && strings.Contains(c.Detail, "remove") {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("missing target privileges not reported: %+v", checks)
	}

	// Also SCRAM on the source.
	for _, srcURI := range []string{x509URI, scram("cdc_src", "s3cr3t:/@pw", src)} {
		c := cfg
		c.SourceURI = srcURI
		c.Pipeline = "sec-" + map[bool]string{true: "x509", false: "scram"}[srcURI == x509URI]
		c.TargetDB = db + "_r"
		c.OverwriteTarget = true
		p := &pair{srcURI: srcURI, dstURI: c.TargetURI, srcC: sc, dstC: dc}
		r := start(c)
		load := startLoad(t, sc.Database(db), []string{"a", "b"}, 2500, 2, 5, intID)
		time.Sleep(3 * time.Second)
		ops := load.halt(t)
		waitIdenticalDBs(t, p, db, db+"_r", 2*time.Minute, r)
		r.stop(t)
		t.Logf("%s: %d concurrent ops replicated over TLS with least privileges", c.Pipeline, ops)
	}
}

// waitIdenticalDBs is waitIdentical for different source and target database names.
func waitIdenticalDBs(t *testing.T, p *pair, srcDB, dstDB string, timeout time.Duration, alive ...*running) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		for _, r := range alive {
			r.failIfExited(t)
		}
		a, errA := dbDigest(ctx, p.srcC.Database(srcDB))
		b, errB := dbDigest(ctx, p.dstC.Database(dstDB))
		last := ""
		if errA != nil || errB != nil {
			last = "read error"
		} else if last = diffDigest(a, b); last == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not converged: %s %v %v", last, errA, errB)
		}
		time.Sleep(300 * time.Millisecond)
	}
}
