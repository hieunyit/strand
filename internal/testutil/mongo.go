package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoImage is the MongoDB image for container tests (override: STRAND_TEST_MONGO_IMAGE).
func MongoImage() string {
	if v := os.Getenv("STRAND_TEST_MONGO_IMAGE"); v != "" {
		return v
	}
	return "mongo:8.0"
}

var mongoSeq atomic.Int64

// MongoRS is a single-node replica set in its own container (change streams and
// transactions need a replica set). URI uses directConnection so the member's
// in-container host name does not matter.
type MongoRS struct {
	URI       string
	Container testcontainers.Container
}

// MongoReplicaSet starts a fresh single-node replica set; the container is removed when
// the test ends. Container names are <STRAND_TEST_CONTAINER_PREFIX|strand-test>-mongo-<pid>-<n>.
func MongoReplicaSet(t testing.TB, extraArgs ...string) *MongoRS {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	prefix := os.Getenv("STRAND_TEST_CONTAINER_PREFIX")
	if prefix == "" {
		prefix = "strand-test"
	}
	name := fmt.Sprintf("%s-mongo-%d-%d", prefix, os.Getpid(), mongoSeq.Add(1))
	cmd := append([]string{"mongod", "--replSet", "rs0", "--bind_ip_all"}, extraArgs...)
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Name: name, Image: MongoImage(), ExposedPorts: []string{"27017/tcp"}, Cmd: cmd,
		WaitingFor: wait.ForListeningPort("27017/tcp").WithStartupTimeout(2 * time.Minute),
	}, Started: true})
	if c != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := c.Terminate(ctx); err != nil {
				t.Errorf("terminate MongoDB container: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "27017/tcp")
	if err != nil {
		t.Fatal(err)
	}
	uri := "mongodb://" + net.JoinHostPort(host, port.Port()) + "/?directConnection=true"
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	cfg := bson.D{{Key: "_id", Value: "rs0"}, {Key: "members", Value: bson.A{bson.D{{Key: "_id", Value: 0}, {Key: "host", Value: "localhost:27017"}}}}}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: cfg}}).Err(); err != nil {
		t.Fatalf("replSetInitiate: %v", err)
	}
	for {
		var h struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}
		if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h); err == nil && h.IsWritablePrimary {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("MongoDB replica set did not elect a primary")
		}
		time.Sleep(200 * time.Millisecond)
	}
	return &MongoRS{URI: uri, Container: c}
}
