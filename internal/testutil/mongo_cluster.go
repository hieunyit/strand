package testutil

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// MongoClusterOptions configures a replica set started with the docker CLI on the host
// network (members reach each other and the test process at 127.0.0.1:<port>, so
// failover, kill and restart behave as in a real deployment).
type MongoClusterOptions struct {
	Members int    // 1 or 3 (default 3)
	Image   string // default MongoImage()
	Auth    bool   // keyFile + SCRAM-SHA-256 root user (AdminUser/AdminPassword)
	TLS     bool   // requireTLS with a generated self-signed CA (CAFile, ClientCertFile for x.509)
}

// MongoCluster is a running replica set. Containers are named
// <STRAND_TEST_CONTAINER_PREFIX|strand-test>-mrs-<pid>-<n>-<member> and removed when the test ends.
type MongoCluster struct {
	t       testing.TB
	SetName string
	Ports   []int
	Names   []string
	Dir     string // keyfile / certificates (mounted at /cfg)
	opts    MongoClusterOptions

	AdminUser, AdminPassword string
	CAFile                   string // TLS: CA certificate (PEM)
	ClientCertFile           string // TLS: client certificate+key (PEM), subject ClientSubject
	ClientSubject            string
}

const (
	clusterAdminUser     = "root"
	clusterAdminPassword = "r00t-Pa$$:/@word"
)

// StartMongoCluster starts a replica set and waits for a primary.
func StartMongoCluster(t testing.TB, opts MongoClusterOptions) *MongoCluster {
	t.Helper()
	if opts.Members == 0 {
		opts.Members = 3
	}
	if opts.Image == "" {
		opts.Image = MongoImage()
	}
	prefix := os.Getenv("STRAND_TEST_CONTAINER_PREFIX")
	if prefix == "" {
		prefix = "strand-test"
	}
	seq := mongoSeq.Add(1)
	c := &MongoCluster{t: t, SetName: fmt.Sprintf("rs%d", seq), Dir: t.TempDir(), opts: opts}
	for i := 0; i < opts.Members; i++ {
		c.Ports = append(c.Ports, freePort(t))
		c.Names = append(c.Names, fmt.Sprintf("%s-mrs-%d-%d-%d", prefix, os.Getpid(), seq, i))
	}
	t.Cleanup(func() {
		for _, n := range c.Names {
			_ = exec.Command("docker", "rm", "-f", "-v", n).Run()
		}
	})
	if err := os.Chmod(c.Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if opts.Auth {
		key := make([]byte, 96)
		_, _ = rand.Read(key)
		if err := os.WriteFile(filepath.Join(c.Dir, "keyfile"), []byte(base64.StdEncoding.EncodeToString(key)), 0o400); err != nil {
			t.Fatal(err)
		}
		c.AdminUser, c.AdminPassword = clusterAdminUser, clusterAdminPassword
	}
	if opts.TLS {
		c.writeCerts()
	}
	var wg sync.WaitGroup
	errs := make([]error, len(c.Names))
	for i := range c.Names {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = c.run(i) }(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	c.initiate()
	return c
}

func freePort(t testing.TB) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func (c *MongoCluster) run(i int) error {
	args := []string{"run", "-d", "--name", c.Names[i], "--network", "host",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-v", c.Dir + ":/cfg:ro",
		c.opts.Image, "mongod", "--port", strconv.Itoa(c.Ports[i]), "--bind_ip", "127.0.0.1",
		"--replSet", c.SetName, "--dbpath", "/tmp", "--wiredTigerCacheSizeGB", "0.25", "--oplogSize", "1024"}
	if c.opts.Auth {
		args = append(args, "--keyFile", "/cfg/keyfile")
	}
	if c.opts.TLS {
		args = append(args, "--tlsMode", "requireTLS", "--tlsCertificateKeyFile", "/cfg/server.pem",
			"--tlsCAFile", "/cfg/ca.pem", "--tlsAllowConnectionsWithoutCertificates")
	}
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run %s: %v: %s", c.Names[i], err, out)
	}
	return nil
}

// Hosts returns the member list "127.0.0.1:p1,127.0.0.1:p2,...".
func (c *MongoCluster) Hosts() string {
	hs := make([]string, len(c.Ports))
	for i, p := range c.Ports {
		hs[i] = "127.0.0.1:" + strconv.Itoa(p)
	}
	return strings.Join(hs, ",")
}

// URI returns a replica-set URI for user:password (empty user: no credentials).
func (c *MongoCluster) URI(user, password string) string {
	auth := ""
	if user != "" {
		auth = url.UserPassword(user, password).String() + "@"
	}
	q := url.Values{}
	q.Set("replicaSet", c.SetName)
	if user != "" {
		q.Set("authSource", "admin")
	}
	c.tlsQuery(q)
	return "mongodb://" + auth + c.Hosts() + "/?" + q.Encode()
}

// AdminURI returns the URI of the root user (or no credentials without Auth).
func (c *MongoCluster) AdminURI() string { return c.URI(c.AdminUser, c.AdminPassword) }

func (c *MongoCluster) tlsQuery(q url.Values) {
	if c.opts.TLS {
		q.Set("tls", "true")
		q.Set("tlsCAFile", c.CAFile)
	}
}

func (c *MongoCluster) memberURI(i int, withAuth bool) string {
	q := url.Values{}
	q.Set("directConnection", "true")
	c.tlsQuery(q)
	auth := ""
	if withAuth && c.AdminUser != "" {
		auth = url.UserPassword(c.AdminUser, c.AdminPassword).String() + "@"
	}
	return "mongodb://" + auth + "127.0.0.1:" + strconv.Itoa(c.Ports[i]) + "/?" + q.Encode()
}

func (c *MongoCluster) initiate() {
	t := c.t
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cli := c.connectMember(ctx, 0, false)
	defer func() { _ = cli.Disconnect(context.Background()) }()
	members := bson.A{}
	for i, p := range c.Ports {
		members = append(members, bson.D{{Key: "_id", Value: i}, {Key: "host", Value: "127.0.0.1:" + strconv.Itoa(p)}})
	}
	cfg := bson.D{{Key: "_id", Value: c.SetName}, {Key: "members", Value: members}}
	for {
		err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetInitiate", Value: cfg}}).Err()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("replSetInitiate %s: %v", c.SetName, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if c.opts.Auth {
		// Localhost exception: the first user can be created on the primary without credentials.
		c.waitPrimaryDirect(ctx, cli)
		err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "createUser", Value: c.AdminUser}, {Key: "pwd", Value: c.AdminPassword},
			{Key: "roles", Value: bson.A{"root"}}, {Key: "mechanisms", Value: bson.A{"SCRAM-SHA-256"}}}).Err()
		if err != nil {
			t.Fatalf("create root user: %v", err)
		}
	}
	admin := c.Client()
	for {
		if err := admin.Ping(ctx, readpref.Primary()); err == nil {
			// All members up (secondaries replicating) so majority writes succeed.
			var st struct {
				Members []struct {
					StateStr string `bson:"stateStr"`
				} `bson:"members"`
			}
			if err := admin.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&st); err == nil {
				ok := 0
				for _, m := range st.Members {
					if m.StateStr == "PRIMARY" || m.StateStr == "SECONDARY" {
						ok++
					}
				}
				if ok == len(c.Ports) {
					return
				}
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("replica set %s did not become healthy", c.SetName)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (c *MongoCluster) connectMember(ctx context.Context, i int, withAuth bool) *mongo.Client {
	for {
		cli, err := mongo.Connect(options.Client().ApplyURI(c.memberURI(i, withAuth)).SetServerSelectionTimeout(2 * time.Second))
		if err == nil {
			if err = cli.Ping(ctx, readpref.PrimaryPreferred()); err == nil {
				return cli
			}
			_ = cli.Disconnect(context.Background())
		}
		if ctx.Err() != nil {
			c.t.Fatalf("connect member %d: %v", i, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (c *MongoCluster) waitPrimaryDirect(ctx context.Context, cli *mongo.Client) {
	for {
		var h struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}
		if err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h); err == nil && h.IsWritablePrimary {
			return
		}
		if ctx.Err() != nil {
			c.t.Fatal("no primary")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Client returns a new admin client (disconnected when the test ends).
func (c *MongoCluster) Client() *mongo.Client {
	cli, err := mongo.Connect(options.Client().ApplyURI(c.AdminURI()))
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	return cli
}

// Primary returns the index of the current primary (-1 if none answers within timeout).
func (c *MongoCluster) Primary(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		for i := range c.Ports {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			cli, err := mongo.Connect(options.Client().ApplyURI(c.memberURI(i, true)).SetServerSelectionTimeout(time.Second))
			if err == nil {
				var h struct {
					IsWritablePrimary bool `bson:"isWritablePrimary"`
				}
				err = cli.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h)
				_ = cli.Disconnect(context.Background())
				if err == nil && h.IsWritablePrimary {
					cancel()
					return i
				}
			}
			cancel()
		}
		if time.Now().After(deadline) {
			return -1
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// StepDown asks the current primary to step down (replSetStepDown) and returns its index.
func (c *MongoCluster) StepDown() int {
	p := c.Primary(time.Minute)
	if p < 0 {
		c.t.Fatal("step down: no primary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cli := c.connectMember(ctx, p, true)
	defer func() { _ = cli.Disconnect(context.Background()) }()
	err := cli.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetStepDown", Value: 30}, {Key: "secondaryCatchUpPeriodSecs", Value: 10}, {Key: "force", Value: true}}).Err()
	if err != nil && !isConnClosed(err) {
		c.t.Logf("replSetStepDown on member %d: %v", p, err)
	}
	return p
}

func isConnClosed(err error) bool {
	s := err.Error()
	return strings.Contains(s, "connection") || strings.Contains(s, "EOF") || mongo.IsNetworkError(err)
}

// KillPrimary sends SIGKILL to the primary's container and returns its index.
func (c *MongoCluster) KillPrimary() int {
	p := c.Primary(time.Minute)
	if p < 0 {
		c.t.Fatal("kill primary: no primary")
	}
	c.Kill(p)
	return p
}

// Kill sends SIGKILL to member i.
func (c *MongoCluster) Kill(i int) {
	if out, err := exec.Command("docker", "kill", "-s", "KILL", c.Names[i]).CombinedOutput(); err != nil {
		c.t.Fatalf("docker kill %s: %v: %s", c.Names[i], err, out)
	}
}

// Restart starts a stopped/killed member again (its data directory is kept).
func (c *MongoCluster) Restart(i int) {
	if out, err := exec.Command("docker", "start", c.Names[i]).CombinedOutput(); err != nil {
		c.t.Fatalf("docker start %s: %v: %s", c.Names[i], err, out)
	}
}

// WaitHealthy waits until every member is PRIMARY or SECONDARY.
func (c *MongoCluster) WaitHealthy(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	admin := c.Client()
	for {
		var st struct {
			Members []struct {
				StateStr string `bson:"stateStr"`
			} `bson:"members"`
		}
		if err := admin.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&st); err == nil {
			ok := 0
			for _, m := range st.Members {
				if m.StateStr == "PRIMARY" || m.StateStr == "SECONDARY" {
					ok++
				}
			}
			if ok == len(c.Ports) {
				return
			}
		}
		if ctx.Err() != nil {
			c.t.Fatalf("replica set %s not healthy within %s", c.SetName, timeout)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// Logs returns the container log of member i (for failure diagnostics).
func (c *MongoCluster) Logs(i int) string {
	out, _ := exec.Command("docker", "logs", "--tail", "50", c.Names[i]).CombinedOutput()
	return string(out)
}

func (c *MongoCluster) writeCerts() {
	t := c.t
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "strand-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	issue := func(serial int64, subject pkix.Name, server bool) []byte {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: subject,
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		if server {
			tmpl.ExtKeyUsage = append(tmpl.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
			tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			tmpl.DNSNames = []string{"localhost"}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, _ := x509.MarshalECPrivateKey(key)
		var b bytes.Buffer
		_ = pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: der})
		_ = pem.Encode(&b, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		return b.Bytes()
	}
	var caPEM bytes.Buffer
	_ = pem.Encode(&caPEM, &pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	c.CAFile = filepath.Join(c.Dir, "ca.pem")
	c.ClientCertFile = filepath.Join(c.Dir, "client.pem")
	c.ClientSubject = "CN=strand-x509,OU=cdc,O=strand-test"
	write := func(path string, b []byte) {
		if err := os.WriteFile(path, b, 0o444); err != nil {
			t.Fatal(err)
		}
	}
	write(c.CAFile, caPEM.Bytes())
	// The members use one certificate (O/OU/DC identical: required for membership auth).
	write(filepath.Join(c.Dir, "server.pem"), issue(2, pkix.Name{CommonName: "127.0.0.1", Organization: []string{"strand-test-server"}}, true))
	write(c.ClientCertFile, issue(3, pkix.Name{CommonName: "strand-x509", OrganizationalUnit: []string{"cdc"}, Organization: []string{"strand-test"}}, false))
}
