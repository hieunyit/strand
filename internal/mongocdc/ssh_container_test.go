package mongocdc

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"strand/internal/testutil"
	"strand/internal/tunnel"
	"strand/internal/tunnel/tunneltest"
)

func TestContainer_SSHReplicaSetDiscoveryAndCDC(t *testing.T) {
	source := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 3, TLS: true})
	target := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 1})
	p := &pair{srcURI: source.AdminURI(), dstURI: target.AdminURI()}
	p.srcC, p.dstC = mustClient(t, p.srcURI), mustClient(t, p.dstURI)
	cfg := p.cfg(t, "ssh-cdc", "ssh_data")
	uri, err := url.Parse(cfg.SourceURI)
	if err != nil {
		t.Fatal(err)
	}
	uri.Host, _, _ = strings.Cut(uri.Host, ",")
	cfg.SourceURI = uri.String() // the driver must find the other hosts via hello
	cfg.LeaseDuration = 20 * time.Second
	bastion := tunneltest.StartSSHServer(t, false)
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", "")
	dial := func() *tunnel.Dialer {
		key, err := tunnel.ProbeHostKey(t.Context(), bastion.Addr())
		if err != nil {
			t.Fatal(err)
		}
		d := tunnel.NewDialer(t.Context(), func(context.Context) (tunnel.Config, error) {
			identity, err := os.ReadFile(bastion.IdentityFile())
			return tunnel.Config{SSHAddress: bastion.Addr(), SSHUser: "strand", HostKey: key, PrivateKey: identity}, err
		})
		t.Cleanup(d.Close)
		return d
	}
	cfg.SourceDialer, cfg.TargetDialer = dial(), dial()
	coll := p.srcC.Database(cfg.SourceDB).Collection("orders")
	if _, err := coll.InsertOne(t.Context(), bson.D{{Key: "_id", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	checks, _, err := Preflight(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range checks {
		if check.Status == CheckFail {
			t.Fatalf("preflight over SSH: %+v", check)
		}
	}
	r := start(cfg)
	t.Cleanup(r.cancel)
	p.waitIdentical(t, cfg.SourceDB, 30*time.Second, r)
	if _, err := coll.InsertOne(t.Context(), bson.D{{Key: "_id", Value: 2}}); err != nil {
		t.Fatal(err)
	}
	p.waitIdentical(t, cfg.SourceDB, 30*time.Second, r)
	r.stop(t)
	for _, port := range append(source.Ports, target.Ports...) {
		want := "127.0.0.1:" + strconv.Itoa(port)
		seen := false
		for _, requested := range bastion.Requested() {
			seen = seen || requested == want
		}
		if !seen {
			t.Errorf("MongoDB member %s was not routed through SSH", want)
		}
	}
}
