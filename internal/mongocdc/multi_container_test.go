package mongocdc

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// waitSame polls until source database src and target database dst are byte-identical.
func (p *pair) waitSame(t *testing.T, src, dst string, timeout time.Duration) int {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		a, errA := dbDigest(ctx, p.srcC.Database(src))
		b, errB := dbDigest(ctx, p.dstC.Database(dst))
		if errA != nil || errB != nil {
			last = fmt.Sprintf("read: %v %v", errA, errB)
		} else if last = diffDigest(a, b); last == "" {
			return countDocs(a)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s -> %s did not converge within %s: %s", src, dst, timeout, last)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func writeMultiConfig(t *testing.T, f *pipelineFixture, name, source, target, include string) {
	t.Helper()
	dir := strings.TrimSuffix(f.cfg, "/p.yaml")
	yaml := fmt.Sprintf(`version: 1
name: %s
source:
  kind: mongodb
  uri_env: MGP_SRC_URI
%s
target:
  kind: mongodb
  uri_env: MGP_DST_URI
%s
tables:
  include: [%s]
bootstrap:
  logical: { max_parallel: 4, chunk_rows: "200" }
checkpoint: { flush_every: 200ms }
mongodb:
  lease_ttl: 6s
resources:
  spool: { path: %s }
observability:
  metrics: "%s"
  admin_socket: %s
`, name, source, target, include, dir, f.metrics, f.sock)
	if err := os.WriteFile(f.cfg, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
}

// One pipeline replicates several databases: a pipeline started with one database
// gets a second one (mapped to another target name) without copying the first again,
// both follow concurrent writes, and top/tables/verify/repair name collections
// "database.collection" (the same collection name exists in both databases).
func TestContainer_SeveralDatabases(t *testing.T) {
	bin := buildStrand(t)
	p := newPair(t)
	ctx := context.Background()
	seedDocs(t, p.srcC.Database("ma"), "a", 2000, 1, intID)
	seedDocs(t, p.srcC.Database("ma"), "b", 1000, 2, intID)
	seedDocs(t, p.srcC.Database("mb"), "a", 1500, 3, intID)
	seedDocs(t, p.srcC.Database("mb"), "c", 500, 4, intID)

	f := newPipelineFixture(t, bin, "mp", p.srcURI, p.dstURI, "ma", "")
	writeMultiConfig(t, f, "mp", "  database: ma", "", `"ma.*"`)
	if out, err := f.run("sync", "--background"); err != nil {
		t.Fatalf("sync one database: %v\n%s", err, out)
	}
	t.Cleanup(func() { _, _ = f.run("stop") })
	f.waitState("synced", time.Minute)
	p.waitSame(t, "ma", "ma", time.Minute)
	if out, err := f.run("stop"); err != nil {
		t.Fatalf("stop: %v\n%s", err, out)
	}

	// Add mb (copied to mb_copy). ma's target collections exist and overwrite_target is
	// off: a fresh snapshot of ma would be refused, so ma must resume its checkpoint.
	writeMultiConfig(t, f, "mp", "  databases: [ma, mb]", "  databases: { mb: mb_copy }", `"ma.*", "mb.*"`)
	out, err := f.run("validate")
	if err != nil || !strings.Contains(out, "OK") || !strings.Contains(out, "ma: checkpoint found") || !strings.Contains(out, "mb: no selected collection exists") {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	if out, err = f.run("plan"); err != nil || !strings.Contains(out, "mb.a") || !strings.Contains(out, "ma.b") || !strings.Contains(out, "ma, mb -> mb_copy") {
		t.Fatalf("plan: %v\n%s", err, out)
	}
	if out, err = f.run("sync", "--background"); err != nil {
		t.Fatalf("sync several databases: %v\n%s", err, out)
	}
	f.waitState("synced", time.Minute)
	p.waitSame(t, "ma", "ma", time.Minute)
	p.waitSame(t, "mb", "mb_copy", time.Minute)
	if n, _ := p.dstC.Database("mb").Collection("a").CountDocuments(ctx, bson.D{}); n != 0 {
		t.Fatalf("mb was written under its source name: %d documents", n)
	}
	var ids []string
	cur, err := p.dstC.Database(stateDatabase).Collection(stateCollection).Find(ctx, bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	for cur.Next(ctx) {
		ids = append(ids, cur.Current.Lookup("_id").StringValue()+"="+cur.Current.Lookup("source_db").StringValue())
	}
	if strings.Join(ids, ",") != "mp=ma,mp/mb=mb" && strings.Join(ids, ",") != "mp/mb=mb,mp=ma" {
		t.Fatalf("checkpoints %v", ids)
	}

	loadA := startLoad(t, p.srcC.Database("ma"), []string{"a", "b"}, 3000, 2, 11, intID)
	loadB := startLoad(t, p.srcC.Database("mb"), []string{"a", "c"}, 3000, 2, 12, intID)
	time.Sleep(3 * time.Second)
	opsA, opsB := loadA.halt(t), loadB.halt(t)
	p.waitSame(t, "ma", "ma", time.Minute)
	p.waitSame(t, "mb", "mb_copy", time.Minute)
	t.Logf("concurrent ops: ma %d, mb %d", opsA, opsB)

	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	top := f.waitState("synced", 30*time.Second)
	if top.PipelineID != "mp" {
		t.Fatalf("top %+v", top)
	}
	tables, err := f.admin().Tables(actx)
	if err != nil || len(tables) != 4 {
		t.Fatalf("tables: %v %+v", err, tables)
	}
	names := map[string]bool{}
	for _, tb := range tables {
		names[tb.Table] = true
	}
	for _, want := range []string{"ma.a", "ma.b", "mb.a", "mb.c"} {
		if !names[want] {
			t.Fatalf("tables lack %s: %+v", want, tables)
		}
	}
	if err := f.admin().SnapshotAdd(actx, "a"); err == nil || !strings.Contains(err.Error(), "database.collection") {
		t.Fatalf("bare collection name must be refused: %v", err)
	}
	if m := f.metricsText(); !strings.Contains(m, `strand_mongodb_snapshot_docs_total{pipeline="mp/mb"} 2000`) {
		t.Fatalf("metrics:\n%s", grepLines(m, "snapshot_docs"))
	}

	if out, err = f.run("verify", "--mode", "full"); err != nil || strings.Count(out, "OK ") != 4 || !strings.Contains(out, "mb.c:") {
		t.Fatalf("verify: %v\n%s", err, out)
	}
	if _, err := p.dstC.Database("mb_copy").Collection("a").DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: 7}}}}); err != nil {
		t.Fatal(err)
	}
	if out, err = f.run("verify", "--mode", "full"); err == nil || !strings.Contains(out, "differences in: mb.a") || strings.Count(out, "OK ") != 3 {
		t.Fatalf("verify must find the damage in mb.a only: %v\n%s", err, out)
	}
	if out, err = f.run("verify", "--mode", "full", "--table", "a"); err == nil || !strings.Contains(out, "database.collection") {
		t.Fatalf("bare --table: %v\n%s", err, out)
	}
	if out, err = f.run("verify", "--mode", "full", "--table", "mb.a", "--repair", "fix"); err != nil || !strings.Contains(out, "re-copy of mb.a") {
		t.Fatalf("repair: %v\n%s", err, out)
	}
	p.waitSame(t, "mb", "mb_copy", time.Minute)
	f.waitState("synced", 30*time.Second)
	if out, err = f.run("verify", "--mode", "full"); err != nil {
		t.Fatalf("verify after repair: %v\n%s", err, out)
	}
	if out, err = f.run("stop"); err != nil {
		t.Fatalf("stop: %v\n%s", err, out)
	}
}
