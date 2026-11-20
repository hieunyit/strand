package mongocdc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"strand/internal/admin"
)

// pipelineFixture is a MongoDB pipeline driven through the strand binary and a config
// file, exactly as an operator (or the Web UI) runs it.
type pipelineFixture struct {
	t        *testing.T
	bin, cfg string
	sock     string
	metrics  string
	env      []string
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newPipelineFixture(t *testing.T, bin, name, srcURI, dstURI, db, extra string) *pipelineFixture {
	dir, err := os.MkdirTemp("", "mgp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f := &pipelineFixture{t: t, bin: bin, cfg: filepath.Join(dir, "p.yaml"), sock: filepath.Join(dir, "a.sock"), metrics: freeAddr(t)}
	f.env = append(os.Environ(), "MGP_SRC_URI="+srcURI, "MGP_DST_URI="+dstURI)
	yaml := fmt.Sprintf(`version: 1
name: %s
source:
  kind: mongodb
  uri_env: MGP_SRC_URI
  database: %s
target:
  kind: mongodb
  uri_env: MGP_DST_URI
tables:
  include: ["%s.*"]
bootstrap:
  logical: { max_parallel: 3, chunk_rows: "200" }
checkpoint: { flush_every: 200ms }
mongodb:
  lease_ttl: 6s
%s
resources:
  spool: { path: %s }
observability:
  metrics: "%s"
  admin_socket: %s
`, name, db, db, extra, dir, f.metrics, f.sock)
	if err := os.WriteFile(f.cfg, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *pipelineFixture) run(args ...string) (string, error) {
	cmd := exec.Command(f.bin, append(args, "-c", f.cfg)...)
	cmd.Env = f.env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (f *pipelineFixture) admin() *admin.Client { return admin.NewClient(f.sock) }

func (f *pipelineFixture) metricsText() string {
	resp, err := http.Get("http://" + f.metrics + "/metrics")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func (f *pipelineFixture) waitState(want string, timeout time.Duration) *admin.TopInfo {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	var last *admin.TopInfo
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		top, err := f.admin().Top(ctx)
		cancel()
		if err == nil {
			last = top
			if top.State == want {
				return top
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	f.t.Fatalf("state %q not reached; last top %+v", want, last)
	return nil
}

// `strand validate/plan/sync --background/top/status/stop/verify` and the admin socket
// protocol of SQL pipelines (top, status, tables, events, checkpoint, pause/resume,
// snapshot add) plus /metrics, on a MongoDB pipeline defined by the normal config.
func TestContainer_FirstClassPipeline(t *testing.T) {
	bin := buildStrand(t)
	p := newPair(t)
	ctx := context.Background()
	db := "fc"
	for _, c := range []string{"a", "b", "c"} {
		seedDocs(t, p.srcC.Database(db), c, 3000, 1, intID)
	}
	f := newPipelineFixture(t, bin, "fc-pipe", p.srcURI, p.dstURI, db, "")
	out, err := f.run("validate")
	if err != nil || !strings.Contains(out, "OK") || !strings.Contains(out, "[ok] source topology") {
		t.Fatalf("validate: %v\n%s", err, out)
	}
	t.Logf("validate:\n%s", out)
	if out, err = f.run("plan"); err != nil || !strings.Contains(out, "estimated copy") {
		t.Fatalf("plan: %v\n%s", err, out)
	}
	if out, err = f.run("sync", "--background"); err != nil || !strings.Contains(out, "started in the background") {
		t.Fatalf("sync --background: %v\n%s", err, out)
	}
	t.Cleanup(func() { _, _ = f.run("stop") })
	f.waitState("synced", time.Minute)
	p.waitIdentical(t, db, time.Minute)

	load := startLoad(t, p.srcC.Database(db), []string{"a", "b"}, 4000, 3, 7, intID)
	time.Sleep(2 * time.Second)
	top := f.waitState("synced", 30*time.Second)
	if top.PipelineID != "fc-pipe" {
		t.Fatalf("top: %+v", top)
	}
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	st, err := f.admin().Status(actx)
	if err != nil || st.SourceKind != "mongodb" || st.AppliedRows == 0 || st.AppliedTxns == 0 {
		t.Fatalf("status: %v %+v", err, st)
	}
	tables, err := f.admin().Tables(actx)
	if err != nil || len(tables) != 3 || tables[0].State != CollDone || tables[0].RowsCopied != 3000 || tables[0].ProgressPct != 100 {
		t.Fatalf("tables: %v %+v", err, tables)
	}
	if _, err := f.admin().Events(actx); err != nil {
		t.Fatalf("events: %v", err)
	}
	cp, err := f.admin().CheckpointShow(actx)
	if err != nil || !strings.Contains(cp.Applied, "resume_token=") {
		t.Fatalf("checkpoint: %v %+v", err, cp)
	}
	if src, err := f.admin().SourceTables(actx); err != nil || len(src) != 3 || !src[0].InPipeline {
		t.Fatalf("source_tables: %v %+v", err, src)
	}
	m := f.metricsText()
	for _, name := range []string{`strand_lag_seconds{phase="synced",pipeline="fc-pipe"}`, `strand_apply_rows_total{pipeline="fc-pipe"}`,
		`strand_seed_progress_ratio{pipeline="fc-pipe",table="a"} 1`, `strand_mongodb_events_total{op="update",pipeline="fc-pipe"}`,
		`strand_mongodb_snapshot_docs_total{pipeline="fc-pipe"} 9000`, `strand_snapshot_ranges{pipeline="fc-pipe",state="done",table="b"} 1`} {
		if !strings.Contains(m, name) {
			t.Fatalf("metric %s missing:\n%s", name, grepLines(m, "strand_"))
		}
	}

	// pause: nothing is applied; resume: catches up.
	if _, err := f.admin().Pause(actx); err != nil {
		t.Fatal(err)
	}
	f.waitState("paused", 10*time.Second)
	ops := load.halt(t)
	time.Sleep(time.Second)
	before, _ := dbDigest(ctx, p.dstC.Database(db))
	if _, err := p.srcC.Database(db).Collection("a").InsertOne(ctx, bson.D{{Key: "_id", Value: "while-paused"}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if n, _ := p.dstC.Database(db).Collection("a").CountDocuments(ctx, bson.D{{Key: "_id", Value: "while-paused"}}); n != 0 {
		t.Fatal("a paused pipeline applied a change")
	}
	_ = before
	if _, err := f.admin().Resume(actx, false); err != nil {
		t.Fatal(err)
	}
	p.waitIdentical(t, db, time.Minute)
	t.Logf("%d concurrent ops; pause/resume ok", ops)

	// verify: identical; then damage the target and verify/repair one collection.
	if out, err = f.run("verify", "--mode", "full"); err != nil || strings.Count(out, "OK ") != 3 {
		t.Fatalf("verify: %v\n%s", err, out)
	}
	if _, err := p.dstC.Database(db).Collection("b").DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: 10}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.dstC.Database(db).Collection("b").UpdateOne(ctx, bson.D{{Key: "_id", Value: 20}}, bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}}}); err != nil {
		t.Fatal(err)
	}
	out, err = f.run("verify", "--mode", "full", "--table", db+".b")
	if err == nil || !strings.Contains(out, "missing 10") || !strings.Contains(out, "different 1") {
		t.Fatalf("verify must report the damage: %v\n%s", err, out)
	}
	if out, err = f.run("verify", "--mode", "sample", "--rate", "500"); err == nil {
		// a 500-document sample of 3x3000 may or may not hit the 11 damaged documents
		t.Logf("sample verify: %s", out)
	}
	if out, err = f.run("verify", "--mode", "full", "--table", db+".b", "--repair", "fix"); err != nil || !strings.Contains(out, "re-copy") {
		t.Fatalf("verify --repair fix: %v\n%s", err, out)
	}
	p.waitIdentical(t, db, time.Minute)
	f.waitState("synced", 30*time.Second)
	if out, err = f.run("verify", "--mode", "full"); err != nil {
		t.Fatalf("verify after repair: %v\n%s", err, out)
	}
	if out, err = f.run("status"); err != nil || !strings.Contains(out, "running in the background") {
		t.Fatalf("status: %v\n%s", err, out)
	}
	if out, err = f.run("stop"); err != nil {
		t.Fatalf("stop: %v\n%s", err, out)
	}
	if out, _ = f.run("status"); !strings.Contains(out, "not running") {
		t.Fatalf("status after stop:\n%s", out)
	}
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
