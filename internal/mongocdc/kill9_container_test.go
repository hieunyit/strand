package mongocdc

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func buildStrand(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "strand")
	cmd := exec.Command("go", "build", "-o", bin, "strand/cmd/strand")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build strand: %v", err)
	}
	return bin
}

type proc struct {
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	done   chan error
}

func startProc(t *testing.T, bin string, p *pair, pipeline, db string) *proc {
	cmd := exec.Command(bin, "mongodb", "sync", "--pipeline", pipeline, "--source-database", db, "--target-database", db,
		"--include", db+".*", "--lease-ttl", "2s", "--snapshot-batch", "200")
	cmd.Env = append(os.Environ(), "MONGO_SOURCE_URI="+p.srcURI, "MONGO_TARGET_URI="+p.dstURI)
	var buf bytes.Buffer
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pr := &proc{cmd: cmd, stderr: &buf, done: make(chan error, 1)}
	go func() { pr.done <- cmd.Wait() }()
	return pr
}

// startOwning starts the binary, retrying while the dead predecessor's lease expires.
func startOwning(t *testing.T, bin string, p *pair, pipeline, db string) *proc {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		pr := startProc(t, bin, p, pipeline, db)
		select {
		case err := <-pr.done:
			if !strings.Contains(pr.stderr.String(), "holds the lease") || time.Now().After(deadline) {
				t.Fatalf("strand exited: %v\n%s", err, pr.stderr.String())
			}
			time.Sleep(300 * time.Millisecond)
		case <-time.After(1500 * time.Millisecond):
			return pr
		}
	}
}

// kill -9 at random moments during snapshot and streaming, under concurrent load;
// after the final restart the target is byte-identical (no loss, no duplicates).
func TestContainer_Kill9SnapshotAndStream(t *testing.T) {
	bin := buildStrand(t)
	p := newPair(t)
	seed := testSeed(t)
	rng := rand.New(rand.NewSource(seed))
	for round := 0; round < envInt("STRAND_MONGO_KILL_ROUNDS", 2); round++ {
		db := fmt.Sprintf("kill%d", round)
		n := envInt("STRAND_MONGO_DOCS", 20000)
		seedDocs(t, p.srcC.Database(db), "a", n, seed+int64(round), intID)
		seedDocs(t, p.srcC.Database(db), "mixed", n/2, seed+int64(round)+7, mixedID)
		load := startLoad(t, p.srcC.Database(db), []string{"a"}, n+n/5, 4, seed+int64(round)*31, intID)
		loadMixed := startLoad(t, p.srcC.Database(db), []string{"mixed"}, n/2+n/10, 2, seed+int64(round)*31+5, mixedID)
		kills := envInt("STRAND_MONGO_KILLS", 8)
		for k := 0; k < kills; k++ {
			pr := startOwning(t, bin, p, "kill-"+db, db)
			time.Sleep(time.Duration(200+rng.Intn(3000)) * time.Millisecond)
			_ = pr.cmd.Process.Signal(syscall.SIGKILL)
			<-pr.done
		}
		ops := load.halt(t) + loadMixed.halt(t)
		pr := startOwning(t, bin, p, "kill-"+db, db)
		docs := p.waitIdentical(t, db, 3*time.Minute)
		_ = pr.cmd.Process.Signal(syscall.SIGTERM)
		if err := <-pr.done; err != nil {
			t.Fatalf("graceful stop: %v\n%s", err, pr.stderr.String())
		}
		t.Logf("round %d: %d kill -9, %d concurrent ops, %d documents identical", round, kills, ops, docs)
	}
}
