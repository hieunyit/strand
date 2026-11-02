package e2e

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/network"
)

func snapshotProgress(t *testing.T, dir string) int64 {
	raw, err := os.ReadFile(filepath.Join(dir, "state", "pipeline-state.json"))
	if err != nil {
		return -1
	}
	var st struct {
		Plans map[string]struct {
			NextPK int64 `json:"next_pk"`
		} `json:"snapshot_plans"`
		Done map[string]bool `json:"snapshot_done"`
	}
	_ = json.Unmarshal(raw, &st)
	var sum int64
	for _, p := range st.Plans {
		sum += p.NextPK
	}
	return sum + int64(len(st.Done))*1_000_000_000
}

// P1-16: tín hiệu đọc trực tiếp từ DB (replica lag) kích hoạt breaker: background (snapshot)
// dừng; hết lag và qua cooldown thì chạy lại; không cần metrics_source; 0 lệch.
func TestP1_E2E_BreakerStopsSnapshotOnReplicaLag(t *testing.T) {
	seed := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	primary := startMySQL(t, mysqlImage("source"), "primary", 1, nw)
	r1 := startMySQL(t, mysqlImage("source"), "replica1", 3, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	makeReplica(t, r1, "primary")
	createSchema(t, primary)
	createSchema(t, dst)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	wl := newWorkload(primary.DB, seed)
	wl.Preload(t, 60000)
	waitReplicaCaughtUp(t, primary, r1)

	r1.exec(t, "STOP REPLICA", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 40", "START REPLICA")
	wl.Start(4, 60000)
	time.Sleep(15 * time.Second) // lag trên replica tăng tới ~15s

	cfg := strandCfg{Name: "e2e-breaker", SourceDSN: r1.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t), BreakerLag: "5s", SourceIOPS: 2000}
	p := startStrand(t, cfg.write(t))
	time.Sleep(10 * time.Second)
	before := snapshotProgress(t, cfg.Dir)
	time.Sleep(20 * time.Second)
	after := snapshotProgress(t, cfg.Dir)
	if after != before {
		t.Fatalf("snapshot progressed (%d -> %d) while replica lag exceeded the breaker threshold", before, after)
	}
	t.Logf("breaker open: snapshot held at progress %d while replica lag > 5s", before)

	r1.exec(t, "STOP REPLICA", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0", "START REPLICA")
	wl.Stop()
	waitReplicaCaughtUp(t, primary, r1)
	waitSynced(t, &cfg, p, 6*time.Minute) // cooldown 60 s rồi snapshot chạy tiếp
	waitCaughtUp(t, primary, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, primary, dst, appTables...)
}
