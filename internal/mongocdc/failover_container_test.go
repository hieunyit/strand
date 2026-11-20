package mongocdc

import (
	"fmt"
	"testing"
	"time"

	"strand/internal/testutil"
)

// Failover on 3-member source and target replica sets while the pipeline copies and
// streams under concurrent load: primary step-down (replSetStepDown) and kill -9 of the
// primary container (then restart), on each side, during the snapshot and during
// streaming. The process must keep running (automatic resume from the checkpoint),
// and the target must end byte-identical (no loss; no duplicates: unique _id and an
// exact per-_id digest).
func TestContainer_FailoverThreeNode(t *testing.T) {
	src := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 3})
	dst := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 3})
	p := &pair{srcURI: src.AdminURI(), dstURI: dst.AdminURI()}
	p.srcC, p.dstC = mustClient(t, p.srcURI), mustClient(t, p.dstURI)
	seed := testSeed(t)
	db := "fail"
	n := envInt("STRAND_MONGO_DOCS", 30000)
	colls := []string{"a", "b", "c"}
	for i, c := range colls {
		seedDocs(t, p.srcC.Database(db), c, n, seed+int64(i), intID)
	}
	seedDocs(t, p.srcC.Database(db), "mixed", n/3, seed+9, mixedID)
	load := startLoad(t, p.srcC.Database(db), colls, n+n/5, 4, seed*7, intID)
	loadMixed := startLoad(t, p.srcC.Database(db), []string{"mixed"}, n/3+n/10, 1, seed*7+1, mixedID)

	cfg := p.cfg(t, "fail", db)
	cfg.SnapshotBatch = 200
	cfg.SnapshotParallel = 2
	cfg.LeaseDuration = 30 * time.Second // > an election; a refresh may fail meanwhile
	cfg.RetryBackoffMax = 2 * time.Second
	obs := NewObserver("fail")
	cfg.Observer = obs
	r := start(cfg)
	waitFor := func(what string, cond func(Snapshot) bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for !cond(obs.Snapshot()) {
			r.failIfExited(t)
			if time.Now().After(deadline) {
				t.Fatalf("timeout waiting for %s: %+v", what, obs.Snapshot())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	event := func(format string, a ...any) { t.Logf("%s %s", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, a...)) }

	// During the snapshot.
	waitFor("snapshot progress", func(s Snapshot) bool { return s.Phase == PhaseSnapshot && s.SnapshotDocs > 2000 })
	event("snapshot at %d docs: source step-down (old primary %d)", obs.Snapshot().SnapshotDocs, src.StepDown())
	waitFor("more snapshot progress", func(s Snapshot) bool { return s.SnapshotDocs > 8000 })
	k := dst.KillPrimary()
	event("snapshot at %d docs: target primary %d killed", obs.Snapshot().SnapshotDocs, k)
	time.Sleep(5 * time.Second)
	dst.Restart(k)
	waitFor("streaming", func(s Snapshot) bool { return s.Phase == PhaseStreaming && s.Events > 0 })
	event("streaming (retries so far %d)", obs.Snapshot().Retries)
	dst.WaitHealthy(2 * time.Minute)

	// During streaming.
	k = src.KillPrimary()
	event("streaming: source primary %d killed", k)
	time.Sleep(5 * time.Second)
	src.Restart(k)
	waitFor("events after source kill", func(s Snapshot) bool { return s.Events > 0 && time.Since(s.LastEventTime) < 20*time.Second })
	event("target step-down (old primary %d)", dst.StepDown())
	time.Sleep(3 * time.Second)
	src.WaitHealthy(2 * time.Minute)
	event("source step-down (old primary %d)", src.StepDown())
	time.Sleep(2 * time.Second)
	k = dst.KillPrimary()
	event("target primary %d killed", k)
	time.Sleep(5 * time.Second)
	dst.Restart(k)
	time.Sleep(3 * time.Second)

	ops := load.halt(t) + loadMixed.halt(t)
	docs := p.waitIdentical(t, db, 4*time.Minute, r)
	s := obs.Snapshot()
	r.stop(t)
	event("converged: %d concurrent source ops, %d documents byte-identical, %d automatic restarts, last: %s", ops, docs, s.Retries, s.LastRetryError)
}
