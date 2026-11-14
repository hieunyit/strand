package e2e

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// P3-01b — schema evolution cho nguồn PostgreSQL (PG→PG):
// Thay đổi cộng thêm (ADD COLUMN nullable/default, nới kiểu) tự áp dưới tải, 0 lệch;
// Thay đổi phá hủy (DROP COLUMN) dừng exit 10 schema_change_pending,
// `strand schema diff` liệt kê, `strand schema approve` áp, chạy lại tiếp tục, 0 lệch.
// Cả hai chế độ commit_order: strict và relaxed.

type evoWriterPG struct {
	db    *sql.DB
	table string
	rng   *rand.Rand
	next  atomic.Int64
	flags atomic.Int32
	stop  atomic.Bool
	ops   atomic.Int64
	wg    sync.WaitGroup
}

func newEvoWriterPG(db *sql.DB, table string, seed, firstID int64) *evoWriterPG {
	w := &evoWriterPG{db: db, table: table, rng: rand.New(rand.NewSource(seed))}
	w.next.Store(firstID)
	return w
}

func (w *evoWriterPG) Start(clients int, span int64) {
	for c := 0; c < clients; c++ {
		w.wg.Add(1)
		r := rand.New(rand.NewSource(w.rng.Int63()))
		go func() {
			defer w.wg.Done()
			for !w.stop.Load() {
				w.one(r, span)
				w.ops.Add(1)
				time.Sleep(time.Millisecond)
			}
		}()
	}
}

func (w *evoWriterPG) Stop() { w.stop.Store(true); w.wg.Wait() }

func (w *evoWriterPG) one(r *rand.Rand, span int64) {
	f := w.flags.Load()
	v := strings.Repeat("a", 5+r.Intn(10))
	var cnt any = r.Intn(1000)
	if f&evoWidened != 0 {
		v = strings.Repeat("w", 60+r.Intn(30))
		cnt = int64(5_000_000_000) + int64(r.Intn(1000))
	}
	ver := time.Now().UnixNano()
	switch k := r.Intn(100); {
	case k < 55:
		id := w.next.Add(1)
		if f&evoAddedCols != 0 {
			extra := r.Intn(100)
			_, _ = w.db.Exec(fmt.Sprintf("INSERT INTO %s (id, ver, v, cnt, extra, flag) VALUES ($1, $2, $3, $4, $5, 1)", w.table),
				id, ver, v, cnt, extra)
		} else {
			_, _ = w.db.Exec(fmt.Sprintf("INSERT INTO %s (id, ver, v, cnt) VALUES ($1, $2, $3, $4)", w.table),
				id, ver, v, cnt)
		}
	case k < 90:
		targetID := 1 + r.Int63n(span)
		_, _ = w.db.Exec(fmt.Sprintf("UPDATE %s SET ver = $1, v = $2, cnt = $3 WHERE id = $4", w.table),
			ver, v, cnt, targetID)
	default:
		targetID := 1 + r.Int63n(span)
		_, _ = w.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = $1", w.table), targetID)
	}
}

const evoPGTable = `CREATE TABLE app.evo (
	id BIGINT PRIMARY KEY,
	ver BIGINT NOT NULL,
	v VARCHAR(20) NOT NULL,
	cnt INT NOT NULL DEFAULT 0,
	junk VARCHAR(10) NULL)`

func evoPGPreload(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, ver, v, cnt, junk) VALUES ($1, $2, $3, $4, 'j')", table),
			i, time.Now().UnixNano(), fmt.Sprintf("v%d", i), i); err != nil {
			t.Fatal(err)
		}
	}
}

type evoPGPairEnv struct {
	src, dst *pgNode
	cfg      strandCfg
	cfgPath  string
	ew       *evoWriterPG
	p        *strandProc
	slot     string
	pub      string
}

func evoStartPGPair(t *testing.T, name, order, additive string) *evoPGPairEnv {
	t.Helper()
	seed := seedFromEnv(t)
	slot := "strand_evo_" + strings.ReplaceAll(name, "-", "_")
	pub := slot + "_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)

	src.exec(t, evoPGTable)
	dst.exec(t, evoPGTable)

	src.exec(t, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE app.evo", pub))

	evoPGPreload(t, src.DB, "app.evo", 500)
	ew := newEvoWriterPG(src.DB, "app.evo", seed, 10000)
	ew.Start(3, 500)
	t.Cleanup(ew.Stop)

	cfg := strandCfg{
		Name:           name,
		SourceKind:     "postgres",
		TargetKind:     "postgres",
		SourceDSN:      src.DSN,
		TargetDSN:      dst.DSN,
		CommitOrder:    order,
		SourceExtra:    fmt.Sprintf(", slot: %s, publication: %s, sequence_sync_interval: 1s", slot, pub),
		Include:        []string{"app.*"},
		Dir:            testDir(t),
		Bootstrap:      "start_at",
		SchemaAdditive: additive,
	}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath, "--start-at", lsn)
	waitSynced(t, &cfg, p, 3*time.Minute)

	return &evoPGPairEnv{
		src: src, dst: dst, cfg: cfg, cfgPath: cfgPath,
		ew: ew, p: p, slot: slot, pub: pub,
	}
}

func (e *evoPGPairEnv) finish(t *testing.T) {
	t.Helper()
	e.ew.Stop()
	waitPGCaughtUp(t, e.src, e.dst, &e.cfg, e.p, 3*time.Minute)
	if code := e.p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, e.p.tail())
	}
	comparePG(t, e.src, e.dst, "app.items", "app.events", "app.accounts", "app.evo")
}

func evoPGCommitOrders() []string {
	if v := os.Getenv("STRAND_E2E_COMMIT_ORDER"); v != "" {
		return []string{v}
	}
	return []string{"strict", "relaxed"}
}

// TestP3_E2E_SchemaEvolution_PG_AddColumnAuto: ADD COLUMN dưới tải tự áp, 0 lệch dữ liệu.
func TestP3_E2E_SchemaEvolution_PG_AddColumnAuto(t *testing.T) {
	for _, order := range evoPGCommitOrders() {
		t.Run("order_"+order, func(t *testing.T) {
			e := evoStartPGPair(t, "evo-pg-add-"+order, order, "")
			// DDL ADD COLUMN tại source dưới tải
			e.src.exec(t, "ALTER TABLE app.evo ADD COLUMN extra INT NULL")
			e.src.exec(t, "ALTER TABLE app.evo ADD COLUMN flag SMALLINT NOT NULL DEFAULT 0")
			e.ew.flags.Or(evoAddedCols)
			time.Sleep(time.Second)

			deadline := time.Now().Add(2 * time.Minute)
			for pgColType(t, e.dst.DB, "app", "evo", "extra") == "" || pgColType(t, e.dst.DB, "app", "evo", "flag") == "" {
				mustStayRunning(t, e.p, "additive DDL on PG source")
				if time.Now().After(deadline) {
					t.Fatalf("additive change not applied on target PG\n%s", e.p.tail())
				}
				time.Sleep(200 * time.Millisecond)
			}
			mustStayRunning(t, e.p, "additive DDL on PG source")
			time.Sleep(2 * time.Second)
			e.finish(t)

			if got := pgColType(t, e.dst.DB, "app", "evo", "extra"); got != "integer" {
				t.Fatalf("target extra type = %q, want integer", got)
			}
			if !strings.Contains(e.p.output.String(), "additive schema change applied") {
				t.Fatalf("log lacks the auto-apply line\n%s", e.p.tail())
			}
		})
	}
}

// TestP3_E2E_SchemaEvolution_PG_DropColumnApprove: DROP COLUMN dừng exit 10, diff liệt kê, approve áp, chạy lại tiếp, 0 lệch.
func TestP3_E2E_SchemaEvolution_PG_DropColumnApprove(t *testing.T) {
	for _, order := range evoPGCommitOrders() {
		t.Run("order_"+order, func(t *testing.T) {
			e := evoStartPGPair(t, "evo-pg-drop-"+order, order, "")
			e.src.exec(t, "ALTER TABLE app.evo DROP COLUMN junk")

			// Dừng exit 10 schema_change_pending
			waitExitPending(t, e.p, "DROP COLUMN on PG source")
			if !strings.Contains(e.p.output.String(), "strand schema approve") {
				t.Fatalf("message must instruct operator to approve\n%s", e.p.tail())
			}
			if pgColType(t, e.dst.DB, "app", "evo", "junk") == "" {
				t.Fatal("DROP COLUMN was applied to the target without approval")
			}

			// diff liệt kê thay đổi
			id, _ := pendingID(t, e.cfgPath, "destructive", `DROP COLUMN "junk"`, "app.evo")

			// approve áp lên target
			out, code := runStrandCLI(t, time.Minute, "schema", "approve", id, "-c", e.cfgPath)
			if code != 0 || !strings.Contains(out, "applied") {
				t.Fatalf("approve exit %d:\n%s", code, out)
			}
			if pgColType(t, e.dst.DB, "app", "evo", "junk") != "" {
				t.Fatal("approve did not drop column on target PG")
			}

			// Chạy lại tiếp tục bình thường
			e.p = startStrand(t, e.cfgPath)
			waitSynced(t, &e.cfg, e.p, 3*time.Minute)
			e.finish(t)
		})
	}
}
