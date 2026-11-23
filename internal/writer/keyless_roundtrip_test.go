package writer_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/writer"
)

// A low-volume measurement of the application's real COM_QUERY commands. The
// fixture's packets fit in a single write (no TLS, compression, or long data).
type queryRecorder struct {
	sync.Mutex
	queries []string
}
type recordedConn struct {
	net.Conn
	recorder *queryRecorder
}

func (c *recordedConn) Write(p []byte) (int, error) {
	if len(p) >= 5 && int(p[0])+(int(p[1])<<8)+(int(p[2])<<16) == len(p)-4 && p[3] == 0 && p[4] == 3 {
		c.recorder.Lock()
		c.recorder.queries = append(c.recorder.queries, string(p[5:]))
		c.recorder.Unlock()
	}
	return c.Conn.Write(p)
}
func (r *queryRecorder) take() []string {
	r.Lock()
	defer r.Unlock()
	queries := r.queries
	r.queries = nil
	return queries
}

// The reference uses the identical MySQL driver and configuration through a
// wrapper. The experiment only enables grouped result sets on the native driver.
type protocolReferenceConnector struct{ driver.Connector }
type protocolReferenceDriver struct{ driver.Driver }

func (c protocolReferenceConnector) Driver() driver.Driver {
	return protocolReferenceDriver{c.Connector.Driver()}
}

func TestMySQLKeylessProtocolRoundTrips(t *testing.T) {
	if os.Getenv("STRAND_BATCH_SWEEP") != "1" {
		t.Skip("set STRAND_BATCH_SWEEP=1")
	}
	ctx := context.Background()
	control, dsn := getMySQL(t)
	if _, err := control.Exec("CREATE TABLE protocol_k (id INT, v VARCHAR(10), INDEX(id))"); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec("INSERT INTO protocol_k VALUES (1,'old'), (1,'old'),(1,'OLD'),(1,NULL)"); err != nil {
		t.Fatal(err)
	}
	store := checkpoint.NewMySQLStore(control, "protocol", model.EngineMySQL)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	epoch, err := store.AdvanceEpoch(ctx, "protocol", "bench", "hash")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	recorder := new(queryRecorder)
	const network = "strand_protocol_bench"
	mysql.RegisterDialContext(network, func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := new(net.Dialer).DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return &recordedConn{Conn: conn, recorder: recorder}, nil
	})
	t.Cleanup(func() { mysql.DeregisterDialContext(network) })
	cfg.Net = network
	cfg.InterpolateParams = true
	cfg.MultiStatements = true
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tbl := model.TableID{Schema: "strand", Table: "protocol_k"}
	keyed := model.TableID{Schema: "strand", Table: "protocol_pk"}
	if _, err := control.Exec("CREATE TABLE protocol_pk(id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	open := func(connector driver.Connector) *writer.MySQLWriter {
		db := sql.OpenDB(connector)
		t.Cleanup(func() { _ = db.Close() })
		db.SetMaxOpenConns(1)
		w := writer.NewMySQLWriter(db, "protocol", 1, epoch)
		if err := w.EnsureWorkerRow(ctx); err != nil {
			t.Fatal(err)
		}
		w.SetStatementPipelining(true)
		w.SetPrimaryKey(tbl, []string{"id", "v"})
		w.SetKeylessStringCols(tbl, []string{"v"})
		w.SetKeyless(tbl, true)
		w.SetPrimaryKey(keyed, []string{"id"})
		return w
	}
	reference, native := open(protocolReferenceConnector{connector}), open(connector)
	// Net zero change to the multiset: each triplet inserts, changes and deletes
	// exactly one occurrence. Existing duplicates, case variants and NULL survive.
	var rows []model.RowChange
	cols := func(v string) []model.ColValue {
		return []model.ColValue{{Name: "id", Value: 1}, {Name: "v", Value: v}}
	}
	before, after := []model.Value{1, "old"}, []model.Value{1, "moved"}
	for range 4 {
		rows = append(rows, model.RowChange{Table: tbl, Op: model.OpInsert, Key: before, Cols: cols("old")},
			model.RowChange{Table: tbl, Op: model.OpUpdate, OldKey: before, Key: after, Cols: cols("moved")},
			model.RowChange{Table: tbl, Op: model.OpDelete, Key: after})
	}
	txn := []*model.Txn{{Rows: rows}}
	apply := func(w *writer.MySQLWriter) {
		t.Helper()
		if err := w.ApplyBatch(ctx, txn, writer.DeltaRow{Applied: "test-pos"}); err != nil {
			t.Fatal(err)
		}
	}
	apply(reference)
	apply(native) // warm the connection and max_allowed_packet cache
	recorder.take()
	batchNo := func() int64 {
		t.Helper()
		var n int64
		if err := control.QueryRow("SELECT batch_no FROM _strand_worker_delta WHERE pipeline_id='protocol' AND worker_id=1").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	checkRows := func() {
		t.Helper()
		var all, lower, upper, nulls int
		if err := control.QueryRow("SELECT count(*),sum(BINARY v='old'),sum(BINARY v='OLD'),sum(v IS NULL) FROM protocol_k").Scan(&all, &lower, &upper, &nulls); err != nil || all != 4 || lower != 2 || upper != 1 || nulls != 1 {
			t.Fatalf("multiset %d %d %d %d: %v", all, lower, upper, nulls, err)
		}
	}
	mark := batchNo()
	for _, w := range []*writer.MySQLWriter{reference, native} {
		// A keyed insert, a no-op and a keyless insert precede the offending
		// change. The SQL-error case fails after successful keyless row counts.
		valid := []model.RowChange{
			{Table: keyed, Op: model.OpInsert, Key: []model.Value{7}, Cols: []model.ColValue{{Name: "id", Value: 7}}},
			{Table: tbl, Op: model.OpUpdate, Key: before, OldKey: before, Cols: cols("old")},
			rows[0],
			rows[1],
		}
		for _, bad := range []model.RowChange{
			{Table: tbl, Op: model.OpDelete, Key: []model.Value{-1, "missing"}},
			{Table: tbl, Op: model.OpUpdate, Key: []model.Value{-1, "missing"}},
			{Table: keyed, Op: model.OpInsert, Key: []model.Value{1}, Cols: []model.ColValue{{Name: "unknown_column", Value: 1}}},
		} {
			fail := &model.Txn{Rows: append(valid, bad)}
			if err := w.ApplyBatch(ctx, []*model.Txn{fail}, writer.DeltaRow{Applied: "must-not-commit"}); err == nil {
				t.Fatal("failed batch committed")
			} else if bad.Table == tbl && !strings.Contains(err.Error(), "matched no row") {
				t.Fatal(err)
			}
			if batchNo() != mark {
				t.Fatal("checkpoint advanced after failed batch")
			}
			checkRows()
			var n int
			if err := control.QueryRow("SELECT count(*) FROM protocol_pk").Scan(&n); err != nil || n != 0 {
				t.Fatalf("keyed change survived rollback: %d %v", n, err)
			}
		}
	}
	recorder.take()
	const measurements = 256
	tail := func(durations []time.Duration) (time.Duration, time.Duration) {
		slices.Sort(durations)
		return durations[(95*len(durations)+99)/100-1], durations[(99*len(durations)+99)/100-1]
	}
	for repeat := 0; repeat < 3; repeat++ {
		for variant := 0; variant < 2; variant++ {
			candidate := (repeat+variant)%2 == 1
			start := time.Now()
			w := reference
			if candidate {
				w = native
			}
			durations := make([]time.Duration, measurements)
			for i := range durations {
				began := time.Now()
				apply(w)
				durations[i] = time.Since(began)
			}
			elapsed := time.Since(start)
			commands := len(recorder.take())
			want := 15
			if candidate && os.Getenv("STRAND_EXPECT_GROUPED") == "1" {
				want = 4
			}
			if commands != measurements*want {
				t.Fatalf("commands=%d want=%d", commands, measurements*want)
			}
			p95, p99 := tail(durations)
			t.Logf("repeat=%d group=%t target_commands/txn=%d apply_us/txn=%.1f apply_p95=%s apply_p99=%s", repeat+1, candidate, want, float64(elapsed.Microseconds())/measurements, p95, p99)
			checkRows()
		}
	}
	if batchNo() != mark+6*measurements {
		t.Fatalf("checkpoint after successful test batches %d want %d", batchNo(), mark+6*measurements)
	}
	// Same two-connector crossover with only keyed DML.
	keyedChanges := []*model.Txn{{Rows: []model.RowChange{
		{Table: keyed, Op: model.OpInsert, Key: []model.Value{17}, Cols: []model.ColValue{{Name: "id", Value: 17}}},
		{Table: keyed, Op: model.OpDelete, Key: []model.Value{17}},
	}}}
	for repeat := 0; repeat < 3; repeat++ {
		for i := 0; i < 2; i++ {
			candidate := (i+repeat)%2 == 1
			w := reference
			if candidate {
				w = native
			}
			start := time.Now()
			durations := make([]time.Duration, measurements)
			for i := range durations {
				began := time.Now()
				if err := w.ApplyBatch(ctx, keyedChanges, writer.DeltaRow{Applied: "keyed"}); err != nil {
					t.Fatal(err)
				}
				durations[i] = time.Since(began)
			}
			elapsed := time.Since(start)
			if n := len(recorder.take()); n != 4*measurements {
				t.Fatalf("keyed commands %d, want %d", n, 4*measurements)
			}
			p95, p99 := tail(durations)
			t.Logf("keyed repeat=%d group=%t target_commands/txn=4 apply_us/txn=%.1f apply_p95=%s apply_p99=%s", repeat+1, candidate, float64(elapsed.Microseconds())/measurements, p95, p99)
		}
	}
	mark = batchNo()
	// Four triplets contain eight checked statements. Cross the 64-check
	// flush boundary inside one target transaction, then induce a failure
	// in a later group and verify the earlier group's data rolls back too.
	var large []model.RowChange
	for range 9 {
		large = append(large, rows...)
	}
	recorder.take()
	if err := native.ApplyBatch(ctx, []*model.Txn{{Rows: large}}, writer.DeltaRow{Applied: "large"}); err != nil {
		t.Fatal(err)
	}
	if n := len(recorder.take()); os.Getenv("STRAND_EXPECT_GROUPED") == "1" && n != 5 {
		t.Fatalf("want two bounded data commands plus begin, checkpoint and commit; got %d", n)
	}
	if batchNo() != mark+1 {
		t.Fatal("the multi-group transaction must advance once")
	}
	checkRows()
	// Place one extra insert in group one. A missed delete in the last
	// group must roll it back alongside all the earlier matched updates.
	fail := append([]model.RowChange{rows[0]}, large...)
	fail = append(fail, model.RowChange{Table: tbl, Op: model.OpDelete, Key: []model.Value{-1, "missing"}})
	if err := native.ApplyBatch(ctx, []*model.Txn{{Rows: fail}}, writer.DeltaRow{Applied: "unexpected"}); err == nil {
		t.Fatal("expected late mismatch")
	}
	if batchNo() != mark+1 {
		t.Fatal("late mismatch advanced checkpoint")
	}
	checkRows()
	// Hold a target row needed by the second group and cancel the writer
	// while its transaction is live. Releasing the lock lets rollback finish.
	blocker, err := control.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err := blocker.Exec("INSERT INTO protocol_pk VALUES(99)"); err != nil {
		t.Fatal(err)
	}
	locked := append([]model.RowChange{rows[0]}, large...)
	locked = append(locked, model.RowChange{Table: keyed, Op: model.OpInsert, Key: []model.Value{99}, Cols: []model.ColValue{{Name: "id", Value: 99}}})
	deadline, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if err := native.ApplyBatch(deadline, []*model.Txn{{Rows: locked}}, writer.DeltaRow{Applied: "unexpected"}); err == nil || deadline.Err() == nil {
		t.Fatalf("expected cancellation during target transaction: %v", err)
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	if batchNo() != mark+1 {
		t.Fatal("cancellation advanced checkpoint")
	}
	checkRows()

}
