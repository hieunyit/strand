package governor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// --- driver SQL giả: trả kết quả cố định theo chuỗi con của câu truy vấn (không cần DB thật) ---

type fakeResult struct {
	cols []string
	rows [][]driver.Value
	err  error
}

type fakeConnector struct{ answer func(q string) fakeResult }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn{c.answer}, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{c.answer} }

type fakeDriver struct{ answer func(q string) fakeResult }

func (d fakeDriver) Open(string) (driver.Conn, error) { return fakeConn{d.answer}, nil }

type fakeConn struct{ answer func(q string) fakeResult }

func (c fakeConn) Prepare(q string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c fakeConn) Close() error                          { return nil }
func (c fakeConn) Begin() (driver.Tx, error)             { return nil, errors.New("not supported") }
func (c fakeConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	r := c.answer(q)
	if r.err != nil {
		return nil, r.err
	}
	return &fakeRows{cols: r.cols, rows: r.rows}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

func one(cols ...string) func(vals ...driver.Value) fakeResult {
	return func(vals ...driver.Value) fakeResult { return fakeResult{cols: cols, rows: [][]driver.Value{vals}} }
}

// fakePG trả lời các câu của PGDBSignalReader; slot == nil = slot không tồn tại; standbys: các dòng
// (replays, caught_up, replay_lag) của pg_stat_replication.
func fakePG(instance fakeResult, slot *fakeResult, monitor bool, standbys ...[]driver.Value) *sql.DB {
	return sql.OpenDB(fakeConnector{answer: func(q string) fakeResult {
		switch {
		case strings.Contains(q, "pg_has_role"):
			return one("m")(monitor)
		case strings.Contains(q, "r.replay_lsn IS NOT NULL"):
			if instance.err != nil {
				return instance
			}
			return fakeResult{cols: []string{"replays", "caught_up", "lag"}, rows: standbys}
		case strings.Contains(q, "FROM pg_replication_slots s WHERE s.slot_name"):
			if slot == nil {
				return fakeResult{cols: []string{"a", "b", "c"}}
			}
			return *slot
		case strings.Contains(q, "pg_is_in_recovery()"):
			return instance
		}
		return fakeResult{err: errors.New("unexpected query: " + q)}
	}})
}

var instCols = one("rec", "delay", "keep")

// standby: một dòng của pgStandbysSQL.
func standby(replays, caughtUp bool, lag float64) []driver.Value {
	return []driver.Value{replays, caughtUp, lag}
}
var slotCols = one("status", "retained", "sent")

// --- hàm thuần ---

func TestPGSample_Derivations(t *testing.T) {
	s := PGSample{ReplayDelaySeconds: 2, StandbyReplayLagSeconds: 7}
	if got := s.ReplicaLagSeconds(); got != 7 {
		t.Fatalf("replica lag = %v, want max(2, 7)", got)
	}
	for status, want := range map[string]bool{"reserved": false, "extended": false, "unreserved": true, "lost": true, "": false} {
		s := PGSample{SlotFound: true, SlotWALStatus: status}
		if got, reason := s.SlotHard(); got != want || (got && !strings.Contains(reason, status)) {
			t.Fatalf("wal_status %q: hard = %v (%q), want %v", status, got, reason, want)
		}
	}
	if hard, _ := (PGSample{SlotWALStatus: "lost"}).SlotHard(); hard {
		t.Fatal("slot not found must not raise a hard signal")
	}
	if r := (PGSample{SlotFound: true, SlotRetainedBytes: 512 << 20, MaxSlotWALKeepBytes: 1 << 30}).WALRetainedRatio(); r != 0.5 {
		t.Fatalf("WAL ratio = %v, want 0.5", r)
	}
	for _, s := range []PGSample{
		{SlotFound: true, SlotRetainedBytes: 1 << 30, MaxSlotWALKeepBytes: -1}, // không giới hạn
		{SlotFound: true, SlotRetainedBytes: -1, MaxSlotWALKeepBytes: 1 << 30}, // không biết
		{SlotFound: false, SlotRetainedBytes: 1 << 30, MaxSlotWALKeepBytes: 1 << 30},
	} {
		if r := s.WALRetainedRatio(); r != 0 {
			t.Fatalf("WAL ratio of %+v = %v, want 0", s, r)
		}
	}
	if !(PGSample{SlotFound: true, SlotSentLagBytes: 4096}).ReaderCaughtUp(PGCaughtUpSlackBytes) {
		t.Fatal("walsender 4 KiB behind head must count as caught up")
	}
	for _, s := range []PGSample{
		{SlotFound: true, SlotSentLagBytes: PGCaughtUpSlackBytes + 1},
		{SlotFound: true, SlotSentLagBytes: -1}, // slot không active
		{SlotFound: false, SlotSentLagBytes: 0},
	} {
		if s.ReaderCaughtUp(PGCaughtUpSlackBytes) {
			t.Fatalf("%+v must not count as caught up", s)
		}
	}
	for in, want := range map[string]int64{"-1": -1, "1024": 1 << 30, " 16 ": 16 << 20, "x": -1, "": -1} {
		if got := ParseMaxSlotWALKeepSize(in); got != want {
			t.Fatalf("ParseMaxSlotWALKeepSize(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestApplyPGSample_MergesHardReasons(t *testing.T) {
	sig := applyPGSample(DBSignals{HardSignal: true, HardReason: "spool"}, PGSample{SlotFound: true, SlotWALStatus: "unreserved", StandbyReplayLagSeconds: 3})
	if !sig.HardSignal || !strings.Contains(sig.HardReason, "spool") || !strings.Contains(sig.HardReason, "unreserved") || sig.ReplicaLagSeconds != 3 {
		t.Fatalf("merged signals = %+v", sig)
	}
}

// --- đọc từ kết quả truy vấn giả ---

func TestPGReader_ReadsSignalsFromQueries(t *testing.T) {
	slot := slotCols("unreserved", float64(768<<20), float64(1024))
	db := fakePG(instCols(false, 0.0, "1024"), &slot, true, standby(true, false, 4.5))
	r := NewPGDBSignalReader(db, "strand_p", nil)
	sig, err := r.ReadSignals(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sig.ReplicaLagSeconds != 4.5 || !sig.HardSignal || !strings.Contains(sig.HardReason, "unreserved") {
		t.Fatalf("signals = %+v", sig)
	}
	if sig.QueryP99 <= 0 || sig.QueryBaseline <= 0 {
		t.Fatalf("poll latency must feed QueryP99/QueryBaseline: %+v", sig)
	}
	last, ok := r.Last()
	if !ok || last.WALRetainedRatio() != 0.75 || !last.ReaderCaughtUp(PGCaughtUpSlackBytes) || last.InRecovery {
		t.Fatalf("last sample = %+v (ok=%v)", last, ok)
	}
}

func TestPGReader_StandbyAndMissingSlot(t *testing.T) {
	db := fakePG(instCols(true, 9.0, "-1"), nil, true)
	r := NewPGDBSignalReader(db, "strand_p", nil)
	sig, err := r.ReadSignals(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	last, _ := r.Last()
	if sig.ReplicaLagSeconds != 9 || sig.HardSignal || last.SlotFound || last.MaxSlotWALKeepBytes != -1 || !last.InRecovery {
		t.Fatalf("signals = %+v, sample = %+v", sig, last)
	}
}

func TestPGReader_ProbeFailsOnPrivilegeErrorAndWarnsWithoutPgMonitor(t *testing.T) {
	denied := fakePG(fakeResult{err: errors.New("ERROR: permission denied for view pg_stat_replication (SQLSTATE 42501)")}, nil, false)
	if _, err := NewPGDBSignalReader(denied, "s", nil).Probe(context.Background()); err == nil {
		t.Fatal("probe must fail when the signal views cannot be read")
	}
	if _, err := NewPGDBSignalReader(nil, "s", nil).Probe(context.Background()); err == nil {
		t.Fatal("probe without a connection must fail")
	}
	limited := fakePG(instCols(false, 0.0, "-1"), nil, false)
	warns, err := NewPGDBSignalReader(limited, "s", nil).Probe(context.Background())
	if err != nil || len(warns) != 1 || !strings.Contains(warns[0], "pg_monitor") {
		t.Fatalf("probe without pg_monitor: warns=%v err=%v", warns, err)
	}
	full := fakePG(instCols(false, 0.0, "-1"), nil, true)
	if warns, err := NewPGDBSignalReader(full, "s", nil).Probe(context.Background()); err != nil || len(warns) != 0 {
		t.Fatalf("probe with pg_monitor: warns=%v err=%v", warns, err)
	}
}

// Lỗi đọc giữa chừng là "mất metric" (design §5.1 hàng 14), không phải tín hiệu bằng 0.
func TestPGReader_ReadErrorIsReported(t *testing.T) {
	db := fakePG(fakeResult{err: errors.New("connection reset")}, nil, true)
	if _, err := NewPGDBSignalReader(db, "s", nil).ReadSignals(context.Background()); err == nil {
		t.Fatal("read error must be returned so the governor treats the tick as blind")
	}
}

// Không có kết nối: vẫn có tín hiệu cứng của spool (như MySQL reader).
func TestPGReader_SpoolHardSignalWithoutDB(t *testing.T) {
	r := NewPGDBSignalReader(nil, "", nil)
	r.SpoolUsage = func() (int64, int64) { return 96, 100 }
	sig, err := r.ReadSignals(context.Background())
	if err != nil || !sig.HardSignal {
		t.Fatalf("spool hard signal: %+v %v", sig, err)
	}
}

// RecordQueryLatency của GRACE/onoff phải tới được latency tracker của reader PG (trước đây type switch
// chỉ biết MySQL và reader mô phỏng: latency bị bỏ im lặng).
func TestRecordQueryLatency_ReachesPGReader(t *testing.T) {
	for name, mk := range map[string]func(r DBSignalReader) Governor{
		"grace": func(r DBSignalReader) Governor { return NewGraceGovernor(GraceConfig{SignalReader: r}) },
		"onoff": func(r DBSignalReader) Governor { return NewOnoffGovernor(OnoffConfig{SignalReader: r}) },
	} {
		r := NewPGDBSignalReader(nil, "", nil)
		mk(r).RecordQueryLatency(900 * time.Millisecond)
		sig, _ := r.ReadSignals(context.Background())
		if sig.QueryP99 != 900*time.Millisecond {
			t.Fatalf("%s: QueryP99 = %v, want the recorded 900ms", name, sig.QueryP99)
		}
	}
}

// GRACE nhận tín hiệu PG: replica lag vượt lag_slo đẩy P lên và R giảm.
func TestGrace_ReactsToPGReplicaLag(t *testing.T) {
	db := fakePG(instCols(false, 0.0, "-1"), nil, true, standby(true, false, 20.0))
	g := NewGraceGovernor(GraceConfig{SoftRate: 600, HardRate: 1200, LagSLO: 5 * time.Second,
		SignalReader: NewPGDBSignalReader(db, "s", nil)})
	for i := 0; i < 6; i++ {
		if err := g.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if g.Zone() != ZoneRed || g.TotalRate() >= 600 {
		t.Fatalf("zone = %v, R = %v: PG replica lag 4× lag_slo must push GRACE to Red and cut R", g.Zone(), g.TotalRate())
	}
}

// Primary có standby đồng bộ là pg_receivewal (e2e SyncRepRace): replay_lsn luôn NULL, còn replay_lag do
// walsender ngoại suy từ mẫu WAL cũ nhất thì tăng mãi. Trước đây MAX(replay_lag) của mọi walsender vật
// lý được đọc là replica lag → onoff dừng background vĩnh viễn, snapshot treo.
func TestStandbyReplayLag_IgnoresNonReplayingAndCaughtUp(t *testing.T) {
	cases := []struct {
		name string
		in   []PGStandby
		want float64
	}{
		{"none", nil, 0},
		{"pg_receivewal sync standby", []PGStandby{{Replays: false, ReplayLagSeconds: 600}}, 0},
		{"caught-up standby with stale lag", []PGStandby{{Replays: true, CaughtUp: true, ReplayLagSeconds: 42}}, 0},
		{"replay_lag NULL", []PGStandby{{Replays: true, ReplayLagSeconds: -1}}, 0},
		{"lagging standby counts", []PGStandby{{Replays: true, ReplayLagSeconds: 7.5}}, 7.5},
		{"max of real standbys only", []PGStandby{
			{Replays: false, ReplayLagSeconds: 900},
			{Replays: true, ReplayLagSeconds: 3},
			{Replays: true, CaughtUp: true, ReplayLagSeconds: 50},
			{Replays: true, ReplayLagSeconds: 8},
		}, 8},
	}
	for _, c := range cases {
		if got := StandbyReplayLag(c.in); got != c.want {
			t.Fatalf("%s: lag = %v, want %v", c.name, got, c.want)
		}
	}
}

// Tái hiện treo của e2e từ mẫu giả: replay_lag của pg_receivewal tăng dần qua từng tick. onoff phải giữ
// background chạy (replica lag = 0); một standby thật trễ vẫn phải dừng background.
func TestOnoff_SyncReceivewalStandbyDoesNotStopBackground(t *testing.T) {
	lag := 0.0
	db := sql.OpenDB(fakeConnector{answer: func(q string) fakeResult {
		switch {
		case strings.Contains(q, "pg_has_role"):
			return one("m")(true)
		case strings.Contains(q, "r.replay_lsn IS NOT NULL"):
			lag += 2 // tăng mãi như walsender ngoại suy
			return fakeResult{cols: []string{"replays", "caught_up", "lag"}, rows: [][]driver.Value{standby(false, false, lag)}}
		case strings.Contains(q, "FROM pg_replication_slots s WHERE s.slot_name"):
			return slotCols("reserved", float64(1<<20), float64(0))
		case strings.Contains(q, "pg_is_in_recovery()"):
			return instCols(false, 0.0, "-1")
		}
		return fakeResult{err: errors.New("unexpected query: " + q)}
	}})
	g := NewOnoffGovernor(OnoffConfig{SoftRate: 20000, LagSLO: 5 * time.Second, SignalReader: NewPGDBSignalReader(db, "s", nil)})
	for i := 0; i < 30; i++ {
		if err := g.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !g.BackgroundRunning() {
			t.Fatalf("tick %d: background stopped (pressure %.2f) by the replay_lag of a non-replaying pg_receivewal", i, g.Pressure())
		}
	}

	real := fakePG(instCols(false, 0.0, "-1"), nil, true, standby(true, false, 20))
	g = NewOnoffGovernor(OnoffConfig{SoftRate: 20000, LagSLO: 5 * time.Second, SignalReader: NewPGDBSignalReader(real, "s", nil)})
	for i := 0; i < 10; i++ {
		_ = g.Tick(context.Background())
	}
	if g.BackgroundRunning() {
		t.Fatal("a replaying standby 4x lag_slo behind must still stop background")
	}
}
