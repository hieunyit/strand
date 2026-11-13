package governor

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Tín hiệu của source PostgreSQL (02 §7; design §5.2 bảng loại tín hiệu, cột PostgreSQL), cùng bộ với
// MySQLDBSignalReader:
//
//   - ReplicaLagSeconds: source là standby → độ trễ replay của chính nó; source là primary → replay_lag
//     lớn nhất của các standby vật lý thật sự replay WAL (walsender logical, gồm của Strand, và client chỉ
//     nhận WAL như pg_receivewal bị loại — xem StandbyReplayLag). Breaker so với
//     governor.breaker.replica_lag.max như MySQL.
//   - QueryP99: latency của chính câu poll tín hiệu (hàng "Tự đo"), như MySQL.
//   - Tín hiệu cứng: wal_status của slot của pipeline là `unreserved` / `lost`, và spool local ≥ 95%.
//
// Ngoài P, reader còn cho số hạng WAL của P_lag (WAL slot đang giữ / max_slot_wal_keep_size) và
// khoảng cách walsender → head (bootstrap: reader đã đuổi kịp source chưa).

// pgHeadLSN: head WAL của server — primary: pg_current_wal_lsn(); standby: vị trí đã replay
// (pg_current_wal_lsn() báo lỗi khi đang recovery).
const pgHeadLSN = `CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END`

// pgInstanceSignalsSQL: một dòng — đang recovery, độ trễ replay của chính server (0 khi đã replay hết
// WAL đã nhận: standby của primary rảnh không bị tính là trễ).
const pgInstanceSignalsSQL = `SELECT pg_is_in_recovery(),
  CASE WHEN pg_is_in_recovery() AND pg_last_wal_receive_lsn() IS DISTINCT FROM pg_last_wal_replay_lsn()
       THEN COALESCE(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp()), 0)::float8
       ELSE 0::float8 END`

// pgKeepSQL: max_slot_wal_keep_size. Cấu hình gần như tĩnh và quét pg_settings đắt hơn hẳn các câu khác
// (p50 ~5 ms so với ~2 ms, đuôi dài), nên đọc cache theo pgKeepTTL và không tính vào latency probe.
const pgKeepSQL = `SELECT COALESCE((SELECT setting FROM pg_settings WHERE name = 'max_slot_wal_keep_size'), '-1')`

const pgKeepTTL = time.Minute

// pgStandbysSQL: mỗi walsender vật lý (walsender logical, gồm của Strand, bị loại) một dòng — có báo vị
// trí replay không (replay_lsn khác NULL), đã replay tới sent_lsn chưa, replay_lag (-1 = NULL). Việc
// diễn giải nằm ở StandbyReplayLag (hàm thuần, có test) thay vì gộp MAX trong SQL.
const pgStandbysSQL = `SELECT r.replay_lsn IS NOT NULL,
  COALESCE(r.replay_lsn >= r.sent_lsn, false),
  COALESCE(EXTRACT(EPOCH FROM r.replay_lag)::float8, -1::float8)
  FROM pg_stat_replication r
  WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots s WHERE s.active_pid = r.pid AND s.slot_type = 'logical')`

// pgSlotSignalsSQL: slot của pipeline — wal_status, số byte WAL slot đang giữ (head − restart_lsn) và
// khoảng cách head − sent_lsn của walsender đang dùng slot (NULL khi slot không active).
const pgSlotSignalsSQL = `SELECT COALESCE(s.wal_status, ''),
  COALESCE(pg_wal_lsn_diff(` + pgHeadLSN + `, s.restart_lsn), -1)::float8,
  COALESCE((SELECT pg_wal_lsn_diff(` + pgHeadLSN + `, r.sent_lsn) FROM pg_stat_replication r WHERE r.pid = s.active_pid), -1)::float8
  FROM pg_replication_slots s WHERE s.slot_name = $1`

// pgMonitorRoleSQL: role có xem được cột lag / wait của backend khác không (pg_monitor hoặc superuser).
const pgMonitorRoleSQL = `SELECT pg_has_role(current_user, 'pg_monitor', 'USAGE')
  OR COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false)`

// PGCaughtUpSlackBytes: walsender cách head không quá một segment WAL mặc định (16 MiB) thì coi reader
// đã đuổi kịp source (head luôn nhích vì WAL không thuộc publication: checkpoint, bảng khác...).
const PGCaughtUpSlackBytes = 16 << 20

// PGSample là một lần đọc tín hiệu thô của source PG. Giá trị -1 = không biết.
type PGSample struct {
	InRecovery              bool
	ReplayDelaySeconds      float64 // standby: now − thời điểm replay cuối, khi còn WAL đã nhận chưa replay
	StandbyReplayLagSeconds float64 // primary: replay_lag lớn nhất của standby vật lý
	MaxSlotWALKeepBytes     int64   // max_slot_wal_keep_size; -1 = không giới hạn
	SlotFound               bool
	SlotWALStatus           string // reserved | extended | unreserved | lost
	SlotRetainedBytes       int64  // head − restart_lsn của slot; -1 = không biết
	SlotSentLagBytes        int64  // head − sent_lsn của walsender dùng slot; -1 = slot không active
}

// ReplicaLagSeconds: tín hiệu replica lag của PG (max của hai phía, xem đầu file).
func (s PGSample) ReplicaLagSeconds() float64 {
	if s.ReplayDelaySeconds > s.StandbyReplayLagSeconds {
		return s.ReplayDelaySeconds
	}
	return s.StandbyReplayLagSeconds
}

// PGStandby là một walsender vật lý trong pg_stat_replication (nhìn từ primary).
type PGStandby struct {
	Replays          bool    // replay_lsn khác NULL: client có replay WAL (standby thật)
	CaughtUp         bool    // replay_lsn >= sent_lsn: đã replay hết những gì đã nhận
	ReplayLagSeconds float64 // replay_lag; -1 = NULL
}

// StandbyReplayLag: replay_lag lớn nhất của các standby thật sự replay WAL.
//
//   - replay_lsn NULL (pg_receivewal — kể cả khi là standby đồng bộ —, pg_basebackup): client không bao
//     giờ báo vị trí replay. Walsender khi đó vẫn tính replay_lag = now − thời điểm của mẫu WAL CŨ NHẤT
//     chưa "replay" (nhánh "chỉ có mẫu tương lai" của LagTrackerRead), nên giá trị tăng mãi hoặc đứng ở
//     một số lớn sau khi bộ đệm lag tracker đầy. Đọc nó là replica lag làm onoff dừng background vĩnh
//     viễn (e2e SyncRepRace: snapshot app.items treo 10 phút). Không có replica đọc nào trễ ở đây → bỏ.
//   - Đã replay tới sent_lsn: 0 (walsender chỉ xóa replay_lag sau hai bản tin "đã replay hết" liên
//     tiếp; trước đó giá trị cũ của đợt WAL trước vẫn hiện).
//   - replay_lag NULL: 0.
func StandbyReplayLag(sts []PGStandby) float64 {
	lag := 0.0
	for _, st := range sts {
		if !st.Replays || st.CaughtUp || st.ReplayLagSeconds < 0 {
			continue
		}
		if st.ReplayLagSeconds > lag {
			lag = st.ReplayLagSeconds
		}
	}
	return lag
}

// SlotHard: wal_status `unreserved` (slot sắp mất WAL: đã vượt max_slot_wal_keep_size) hoặc `lost` là
// tín hiệu cứng (design §5.2).
func (s PGSample) SlotHard() (bool, string) {
	if !s.SlotFound {
		return false, ""
	}
	switch s.SlotWALStatus {
	case "unreserved", "lost":
		return true, fmt.Sprintf("replication slot wal_status = %s", s.SlotWALStatus)
	}
	return false, ""
}

// WALRetainedRatio: số hạng WAL của P_lag = WAL slot đang giữ / max_slot_wal_keep_size (02 §7).
// Không giới hạn (-1) hoặc không biết thì 0.
func (s PGSample) WALRetainedRatio() float64 {
	if !s.SlotFound || s.MaxSlotWALKeepBytes <= 0 || s.SlotRetainedBytes < 0 {
		return 0
	}
	return float64(s.SlotRetainedBytes) / float64(s.MaxSlotWALKeepBytes)
}

// ReaderCaughtUp: walsender của slot đã gửi tới head (trong slack byte). Slot không active hoặc không
// biết → false (thận trọng: bootstrap tính lag theo thời gian như trước).
func (s PGSample) ReaderCaughtUp(slack int64) bool {
	return s.SlotFound && s.SlotSentLagBytes >= 0 && s.SlotSentLagBytes <= slack
}

// ParseMaxSlotWALKeepSize đổi pg_settings.setting của max_slot_wal_keep_size (đơn vị MB) sang byte;
// -1 hoặc giá trị lạ = không giới hạn (-1).
func ParseMaxSlotWALKeepSize(setting string) int64 {
	v, err := strconv.ParseInt(strings.TrimSpace(setting), 10, 64)
	if err != nil || v < 0 {
		return -1
	}
	return v << 20
}

// ReadPGSample đọc tín hiệu thô của source PG. slot rỗng = bỏ phần slot. Lỗi của câu instance (quyền,
// kết nối) được trả về; slot chưa tồn tại không phải lỗi (SlotFound = false).
func ReadPGSample(ctx context.Context, db *sql.DB, slot string) (PGSample, error) {
	return readPGSample(ctx, db, slot, nil, nil)
}

// pgKeepCache giữ max_slot_wal_keep_size đã đọc (byte) và thời điểm đọc.
type pgKeepCache struct {
	bytes int64
	at    time.Time
}

// readPGSample: rec (tùy chọn) nhận latency của TỪNG câu truy vấn — mỗi câu một mẫu, như một câu
// SHOW của MySQL — thay vì tổng ba round-trip (tổng có đuôi nặng hơn hẳn và làm p99 giả cao). keep
// (tùy chọn) cache max_slot_wal_keep_size.
func readPGSample(ctx context.Context, db *sql.DB, slot string, rec func(time.Duration), keep *pgKeepCache) (PGSample, error) {
	timed := func(f func() error) error {
		start := time.Now()
		err := f()
		if rec != nil && err == nil {
			rec(time.Since(start))
		}
		return err
	}
	s := PGSample{MaxSlotWALKeepBytes: -1, SlotRetainedBytes: -1, SlotSentLagBytes: -1}
	if err := timed(func() error {
		return db.QueryRowContext(ctx, pgInstanceSignalsSQL).Scan(&s.InRecovery, &s.ReplayDelaySeconds)
	}); err != nil {
		return s, fmt.Errorf("read PostgreSQL governor signals: %w", err)
	}
	if keep != nil && !keep.at.IsZero() && time.Since(keep.at) < pgKeepTTL {
		s.MaxSlotWALKeepBytes = keep.bytes
	} else {
		var setting string
		if err := db.QueryRowContext(ctx, pgKeepSQL).Scan(&setting); err != nil {
			return s, fmt.Errorf("read PostgreSQL governor signals: %w", err)
		}
		s.MaxSlotWALKeepBytes = ParseMaxSlotWALKeepSize(setting)
		if keep != nil {
			keep.bytes, keep.at = s.MaxSlotWALKeepBytes, time.Now()
		}
	}
	var sts []PGStandby
	if err := timed(func() (err error) { sts, err = readPGStandbys(ctx, db); return }); err != nil {
		return s, err
	}
	s.StandbyReplayLagSeconds = StandbyReplayLag(sts)
	if slot == "" {
		return s, nil
	}
	var retained, sentLag float64
	found := true
	err := timed(func() error {
		err := db.QueryRowContext(ctx, pgSlotSignalsSQL, slot).Scan(&s.SlotWALStatus, &retained, &sentLag)
		if err == sql.ErrNoRows {
			found = false
			return nil
		}
		return err
	})
	if err != nil {
		return s, fmt.Errorf("read PostgreSQL replication slot %q signals: %w", slot, err)
	}
	if !found {
		return s, nil
	}
	s.SlotFound = true
	s.SlotRetainedBytes, s.SlotSentLagBytes = int64(retained), int64(sentLag)
	return s, nil
}

// readPGStandbys đọc các walsender vật lý (pgStandbysSQL).
func readPGStandbys(ctx context.Context, db *sql.DB) ([]PGStandby, error) {
	rows, err := db.QueryContext(ctx, pgStandbysSQL)
	if err != nil {
		return nil, fmt.Errorf("read PostgreSQL standby replay lag: %w", err)
	}
	defer rows.Close()
	var sts []PGStandby
	for rows.Next() {
		var st PGStandby
		if err := rows.Scan(&st.Replays, &st.CaughtUp, &st.ReplayLagSeconds); err != nil {
			return nil, fmt.Errorf("read PostgreSQL standby replay lag: %w", err)
		}
		sts = append(sts, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read PostgreSQL standby replay lag: %w", err)
	}
	return sts, nil
}

// PGDBSignalReader đọc tín hiệu sức khỏe trực tiếp từ source PostgreSQL (02 §7: không phụ thuộc
// metrics_source). Mẫu gần nhất được giữ lại cho P_lag (Last).
type PGDBSignalReader struct {
	db             *sql.DB
	slot           string
	latencyTracker *LatencyTracker
	hard           *HardSignalDetector

	// SpoolUsage (tùy chọn): như MySQLDBSignalReader.SpoolUsage.
	SpoolUsage func() (used, hard int64)

	keep    pgKeepCache
	mu      sync.Mutex
	last    PGSample
	hasLast bool
}

func NewPGDBSignalReader(db *sql.DB, slot string, tracker *LatencyTracker) *PGDBSignalReader {
	if tracker == nil {
		// Baseline đo được, không phải hằng 10 ms (xem NewMeasuredLatencyTracker).
		tracker = NewMeasuredLatencyTracker()
	}
	return &PGDBSignalReader{db: db, slot: slot, latencyTracker: tracker, hard: NewHardSignalDetector()}
}

// Probe đọc thử một lần lúc dựng governor. Lỗi (thiếu quyền trên view, kết nối) → pipeline cảnh báo
// và dùng static. warnings: đọc được nhưng một phần tín hiệu bị ẩn (role không có pg_monitor:
// replay_lag của standby luôn NULL → 0).
func (r *PGDBSignalReader) Probe(ctx context.Context) (warnings []string, err error) {
	if r.db == nil {
		return nil, fmt.Errorf("no PostgreSQL source connection")
	}
	if _, err := r.Sample(ctx); err != nil {
		return nil, err
	}
	var monitor bool
	if err := r.db.QueryRowContext(ctx, pgMonitorRoleSQL).Scan(&monitor); err == nil && !monitor {
		warnings = append(warnings, "source role is neither superuser nor a member of pg_monitor: standby replay_lag is hidden and reads as 0")
	}
	return warnings, nil
}

// Sample đọc một mẫu mới (đo latency của câu poll) và giữ làm Last.
func (r *PGDBSignalReader) Sample(ctx context.Context) (PGSample, error) {
	r.mu.Lock()
	keep := r.keep
	r.mu.Unlock()
	s, err := readPGSample(ctx, r.db, r.slot, r.latencyTracker.Record, &keep)
	r.mu.Lock()
	r.keep = keep
	r.mu.Unlock()
	if err != nil {
		return s, err
	}
	r.mu.Lock()
	r.last, r.hasLast = s, true
	r.mu.Unlock()
	return s, nil
}

// Last trả mẫu đọc thành công gần nhất (ok = false nếu chưa có).
func (r *PGDBSignalReader) Last() (PGSample, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last, r.hasLast
}

func (r *PGDBSignalReader) ReadSignals(ctx context.Context) (DBSignals, error) {
	var used, hard int64
	if r.SpoolUsage != nil {
		used, hard = r.SpoolUsage()
	}
	signals := DBSignals{}
	signals.HardSignal, signals.HardReason = r.hard.Evaluate(nil, used, hard)
	if r.db != nil {
		s, err := r.Sample(ctx)
		if err != nil {
			// Mất metric: governor xử lý theo design §5.1 hàng 14 (đóng băng tăng, rồi giảm một nửa).
			return signals, err
		}
		signals = applyPGSample(signals, s)
	}
	signals.QueryP99 = r.latencyTracker.P99()
	signals.QueryBaseline = r.latencyTracker.Baseline()
	return signals, nil
}

// applyPGSample gộp mẫu PG vào DBSignals: replica lag và tín hiệu cứng của slot (cộng với tín hiệu
// cứng đã có, ví dụ spool).
func applyPGSample(sig DBSignals, s PGSample) DBSignals {
	sig.ReplicaLagSeconds = s.ReplicaLagSeconds()
	if hard, reason := s.SlotHard(); hard {
		if sig.HardSignal {
			sig.HardReason += "; " + reason
		} else {
			sig.HardSignal, sig.HardReason = true, reason
		}
	}
	return sig
}
