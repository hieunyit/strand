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
//     lớn nhất của các standby vật lý (walsender logical, gồm của Strand, bị loại). Breaker so với
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
// WAL đã nhận: standby của primary rảnh không bị tính là trễ), replay_lag lớn nhất của standby vật lý,
// max_slot_wal_keep_size (MB, -1 = không giới hạn).
const pgInstanceSignalsSQL = `SELECT pg_is_in_recovery(),
  CASE WHEN pg_is_in_recovery() AND pg_last_wal_receive_lsn() IS DISTINCT FROM pg_last_wal_replay_lsn()
       THEN COALESCE(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp()), 0)::float8
       ELSE 0::float8 END,
  (SELECT COALESCE(MAX(EXTRACT(EPOCH FROM r.replay_lag)), 0)::float8 FROM pg_stat_replication r
    WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots s WHERE s.active_pid = r.pid AND s.slot_type = 'logical')),
  COALESCE((SELECT setting FROM pg_settings WHERE name = 'max_slot_wal_keep_size'), '-1')`

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
	s := PGSample{MaxSlotWALKeepBytes: -1, SlotRetainedBytes: -1, SlotSentLagBytes: -1}
	var keep string
	if err := db.QueryRowContext(ctx, pgInstanceSignalsSQL).Scan(&s.InRecovery, &s.ReplayDelaySeconds,
		&s.StandbyReplayLagSeconds, &keep); err != nil {
		return s, fmt.Errorf("read PostgreSQL governor signals: %w", err)
	}
	s.MaxSlotWALKeepBytes = ParseMaxSlotWALKeepSize(keep)
	if slot == "" {
		return s, nil
	}
	var retained, sentLag float64
	err := db.QueryRowContext(ctx, pgSlotSignalsSQL, slot).Scan(&s.SlotWALStatus, &retained, &sentLag)
	switch {
	case err == sql.ErrNoRows:
		return s, nil
	case err != nil:
		return s, fmt.Errorf("read PostgreSQL replication slot %q signals: %w", slot, err)
	}
	s.SlotFound = true
	s.SlotRetainedBytes, s.SlotSentLagBytes = int64(retained), int64(sentLag)
	return s, nil
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

	mu      sync.Mutex
	last    PGSample
	hasLast bool
}

func NewPGDBSignalReader(db *sql.DB, slot string, tracker *LatencyTracker) *PGDBSignalReader {
	if tracker == nil {
		tracker = NewLatencyTracker(10 * time.Millisecond)
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
	start := time.Now()
	s, err := ReadPGSample(ctx, r.db, r.slot)
	r.latencyTracker.Record(time.Since(start))
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
