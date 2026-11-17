package breaker

import (
	"time"
)

// GlobalCounters holds selected status counters relevant to InnoDB redo log, buffer pool, and commits.
type GlobalCounters struct {
	Timestamp                time.Time         `json:"timestamp"`
	InnodbLogWaits           uint64            `json:"innodb_log_waits"`
	InnodbBufferPoolWaitFree uint64            `json:"innodb_buffer_pool_wait_free"`
	InnodbDataFsyncs         uint64            `json:"innodb_data_fsyncs"`
	InnodbOsLogFsyncs        uint64            `json:"innodb_os_log_fsyncs"`
	InnodbOsLogWritten       uint64            `json:"innodb_os_log_written"`
	InnodbDataWrites         uint64            `json:"innodb_data_writes"`
	HandlerCommit            uint64            `json:"handler_commit"`
	RawStatus                map[string]uint64 `json:"raw_status,omitempty"`
}

// GlobalCounterDelta captures the difference between two sample points, handling counter resets.
type GlobalCounterDelta struct {
	ElapsedDuration         time.Duration `json:"elapsed_duration"`
	ResetDetected           bool          `json:"reset_detected"`
	DeltaInnodbLogWaits     uint64        `json:"delta_innodb_log_waits"`
	DeltaBufferPoolWaitFree uint64        `json:"delta_innodb_buffer_pool_wait_free"`
	DeltaInnodbDataFsyncs   uint64        `json:"delta_innodb_data_fsyncs"`
	DeltaInnodbOsLogFsyncs  uint64        `json:"delta_innodb_os_log_fsyncs"`
	DeltaInnodbOsLogWritten uint64        `json:"delta_innodb_os_log_written"`
	DeltaInnodbDataWrites   uint64        `json:"delta_innodb_data_writes"`
	DeltaHandlerCommit      uint64        `json:"delta_handler_commit"`
}

// SafeVariables captures relevant, safe configuration variables without secrets.
type SafeVariables struct {
	Version                   string `json:"version"`
	VersionComment            string `json:"version_comment"`
	InnodbLogFileSize         uint64 `json:"innodb_log_file_size"`
	InnodbLogBufferSize       uint64 `json:"innodb_log_buffer_size"`
	InnodbBufferPoolSize      uint64 `json:"innodb_buffer_pool_size"`
	InnodbFlushLogAtTrxCommit int    `json:"innodb_flush_log_at_trx_commit"`
	MaxConnections            uint64 `json:"max_connections"`
}

// ActiveWaitEntry captures non-sensitive process/wait status without query text or credentials.
type ActiveWaitEntry struct {
	ID       uint64 `json:"id"`
	User     string `json:"user"`
	Host     string `json:"host"`
	DB       string `json:"db"`
	Command  string `json:"command"`
	Time     int64  `json:"time_seconds"`
	State    string `json:"state"`
	WaitInfo string `json:"wait_info,omitempty"`
}

// SanitizedEngineStatus contains parsed/sanitized sections of SHOW ENGINE INNODB STATUS.
type SanitizedEngineStatus struct {
	Timestamp           time.Time `json:"timestamp"`
	RawSanitizedText    string    `json:"raw_sanitized_text"`
	LogSequenceNumber   uint64    `json:"log_sequence_number,omitempty"`
	LogFlushedUpTo      uint64    `json:"log_flushed_up_to,omitempty"`
	PagesFlushedUpTo    uint64    `json:"pages_flushed_up_to,omitempty"`
	LastCheckpointAt    uint64    `json:"last_checkpoint_at,omitempty"`
	LogPendingFsyncs    uint64    `json:"log_pending_fsyncs,omitempty"`
	LogPendingWrites    uint64    `json:"log_pending_writes,omitempty"`
	PendingChkpWrites   uint64    `json:"pending_chkp_writes,omitempty"`
	PendingBufferPoolIO uint64    `json:"pending_buffer_pool_io,omitempty"`
	SemaphoresWaitInfo  string    `json:"semaphores_wait_info,omitempty"`
}

// TelemetryRow represents a sample from telemetry CSV.
type TelemetryRow struct {
	TimestampRaw  string    `json:"timestamp_raw"`
	ParsedTime    time.Time `json:"parsed_time"`
	UnixNs        int64     `json:"unix_ns"`
	R             float64   `json:"r"`
	P99           float64   `json:"p99"`
	Baseline      float64   `json:"baseline"`
	Ratio         float64   `json:"ratio"`
	Breaker       int       `json:"breaker"`
	Zone          int       `json:"zone"`
	Pressure      float64   `json:"pressure"`
	LagSeconds    float64   `json:"lag_seconds"`
	BreakerReason string    `json:"breaker_reason,omitempty"`
	CPUPct        *float64  `json:"cpu_pct,omitempty"`
	IOWaitPct     *float64  `json:"io_wait_pct,omitempty"`
}

// DiagnosticSnapshot is a point-in-time read-only diagnostic collection.
type DiagnosticSnapshot struct {
	CollectedAt      time.Time             `json:"collected_at"`
	ServerVersion    string                `json:"server_version"`
	Variables        SafeVariables         `json:"variables"`
	StatusCounters   GlobalCounters        `json:"status_counters"`
	StatusDelta      *GlobalCounterDelta   `json:"status_delta,omitempty"`
	ActiveWaits      []ActiveWaitEntry     `json:"active_waits"`
	EngineStatus     SanitizedEngineStatus `json:"engine_status"`
	AlignedTelemetry *TelemetryRow         `json:"aligned_telemetry,omitempty"`
}

// P2PAnalysisResult summarizes the p2p analysis.
type P2PAnalysisResult struct {
	WorstLogRatio       float64      `json:"worst_log_ratio"`
	WindowIndex         int          `json:"window_index"`
	MaximumSample       TelemetryRow `json:"maximum_sample"`
	MinimumSample       TelemetryRow `json:"minimum_sample"`
	DirectCause         string       `json:"direct_cause"`
	LatencyCause        string       `json:"latency_cause"`
	LatencyCauseStatus  string       `json:"latency_cause_status"` // "RESOLVED" or "UNRESOLVED"
	DiagnosisNotes      []string     `json:"diagnosis_notes"`
	CounterResetsSeen   int          `json:"counter_resets_seen"`
	MissingMetricsCount int          `json:"missing_metrics_count"`
}
