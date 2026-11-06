package seed

import (
	"context"
	"time"
)

// spec: 02 §3, §9

// SeedSource biểu diễn thông tin đầu vào cho một kế hoạch seed.
type SeedSource struct {
	Engine                   string   `json:"engine"` // mysql | pg
	Kind                     string   `json:"kind"`   // xtrabackup | clone | mydumper | mysqldump | pgbackrest | walg | strand-export
	BackupRef                string   `json:"backup_ref"`
	AllowPrereleaseTool      bool     `json:"allow_prerelease_tool"`
	Tables                   []string `json:"tables"`
	TargetVersion            string   `json:"target_version"`
	TargetCompatible         bool     `json:"target_compatible"`
	TargetRestoredGTID       string   `json:"target_restored_gtid"`
	SourcePurgedGTID         string   `json:"source_purged_gtid"`
	ArchiveUntilGTID         string   `json:"archive_until_gtid"`
	DumpLog                  string   `json:"dump_log,omitempty"`
	AcceptUnverifiedLockMode bool     `json:"accept_unverified_lock_mode,omitempty"`
	// SourceArch / TargetArch: kiến trúc CPU (@@version_compile_machine) của server tạo backup và của
	// target, để kiểm SD-4 cho backup vật lý MySQL; rỗng = không biết, seed plan từ chối (review R32).
	SourceArch string `json:"source_arch,omitempty"`
	TargetArch string `json:"target_arch,omitempty"`

	// --- PostgreSQL (pgbackrest | walg) ---
	// Slot logical trên source (hoặc standby PG16+) mà CDC sẽ đọc; P = consistent_point của slot.
	Slot string `json:"slot,omitempty"`
	// ConsistentPointLSN: P (dạng X/Y) đọc từ slot (confirmed_flush_lsn của slot mới tạo), không gõ tay (SD-1).
	ConsistentPointLSN string `json:"consistent_point_lsn,omitempty"`
	// TargetRestoredLSN: pg_last_wal_replay_lsn() của target sau PITR (đối chiếu chéo SD-1), có thể rỗng.
	TargetRestoredLSN string `json:"target_restored_lsn,omitempty"`
	// SourceSystemID / SourceVersion: systemid (IDENTIFY_SYSTEM) và server_version của source.
	SourceSystemID string `json:"source_system_id,omitempty"`
	SourceVersion  string `json:"source_version,omitempty"`
	// BackupLabel chọn một backup cụ thể (label pgBackRest / backup_name WAL-G); rỗng = backup mới nhất có stop LSN ≤ P.
	BackupLabel string `json:"backup_label,omitempty"`
	// ArchiveInfoRef: file JSON `wal-g wal-show --detailed-json` (WAL-G không có phạm vi archive trong backup-list).
	ArchiveInfoRef string `json:"archive_info_ref,omitempty"`
	// ToolVersion: phiên bản công cụ khi metadata không ghi (WAL-G).
	ToolVersion string `json:"tool_version,omitempty"`
	// ETA restore; RetentionFull (repo1-retention-full / wal-g delete retain FULL N) và chu kỳ full backup
	// để ước lượng thời điểm backup bị expire (02 §9).
	ETA                time.Duration `json:"eta,omitempty"`
	RetentionFull      int           `json:"retention_full,omitempty"`
	FullBackupInterval time.Duration `json:"full_backup_interval,omitempty"`
	// WALSegmentSize của cluster; 0 = 16 MiB.
	WALSegmentSize uint64 `json:"wal_segment_size,omitempty"`
}

// StartPosition biểu diễn vị trí P nhất quán của backup.
type StartPosition struct {
	GTIDSet string `json:"gtid_set,omitempty"`
	// LSN (PG) dạng "X/Y" = consistent_point của slot. Txn có commit LSN < LSN nằm trong seed;
	// Txn có commit LSN ≥ LSN được slot stream (commit record có thể bắt đầu đúng tại consistent_point).
	LSN string `json:"lsn,omitempty"`
	// Slot (PG): slot logical gắn với P; CDC catch-up đọc từ slot này (thêm vào, cộng thêm theo 02 §9).
	Slot string `json:"slot,omitempty"`
}

// PositionCrossCheck đối chiếu chéo vị trí P từ nhiều nguồn metadata (SD-1).
type PositionCrossCheck struct {
	RestoredGTIDExecuted string `json:"restored_gtid_executed,omitempty"`
	RestoredLSN          string `json:"restored_lsn,omitempty"`
	Match                bool   `json:"match"`
}

// TargetInfo thông tin phiên bản và tính tương thích của đích (SD-4).
type TargetInfo struct {
	Version    string `json:"version"`
	Compatible bool   `json:"compatible"`
	// Arch: kiến trúc CPU đã đối chiếu với source (backup vật lý MySQL, review R32).
	Arch string `json:"arch,omitempty"`
}

// IntegrityInfo kiểm tra tính toàn vẹn của backup (SD-1).
type IntegrityInfo struct {
	PrepareOK bool `json:"prepare_ok"`
}

// LogCoverageInfo độ bao phủ của log từ P đến hiện tại (SD-2).
type LogCoverageInfo struct {
	SourceGTIDPurged string `json:"source_gtid_purged,omitempty"`
	ArchiveUntil     string `json:"archive_until,omitempty"`
	OK               bool   `json:"ok"`
}

// SeedManifest cấu trúc đại diện cho file seed.json v1 theo schema/seed.schema.json và spec: 02 §9.
type SeedManifest struct {
	Version            int                 `json:"version"`
	Engine             string              `json:"engine"`
	Kind               string              `json:"kind"`
	ToolVersion        string              `json:"tool_version"`
	ToolPrerelease     bool                `json:"tool_prerelease"`
	SourceIdentity     string              `json:"source_identity"`
	BackupRef          string              `json:"backup_ref"`
	StartPosition      StartPosition       `json:"start_position"`
	PositionCrossCheck *PositionCrossCheck `json:"position_cross_check,omitempty"`
	SchemaSnapshot     string              `json:"schema_snapshot"`
	Tables             []string            `json:"tables"`
	Integrity          *IntegrityInfo      `json:"integrity,omitempty"`
	LogCoverage        *LogCoverageInfo    `json:"log_coverage,omitempty"`
	LogPrefetchUntil   *string             `json:"log_prefetch_until,omitempty"`
	LegalHoldRef       *string             `json:"legal_hold_ref,omitempty"`
	BackupExpiresAt    *string             `json:"backup_expires_at,omitempty"`
	Target             TargetInfo          `json:"target"`
	Warnings           []string            `json:"warnings,omitempty"`
	CreatedAt          string              `json:"created_at"`
}

// Seeder interface Go quy định tại spec: 02 §3.
type Seeder interface {
	Plan(ctx context.Context, src SeedSource) (*SeedManifest, error) // kiểm SD-1..SD-4
	Apply(ctx context.Context, p *SeedManifest) error
}
