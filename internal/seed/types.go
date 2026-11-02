package seed

import (
	"context"
)

// spec: 02 §3, §9

// SeedSource biểu diễn thông tin đầu vào cho một kế hoạch seed.
type SeedSource struct {
<<<<<<< HEAD
	Engine              string   `json:"engine"` // mysql | pg
	Kind                string   `json:"kind"`   // xtrabackup | clone | mydumper | mysqldump | pgbackrest
	BackupRef           string   `json:"backup_ref"`
	AllowPrereleaseTool bool     `json:"allow_prerelease_tool"`
	Tables              []string `json:"tables"`
	TargetVersion       string   `json:"target_version"`
	TargetCompatible    bool     `json:"target_compatible"`
	TargetRestoredGTID  string   `json:"target_restored_gtid"`
	SourcePurgedGTID    string   `json:"source_purged_gtid"`
	ArchiveUntilGTID    string   `json:"archive_until_gtid"`
=======
	Engine                   string   `json:"engine"` // mysql | pg
	Kind                     string   `json:"kind"`   // xtrabackup | clone | mydumper | pgbackrest
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
>>>>>>> agent/dumplog
}

// StartPosition biểu diễn vị trí P nhất quán của backup.
type StartPosition struct {
	GTIDSet string `json:"gtid_set,omitempty"`
	LSN     string `json:"lsn,omitempty"`
}

// PositionCrossCheck đối chiếu chéo vị trí P từ nhiều nguồn metadata (SD-1).
type PositionCrossCheck struct {
	RestoredGTIDExecuted string `json:"restored_gtid_executed,omitempty"`
	Match                bool   `json:"match"`
}

// TargetInfo thông tin phiên bản và tính tương thích của đích (SD-4).
type TargetInfo struct {
	Version    string `json:"version"`
	Compatible bool   `json:"compatible"`
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
