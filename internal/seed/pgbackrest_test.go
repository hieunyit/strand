package seed_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/exitcode"
	"strand/internal/seed"
	"strand/schema"
)

// spec: 02 §9; Invariants: SD-1, SD-2, SD-3, SD-4
// Planner pgBackRest / WAL-G chỉ đọc JSON metadata mẫu (không gọi binary).

var pgNow = time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)

const pgSysID = "7123456789012345678" // > 2^53: phải giữ nguyên chính xác, không qua float64

func writeJSON(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// pgbrInfo dựng `pgbackrest info --output=json`: full 10-25, full 11-01 (hằng tuần), diff 11-02.
func pgbrInfo(t *testing.T, version, archiveMax string) string {
	ts := func(s string) int64 { tt, _ := time.Parse(time.RFC3339, s); return tt.Unix() }
	return writeJSON(t, "info.json", fmt.Sprintf(`[{
  "name": "main",
  "status": {"code": 0, "message": "ok"},
  "db": [{"id": 1, "repo-key": 1, "system-id": %s, "version": %q}],
  "archive": [{"database": {"id": 1, "repo-key": 1}, "id": "%s-1",
               "min": "000000010000000000000001", "max": %q}],
  "backup": [
    {"label": "20261025-000000F", "type": "full", "error": false, "backrest": {"format": 5, "version": "2.60.0"},
     "database": {"id": 1, "repo-key": 1}, "lsn": {"start": "0/2000028", "stop": "0/2000100"},
     "timestamp": {"start": %d, "stop": %d}, "archive": {"start": "000000010000000000000002", "stop": "000000010000000000000002"}},
    {"label": "20261101-000000F", "type": "full", "error": false, "backrest": {"format": 5, "version": "2.60.0"},
     "database": {"id": 1, "repo-key": 1}, "lsn": {"start": "0/5000028", "stop": "0/5000138"},
     "timestamp": {"start": %d, "stop": %d}, "archive": {"start": "000000010000000000000005", "stop": "000000010000000000000005"}},
    {"label": "20261101-000000F_20261102-000000D", "type": "diff", "error": false, "backrest": {"format": 5, "version": "2.60.0"},
     "database": {"id": 1, "repo-key": 1}, "lsn": {"start": "0/7000028", "stop": "0/7000100"},
     "timestamp": {"start": %d, "stop": %d}, "archive": {"start": "000000010000000000000007", "stop": "000000010000000000000007"}}
  ]
}]`, pgSysID, version, version, archiveMax,
		ts("2026-10-25T00:00:00Z"), ts("2026-10-25T01:00:00Z"),
		ts("2026-11-01T00:00:00Z"), ts("2026-11-01T01:00:00Z"),
		ts("2026-11-02T00:00:00Z"), ts("2026-11-02T00:10:00Z")))
}

// walgList dựng `wal-g backup-list --detail --json` (LSN dạng số như WAL-G in ra) + wal-show.
func walgList(t *testing.T, pgVersion int) string {
	return writeJSON(t, "backups.json", fmt.Sprintf(`[
  {"backup_name": "base_000000010000000000000002", "time": "2026-10-25T01:00:00Z", "start_time": "2026-10-25T00:00:00Z",
   "finish_time": "2026-10-25T01:00:00Z", "pg_version": %d, "start_lsn": %d, "finish_lsn": %d, "is_permanent": false,
   "system_identifier": %s},
  {"backup_name": "base_000000010000000000000005", "time": "2026-11-01T01:00:00Z", "start_time": "2026-11-01T00:00:00Z",
   "finish_time": "2026-11-01T01:00:00Z", "pg_version": %d, "start_lsn": %d, "finish_lsn": %d, "is_permanent": false,
   "system_identifier": %s},
  {"backup_name": "base_000000010000000000000007_D_000000010000000000000005", "time": "2026-11-02T00:10:00Z",
   "start_time": "2026-11-02T00:00:00Z", "finish_time": "2026-11-02T00:10:00Z", "pg_version": %d,
   "start_lsn": %d, "finish_lsn": %d, "is_permanent": false, "system_identifier": %s}
]`, pgVersion, 0x2000028, 0x2000100, pgSysID, pgVersion, 0x5000028, 0x5000138, pgSysID,
		pgVersion, 0x7000028, 0x7000100, pgSysID))
}

func walShow(t *testing.T, end string, missing ...string) string {
	m, _ := json.Marshal(missing)
	if missing == nil {
		m = []byte("[]")
	}
	return writeJSON(t, "walshow.json", fmt.Sprintf(`[{"id": 1, "parent_id": 0, "switch_point_lsn": 0,
  "start_segment": "000000010000000000000001", "end_segment": %q, "segments_count": 10,
  "missing_segments": %s, "segment_range_size": 10, "status": "OK"}]`, end, m))
}

// pgSrc là SeedSource hợp lệ: P = 0/9000060 (segment 9) đọc từ slot, target PG 16.
func pgSrc(ref string) seed.SeedSource {
	return seed.SeedSource{
		Engine: "pg", BackupRef: ref, Slot: "strand_orders",
		ConsistentPointLSN: "0/9000060", SourceSystemID: pgSysID, SourceVersion: "16.4",
		TargetVersion: "16.6", ETA: 2 * time.Hour, RetentionFull: 2,
		Tables: []string{"public.orders"},
	}
}

type pgPlanner interface {
	Plan(context.Context, seed.SeedSource) (*seed.SeedManifest, error)
}

func pgPlanners(t *testing.T) map[string]func(mut func(*seed.SeedSource), version string, archiveMax string) (*seed.SeedManifest, error) {
	return map[string]func(func(*seed.SeedSource), string, string) (*seed.SeedManifest, error){
		"pgbackrest": func(mut func(*seed.SeedSource), version, archiveMax string) (*seed.SeedManifest, error) {
			src := pgSrc(pgbrInfo(t, version, archiveMax))
			if mut != nil {
				mut(&src)
			}
			return (&seed.PgBackRestPlanner{Now: func() time.Time { return pgNow }}).Plan(context.Background(), src)
		},
		"walg": func(mut func(*seed.SeedSource), version, archiveMax string) (*seed.SeedManifest, error) {
			v := 160004
			if version == "17" {
				v = 170002
			}
			src := pgSrc(walgList(t, v))
			src.ArchiveInfoRef = walShow(t, archiveMax)
			src.ToolVersion = "v3.0.5"
			if mut != nil {
				mut(&src)
			}
			return (&seed.WalgPlanner{Now: func() time.Time { return pgNow }}).Plan(context.Background(), src)
		},
	}
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var se *exitcode.Error
	if !errors.As(err, &se) || se.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

// TestSD1_PGPhysical_Valid: backup hợp lệ → P = LSN của slot, chọn backup mới nhất kết thúc trước P,
// source_identity = system-id chính xác, expire ước lượng, manifest khớp seed.schema.json.
func TestSD1_PGPhysical_Valid(t *testing.T) {
	schemaBytes, err := os.ReadFile("../../schema/seed.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	val, err := schema.NewSeedSchemaValidator(schemaBytes)
	if err != nil {
		t.Fatal(err)
	}
	wantRef := map[string]string{
		"pgbackrest": "pgbackrest://main/20261101-000000F_20261102-000000D",
		"walg":       "wal-g://base_000000010000000000000007_D_000000010000000000000005",
	}
	for kind, plan := range pgPlanners(t) {
		t.Run(kind, func(t *testing.T) {
			m, err := plan(nil, "16", "00000001000000000000000A")
			if err != nil {
				t.Fatalf("plan: %v", err)
			}
			if m.StartPosition.LSN != "0/9000060" || m.StartPosition.Slot != "strand_orders" {
				t.Fatalf("start_position = %+v", m.StartPosition)
			}
			if m.SourceIdentity != pgSysID {
				t.Fatalf("source_identity = %q, want %q", m.SourceIdentity, pgSysID)
			}
			if m.BackupRef != wantRef[kind] || m.Kind != kind || m.Engine != "pg" {
				t.Fatalf("kind/engine/ref = %s/%s/%s", m.Kind, m.Engine, m.BackupRef)
			}
			// root full 11-01, k=1, retention 2 → expire khi full thứ 2 kể từ 11-01: 11-15.
			if m.BackupExpiresAt == nil || *m.BackupExpiresAt != "2026-11-15T00:00:00Z" {
				t.Fatalf("backup_expires_at = %v", m.BackupExpiresAt)
			}
			if !m.Target.Compatible || m.LogCoverage == nil || !m.LogCoverage.OK {
				t.Fatalf("target/log coverage = %+v / %+v", m.Target, m.LogCoverage)
			}
			raw, _ := json.Marshal(m)
			if err := val.Validate(raw); err != nil {
				t.Fatalf("schema: %v", err)
			}
		})
	}
}

// TestSD1_PGPhysical_ExpiresInETA: backup expire trong ETA → từ chối.
func TestSD1_PGPhysical_ExpiresInETA(t *testing.T) {
	for kind, plan := range pgPlanners(t) {
		t.Run(kind, func(t *testing.T) {
			// retention 1: full 11-01 bị xóa khi full kế tiếp (11-08) xong; ETA 6 ngày → 11-09 > 11-08.
			_, err := plan(func(s *seed.SeedSource) { s.RetentionFull = 1; s.ETA = 144 * time.Hour }, "16", "00000001000000000000000A")
			wantCode(t, err, exitcode.ErrSeedInvariantFailed)
			if !strings.Contains(err.Error(), "expire") {
				t.Fatalf("err = %v", err)
			}
			// cùng retention, ETA ngắn → chấp nhận (ranh giới đúng chiều).
			if _, err := plan(func(s *seed.SeedSource) { s.RetentionFull = 1; s.ETA = 2 * time.Hour }, "16", "00000001000000000000000A"); err != nil {
				t.Fatalf("short ETA: %v", err)
			}
			// không biết retention → không chứng minh được → từ chối.
			_, err = plan(func(s *seed.SeedSource) { s.RetentionFull = 0 }, "16", "00000001000000000000000A")
			wantCode(t, err, exitcode.ErrSeedInvariantFailed)
		})
	}
}

// TestSD4_PGPhysical_MajorVersionMismatch: backup PG16, target PG17 → từ chối.
func TestSD4_PGPhysical_MajorVersionMismatch(t *testing.T) {
	for kind, plan := range pgPlanners(t) {
		t.Run(kind, func(t *testing.T) {
			_, err := plan(func(s *seed.SeedSource) { s.TargetVersion = "17.2" }, "16", "00000001000000000000000A")
			wantCode(t, err, exitcode.ErrSeedInvariantFailed)
			if !strings.Contains(err.Error(), "SD-4") {
				t.Fatalf("err = %v", err)
			}
			_, err = plan(func(s *seed.SeedSource) { s.TargetVersion = "" }, "16", "00000001000000000000000A")
			wantCode(t, err, exitcode.ErrSeedInvariantFailed)
		})
	}
}

// TestSD2_PGPhysical_ArchiveGap: archive không phủ tới segment chứa P → log_gap.
func TestSD2_PGPhysical_ArchiveGap(t *testing.T) {
	for kind, plan := range pgPlanners(t) {
		t.Run(kind, func(t *testing.T) {
			_, err := plan(nil, "16", "000000010000000000000008")
			wantCode(t, err, exitcode.ErrLogGap)
			if exitcode.GetExitCode(err) != exitcode.DataError {
				t.Fatalf("exit code = %d", exitcode.GetExitCode(err))
			}
		})
	}
	t.Run("walg-missing-segment", func(t *testing.T) {
		src := pgSrc(walgList(t, 160004))
		src.ArchiveInfoRef = walShow(t, "00000001000000000000000A", "000000010000000000000008")
		_, err := (&seed.WalgPlanner{Now: func() time.Time { return pgNow }}).Plan(context.Background(), src)
		wantCode(t, err, exitcode.ErrLogGap)
	})
	t.Run("walg-no-archive-info", func(t *testing.T) {
		src := pgSrc(walgList(t, 160004))
		_, err := (&seed.WalgPlanner{Now: func() time.Time { return pgNow }}).Plan(context.Background(), src)
		wantCode(t, err, exitcode.ErrLogGap)
	})
}

// TestSD1_PGPhysical_PositionAndIdentity: P phải từ slot, backup phải kết thúc trước P, cùng cluster,
// target đã PITR phải dừng đúng P.
func TestSD1_PGPhysical_PositionAndIdentity(t *testing.T) {
	cases := map[string]func(*seed.SeedSource){
		"no-P":             func(s *seed.SeedSource) { s.ConsistentPointLSN = "" },
		"P-before-backups": func(s *seed.SeedSource) { s.ConsistentPointLSN = "0/1000000" },
		"label-after-P": func(s *seed.SeedSource) {
			s.ConsistentPointLSN = "0/6000000"
			s.BackupLabel = "20261101-000000F_20261102-000000D"
		},
		"other-cluster":       func(s *seed.SeedSource) { s.SourceSystemID = "7123456789012345679" },
		"replay-lsn-mismatch": func(s *seed.SeedSource) { s.TargetRestoredLSN = "0/9000000" },
	}
	plan := pgPlanners(t)["pgbackrest"]
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := plan(mut, "16", "00000001000000000000000A")
			wantCode(t, err, exitcode.ErrSeedInvariantFailed)
		})
	}
	// P giữa full 11-01 và diff 11-02 → chọn full 11-01; replay LSN khớp → cross-check ghi nhận.
	m, err := plan(func(s *seed.SeedSource) {
		s.ConsistentPointLSN = "0/6000000"
		s.TargetRestoredLSN = "0/6000000"
	}, "16", "00000001000000000000000A")
	if err != nil {
		t.Fatal(err)
	}
	if m.BackupRef != "pgbackrest://main/20261101-000000F" || m.PositionCrossCheck == nil || !m.PositionCrossCheck.Match {
		t.Fatalf("ref=%s cross=%+v", m.BackupRef, m.PositionCrossCheck)
	}
}

// TestPGPhysical_PrereleaseTool: công cụ bản RC bị từ chối nếu không có --allow-prerelease-tool.
func TestPGPhysical_PrereleaseTool(t *testing.T) {
	plan := pgPlanners(t)["walg"]
	_, err := plan(func(s *seed.SeedSource) { s.ToolVersion = "v3.1.0-rc1" }, "16", "00000001000000000000000A")
	wantCode(t, err, exitcode.ErrPrereleaseTool)
	if _, err := plan(func(s *seed.SeedSource) { s.ToolVersion = "v3.1.0-rc1"; s.AllowPrereleaseTool = true }, "16", "00000001000000000000000A"); err != nil {
		t.Fatal(err)
	}
}

var _ pgPlanner = seed.NewPgBackRestPlanner()
var _ pgPlanner = seed.NewWalgPlanner()
