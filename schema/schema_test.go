package schema

import (
	"encoding/json"
	"os"
	"testing"
)

// AC4: Schema chấp nhận ví dụ seed.json ở S02 §9, từ chối khi thiếu trường bắt buộc.
func TestAC4_SeedSchemaValidation(t *testing.T) {
	schemaBytes, err := os.ReadFile("seed.schema.json")
	if err != nil {
		t.Fatalf("failed to read seed.schema.json: %v", err)
	}

	validator, err := NewSeedSchemaValidator(schemaBytes)
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}

	// 1. Ví dụ chuẩn từ spec 02 §9
	validSeedExample := `{
  "version": 1,
  "engine": "mysql",
  "kind": "xtrabackup",
  "tool_version": "8.4.0-6",
  "tool_prerelease": false,
  "source_identity": "3E11FA47-71CA-11E1-9E33-C80AA9429562",
  "backup_ref": "s3://mysql-backup/orders-db/full-2026-10-30",
  "start_position": { "gtid_set": "3E11FA47-…:1-982341" },
  "position_cross_check": { "restored_gtid_executed": "…:1-982341", "match": true },
  "schema_snapshot": "s3://mysql-backup/orders-db/full-2026-10-30/schema.sql",
  "tables": ["app.orders", "app.users"],
  "integrity": { "prepare_ok": true },
  "log_coverage": { "source_gtid_purged": "…:1-950000", "archive_until": "…:1-991200", "ok": true },
  "log_prefetch_until": "…:1-993000",
  "legal_hold_ref": "s3://mysql-binlog/orders-db/?hold=seed-20261031",
  "backup_expires_at": null,
  "target": { "version": "8.4.6", "compatible": true },
  "created_at": "2026-10-31T02:10:00Z"
}`

	if err := validator.Validate([]byte(validSeedExample)); err != nil {
		t.Fatalf("Schema phải chấp nhận ví dụ chuẩn ở S02 §9, nhưng bị từ chối: %v", err)
	}
	t.Log("AC4: Đã chấp nhận ví dụ seed.json chuẩn trong spec 02 §9")

	// 2. Từ chối khi thiếu từng trường bắt buộc
	requiredFields := []string{
		"version", "engine", "kind", "tool_version", "tool_prerelease",
		"source_identity", "backup_ref", "start_position", "schema_snapshot",
		"tables", "target", "created_at",
	}

	for _, field := range requiredFields {
		var doc map[string]any
		if err := json.Unmarshal([]byte(validSeedExample), &doc); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		delete(doc, field) // Xóa trường bắt buộc

		mutatedJSON, _ := json.Marshal(doc)
		if err := validator.Validate(mutatedJSON); err == nil {
			t.Fatalf("Kỳ vọng schema từ chối khi thiếu trường bắt buộc %q nhưng lại chấp nhận", field)
		}
	}
	t.Log("AC4: Đã từ chối khi thiếu bất kỳ trường bắt buộc nào")

	// 3. Từ chối khi engine không hợp lệ
	invalidEngineJSON := `{
  "version": 1,
  "engine": "oracle",
  "kind": "xtrabackup",
  "tool_version": "8.4.0-6",
  "tool_prerelease": false,
  "source_identity": "uuid",
  "backup_ref": "s3://ref",
  "start_position": {},
  "schema_snapshot": "s3://schema",
  "tables": ["t"],
  "target": { "version": "1.0", "compatible": true },
  "created_at": "2026-10-31T02:10:00Z"
}`
	if err := validator.Validate([]byte(invalidEngineJSON)); err == nil {
		t.Fatalf("Kỳ vọng schema từ chối engine không hợp lệ 'oracle'")
	}

	// 4. Từ chối khi target.compatible thiếu hoặc sai kiểu
	invalidTargetJSON := `{
  "version": 1,
  "engine": "mysql",
  "kind": "xtrabackup",
  "tool_version": "8.4.0-6",
  "tool_prerelease": false,
  "source_identity": "uuid",
  "backup_ref": "s3://ref",
  "start_position": {},
  "schema_snapshot": "s3://schema",
  "tables": ["t"],
  "target": { "version": "8.4.6" },
  "created_at": "2026-10-31T02:10:00Z"
}`
	if err := validator.Validate([]byte(invalidTargetJSON)); err == nil {
		t.Fatalf("Kỳ vọng schema từ chối khi target thiếu compatible")
	}

	t.Log("AC4 PASS: Schema seed.schema.json kiểm thực đầy đủ và chính xác các quy tắc")
}
