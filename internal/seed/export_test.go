package seed_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/seed"
	"strand/schema"
)

// TestSD1_Export_ValidateConfig kiểm tra cấu hình ExportConfig bắt buộc các trường hợp lệ.
func TestSD1_Export_ValidateConfig(t *testing.T) {
	ctx := context.Background()

	// 1. Thiếu SourceDSN
	_, err := seed.RunExport(ctx, seed.ExportConfig{
		Tables: []string{"t1"},
		Out:    "/tmp/out",
	})
	if err == nil {
		t.Fatalf("expected error for missing SourceDSN, got nil")
	}

	// 2. Thiếu Tables
	_, err = seed.RunExport(ctx, seed.ExportConfig{
		SourceDSN: "postgres://user:pass@localhost:5432/db",
		Out:       "/tmp/out",
	})
	if err == nil {
		t.Fatalf("expected error for missing Tables, got nil")
	}

	// 3. Thiếu OutDir
	_, err = seed.RunExport(ctx, seed.ExportConfig{
		SourceDSN: "postgres://user:pass@localhost:5432/db",
		Tables:    []string{"t1"},
	})
	if err == nil {
		t.Fatalf("expected error for missing Out, got nil")
	}

	// 4. --out từ xa (s3://) chưa hỗ trợ; định dạng lạ bị từ chối — đều trước khi chạm vào source.
	for _, c := range []seed.ExportConfig{
		{SourceDSN: "postgres://u@localhost:1/db", Tables: []string{"t1"}, Out: "s3://b/x"},
		{SourceDSN: "postgres://u@localhost:1/db", Tables: []string{"t1"}, Out: "/tmp/x", Format: "z"},
	} {
		if _, err := seed.RunExport(ctx, c); err == nil {
			t.Fatalf("expected error for %+v", c)
		}
	}
}

// TestSD1_Export_ManifestSchema kiểm tra cấu trúc seed.json sinh bởi strand export tuân thủ seed.schema.json.
func TestSD1_Export_ManifestSchema(t *testing.T) {
	manifest := &seed.SeedManifest{
		Version:        1,
		Engine:         "pg",
		Kind:           "strand-export",
		ToolVersion:    "pg_dump 18.0",
		ToolPrerelease: false,
		SourceIdentity: "7123456789012345678",
		BackupRef:      "/var/backup/pg-export",
		StartPosition: seed.StartPosition{
			LSN:  checkpoint.FormatLSN(0x16B2780),
			Slot: "strand_export_1",
		},
		PositionCrossCheck: nil,
		SchemaSnapshot:     "/var/backup/pg-export",
		Tables:             []string{"public.orders", "public.users"},
		Integrity:          &seed.IntegrityInfo{PrepareOK: true},
		Target: seed.TargetInfo{
			Version:    "18.0",
			Compatible: true,
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	schemaBytes, err := os.ReadFile("../../schema/seed.schema.json")
	if err != nil {
		t.Fatalf("read seed.schema.json failed: %v", err)
	}
	validator, err := schema.NewSeedSchemaValidator(schemaBytes)
	if err != nil {
		t.Fatalf("create validator failed: %v", err)
	}
	if err := validator.Validate(raw); err != nil {
		t.Fatalf("manifest failed seed.schema.json validation: %v", err)
	}
}
