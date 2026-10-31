package schema

import (
	"encoding/json"
	"fmt"
	"time"
)

// SeedSchemaValidator kiểm thực tài liệu seed.json dựa trên lược đồ seed.schema.json.
type SeedSchemaValidator struct {
	schema map[string]any
}

// NewSeedSchemaValidator khởi tạo validator từ nội dung JSON của seed.schema.json.
func NewSeedSchemaValidator(schemaJSON []byte) (*SeedSchemaValidator, error) {
	var m map[string]any
	if err := json.Unmarshal(schemaJSON, &m); err != nil {
		return nil, fmt.Errorf("invalid schema json: %w", err)
	}
	return &SeedSchemaValidator{schema: m}, nil
}

// Validate kiểm tra tài liệu documentJSON có tuân thủ schema không.
func (v *SeedSchemaValidator) Validate(documentJSON []byte) error {
	var doc map[string]any
	if err := json.Unmarshal(documentJSON, &doc); err != nil {
		return fmt.Errorf("invalid json document: %w", err)
	}

	// 1. Kiểm tra các trường bắt buộc (required)
	reqRaw, ok := v.schema["required"]
	if ok {
		if reqSlice, ok := reqRaw.([]any); ok {
			for _, r := range reqSlice {
				fieldName, ok := r.(string)
				if !ok {
					continue
				}
				val, exists := doc[fieldName]
				if !exists || val == nil {
					return fmt.Errorf("thiếu trường bắt buộc: %q", fieldName)
				}
			}
		}
	}

	// 2. Kiểm tra phiên bản
	if ver, ok := doc["version"].(float64); !ok || int(ver) != 1 {
		return fmt.Errorf("trường version không hợp lệ: %v (kỳ vọng 1)", doc["version"])
	}

	// 3. Kiểm tra engine
	engine, ok := doc["engine"].(string)
	if !ok || (engine != "mysql" && engine != "pg" && engine != "postgres") {
		return fmt.Errorf("trường engine không hợp lệ: %v", doc["engine"])
	}

	// 4. Kiểm tra kind
	kind, ok := doc["kind"].(string)
	validKinds := map[string]bool{
		"xtrabackup": true, "clone": true, "mydumper": true,
		"pgbackrest": true, "wal-g": true, "strand_export": true,
	}
	if !ok || !validKinds[kind] {
		return fmt.Errorf("trường kind không hợp lệ: %v", doc["kind"])
	}

	// 5. Kiểm tra tool_version và tool_prerelease
	if _, ok := doc["tool_version"].(string); !ok {
		return fmt.Errorf("trường tool_version phải là chuỗi")
	}
	if _, ok := doc["tool_prerelease"].(bool); !ok {
		return fmt.Errorf("trường tool_prerelease phải là boolean")
	}

	// 6. Kiểm tra tables
	tables, ok := doc["tables"].([]any)
	if !ok || len(tables) == 0 {
		return fmt.Errorf("trường tables phải là mảng không rỗng")
	}
	for _, tbl := range tables {
		if _, ok := tbl.(string); !ok {
			return fmt.Errorf("phần tử trong tables phải là string")
		}
	}

	// 7. Kiểm tra target
	target, ok := doc["target"].(map[string]any)
	if !ok {
		return fmt.Errorf("trường target phải là object")
	}
	if _, ok := target["version"].(string); !ok {
		return fmt.Errorf("target.version phải là chuỗi")
	}
	if _, ok := target["compatible"].(bool); !ok {
		return fmt.Errorf("target.compatible phải là boolean")
	}

	// 8. Kiểm tra created_at theo chuẩn RFC3339 / ISO 8601
	createdAtStr, ok := doc["created_at"].(string)
	if !ok {
		return fmt.Errorf("trường created_at phải là chuỗi thời gian")
	}
	if _, err := time.Parse(time.RFC3339, createdAtStr); err != nil {
		return fmt.Errorf("created_at không đúng định dạng RFC3339: %w", err)
	}

	return nil
}
