// Package archive hiện thực reader đọc binlog từ file lưu trữ theo spec/02 §3 và §9.
package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"strand/internal/model"
)

// IdentityMode defines how row identity is resolved for an archive table.
type IdentityMode string

const (
	// IdentityModePK: table has an explicit Primary Key. Key is the ordered PK columns.
	IdentityModePK IdentityMode = "pk"
	// IdentityModeUnique: table has no PK, but has a verified NOT NULL UNIQUE index used as identity.
	IdentityModeUnique IdentityMode = "unique"
	// IdentityModeKeyless: table has neither PK nor unique key, requiring full_row identity and FULL row images.
	IdentityModeKeyless IdentityMode = "keyless"
)

// TableIdentity contains the verified identity contract for a table in an archive binlog stream.
type TableIdentity struct {
	Schema       string       `json:"schema"`
	Table        string       `json:"table"`
	Columns      []string     `json:"columns"`
	ColumnTypes  []byte       `json:"column_types,omitempty"` // raw MySQL type bytes
	IdentityMode IdentityMode `json:"identity_mode"`          // "pk", "unique", "keyless"
	KeyColumns   []string     `json:"key_columns,omitempty"`  // ordered key columns (for pk or unique)
	Fingerprint  string       `json:"fingerprint,omitempty"`  // deterministic schema fingerprint
	Version      uint64       `json:"version,omitempty"`      // schema generation/version
}

// CanonicalName returns "schema.table" in lower case.
func (t *TableIdentity) CanonicalName() string {
	return strings.ToLower(fmt.Sprintf("%s.%s", t.Schema, t.Table))
}

// ComputeFingerprint calculates a deterministic SHA-256 fingerprint of the table identity contract.
func (t *TableIdentity) ComputeFingerprint() string {
	h := sha256.New()
	fmt.Fprintf(h, "table:%s.%s\n", strings.ToLower(t.Schema), strings.ToLower(t.Table))
	fmt.Fprintf(h, "columns:%s\n", strings.Join(t.Columns, ","))
	if len(t.ColumnTypes) > 0 {
		fmt.Fprintf(h, "types:%x\n", t.ColumnTypes)
	}
	fmt.Fprintf(h, "identity_mode:%s\n", t.IdentityMode)
	fmt.Fprintf(h, "key_columns:%s\n", strings.Join(t.KeyColumns, ","))
	if t.Version > 0 {
		fmt.Fprintf(h, "version:%d\n", t.Version)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Validate validates that the table identity contract is consistent and non-empty.
func (t *TableIdentity) Validate() error {
	if strings.TrimSpace(t.Schema) == "" || strings.TrimSpace(t.Table) == "" {
		return fmt.Errorf("table identity requires non-empty schema and table")
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("table %s.%s has no columns", t.Schema, t.Table)
	}
	colSet := make(map[string]bool, len(t.Columns))
	for _, c := range t.Columns {
		if strings.TrimSpace(c) == "" {
			return fmt.Errorf("table %s.%s has empty column name", t.Schema, t.Table)
		}
		if colSet[c] {
			return fmt.Errorf("table %s.%s has duplicate column name %q", t.Schema, t.Table, c)
		}
		colSet[c] = true
	}
	if len(t.ColumnTypes) > 0 && len(t.ColumnTypes) != len(t.Columns) {
		return fmt.Errorf("table %s.%s column_types length %d != columns length %d",
			t.Schema, t.Table, len(t.ColumnTypes), len(t.Columns))
	}
	switch t.IdentityMode {
	case IdentityModePK:
		if len(t.KeyColumns) == 0 {
			return fmt.Errorf("table %s.%s with identity_mode=pk must have non-empty key_columns", t.Schema, t.Table)
		}
		if err := validateKeyColumns(t.Schema, t.Table, t.KeyColumns, colSet); err != nil {
			return err
		}
	case IdentityModeUnique:
		if len(t.KeyColumns) == 0 {
			return fmt.Errorf("table %s.%s with identity_mode=unique must have non-empty key_columns", t.Schema, t.Table)
		}
		if err := validateKeyColumns(t.Schema, t.Table, t.KeyColumns, colSet); err != nil {
			return err
		}
	case IdentityModeKeyless:
		if len(t.KeyColumns) > 0 && !sameStringSlice(t.KeyColumns, t.Columns) {
			return fmt.Errorf("table %s.%s with identity_mode=keyless key_columns must be empty or full columns", t.Schema, t.Table)
		}
	default:
		return fmt.Errorf("table %s.%s has unsupported identity_mode %q (must be pk, unique, or keyless)",
			t.Schema, t.Table, t.IdentityMode)
	}
	if t.Fingerprint != "" {
		expected := t.ComputeFingerprint()
		if t.Fingerprint != expected {
			return fmt.Errorf("table %s.%s fingerprint mismatch (got %s, computed %s)",
				t.Schema, t.Table, t.Fingerprint, expected)
		}
	}
	return nil
}

func validateKeyColumns(schema, table string, keys []string, colSet map[string]bool) error {
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if !colSet[k] {
			return fmt.Errorf("table %s.%s key column %q not found in columns", schema, table, k)
		}
		if seen[k] {
			return fmt.Errorf("table %s.%s duplicate key column %q", schema, table, k)
		}
		seen[k] = true
	}
	return nil
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SchemaSidecar represents an immutable operator-supplied or trusted seed snapshot schema sidecar.
type SchemaSidecar struct {
	Version uint64                   `json:"version,omitempty"`
	Tables  map[string]TableIdentity `json:"tables"`
}

// NewSchemaSidecar creates a new SchemaSidecar from TableIdentity items.
func NewSchemaSidecar(tables ...TableIdentity) (*SchemaSidecar, error) {
	s := &SchemaSidecar{Tables: make(map[string]TableIdentity, len(tables))}
	for _, t := range tables {
		if err := t.Validate(); err != nil {
			return nil, err
		}
		if t.Fingerprint == "" {
			t.Fingerprint = t.ComputeFingerprint()
		}
		s.Tables[t.CanonicalName()] = t
	}
	return s, nil
}

// LoadSchemaSidecar loads a SchemaSidecar from a JSON file.
func LoadSchemaSidecar(path string) (*SchemaSidecar, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema sidecar %s: %w", path, err)
	}
	var s SchemaSidecar
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("unmarshal schema sidecar %s: %w", path, err)
	}
	normalized := make(map[string]TableIdentity, len(s.Tables))
	for k, t := range s.Tables {
		if t.Schema == "" || t.Table == "" {
			parts := strings.SplitN(k, ".", 2)
			if len(parts) == 2 {
				if t.Schema == "" {
					t.Schema = parts[0]
				}
				if t.Table == "" {
					t.Table = parts[1]
				}
			}
		}
		if err := t.Validate(); err != nil {
			return nil, fmt.Errorf("schema sidecar invalid table %s: %w", k, err)
		}
		if t.Fingerprint == "" {
			t.Fingerprint = t.ComputeFingerprint()
		}
		normalized[t.CanonicalName()] = t
	}
	s.Tables = normalized
	return &s, nil
}

// GetTable looks up a TableIdentity by schema and table name.
func (s *SchemaSidecar) GetTable(schema, table string) (*TableIdentity, bool) {
	if s == nil || s.Tables == nil {
		return nil, false
	}
	key := strings.ToLower(fmt.Sprintf("%s.%s", schema, table))
	t, ok := s.Tables[key]
	if !ok {
		return nil, false
	}
	return &t, true
}

// VerifyRowChangeIdentity verifies that a RowChange conforms to the table's verified identity contract.
// If downstream consumer expects a fallback unique key, emitting full-row keys fails closed.
func VerifyRowChangeIdentity(t TableIdentity, rc model.RowChange) error {
	switch t.IdentityMode {
	case IdentityModePK:
		if len(rc.Key) != len(t.KeyColumns) {
			return fmt.Errorf("table %s.%s row key length %d does not match PK columns length %d",
				t.Schema, t.Table, len(rc.Key), len(t.KeyColumns))
		}
	case IdentityModeUnique:
		if len(rc.Key) != len(t.KeyColumns) {
			return fmt.Errorf("table %s.%s row key length %d does not match fallback unique key columns length %d; refusing to emit mismatched key shape",
				t.Schema, t.Table, len(rc.Key), len(t.KeyColumns))
		}
	case IdentityModeKeyless:
		if len(rc.Key) != len(t.Columns) {
			return fmt.Errorf("table %s.%s is keyless requiring full_row key length %d, got %d",
				t.Schema, t.Table, len(t.Columns), len(rc.Key))
		}
	}
	return nil
}
