package model

import "fmt"

// ArchiveIdentity binds row-key values to the exact ordered source column names.
// It describes source tables, before pipeline rename or type conversion.
type ArchiveIdentity struct {
	Table      TableID  `json:"table"`
	Mode       string   `json:"mode"`
	Columns    []string `json:"columns"`
	KeyColumns []string `json:"key_columns"`
}

func (i ArchiveIdentity) Validate() error {
	if i.Table.Schema == "" || i.Table.Table == "" || len(i.Columns) == 0 || len(i.KeyColumns) == 0 {
		return fmt.Errorf("incomplete archive identity for %s", i.Table)
	}
	columns := make(map[string]bool, len(i.Columns))
	for _, c := range i.Columns {
		if c == "" || columns[c] {
			return fmt.Errorf("invalid archive column %q for %s", c, i.Table)
		}
		columns[c] = true
	}
	keys := make(map[string]bool, len(i.KeyColumns))
	for _, k := range i.KeyColumns {
		if !columns[k] || keys[k] {
			return fmt.Errorf("invalid archive key %q for %s", k, i.Table)
		}
		keys[k] = true
	}
	switch i.Mode {
	case "pk", "unique":
	case "keyless":
		if len(i.KeyColumns) != len(i.Columns) {
			return fmt.Errorf("archive full-row key is incomplete for %s", i.Table)
		}
		for n, c := range i.Columns {
			if i.KeyColumns[n] != c {
				return fmt.Errorf("archive full-row key order differs for %s", i.Table)
			}
		}
	default:
		return fmt.Errorf("unknown archive identity mode %q for %s", i.Mode, i.Table)
	}
	return nil
}

// CheckRowKey also guards NOT NULL row identities. Full-row identity deliberately
// permits NULL, whereas NULL cannot identify one row through a PK/unique predicate.
func (i ArchiveIdentity) CheckRowKey(row *RowChange) error {
	if row.Table != i.Table {
		return fmt.Errorf("prefetch row table does not match identity for %s", i.Table)
	}
	if len(row.Key) != len(i.KeyColumns) || (len(row.OldKey) > 0 && len(row.OldKey) != len(i.KeyColumns)) {
		return fmt.Errorf("prefetch row for %s has wrong key shape", row.Table)
	}
	if i.Mode != "keyless" {
		for _, key := range [][]Value{row.Key, row.OldKey} {
			for _, v := range key {
				null := v == nil
				if b, ok := v.([]byte); ok && b == nil {
					null = true
				}
				if null {
					return fmt.Errorf("prefetch PK/unique key for %s contains NULL", row.Table)
				}
			}
		}
	}
	return nil
}
