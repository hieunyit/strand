package pipeline

import (
	"fmt"
	"slices"

	"strand/internal/model"
	"strand/internal/rowkey"
)

// The bootstrap scan checks contracts before treating spool positions as covered.
// Routing checks again against current metadata before admitting any row writes.
func (p *Pipeline) archiveIdentityChecker(t *model.Txn, requireProvenance bool) (func(*model.RowChange) error, error) {
	noop := func(*model.RowChange) error { return nil }
	switch t.PrefetchOrigin {
	case "":
		if requireProvenance {
			return nil, fmt.Errorf("legacy prefetch transaction has no verified provenance; rebuild the prefetch spool from a trusted source/archive before retrying")
		}
		if len(t.ArchiveIdentities) > 0 {
			return nil, fmt.Errorf("archive identity without prefetch provenance")
		}
		return noop, nil
	case "source", "archive":
	default:
		return nil, fmt.Errorf("unknown prefetch provenance %q", t.PrefetchOrigin)
	}
	identities := make(map[model.TableID]model.ArchiveIdentity, len(t.ArchiveIdentities))
	for _, identity := range t.ArchiveIdentities {
		if err := identity.Validate(); err != nil {
			return nil, err
		}
		if _, exists := identities[identity.Table]; exists {
			return nil, fmt.Errorf("duplicate archive identity for %s", identity.Table)
		}
		identities[identity.Table] = identity
		m := p.table(identity.Table)
		if m == nil {
			if p.filter != nil && !p.filter.Included(identity.Table) {
				continue
			}
			return nil, fmt.Errorf("archive table %s has no consuming pipeline metadata", identity.Table)
		}
		mode := "unique"
		if m.Keyless {
			mode = "keyless"
		} else if m.KeyName == rowkey.PrimaryName {
			mode = "pk"
		}
		if identity.Mode != mode || !slices.Equal(identity.KeyColumns, m.PK) || !slices.Equal(identity.Columns, m.Columns) {
			return nil, fmt.Errorf("archive identity for %s does not match consuming pipeline (mode %s vs %s, ordered key %v vs %v, columns %v vs %v); rebuild prefetch with matching identity metadata", identity.Table, identity.Mode, mode, identity.KeyColumns, m.PK, identity.Columns, m.Columns)
		}
	}
	if t.TotalRows() > 0 && len(identities) == 0 {
		return nil, fmt.Errorf("archive rows have no durable identity contracts")
	}
	check := func(row *model.RowChange) error {
		identity, ok := identities[row.Table]
		if !ok {
			return fmt.Errorf("archive row for %s has no durable identity contract", row.Table)
		}
		return identity.CheckRowKey(row)
	}
	for i := range t.Rows {
		if err := check(&t.Rows[i]); err != nil {
			return nil, err
		}
	}
	return check, nil
}
