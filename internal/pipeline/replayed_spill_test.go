package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"strand/internal/model"
)

// An already-applied PG transaction can be decoded again after restart.
// Skip the transaction, releasing only the assembler-owned copy.
func TestReplaySkipReleasesOwnedSpill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sim := newCSim(t, 41)
	sim.build(ctx, cancel)
	for _, tc := range []struct {
		name  string
		owned bool
	}{{"runtime", true}, {"prefetch", false}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "payload.spill")
			if err := os.WriteFile(path, []byte("already applied"), 0600); err != nil {
				t.Fatal(err)
			}
			txn := &model.Txn{RowCount: 12000, SourcePos: sim.p.applied0.Lwd(), SpillPath: path, SpillOwned: tc.owned}
			if err := sim.p.route(ctx, txn); err != nil {
				t.Fatal(err)
			}
			_, err := os.Stat(path)
			if tc.owned && !errors.Is(err, os.ErrNotExist) || !tc.owned && err != nil {
				t.Fatalf("after skipping replay, owned=%t: stat %s: %v", tc.owned, path, err)
			}
		})
	}
}
