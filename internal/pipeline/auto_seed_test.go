package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"strand/internal/model"
)

func TestAutoSeedJournalRecovery(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "keyless"}
	for _, tc := range []struct {
		name      string
		committed bool
	}{
		{name: "rollback"},
		{name: "committed", committed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			state, err := openLocalState(dir, "test")
			if err != nil {
				t.Fatal(err)
			}
			state.targetOf = func(table string) string { return table }
			if err := state.prepareAutoSeed([]model.TableID{table}); err != nil {
				t.Fatal(err)
			}
			reopened, err := openLocalState(dir, "test")
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.resolveAutoSeed(tc.committed); err != nil {
				t.Fatal(err)
			}
			if reopened.IsDone(table.String()) != tc.committed || reopened.VerifyPending() != tc.committed {
				t.Fatalf("seed state disagrees with the committed checkpoint: want committed=%v", tc.committed)
			}
			if len(reopened.data.AutoSeedPending) != 0 {
				t.Fatal("seed journal was not resolved")
			}
		})
	}
}

func TestAutoSeedBinlogGuardThrottlesChecks(t *testing.T) {
	calls := 0
	guard := &autoSeedBinlogGuard{check: func(context.Context) error {
		calls++
		return nil
	}}
	if err := guard.checkIfDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := guard.checkIfDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("checks within the interval were not throttled: got %d, want 1", calls)
	}
	guard.checkedAt = time.Now().Add(-autoSeedBinlogCheckInterval)
	if err := guard.checkIfDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expired interval was not checked: got %d checks, want 2", calls)
	}
}

func TestAutoSeedBinlogGuardPropagatesRetentionFailure(t *testing.T) {
	want := errors.New("binlog anchor was purged")
	guard := &autoSeedBinlogGuard{check: func(context.Context) error { return want }}
	if err := guard.checkIfDue(context.Background()); !errors.Is(err, want) {
		t.Fatalf("got %v, want retention error %v", err, want)
	}
}
