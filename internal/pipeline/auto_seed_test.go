package pipeline

import (
	"testing"

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
