package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
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

func TestValidateAutoSeedPreflightReportsFirstStartBlockers(t *testing.T) {
	child := model.TableID{Schema: "app", Table: "child"}
	parent := model.TableID{Schema: "app", Table: "parent"}
	newDB := func(targetHasRows bool) *sql.DB {
		return sql.OpenDB(wiringConnector{answer: func(q string) ([]string, []driver.Value, error) {
			switch {
			case strings.HasPrefix(q, "SHOW CREATE TABLE"):
				if strings.Contains(q, "`child`") {
					return []string{"table", "ddl"}, []driver.Value{"child", "CREATE TABLE `child` (\n  CONSTRAINT `fk_parent` FOREIGN KEY (`parent_id`) REFERENCES `parent` (`id`) ON DELETE CASCADE\n)"}, nil
				}
				return []string{"table", "ddl"}, []driver.Value{"parent", "CREATE TABLE `parent` (`id` int PRIMARY KEY)"}, nil
			case strings.Contains(q, "REFERENTIAL_CONSTRAINTS"):
				return []string{"fk"}, nil, nil
			case strings.Contains(q, "COLLATION_NAME"):
				return []string{"collation"}, []driver.Value{""}, nil
			case strings.Contains(q, "KEY_COLUMN_USAGE"):
				return []string{"a", "b", "c", "d"}, nil, nil
			case strings.Contains(q, "TABLE_TYPE"):
				return []string{"type"}, []driver.Value{"BASE TABLE"}, nil
			case strings.HasPrefix(q, "SELECT 1 FROM `app`."):
				if targetHasRows {
					return []string{"one"}, []driver.Value{int64(1)}, nil
				}
				return []string{"one"}, nil, nil
			default:
				return nil, nil, errors.New("unexpected query: " + q)
			}
		}})
	}
	source := newDB(false)
	target := newDB(false)
	defer source.Close()
	defer target.Close()
	if err := ValidateAutoSeedPreflight(context.Background(), source, target, true, []model.TableID{child}, nil, nil); err == nil || !strings.Contains(err.Error(), "app.parent") {
		t.Fatalf("missing FK scope error = %v, want app.parent", err)
	}
	_ = source.Close()
	source = newDB(false)
	defer source.Close()
	_ = target.Close()
	target = newDB(true)
	defer target.Close()
	if err := ValidateAutoSeedPreflight(context.Background(), source, target, true, []model.TableID{child, parent}, nil, nil); err == nil || !strings.Contains(err.Error(), "already contains data") {
		t.Fatalf("nonempty destination error = %v, want already contains data", err)
	}
}

func TestValidateAutoSeedPreflightChecksKeylessSeedTables(t *testing.T) {
	id := model.TableID{Schema: "app", Table: "keyless"}
	newDB := func(targetHasRows bool) *sql.DB {
		return sql.OpenDB(wiringConnector{answer: func(q string) ([]string, []driver.Value, error) {
			switch {
			case strings.HasPrefix(q, "SHOW CREATE TABLE"):
				return []string{"table", "ddl"}, []driver.Value{"keyless", "CREATE TABLE `keyless` (`v` text)"}, nil
			case strings.Contains(q, "KEY_COLUMN_USAGE"):
				return []string{"a", "b", "c", "d"}, nil, nil
			case strings.Contains(q, "TABLE_TYPE"):
				return []string{"type"}, []driver.Value{"BASE TABLE"}, nil
			case strings.HasPrefix(q, "SELECT 1 FROM `app`."):
				if targetHasRows {
					return []string{"one"}, []driver.Value{int64(1)}, nil
				}
				return []string{"one"}, nil, nil
			default:
				return nil, nil, errors.New("unexpected query: " + q)
			}
		}})
	}
	source, target := newDB(false), newDB(true)
	defer source.Close()
	defer target.Close()
	err := ValidateAutoSeedPreflight(context.Background(), source, target, true, []model.TableID{id}, []model.TableID{id}, nil)
	if err == nil || !strings.Contains(err.Error(), "already contains data") {
		t.Fatalf("keyless seed target error = %v, want already contains data", err)
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
