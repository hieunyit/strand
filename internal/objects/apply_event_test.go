package objects

import (
	"testing"

	"strand/internal/testutil"
)

func TestPostApplyEnablesOnlyMatchingEvent(t *testing.T) {
	db := testutil.LegacyMySQL(t, "mysql:8.4")
	ctx := t.Context()
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE DATABASE event_review")
	const definition = "CREATE EVENT event_review.sync_event ON SCHEDULE EVERY 1 DAY STARTS '2035-01-01 00:00:00' ON COMPLETION PRESERVE DISABLE DO SET @value = "
	event := Object{Kind: KindEvent, TargetSchema: "event_review", TargetName: "sync_event",
		Stmt: definition + "2", EnableStmt: "ALTER EVENT event_review.sync_event ENABLE"}
	exec(definition + "1")
	if res := enableEvent(ctx, db, event, false); res.Action != ActFailed {
		t.Fatalf("mismatched event = %+v", res)
	}
	var status string
	if err := db.QueryRowContext(ctx, "SELECT STATUS FROM information_schema.EVENTS WHERE EVENT_SCHEMA='event_review' AND EVENT_NAME='sync_event'").Scan(&status); err != nil || status != "DISABLED" {
		t.Fatalf("mismatched event was altered: status=%q error=%v", status, err)
	}
	exec("DROP EVENT event_review.sync_event")
	exec(event.Stmt)
	if res := enableEvent(ctx, db, event, false); res.Action != ActEnabled {
		t.Fatalf("matching event = %+v", res)
	}
}
