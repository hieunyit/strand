package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// review G1: PRIMARY KEY có cột prefix bị từ chối (exit 3); prefix chỉ ở UNIQUE thì vẫn nhận (N01).
func TestG1_PrimaryKeyPrefixRejected(t *testing.T) {
	id := model.TableID{Schema: "app", Table: "t"}
	err := checkPrimaryKeyPrefix(id, map[string]bool{"PRIMARY": true})
	var se *exitcode.Error
	if !errors.As(err, &se) || se.ExitCode != exitcode.Rejected ||
		!strings.Contains(err.Error(), "primary key with prefix part is not supported") || !strings.Contains(err.Error(), "app.t") {
		t.Fatalf("want exit-3 prefix-PK rejection naming app.t, got %v", err)
	}
	if err := checkPrimaryKeyPrefix(id, map[string]bool{"uk_code": true}); err != nil {
		t.Fatalf("prefix only on a UNIQUE index must be accepted: %v", err)
	}
	if err := checkPrimaryKeyPrefix(id, nil); err != nil {
		t.Fatalf("no prefix index must be accepted: %v", err)
	}
}

// review G1: metadata thật của MySQL — bảng PRIMARY KEY (code(10)) bị loadSourceTables từ chối.
// Cần container MySQL.
func TestG1_MySQLPrimaryKeyPrefixRejectedAtLoad(t *testing.T) {
	db, _ := startTestDB(t, "mysql")
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE pkpfx (code VARCHAR(40) NOT NULL, v INT, PRIMARY KEY (code(10)))`); err != nil {
		t.Fatal(err)
	}
	f, err := newTableFilter([]string{"app.pkpfx"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = loadSourceTables(ctx, db, f)
	var se *exitcode.Error
	if !errors.As(err, &se) || se.ExitCode != exitcode.Rejected || !strings.Contains(err.Error(), "prefix part") {
		t.Fatalf("want exit-3 prefix-PK rejection, got %v", err)
	}
}
