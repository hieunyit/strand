package pipeline

// Regression test SD-1 MySQL (cùng lỗ review N09 phía PG): --start-at seed phải đối chiếu
// source_identity / vị trí P của seed.json với source đang nối. Unit test thuần, không cần DB.

import (
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
)

func TestSD1MySQLSeedSourceIdentity(t *testing.T) {
	const (
		a = "3e11fa47-71ca-11e1-9e33-c80aa9429562" // primary gốc
		b = "4f22ab58-82db-22f2-af44-d91bb0530673" // replica / primary mới sau failover
		x = "9a9a9a9a-0000-1111-2222-333333333333" // cluster khác
	)
	ok := []struct{ name, id, p, uuid, exec string }{
		{"same server", a, a + ":1-100", a, a + ":1-200"},
		{"uppercase identity", strings.ToUpper(a), a + ":1-100", a, a + ":1-200"},
		// CDC đọc từ replica b, seed lấy từ primary a: UUID khác nhau hợp lệ, a có trong GTID history.
		{"source is a replica", a, a + ":1-100", b, a + ":1-200"},
		// Sau failover sang b: b có lịch sử của a.
		{"source after failover", a, a + ":1-100", b, a + ":1-200,\n" + b + ":1-5"},
		{"identity is the source uuid", b, a + ":1-100", b, a + ":1-200"},
	}
	for _, c := range ok {
		if err := checkMySQLSeedSourceIdentity(c.id, c.p, c.uuid, c.exec); err != nil {
			t.Fatalf("%s: rejected: %v", c.name, err)
		}
	}
	bad := []struct{ name, id, p, uuid, exec, want string }{
		{"no identity", "", a + ":1-100", a, a + ":1-200", "no source_identity"},
		{"seed from another cluster", x, x + ":1-100", a, a + ":1-200", "never executed"},
		{"seed ahead of source", a, a + ":1-300", a, a + ":1-200", "never executed"},
		{"identity not in source history", x, a + ":1-100", b, a + ":1-200", "neither the source server_uuid"},
		{"empty P", a, "", a, a + ":1-200", "not a valid non-empty GTID set"},
	}
	for _, c := range bad {
		err := checkMySQLSeedSourceIdentity(c.id, c.p, c.uuid, c.exec)
		var xe *exitcode.Error
		if err == nil || !errors.As(err, &xe) || xe.ExitCode != exitcode.Rejected || xe.Code != exitcode.ErrSeedInvariantFailed {
			t.Fatalf("%s: err = %v, want seed_invariant_failed (exit 3)", c.name, err)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: message %q, want %q", c.name, err.Error(), c.want)
		}
	}
}
