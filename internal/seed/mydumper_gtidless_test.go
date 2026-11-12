package seed

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeMeta(t *testing.T, body string) string {
	t.Helper()
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "metadata"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return d
}

// Nguồn không GTID: metadata dạng cũ (Log:/Pos:, GTID rỗng) và dạng INI đều cho manifest mang vị trí.
func TestMydumperGTIDLess(t *testing.T) {
	old := "Started dump at: 2026-01-01 00:00:00\nSHOW MASTER STATUS:\n\tLog: mysql-bin.000004\n\tPos: 3571\n\tGTID:\n\nFinished dump at: 2026-01-01 00:00:01\n"
	ini := "# Started dump at: 2026-01-01 00:00:00\n[source]\n# SOURCE_LOG_FILE = \"mysql-bin.000009\"\n# SOURCE_LOG_POS = 77\n"
	for name, c := range map[string]struct {
		body, file string
		pos        uint64
	}{"old": {old, "mysql-bin.000004", 3571}, "ini": {ini, "mysql-bin.000009", 77}} {
		dir := writeMeta(t, c.body)
		m, err := NewMydumperPlanner().Plan(context.Background(), SeedSource{BackupRef: dir, AcceptUnverifiedLockMode: true})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		sp := m.StartPosition
		if sp.GTIDSet != "" || sp.BinlogFile != c.file || sp.BinlogPos != c.pos {
			t.Fatalf("%s: %+v", name, sp)
		}
	}
}

func TestMydumperNoGTIDNoPositionRejected(t *testing.T) {
	dir := writeMeta(t, "Started dump at: x\nFinished dump at: y\n")
	if _, err := NewMydumperPlanner().Plan(context.Background(), SeedSource{BackupRef: dir, AcceptUnverifiedLockMode: true}); err == nil {
		t.Fatal("want rejection")
	}
}
