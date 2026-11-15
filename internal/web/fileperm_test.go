package web

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func writeModeFile(t *testing.T, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "web.yaml")
	if err := os.WriteFile(p, []byte("listen: 127.0.0.1:9000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSetConfigKeyPreservesAndTightensMode(t *testing.T) {
	cases := []struct{ in, want os.FileMode }{
		{0o640, 0o640}, {0o644, 0o640}, {0o666, 0o640}, {0o600, 0o600}, {0o660, 0o640},
	}
	for _, c := range cases {
		var note bytes.Buffer
		old := notePrinter
		notePrinter = &note
		p := writeModeFile(t, c.in)
		err := SetConfigKey(p, "password_hash", "x")
		notePrinter = old
		if err != nil {
			t.Fatal(err)
		}
		st, _ := os.Stat(p)
		if st.Mode().Perm() != c.want {
			t.Errorf("%04o -> %04o, want %04o", c.in, st.Mode().Perm(), c.want)
		}
		if (c.in != c.want) != strings.Contains(note.String(), "tightened") {
			t.Errorf("%04o: note=%q", c.in, note.String())
		}
	}
	np := filepath.Join(t.TempDir(), "new.yaml")
	if err := SetConfigKey(np, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(np); st.Mode().Perm() != 0o600 {
		t.Errorf("new file mode %v", st.Mode().Perm())
	}
}

func TestSetConfigKeyPreservesGroup(t *testing.T) {
	other := -1
	groups, _ := os.Getgroups()
	for _, g := range groups {
		if g != os.Getgid() {
			other = g
			break
		}
	}
	if other < 0 {
		t.Log("no supplementary group; seam test covers ownership")
		return
	}
	p := writeModeFile(t, 0o640)
	if err := os.Chown(p, -1, other); err != nil {
		t.Fatal(err)
	}
	if err := SetConfigKey(p, "password_hash", "x"); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(p)
	if int(st.Sys().(*syscall.Stat_t).Gid) != other {
		t.Errorf("group not preserved")
	}
}

func TestChownSeam(t *testing.T) {
	p := writeModeFile(t, 0o640)
	fi, _ := os.Stat(p)
	st := fi.Sys().(*syscall.Stat_t)
	oldChown, oldIDs := chownFunc, procIDs
	defer func() { chownFunc, procIDs = oldChown, oldIDs }()

	// Same owner as the process: chown must not even be attempted.
	called := false
	chownFunc = func(string, int, int) error { called = true; return nil }
	if err := SetConfigKey(p, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("chown attempted although owner already matches")
	}

	// Process "is" a different user (e.g. file root:strand, we are root-less):
	// the file's uid/gid must be handed to chown.
	procIDs = func() (int, int) { return int(st.Uid) + 1234, int(st.Gid) + 1234 }
	var gu, gg int
	chownFunc = func(_ string, u, g int) error { gu, gg = u, g; return nil }
	if err := SetConfigKey(p, "k2", "v"); err != nil {
		t.Fatal(err)
	}
	if gu != int(st.Uid) || gg != int(st.Gid) {
		t.Errorf("chown to %d:%d, want %d:%d", gu, gg, st.Uid, st.Gid)
	}

	// chown failure is surfaced and leaves the file untouched.
	boom := errors.New("operation not permitted")
	chownFunc = func(string, int, int) error { return boom }
	err := SetConfigKey(p, "password_hash", "x")
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("want chown error, got %v", err)
	}
	b, _ := os.ReadFile(p)
	if strings.Contains(string(b), "password_hash") {
		t.Error("file modified despite chown failure")
	}
	// Best effort (config tab) ignores it.
	if err := writeFilePreserving(p, []byte("a: b\n"), true, false); err != nil {
		t.Errorf("non-strict should ignore chown error: %v", err)
	}
}

func TestBackupNotMorePermissiveThanOriginal(t *testing.T) {
	p := writeModeFile(t, 0o640)
	bak := p + ".bak.1"
	if err := writeBackup(bak, []byte("x"), p, 0o640&secretPermMask); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(bak); st.Mode().Perm() != 0o640 {
		t.Errorf("bak mode %v", st.Mode().Perm())
	}
	if err := writeBackup(bak+"2", []byte("x"), p, 0o600); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(bak + "2"); st.Mode().Perm() != 0o600 {
		t.Errorf("bak mode %v", st.Mode().Perm())
	}
}
