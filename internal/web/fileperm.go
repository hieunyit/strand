package web

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// chownFunc is a seam for tests (os.Chown cannot be exercised as non-root).
var chownFunc = os.Chown

// procIDs reports the process uid/gid (seam for tests).
var procIDs = func() (int, int) { return os.Getuid(), os.Getgid() }

// notePrinter receives the one-line notice when permission bits are tightened.
var notePrinter io.Writer = os.Stderr

// secretPermMask: owner keeps rwx, group keeps r-x (no write), other loses all.
const secretPermMask os.FileMode = 0o750

// writeFilePreserving atomically replaces path with data (temp file in the
// same dir + rename).
//
// If path already exists its permission bits and owner/group are carried over
// to the new file, so that e.g. a root:strand 0640 web.yaml stays readable by
// the strand service after `sudo strand web passwd`. When secret is true the
// bits are additionally masked with 0750: "other" access and group write are
// never granted to a secret-bearing file (a note goes to notePrinter when bits
// are tightened). A new file is created 0600.
//
// strictOwner selects chown failure handling. When true (CLI rewrites of
// web.yaml) a failed chown is returned as an error: silently handing the file
// to a different owner is precisely the bug being fixed, and the operator
// should know. When false (web config-tab saves, usually running as the
// unprivileged service user) it is best effort. In both modes chown is skipped
// when uid/gid already match the current process, so the common case never
// needs privileges.
func writeFilePreserving(path string, data []byte, secret, strictOwner bool) error {
	perm := os.FileMode(0o600)
	var uid, gid int
	haveOwner := false
	if fi, err := os.Stat(path); err == nil {
		perm = fi.Mode().Perm()
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			uid, gid, haveOwner = int(st.Uid), int(st.Gid), true
		}
		if secret {
			if tight := perm & secretPermMask; tight != perm {
				fmt.Fprintf(notePrinter, "note: %s mode tightened %04o -> %04o (secrets must not be readable by others)\n", path, perm, tight)
				perm = tight
			}
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	curUID, curGID := procIDs()
	if haveOwner && (uid != curUID || gid != curGID) {
		if err := chownFunc(tmp.Name(), uid, gid); err != nil && strictOwner {
			return fmt.Errorf("preserve owner %d:%d of %s: %w (re-run as root, or chown the file yourself)", uid, gid, path, err)
		}
	}
	// chmod after chown: chown may clear setuid bits, and umask must not apply.
	if err := os.Chmod(tmp.Name(), perm); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// copyOwner makes tmp carry the owner/group of orig, best effort (the web
// service normally runs unprivileged; when it already owns the file this is a
// no-op, and a group it belongs to is preserved).
func copyOwner(tmp, orig string) {
	fi, err := os.Stat(orig)
	if err != nil {
		return
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if cu, cg := procIDs(); !ok || (int(st.Uid) == cu && int(st.Gid) == cg) {
		return
	}
	if chownFunc(tmp, int(st.Uid), int(st.Gid)) != nil {
		// Keep the group at least when we are allowed to.
		_ = chownFunc(tmp, -1, int(st.Gid))
	}
}

// writeBackup writes a .bak copy never more permissive than the original:
// created 0600, then set to perm (and the original's owner when possible).
func writeBackup(bak string, data []byte, orig string, perm os.FileMode) error {
	if err := os.WriteFile(bak, data, 0o600); err != nil {
		return err
	}
	copyOwner(bak, orig)
	return os.Chmod(bak, perm)
}
