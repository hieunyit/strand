//go:build unix

package web

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// pipelineLockFallbackDir holds lock files for config directories the Web
// process may write files in but not create files in (for example a root-owned
// /etc/strand with group-writable pipeline files).
var pipelineLockFallbackDir = filepath.Join(os.TempDir(), "strand-web-locks")

// lockPipelineFiles coordinates DB materialization with file saves across Web
// processes sharing a pipeline's config directory.
func lockPipelineFiles(configPath string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(configPath), 0o750); err != nil && !errors.Is(err, fs.ErrPermission) {
		return nil, err
	}
	f, err := os.OpenFile(configPath+".strand.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if errors.Is(err, fs.ErrPermission) || errors.Is(err, syscall.EROFS) {
		f, err = openFallbackPipelineLock(configPath)
	}
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// openFallbackPipelineLock keys the lock by the absolute config path, so every
// Web process on this host serializes on the same file.
func openFallbackPipelineLock(configPath string) (*os.File, error) {
	abs, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(pipelineLockFallbackDir, 0o700); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(abs))
	return os.OpenFile(filepath.Join(pipelineLockFallbackDir, hex.EncodeToString(sum[:16])+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
}
