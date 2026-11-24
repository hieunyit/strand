//go:build linux

package web

import (
	"os"
	"path/filepath"
	"syscall"
)

// lockPipelineFiles coordinates DB materialization with file saves across Web
// processes sharing a pipeline's config directory.
func lockPipelineFiles(configPath string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(configPath), 0o750); err != nil {
		return nil, err
	}
	path := configPath + ".strand.lock"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
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
