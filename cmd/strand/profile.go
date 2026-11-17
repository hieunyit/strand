package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
)

type profileConfig struct {
	cpuPath        string
	blockPath      string
	blockRate      int
	allowOverwrite bool
}

// CPU profiling does not return asynchronous writer errors through StopCPUProfile.
// Record them at the writer boundary and read them after the runtime has joined it.
type profileWriter struct {
	writer io.Writer
	mu     sync.Mutex
	err    error
}

func (w *profileWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.mu.Unlock()
	}
	return n, err
}
func (w *profileWriter) Err() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

type profiler struct {
	once      sync.Once
	cpuFile   *os.File
	blockFile *os.File
	cpuWriter *profileWriter
	cpuOn     bool
	blockOn   bool
	stopErr   error
}

func canonicalProfilePath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(absolute); err == nil {
		return resolved, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	// Resolve parent aliases even when the output file has not been created yet.
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}
func checkProfileConflict(cpuPath, blockPath string) error {
	if cpuPath == "" || blockPath == "" {
		return nil
	}
	cpu, err := canonicalProfilePath(cpuPath)
	if err != nil {
		return fmt.Errorf("cpu profile path: %w", err)
	}
	block, err := canonicalProfilePath(blockPath)
	if err != nil {
		return fmt.Errorf("block profile path: %w", err)
	}
	if cpu == block {
		return fmt.Errorf("cpu profile and block profile cannot target the same path: %s", cpu)
	}
	a, aerr := os.Stat(cpu)
	b, berr := os.Stat(block)
	if aerr == nil && berr == nil && os.SameFile(a, b) {
		return fmt.Errorf("cpu profile and block profile point to the same inode: %s", cpu)
	}
	return nil
}

// Open without truncation: validate both file handles before modifying either.
func openProfileFile(path string, overwrite bool) (*os.File, error) {
	flags := os.O_WRONLY | os.O_CREATE
	if !overwrite {
		flags |= os.O_EXCL
	}
	f, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !st.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("profile target %s must be a regular file", path)
	}
	return f, nil
}
func startProfiling(cfg profileConfig) (*profiler, error) {
	if cfg.cpuPath == "" && cfg.blockPath == "" {
		return nil, nil
	}
	if cfg.blockRate < 0 {
		return nil, fmt.Errorf("block profile rate must be non-negative")
	}
	if err := checkProfileConflict(cfg.cpuPath, cfg.blockPath); err != nil {
		return nil, err
	}
	p := &profiler{}
	fail := func(err error) (*profiler, error) { return nil, errors.Join(err, p.Stop()) }
	for _, entry := range []struct {
		path string
		dst  **os.File
		kind string
	}{{cfg.cpuPath, &p.cpuFile, "cpu"}, {cfg.blockPath, &p.blockFile, "block"}} {
		if entry.path == "" {
			continue
		}
		f, err := openProfileFile(entry.path, cfg.allowOverwrite)
		if err != nil {
			return fail(fmt.Errorf("open %s profile %q (existing files require --profile-overwrite): %w", entry.kind, entry.path, err))
		}
		*entry.dst = f
	}
	if p.cpuFile != nil && p.blockFile != nil {
		a, err := p.cpuFile.Stat()
		if err != nil {
			return fail(err)
		}
		b, err := p.blockFile.Stat()
		if err != nil {
			return fail(err)
		}
		if os.SameFile(a, b) {
			return fail(fmt.Errorf("cpu profile and block profile opened the same inode"))
		}
	}
	for _, f := range []*os.File{p.cpuFile, p.blockFile} {
		if f == nil {
			continue
		}
		if err := f.Chmod(0600); err != nil {
			return fail(fmt.Errorf("protect profile file: %w", err))
		}
		if err := f.Truncate(0); err != nil {
			return fail(fmt.Errorf("truncate profile file: %w", err))
		}
	}
	if p.cpuFile != nil {
		p.cpuWriter = &profileWriter{writer: p.cpuFile}
		if err := pprof.StartCPUProfile(p.cpuWriter); err != nil {
			return fail(fmt.Errorf("start cpu profile: %w", err))
		}
		p.cpuOn = true
	}
	if p.blockFile != nil {
		rate := cfg.blockRate
		if rate == 0 {
			rate = 1
		}
		runtime.SetBlockProfileRate(rate)
		p.blockOn = true
	}
	return p, nil
}
func (p *profiler) Stop() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		var errs []error
		if p.cpuOn {
			pprof.StopCPUProfile()
			p.cpuOn = false
		}
		if err := p.cpuWriter.Err(); err != nil {
			errs = append(errs, fmt.Errorf("write cpu profile: %w", err))
		}
		if p.cpuFile != nil {
			if err := p.cpuFile.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close cpu profile: %w", err))
			}
			p.cpuFile = nil
		}
		if p.blockOn {
			runtime.SetBlockProfileRate(0)
			p.blockOn = false
			if bp := pprof.Lookup("block"); bp != nil && p.blockFile != nil {
				if err := bp.WriteTo(p.blockFile, 0); err != nil {
					errs = append(errs, fmt.Errorf("write block profile: %w", err))
				}
			}
		}
		if p.blockFile != nil {
			if err := p.blockFile.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close block profile: %w", err))
			}
			p.blockFile = nil
		}
		p.stopErr = errors.Join(errs...)
	})
	return p.stopErr
}
