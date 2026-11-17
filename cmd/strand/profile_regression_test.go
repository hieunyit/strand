package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"
)

type failingProfileWriter struct{ err error }

func (w failingProfileWriter) Write([]byte) (int, error) { return 0, w.err }
func TestProfileCPUWriteFailureSurvivesStop(t *testing.T) {
	want := errors.New("profile disk full")
	recorder := &profileWriter{writer: failingProfileWriter{want}}
	if err := pprof.StartCPUProfile(recorder); err != nil {
		t.Fatal(err)
	}
	p := &profiler{cpuOn: true, cpuWriter: recorder}
	if err := p.Stop(); !errors.Is(err, want) {
		t.Fatalf("CPU write failure not returned: %v", err)
	}
	if err := p.Stop(); !errors.Is(err, want) {
		t.Fatalf("repeat Stop lost CPU write failure: %v", err)
	}
}
func TestProfileShortWriteIsFailure(t *testing.T) {
	r := &profileWriter{writer: shortProfileWriter{}}
	if _, err := r.Write([]byte("profile")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	if !errors.Is(r.Err(), io.ErrShortWrite) {
		t.Fatal("short write was not retained")
	}
}

type shortProfileWriter struct{}

func (shortProfileWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestProfileRejectsNewRelativeAbsoluteAlias(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := startProfiling(profileConfig{cpuPath: path, blockPath: relative, allowOverwrite: true})
	if p != nil {
		defer p.Stop()
	}
	if err == nil {
		t.Fatal("same new file allowed via absolute/relative aliases")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("alias rejection touched output: %v", err)
	}
}
func TestProfileOverwriteProtectsExistingPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu.pprof")
	if err := os.WriteFile(path, []byte("previous"), 0644); err != nil {
		t.Fatal(err)
	}
	p, err := startProfiling(profileConfig{cpuPath: path, allowOverwrite: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0600 {
		t.Fatalf("overwrite retained public permissions: %o", st.Mode().Perm())
	}
}
func TestProfileOpenFailureDoesNotTruncateOtherOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.pprof")
	want := []byte("keep previous profile")
	if err := os.WriteFile(path, want, 0600); err != nil {
		t.Fatal(err)
	}
	p, err := startProfiling(profileConfig{cpuPath: path, blockPath: filepath.Join(t.TempDir(), "missing", "block.pprof"), allowOverwrite: true})
	if p != nil {
		defer p.Stop()
	}
	if err == nil {
		t.Fatal("missing directory accepted")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(want) {
		t.Fatalf("failed startup changed prior profile: %q %v", got, err)
	}
}
func TestProfileRefusesNonregularOutput(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("requires device fixture")
	}
	p, err := startProfiling(profileConfig{cpuPath: "/dev/full", allowOverwrite: true})
	if p != nil {
		defer p.Stop()
	}
	if err == nil {
		t.Fatal("nonregular profile output accepted")
	}
}
