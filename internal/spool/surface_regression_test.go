package spool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestInventoryStaleReportPreservesLiveSize(t *testing.T) {
	temp := t.TempDir()
	trusted := filepath.Join(temp, "trusted")
	if err := os.Mkdir(trusted, 0700); err != nil {
		t.Fatal(err)
	}
	path := writeNamedFile(t, trusted, "orphan", 100)
	dbdir := filepath.Join(temp, "db")
	s, err := New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	report, err := s.ScanInventory([]string{trusted})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 200), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), spillTxn(path, "1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ImportInventory(report); err != nil {
		t.Fatal(err)
	}
	if got := s.sidecars[path]; got.size != 200 || got.refs != 1 || s.sideBytes != 200 {
		t.Fatalf("stale report changed live accounting: %+v total=%d", got, s.sideBytes)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(dbdir)
	if err != nil {
		t.Fatalf("stale report corrupted persistent accounting: %v", err)
	}
	defer s.Close()
	if got := s.sidecars[path]; got.size != 200 || got.refs != 1 {
		t.Fatalf("reopened accounting: %+v", got)
	}
}

func TestInventoryImportRevalidatesFilesystem(t *testing.T) {
	for _, change := range []string{"grown orphan", "grown indexed orphan", "changed live file", "removed file", "escaped symlink"} {
		t.Run(change, func(t *testing.T) {
			temp := t.TempDir()
			trusted := filepath.Join(temp, "trusted")
			if err := os.Mkdir(trusted, 0700); err != nil {
				t.Fatal(err)
			}
			path := writeNamedFile(t, trusted, "orphan", 100)
			s, err := New(filepath.Join(temp, "db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if change == "grown indexed orphan" {
				if _, err := s.ImportTrustedRoots([]string{trusted}); err != nil {
					t.Fatal(err)
				}
			}
			report, err := s.ScanInventory([]string{trusted})
			if err != nil {
				t.Fatal(err)
			}
			if change == "changed live file" {
				if err := s.Append(context.Background(), spillTxn(path, "1")); err != nil {
					t.Fatal(err)
				}
			}
			switch change {
			case "removed file", "escaped symlink":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if change == "escaped symlink" {
					outside := writeNamedFile(t, temp, "outside", 300)
					if err := os.Symlink(outside, path); err != nil {
						t.Fatal(err)
					}
				}
			default:
				if err := os.WriteFile(path, make([]byte, 200), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err = s.ImportInventory(report)
			if change == "grown orphan" || change == "grown indexed orphan" {
				if err != nil {
					t.Fatal(err)
				}
				if got := s.sidecars[path]; got.size != 200 || got.refs != 0 || s.sideBytes != 200 {
					t.Fatalf("fresh orphan size not charged: %+v total=%d", got, s.sideBytes)
				}
			} else {
				if err == nil {
					t.Fatal("import accepted invalid stale inventory")
				}
				if change == "changed live file" {
					if got := s.sidecars[path]; got.size != 100 || got.refs != 1 || s.sideBytes != 100 {
						t.Fatalf("failed import changed live entry: %+v total=%d", got, s.sideBytes)
					}
				} else if s.sideBytes != 0 || len(s.sidecars) != 0 {
					t.Fatalf("failed import published charges: %d, %+v", s.sideBytes, s.sidecars)
				}
			}
		})
	}
}

func TestSidecarPostVersionCrashOrphanReconciled(t *testing.T) {
	temp := t.TempDir()
	dbdir := filepath.Join(temp, "db")
	s, err := New(dbdir)
	if err != nil {
		t.Fatal(err)
	}
	spilldir := filepath.Join(dbdir, "spill")
	if err := os.MkdirAll(filepath.Join(spilldir, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	live := writeNamedFile(t, spilldir, "live", 200)
	for _, seq := range []string{"1", "2"} {
		if err := s.Append(context.Background(), spillTxn(live, seq)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The accounting schema is already migrated. Model assembler spills that
	// survive a crash before Append creates their durable references.
	orphan := writeNamedFile(t, spilldir, "crashed", 4096)
	nested := writeNamedFile(t, filepath.Join(spilldir, "nested"), "partial", 512)
	outside := writeNamedFile(t, temp, "borrowed-outside", 1024)
	if err := os.Symlink(outside, filepath.Join(spilldir, "external-link")); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		s, err = New(dbdir, WithTotalHardLimit(1024))
		if err != nil {
			t.Fatal(err)
		}
		if s.sideBytes != 200+4096+512 || len(s.sidecars) != 3 {
			t.Fatalf("restart %d sidecar charges=%d entries=%+v", attempt, s.sideBytes, s.sidecars)
		}
		if got := s.sidecars[live]; got.size != 200 || got.refs != 2 {
			t.Fatalf("startup scan changed live references: %+v", got)
		}
		for _, path := range []string{orphan, nested} {
			if s.sidecars[path] == nil || s.sidecars[path].refs != 0 {
				t.Fatalf("crash orphan not indexed: %s", path)
			}
			value, closer, err := s.db.Get(sidecarKey(path))
			if err != nil || len(value) != 8 {
				t.Fatalf("crash orphan charge not durable: %s, %v", path, err)
			}
			closer.Close()
		}
		if err := s.Append(context.Background(), spillTxn("", "3")); err == nil {
			t.Fatal("append accepted with crash orphans over quota")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{orphan, nested, live, outside} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("startup removed borrowed/retained file %s: %v", path, err)
		}
	}
}
