package spool

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/pebble"
)

// Sidecars are retained outside Pebble even after their last txn is truncated.
// Keep charging them until their owner removes them; the spool never unlinks them.
type sidecar struct {
	size int64
	refs int
}

var sidecarVersionKey = []byte("meta/sidecar_version")

func sidecarKey(path string) []byte {
	return []byte("spill/path/" + base64.RawURLEncoding.EncodeToString([]byte(path)))
}
func sidecarRefKey(seq uint64) []byte { return []byte(fmt.Sprintf("spill/ref/%016x", seq)) }
func sidecarValue(size int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(size))
	return b
}
func normalizeSidecar(path string) (string, error) { return filepath.Abs(filepath.Clean(path)) }
func sidecarStat(path string) (int64, error) {
	f, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("spool sidecar %s: %w", path, err)
	}
	if !f.Mode().IsRegular() {
		return 0, fmt.Errorf("spool sidecar %s is not a regular file", path)
	}
	return f.Size(), nil
}

// Initialize by reconstructing references from pre-accounting databases. A bucket
// object must be readable or migration fails rather than silently losing its charge.
func (s *PebbleSpool) initSidecars() error {
	s.sidecars = make(map[string]*sidecar)
	s.sidecarRefs = make(map[uint64]string)
	_, closer, err := s.db.Get(sidecarVersionKey)
	if err == nil {
		closer.Close()
	} else if errors.Is(err, pebble.ErrNotFound) {
		if err := s.migrateSidecars(); err != nil {
			return err
		}
	} else {
		return err
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("spill/path/"), UpperBound: []byte("spill/path0")})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		raw, e := base64.RawURLEncoding.DecodeString(string(it.Key()[len("spill/path/"):]))
		if e != nil || len(it.Value()) != 8 {
			it.Close()
			return fmt.Errorf("corrupt spool sidecar metadata")
		}
		size := int64(binary.BigEndian.Uint64(it.Value()))
		if size < 0 {
			it.Close()
			return fmt.Errorf("invalid spool sidecar size")
		}
		s.sidecars[string(raw)] = &sidecar{size: size}
		s.sideBytes += size
	}
	if err := it.Error(); err != nil {
		it.Close()
		return err
	}
	it.Close()
	it, err = s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("spill/ref/"), UpperBound: []byte("spill/ref0")})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		seq, e := strconv.ParseUint(string(it.Key()[len("spill/ref/"):]), 16, 64)
		path := string(it.Value())
		v := s.sidecars[path]
		if e != nil || v == nil {
			it.Close()
			return fmt.Errorf("corrupt spool sidecar reference")
		}
		v.refs++
		s.sidecarRefs[seq] = path
	}
	if err := it.Error(); err != nil {
		it.Close()
		return err
	}
	it.Close()
	if err := s.refreshSidecars(); err != nil {
		return err
	}
	return s.reconcileStandardSpill()
}
func (s *PebbleSpool) migrateSidecars() error {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("txn/"), UpperBound: []byte("txn/\xff")})
	if err != nil {
		return err
	}
	defer it.Close()
	b := s.db.NewBatch()
	defer b.Close()
	seen := map[string]bool{}
	var lastSegment string
	var lastEntries [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		seq, e := strconv.ParseUint(string(it.Key()[len("txn/"):]), 16, 64)
		if e != nil {
			return e
		}
		raw := bytes.Clone(it.Value())
		if bytes.HasPrefix(raw, bucketSegRefPrefix) {
			key, idx, e := parseBucketSegRef(raw)
			if e != nil {
				return e
			}
			if s.bucket == nil {
				return fmt.Errorf("spool sidecar migration needs bucket %s", key)
			}
			if key != lastSegment {
				obj, err := s.bucket.Get(context.Background(), key)
				if err != nil {
					return fmt.Errorf("spool sidecar migration: %w", err)
				}
				entries, err := decodeSegment(obj)
				if err != nil {
					return err
				}
				lastSegment, lastEntries = key, entries
			}
			if idx >= len(lastEntries) {
				return fmt.Errorf("spool sidecar migration: invalid segment index")
			}
			raw = lastEntries[idx]
		} else if bytes.HasPrefix(raw, bucketRefPrefix) {
			key := string(raw[len(bucketRefPrefix):])
			if s.bucket == nil {
				return fmt.Errorf("spool sidecar migration needs bucket %s", key)
			}
			raw, e = s.bucket.Get(context.Background(), key)
			if e != nil {
				return fmt.Errorf("spool sidecar migration: %w", e)
			}
		}
		t, e := decodeTxn(raw)
		if e != nil {
			return fmt.Errorf("spool sidecar migration: %w", e)
		}
		if t.SpillPath == "" {
			continue
		}
		path, e := normalizeSidecar(t.SpillPath)
		if e != nil {
			return e
		}
		if !seen[path] {
			size, e := sidecarStat(path)
			if e != nil {
				return e
			}
			if e = b.Set(sidecarKey(path), sidecarValue(size), nil); e != nil {
				return e
			}
			seen[path] = true
		}
		if e = b.Set(sidecarRefKey(seq), []byte(path), nil); e != nil {
			return e
		}
	}
	if e := it.Error(); e != nil {
		return e
	}

	if err = b.Set(sidecarVersionKey, []byte{1}, nil); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}

// A spill can survive a crash between assembler creation and Append. Discover
// these on every startup; the version gates only database schema migration.
// This walk is restricted to the assembler's standard spill tree and never
// follows symlinks or removes files owned by callers.
func (s *PebbleSpool) reconcileStandardSpill() error {
	b := s.db.NewBatch()
	defer b.Close()
	discovered := make(map[string]int64)
	spillDir := filepath.Join(s.dir, "spill")
	if err := filepath.WalkDir(spillDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) && path == spillDir {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if path == spillDir {
				return fmt.Errorf("standard spill directory is a symlink")
			}
			return nil
		}
		absolute, err := normalizeSidecar(path)
		if err != nil {
			return err
		}
		if s.sidecars[absolute] != nil {
			return nil
		}
		size, err := sidecarStat(absolute)
		if err != nil {
			return err
		}
		if err := b.Set(sidecarKey(absolute), sidecarValue(size), nil); err != nil {
			return err
		}
		discovered[absolute] = size
		return nil
	}); err != nil {
		return fmt.Errorf("reconcile spool spill files: %w", err)
	}
	if len(discovered) == 0 {
		return nil
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	// Publish charges only after all newly discovered paths are durable.
	for path, size := range discovered {
		s.sidecars[path] = &sidecar{size: size}
		s.sideBytes += size
	}
	return nil
}

// refreshSidecars accounts size changes and only removes a charge after physical
// deletion and the last durable or buffered reference has disappeared.
func (s *PebbleSpool) refreshSidecars() error {
	for path, v := range s.sidecars {
		size, err := sidecarStat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && v.refs == 0 {
				if err := s.db.Delete(sidecarKey(path), pebble.Sync); err != nil {
					return err
				}
				s.sideBytes -= v.size
				delete(s.sidecars, path)
				continue
			}
			return err
		}
		if size != v.size {
			if v.refs > 0 {
				return fmt.Errorf("spool sidecar %s changed size while referenced", path)
			}
			if err := s.db.Set(sidecarKey(path), sidecarValue(size), pebble.Sync); err != nil {
				return err
			}
			s.sideBytes += size - v.size
			v.size = size
		}
	}
	return nil
}
