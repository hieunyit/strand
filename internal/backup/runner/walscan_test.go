package runner

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	testSegSize  = 1 << 20
	testPageSize = 8192
)

// walRecord builds an XLogRecord of rmgr rmid/info whose main data is data.
func walRecord(rmid, info byte, data []byte) []byte {
	rec := make([]byte, walRecordHeader, walRecordHeader+2+len(data))
	if len(data) < 256 {
		rec = append(rec, 255, byte(len(data)))
	} else {
		rec = append(rec, 254, 0, 0, 0, 0)
		binary.LittleEndian.PutUint32(rec[walRecordHeader+1:], uint32(len(data)))
	}
	rec = append(rec, data...)
	binary.LittleEndian.PutUint32(rec[0:], uint32(len(rec)))
	rec[16], rec[17] = info, rmid
	crc := crc32.Update(0, crc32c, rec[walRecordHeader:])
	crc = crc32.Update(crc, crc32c, rec[:20])
	binary.LittleEndian.PutUint32(rec[20:], crc)
	return rec
}

func commitRecord(at time.Time, pad int) []byte {
	data := make([]byte, 8+pad)
	binary.LittleEndian.PutUint64(data, uint64(at.Sub(pgEpoch)/time.Microsecond))
	return walRecord(walRmXact, walXactCommit, data)
}

// walSegment lays records out like PostgreSQL: page headers, MAXALIGNed records, continuation records.
// It returns the segment and the LSN of each record.
func walSegment(segno uint64, records ...[]byte) ([]byte, []uint64) {
	seg := make([]byte, testSegSize)
	start := segno * testSegSize
	header := func(off uint64, rem int) uint64 {
		h := seg[off:]
		binary.LittleEndian.PutUint16(h[0:], 0xD10D)
		info := uint16(0)
		size := uint64(walShortPHD)
		if off == 0 {
			info |= walLongHeader
			binary.LittleEndian.PutUint32(h[32:], testSegSize)
			binary.LittleEndian.PutUint32(h[36:], testPageSize)
			size = walLongPHD
		}
		if rem > 0 {
			info |= walFirstIsContrecord
		}
		binary.LittleEndian.PutUint16(h[2:], info)
		binary.LittleEndian.PutUint32(h[4:], 1)
		binary.LittleEndian.PutUint64(h[8:], start+off)
		binary.LittleEndian.PutUint32(h[16:], uint32(rem))
		return off + size
	}
	pos := header(0, 0)
	var lsns []uint64
	for _, rec := range records {
		lsns = append(lsns, start+pos)
		for left := rec; len(left) > 0; {
			if pos%testPageSize == 0 {
				rem := len(left)
				if len(left) == len(rec) {
					rem = 0 // a record starting at a page boundary is not a continuation
				}
				pos = header(pos, rem)
			}
			n := copy(seg[pos:pos+min(uint64(len(left)), testPageSize-pos%testPageSize)], left)
			left, pos = left[n:], pos+uint64(n)
		}
		pos = align8(pos)
	}
	return seg, lsns
}

func writeSegment(t *testing.T, dir string, segno uint64, seg []byte) string {
	t.Helper()
	p := filepath.Join(dir, fmt.Sprintf("00000001%08X%08X", segno/(0x100000000/testSegSize), segno%(0x100000000/testSegSize)))
	if err := os.WriteFile(p, seg, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWALScanFindsTheCommitThatStopsRecovery(t *testing.T) {
	t0 := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	before, after := t0.Add(-time.Second), t0.Add(time.Second)
	// Fill most of the first page so the later commit record spans a page boundary.
	filler := walRecord(2, 0, make([]byte, testPageSize-400))
	other := walRecord(walRmXact, 0x50, make([]byte, 16)) // XLOG_XACT_ASSIGNMENT: not a commit
	seg, lsns := walSegment(3, filler, commitRecord(before, 0), other, commitRecord(after, 600))
	if lsns[3]/testPageSize == (lsns[3]+uint64(len(commitRecord(after, 600))))/testPageSize {
		t.Fatal("fixture: the last commit does not span a page")
	}
	dir := t.TempDir()
	path := writeSegment(t, dir, 3, seg)
	from := 3 * uint64(testSegSize)

	res, err := walReachesTarget([]string{path}, from, t0)
	if err != nil || !res.Reached || !res.LastCommit.Equal(after) {
		t.Fatalf("target between two commits: %+v %v", res, err)
	}
	res, err = walReachesTarget([]string{path}, from, after)
	if err != nil || res.Reached || !res.LastCommit.Equal(after) {
		t.Fatalf("target at the last commit (inclusive): %+v %v", res, err)
	}
	// Commits before the end of the base backup do not count.
	if res, err := walReachesTarget([]string{path}, lsns[3]+1, t0); err != nil || res.Reached {
		t.Fatalf("commit before the base backup end counted: %+v %v", res, err)
	}
	// A torn record ends the readable WAL.
	torn := append([]byte(nil), seg...)
	torn[lsns[3]-from+30] ^= 0xFF
	tornPath := filepath.Join(t.TempDir(), filepath.Base(path))
	if err := os.WriteFile(tornPath, torn, 0o600); err != nil {
		t.Fatal(err)
	}
	if res, err := walReachesTarget([]string{tornPath}, from, t0); err != nil || res.Reached {
		t.Fatalf("torn record accepted: %+v %v", res, err)
	}

	// A missing segment: the commit after the gap is unreachable.
	early, _ := walSegment(3, commitRecord(before, 0), walRecord(walRmXLOG, walXLOGSwitch, nil))
	late, _ := walSegment(5, commitRecord(after, 0))
	gapDir := t.TempDir()
	files := []string{writeSegment(t, gapDir, 3, early), writeSegment(t, gapDir, 5, late)}
	if res, err := walReachesTarget(files, from, t0); err != nil || res.Reached {
		t.Fatalf("commit after a gap counted: %+v %v", res, err)
	}
	// Contiguous after a WAL switch: found in the next segment.
	next, _ := walSegment(4, commitRecord(after, 0))
	files[1] = writeSegment(t, gapDir, 4, next)
	if res, err := walReachesTarget(files, from, t0); err != nil || !res.Reached {
		t.Fatalf("commit after a WAL switch: %+v %v", res, err)
	}
}
