package runner

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// PostgreSQL 12 ends recovery without an error when the WAL runs out before recovery_target_time (the
// "recovery ended before configured recovery target was reached" error only exists from 13): the server
// promotes at the end of the archive and the restore silently stops short. walReachesTarget proves from
// the staged WAL, before the data directory is published, that recovery will stop at the target.
//
// Recovery to a time (recovery_target_inclusive = on) stops before the first transaction commit or abort
// record whose timestamp is later than the target. Such a record must therefore be present in the
// contiguous WAL after the base backup's end; otherwise the target is beyond the archive (or a segment is
// missing) and recovery would end early.
//
// The reader follows the WAL format shared by PostgreSQL 9.5 and later: pages with a short or long header,
// records (XLogRecord, MAXALIGNed, possibly spanning pages) validated by their CRC-32C. Reading stops at the
// first gap, invalid page or invalid record: what follows is not WAL recovery can replay.

const (
	walLongHeader         = 0x0002
	walFirstIsContrecord  = 0x0001
	walShortPHD           = 24
	walLongPHD            = 40
	walRecordHeader       = 24
	walRmXLOG             = 0
	walRmXact             = 1
	walXLOGSwitch         = 0x40
	walXactOpMask         = 0x70
	walXactCommit         = 0x00
	walXactAbort          = 0x20
	walXactCommitPrepared = 0x30
	walXactAbortPrepared  = 0x40
)

var crc32c = crc32.MakeTable(crc32.Castagnoli)

var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// walScanResult is what the staged WAL proves.
type walScanResult struct {
	Reached    bool      // a commit/abort later than the target follows the base backup
	LastCommit time.Time // the last commit/abort time read (zero if none)
	EndLSN     uint64    // where valid, contiguous WAL ends
}

type walScanner struct {
	from     uint64
	target   time.Time
	res      walScanResult
	rec      []byte // record being assembled across pages
	recLSN   uint64
	need     int
	skip     int // continuation bytes of a record that started before the first staged segment
	switched bool
	stop     bool
}

// walReachesTarget scans the staged WAL segment files (in order).
func walReachesTarget(files []string, from uint64, target time.Time) (walScanResult, error) {
	s := &walScanner{from: from, target: target.Truncate(time.Microsecond)}
	var nextSeg uint64
	for i, path := range files {
		if s.stop || s.res.Reached {
			break
		}
		f, err := os.Open(path)
		if err != nil {
			return s.res, err
		}
		segStart, segSize, err := s.segment(f, filepath.Base(path), i == 0, nextSeg)
		f.Close()
		if err != nil {
			return s.res, err
		}
		nextSeg = segStart + segSize
	}
	return s.res, nil
}

// segment reads one segment; it returns the segment's start LSN and size.
func (s *walScanner) segment(f *os.File, name string, first bool, expect uint64) (uint64, uint64, error) {
	if len(name) != 24 {
		s.stop = true
		return 0, 0, nil
	}
	logID, err1 := strconv.ParseUint(name[8:16], 16, 32)
	segID, err2 := strconv.ParseUint(name[16:24], 16, 32)
	if err1 != nil || err2 != nil {
		s.stop = true
		return 0, 0, nil
	}
	head := make([]byte, walLongPHD)
	if _, err := io.ReadFull(f, head); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			s.stop = true
			return 0, 0, nil
		}
		return 0, 0, err
	}
	info := binary.LittleEndian.Uint16(head[2:])
	segSize := uint64(binary.LittleEndian.Uint32(head[32:]))
	pageSize := uint64(binary.LittleEndian.Uint32(head[36:]))
	if info&walLongHeader == 0 || segSize == 0 || segSize&(segSize-1) != 0 || pageSize == 0 || pageSize&(pageSize-1) != 0 ||
		pageSize > segSize || segSize > 1<<30 {
		s.stop = true
		return 0, 0, nil
	}
	segStart := (logID*(0x100000000/segSize) + segID) * segSize
	if !first && segStart != expect {
		s.stop = true // a gap: recovery cannot read past it
		return segStart, segSize, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	page := make([]byte, pageSize)
	s.switched = false
	for off := uint64(0); off < segSize && !s.stop && !s.switched && !s.res.Reached; off += pageSize {
		if _, err := io.ReadFull(f, page); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				s.stop = true
				break
			}
			return 0, 0, err
		}
		s.page(page, segStart+off, off == 0, first && off == 0)
	}
	return segStart, segSize, nil
}

func align8(n uint64) uint64 { return (n + 7) &^ 7 }

func (s *walScanner) page(page []byte, pageLSN uint64, long, firstPage bool) {
	info := binary.LittleEndian.Uint16(page[2:])
	if binary.LittleEndian.Uint64(page[8:]) != pageLSN || (long != (info&walLongHeader != 0)) {
		s.stop = true // not written in this segment's current life (recycled or never written)
		return
	}
	remLen := int(binary.LittleEndian.Uint32(page[16:]))
	pos := uint64(walShortPHD)
	if long {
		pos = walLongPHD
	}
	size := uint64(len(page))
	switch {
	case firstPage && info&walFirstIsContrecord != 0:
		s.skip = remLen // the tail of a record that began before the staged WAL
	case s.need > 0:
		if info&walFirstIsContrecord == 0 || remLen != s.need-len(s.rec) {
			s.stop = true
			return
		}
	}
	if s.skip > 0 {
		n := min(uint64(s.skip), size-pos)
		s.skip -= int(n)
		pos += n
		if s.skip > 0 {
			return
		}
		pos = align8(pos)
	}
	if s.need > 0 {
		n := min(uint64(s.need-len(s.rec)), size-pos)
		s.rec = append(s.rec, page[pos:pos+n]...)
		pos += n
		if len(s.rec) < s.need {
			return
		}
		s.record(s.rec, s.recLSN)
		s.rec, s.need = nil, 0
		pos = align8(pos)
	}
	for pos+4 <= size && !s.stop && !s.switched && !s.res.Reached {
		total := int(binary.LittleEndian.Uint32(page[pos:]))
		if total == 0 {
			s.stop = true // end of WAL
			return
		}
		if total < walRecordHeader || total > 1<<30 {
			s.stop = true
			return
		}
		lsn := pageLSN + pos
		if avail := size - pos; uint64(total) > avail {
			s.rec = append(make([]byte, 0, total), page[pos:]...)
			s.recLSN, s.need = lsn, total
			return
		}
		s.record(page[pos:pos+uint64(total)], lsn)
		pos = align8(pos + uint64(total))
	}
}

func (s *walScanner) record(rec []byte, lsn uint64) {
	crc := crc32.Update(0, crc32c, rec[walRecordHeader:])
	crc = crc32.Update(crc, crc32c, rec[:20])
	if crc != binary.LittleEndian.Uint32(rec[20:]) {
		s.stop = true // a torn or stale record: the end of valid WAL
		return
	}
	s.res.EndLSN = lsn + uint64(len(rec))
	info, rmid := rec[16], rec[17]
	if rmid == walRmXLOG && info&0xF0 == walXLOGSwitch {
		s.switched = true // the rest of the segment is unused
		return
	}
	if rmid != walRmXact {
		return
	}
	switch info & walXactOpMask {
	case walXactCommit, walXactAbort, walXactCommitPrepared, walXactAbortPrepared:
	default:
		return
	}
	at, ok := xactTime(rec)
	if !ok || lsn < s.from {
		return
	}
	s.res.LastCommit = at
	if at.After(s.target) {
		s.res.Reached = true
	}
}

// xactTime returns the xact_time of a commit/abort record: the first field of its main data.
func xactTime(rec []byte) (time.Time, bool) {
	p := walRecordHeader
	for p < len(rec) {
		switch id := rec[p]; id {
		case 255: // XLR_BLOCK_ID_DATA_SHORT
			if p+2 > len(rec) {
				return time.Time{}, false
			}
			n := int(rec[p+1])
			p += 2
			return mainDataTime(rec, p, n)
		case 254: // XLR_BLOCK_ID_DATA_LONG
			if p+5 > len(rec) {
				return time.Time{}, false
			}
			n := int(binary.LittleEndian.Uint32(rec[p+1:]))
			p += 5
			return mainDataTime(rec, p, n)
		case 253: // XLR_BLOCK_ID_ORIGIN
			p += 3
		case 252: // XLR_BLOCK_ID_TOPLEVEL_XID
			p += 5
		default: // a block reference: not expected in a transaction record
			return time.Time{}, false
		}
	}
	return time.Time{}, false
}

func mainDataTime(rec []byte, p, n int) (time.Time, bool) {
	if n < 8 || p+n != len(rec) {
		return time.Time{}, false
	}
	us := int64(binary.LittleEndian.Uint64(rec[p:]))
	return pgEpoch.Add(time.Duration(us) * time.Microsecond), true
}

func (r walScanResult) String() string {
	last := "no transaction after the base backup"
	if !r.LastCommit.IsZero() {
		last = "last transaction at " + r.LastCommit.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("WAL readable up to %s, %s", FormatLSN(r.EndLSN), last)
}
