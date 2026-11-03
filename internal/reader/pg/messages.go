package pg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

// Tag các message two-phase của pgoutput (proto ≥ 3). pglogrepl chưa giải mã các message này
// nên reader tự parse. Giá trị lấy đúng theo src/include/replication/logicalproto.h:
//
//	LOGICAL_REP_MSG_BEGIN_PREPARE    = 'b'
//	LOGICAL_REP_MSG_PREPARE          = 'P'
//	LOGICAL_REP_MSG_COMMIT_PREPARED  = 'K'
//	LOGICAL_REP_MSG_ROLLBACK_PREPARED = 'r'
//	LOGICAL_REP_MSG_STREAM_PREPARE   = 'p'
const (
	msgBeginPrepare     = 'b'
	msgPrepare          = 'P'
	msgCommitPrepared   = 'K'
	msgRollbackPrepared = 'r'
	msgStreamPrepare    = 'p'
)

// pgEpoch là mốc thời gian của PostgreSQL (2000-01-01 00:00:00 UTC).
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func pgTimeToTime(microsec int64) time.Time {
	return pgEpoch.Add(time.Duration(microsec) * time.Microsecond).UTC()
}

// readCString đọc chuỗi kết thúc bằng byte 0x00.
func readCString(data []byte) (string, error) {
	idx := bytes.IndexByte(data, 0)
	if idx < 0 {
		return "", fmt.Errorf("thiếu byte kết thúc chuỗi")
	}
	return string(data[:idx]), nil
}

// prepareMsg gom chung các message two-phase. Trường nào không có trong loại message thì để 0.
//
// Định dạng (sau byte tag), theo "Logical Replication Message Formats":
//
//	'b' Begin Prepare:     Int64 prepare_lsn, Int64 end_lsn, Int64 prepare_ts, Int32 xid, String gid
//	'P' Prepare:           Int8 flags, Int64 prepare_lsn, Int64 end_lsn, Int64 prepare_ts, Int32 xid, String gid
//	'p' Stream Prepare:    giống 'P'
//	'K' Commit Prepared:   Int8 flags, Int64 commit_lsn, Int64 end_lsn, Int64 commit_ts, Int32 xid, String gid
//	'r' Rollback Prepared: Int8 flags, Int64 prepare_end_lsn, Int64 rollback_end_lsn, Int64 prepare_ts,
//	                       Int64 rollback_ts, Int32 xid, String gid
type prepareMsg struct {
	Tag byte
	// LSN là prepare_lsn ('b','P','p') hoặc commit_lsn ('K'); với 'r' là prepare_end_lsn.
	LSN    pglogrepl.LSN
	EndLSN pglogrepl.LSN
	Time   time.Time
	Xid    uint32
	GID    string
}

// parsePrepareMsg giải mã một message two-phase; data bao gồm cả byte tag.
func parsePrepareMsg(data []byte) (*prepareMsg, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("message rỗng")
	}
	m := &prepareMsg{Tag: data[0]}
	src := data[1:]
	need := func(n int) error {
		if len(src) < n {
			return fmt.Errorf("message two-phase %q quá ngắn: %d byte, cần ≥ %d", m.Tag, len(data), n+1)
		}
		return nil
	}
	u64 := func(off int) uint64 { return binary.BigEndian.Uint64(src[off : off+8]) }

	var gidOff int
	switch m.Tag {
	case msgBeginPrepare:
		if err := need(28); err != nil {
			return nil, err
		}
		m.LSN = pglogrepl.LSN(u64(0))
		m.EndLSN = pglogrepl.LSN(u64(8))
		m.Time = pgTimeToTime(int64(u64(16)))
		m.Xid = binary.BigEndian.Uint32(src[24:28])
		gidOff = 28
	case msgPrepare, msgStreamPrepare, msgCommitPrepared:
		if err := need(29); err != nil {
			return nil, err
		}
		m.LSN = pglogrepl.LSN(u64(1))
		m.EndLSN = pglogrepl.LSN(u64(9))
		m.Time = pgTimeToTime(int64(u64(17)))
		m.Xid = binary.BigEndian.Uint32(src[25:29])
		gidOff = 29
	case msgRollbackPrepared:
		if err := need(37); err != nil {
			return nil, err
		}
		m.LSN = pglogrepl.LSN(u64(1))
		m.EndLSN = pglogrepl.LSN(u64(9))
		// prepare_ts ở offset 17 không dùng; lấy rollback_ts.
		m.Time = pgTimeToTime(int64(u64(25)))
		m.Xid = binary.BigEndian.Uint32(src[33:37])
		gidOff = 37
	default:
		return nil, fmt.Errorf("không phải message two-phase: tag %q", m.Tag)
	}
	gid, err := readCString(src[gidOff:])
	if err != nil {
		return nil, fmt.Errorf("đọc GID của message %q: %w", m.Tag, err)
	}
	m.GID = gid
	return m, nil
}
