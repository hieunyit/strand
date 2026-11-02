package spool

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

const (
	segmentMagic    = "STSG1"
	segmentMaxBytes = 4 * 1024 * 1024
	segmentMaxTxns  = 2000
	segmentMaxAge   = 500 * time.Millisecond
)

var (
	metaNextSeqKey     = []byte("meta/next_seq")
	metaUsageKey       = []byte("meta/usage_bytes")
	metaBucketUsageKey = []byte("meta/bucket_usage_bytes")
	bucketRefPrefix    = []byte("BUCKET:")
	bucketSegRefPrefix = []byte("BUCKETSEG:")
)

type segEntry struct {
	seq  uint64
	pos  model.Pos
	data []byte
}

// Option cấu hình PebbleSpool.
type Option func(*PebbleSpool)

// WithLocalHardLimit đặt giới hạn dung lượng cứng cho tầng Pebble local (bytes).
// Vượt qua ngưỡng này thì Txn sẽ tràn sang bucket staging (nếu có bucket).
func WithLocalHardLimit(limit int64) Option {
	return func(s *PebbleSpool) {
		s.localHardLimit = limit
	}
}

// WithTotalHardLimit đặt giới hạn dung lượng cứng cho toàn bộ các tầng spool (bytes).
// spec: 02 §9: mọi tầng spool đầy thì báo prefetch_exhausted.
func WithTotalHardLimit(limit int64) Option {
	return func(s *PebbleSpool) {
		s.totalHardLimit = limit
	}
}

// WithBucket cấu hình tầng bucket staging (MinIO / S3) cho spool.
func WithBucket(b BucketBackend, prefix string) Option {
	return func(s *PebbleSpool) {
		s.bucket = b
		s.bucketPrefix = prefix
	}
}

// WithSegmentMaxAge đổi tuổi tối đa của segment đang đệm trước khi bị đẩy lên bucket (mặc định 500 ms).
func WithSegmentMaxAge(d time.Duration) Option {
	return func(s *PebbleSpool) { s.segMaxAge = d }
}

// WithBeforeBatchCommit đặt hook kiểm thử chạy sau khi Put segment thành công và trước khi commit Pebble batch.
func WithBeforeBatchCommit(fn func() error) Option {
	return func(s *PebbleSpool) {
		s.beforeBatchCommit = fn
	}
}

// PebbleSpool cài đặt Spool lưu trữ trên đĩa bằng Pebble và hỗ trợ phân tầng tràn bucket.
// spec: 02 §3, §9, D §1
type PebbleSpool struct {
	mu                sync.RWMutex
	dir               string
	db                *pebble.DB
	nextSeq           uint64
	usage             SpoolUsage
	closed            bool
	localHardLimit    int64
	totalHardLimit    int64
	bucket            BucketBackend
	bucketPrefix      string
	segEntries        []segEntry
	segBytes          int64
	segRawBytes       int64
	segOldestAt       time.Time
	segMaxAge         time.Duration
	beforeBatchCommit func() error
}

// New khởi tạo hoặc mở lại một PebbleSpool tại thư mục chỉ định.
func New(dir string, opts ...Option) (*PebbleSpool, error) {
	pebbleOpts := &pebble.Options{}
	db, err := pebble.Open(dir, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", dir, err)
	}

	s := &PebbleSpool{
		segMaxAge: segmentMaxAge,
		dir:       dir,
		db:        db,
		nextSeq:   1,
	}
	for _, opt := range opts {
		opt(s)
	}

	// Đọc sequence tiếp theo
	val, closer, err := db.Get(metaNextSeqKey)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		_ = db.Close()
		return nil, fmt.Errorf("failed to read next_seq: %w", err)
	}
	if err == nil {
		if len(val) >= 8 {
			s.nextSeq = binary.BigEndian.Uint64(val)
		}
		_ = closer.Close()
	}

	// Đọc dung lượng usage local hiện tại
	val, closer, err = db.Get(metaUsageKey)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		_ = db.Close()
		return nil, fmt.Errorf("failed to read usage_bytes: %w", err)
	}
	if err == nil {
		if len(val) >= 8 {
			s.usage.LocalBytes = int64(binary.BigEndian.Uint64(val))
		}
		_ = closer.Close()
	} else {
		// Quét tính dung lượng local ban đầu nếu chưa có
		var totalBytes int64
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: []byte("txn/"),
			UpperBound: []byte("txn/\xff"),
		})
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("failed to scan txns for usage: %w", err)
		}
		for ok := iter.First(); ok; ok = iter.Next() {
			v := iter.Value()
			if !bytes.HasPrefix(v, bucketRefPrefix) && !bytes.HasPrefix(v, bucketSegRefPrefix) {
				totalBytes += int64(len(v))
			}
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			_ = db.Close()
			return nil, fmt.Errorf("failed to read iter during usage scan: %w", err)
		}
		_ = iter.Close()
		s.usage.LocalBytes = totalBytes
	}

	// Đọc dung lượng usage bucket hiện tại
	bVal, bCloser, bErr := db.Get(metaBucketUsageKey)
	if bErr == nil {
		if len(bVal) >= 8 {
			s.usage.BucketBytes = int64(binary.BigEndian.Uint64(bVal))
		}
		_ = bCloser.Close()
	}

	return s, nil
}

func txnKey(seq uint64) []byte {
	return []byte(fmt.Sprintf("txn/%016x", seq))
}

func posKey(pos model.Pos) []byte {
	if pos.Engine == model.EngineMySQL {
		return []byte(fmt.Sprintf("pos/mysql/%s", pos.GTID))
	} else if pos.Engine == model.EnginePG {
		return []byte(fmt.Sprintf("pos/pg/%016x", pos.LSN))
	}
	return []byte(fmt.Sprintf("pos/unknown/%s", pos.GTID))
}

func encodeSegment(entries []segEntry, hintBytes int64) []byte {
	var buf bytes.Buffer
	if hintBytes > 0 {
		buf.Grow(int(hintBytes))
	}
	buf.WriteString(segmentMagic)
	var uvBuf [binary.MaxVarintLen64]byte
	for _, e := range entries {
		n := binary.PutUvarint(uvBuf[:], uint64(len(e.data)))
		buf.Write(uvBuf[:n])
		buf.Write(e.data)
	}
	return buf.Bytes()
}

func decodeSegment(raw []byte) ([][]byte, error) {
	if len(raw) < len(segmentMagic) || string(raw[:len(segmentMagic)]) != segmentMagic {
		return nil, fmt.Errorf("spool: invalid segment magic")
	}
	var entries [][]byte
	offset := len(segmentMagic)
	for offset < len(raw) {
		length, n := binary.Uvarint(raw[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("spool: invalid uvarint in segment at offset %d", offset)
		}
		offset += n
		if uint64(len(raw)-offset) < length {
			return nil, fmt.Errorf("spool: truncated segment payload at offset %d (need %d bytes, have %d)", offset, length, len(raw)-offset)
		}
		entries = append(entries, raw[offset:offset+int(length)])
		offset += int(length)
	}
	return entries, nil
}

func parseBucketSegRef(val []byte) (string, int, error) {
	rest := string(val[len(bucketSegRefPrefix):])
	sep := strings.LastIndexByte(rest, ':')
	if sep <= 0 || sep == len(rest)-1 {
		return "", 0, fmt.Errorf("spool: invalid bucket segment reference %q", string(val))
	}
	objKey := rest[:sep]
	idx, err := strconv.Atoi(rest[sep+1:])
	if err != nil || idx < 0 {
		return "", 0, fmt.Errorf("spool: invalid bucket segment index in %q: %w", string(val), err)
	}
	return objKey, idx, nil
}

func (s *PebbleSpool) discardSegmentLocked() {
	if len(s.segEntries) > 0 {
		s.nextSeq = s.segEntries[0].seq
	}
	s.segEntries = nil
	s.segBytes = 0
	s.segRawBytes = 0
	s.segOldestAt = time.Time{}
}

func (s *PebbleSpool) flushSegmentLocked(ctx context.Context) error {
	if len(s.segEntries) == 0 {
		return nil
	}
	if s.bucket == nil {
		s.discardSegmentLocked()
		return errors.New("spool: cannot flush overflow segment: bucket backend is nil")
	}

	entries := s.segEntries
	segBytes := s.segBytes
	firstSeq := entries[0].seq
	lastSeq := entries[len(entries)-1].seq
	objKey := fmt.Sprintf("%sseg/%016x-%016x", s.bucketPrefix, firstSeq, lastSeq)
	segData := encodeSegment(entries, s.segRawBytes)

	if err := s.bucket.Put(ctx, objKey, segData); err != nil {
		s.discardSegmentLocked()
		return fmt.Errorf("failed to put overflow segment to bucket: %w", err)
	}

	if s.beforeBatchCommit != nil {
		if err := s.beforeBatchCommit(); err != nil {
			s.discardSegmentLocked()
			return err
		}
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	for i, e := range entries {
		refVal := []byte(fmt.Sprintf("BUCKETSEG:%s:%d", objKey, i))
		if err := batch.Set(txnKey(e.seq), refVal, nil); err != nil {
			s.discardSegmentLocked()
			return err
		}
		if e.pos.GTID != "" || e.pos.LSN != 0 {
			var seqBuf [8]byte
			binary.BigEndian.PutUint64(seqBuf[:], e.seq)
			if err := batch.Set(posKey(e.pos), seqBuf[:], nil); err != nil {
				s.discardSegmentLocked()
				return err
			}
		}
	}

	newBucketBytes := s.usage.BucketBytes + segBytes
	var bUsageBuf [8]byte
	binary.BigEndian.PutUint64(bUsageBuf[:], uint64(newBucketBytes))
	if err := batch.Set(metaBucketUsageKey, bUsageBuf[:], nil); err != nil {
		s.discardSegmentLocked()
		return err
	}

	newNextSeq := lastSeq + 1
	var nextSeqBuf [8]byte
	binary.BigEndian.PutUint64(nextSeqBuf[:], newNextSeq)
	if err := batch.Set(metaNextSeqKey, nextSeqBuf[:], nil); err != nil {
		s.discardSegmentLocked()
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		s.discardSegmentLocked()
		return err
	}

	s.usage.BucketBytes = newBucketBytes
	s.segEntries = nil
	s.segBytes = 0
	s.segRawBytes = 0
	s.segOldestAt = time.Time{}
	return nil
}

// Append ghi một transaction vào spool, tự động tràn sang bucket nếu local đầy.
// spec: 02 §9
func (s *PebbleSpool) Append(ctx context.Context, t *model.Txn) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("spool is closed")
	}

	data, err := encodeTxn(t)
	if err != nil {
		return fmt.Errorf("failed to encode txn: %w", err)
	}

	seq := s.nextSeq
	newNextSeq := seq + 1
	dataLen := int64(len(data))

	// 1. Kiểm tra giới hạn tổng hard limit của mọi tầng spool (spec: 02 §9)
	if s.totalHardLimit > 0 && (s.usage.LocalBytes+s.usage.BucketBytes+s.segBytes+dataLen) > s.totalHardLimit {
		return exitcode.New(
			exitcode.ErrPrefetchExhausted,
			exitcode.DataError,
			fmt.Sprintf("spool capacity reached hard limit (%d bytes) across all tiers", s.totalHardLimit),
			nil,
		)
	}

	// 2. Kiểm tra xem có cần tràn sang tầng Bucket không
	overflow := false
	if s.localHardLimit > 0 && (s.usage.LocalBytes+dataLen) > s.localHardLimit {
		if s.bucket == nil {
			// Không có cấu hình bucket overflow -> chạm hard limit local
			return exitcode.New(
				exitcode.ErrPrefetchExhausted,
				exitcode.DataError,
				fmt.Sprintf("local spool hard limit reached (%d bytes) and no bucket overflow configured", s.localHardLimit),
				nil,
			)
		}
		overflow = true
	}

	if overflow {
		if len(s.segEntries) == 0 {
			s.segOldestAt = time.Now()
			s.segRawBytes = int64(len(segmentMagic))
		}
		var uvBuf [binary.MaxVarintLen64]byte
		uvLen := binary.PutUvarint(uvBuf[:], uint64(dataLen))
		s.segEntries = append(s.segEntries, segEntry{
			seq:  seq,
			pos:  t.SourcePos,
			data: data,
		})
		s.segBytes += dataLen
		s.segRawBytes += int64(uvLen) + dataLen
		s.nextSeq = newNextSeq

		if s.segBytes >= segmentMaxBytes || s.segRawBytes >= segmentMaxBytes || len(s.segEntries) >= segmentMaxTxns || time.Since(s.segOldestAt) >= s.segMaxAge {
			if err := s.flushSegmentLocked(ctx); err != nil {
				return err
			}
		}
		return nil
	}

	if len(s.segEntries) > 0 {
		if err := s.flushSegmentLocked(ctx); err != nil {
			return err
		}
		seq = s.nextSeq
		newNextSeq = seq + 1
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	newLocalBytes := s.usage.LocalBytes + dataLen
	if err := batch.Set(txnKey(seq), data, nil); err != nil {
		return err
	}
	var usageBuf [8]byte
	binary.BigEndian.PutUint64(usageBuf[:], uint64(newLocalBytes))
	if err := batch.Set(metaUsageKey, usageBuf[:], nil); err != nil {
		return err
	}

	// Lưu index vị trí SourcePos -> seq
	if t.SourcePos.GTID != "" || t.SourcePos.LSN != 0 {
		var seqBuf [8]byte
		binary.BigEndian.PutUint64(seqBuf[:], seq)
		if err := batch.Set(posKey(t.SourcePos), seqBuf[:], nil); err != nil {
			return err
		}
	}

	// Cập nhật next_seq
	var nextSeqBuf [8]byte
	binary.BigEndian.PutUint64(nextSeqBuf[:], newNextSeq)
	if err := batch.Set(metaNextSeqKey, nextSeqBuf[:], nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	s.nextSeq = newNextSeq
	s.usage.LocalBytes = newLocalBytes
	return nil
}

// Flush đẩy các transaction tràn đang đệm trong bộ nhớ thành một segment lên bucket và commit Pebble batch.
func (s *PebbleSpool) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("spool is closed")
	}

	return s.flushSegmentLocked(ctx)
}

// Iter mở một iterator đọc tuần tự từ vị trí `from` (nếu from rỗng thì đọc từ đầu).
func (s *PebbleSpool) Iter(ctx context.Context, from model.Pos) (SpoolIter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, fmt.Errorf("spool is closed")
	}

	if err := s.flushSegmentLocked(ctx); err != nil {
		return nil, err
	}

	startKey := []byte("txn/0000000000000000")
	if from.GTID != "" || from.LSN != 0 {
		pKey := posKey(from)
		val, closer, err := s.db.Get(pKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil, fmt.Errorf("start position %v not found in spool: %w", from, err)
			}
			return nil, fmt.Errorf("failed to lookup position %v: %w", from, err)
		}
		if len(val) >= 8 {
			seq := binary.BigEndian.Uint64(val)
			startKey = txnKey(seq)
		}
		_ = closer.Close()
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: []byte("txn/\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble iterator: %w", err)
	}

	return &pebbleSpoolIter{
		iter:   iter,
		first:  true,
		bucket: s.bucket,
	}, nil
}

// Truncate dọn dẹp các Txn đã được ghi nhận trong AppliedSet.
func (s *PebbleSpool) Truncate(ctx context.Context, applied checkpoint.AppliedSet) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("spool is closed")
	}

	if err := s.flushSegmentLocked(ctx); err != nil {
		return err
	}

	if applied == nil {
		return nil
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("txn/"),
		UpperBound: []byte("txn/\xff"),
	})
	if err != nil {
		return fmt.Errorf("failed to create iterator for truncate: %w", err)
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()

	var deletedLocalBytes int64
	var deletedBucketBytes int64
	// Object trong bucket chỉ được xóa SAU khi batch Pebble gỡ tham chiếu đã commit;
	// crash ở giữa chỉ để lại object mồ côi, không để lại tham chiếu treo.
	var bucketKeysToDelete []string
	segRemainingRefs := make(map[string]int)
	segDeletedRefs := make(map[string]int)
	var segCandidates []string
	var lastSegKey string
	var lastSegEntries [][]byte

	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		val := iter.Value()
		var t *model.Txn
		var isBucket bool
		var bKey string
		var rawLen int64

		if bytes.HasPrefix(val, bucketSegRefPrefix) {
			objKey, idx, pErr := parseBucketSegRef(val)
			if pErr != nil {
				return pErr
			}
			if s.bucket != nil {
				if lastSegKey != objKey {
					raw, bErr := s.bucket.Get(ctx, objKey)
					if bErr == nil {
						entries, sErr := decodeSegment(raw)
						if sErr != nil {
							return sErr
						}
						lastSegKey = objKey
						lastSegEntries = entries
					} else {
						lastSegKey = ""
						lastSegEntries = nil
					}
				}
				if lastSegKey == objKey && idx < len(lastSegEntries) {
					rawTxn := lastSegEntries[idx]
					t, _ = decodeTxn(rawTxn)
					rawLen = int64(len(rawTxn))
				}
			}
			if t != nil && applied.Contains(t.SourcePos) {
				if err := batch.Delete(iter.Key(), nil); err != nil {
					return err
				}
				if t.SourcePos.GTID != "" || t.SourcePos.LSN != 0 {
					if err := batch.Delete(posKey(t.SourcePos), nil); err != nil {
						return err
					}
				}
				deletedBucketBytes += rawLen
				if segDeletedRefs[objKey] == 0 {
					segCandidates = append(segCandidates, objKey)
				}
				segDeletedRefs[objKey]++
			} else {
				segRemainingRefs[objKey]++
			}
			continue
		}

		if bytes.HasPrefix(val, bucketRefPrefix) {
			isBucket = true
			bKey = string(val[len(bucketRefPrefix):])
			if s.bucket != nil {
				raw, bErr := s.bucket.Get(ctx, bKey)
				if bErr == nil {
					t, _ = decodeTxn(raw)
					rawLen = int64(len(raw))
				}
			}
		} else {
			var dErr error
			t, dErr = decodeTxn(val)
			if dErr != nil {
				return fmt.Errorf("corrupted txn in spool: %w", dErr)
			}
		}

		if t != nil && applied.Contains(t.SourcePos) {
			if err := batch.Delete(iter.Key(), nil); err != nil {
				return err
			}
			if t.SourcePos.GTID != "" || t.SourcePos.LSN != 0 {
				if err := batch.Delete(posKey(t.SourcePos), nil); err != nil {
					return err
				}
			}
			if isBucket {
				bucketKeysToDelete = append(bucketKeysToDelete, bKey)
				deletedBucketBytes += rawLen
			} else {
				deletedLocalBytes += int64(len(val))
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during truncate: %w", err)
	}

	for _, objKey := range segCandidates {
		if segRemainingRefs[objKey] == 0 {
			bucketKeysToDelete = append(bucketKeysToDelete, objKey)
		}
	}

	if deletedLocalBytes > 0 || deletedBucketBytes > 0 {
		newLocalBytes := s.usage.LocalBytes - deletedLocalBytes
		if newLocalBytes < 0 {
			newLocalBytes = 0
		}
		newBucketBytes := s.usage.BucketBytes - deletedBucketBytes
		if newBucketBytes < 0 {
			newBucketBytes = 0
		}

		var usageBuf [8]byte
		binary.BigEndian.PutUint64(usageBuf[:], uint64(newLocalBytes))
		if err := batch.Set(metaUsageKey, usageBuf[:], nil); err != nil {
			return err
		}

		var bUsageBuf [8]byte
		binary.BigEndian.PutUint64(bUsageBuf[:], uint64(newBucketBytes))
		if err := batch.Set(metaBucketUsageKey, bUsageBuf[:], nil); err != nil {
			return err
		}

		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}

		s.usage.LocalBytes = newLocalBytes
		s.usage.BucketBytes = newBucketBytes
	}

	if s.bucket != nil {
		for _, k := range bucketKeysToDelete {
			_ = s.bucket.Delete(ctx, k) // lỗi chỉ để lại object mồ côi, không ảnh hưởng tính đúng
		}
	}
	return nil
}

// Usage trả về thông tin dung lượng byte của các tầng spool.
func (s *PebbleSpool) Usage() SpoolUsage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SpoolUsage{
		LocalBytes:  s.usage.LocalBytes,
		BucketBytes: s.usage.BucketBytes + s.segBytes,
	}
}

// Close đóng PebbleSpool.
func (s *PebbleSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	flushErr := s.flushSegmentLocked(context.Background())
	s.closed = true
	if s.bucket != nil {
		_ = s.bucket.Close()
	}
	dbErr := s.db.Close()
	if flushErr != nil {
		return flushErr
	}
	return dbErr
}

type pebbleSpoolIter struct {
	iter       *pebble.Iterator
	first      bool
	bucket     BucketBackend
	lastSegKey string
	lastSeg    [][]byte
}

func (it *pebbleSpoolIter) Next(ctx context.Context) (*model.Txn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var ok bool
	if it.first {
		it.first = false
		ok = it.iter.First()
	} else {
		ok = it.iter.Next()
	}

	if !ok {
		if err := it.iter.Error(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	val := it.iter.Value()
	if bytes.HasPrefix(val, bucketSegRefPrefix) {
		if it.bucket == nil {
			return nil, errors.New("cannot read overflow txn: bucket backend is nil")
		}
		objKey, idx, err := parseBucketSegRef(val)
		if err != nil {
			return nil, err
		}
		if it.lastSegKey != objKey {
			raw, err := it.bucket.Get(ctx, objKey)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch overflow segment from bucket %s: %w", objKey, err)
			}
			entries, err := decodeSegment(raw)
			if err != nil {
				return nil, err
			}
			it.lastSegKey = objKey
			it.lastSeg = entries
		}
		if idx < 0 || idx >= len(it.lastSeg) {
			return nil, fmt.Errorf("spool: segment %s index %d out of range (len %d)", objKey, idx, len(it.lastSeg))
		}
		return decodeTxn(it.lastSeg[idx])
	}

	if bytes.HasPrefix(val, bucketRefPrefix) {
		if it.bucket == nil {
			return nil, errors.New("cannot read overflow txn: bucket backend is nil")
		}
		bKey := string(val[len(bucketRefPrefix):])
		data, err := it.bucket.Get(ctx, bKey)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch overflow txn from bucket %s: %w", bKey, err)
		}
		return decodeTxn(data)
	}

	return decodeTxn(val)
}

func (it *pebbleSpoolIter) Close() error {
	return it.iter.Close()
}
