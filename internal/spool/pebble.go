package spool

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

var (
	metaNextSeqKey       = []byte("meta/next_seq")
	metaUsageKey         = []byte("meta/usage_bytes")
	metaBucketUsageKey   = []byte("meta/bucket_usage_bytes")
	bucketRefPrefix      = []byte("BUCKET:")
)

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

// PebbleSpool cài đặt Spool lưu trữ trên đĩa bằng Pebble và hỗ trợ phân tầng tràn bucket.
// spec: 02 §3, §9, D §1
type PebbleSpool struct {
	mu             sync.RWMutex
	dir            string
	db             *pebble.DB
	nextSeq        uint64
	usage          SpoolUsage
	closed         bool
	localHardLimit int64
	totalHardLimit int64
	bucket         BucketBackend
	bucketPrefix   string
}

// New khởi tạo hoặc mở lại một PebbleSpool tại thư mục chỉ định.
func New(dir string, opts ...Option) (*PebbleSpool, error) {
	pebbleOpts := &pebble.Options{}
	db, err := pebble.Open(dir, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", dir, err)
	}

	s := &PebbleSpool{
		dir:     dir,
		db:      db,
		nextSeq: 1,
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
			if !bytes.HasPrefix(v, bucketRefPrefix) {
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
	if s.totalHardLimit > 0 && (s.usage.LocalBytes+s.usage.BucketBytes+dataLen) > s.totalHardLimit {
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

	batch := s.db.NewBatch()
	defer batch.Close()

	var newLocalBytes = s.usage.LocalBytes
	var newBucketBytes = s.usage.BucketBytes

	if overflow {
		// Tràn sang bucket staging (MinIO / S3)
		bKey := fmt.Sprintf("%stxn/%016x", s.bucketPrefix, seq)
		if err := s.bucket.Put(ctx, bKey, data); err != nil {
			return fmt.Errorf("failed to put overflow txn to bucket: %w", err)
		}

		refVal := append(bucketRefPrefix, []byte(bKey)...)
		if err := batch.Set(txnKey(seq), refVal, nil); err != nil {
			return err
		}

		newBucketBytes += dataLen
		var bUsageBuf [8]byte
		binary.BigEndian.PutUint64(bUsageBuf[:], uint64(newBucketBytes))
		if err := batch.Set(metaBucketUsageKey, bUsageBuf[:], nil); err != nil {
			return err
		}
	} else {
		// Lưu trực tiếp vào Pebble local
		if err := batch.Set(txnKey(seq), data, nil); err != nil {
			return err
		}
		newLocalBytes += dataLen
		var usageBuf [8]byte
		binary.BigEndian.PutUint64(usageBuf[:], uint64(newLocalBytes))
		if err := batch.Set(metaUsageKey, usageBuf[:], nil); err != nil {
			return err
		}
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
	s.usage.BucketBytes = newBucketBytes
	return nil
}

// Iter mở một iterator đọc tuần tự từ vị trí `from` (nếu from rỗng thì đọc từ đầu).
func (s *PebbleSpool) Iter(ctx context.Context, from model.Pos) (SpoolIter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("spool is closed")
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
	if applied == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("spool is closed")
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

	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		val := iter.Value()
		var t *model.Txn
		var isBucket bool
		var bKey string

		if bytes.HasPrefix(val, bucketRefPrefix) {
			isBucket = true
			bKey = string(val[len(bucketRefPrefix):])
			if s.bucket != nil {
				raw, bErr := s.bucket.Get(ctx, bKey)
				if bErr == nil {
					t, _ = decodeTxn(raw)
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
				if s.bucket != nil {
					_ = s.bucket.Delete(ctx, bKey)
				}
				deletedBucketBytes += int64(t.Bytes)
			} else {
				deletedLocalBytes += int64(len(val))
			}
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during truncate: %w", err)
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

	return nil
}

// Usage trả về thông tin dung lượng byte của các tầng spool.
func (s *PebbleSpool) Usage() SpoolUsage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.usage
}

// Close đóng PebbleSpool.
func (s *PebbleSpool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true
	if s.bucket != nil {
		_ = s.bucket.Close()
	}
	return s.db.Close()
}

type pebbleSpoolIter struct {
	iter   *pebble.Iterator
	first  bool
	bucket BucketBackend
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
