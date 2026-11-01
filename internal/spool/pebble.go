package spool

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/pebble"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

var (
	metaNextSeqKey = []byte("meta/next_seq")
	metaUsageKey   = []byte("meta/usage_bytes")
)

// PebbleSpool cài đặt Spool lưu trữ trên đĩa bằng Pebble.
// spec: 02 §3, D §1
type PebbleSpool struct {
	mu      sync.RWMutex
	dir     string
	db      *pebble.DB
	nextSeq uint64
	usage   SpoolUsage
	closed  bool
}

// New khởi tạo hoặc mở lại một PebbleSpool tại thư mục chỉ định.
func New(dir string) (*PebbleSpool, error) {
	opts := &pebble.Options{}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", dir, err)
	}

	s := &PebbleSpool{
		dir:     dir,
		db:      db,
		nextSeq: 1,
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

	// Đọc dung lượng usage hiện tại
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
		// Nếu chưa lưu usage_bytes, quét và tính toán từ toàn bộ các Txn đang lưu
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
			totalBytes += int64(len(iter.Value()))
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			_ = db.Close()
			return nil, fmt.Errorf("failed to read iter during usage scan: %w", err)
		}
		_ = iter.Close()
		s.usage.LocalBytes = totalBytes
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

// Append ghi một transaction vào spool.
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
	newLocalBytes := s.usage.LocalBytes + dataLen

	batch := s.db.NewBatch()
	defer batch.Close()

	// Lưu bản ghi txn
	if err := batch.Set(txnKey(seq), data, nil); err != nil {
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

	// Cập nhật usage bytes
	var usageBuf [8]byte
	binary.BigEndian.PutUint64(usageBuf[:], uint64(newLocalBytes))
	if err := batch.Set(metaUsageKey, usageBuf[:], nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// Cập nhật trạng thái in-memory sau khi ghi đĩa thành công
	s.nextSeq = newNextSeq
	s.usage.LocalBytes = newLocalBytes
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
		iter:  iter,
		first: true,
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

	var deletedBytes int64
	for ok := iter.First(); ok; ok = iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}

		val := iter.Value()
		t, err := decodeTxn(val)
		if err != nil {
			return fmt.Errorf("corrupted txn in spool: %w", err)
		}

		if applied.Contains(t.SourcePos) {
			if err := batch.Delete(iter.Key(), nil); err != nil {
				return err
			}
			if t.SourcePos.GTID != "" || t.SourcePos.LSN != 0 {
				if err := batch.Delete(posKey(t.SourcePos), nil); err != nil {
					return err
				}
			}
			deletedBytes += int64(len(val))
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error during truncate: %w", err)
	}

	if deletedBytes > 0 {
		newLocalBytes := s.usage.LocalBytes - deletedBytes
		if newLocalBytes < 0 {
			newLocalBytes = 0
		}
		var usageBuf [8]byte
		binary.BigEndian.PutUint64(usageBuf[:], uint64(newLocalBytes))
		if err := batch.Set(metaUsageKey, usageBuf[:], nil); err != nil {
			return err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}

		// Cập nhật trạng thái in-memory sau khi commit thành công
		s.usage.LocalBytes = newLocalBytes
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
	return s.db.Close()
}

type pebbleSpoolIter struct {
	iter  *pebble.Iterator
	first bool
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

	return decodeTxn(it.iter.Value())
}

func (it *pebbleSpoolIter) Close() error {
	return it.iter.Close()
}
