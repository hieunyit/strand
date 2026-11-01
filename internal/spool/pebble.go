package spool

import (
	"context"
	"encoding/binary"
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
	if err == nil {
		if len(val) >= 8 {
			s.nextSeq = binary.BigEndian.Uint64(val)
		}
		_ = closer.Close()
	}

	// Đọc dung lượng usage hiện tại
	val, closer, err = db.Get(metaUsageKey)
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
		if err == nil {
			for ok := iter.First(); ok; ok = iter.Next() {
				totalBytes += int64(len(iter.Value()))
			}
			_ = iter.Close()
		}
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
	s.nextSeq++

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
	binary.BigEndian.PutUint64(nextSeqBuf[:], s.nextSeq)
	if err := batch.Set(metaNextSeqKey, nextSeqBuf[:], nil); err != nil {
		return err
	}

	// Cập nhật usage bytes
	s.usage.LocalBytes += int64(len(data))
	var usageBuf [8]byte
	binary.BigEndian.PutUint64(usageBuf[:], uint64(s.usage.LocalBytes))
	if err := batch.Set(metaUsageKey, usageBuf[:], nil); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
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
		if err == nil {
			if len(val) >= 8 {
				seq := binary.BigEndian.Uint64(val)
				startKey = txnKey(seq)
			}
			_ = closer.Close()
		}
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
			continue
		}

		if applied.Contains(t.SourcePos) {
			_ = batch.Delete(iter.Key(), nil)
			if t.SourcePos.GTID != "" || t.SourcePos.LSN != 0 {
				_ = batch.Delete(posKey(t.SourcePos), nil)
			}
			deletedBytes += int64(len(val))
		}
	}

	if deletedBytes > 0 {
		s.usage.LocalBytes -= deletedBytes
		if s.usage.LocalBytes < 0 {
			s.usage.LocalBytes = 0
		}
		var usageBuf [8]byte
		binary.BigEndian.PutUint64(usageBuf[:], uint64(s.usage.LocalBytes))
		if err := batch.Set(metaUsageKey, usageBuf[:], nil); err != nil {
			return err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
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
