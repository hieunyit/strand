package spool

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"

	"strand/internal/model"
)

// EncodeTxnForTest xuất encodeTxn cho package spool_test.
func EncodeTxnForTest(t *model.Txn) ([]byte, error) {
	return encodeTxn(t)
}

// DecodeTxnForTest xuất decodeTxn cho package spool_test.
func DecodeTxnForTest(data []byte) (*model.Txn, error) {
	return decodeTxn(data)
}

// SetBeforeBatchCommitForTest đặt hook kiểm thử trước khi commit Pebble batch của segment.
func SetBeforeBatchCommitForTest(s *PebbleSpool, fn func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beforeBatchCommit = fn
}

// AppendLegacyBucketForTest ghi một Txn theo định dạng BUCKET:<key> cũ để kiểm thử tương thích ngược.
func AppendLegacyBucketForTest(ctx context.Context, s *PebbleSpool, t *model.Txn) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.flushSegmentLocked(ctx); err != nil {
		return err
	}

	data, err := encodeTxn(t)
	if err != nil {
		return err
	}
	seq := s.nextSeq
	newNextSeq := seq + 1
	dataLen := int64(len(data))

	bKey := fmt.Sprintf("%stxn/%016x", s.bucketPrefix, seq)
	if err := s.bucket.Put(ctx, bKey, data); err != nil {
		return err
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	refVal := append(append([]byte(nil), bucketRefPrefix...), []byte(bKey)...)
	if err := batch.Set(txnKey(seq), refVal, nil); err != nil {
		return err
	}

	newBucketBytes := s.usage.BucketBytes + dataLen
	var bUsageBuf [8]byte
	binary.BigEndian.PutUint64(bUsageBuf[:], uint64(newBucketBytes))
	if err := batch.Set(metaBucketUsageKey, bUsageBuf[:], nil); err != nil {
		return err
	}

	if t.SourcePos.GTID != "" || t.SourcePos.LSN != 0 {
		var seqBuf [8]byte
		binary.BigEndian.PutUint64(seqBuf[:], seq)
		if err := batch.Set(posKey(t.SourcePos), seqBuf[:], nil); err != nil {
			return err
		}
	}

	var nextSeqBuf [8]byte
	binary.BigEndian.PutUint64(nextSeqBuf[:], newNextSeq)
	if err := batch.Set(metaNextSeqKey, nextSeqBuf[:], nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	s.nextSeq = newNextSeq
	s.usage.BucketBytes = newBucketBytes
	return nil
}
