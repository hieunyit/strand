package runner

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"time"

	"strand/internal/backup"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// MongoDB Database Tools archive v0.1: a magic word, BSON prelude ending in
// 0xffffffff, then namespace blocks consisting of a header, documents and
// the same terminator. The special namespace ("", "oplog") carries the log.
const mongoArchiveMagic = 0x8199e26d
const mongoArchiveEnd = 0xffffffff

type mongoNamespaceHeader struct {
	Database   string `bson:"db"`
	Collection string `bson:"collection"`
	EOF        bool   `bson:"EOF"`
	CRC        int64  `bson:"CRC"`
}

type mongoBSONReader struct {
	r   io.Reader
	buf []byte
}

func (r *mongoBSONReader) next() (bson.Raw, bool, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r.r, prefix[:]); err != nil {
		return nil, false, err
	}
	size := binary.LittleEndian.Uint32(prefix[:])
	if size == mongoArchiveEnd {
		return nil, true, nil
	}
	if size < 5 || size > 32<<20 {
		return nil, false, fmt.Errorf("invalid BSON length %d", size)
	}
	if cap(r.buf) < int(size) {
		r.buf = make([]byte, size)
	}
	doc := r.buf[:size]
	copy(doc, prefix[:])
	if _, err := io.ReadFull(r.r, doc[4:]); err != nil {
		return nil, false, fmt.Errorf("read BSON document: %w", err)
	}
	if doc[len(doc)-1] != 0 {
		return nil, false, errors.New("unterminated BSON document")
	}
	return bson.Raw(doc), false, nil
}

func mongoOplogTime(doc bson.Raw) (bson.Timestamp, error) {
	t, i, ok := doc.Lookup("ts").TimestampOK()
	if !ok || t == 0 {
		return bson.Timestamp{}, errors.New("oplog document has no valid BSON timestamp")
	}
	return bson.Timestamp{T: t, I: i}, nil
}

// streamMongoArchive copies the original archive and verifies its embedded
// oplog boundary. When supplied, extend writes continuation documents into
// one extra block before the oplog EOF, so the native restore retains one
// transaction buffer across the base/continuation boundary.
func streamMongoArchive(src io.Reader, dst io.Writer, expected bson.Timestamp, extend func(io.Writer) error) (bson.Timestamp, error) {
	var last bson.Timestamp
	var magic [4]byte
	if _, err := io.ReadFull(src, magic[:]); err != nil {
		return last, err
	}
	if binary.LittleEndian.Uint32(magic[:]) != mongoArchiveMagic {
		return last, errors.New("invalid mongodump archive magic")
	}
	if _, err := dst.Write(magic[:]); err != nil {
		return last, err
	}
	endWord := binary.LittleEndian.AppendUint32(nil, mongoArchiveEnd)
	reader := mongoBSONReader{r: src}
	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	for first := true; ; first = false {
		doc, end, err := reader.next()
		if err != nil {
			return last, fmt.Errorf("read mongodump prelude: %w", err)
		}
		version, _ := doc.Lookup("version").StringValueOK()
		if first && (end || version != "0.1") {
			return last, errors.New("unsupported mongodump archive version")
		}
		if end {
			if _, err := dst.Write(endWord); err != nil {
				return last, err
			}
			break
		}
		if _, err := dst.Write(doc); err != nil {
			return last, err
		}
	}
	oplogEOF := false
	for {
		doc, end, err := reader.next()
		if err == io.EOF {
			if !oplogEOF {
				return last, errors.New("mongodump archive is missing its oplog EOF")
			}
			return last, nil
		}
		if err != nil || end {
			return last, fmt.Errorf("invalid mongodump namespace header (terminator=%t): %v", end, err)
		}
		var header mongoNamespaceHeader
		if err := bson.Unmarshal(doc, &header); err != nil {
			return last, err
		}
		isOplog := header.Database == "" && header.Collection == "oplog"
		if isOplog && oplogEOF {
			return last, errors.New("unexpected block after oplog EOF")
		}
		// The BSON reader reuses its buffer. Copy the header before reading
		// the next record, except when the oplog EOF needs a new checksum.
		if !isOplog || !header.EOF {
			if _, err := dst.Write(doc); err != nil {
				return last, err
			}
		}
		for {
			doc, end, err := reader.next()
			if err != nil {
				return last, fmt.Errorf("read mongodump namespace body: %w", err)
			}
			if end {
				break
			}
			if header.EOF {
				return last, errors.New("unexpected document in namespace EOF block")
			}
			if isOplog {
				ts, err := mongoOplogTime(doc)
				if err != nil || ts.Compare(last) <= 0 {
					return last, errors.New("invalid timestamp order in mongodump oplog")
				}
				last = ts
				_, _ = crc.Write(doc)
			}
			if _, err := dst.Write(doc); err != nil {
				return last, err
			}
		}
		if isOplog && header.EOF {
			oplogEOF = true
			if crc.Sum64() != uint64(header.CRC) || (expected.T != 0 && expected != last) {
				return last, errors.New("mongodump oplog does not match its checksum or catalog boundary")
			}
			if extend != nil {
				block, err := bson.Marshal(mongoNamespaceHeader{Collection: "oplog"})
				if err != nil {
					return last, err
				}
				if _, err := dst.Write(block); err != nil {
					return last, err
				}
				if err := extend(io.MultiWriter(dst, crc)); err != nil {
					return last, err
				}
				if _, err := dst.Write(endWord); err != nil {
					return last, err
				}
				header.CRC = int64(crc.Sum64())
			}
			block, err := bson.Marshal(header)
			if err != nil {
				return last, err
			}
			if _, err := dst.Write(block); err != nil {
				return last, err
			}
		}
		if _, err := dst.Write(endWord); err != nil {
			return last, err
		}
	}
}

// MongoDB oplog timestamps have second precision and an ordinal, not a
// wall-clock fraction. The named UTC second is an exclusive cutoff.
func mongoRecoveryCutoff(target time.Time) (bson.Timestamp, error) {
	secs := target.Unix()
	if secs <= 0 || secs > int64(^uint32(0)) || target.Nanosecond() != 0 {
		return bson.Timestamp{}, errors.New("MongoDB PITR requires a whole-second UTC time in BSON timestamp range")
	}
	return bson.Timestamp{T: uint32(secs)}, nil
}

func (e *Env) stageMongoContinuation(ctx context.Context, plan RestorePlan, dst io.Writer, anchor, cutoff bson.Timestamp) error {
	var previous backup.Record
	for i, rec := range plan.Logs {
		if !samePITRLineage(rec, plan.Base()) || (i == 0 && !logCoversBase(rec, plan.Base())) ||
			(i > 0 && !Contiguous(previous, rec)) {
			return fmt.Errorf("unverified MongoDB oplog lineage at %s", rec.ID)
		}
		previous = rec
		body, err := e.openRecord(ctx, rec)
		if err != nil {
			return err
		}
		err = copyMongoSlice(body, dst, rec, anchor, cutoff)
		closeErr := body.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	end, err := ParseOplogTS(previous.EndPosition.OplogTS)
	if err != nil || end.Compare(cutoff) < 0 {
		return errors.New("MongoDB oplog archive does not prove coverage through the requested second")
	}
	return nil
}

func copyMongoSlice(src io.Reader, dst io.Writer, rec backup.Record, anchor, cutoff bson.Timestamp) error {
	start, err := ParseOplogTS(rec.Position.OplogTS)
	if err != nil {
		return err
	}
	end, err := ParseOplogTS(rec.EndPosition.OplogTS)
	if err != nil {
		return err
	}
	reader := mongoBSONReader{r: src}
	last := start
	anchorSeen := anchor.Compare(start) <= 0
	for {
		doc, terminator, err := reader.next()
		if err == io.EOF {
			if last == end && end.Compare(start) > 0 && anchorSeen {
				return nil
			}
			return fmt.Errorf("oplog slice %s does not prove its catalog endpoint and base coverage", rec.ID)
		}
		if err != nil || terminator {
			return fmt.Errorf("invalid BSON in oplog slice %s (terminator=%t): %v", rec.ID, terminator, err)
		}
		ts, err := mongoOplogTime(doc)
		if err != nil || ts.Compare(last) <= 0 || ts.Compare(end) > 0 {
			return fmt.Errorf("invalid BSON timestamp order in oplog slice %s", rec.ID)
		}
		last = ts
		anchorSeen = anchorSeen || ts == anchor
		if ts.Compare(anchor) > 0 && ts.Compare(cutoff) < 0 {
			if _, err := dst.Write(doc); err != nil {
				return err
			}
		}
	}
}
