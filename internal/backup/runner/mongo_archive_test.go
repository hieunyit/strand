package runner

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"io"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"strand/internal/backup"
)

func TestMongoArchiveVerifiesEmbeddedBoundaryAndContinuation(t *testing.T) {
	marshal := func(v any) []byte {
		t.Helper()
		b, err := bson.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	timestamp := func(sec, inc uint32) bson.Timestamp { return bson.Timestamp{T: sec, I: inc} }
	entry := func(sec, inc uint32) []byte { return marshal(bson.D{{Key: "ts", Value: timestamp(sec, inc)}}) }

	end := binary.LittleEndian.AppendUint32(nil, mongoArchiveEnd)
	prelude := binary.LittleEndian.AppendUint32(nil, mongoArchiveMagic)
	prelude = append(prelude, marshal(bson.D{{Key: "version", Value: "0.1"}})...)
	body := entry(10, 2)
	crc := crc64.Checksum(body, crc64.MakeTable(crc64.ECMA))
	archive := bytes.Join([][]byte{prelude, end, marshal(mongoNamespaceHeader{Collection: "oplog"}), body, end,
		marshal(mongoNamespaceHeader{Collection: "oplog", EOF: true, CRC: int64(crc)}), end}, nil)
	anchor := timestamp(10, 2)
	last, err := streamMongoArchive(bytes.NewReader(archive), io.Discard, anchor, nil)
	if err != nil || last != anchor {
		t.Fatalf("embedded boundary = %v, %v", last, err)
	}
	if _, err := streamMongoArchive(bytes.NewReader(archive), io.Discard, timestamp(10, 3), nil); err == nil {
		t.Fatal("catalog boundary that differs from the actual dump was accepted")
	}

	var output bytes.Buffer
	cutoff := timestamp(12, 0)
	segment := backup.Record{ID: "slice", Position: backup.Position{OplogTS: "10.1"}, EndPosition: backup.Position{OplogTS: "12.1"}}
	input := bytes.Join([][]byte{body, entry(11, 1), entry(12, 1)}, nil)
	if _, err := streamMongoArchive(bytes.NewReader(archive), &output, anchor, func(dst io.Writer) error {
		return copyMongoSlice(bytes.NewReader(input), dst, segment, anchor, cutoff)
	}); err != nil {
		t.Fatal(err)
	}
	last, err = streamMongoArchive(&output, io.Discard, timestamp(11, 1), nil)
	if err != nil || last != timestamp(11, 1) {
		t.Fatalf("continuation timestamp = %v, %v", last, err)
	}
	for name, bad := range map[string][]byte{
		"anchor missing": bytes.Join([][]byte{entry(11, 1), entry(12, 1)}, nil),
		"duplicate":      bytes.Join([][]byte{body, body, entry(12, 1)}, nil),
		"truncated":      input[:len(input)-1],
	} {
		t.Run(name, func(t *testing.T) {
			if err := copyMongoSlice(bytes.NewReader(bad), io.Discard, segment, anchor, cutoff); err == nil {
				t.Fatal("unverified oplog replay was accepted")
			}
		})
	}
}
