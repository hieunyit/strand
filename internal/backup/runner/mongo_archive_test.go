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

// Bằng chứng sở hữu khi dọn restore lỗi: database của khối namespace và của mọi ns trong oplog (applyOps lồng
// nhau, renameCollection sang database khác). Archive bị cắt giữ những gì đã thấy; sai định dạng thì rỗng.
func TestMongoArchiveDatabasesCollectsEveryWrittenDatabase(t *testing.T) {
	marshal := func(v any) []byte {
		t.Helper()
		b, err := bson.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	end := binary.LittleEndian.AppendUint32(nil, mongoArchiveEnd)
	prelude := binary.LittleEndian.AppendUint32(nil, mongoArchiveMagic)
	prelude = append(prelude, marshal(bson.D{{Key: "version", Value: "0.1"}})...)
	oplog := [][]byte{
		marshal(bson.D{{Key: "ts", Value: bson.Timestamp{T: 1}}, {Key: "op", Value: "i"}, {Key: "ns", Value: "late.items"}}),
		marshal(bson.D{{Key: "ts", Value: bson.Timestamp{T: 2}}, {Key: "op", Value: "c"}, {Key: "ns", Value: "admin.$cmd"},
			{Key: "o", Value: bson.D{{Key: "applyOps", Value: bson.A{bson.D{{Key: "op", Value: "i"}, {Key: "ns", Value: "txn.orders"}}}}}}}),
		marshal(bson.D{{Key: "ts", Value: bson.Timestamp{T: 3}}, {Key: "op", Value: "c"}, {Key: "ns", Value: "app.$cmd"},
			{Key: "o", Value: bson.D{{Key: "renameCollection", Value: "app.a"}, {Key: "to", Value: "moved.a"}}}}),
	}
	archive := bytes.Join([][]byte{prelude, end,
		marshal(mongoNamespaceHeader{Database: "app", Collection: "a"}), marshal(bson.D{{Key: "x", Value: 1}}), end,
		marshal(mongoNamespaceHeader{Collection: "oplog"}), bytes.Join(oplog, nil), end,
	}, nil)
	var got mongoArchiveDatabases
	got.collect(bytes.NewReader(archive))
	for _, db := range []string{"app", "late", "txn", "moved", "admin"} {
		if !got.has(db) {
			t.Errorf("database %q of the archive not collected", db)
		}
	}
	if got.has("otherapp") {
		t.Error("collected a database the archive does not contain")
	}
	// Bị cắt giữa oplog (mongorestore dừng giữa chừng): những gì đã thấy vẫn là bằng chứng.
	var cut mongoArchiveDatabases
	cut.collect(bytes.NewReader(archive[:len(archive)-len(oplog[2])-len(end)-3]))
	if !cut.has("app") || !cut.has("late") || cut.has("moved") {
		t.Errorf("truncated archive: app=%v late=%v moved=%v", cut.has("app"), cut.has("late"), cut.has("moved"))
	}
	var bad mongoArchiveDatabases
	bad.collect(bytes.NewReader(append([]byte{1, 2, 3, 4}, archive[4:]...)))
	if bad.has("app") {
		t.Error("an archive with an invalid format proves nothing")
	}
}
