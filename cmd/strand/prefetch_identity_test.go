package main

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/exitcode"
)

var prefetchTestSID = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

func makeRawEvent(t replication.EventType, ts, logPos uint32, body []byte) []byte {
	b := make([]byte, replication.EventHeaderSize, replication.EventHeaderSize+len(body))
	binary.LittleEndian.PutUint32(b[0:], ts)
	b[4] = byte(t)
	binary.LittleEndian.PutUint32(b[5:], 1)
	binary.LittleEndian.PutUint32(b[9:], uint32(replication.EventHeaderSize+len(body)))
	binary.LittleEndian.PutUint32(b[13:], logPos)
	return append(b, body...)
}

func makePrefetchFDE() []byte {
	fde := make([]byte, 57+40)
	binary.LittleEndian.PutUint16(fde, 4)
	copy(fde[2:52], "5.5.0")
	fde[56] = replication.EventHeaderSize
	for i := 57; i < len(fde); i++ {
		fde[i] = 8
	}
	fde[57+int(replication.WRITE_ROWS_EVENTv2)-1] = 10
	fde[57+int(replication.UPDATE_ROWS_EVENTv2)-1] = 10
	fde[57+int(replication.PARTIAL_UPDATE_ROWS_EVENT)-1] = 10
	return makeRawEvent(replication.FORMAT_DESCRIPTION_EVENT, 1, 0, fde)
}

func makePrefetchGTID(sid []byte, gno uint64) []byte {
	gtid := make([]byte, 25)
	copy(gtid[1:17], sid)
	binary.LittleEndian.PutUint64(gtid[17:], gno)
	return makeRawEvent(replication.GTID_EVENT, 1, 0, gtid)
}

func makePrefetchXID(xid uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, xid)
	return makeRawEvent(replication.XID_EVENT, 1, 0, b)
}

func makePrefetchTableMap(tableID uint64, schema, table string, colTypes []byte, colNames []string, pkIndices []uint64) []byte {
	var body []byte
	tBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tBytes, tableID)
	body = append(body, tBytes[:6]...)
	body = append(body, 0, 0)
	body = append(body, byte(len(schema)))
	body = append(body, []byte(schema)...)
	body = append(body, 0)
	body = append(body, byte(len(table)))
	body = append(body, []byte(table)...)
	body = append(body, 0)
	body = append(body, byte(len(colTypes)))
	body = append(body, colTypes...)
	body = append(body, byte(len(colTypes)))
	for range colTypes {
		body = append(body, 0)
	}
	nullBytes := (len(colTypes) + 7) / 8
	for i := 0; i < nullBytes; i++ {
		body = append(body, 0)
	}
	if len(colNames) > 0 {
		var nameData []byte
		for _, name := range colNames {
			nameData = append(nameData, byte(len(name)))
			nameData = append(nameData, []byte(name)...)
		}
		body = append(body, byte(replication.TABLE_MAP_OPT_META_COLUMN_NAME))
		body = append(body, byte(len(nameData)))
		body = append(body, nameData...)
	}
	if len(pkIndices) > 0 {
		var pkData []byte
		for _, idx := range pkIndices {
			pkData = append(pkData, byte(idx))
		}
		body = append(body, byte(replication.TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY))
		body = append(body, byte(len(pkData)))
		body = append(body, pkData...)
	}
	return makeRawEvent(replication.TABLE_MAP_EVENT, 1, 0, body)
}

func makePrefetchWriteRows(tableID uint64, colCount int, rows [][]int32) []byte {
	var body []byte
	tBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tBytes, tableID)
	body = append(body, tBytes[:6]...)
	body = append(body, 0, 0)
	body = append(body, 2, 0)
	body = append(body, byte(colCount))
	body = append(body, byte((1<<colCount)-1))
	for _, row := range rows {
		body = append(body, 0)
		for _, val := range row {
			vBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(vBytes, uint32(val))
			body = append(body, vBytes...)
		}
	}
	return makeRawEvent(replication.WRITE_ROWS_EVENTv2, 1, 0, body)
}

func writePrefetchBinlog(t *testing.T, dir, filename string, events ...[]byte) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	data := append([]byte{}, replication.BinLogFileHeader...)
	for _, ev := range events {
		data = append(data, ev...)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// 1. Concrete issue: Archive error with no usable source fallback must return error, not report success.
func TestPrefetch_ArchiveErrorNoSourceFallbackFails(t *testing.T) {
	spoolDir := t.TempDir()
	archiveDir := t.TempDir() // Empty archive directory causes ErrLogGap

	m := &seedManifestLite{P: "00010203-0405-0607-0809-0a0b0c0d0e0f:1"}

	// When sourceDSN == "", archive failure must return error immediately
	_, err := prefetch(context.Background(), m, spoolDir, 0, 0, "", archiveDir, "", nil)
	if err == nil {
		t.Fatal("expected prefetch to return error when archive fails with no source fallback, got nil")
	}
}

// 2. Concrete issue: EOF while inTxn must not mark partial transaction covered.
func TestPrefetch_EOFInTxnFails(t *testing.T) {
	spoolDir := t.TempDir()
	archiveDir := t.TempDir()

	// Binlog file cuts off after WRITE_ROWS without XID / COMMIT (EOF while inTxn)
	tm := makePrefetchTableMap(7, "app", "t", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"id", "v"}, []uint64{0})
	rows := makePrefetchWriteRows(7, 2, [][]int32{{1, 42}})
	writePrefetchBinlog(t, archiveDir, "binlog.000001",
		makePrefetchFDE(),
		makePrefetchGTID(prefetchTestSID, 2),
		tm,
		rows,
		// Omit XID to trigger EOF inTxn
	)

	m := &seedManifestLite{P: "00010203-0405-0607-0809-0a0b0c0d0e0f:1"}
	_, err := prefetch(context.Background(), m, spoolDir, 0, 0, "", archiveDir, "", nil)
	if err == nil {
		t.Fatal("expected error when event stream closes while inside transaction, got nil")
	}
	if exitcode.GetExitCode(err) != exitcode.DataError {
		t.Fatalf("expected DataError exit code, got %v", err)
	}
}

// 3. Prefetch with valid archive and sidecar writes transactions to spool.
func TestPrefetch_ValidArchiveWithSidecar(t *testing.T) {
	spoolDir := t.TempDir()
	archiveDir := t.TempDir()

	tm := makePrefetchTableMap(7, "app", "t", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"id", "v"}, []uint64{0})
	rows := makePrefetchWriteRows(7, 2, [][]int32{{10, 88}})
	writePrefetchBinlog(t, archiveDir, "binlog.000001",
		makePrefetchFDE(),
		makePrefetchGTID(prefetchTestSID, 2),
		tm,
		rows,
		makePrefetchXID(1),
	)

	sidecarJSON := `{
		"tables": {
			"app.t": {
				"schema": "app",
				"table": "t",
				"columns": ["id", "v"],
				"column_types": [3, 3],
				"identity_mode": "pk",
				"key_columns": ["id"]
			}
		}
	}`
	sidecarPath := filepath.Join(t.TempDir(), "sidecar.json")
	if err := os.WriteFile(sidecarPath, []byte(sidecarJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	m := &seedManifestLite{
		P:             "00010203-0405-0607-0809-0a0b0c0d0e0f:1",
		Tables:        []string{"app.t"},
		ArchiveSchema: sidecarPath,
	}

	n, err := prefetch(context.Background(), m, spoolDir, 0, 0, "", archiveDir, "", nil)
	if err != nil {
		t.Fatalf("prefetch failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 prefetched transaction, got %d", n)
	}
}

func TestPrefetchSourceOnlyIgnoresSQLSnapshot(t *testing.T) {
	p := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(p, []byte("CREATE TABLE t (id int);"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prefetch(context.Background(), &seedManifestLite{SchemaSnapshot: p}, t.TempDir(), 0, 0, "", "", "", nil); err != nil {
		t.Fatal(err)
	}
}
