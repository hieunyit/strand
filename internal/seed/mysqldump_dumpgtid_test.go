package seed_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"

	"strand/internal/exitcode"
	"strand/internal/seed"
)

// Dump kiểu mysqldump 5.7 --single-transaction --master-data=2: CHANGE MASTER ở header, GTID_PURGED ở CUỐI file.
const dump57Trailer = `-- MySQL dump 10.13  Distrib 5.7.44, for Linux (x86_64)
--
-- Position to start replication or point-in-time recovery from
--
-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000004', MASTER_LOG_POS=1234;
--
-- Current Database: ` + "`app`" + `
--
CREATE DATABASE /*!32312 IF NOT EXISTS*/ ` + "`app`" + `;
USE ` + "`app`" + `;
CREATE TABLE t (id int);
INSERT INTO t VALUES (1);
--
-- GTID state at the end of the backup
--
SET @@GLOBAL.GTID_PURGED='3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50,
4e22fb58-82db-22f2-af44-d91bb0530673:1-9';
-- Dump completed on 2026-11-12 10:00:00
`

func writeDump(t *testing.T, name, content string, gz bool) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	data := []byte(content)
	if gz {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		_, _ = w.Write(data)
		_ = w.Close()
		data = b.Bytes()
	}
	if err := os.WriteFile(p, data, 0o640); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMysqldump_TrailerGTID_ParsedMultiLine(t *testing.T) {
	for _, gz := range []bool{false, true} {
		name := "d.sql"
		if gz {
			name = "d.sql.gz"
		}
		meta, err := seed.ParseMysqldumpMetadata(writeDump(t, name, dump57Trailer, gz))
		if err != nil {
			t.Fatalf("gz=%v: %v", gz, err)
		}
		if !meta.GTIDInTrailer || meta.BinlogFile != "mysql-bin.000004" || meta.BinlogPos != 1234 {
			t.Fatalf("gz=%v: meta = %+v", gz, meta)
		}
		if meta.GTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50,4e22fb58-82db-22f2-af44-d91bb0530673:1-9" {
			t.Fatalf("gz=%v: GTID = %q", gz, meta.GTID)
		}
	}
}

func TestMysqldump_TrailerGTID_UsesBinlogPosition(t *testing.T) {
	path := writeDump(t, "d.sql", dump57Trailer, false)
	const p = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-40"
	var gotFile string
	var gotPos uint64
	pl := &seed.MysqldumpPlanner{Resolver: func(_ context.Context, f string, pos uint64) (string, bool, error) {
		gotFile, gotPos = f, pos
		return p, true, nil
	}}
	// target sau load mang GTID_PURGED cuối file (⊇ P): phải được chấp nhận.
	m, err := pl.Plan(context.Background(), seed.SeedSource{BackupRef: path, TargetVersion: "8.4.3",
		TargetRestoredGTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50,4e22fb58-82db-22f2-af44-d91bb0530673:1-9"})
	if err != nil {
		t.Fatal(err)
	}
	if gotFile != "mysql-bin.000004" || gotPos != 1234 {
		t.Fatalf("resolver got %s:%d", gotFile, gotPos)
	}
	sp := m.StartPosition
	if sp.GTIDSet != p || !sp.GTIDFromBinlog || sp.BinlogFile != "mysql-bin.000004" || sp.BinlogPos != 1234 {
		t.Fatalf("start position = %+v", sp)
	}
	if !strings.Contains(strings.Join(m.Warnings, "\n"), "was ignored") {
		t.Fatalf("missing note about ignored trailer GTID: %v", m.Warnings)
	}
	// target thiếu GTID của P -> từ chối.
	_, err = pl.Plan(context.Background(), seed.SeedSource{BackupRef: path, TargetVersion: "8.4.3",
		TargetRestoredGTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10"})
	if err == nil || !strings.Contains(err.Error(), "does not contain backup P") {
		t.Fatalf("want superset cross-check error, got %v", err)
	}
}

func TestMysqldump_TrailerGTID_NoSourceRejected(t *testing.T) {
	_, err := seed.NewMysqldumpPlanner().Plan(context.Background(),
		seed.SeedSource{BackupRef: writeDump(t, "d.sql", dump57Trailer, false)})
	if err == nil || !strings.Contains(err.Error(), "pass --source") {
		t.Fatalf("got %v", err)
	}
}

func TestMysqldump_TrailerGTID_PurgedBinlogIsLogGap(t *testing.T) {
	gap := exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "log gap: binlog purged", nil)
	pl := &seed.MysqldumpPlanner{Resolver: func(context.Context, string, uint64) (string, bool, error) { return "", true, gap }}
	_, err := pl.Plan(context.Background(), seed.SeedSource{BackupRef: writeDump(t, "d.sql", dump57Trailer, false)})
	if err == nil || exitcode.GetExitCode(err) != exitcode.DataError {
		t.Fatalf("got %v", err)
	}
}

func TestMysqldump_GTIDLessSource_RecordsPositionOnly(t *testing.T) {
	dump := `-- MySQL dump 10.13  Distrib 5.5.62, for Linux (x86_64)
-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000002', MASTER_LOG_POS=107;
CREATE TABLE t (id int);
-- Dump completed on 2026-11-12 10:00:00
`
	pl := &seed.MysqldumpPlanner{Resolver: func(context.Context, string, uint64) (string, bool, error) { return "", false, nil }}
	m, err := pl.Plan(context.Background(), seed.SeedSource{BackupRef: writeDump(t, "d.sql", dump, false), TargetVersion: "8.4.3"})
	if err != nil {
		t.Fatal(err)
	}
	if m.StartPosition.GTIDSet != "" || m.StartPosition.BinlogFile != "mysql-bin.000002" || m.StartPosition.BinlogPos != 107 {
		t.Fatalf("start position = %+v", m.StartPosition)
	}
	if !strings.Contains(strings.Join(m.Warnings, "\n"), "binlog position mode") {
		t.Fatalf("missing position-mode note: %v", m.Warnings)
	}
}

func TestMysqldump_HeaderGTID80_KeepsHeaderAndSkipsBinlog(t *testing.T) {
	dump := `-- MySQL dump 10.13  Distrib 8.0.36, for Linux (x86_64)
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77';
-- CHANGE MASTER TO MASTER_LOG_FILE='binlog.000003', MASTER_LOG_POS=154;
CREATE TABLE t (id int);
-- Dump completed on 2026-11-12 10:00:00
`
	pl := &seed.MysqldumpPlanner{Resolver: func(context.Context, string, uint64) (string, bool, error) {
		t.Fatal("resolver must not be called when the header has GTID_PURGED")
		return "", false, nil
	}}
	m, err := pl.Plan(context.Background(), seed.SeedSource{BackupRef: writeDump(t, "d.sql", dump, false), TargetVersion: "8.4.3"})
	if err != nil {
		t.Fatal(err)
	}
	if m.StartPosition.GTIDSet != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77" || m.StartPosition.GTIDFromBinlog {
		t.Fatalf("start position = %+v", m.StartPosition)
	}
}

// ---- GTIDAtBinlogPosition với event tổng hợp ----

const hdrSize = replication.EventHeaderSize

func evt(typ replication.EventType, body []byte, endPos uint32) *replication.BinlogEvent {
	raw := make([]byte, hdrSize+len(body))
	copy(raw[hdrSize:], body)
	return &replication.BinlogEvent{
		RawData: raw,
		Header:  &replication.EventHeader{Timestamp: 1, EventType: typ, EventSize: uint32(len(raw)), LogPos: endPos},
	}
}

func gtidEv(u uuid.UUID, gno uint64, endPos uint32) *replication.BinlogEvent {
	b := make([]byte, 25)
	b[0] = 1
	copy(b[1:17], u[:])
	binary.LittleEndian.PutUint64(b[17:], gno)
	return evt(replication.GTID_EVENT, b, endPos)
}

func prevGTIDsEv(u uuid.UUID, lo, hiExcl uint64, endPos uint32) *replication.BinlogEvent {
	b := make([]byte, 0, 48)
	b = binary.LittleEndian.AppendUint64(b, 1)
	b = append(b, u[:]...)
	b = binary.LittleEndian.AppendUint64(b, 1)
	b = binary.LittleEndian.AppendUint64(b, lo)
	b = binary.LittleEndian.AppendUint64(b, hiExcl)
	return evt(replication.PREVIOUS_GTIDS_EVENT, b, endPos)
}

func feeder(evs []*replication.BinlogEvent) func() (*replication.BinlogEvent, error) {
	i := 0
	return func() (*replication.BinlogEvent, error) {
		if i >= len(evs) {
			return nil, errors.New("event stream exhausted (would block)")
		}
		e := evs[i]
		i++
		return e, nil
	}
}

func TestGTIDAtBinlogPosition_Synthetic(t *testing.T) {
	u := uuid.MustParse("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	fake := evt(replication.ROTATE_EVENT, nil, 0) // rotate giả: LogPos=0
	fake.Header.Timestamp = 0
	stream := func() []*replication.BinlogEvent {
		g := func(gno uint64, start uint32) []*replication.BinlogEvent {
			// mỗi txn: GTID(30) + QUERY BEGIN + ROWS + XID; GTID bắt đầu tại start.
			return []*replication.BinlogEvent{
				gtidEv(u, gno, start+44),
				evt(replication.QUERY_EVENT, make([]byte, 10), start+44+29),
				evt(replication.XID_EVENT, make([]byte, 8), start+44+29+27),
			}
		}
		evs := []*replication.BinlogEvent{fake, evt(replication.FORMAT_DESCRIPTION_EVENT, make([]byte, 4), 123),
			prevGTIDsEv(u, 1, 11, 200)} // đã có 1-10 trước file
		pos := uint32(200)
		for gno := uint64(11); gno <= 15; gno++ {
			evs = append(evs, g(gno, pos)...)
			pos += 44 + 29 + 27
		}
		return evs
	}
	txnLen := uint64(44 + 29 + 27)
	cases := []struct {
		name string
		pos  uint64
		want string
	}{
		{"before-any-txn", 200, u.String() + ":1-10"},
		{"after-two", 200 + 2*txnLen, u.String() + ":1-12"},
		{"after-all", 200 + 5*txnLen, u.String() + ":1-15"},
	}
	for _, c := range cases {
		got, err := seed.GTIDAtBinlogPosition("f.000001", c.pos, feeder(stream()))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Fatalf("%s: got %q want %q", c.name, got, c.want)
		}
	}
	// vị trí sau cuối file: gặp rotate thật -> lỗi rõ ràng
	evs := append(stream(), evt(replication.ROTATE_EVENT, []byte{}, 9999))
	if _, err := seed.GTIDAtBinlogPosition("f.000001", 1_000_000, feeder(evs)); err == nil || err == io.EOF {
		t.Fatalf("want beyond-end error, got %v", err)
	}
}
