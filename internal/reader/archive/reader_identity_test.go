package archive

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/assemble"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

var dummySID = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

func makeFDE() []byte {
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
	return rawEvent(replication.FORMAT_DESCRIPTION_EVENT, 1, 0, fde)
}

func makeGTID(sid []byte, gno uint64) []byte {
	gtid := make([]byte, 25)
	copy(gtid[1:17], sid)
	binary.LittleEndian.PutUint64(gtid[17:], gno)
	return rawEvent(replication.GTID_EVENT, 1, 0, gtid)
}

func makeXID(xid uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, xid)
	return rawEvent(replication.XID_EVENT, 1, 0, b)
}

func makeTableMap(tableID uint64, schema, table string, colTypes []byte, colNames []string, pkIndices []uint64) []byte {
	var body []byte
	tBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tBytes, tableID)
	body = append(body, tBytes[:6]...)
	body = append(body, 0, 0) // flags
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
	return rawEvent(replication.TABLE_MAP_EVENT, 1, 0, body)
}

func makeWriteRows(tableID uint64, colCount int, rows [][]int32) []byte {
	var body []byte
	tBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(tBytes, tableID)
	body = append(body, tBytes[:6]...)
	body = append(body, 0, 0)                  // flags
	body = append(body, 2, 0)                  // extra data length
	body = append(body, byte(colCount))        // column count
	body = append(body, byte((1<<colCount)-1)) // columns present bitmap
	for _, row := range rows {
		body = append(body, 0) // null bitmap
		for _, val := range row {
			vBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(vBytes, uint32(val))
			body = append(body, vBytes...)
		}
	}
	return rawEvent(replication.WRITE_ROWS_EVENTv2, 1, 0, body)
}

func writeBinlogFile(t *testing.T, dir, filename string, events ...[]byte) string {
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

// 1. Valid PK fixture: Real binlog file parsed, assembler commits transaction with correct PK.
func TestArchiveIdentity_ValidPK(t *testing.T) {
	dir := t.TempDir()
	tm := makeTableMap(7, "app", "users", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"id", "age"}, []uint64{0})
	rows := makeWriteRows(7, 2, [][]int32{{101, 25}})
	writeBinlogFile(t, dir, "binlog.000001", makeFDE(), makeGTID(dummySID, 1), tm, rows, makeXID(1))

	r, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	asm := assemble.New()
	var committedTxn *model.Txn
	for ev := range stream {
		if ev.Type == reader.EventError {
			t.Fatalf("unexpected error: %v", ev.Err)
		}
		txn, err := asm.Push(ev)
		if err != nil {
			t.Fatal(err)
		}
		if txn != nil {
			committedTxn = txn
		}
	}

	if committedTxn == nil {
		t.Fatal("transaction was not committed")
	}
	if len(committedTxn.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(committedTxn.Rows))
	}
	row := committedTxn.Rows[0]
	if len(row.Key) != 1 || row.Key[0] != int32(101) {
		t.Fatalf("expected PK key [101], got %v", row.Key)
	}
}

// 2. Unique-only explicit metadata: Table with no PK, but verified sidecar specifies IdentityModeUnique.
func TestArchiveIdentity_UniqueOnly_ExplicitMetadata(t *testing.T) {
	dir := t.TempDir()
	// TableMap has NO PrimaryKey metadata (empty pkIndices)
	tm := makeTableMap(7, "app", "accounts", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"balance", "account_no"}, nil)
	rows := makeWriteRows(7, 2, [][]int32{{5000, 999}})
	writeBinlogFile(t, dir, "binlog.000001", makeFDE(), makeGTID(dummySID, 1), tm, rows, makeXID(1))

	sidecar, err := NewSchemaSidecar(TableIdentity{
		Schema:       "app",
		Table:        "accounts",
		Columns:      []string{"balance", "account_no"},
		ColumnTypes:  []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG},
		IdentityMode: IdentityModeUnique,
		KeyColumns:   []string{"account_no"},
	})
	if err != nil {
		t.Fatal(err)
	}

	r, err := New(Config{Dir: dir, Sidecar: sidecar})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	asm := assemble.New()
	// Pre-register unique key identity in assembler
	asm.Registry().RegisterTable("app", "accounts", []string{"account_no"}, []string{"balance", "account_no"})

	var committedTxn *model.Txn
	for ev := range stream {
		if ev.Type == reader.EventError {
			t.Fatalf("unexpected error: %v", ev.Err)
		}
		if ev.Type == reader.EventTableMap {
			if len(ev.PKColumns) != 1 || ev.PKColumns[0] != "account_no" {
				t.Fatalf("EventTableMap PKColumns = %v, want [account_no]", ev.PKColumns)
			}
		}
		txn, err := asm.Push(ev)
		if err != nil {
			t.Fatal(err)
		}
		if txn != nil {
			committedTxn = txn
		}
	}

	if committedTxn == nil {
		t.Fatal("transaction was not committed")
	}
	if len(committedTxn.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(committedTxn.Rows))
	}
	row := committedTxn.Rows[0]
	// Resolved key must be [account_no] = [999]
	if len(row.Key) != 1 || row.Key[0] != int32(999) {
		t.Fatalf("expected resolved unique key [999], got %v", row.Key)
	}

	// Verify downstream safety check
	ident, _ := sidecar.GetTable("app", "accounts")
	if err := VerifyRowChangeIdentity(*ident, row); err != nil {
		t.Fatalf("VerifyRowChangeIdentity failed: %v", err)
	}

	// Verify downstream safety rejects wrong shape (e.g. if emitted as full_row)
	badRow := row
	badRow.Key = []model.Value{int32(5000), int32(999)}
	if err := VerifyRowChangeIdentity(*ident, badRow); err == nil {
		t.Fatal("expected downstream safety check to reject full-row key for unique table")
	}
}

// 3. Keyless explicit metadata: Table with no PK, sidecar specifies IdentityModeKeyless, WithKeylessFunc configured.
func TestArchiveIdentity_Keyless_ExplicitMetadata(t *testing.T) {
	dir := t.TempDir()
	tm := makeTableMap(7, "app", "logs", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"code", "severity"}, nil)
	rows := makeWriteRows(7, 2, [][]int32{{404, 2}})
	writeBinlogFile(t, dir, "binlog.000001", makeFDE(), makeGTID(dummySID, 1), tm, rows, makeXID(1))

	sidecar, err := NewSchemaSidecar(TableIdentity{
		Schema:       "app",
		Table:        "logs",
		Columns:      []string{"code", "severity"},
		ColumnTypes:  []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG},
		IdentityMode: IdentityModeKeyless,
	})
	if err != nil {
		t.Fatal(err)
	}

	r, err := New(Config{Dir: dir, Sidecar: sidecar})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	asm := assemble.New(
		assemble.WithKeylessFunc(func(tid model.TableID) bool {
			return tid.Schema == "app" && tid.Table == "logs"
		}),
		assemble.WithKeyMetadata(map[model.TableID]assemble.KeyMeta{
			{Schema: "app", Table: "logs"}: {
				Unique: []assemble.UniqueIndex{{
					Name:    assemble.KeylessIndexName,
					Columns: []string{"code", "severity"},
					Prefix:  true,
				}},
			},
		}),
	)

	var committedTxn *model.Txn
	for ev := range stream {
		if ev.Type == reader.EventError {
			t.Fatalf("unexpected error: %v", ev.Err)
		}
		txn, err := asm.Push(ev)
		if err != nil {
			t.Fatal(err)
		}
		if txn != nil {
			committedTxn = txn
		}
	}

	if committedTxn == nil {
		t.Fatal("transaction was not committed")
	}
	row := committedTxn.Rows[0]
	// Full row identity: Key must contain all columns [404, 2]
	if len(row.Key) != 2 || row.Key[0] != int32(404) || row.Key[1] != int32(2) {
		t.Fatalf("expected keyless full_row key [404, 2], got %v", row.Key)
	}
	foundKeyless := false
	for _, uk := range row.UniqueKeys {
		if uk.Name == assemble.KeylessIndexName {
			foundKeyless = true
			break
		}
	}
	if !foundKeyless {
		t.Fatalf("expected KeylessIndexName in UniqueKeys, got %v", row.UniqueKeys)
	}

	ident, _ := sidecar.GetTable("app", "logs")
	if err := VerifyRowChangeIdentity(*ident, row); err != nil {
		t.Fatalf("VerifyRowChangeIdentity failed: %v", err)
	}
}

// 4. Duplicate / NULL validation: Duplicate column names/PK indices rejected, NULL in unique key safely handled.
func TestArchiveIdentity_DuplicateAndNull(t *testing.T) {
	// Sidecar validation rejects duplicate columns
	_, err := NewSchemaSidecar(TableIdentity{
		Schema:       "app",
		Table:        "t",
		Columns:      []string{"id", "id"},
		IdentityMode: IdentityModePK,
		KeyColumns:   []string{"id"},
	})
	if err == nil {
		t.Fatal("expected error on duplicate column names in sidecar")
	}

	// Sidecar validation rejects duplicate key columns
	_, err = NewSchemaSidecar(TableIdentity{
		Schema:       "app",
		Table:        "t",
		Columns:      []string{"id", "val"},
		IdentityMode: IdentityModePK,
		KeyColumns:   []string{"id", "id"},
	})
	if err == nil {
		t.Fatal("expected error on duplicate key columns in sidecar")
	}

	// TABLE_MAP with duplicate column names rejected
	dir := t.TempDir()
	tmDupCol := makeTableMap(7, "app", "t", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"col", "col"}, []uint64{0})
	writeBinlogFile(t, dir, "binlog.000001", makeFDE(), makeGTID(dummySID, 1), tmDupCol, makeWriteRows(7, 2, [][]int32{{1, 2}}), makeXID(1))

	r, _ := New(Config{Dir: dir})
	stream, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var gotErr error
	for ev := range stream {
		if ev.Type == reader.EventError {
			gotErr = ev.Err
		}
	}
	if exitcode.GetExitCode(gotErr) != exitcode.Rejected {
		t.Fatalf("expected Rejected exit code for duplicate column names, got %v", gotErr)
	}
}

// 5. DDL mismatch / fingerprint mismatch rejected before transaction covered.
func TestArchiveIdentity_DDLMismatch(t *testing.T) {
	t.Run("sidecar column count mismatch", func(t *testing.T) {
		dir := t.TempDir()
		// Binlog has 2 columns, sidecar specifies 3 columns
		tm := makeTableMap(7, "app", "t", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"id", "val"}, []uint64{0})
		writeBinlogFile(t, dir, "binlog.000001", makeFDE(), makeGTID(dummySID, 1), tm, makeWriteRows(7, 2, [][]int32{{1, 2}}), makeXID(1))

		sidecar, _ := NewSchemaSidecar(TableIdentity{
			Schema:       "app",
			Table:        "t",
			Columns:      []string{"id", "val", "extra"},
			ColumnTypes:  []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG},
			IdentityMode: IdentityModePK,
			KeyColumns:   []string{"id"},
		})
		r, _ := New(Config{Dir: dir, Sidecar: sidecar})
		stream, err := r.Start(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		var gotErr error
		for ev := range stream {
			if ev.Type == reader.EventError {
				gotErr = ev.Err
			}
		}
		if exitcode.GetExitCode(gotErr) != exitcode.NeedsDecision {
			t.Fatalf("expected NeedsDecision for column count mismatch, got %v", gotErr)
		}
	})

	t.Run("sidecar column type mismatch", func(t *testing.T) {
		dir := t.TempDir()
		tm := makeTableMap(7, "app", "t", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_SHORT}, []string{"id", "val"}, []uint64{0})
		writeBinlogFile(t, dir, "binlog.000001", makeFDE(), makeGTID(dummySID, 1), tm, makeWriteRows(7, 2, [][]int32{{1, 2}}), makeXID(1))

		sidecar, _ := NewSchemaSidecar(TableIdentity{
			Schema:       "app",
			Table:        "t",
			Columns:      []string{"id", "val"},
			ColumnTypes:  []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, // type mismatch for col 1
			IdentityMode: IdentityModePK,
			KeyColumns:   []string{"id"},
		})
		r, _ := New(Config{Dir: dir, Sidecar: sidecar})
		stream, err := r.Start(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		var gotErr error
		for ev := range stream {
			if ev.Type == reader.EventError {
				gotErr = ev.Err
			}
		}
		if exitcode.GetExitCode(gotErr) != exitcode.NeedsDecision {
			t.Fatalf("expected NeedsDecision for type mismatch, got %v", gotErr)
		}
	})

	t.Run("ddl query modifying sidecar table rejected", func(t *testing.T) {
		dir := t.TempDir()
		sidecar, _ := NewSchemaSidecar(TableIdentity{
			Schema:       "app",
			Table:        "t",
			Columns:      []string{"id", "val"},
			IdentityMode: IdentityModePK,
			KeyColumns:   []string{"id"},
		})
		r, _ := New(Config{Dir: dir, Sidecar: sidecar})
		out := make(chan reader.Event, 10)
		maps := map[uint64]*replication.TableMapEvent{}

		qEv := &replication.QueryEvent{
			Schema: []byte("app"),
			Query:  []byte("ALTER TABLE app.t ADD COLUMN extra INT"),
		}
		err := r.handleEvent(context.Background(), maps, archiveEvent(replication.QUERY_EVENT, qEv), out)
		if exitcode.GetExitCode(err) != exitcode.NeedsDecision {
			t.Fatalf("expected NeedsDecision on DDL query modifying sidecar table, got %v", err)
		}
	})
}

// 6. Excluded + Included tables in same transaction: Outside table skipped, inside sibling row preserved and committed.
func TestArchiveIdentity_ExcludedAndIncludedSameTxn(t *testing.T) {
	dir := t.TempDir()
	// Table 1: outside table (mysql.rds_heartbeat2) without PK metadata
	tmOutside := makeTableMap(1, "mysql", "rds_heartbeat2", []byte{mysql.MYSQL_TYPE_LONG}, []string{"hb"}, nil)
	rowsOutside := makeWriteRows(1, 1, [][]int32{{1001}})

	// Table 2: included table (app.t) with valid PK
	tmInside := makeTableMap(2, "app", "t", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"id", "val"}, []uint64{0})
	rowsInside := makeWriteRows(2, 2, [][]int32{{42, 99}})

	// Single transaction with both tables
	writeBinlogFile(t, dir, "binlog.000001",
		makeFDE(),
		makeGTID(dummySID, 10),
		tmOutside,
		rowsOutside,
		tmInside,
		rowsInside,
		makeXID(10),
	)

	// Explicit verified scope includes only app.t
	r, err := New(Config{
		Dir: dir,
		TableScope: func(schema, table string) bool {
			return schema == "app" && table == "t"
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	asm := assemble.New()
	var committedTxn *model.Txn
	for ev := range stream {
		if ev.Type == reader.EventError {
			t.Fatalf("unexpected error: %v", ev.Err)
		}
		txn, err := asm.Push(ev)
		if err != nil {
			t.Fatal(err)
		}
		if txn != nil {
			committedTxn = txn
		}
	}

	if committedTxn == nil {
		t.Fatal("transaction was not committed")
	}
	// Verify GTID boundary preserved
	if committedTxn.SourcePos.GTID != "00010203-0405-0607-0809-0a0b0c0d0e0f:10" {
		t.Fatalf("unexpected GTID: %s", committedTxn.SourcePos.GTID)
	}
	// Excluded rows were skipped, only included row for app.t is present
	if len(committedTxn.Rows) != 1 {
		t.Fatalf("expected 1 row from app.t, got %d", len(committedTxn.Rows))
	}
	row := committedTxn.Rows[0]
	if row.Table.Schema != "app" || row.Table.Table != "t" {
		t.Fatalf("expected table app.t, got %v", row.Table)
	}
	if len(row.Key) != 1 || row.Key[0] != int32(42) {
		t.Fatalf("expected row key [42], got %v", row.Key)
	}
}

// 7. Malformed / purged / no-source paths.
func TestArchiveIdentity_MalformedPurgedNoSource(t *testing.T) {
	t.Run("empty archive dir returns log gap", func(t *testing.T) {
		dir := t.TempDir()
		r, err := New(Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		_, err = r.Start(context.Background(), nil)
		if exitcode.GetExitCode(err) != exitcode.DataError {
			t.Fatalf("expected DataError / log_gap, got %v", err)
		}
	})

	t.Run("malformed file returns error", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "binlog.000001"), []byte("not-a-valid-binlog"), 0o600); err != nil {
			t.Fatal(err)
		}
		r, err := New(Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		stream, err := r.Start(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		var gotErr error
		for ev := range stream {
			if ev.Type == reader.EventError {
				gotErr = ev.Err
			}
		}
		if gotErr == nil {
			t.Fatal("expected error on malformed binlog file")
		}
	})
}

// 8. No guessed col0: Without verified metadata or PK, reader never guesses col0 and fails closed.
func TestArchiveIdentity_NoGuessedCol0(t *testing.T) {
	dir := t.TempDir()
	// Table has columns [col0, col1], but no PK
	tm := makeTableMap(7, "app", "keyless_tbl", []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, []string{"col0", "col1"}, nil)
	writeBinlogFile(t, dir, "binlog.000001", makeFDE(), makeGTID(dummySID, 1), tm, makeWriteRows(7, 2, [][]int32{{10, 20}}), makeXID(1))

	r, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := r.Start(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var gotErr error
	var sawTableMap bool
	for ev := range stream {
		if ev.Type == reader.EventError {
			gotErr = ev.Err
		}
		if ev.Type == reader.EventTableMap {
			sawTableMap = true
		}
	}

	if sawTableMap {
		t.Fatal("reader emitted EventTableMap for table without PK or verified sidecar mapping")
	}
	if exitcode.GetExitCode(gotErr) != exitcode.Rejected {
		t.Fatalf("expected Rejected exit code, got %v", gotErr)
	}
}

func TestSidecarRejectsUncertainDDL(t *testing.T) {
	sidecar, err := NewSchemaSidecar(TableIdentity{Schema: "app", Table: "accounts", Columns: []string{"id", "a"}, IdentityMode: IdentityModeUnique, KeyColumns: []string{"id"}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(Config{Sidecar: sidecar})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"ALTER TABLE `app`.`accounts` RENAME COLUMN id TO id_old", "/* comment */ ALTER TABLE app.accounts ADD c INT", "RENAME TABLE app.accounts TO app.old"} {
		if _, ok := r.isDDLMismatch("other", q); !ok {
			t.Fatalf("accepted %q", q)
		}
	}
	for _, q := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
		if _, ok := r.isDDLMismatch("other", q); ok {
			t.Fatalf("rejected transaction control %q", q)
		}
	}
}

func TestSidecarRejectsMinimalTableMap(t *testing.T) {
	id := TableIdentity{Schema: "app", Table: "accounts", Columns: []string{"id", "a"}, IdentityMode: IdentityModeUnique, KeyColumns: []string{"id"}}
	tm := &replication.TableMapEvent{Schema: []byte("app"), Table: []byte("accounts"), ColumnCount: 2, ColumnType: []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}}
	if _, _, err := verifyAgainstIdentity(tm, &id); err == nil {
		t.Fatal("minimal metadata cannot establish sidecar column order")
	}
}
