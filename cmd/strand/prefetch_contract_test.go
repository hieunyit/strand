package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"strand/internal/model"
	"strand/internal/spool"
)

func TestPrefetchArchivePersistsOrderedIdentity(t *testing.T) {
	cases := []struct {
		name, mode string
		pk         []uint64
		key        []string
		expected   []model.Value
	}{
		{"binlog primary", "", []uint64{2, 0}, []string{"c", "a"}, []model.Value{int64(33), int64(11)}},
		{"sidecar unique", "unique", nil, []string{"c", "a"}, []model.Value{int64(33), int64(11)}},
		{"sidecar keyless", "keyless", nil, []string{"a", "b", "c"}, []model.Value{int64(11), int64(22), int64(33)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			archiveDir, spoolDir := t.TempDir(), t.TempDir()
			columns := []string{"a", "b", "c"}
			types := []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}
			writePrefetchBinlog(t, archiveDir, "binlog.000001", makePrefetchFDE(), makePrefetchGTID(prefetchTestSID, 2),
				makePrefetchTableMap(7, "app", "t", types, columns, tc.pk), makePrefetchWriteRows(7, 3, [][]int32{{11, 22, 33}}), makePrefetchXID(1))
			manifest := &seedManifestLite{P: "00010203-0405-0607-0809-0a0b0c0d0e0f:1", Tables: []string{"app.t"}}
			mode := tc.mode
			if mode != "" {
				sidecar := map[string]any{"tables": map[string]any{"app.t": map[string]any{"schema": "app", "table": "t", "columns": columns, "identity_mode": mode, "key_columns": tc.key}}}
				data, err := json.Marshal(sidecar)
				if err != nil {
					t.Fatal(err)
				}
				manifest.ArchiveSchema = filepath.Join(t.TempDir(), "schema.json")
				if err := os.WriteFile(manifest.ArchiveSchema, data, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				mode = "pk"
			}
			if n, err := prefetch(context.Background(), manifest, spoolDir, 0, 0, "", archiveDir, "", nil); err != nil || n != 1 {
				t.Fatalf("prefetch n=%d: %v", n, err)
			}
			sp, err := spool.New(spoolDir)
			if err != nil {
				t.Fatal(err)
			}
			defer sp.Close()
			it, err := sp.Iter(context.Background(), model.Pos{})
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			txn, err := it.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := []model.ArchiveIdentity{{Table: model.TableID{Schema: "app", Table: "t"}, Mode: mode, Columns: columns, KeyColumns: tc.key}}
			if txn.PrefetchOrigin != "archive" || !reflect.DeepEqual(txn.ArchiveIdentities, want) {
				t.Fatalf("origin=%q identities=%+v want=%+v", txn.PrefetchOrigin, txn.ArchiveIdentities, want)
			}
			if len(txn.Rows) != 1 || !reflect.DeepEqual(txn.Rows[0].Key, tc.expected) {
				t.Fatalf("rows=%+v, expected key %v", txn.Rows, tc.expected)
			}
			if _, err := it.Next(context.Background()); err != io.EOF {
				t.Fatalf("expected EOF, got %v", err)
			}
		})
	}
}
