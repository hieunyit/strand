package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strand/internal/checkpoint"
	"strand/internal/spool"
	"testing"

	"strand/internal/model"
	"strand/internal/rowkey"
)

func identityPipeline(t *testing.T, mode string, key []string) *Pipeline {
	t.Helper()
	id := model.TableID{Schema: "app", Table: "items"}
	filter, err := newTableFilter([]string{"app.*"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	name := "uk"
	if mode == "pk" {
		name = rowkey.PrimaryName
	}
	return &Pipeline{filter: filter, tables: map[model.TableID]*tableMeta{id: {ID: id, KeyName: name, Keyless: mode == "keyless", PK: key, Columns: []string{"a", "b"}}}}
}
func boundTxn(origin, mode string, key []string) *model.Txn {
	id := model.TableID{Schema: "app", Table: "items"}
	values := make([]model.Value, len(key))
	for n := range values {
		values[n] = int64(n + 1)
	}
	return &model.Txn{PrefetchOrigin: origin, ArchiveIdentities: []model.ArchiveIdentity{{Table: id, Mode: mode, KeyColumns: key, Columns: []string{"a", "b"}}}, Rows: []model.RowChange{{Table: id, Op: model.OpDelete, Key: values}}}
}
func TestPrefetchConsumerChecksNamesOrderModeAndGeneration(t *testing.T) {
	for _, origin := range []string{"archive", "source"} {
		p := identityPipeline(t, "unique", []string{"b"})
		if _, err := p.archiveIdentityChecker(boundTxn(origin, "unique", []string{"b"}), true); err != nil {
			t.Fatal(err)
		}
		if _, err := p.archiveIdentityChecker(boundTxn(origin, "unique", []string{"a"}), true); err == nil {
			t.Fatal("equal-length different unique key accepted")
		}
		if _, err := p.archiveIdentityChecker(boundTxn(origin, "pk", []string{"b"}), true); err == nil {
			t.Fatal("wrong identity mode accepted")
		}
		txn := boundTxn(origin, "unique", []string{"b"})
		txn.ArchiveIdentities[0].Columns = []string{"b", "a"}
		if _, err := p.archiveIdentityChecker(txn, true); err == nil {
			t.Fatal("different table-map generation/order accepted")
		}
		p = identityPipeline(t, "unique", []string{"b", "a"})
		if _, err := p.archiveIdentityChecker(boundTxn(origin, "unique", []string{"a", "b"}), true); err == nil {
			t.Fatal("reversed composite key accepted")
		}
	}
}
func TestPrefetchConsumerRejectsLegacyAndRechecksMetadata(t *testing.T) {
	p := identityPipeline(t, "unique", []string{"b"})
	if _, err := p.archiveIdentityChecker(&model.Txn{}, true); err == nil {
		t.Fatal("unprovenanced legacy prefetch accepted")
	}
	if _, err := p.archiveIdentityChecker(&model.Txn{}, false); err != nil {
		t.Fatalf("normal live transaction rejected: %v", err)
	}
	txn := boundTxn("archive", "unique", []string{"b"})
	if _, err := p.archiveIdentityChecker(txn, true); err != nil {
		t.Fatal(err)
	}
	p.tables[txn.Rows[0].Table].PK = []string{"a"}
	if _, err := p.archiveIdentityChecker(txn, false); err == nil {
		t.Fatal("schema changed after initial scan without consumer rejection")
	}
}
func TestPrefetchConsumerFullRowAndSpilledRows(t *testing.T) {
	p := identityPipeline(t, "keyless", []string{"a", "b"})
	txn := boundTxn("archive", "keyless", []string{"a", "b"})
	path := filepath.Join(t.TempDir(), "rows.spill")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := model.NewSpillWriter(f)
	if err := w.Write(&txn.Rows[0]); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	txn.Rows = nil
	txn.SpillPath = path
	txn.RowCount = 1
	check, err := p.archiveIdentityChecker(txn, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.ForEachRow(check); err != nil {
		t.Fatalf("valid spilled full-row identity rejected: %v", err)
	}
	missing := model.RowChange{Table: model.TableID{Schema: "app", Table: "other"}, Op: model.OpDelete, Key: []model.Value{1}}
	if err := check(&missing); err == nil {
		t.Fatal("spilled row without identity contract accepted")
	}
}

func TestPrefetchReplayRejectsIdentityBeforeSendingRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := identityPipeline(t, "unique", []string{"b"})
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	txn := boundTxn("archive", "unique", []string{"a"})
	txn.SourcePos = model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}
	if err := sp.Append(ctx, txn); err != nil {
		t.Fatal(err)
	}
	p.prefetch = sp
	p.applied0 = checkpoint.NewMySQLAppliedSet()
	out := make(chan *model.Txn, 1)
	if err := p.drainPrefetch(ctx, out); err == nil {
		t.Fatal("identity mismatch replayed")
	}
	if len(out) != 0 {
		t.Fatal("invalid archive identity reached router")
	}
}
func TestPrefetchReplayRejectsUnverifiedLegacyBeforeSendingRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := identityPipeline(t, "pk", []string{"a"})
	sp, err := spool.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	txn := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}, Rows: []model.RowChange{{Table: model.TableID{Schema: "app", Table: "items"}, Op: model.OpDelete, Key: []model.Value{1}}}}
	if err := sp.Append(ctx, txn); err != nil {
		t.Fatal(err)
	}
	p.prefetch = sp
	p.applied0 = checkpoint.NewMySQLAppliedSet()
	out := make(chan *model.Txn, 1)
	if err := p.drainPrefetch(ctx, out); err == nil {
		t.Fatal("unbound legacy prefetch replayed")
	}
	if len(out) != 0 {
		t.Fatal("legacy record reached router")
	}
}

func TestPrefetchIdentityRejectsNullUniqueKeys(t *testing.T) {
	p := identityPipeline(t, "unique", []string{"b"})
	for _, null := range []model.Value{nil, []byte(nil)} {
		txn := boundTxn("archive", "unique", []string{"b"})
		txn.Rows[0].Key[0] = null
		if _, err := p.archiveIdentityChecker(txn, true); err == nil {
			t.Fatal("nullable unique value accepted as an identity")
		}
		txn = boundTxn("archive", "unique", []string{"b"})
		txn.Rows[0].OldKey = []model.Value{null}
		if _, err := p.archiveIdentityChecker(txn, true); err == nil {
			t.Fatal("NULL old unique key accepted")
		}
	}
	p = identityPipeline(t, "keyless", []string{"a", "b"})
	txn := boundTxn("archive", "keyless", []string{"a", "b"})
	txn.Rows[0].Key = []model.Value{nil, []byte{}}
	if _, err := p.archiveIdentityChecker(txn, true); err != nil {
		t.Fatalf("keyless NULL image must remain supported: %v", err)
	}
}
