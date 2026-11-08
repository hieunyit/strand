package assemble

import (
	"os"
	"path/filepath"
	"testing"

	"strand/internal/model"
)

func TestSpillFlushFailureRemovesFile(t *testing.T) {
	dir := t.TempDir()
	sb := NewSpillBuffer(1, dir)
	for i := 0; i < 2; i++ {
		if err := sb.Append(model.RowChange{Op: model.OpInsert, Key: []model.Value{int64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if sb.spillF == nil {
		t.Fatal("spill not created")
	}
	if err := sb.spillF.Close(); err != nil {
		t.Fatal(err)
	}
	var txn model.Txn
	if err := sb.Finish(&txn); err == nil {
		t.Fatal("expected flush or sync failure")
	}
	files, err := filepath.Glob(filepath.Join(dir, "strand-txn-*.spill"))
	if err != nil || len(files) != 0 {
		t.Fatalf("failed spill still on disk: %v %v", files, err)
	}
	if txn.SpillPath != "" {
		t.Fatalf("failed transaction handed off spill path %s", txn.SpillPath)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
}
