package txntrace

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func enableTestTrace(t *testing.T, capacity int) {
	t.Helper()
	oldOn, oldPath, oldRecs := On, path, recs
	On = true
	path = filepath.Join(t.TempDir(), "trace.txt")
	recs = make([]rec, capacity)
	n.Store(0)
	t.Cleanup(func() { On = oldOn; path = oldPath; recs = oldRecs; n.Store(0) })
}

// Không được dùng sự vắng mặt trong trace đã tràn làm bằng chứng mất Txn.
func TestTraceReportsDroppedRecords(t *testing.T) {
	enableTestTrace(t, 2)
	for i := 0; i < 5; i++ {
		Put(Routed, uint64(i+1), 10, 1, 1, 1)
	}
	Dump()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "allocated=5 stored=2 dropped=3 unpublished=0") {
		t.Fatalf("trace hides its truncation: %s", b)
	}
}

// Dump là snapshot chẩn đoán; phải đọc an toàn kể cả khi còn producer, không làm race test bị nhiễu.
func TestTraceConcurrentPublicationAndDump(t *testing.T) {
	enableTestTrace(t, 100000)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100000; i++ {
			Put(Routed, uint64(i+1), 1, 1, 1, 1)
		}
	}()
	for i := 0; i < 5; i++ {
		Dump()
	}
	wg.Wait()
	Dump()
}
