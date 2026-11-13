package pipeline

import (
	"testing"
	"time"

	"strand/internal/admin"
)

func TestRateWindowAndSnapshotProgress(t *testing.T) {
	var w rateWindow
	t0 := time.Unix(1000, 0)
	for i := 0; i <= 12; i++ {
		w.add(t0.Add(time.Duration(i)*5*time.Second), int64(i*5000)) // 1000 row/s
	}
	if r := w.rate(); r < 999 || r > 1001 {
		t.Fatalf("rate %.1f want 1000", r)
	}
	info := &admin.TopInfo{TablesTotal: 4, TablesDone: 1, SnapshotRows: 250, SnapshotRowsTotal: 1000, SnapshotRate: 50}
	pct, eta := snapshotProgress(info)
	if pct != 25 || eta != 15 {
		t.Fatalf("pct=%v eta=%v", pct, eta)
	}
	info.SnapshotRows = 2000 // ước lượng thấp hơn thực tế: không vượt 100 trước khi xong
	if pct, _ := snapshotProgress(info); pct != 99.9 {
		t.Fatalf("pct=%v", pct)
	}
	info.TablesDone = 4
	if pct, eta := snapshotProgress(info); pct != 100 || eta != 0 {
		t.Fatalf("done pct=%v eta=%v", pct, eta)
	}
}
