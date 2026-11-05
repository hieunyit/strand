package pg

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/reader"
)

func rowEv(i int) reader.Event {
	return reader.Event{Type: reader.EventRows, Op: model.OpUpdate, Schema: "app", Table: "t", TableID: 7, XID: uint64(i),
		Rows: [][]any{
			{int64(i), fmt.Sprintf("v%d", i), nil, []byte{1, 2, byte(i)}, 1.5, true, time.Unix(int64(i), 0).UTC(),
				model.ColValue{Name: "doc", Unchanged: true}},
			{int64(i), "after", nil, []byte{}, -2.0, false, time.Unix(0, int64(i)).UTC(), "big"},
		}}
}

func collect(t *testing.T, l *eventLog) []reader.Event {
	t.Helper()
	var out []reader.Event
	if err := l.replay(func(ev reader.Event) error { out = append(out, ev); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

// review R08: eventLog cho cùng kết quả dù phần thân nằm trong RAM hay đã đổ ra file, kể cả khi rollback
// subtransaction cắt ở mốc đặt trước / sau lúc spill; file tạm bị xóa khi đóng.
func TestR08_EventLogSpillRoundTripAndSubxactTruncate(t *testing.T) {
	for _, spillAt := range []int{-1, 0, 3, 6, 9} {
		t.Run(fmt.Sprintf("spill_at_%d", spillAt), func(t *testing.T) {
			dir := t.TempDir()
			l := newEventLog(dir)
			var want []reader.Event
			tm := reader.Event{Type: reader.EventTableMap, Schema: "app", Table: "t", TableID: 7,
				Columns: []string{"id", "v", "n", "b", "f", "ok", "ts", "doc"}, PKColumns: []string{"id"}}
			add := func(i int, ev reader.Event) {
				if i == spillAt {
					if _, err := l.spill(); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := l.append(ev); err != nil {
					t.Fatal(err)
				}
			}
			add(0, tm)
			want = append(want, tm)
			for i := 1; i <= 4; i++ { // top-level xid 100
				l.mark(100)
				add(i, rowEv(i))
				want = append(want, rowEv(i))
			}
			l.mark(101) // SAVEPOINT s1 — bị rollback
			for i := 5; i <= 8; i++ {
				add(i, rowEv(i))
			}
			l.mark(102) // savepoint lồng trong s1, rollback cùng s1
			add(9, rowEv(9))
			if _, ok, err := l.truncateTo(101); err != nil || !ok {
				t.Fatalf("truncate: ok=%v err=%v", ok, err)
			}
			if _, ok := l.marks[102]; ok {
				t.Fatal("nested savepoint mark survived its parent's rollback")
			}
			add(10, rowEv(10))
			want = append(want, rowEv(10))
			if got := collect(t, l); !reflect.DeepEqual(got, want) {
				t.Fatalf("replay differs:\n got %d events %+v\nwant %d events %+v", len(got), got, len(want), want)
			}
			if spillAt >= 0 && spillAt <= 10 && !l.spilled() {
				t.Fatal("log was not spilled")
			}
			l.close()
			if left, _ := filepath.Glob(filepath.Join(dir, "*")); len(left) != 0 {
				t.Fatalf("files left after close: %v", left)
			}
		})
	}
}
