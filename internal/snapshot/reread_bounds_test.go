package snapshot

import (
	"fmt"
	"strand/internal/model"
	"testing"
)

func TestRereadBoundsOwnershipAndSupersededKeys(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "reread"}
	m := NewRangeStateManager()
	m.RegisterTable(table, "id", nil)
	// Preserve typed tuple values and exclusive bounds, including mutable byte keys.
	a := Bound{K: "a", V: []model.Value{int64(7), []byte("old")}, Excl: true}
	b := Bound{K: "b", V: []model.Value{"text", uint64(9)}}
	replacement := Bound{K: "a", V: []model.Value{int64(8), []byte("new")}, Excl: true}
	old := &Chunk{KeySet: []Bound{b, a}}
	current := &Chunk{KeySet: []Bound{replacement}}
	m.tables[table.String()].Reread = map[Key]*Chunk{"a": current, "b": old}
	got := m.RereadBounds(table)
	if len(got) != 2 || got[0].K != "a" || got[1].K != "b" || !got[0].Excl || got[0].V[0] != int64(8) || got[1].V[1] != uint64(9) {
		t.Fatalf("lost ordering, ownership, or bound types: %#v", got)
	}
	got[0].V[0] = int64(999)
	got[0].V[1].([]byte)[0] = 'X'
	got[1].K = "changed"
	again := m.RereadBounds(table)
	if again[0].V[0] != int64(8) || string(again[0].V[1].([]byte)) != "new" || again[1].K != "b" {
		t.Fatalf("returned bounds alias manager: %#v", again)
	}
}

func BenchmarkRereadBounds(b *testing.B) {
	for _, n := range []int{500, 5000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			table := model.TableID{Table: "bench"}
			m := NewRangeStateManager()
			m.RegisterTable(table, "id", nil)
			c := &Chunk{KeySet: make([]Bound, n)}
			tr := m.tables[table.String()]
			tr.Reread = make(map[Key]*Chunk, n)
			for i := range c.KeySet {
				c.KeySet[i] = IntBound(int64(i))
				tr.Reread[c.KeySet[i].K] = c
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if len(m.RereadBounds(table)) != n {
					b.Fatal("keys lost")
				}
			}
		})
	}
}
