package schemachange

import "testing"

func TestRebindPlaceholders(t *testing.T) {
	my := NewStore(nil, false)
	pg := NewStore(nil, true)
	const q = `SELECT id FROM t WHERE a=? AND b=? AND c=?`
	if got := my.q(q); got != q {
		t.Fatalf("mysql must keep ?: %s", got)
	}
	if got := pg.q(q); got != `SELECT id FROM t WHERE a=$1 AND b=$2 AND c=$3` {
		t.Fatalf("pg rebind: %s", got)
	}
}
