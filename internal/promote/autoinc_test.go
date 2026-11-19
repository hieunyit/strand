package promote

import "testing"

func TestComputeAutoIncrement(t *testing.T) {
	cases := []struct{ src, max, cur, want int64 }{
		{101, 90, 91, 101}, // source đã xóa dòng cao nhất: counter nguồn thắng
		{50, 90, 91, 91},   // target đã vượt: max+1
		{50, 40, 120, 120}, // không bao giờ hạ counter hiện tại của target
		{11, 10, 11, 11},   // bằng nhau
		{5, 0, 1, 5},       // target rỗng
	}
	for _, c := range cases {
		if got := ComputeAutoIncrement(c.src, c.max, c.cur); got != c.want {
			t.Errorf("Compute(%d,%d,%d) = %d want %d", c.src, c.max, c.cur, got, c.want)
		}
	}
}

func TestBuildAutoIncCountersRename(t *testing.T) {
	got := BuildAutoIncCounters([][3]string{{"app", "a", "101"}, {"app", "b", "7"}, {"app", "c", "0"}, {"app", "d", "x"}},
		map[string]string{"app.b": "app2.b_new"})
	if len(got) != 2 || got[0].DstTable != "app.a" || got[0].Next != 101 || got[1].DstTable != "app2.b_new" || got[1].SrcTable != "app.b" {
		t.Fatalf("got %+v", got)
	}
}

func TestAutoIncFloor(t *testing.T) {
	cs := []AutoIncCounter{{DstTable: "public.items", Next: 101}, {DstTable: "app.ai", Next: 7}}
	if v, ok := autoIncFloor(cs, "items"); !ok || v != 100 {
		t.Errorf("public table: %d %v", v, ok)
	}
	if v, ok := autoIncFloor(cs, "app.ai"); !ok || v != 6 {
		t.Errorf("schema table: %d %v", v, ok)
	}
	if _, ok := autoIncFloor(cs, "other"); ok {
		t.Error("unknown table must not match")
	}
}

func TestQuoteMySQL(t *testing.T) {
	cases := []struct {
		input     string
		wantIdent string
	}{
		{"users", "`users`"},
		{"`users`", "```users```"},
		{"user`table", "`user``table`"},
		{"col name", "`col name`"},
		{" id", "` id`"},
		{"`id", "```id`"},
		{"id`", "`id```"},
	}
	for _, tc := range cases {
		if got := quoteMySQLIdent(tc.input); got != tc.wantIdent {
			t.Errorf("quoteMySQLIdent(%q) = %q, want %q", tc.input, got, tc.wantIdent)
		}
	}

	qualCases := []struct {
		input    string
		wantQual string
	}{
		{"users", "`users`"},
		{"`users`", "```users```"},
		{"app.users", "`app`.`users`"},
		{"app.`users`", "`app`.```users```"},
		{"app.my`table", "`app`.`my``table`"},
		{"app.my``table", "`app`.`my````table`"},
		{"app. users", "`app`.` users`"},
		{"app.order.details", "`app`.`order.details`"},
	}
	for _, tc := range qualCases {
		if got := quoteMySQLQualified(tc.input); got != tc.wantQual {
			t.Errorf("quoteMySQLQualified(%q) = %q, want %q", tc.input, got, tc.wantQual)
		}
	}
}
