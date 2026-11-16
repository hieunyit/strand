package writer

import (
	"context"
	"testing"

	"strand/internal/model"
	"strand/internal/testutil"
)

// Trên server thật (MySQL 8.4, MariaDB 10.6): DELETE/UPDATE keyless chỉ trúng ĐÚNG hàng khớp theo byte, dù collation
// của cột là _ci/_ai và PAD SPACE ('abc' = 'ABC' = 'abc '), kể cả cột latin1, CHAR, ENUM và NULL.
func TestContainer_KeylessMatchIsByteExact(t *testing.T) {
	for _, image := range []string{"mysql:8.4", "mariadb:10.6"} {
		image := image
		t.Run(image, func(t *testing.T) {
			db := testutil.LegacyMySQL(t, image)
			ctx := context.Background()
			exec := func(q string, args ...any) int64 {
				t.Helper()
				res, err := db.ExecContext(ctx, q, args...)
				if err != nil {
					t.Fatalf("%s: %v", q, err)
				}
				n, _ := res.RowsAffected()
				return n
			}
			exec("CREATE DATABASE d")
			exec("CREATE TABLE d.k (a INT NULL, v VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL, " +
				"c CHAR(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL, l VARCHAR(20) CHARACTER SET latin1 NULL, " +
				"e ENUM('x','Y') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL)")
			rows := [][]any{
				{1, "abc", "ab", "café", "x"}, {1, "ABC", "AB", "CAFÉ", "Y"}, {1, "abc ", "ab ", "cafe", "x"},
				{1, "é", nil, nil, nil}, {1, "e", nil, nil, nil}, {nil, nil, nil, nil, nil},
			}
			for _, r := range rows {
				exec("INSERT INTO d.k VALUES (?, ?, ?, ?, ?)", r...)
			}
			cols := []string{"a", "v", "c", "l", "e"}
			str := map[string]bool{"v": true, "c": true, "l": true, "e": true}
			del := func(img ...any) int64 {
				vals := make([]model.Value, len(img))
				for i, v := range img {
					vals[i] = v
				}
				return exec(buildKeylessDeleteMySQL(model.TableID{Schema: "d", Table: "k"}, cols, str), keylessMatchArgs(cols, str, vals)...)
			}
			count := func() (n int) {
				if err := db.QueryRow("SELECT COUNT(*) FROM d.k").Scan(&n); err != nil {
					t.Fatal(err)
				}
				return
			}
			// Each image matches exactly its own row, never a case/accent/trailing-space twin.
			if n := del(1, "ABC", "AB", "CAFÉ", "Y"); n != 1 || count() != 5 {
				t.Fatalf("delete ABC row: affected %d, remaining %d", n, count())
			}
			var upper int
			_ = db.QueryRow("SELECT COUNT(*) FROM d.k WHERE BINARY v = 'ABC'").Scan(&upper)
			if upper != 0 {
				t.Fatal("the ABC row must be the deleted one")
			}
			if n := del(1, "abc ", "ab", "cafe", "x"); n != 1 { // CHAR values come back without padding
				t.Fatalf("delete trailing-space row: affected %d", n)
			}
			var plain int
			_ = db.QueryRow("SELECT COUNT(*) FROM d.k WHERE BINARY v = 'abc'").Scan(&plain)
			if plain != 1 {
				t.Fatal("the 'abc' row must survive deleting 'abc '")
			}
			if n := del(1, "e", nil, nil, nil); n != 1 {
				t.Fatalf("delete e: affected %d", n)
			}
			if n := del(nil, nil, nil, nil, nil); n != 1 {
				t.Fatalf("delete all-NULL row: affected %d", n)
			}
			if n := del(1, "ABC", "AB", "CAFÉ", "Y"); n != 0 {
				t.Fatalf("an image with no byte-exact row must match nothing, affected %d", n)
			}
			// UPDATE: only the exact row changes.
			upd := buildKeylessUpdateMySQL(model.TableID{Schema: "d", Table: "k"}, []string{"a"}, cols, str)
			before := []model.Value{int64(1), "abc", "ab", "café", "x"}
			if n := exec(upd, append([]any{int64(9)}, keylessMatchArgs(cols, str, before)...)...); n != 1 {
				t.Fatalf("update: affected %d", n)
			}
			var moved int
			_ = db.QueryRow("SELECT COUNT(*) FROM d.k WHERE a = 9 AND BINARY v = 'abc'").Scan(&moved)
			if moved != 1 {
				t.Fatal("update hit the wrong row")
			}
		})
	}
}
