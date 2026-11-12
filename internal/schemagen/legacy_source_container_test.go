package schemagen

import (
	"context"
	"strings"
	"testing"

	"strand/internal/testutil"
)

// MySQL 5.5/5.6 không có information_schema.COLUMNS.GENERATION_EXPRESSION (5.7+).
func TestCompatM56_Container_ColumnExtrasSelect(t *testing.T) {
	for _, img := range []string{"mysql:5.5", "mysql:5.6"} {
		t.Run(img, func(t *testing.T) {
			db := testutil.LegacyMySQL(t, img)
			ctx := context.Background()
			q := columnExtrasSelect(ctx, Request{Source: db})
			if !strings.Contains(q, "'' AS GENERATION_EXPRESSION") {
				t.Fatalf("got %q", q)
			}
			var n string
			if err := db.QueryRowContext(ctx, q+" FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='user' LIMIT 1").Scan(&n, new(any), new(string), new(string)); err != nil {
				t.Fatalf("query must run on %s: %v", img, err)
			}
		})
	}
}
