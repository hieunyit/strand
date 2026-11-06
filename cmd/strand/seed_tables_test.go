package main

import (
	"reflect"
	"testing"

	"strand/internal/model"
)

// review N06: seed apply (strand-export) đối chiếu pattern "tables" với bảng thật ở target.
func TestN06_MissingSeedTables(t *testing.T) {
	have := []model.TableID{{Schema: "public", Table: "orders"}, {Schema: "app", Table: "Users"}}
	got := missingSeedTables([]string{"public.*", `app."Users"`, "orders", "app.items", "public.t?"}, have)
	if want := []string{"app.items", "public.t?"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("missing = %q, want %q", got, want)
	}
}
