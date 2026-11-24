package pipeline

import (
	"reflect"
	"strings"
	"testing"

	"strand/internal/model"
)

func TestParseMySQLForeignKeys(t *testing.T) {
	child := model.TableID{Schema: "sales", Table: "phones"}
	ddl := `CREATE TABLE "phones" (
  "a" int,
  CONSTRAINT "phone_parent_fk" FOREIGN KEY ("a","b") REFERENCES "crm"."parent" ("k,1","k""2") ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT "ignored_fk" FOREIGN KEY ("c") REFERENCES "other" ("id")
)`
	got, err := parseMySQLForeignKeys(child, ddl)
	want := []cascadeFK{{Child: child, Parent: model.TableID{Schema: "crm", Table: "parent"},
		Columns: []string{"a", "b"}, References: []string{"k,1", `k"2`}, OnDelete: "SET NULL", OnUpdate: "CASCADE", constraint: "phone_parent_fk"},
		{Child: child, Parent: model.TableID{Schema: "sales", Table: "other"}, Columns: []string{"c"}, References: []string{"id"}, OnDelete: "RESTRICT", OnUpdate: "RESTRICT", constraint: "ignored_fk"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("FKs = %#v, error = %v; want %#v", got, err, want)
	}
}

func TestCheckCascadeOrder(t *testing.T) {
	fk := func(parent, child, onDelete, onUpdate string) cascadeFK {
		return cascadeFK{
			Parent:  model.TableID{Schema: "app", Table: parent},
			Child:   model.TableID{Schema: "app", Table: child},
			Columns: []string{"parent_id"}, References: []string{"id"},
			OnDelete: onDelete, OnUpdate: onUpdate,
		}
	}
	for _, tc := range []struct {
		name        string
		fks         []cascadeFK
		wantOverlap string
	}{
		{
			name: "conflicting actions can follow constraint name order",
			fks: []cascadeFK{
				fk("parent", "child", "SET NULL", "RESTRICT"),
				fk("parent", "child", "CASCADE", "RESTRICT"),
			},
			wantOverlap: "from app.parent can reach app.child",
		},
		{
			name: "set null update reaches same table as another delete",
			fks: []cascadeFK{
				fk("parent", "child", "SET NULL", "RESTRICT"),
				fk("child", "grandchild", "RESTRICT", "CASCADE"),
				fk("parent", "grandchild", "CASCADE", "RESTRICT"),
			},
			wantOverlap: "from app.parent can reach app.grandchild",
		},
		{
			name: "independent branches and a chain",
			fks: []cascadeFK{
				fk("parent", "child", "CASCADE", "CASCADE"),
				fk("parent", "sibling", "SET NULL", "SET NULL"),
				fk("child", "grandchild", "CASCADE", "RESTRICT"),
			},
		},
		{
			name: "actions triggered by different operations",
			fks: []cascadeFK{
				fk("parent", "child", "SET NULL", "RESTRICT"),
				fk("parent", "child", "RESTRICT", "CASCADE"),
			},
		},
		{
			name: "actions triggered by unrelated parents",
			fks: []cascadeFK{
				fk("parent", "child", "CASCADE", "RESTRICT"),
				fk("other", "child", "SET NULL", "RESTRICT"),
			},
		},
		{
			name: "single self-referential action",
			fks:  []cascadeFK{fk("tree", "tree", "CASCADE", "RESTRICT")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCascadeOrder(tc.fks)
			if tc.wantOverlap == "" {
				if err != nil {
					t.Fatalf("unexpected rejection: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantOverlap) {
				t.Fatalf("error = %v, want %q", err, tc.wantOverlap)
			}
		})
	}
}

func TestCascadeSignatureKeepsUnpinnedFormat(t *testing.T) {
	// Pipelines started before names could be pinned stored signatures without
	// a Name; an unpinned constraint must keep producing exactly that text.
	fk := cascadeFK{Child: model.TableID{Schema: "app", Table: "c"}, Parent: model.TableID{Schema: "app", Table: "p"},
		Columns: []string{"p_id"}, References: []string{"id"}, OnDelete: "CASCADE", OnUpdate: "NO ACTION", constraint: "c_ibfk_1"}
	const want = `{"Child":{"schema":"app","table":"c"},"Parent":{"schema":"app","table":"p"},"Columns":["p_id"],"References":["id"],"ChildCollations":null,"ParentCollations":null,"OnDelete":"CASCADE","OnUpdate":"RESTRICT"}`
	if got := cascadeSignature(fk); got != want {
		t.Fatalf("signature = %s\nwant        %s", got, want)
	}
	fk.Name = fk.constraint
	if got := cascadeSignature(fk); !strings.Contains(got, `"Name":"c_ibfk_1"`) {
		t.Fatalf("pinned signature lacks the name: %s", got)
	}
}

func TestCascadeMatchSignatureAcceptsOnlyEquivalentCollationAliases(t *testing.T) {
	source := cascadeFK{ChildCollations: []string{"utf8_general_ci"}, ParentCollations: []string{"utf8_bin"}}
	target := cascadeFK{ChildCollations: []string{"utf8mb3_general_ci"}, ParentCollations: []string{"utf8mb3_bin"}}
	if !equivalentFKCollations(source.ChildCollations, target.ChildCollations) ||
		cascadeMatchSignature(source) != cascadeMatchSignature(target) {
		t.Fatal("the same UTF-8 collation under a different catalog alias must match")
	}
	if cascadeSignature(source) == cascadeSignature(target) || source.ChildCollations[0] != "utf8_general_ci" {
		t.Fatal("matching must not change durable signatures or the original metadata")
	}
	target.ChildCollations[0] = "utf8mb3_unicode_ci"
	if equivalentFKCollations(source.ChildCollations, target.ChildCollations) ||
		cascadeMatchSignature(source) == cascadeMatchSignature(target) {
		t.Fatal("different collations must not match")
	}
}

func TestMySQLCascadeMatchesPostgres(t *testing.T) {
	source := cascadeFK{Child: model.TableID{Schema: "app", Table: "child"}, Parent: model.TableID{Schema: "app", Table: "parent"},
		Columns: []string{"tenant", "parent_id"}, References: []string{"tenant", "id"}, OnDelete: "CASCADE", OnUpdate: "RESTRICT"}
	target := source
	target.OnUpdate = "NO ACTION"
	if pgCascadeSignature(source) != pgCascadeSignature(target) {
		t.Fatal("matching PostgreSQL FK with equivalent NO ACTION was rejected")
	}
	target.Columns = []string{"parent_id", "tenant"}
	if pgCascadeSignature(source) == pgCascadeSignature(target) {
		t.Fatal("reordered FK columns matched")
	}
	target = source
	target.OnDelete = "SET NULL"
	if pgCascadeSignature(source) == pgCascadeSignature(target) {
		t.Fatal("mismatched action matched")
	}
	if postgresFKEnforced(postgresCascadeFK{deferrable: true, validated: true, triggers: 4}) {
		t.Fatal("deferrable target FK is considered enforced immediately")
	}
	if postgresFKEnforced(postgresCascadeFK{validated: false, triggers: 4}) || postgresFKEnforced(postgresCascadeFK{validated: true, disabled: true, triggers: 4}) || postgresFKEnforced(postgresCascadeFK{validated: true, triggers: 3}) {
		t.Fatal("NOT VALID or disabled-trigger target FK is considered enforced")
	}
	if postgresFKEnforced(postgresCascadeFK{validated: true, triggers: 4, unsupportedRules: true}) {
		t.Fatal("foreign key with PostgreSQL-specific actions is considered equivalent")
	}
	if postgresCascadeTriggersActive("replica") || !postgresCascadeTriggersActive("origin") {
		t.Fatal("session_replication_role did not gate FK cascade behavior")
	}
	if !mysqlFKCollationProven(source) {
		t.Fatal("numeric FK columns should not need collation proof")
	}
	source.ChildCollations = []string{"utf8mb4_0900_ai_ci"}
	if mysqlFKCollationProven(source) {
		t.Fatal("text FK with engine-specific collation should be rejected")
	}
}

func TestCascadeAddSQL(t *testing.T) {
	fk := cascadeFK{Child: model.TableID{Schema: "call_dashboard", Table: "customer_attribute"}, Parent: model.TableID{Schema: "call_dashboard", Table: "customer"},
		Columns: []string{"customer_id"}, References: []string{"id"}, OnDelete: "CASCADE", OnUpdate: "RESTRICT"}
	const want = "ALTER TABLE `call_dashboard`.`customer_attribute` ADD CONSTRAINT `fk_ca` FOREIGN KEY (`customer_id`) REFERENCES `call_dashboard`.`customer` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT"
	if got := cascadeAddSQL(fk, "fk_ca"); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestTransactionControlQuery(t *testing.T) {
	for q, want := range map[string]bool{
		"BEGIN": true, "COMMIT /* implicit, from Xid_log_event */": true, "XA END X'01',X'',1": true, " savepoint `a`": true,
		"use `app`; ALTER TABLE t ADD c int": false, "TRUNCATE app.t": false, "CREATE TABLE x (id int)": false,
	} {
		if got := transactionControlQuery(q); got != want {
			t.Errorf("transactionControlQuery(%q) = %v, want %v", q, got, want)
		}
	}
}
