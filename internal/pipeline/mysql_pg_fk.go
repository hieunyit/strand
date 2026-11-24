package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"strand/internal/model"
)

type postgresCascadeFK struct {
	cascadeFK
	deferrable       bool
	validated        bool
	triggers         int
	disabled         bool
	unsupportedRules bool
}

// checkMySQLPostgresForeignKeys verifies every source cascade against all
// scoped PostgreSQL constraints. Schema generation may create absent tables;
// the caller verifies again after generation and before snapshots begin.
func (p *Pipeline) checkMySQLPostgresForeignKeys(ctx context.Context, additional ...model.TableID) error {
	if p.srcEngine != model.EngineMySQL || p.dstEngine != model.EnginePG || p.src == nil || p.dst == nil || p.filter == nil {
		return nil
	}
	scope := p.cascadeSourceScope(additional...)
	source, err := readSourceForeignKeys(ctx, p.src, scope, false)
	if err != nil {
		return fmt.Errorf("inspect source foreign keys: %w", err)
	}
	expected := make(map[string]int)
	expectedFK := make(map[string]cascadeFK)
	plan := mysqlCascadePlan{parents: map[model.TableID]bool{}, children: map[model.TableID]bool{}}
	targetScope := make(map[model.TableID]bool, len(scope))
	for id := range scope {
		targetScope[p.filter.Target(id)] = true
	}
	for _, fk := range source {
		if !mysqlFKCollationProven(fk) {
			return cascadeConfigError(fmt.Sprintf("cannot prove MySQL and PostgreSQL use equivalent comparison semantics for cascading text keys on %s; use non-text FK columns or keep this cascade within one engine", fk.Child))
		}
		if !scope[fk.Parent] {
			return cascadeConfigError(fmt.Sprintf("cascading foreign key on %s requires its parent %s in the replication scope", fk.Child, fk.Parent))
		}
		plan.parents[fk.Parent], plan.children[fk.Child] = true, true
		fk.Child, fk.Parent = p.filter.Target(fk.Child), p.filter.Target(fk.Parent)
		sig := pgCascadeSignature(fk)
		expected[sig]++
		expectedFK[sig] = fk
		plan.signatures = append(plan.signatures, sig)
	}
	if err := checkCascadeOrder(source); err != nil {
		return err
	}

	target, err := readPostgresForeignKeys(ctx, p.dst)
	if err != nil {
		return fmt.Errorf("inspect PostgreSQL foreign keys: %w", err)
	}
	if len(expected) > 0 {
		var role string
		if err := p.dst.QueryRowContext(ctx, `SELECT current_setting('session_replication_role')`).Scan(&role); err != nil {
			return fmt.Errorf("inspect PostgreSQL session_replication_role: %w", err)
		}
		if !postgresCascadeTriggersActive(role) {
			return cascadeConfigError(fmt.Sprintf("PostgreSQL session_replication_role is %q; foreign-key cascade triggers must run with role origin", role))
		}
	}
	// Empty tables generated from source metadata receive their FKs later in
	// startup. Permit only absent child tables under auto schema creation.
	for sig, count := range expected {
		for i := 0; i < count; i++ {
			if expected[sig] <= 0 {
				break
			}
			matched := -1
			for j := range target {
				if pgCascadeSignature(target[j].cascadeFK) == sig {
					matched = j
					break
				}
			}
			if matched >= 0 {
				fk := target[matched]
				if !postgresFKEnforced(fk) {
					return cascadeConfigError(fmt.Sprintf("PostgreSQL foreign key %s on %s must be an immediate, validated, enforced MATCH SIMPLE constraint with equivalent actions on all key columns; cascade replay cannot be proven safe", fk.constraint, fk.Child))
				}
				target = append(target[:matched], target[matched+1:]...)
				expected[sig]--
				continue
			}
			fk := expectedFK[sig]
			present, tableErr := postgresTableExists(ctx, p.dst, fk.Child)
			if tableErr == nil && !present && p.cfg.Bootstrap.CreateSchemaAuto() {
				expected[sig]--
				continue
			}
			return cascadeConfigError(fmt.Sprintf("PostgreSQL is missing a matching enforced foreign key for MySQL cascade %s -> %s; target rules and mapped ordered columns must match", fk.Child, fk.Parent))
		}
	}
	for _, fk := range target {
		if !fk.cascades() {
			continue
		}
		if targetScope[fk.Child] || targetScope[fk.Parent] {
			return cascadeConfigError(fmt.Sprintf("unexpected PostgreSQL cascading foreign key %s on %s referencing %s touches the replication scope; remove it or match the MySQL source constraint", fk.constraint, fk.Child, fk.Parent))
		}
	}
	sort.Strings(plan.signatures)
	p.pgCascade = plan
	return nil
}

func postgresFKEnforced(fk postgresCascadeFK) bool {
	return !fk.deferrable && fk.validated && !fk.disabled && !fk.unsupportedRules && fk.triggers >= 4
}

func postgresCascadeTriggersActive(role string) bool { return role == "origin" }

func mysqlFKCollationProven(fk cascadeFK) bool {
	for _, c := range append(append([]string{}, fk.ChildCollations...), fk.ParentCollations...) {
		if c != "" {
			return false
		}
	}
	return true
}

func postgresTableExists(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id model.TableID) (bool, error) {
	var present bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)`, id.Schema, id.Table).Scan(&present)
	return present, err
}

func readPostgresForeignKeys(ctx context.Context, db *sql.DB) ([]postgresCascadeFK, error) {
	rows, err := db.QueryContext(ctx, `SELECT con.oid, ns.nspname, child.relname, con.conname, pns.nspname, parent.relname,
		ca.attname, pa.attname, con.confdeltype, con.confupdtype, con.condeferrable, con.convalidated,
		(SELECT count(*) FROM pg_trigger tr WHERE tr.tgconstraint=con.oid AND tr.tgisinternal),
		EXISTS (SELECT 1 FROM pg_trigger tr WHERE tr.tgconstraint=con.oid AND tr.tgisinternal AND tr.tgenabled IN ('D','R')),
		con.confmatchtype <> 's'
		  OR COALESCE(to_jsonb(con)->>'confdelsetcols', '') <> ''
		  OR COALESCE((to_jsonb(con)->>'conperiod')::boolean, false)
		  OR NOT COALESCE((to_jsonb(con)->>'conenforced')::boolean, true)
		  OR child.relkind = 'p' OR parent.relkind = 'p'
	FROM pg_constraint con
	JOIN pg_class child ON child.oid=con.conrelid
	JOIN pg_namespace ns ON ns.oid=child.relnamespace
	JOIN pg_class parent ON parent.oid=con.confrelid
	JOIN pg_namespace pns ON pns.oid=parent.relnamespace
	JOIN LATERAL unnest(con.conkey) WITH ORDINALITY ck(attnum,ord) ON true
	JOIN LATERAL unnest(con.confkey) WITH ORDINALITY pk(attnum,ord) ON pk.ord=ck.ord
	JOIN pg_attribute ca ON ca.attrelid=child.oid AND ca.attnum=ck.attnum
	JOIN pg_attribute pa ON pa.attrelid=parent.oid AND pa.attnum=pk.attnum
	WHERE con.contype='f' AND con.conparentid=0 ORDER BY con.oid,ck.ord`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []postgresCascadeFK
	var lastOID int64
	for rows.Next() {
		var fk postgresCascadeFK
		var oid int64
		var name, del, upd string
		var childCol, parentCol string
		if err := rows.Scan(&oid, &fk.Child.Schema, &fk.Child.Table, &name, &fk.Parent.Schema, &fk.Parent.Table,
			&childCol, &parentCol, &del, &upd, &fk.deferrable, &fk.validated, &fk.triggers, &fk.disabled, &fk.unsupportedRules); err != nil {
			return nil, err
		}
		if len(out) == 0 || oid != lastOID {
			fk.constraint = name
			fk.OnDelete, fk.OnUpdate = pgAction(del), pgAction(upd)
			out = append(out, fk)
			lastOID = oid
		}
		out[len(out)-1].Columns = append(out[len(out)-1].Columns, childCol)
		out[len(out)-1].References = append(out[len(out)-1].References, parentCol)
	}
	return out, rows.Err()
}

func pgAction(code string) string {
	switch code {
	case "a":
		return "RESTRICT"
	case "r":
		return "RESTRICT"
	case "c":
		return "CASCADE"
	case "n":
		return "SET NULL"
	case "d":
		return "SET DEFAULT"
	}
	return "UNKNOWN"
}

func pgCascadeSignature(fk cascadeFK) string {
	fk.ChildCollations, fk.ParentCollations = nil, nil
	fk.Name, fk.constraint = "", ""
	if fk.OnDelete == "NO ACTION" {
		fk.OnDelete = "RESTRICT"
	}
	if fk.OnUpdate == "NO ACTION" {
		fk.OnUpdate = "RESTRICT"
	}
	b, _ := json.Marshal(fk)
	return string(b)
}
