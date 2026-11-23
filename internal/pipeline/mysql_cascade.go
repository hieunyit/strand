package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/go-sql-driver/mysql"

	"strand/internal/exitcode"
	"strand/internal/model"
)

type cascadeFK struct {
	Child, Parent                     model.TableID
	Columns, References               []string
	ChildCollations, ParentCollations []string
	OnDelete, OnUpdate                string
	// Name pins the constraint name into the signature. It is set only when
	// overlapping cascades make InnoDB's constraint-name order observable.
	Name string `json:",omitempty"`
	// constraint is the name read from the catalog; never part of the signature by itself.
	constraint string
}

func readTargetForeignKeys(ctx context.Context, db *sql.DB) ([]cascadeFK, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT k.TABLE_SCHEMA,k.TABLE_NAME,k.REFERENCED_TABLE_SCHEMA,k.REFERENCED_TABLE_NAME,
		       k.COLUMN_NAME,k.REFERENCED_COLUMN_NAME,r.DELETE_RULE,r.UPDATE_RULE,k.CONSTRAINT_NAME,
		       COALESCE(c.COLLATION_NAME,''),COALESCE(p.COLLATION_NAME,'')
		FROM information_schema.REFERENTIAL_CONSTRAINTS r
		JOIN information_schema.KEY_COLUMN_USAGE k
		  ON k.CONSTRAINT_SCHEMA=r.CONSTRAINT_SCHEMA AND k.TABLE_NAME=r.TABLE_NAME
		 AND k.CONSTRAINT_NAME=r.CONSTRAINT_NAME
		LEFT JOIN information_schema.COLUMNS c
		  ON c.TABLE_SCHEMA=k.TABLE_SCHEMA AND c.TABLE_NAME=k.TABLE_NAME AND c.COLUMN_NAME=k.COLUMN_NAME
		LEFT JOIN information_schema.COLUMNS p
		  ON p.TABLE_SCHEMA=k.REFERENCED_TABLE_SCHEMA AND p.TABLE_NAME=k.REFERENCED_TABLE_NAME
		 AND p.COLUMN_NAME=k.REFERENCED_COLUMN_NAME
		WHERE k.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY k.TABLE_SCHEMA,k.TABLE_NAME,k.CONSTRAINT_NAME,k.ORDINAL_POSITION`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cascadeFK
	var lastName string
	for rows.Next() {
		var fk cascadeFK
		var name, col, ref, childCollation, parentCollation string
		if err := rows.Scan(&fk.Child.Schema, &fk.Child.Table, &fk.Parent.Schema, &fk.Parent.Table,
			&col, &ref, &fk.OnDelete, &fk.OnUpdate, &name, &childCollation, &parentCollation); err != nil {
			return nil, err
		}
		if len(out) == 0 || name != lastName || out[len(out)-1].Child != fk.Child {
			fk.constraint = name
			out = append(out, fk)
			lastName = name
		}
		last := &out[len(out)-1]
		last.Columns = append(last.Columns, col)
		last.References = append(last.References, ref)
		last.ChildCollations = append(last.ChildCollations, childCollation)
		last.ParentCollations = append(last.ParentCollations, parentCollation)
	}
	return out, rows.Err()
}

// MariaDB 10.6 can hide REFERENTIAL_CONSTRAINTS from SELECT-only CDC
// accounts. SHOW CREATE TABLE exposes the FK definitions to those accounts.
const cascadeIdent = "`(?:[^`]|``)+`|[[:alnum:]_$]+|\"(?:[^\"]|\"\")+\""
const cascadeColumns = `(?:` + cascadeIdent + `)(?:\s*,\s*(?:` + cascadeIdent + `))*`

var (
	mysqlFKLine   = regexp.MustCompile(`(?i)^\s*(?:CONSTRAINT\s+(` + cascadeIdent + `)\s+)?FOREIGN\s+KEY\s*\((` + cascadeColumns + `)\)\s+REFERENCES\s+(` + cascadeIdent + `)(?:\s*\.\s*(` + cascadeIdent + `))?\s*\((` + cascadeColumns + `)\)(.*)$`)
	mysqlFKPrefix = regexp.MustCompile(`(?i)^\s*(?:CONSTRAINT\s+(?:` + cascadeIdent + `)\s+)?FOREIGN\s+KEY\b`)
	mysqlFKAction = regexp.MustCompile(`(?i)\bON\s+(DELETE|UPDATE)\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION)\b`)
	mysqlFKColumn = regexp.MustCompile(cascadeIdent)
)

func unquoteMySQLFK(s string) string {
	if len(s) >= 2 && (s[0] == '`' || s[0] == '"') && s[0] == s[len(s)-1] {
		quote := s[:1]
		return strings.ReplaceAll(s[1:len(s)-1], quote+quote, quote)
	}
	return s
}

func mysqlFKQuote(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func parseMySQLForeignKeys(table model.TableID, ddl string) ([]cascadeFK, error) {
	var out []cascadeFK
	for _, line := range strings.Split(ddl, "\n") {
		if !mysqlFKPrefix.MatchString(line) {
			continue
		}
		m := mysqlFKLine.FindStringSubmatch(line)
		if m == nil {
			return nil, fmt.Errorf("cannot inspect foreign key from SHOW CREATE TABLE on %s", table)
		}
		m = m[1:] // m[0] is the optional constraint name
		fk := cascadeFK{Child: table, Parent: model.TableID{Schema: table.Schema, Table: unquoteMySQLFK(m[2])},
			OnDelete: "RESTRICT", OnUpdate: "RESTRICT", constraint: unquoteMySQLFK(m[0])}
		if m[3] != "" {
			fk.Parent.Schema, fk.Parent.Table = unquoteMySQLFK(m[2]), unquoteMySQLFK(m[3])
		}
		for _, col := range mysqlFKColumn.FindAllString(m[1], -1) {
			fk.Columns = append(fk.Columns, unquoteMySQLFK(col))
		}
		for _, col := range mysqlFKColumn.FindAllString(m[4], -1) {
			fk.References = append(fk.References, unquoteMySQLFK(col))
		}
		for _, a := range mysqlFKAction.FindAllStringSubmatch(m[5], -1) {
			rule := strings.ToUpper(strings.Join(strings.Fields(a[2]), " "))
			if strings.EqualFold(a[1], "DELETE") {
				fk.OnDelete = rule
			} else {
				fk.OnUpdate = rule
			}
		}
		if strings.Contains(strings.ToUpper(mysqlFKAction.ReplaceAllString(m[5], "")), "ON ") {
			return nil, fmt.Errorf("unrecognized foreign-key action on %s", table)
		}
		out = append(out, fk)
	}
	return out, nil
}

func isCascadeAction(rule string) bool {
	return rule == "CASCADE" || rule == "SET NULL" || rule == "SET DEFAULT"
}

func (fk cascadeFK) cascades() bool {
	return isCascadeAction(fk.OnDelete) || isCascadeAction(fk.OnUpdate)
}

func readSourceForeignKeys(ctx context.Context, db *sql.DB, scope map[model.TableID]bool, includeRestrictive bool) ([]cascadeFK, error) {
	var out []cascadeFK
	for table := range scope {
		var name, ddl string
		err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+mysqlFKQuote(table.Schema)+"."+mysqlFKQuote(table.Table)).Scan(&name, &ddl)
		if err != nil {
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == 1146 {
				continue // DROP or RENAME can make a previously selected table disappear during DDL replay.
			}
			return nil, fmt.Errorf("read foreign keys of %s: %w", table, err)
		}
		fks, err := parseMySQLForeignKeys(table, ddl)
		if err != nil {
			return nil, err
		}
		for _, fk := range fks {
			if !includeRestrictive && !fk.cascades() {
				continue
			}
			if !scope[fk.Parent] {
				out = append(out, fk) // The caller reports the missing parent even if its columns aren't visible.
				continue
			}
			fk.ChildCollations, err = readFKCollations(ctx, db, fk.Child, fk.Columns)
			if err != nil {
				return nil, err
			}
			fk.ParentCollations, err = readFKCollations(ctx, db, fk.Parent, fk.References)
			if err != nil {
				return nil, err
			}
			out = append(out, fk)
		}
	}
	return out, nil
}

func readFKCollations(ctx context.Context, db *sql.DB, table model.TableID, cols []string) ([]string, error) {
	out := make([]string, len(cols))
	for i, col := range cols {
		err := db.QueryRowContext(ctx, `SELECT COALESCE(COLLATION_NAME,'') FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, table.Schema, table.Table, col).Scan(&out[i])
		if err != nil {
			return nil, fmt.Errorf("inspect collation of foreign-key column %s.%s: %w", table, col, err)
		}
	}
	return out, nil
}

// InnoDB can order actions by constraint name. Distinct cascading FKs reached
// by the same write can affect the same row when they reach the same table.
func checkCascadeOrder(fks []cascadeFK) error {
	outgoing := make(map[model.TableID][]*cascadeFK)
	for i := range fks {
		fk := &fks[i]
		outgoing[fk.Parent] = append(outgoing[fk.Parent], fk)
	}
	type change struct {
		table    model.TableID
		deleting bool
	}
	for parent := range outgoing {
		for _, deleting := range []bool{true, false} {
			pending := []change{{table: parent, deleting: deleting}}
			visited := make(map[change]bool)
			reached := make(map[model.TableID]*cascadeFK)
			for len(pending) > 0 {
				current := pending[len(pending)-1]
				pending = pending[:len(pending)-1]
				if visited[current] {
					continue
				}
				visited[current] = true
				for _, fk := range outgoing[current.table] {
					rule := fk.OnUpdate
					if current.deleting {
						rule = fk.OnDelete
					}
					if !isCascadeAction(rule) {
						continue
					}
					if previous := reached[fk.Child]; previous != nil && previous != fk {
						return cascadeConfigError(fmt.Sprintf("overlapping cascading foreign keys from %s can reach %s through multiple constraints; cascade results can depend on constraint order", parent, fk.Child))
					}
					reached[fk.Child] = fk
					// Only DELETE CASCADE removes the child. SET NULL
					// may in turn trigger ON UPDATE rules on its children.
					pending = append(pending, change{table: fk.Child, deleting: current.deleting && rule == "CASCADE"})
				}
			}
		}
	}
	return nil
}

func cascadeSignature(fk cascadeFK) string {
	// The preflight rejects order-sensitive rule sets, so signatures can omit
	// constraint names. NO ACTION and RESTRICT are equivalent in InnoDB.
	if fk.OnDelete == "NO ACTION" {
		fk.OnDelete = "RESTRICT"
	}
	if fk.OnUpdate == "NO ACTION" {
		fk.OnUpdate = "RESTRICT"
	}
	raw, _ := json.Marshal(fk)
	return string(raw)
}

func cascadeConfigError(message string) error {
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, message, nil)
}

type mysqlCascadePlan struct {
	signatures []string
	parents    map[model.TableID]bool
	children   map[model.TableID]bool
	missing    []cascadeFK
}

func (p *Pipeline) planMySQLCascade(ctx context.Context, additional ...model.TableID) (mysqlCascadePlan, error) {
	return p.compareMySQLForeignKeys(ctx, false, additional...)
}

func (p *Pipeline) compareMySQLForeignKeys(ctx context.Context, allowMissing bool, additional ...model.TableID) (mysqlCascadePlan, error) {
	var plan mysqlCascadePlan
	if p.srcEngine != model.EngineMySQL || p.dstEngine != model.EngineMySQL || p.src == nil || p.dst == nil || p.filter == nil {
		return plan, nil
	}
	plan.parents, plan.children = map[model.TableID]bool{}, map[model.TableID]bool{}
	sourceScope := p.cascadeSourceScope(additional...)
	source, err := readSourceForeignKeys(ctx, p.src, sourceScope, true)
	if err != nil {
		return plan, fmt.Errorf("inspect source foreign keys: %w", err)
	}
	target, err := readTargetForeignKeys(ctx, p.dst)
	if err != nil {
		return plan, fmt.Errorf("inspect target foreign keys: %w", err)
	}
	targetScope := make(map[model.TableID]bool, len(sourceScope))
	for id := range sourceScope {
		targetScope[p.filter.Target(id)] = true
	}
	sourceGroup, sourcePinned, overlap := cascadeGroups(source)
	targetGroup := make(map[model.TableID]bool, len(sourceGroup))
	targetPinned := make(map[model.TableID]bool, len(sourcePinned))
	for id := range sourceGroup {
		dst := p.filter.Target(id)
		targetGroup[dst] = true
		targetPinned[dst] = sourcePinned[id]
	}
	expected := make(map[string]int)
	var rules []cascadeFK
	for _, fk := range source {
		native := fk.cascades()
		if !native && !(sourceGroup[fk.Child] || sourceGroup[fk.Parent]) {
			continue
		}
		if !sourceScope[fk.Parent] {
			return plan, cascadeConfigError(fmt.Sprintf("foreign key %s on %s requires its parent %s in the replication scope", fk.constraint, fk.Child, fk.Parent))
		}
		if fk.OnDelete == "SET DEFAULT" || fk.OnUpdate == "SET DEFAULT" {
			return plan, cascadeConfigError(fmt.Sprintf("SET DEFAULT foreign-key actions on %s are unsupported by the MySQL InnoDB target", fk.Child))
		}
		if native {
			if meta := p.table(fk.Parent); meta != nil && meta.Keyless {
				return plan, cascadeConfigError(fmt.Sprintf("cascading foreign key on %s requires a primary or unique key on parent %s", fk.Child, fk.Parent))
			}
			plan.parents[fk.Parent], plan.children[fk.Child] = true, true
		}
		if sourcePinned[fk.Child] {
			if fk.constraint == "" || p.filter.Target(fk.Child) != fk.Child {
				return plan, cascadeConfigError(fmt.Sprintf("order-sensitive foreign keys: %s must keep its schema, table and constraint names on the target", fk.Child))
			}
			fk.Name = fk.constraint
		}
		sourceChild := fk.Child
		fk.Child, fk.Parent = p.filter.Target(fk.Child), p.filter.Target(fk.Parent)
		key := cascadeSignature(fk)
		expected[key]++
		// Keep the persisted signature format compatible with existing checkpoints.
		// Restrictive FKs are verified directly against the target on every start.
		if native {
			signatureFK := fk
			signatureFK.Name = ""
			if overlap {
				signatureFK.Name = fk.constraint
			}
			plan.signatures = append(plan.signatures, cascadeSignature(signatureFK))
		}
		fk.constraint = p.cascadeTargetName(sourceChild, fk)
		rules = append(rules, fk)
	}
	hint := ""
	if overlap {
		hint = " (overlapping cascading foreign keys: target rules and constraint names must match the source)"
	} else if len(sourcePinned) > 0 {
		hint = " (order-sensitive foreign keys: target rules and constraint names must match the source)"
	}
	sort.Strings(plan.signatures)
	for _, fk := range target {
		if !(fk.cascades() && targetScope[fk.Parent]) && !targetGroup[fk.Child] && !targetGroup[fk.Parent] {
			continue
		}
		if targetPinned[fk.Child] || targetPinned[fk.Parent] {
			fk.Name = fk.constraint
		}
		key := cascadeSignature(fk)
		if expected[key] == 0 {
			kind := "foreign key"
			if fk.cascades() {
				kind = "cascading foreign key"
			}
			return plan, cascadeConfigError(fmt.Sprintf("unexpected %s %s on target %s referencing %s (%s); target rules, columns and key collations must match the source%s",
				kind, fk.constraint, fk.Child, fk.Parent, describeCascadeFK(fk), hint))
		}
		expected[key]--
	}
	for _, fk := range rules {
		key := cascadeSignature(fk)
		if expected[key] == 0 {
			continue
		}
		if !allowMissing {
			return plan, cascadeConfigError(p.missingCascadeMessage(ctx, fk, target) + hint)
		}
		for _, existing := range target {
			sameLink := existing.Child == fk.Child && existing.Parent == fk.Parent && slices.Equal(existing.Columns, fk.Columns)
			sameName := existing.Child.Schema == fk.Child.Schema && strings.EqualFold(existing.constraint, fk.constraint)
			if sameLink || sameName {
				return plan, cascadeConfigError(p.missingCascadeMessage(ctx, fk, target) + hint)
			}
		}
		plan.missing = append(plan.missing, fk)
		expected[key]--
	}
	return plan, nil
}

// cascadeGroups finds the FK components in which native cascade is in use.
// Name order is observable only inside components with overlapping actions or
// restrictive checks, so an unrelated component can still be renamed.
func cascadeGroups(all []cascadeFK) (group, pinned map[model.TableID]bool, hasOverlap bool) {
	neighbors := map[model.TableID][]model.TableID{}
	outgoing := map[model.TableID][]cascadeFK{}
	for _, fk := range all {
		neighbors[fk.Parent] = append(neighbors[fk.Parent], fk.Child)
		neighbors[fk.Child] = append(neighbors[fk.Child], fk.Parent)
		outgoing[fk.Parent] = append(outgoing[fk.Parent], fk)
	}
	group, pinned = map[model.TableID]bool{}, map[model.TableID]bool{}
	for _, fk := range all {
		if !fk.cascades() || group[fk.Parent] {
			continue
		}
		queue := []model.TableID{fk.Parent}
		var component []model.TableID
		var cascades []cascadeFK
		restrictive := false
		for len(queue) > 0 {
			id := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			if group[id] {
				continue
			}
			group[id] = true
			component = append(component, id)
			queue = append(queue, neighbors[id]...)
			for _, edge := range outgoing[id] {
				if edge.cascades() {
					cascades = append(cascades, edge)
				} else {
					restrictive = true
				}
			}
		}
		overlap := checkCascadeOrder(cascades) != nil
		hasOverlap = hasOverlap || overlap
		if restrictive || overlap {
			for _, id := range component {
				pinned[id] = true
			}
		}
	}
	return group, pinned, hasOverlap
}

func (p *Pipeline) cascadeSourceScope(additional ...model.TableID) map[model.TableID]bool {
	p.tablesMu.RLock()
	scope := make(map[model.TableID]bool, len(p.tables)+len(additional))
	for id := range p.tables {
		scope[id] = true
	}
	p.tablesMu.RUnlock()
	for _, id := range additional {
		scope[id] = true
	}
	return scope
}

func describeCascadeFK(fk cascadeFK) string {
	desc := fmt.Sprintf("(%s) -> (%s) ON DELETE %s ON UPDATE %s", strings.Join(fk.Columns, ","), strings.Join(fk.References, ","), fk.OnDelete, fk.OnUpdate)
	if collations := strings.Trim(strings.Join(fk.ChildCollations, ",")+"/"+strings.Join(fk.ParentCollations, ","), ",/"); collations != "" {
		desc += " collations " + strings.Join(fk.ChildCollations, ",") + " / " + strings.Join(fk.ParentCollations, ",")
	}
	return desc
}

// cascadeAddSQL is the target DDL that recreates a source cascading foreign key.
func cascadeAddSQL(fk cascadeFK, name string) string {
	cols := make([]string, len(fk.Columns))
	for i, c := range fk.Columns {
		cols[i] = mysqlFKQuote(c)
	}
	refs := make([]string, len(fk.References))
	for i, c := range fk.References {
		refs[i] = mysqlFKQuote(c)
	}
	return "ALTER TABLE " + autoSeedTableSQL(fk.Child) + " ADD CONSTRAINT " + mysqlFKQuote(name) +
		" FOREIGN KEY (" + strings.Join(cols, ", ") + ") REFERENCES " + autoSeedTableSQL(fk.Parent) +
		" (" + strings.Join(refs, ", ") + ") ON DELETE " + fk.OnDelete + " ON UPDATE " + fk.OnUpdate
}

// cascadeTargetName is the constraint name schemagen gives the target copy.
func (p *Pipeline) cascadeTargetName(sourceChild model.TableID, fk cascadeFK) string {
	if fk.Name != "" {
		return fk.Name
	}
	if dst := p.filter.Target(sourceChild); dst != sourceChild {
		return dst.Table + "_" + fk.constraint
	}
	return fk.constraint
}

// missingCascadeMessage says why no target constraint matches fk (target names)
// and how to add it.
func (p *Pipeline) missingCascadeMessage(ctx context.Context, fk cascadeFK, target []cascadeFK) string {
	kind := "foreign key"
	if fk.cascades() {
		kind = "cascading foreign key"
	}
	msg := fmt.Sprintf("missing matching target %s on %s referencing %s (ON DELETE %s, ON UPDATE %s)", kind, fk.Child, fk.Parent, fk.OnDelete, fk.OnUpdate)
	var exists int
	err := p.dst.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", fk.Child.Schema, fk.Child.Table).Scan(&exists)
	if err == nil && exists == 0 {
		return msg + fmt.Sprintf(": target table %s does not exist; create it with its foreign keys (bootstrap.create_schema: auto does this) or restore a consistent seed", fk.Child)
	}
	var similar []string
	for _, t := range target {
		if t.Child == fk.Child && t.Parent == fk.Parent {
			similar = append(similar, fmt.Sprintf("%s %s", t.constraint, describeCascadeFK(t)))
		}
	}
	if len(similar) > 0 {
		return msg + fmt.Sprintf(": the target has %s, but the source needs %s %s; make the target rules, columns and key collations match the source",
			strings.Join(similar, "; "), fk.constraint, describeCascadeFK(fk))
	}
	name := fk.Name
	if name == "" {
		name = fk.constraint
	}
	if name == "" {
		name = fk.Child.Table + "_" + fk.Parent.Table + "_fk"
	}
	return msg + fmt.Sprintf(": target table %s exists without it (created before Strand, or by a tool that drops foreign keys); add it on the target with: %s", fk.Child, cascadeAddSQL(fk, name))
}

func (p *Pipeline) initMySQLCascade(ctx context.Context) error {
	plan, err := p.planMySQLCascade(ctx)
	if err != nil {
		return err
	}
	if len(plan.signatures) > 0 {
		if _, err := os.Stat(filepath.Join(p.cfg.Resources.Spool.Path, "prefetch")); err == nil {
			return cascadeConfigError("cascading foreign keys require a live binlog reader; prefetch spools do not preserve foreign-key session flags")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		p.strict = true
		p.log.Info("MySQL cascade enabled; applying CDC in source commit order")
	}
	if err := p.state.checkCascadeSignatures(plan.signatures); err != nil {
		return err
	}
	p.cascade = plan
	return nil
}
