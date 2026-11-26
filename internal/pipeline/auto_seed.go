package pipeline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/findings"
	"strand/internal/model"
	"strand/internal/mysqlver"
	mysqlreader "strand/internal/reader/mysql"
	"strand/internal/schemagen"
)

func autoSeedRejected(msg string) error {
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "auto-seed: "+msg, nil)
}

type seedDependency struct {
	child, parent model.TableID
}

// ValidateAutoSeedPreflight runs the same FK-group closure as Start and checks
// that existing destination tables in the first-copy group are empty. Unrelated
// tables may already contain data. target maps source IDs through table renames.
func ValidateAutoSeedPreflight(ctx context.Context, src, dst *sql.DB, allowMissingFK bool, tables, seedTables []model.TableID, target func(model.TableID) model.TableID) error {
	return autoSeedPreflight(ctx, src, dst, allowMissingFK, tables, seedTables, target, nil)
}

// AutoSeedPreflightReport là ValidateAutoSeedPreflight cho strand doctor: mọi vấn đề (FK không khớp, nhóm FK ra
// ngoài phạm vi, bảng đích đã có dữ liệu, FK sẽ được thêm) được ghi vào c thay vì dừng ở vấn đề đầu tiên.
// Chỉ đọc: không tạo bảng hay FK. Lỗi trả về chỉ còn là lỗi không đọc được catalog.
func AutoSeedPreflightReport(ctx context.Context, src, dst *sql.DB, allowMissingFK bool, tables, seedTables []model.TableID, target func(model.TableID) model.TableID, c *findings.Collector) error {
	return autoSeedPreflight(ctx, src, dst, allowMissingFK, tables, seedTables, target, c)
}

func autoSeedPreflight(ctx context.Context, src, dst *sql.DB, allowMissingFK bool, tables, seedTables []model.TableID, target func(model.TableID) model.TableID, c *findings.Collector) error {
	if target == nil {
		target = func(id model.TableID) model.TableID { return id }
	}
	scope := make(map[model.TableID]bool, len(tables))
	for _, id := range tables {
		scope[id] = true
	}
	keyless := make(map[model.TableID]bool, len(seedTables))
	for _, id := range seedTables {
		keyless[id] = true
	}
	plan, err := compareMySQLForeignKeys(ctx, src, dst, scope, target, func(id model.TableID) bool { return keyless[id] }, allowMissingFK, c)
	if err != nil {
		return err
	}
	if c != nil {
		if err := reportCreateNameClashes(ctx, dst, plan, target, c); err != nil {
			return err
		}
	}
	if len(plan.checkedTables) == 0 && len(seedTables) == 0 {
		return nil
	}
	sourceDeps, err := readSeedDependencies(ctx, src)
	if err != nil {
		return fmt.Errorf("inspect source foreign keys: %w", err)
	}
	targetDeps, err := readSeedDependencies(ctx, dst)
	if err != nil {
		return fmt.Errorf("inspect target foreign keys: %w", err)
	}
	roots := append([]model.TableID(nil), seedTables...)
	for id := range plan.checkedTables {
		roots = append(roots, id)
	}
	byTarget := make(map[model.TableID]model.TableID, len(tables))
	for _, id := range tables {
		byTarget[target(id)] = id
	}
	group, err := closeAutoSeedGroup(scope, sourceDeps, targetDeps, byTarget, roots, target, c)
	if err != nil {
		return err
	}
	existing := map[model.TableID]bool{}
	for _, id := range group {
		dstID := target(id)
		present, err := schemagen.Exists(ctx, dst, "mysql", dstID)
		if err != nil {
			return err
		}
		existing[dstID] = present
		if present {
			if err := requireEmptyForeignKeyTarget(ctx, dst, dstID); err != nil {
				if c == nil || exitcode.GetExitCode(err) != exitcode.Rejected {
					return err
				}
				c.Err(findings.Error, "target_not_empty", dstID.String(), err, fmt.Sprintf(
					"auto-seed copies this foreign-key group into empty target tables: empty %s on the target only if its rows are disposable (TRUNCATE TABLE %s;), or seed the group another way and set bootstrap.auto_seed: false",
					dstID, autoSeedTableSQL(dstID)))
			}
		}
	}
	if c != nil && allowMissingFK {
		reportAutoSeedRepairs(ctx, dst, plan, existing, c)
	}
	return nil
}

const autoSeedBinlogCheckInterval = 30 * time.Second

// autoSeedBinlogGuard verifies that the snapshot's resume point is still
// available while a potentially long initial copy is in progress. checkedAt
// keeps the throttle monotonic because time.Now values carry Go's monotonic
// clock reading.
type autoSeedBinlogGuard struct {
	check     func(context.Context) error
	checkedAt time.Time
}

func (g *autoSeedBinlogGuard) checkIfDue(ctx context.Context) error {
	if g == nil || g.check == nil {
		return nil
	}
	now := time.Now()
	if !g.checkedAt.IsZero() && now.Sub(g.checkedAt) < autoSeedBinlogCheckInterval {
		return nil
	}
	if err := g.check(ctx); err != nil {
		return err
	}
	g.checkedAt = time.Now()
	return nil
}

// waitUntil lets the periodic retention check run even when IOPS throttling
// pauses between INSERT batches for longer than the check interval.
func (g *autoSeedBinlogGuard) waitUntil(ctx context.Context, deadline time.Time) error {
	for {
		if err := g.checkIfDue(ctx); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		wait := min(remaining, autoSeedBinlogCheckInterval)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func readSeedDependencies(ctx context.Context, db *sql.DB) ([]seedDependency, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME
		FROM information_schema.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_NAME IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []seedDependency
	for rows.Next() {
		var e seedDependency
		if err := rows.Scan(&e.child.Schema, &e.child.Table, &e.parent.Schema, &e.parent.Table); err != nil {
			return nil, err
		}
		deps = append(deps, e)
	}
	return deps, rows.Err()
}

// closeAutoSeedGroup is the shared graph closure used by Start and Validate.
// Edges are undirected because a child and its parent must be in the same
// consistent initial copy. Source edges use source IDs; target edges use the
// renamed target IDs.
func closeAutoSeedGroup(scope map[model.TableID]bool, source, target []seedDependency, targets map[model.TableID]model.TableID, roots []model.TableID, mapTarget func(model.TableID) model.TableID, c *findings.Collector) ([]model.TableID, error) {
	neighbors := map[model.TableID][]model.TableID{}
	add := func(a, b model.TableID) {
		neighbors[a] = append(neighbors[a], b)
		neighbors[b] = append(neighbors[b], a)
	}
	for _, edge := range source {
		add(edge.child, edge.parent)
	}
	externalTarget := map[model.TableID]model.TableID{}
	for _, edge := range target {
		a, hasA := targets[edge.child]
		b, hasB := targets[edge.parent]
		switch {
		case hasA && hasB:
			add(a, b)
		case hasA:
			externalTarget[a] = edge.parent
		case hasB:
			externalTarget[b] = edge.child
		}
	}
	var result []model.TableID
	seen := map[model.TableID]bool{}
	for len(roots) > 0 {
		id := roots[len(roots)-1]
		roots = roots[:len(roots)-1]
		if seen[id] {
			continue
		}
		if !scope[id] {
			e := autoSeedRejected(fmt.Sprintf("foreign-key group requires source table %s in the replication scope", id))
			if c == nil {
				return nil, e
			}
			c.Err(findings.Error, "autoseed_fk_out_of_scope", id.String(), e, fmt.Sprintf("add %s to tables.include: tables linked by foreign keys are copied together", id))
			seen[id] = true
			continue
		}
		if outside, ok := externalTarget[id]; ok {
			mapped := id
			if mapTarget != nil {
				mapped = mapTarget(id)
			}
			e := autoSeedRejected(fmt.Sprintf("target %s has a foreign-key dependency on %s outside the replication scope", mapped, outside))
			if c == nil {
				return nil, e
			}
			c.Err(findings.Error, "autoseed_target_fk_external", mapped.String(), e, fmt.Sprintf("add the source table of %s to tables.include, or drop that foreign key on the target", outside))
		}
		seen[id] = true
		result = append(result, id)
		roots = append(roots, neighbors[id]...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result, nil
}

// autoSeedTables closes the selected tables over foreign keys in both databases.
// A connected group cannot straddle online snapshot and the consistent initial copy.
func (p *Pipeline) autoSeedTables(ctx context.Context, cascade mysqlCascadePlan) ([]model.TableID, error) {
	source, err := readSeedDependencies(ctx, p.src)
	if err != nil {
		return nil, fmt.Errorf("inspect source foreign keys: %w", err)
	}
	target, err := readSeedDependencies(ctx, p.dst)
	if err != nil {
		return nil, fmt.Errorf("inspect target foreign keys: %w", err)
	}
	targets := make(map[model.TableID]model.TableID, len(p.tables))
	roots := make([]model.TableID, 0)
	for id, table := range p.tables {
		targets[p.filter.Target(id)] = id
		if table.Keyless || cascade.parents[id] || cascade.children[id] || cascade.checkedTables[id] {
			roots = append(roots, id)
		}
	}
	scope := make(map[model.TableID]bool, len(p.tables))
	for id := range p.tables {
		scope[id] = true
	}
	return closeAutoSeedGroup(scope, source, target, targets, roots, p.filter.Target, nil)
}

// autoSeed runs before CDC and only with an empty checkpoint. Rows and the new
// binlog boundary become visible in the same transaction on the destination.
func (p *Pipeline) autoSeed(ctx context.Context, empty checkpoint.AppliedSet) (checkpoint.AppliedSet, error) {
	p.autoSeedTotal.Store(0)
	p.autoSeedDone.Store(0)
	p.autoSeedRows.Store(0)
	p.autoSeedTable.Store(nil)
	p.autoSeeding.Store(true)
	defer p.autoSeeding.Store(false)
	if err := p.clearPartialAutoSeed(ctx); err != nil {
		return nil, err
	}
	scope := make(map[model.TableID]bool, len(p.tables))
	var ids []model.TableID
	for id := range p.tables {
		scope[id] = true
		ids = append(ids, id)
	}
	cascades, err := readSourceForeignKeys(ctx, p.src, scope, false)
	if err != nil {
		return nil, err
	}
	needed := len(cascades) > 0
	for _, m := range p.tables {
		needed = needed || m.Keyless
	}
	if !needed {
		return empty, p.initMySQLCascade(ctx)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	source, pos, err := p.openAutoSeedSnapshot(ctx, ids)
	if err != nil {
		return nil, err
	}
	defer closeAutoSeedConn(source)

	// Pin table definitions before using them for schema creation and the copy.
	fresh, err := p.loadTables(ctx, p.filter)
	if err != nil {
		return nil, err
	}
	if len(fresh) != len(ids) {
		return nil, autoSeedRejected("source table scope changed while opening the snapshot; retry")
	}
	for _, id := range ids {
		m := fresh[id]
		if m == nil {
			return nil, autoSeedRejected("source table scope changed while opening the snapshot; retry")
		}
		if len(m.PK) == 0 {
			return nil, p.missingKeyError(ctx, id, m)
		}
	}
	p.tablesMu.Lock() // the admin socket reads p.tables during the copy
	p.tables = fresh
	p.tablesMu.Unlock()
	if p.cfg.Bootstrap.CreateSchemaAuto() {
		repairs, err := p.preflightAutoSeedForeignKeys(ctx)
		if err != nil {
			return nil, err
		}
		if err := p.createTargetTables(ctx, ids); err != nil {
			return nil, err
		}
		if err := p.addMissingTargetCascades(ctx, repairs); err != nil {
			return nil, err
		}
	}
	if err := p.initMySQLCascade(ctx); err != nil {
		return nil, err
	}
	group, err := p.autoSeedTables(ctx, p.cascade)
	if err != nil {
		return nil, err
	}
	if len(group) == 0 {
		return empty, nil
	}
	if err := p.checkTargetEngines(ctx); err != nil {
		return nil, err
	}
	for _, id := range group {
		if err := p.checkAutoSeedTable(ctx, id); err != nil {
			return nil, err
		}
	}
	for _, id := range ids {
		if !tableInSortedGroup(group, id) {
			if err := p.checkSnapshotKey(ctx, id, p.tables[id]); err != nil {
				return nil, err
			}
		}
	}
	if err := p.loadTargetGenerated(ctx, p.tables); err != nil {
		return nil, err
	}
	base, key, err := checkpoint.ParseBinlogStartAt(fmt.Sprintf("%s:%d", pos.File, pos.Position))
	if err != nil {
		return nil, err
	}
	set := checkpoint.NewMySQLPosAppliedSet(base, key)
	set.SetSource(p.srcIdent)
	p.log.Info("auto-seed initial copy started", "tables", len(group), "file", pos.File, "position", pos.Position)
	if err := p.copyAutoSeed(ctx, source, group, pos, set); err != nil {
		return nil, err
	}
	return set, p.state.resolveAutoSeed(true)
}

// checkSeededTargetTables runs when the pipeline already has a checkpoint, so
// auto-seed will neither create nor copy tables again. A target table missing
// then (dropped to start over, or the wrong target) would otherwise surface as
// a confusing foreign-key or apply error.
func (p *Pipeline) checkSeededTargetTables(ctx context.Context) error {
	bySchema := map[string][]model.TableID{}
	for id := range p.tables {
		if !p.state.IsDone(id.String()) {
			continue // a pending snapshot still creates it (bootstrap.create_schema: auto)
		}
		dst := p.filter.Target(id)
		bySchema[dst.Schema] = append(bySchema[dst.Schema], dst)
	}
	var missing []string
	for schema, tables := range bySchema {
		rows, err := p.dst.QueryContext(ctx, "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=?", schema)
		if err != nil {
			return fmt.Errorf("auto-seed: list target tables of %s: %w", schema, err)
		}
		present := map[string]bool{}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			present[strings.ToLower(name)] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range tables {
			if !present[strings.ToLower(id.Table)] {
				missing = append(missing, id.String())
			}
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	shown := missing
	if len(shown) > 10 {
		shown = append(shown[:10:10], fmt.Sprintf("and %d more", len(missing)-10))
	}
	return autoSeedRejected(fmt.Sprintf("pipeline %s already has a checkpoint, so it will not create or copy tables again, but %d copied target table(s) are missing: %s; "+
		"restore them from the seed, or start over with a new pipeline name (or after removing this pipeline's checkpoint and state)",
		p.id, len(missing), strings.Join(shown, ", ")))
}

func tableInSortedGroup(group []model.TableID, id model.TableID) bool {
	name := id.String()
	i := sort.Search(len(group), func(i int) bool { return group[i].String() >= name })
	return i < len(group) && group[i] == id
}

func autoSeedTableSQL(id model.TableID) string {
	return mysqlFKQuote(id.Schema) + "." + mysqlFKQuote(id.Table)
}

func closeAutoSeedConn(conn *sql.Conn) {
	// Never put a session that may still own locks or a transaction back in the pool.
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}

// preflightAutoSeedForeignKeys checks the entire plan before any target schema
// changes. Missing child tables get their FKs from schemagen; only empty, existing
// child tables are eligible for additional ALTER statements.
func (p *Pipeline) preflightAutoSeedForeignKeys(ctx context.Context) ([]cascadeFK, error) {
	plan, err := p.compareMySQLForeignKeys(ctx, true)
	if err != nil {
		return nil, err
	}
	group, err := p.autoSeedTables(ctx, plan)
	if err != nil {
		return nil, err
	}

	existing := map[model.TableID]bool{}
	for _, id := range group {
		dst := p.filter.Target(id)
		present, err := schemagen.Exists(ctx, p.dst, p.cfg.Target.Kind, dst)
		if err != nil {
			return nil, err
		}
		existing[dst] = present
		if present {
			if err := requireEmptyForeignKeyTarget(ctx, p.dst, dst); err != nil {
				return nil, err
			}
		}
	}
	var repairs []cascadeFK
	for _, fk := range plan.missing {
		for _, id := range []model.TableID{fk.Child, fk.Parent} {
			if _, checked := existing[id]; checked {
				continue
			}
			present, err := schemagen.Exists(ctx, p.dst, p.cfg.Target.Kind, id)
			if err != nil {
				return nil, err
			}
			existing[id] = present
			if present {
				if err := requireEmptyForeignKeyTarget(ctx, p.dst, id); err != nil {
					return nil, err
				}
			}
		}
		for _, side := range []struct {
			table               model.TableID
			columns, collations []string
		}{
			{fk.Child, fk.Columns, fk.ChildCollations}, {fk.Parent, fk.References, fk.ParentCollations},
		} {
			if !existing[side.table] {
				continue
			}
			collations, err := readFKCollations(ctx, p.dst, side.table, side.columns)
			if err != nil {
				return nil, fmt.Errorf("auto-seed: inspect foreign-key columns on target %s: %w", side.table, err)
			}
			if !equivalentFKCollations(collations, side.collations) {
				return nil, autoSeedRejected(fmt.Sprintf("target %s foreign-key columns (%s) must use the source collations before automatic schema creation", side.table, strings.Join(side.columns, ", ")))
			}
		}
		if existing[fk.Child] {
			repairs = append(repairs, fk)
		}
	}
	return repairs, nil
}

type autoSeedQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func requireEmptyForeignKeyTarget(ctx context.Context, db autoSeedQueryer, id model.TableID) error {
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM "+autoSeedTableSQL(id)+" LIMIT 1").Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("auto-seed: inspect target %s: %w", id, err)
	}
	return autoSeedRejected(fmt.Sprintf("target %s already contains data; automatic foreign-key changes require empty tables", id))
}

func (p *Pipeline) addMissingTargetCascades(ctx context.Context, repairs []cascadeFK) error {
	if len(repairs) == 0 {
		return nil
	}
	conn, err := p.dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer closeAutoSeedConn(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1"); err != nil {
		return err
	}
	for _, fk := range repairs {
		for _, id := range []model.TableID{fk.Child, fk.Parent} {
			if err := requireEmptyForeignKeyTarget(ctx, conn, id); err != nil {
				return err
			}
		}
		stmt := cascadeAddSQL(fk, fk.constraint)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return autoSeedRejected(fmt.Sprintf("add missing foreign key on target %s: %v (statement: %s)", fk.Child, err, stmt))
		}
		action := "added a foreign key missing on the empty target table"
		if fk.cascades() {
			action = "added a cascading foreign key missing on the empty target table"
		}
		p.log.Warn("auto-seed: "+action, "table", fk.Child.String(), "references", fk.Parent.String(), "constraint", fk.constraint)
	}
	return nil
}

func (p *Pipeline) openAutoSeedSnapshot(ctx context.Context, ids []model.TableID) (*sql.Conn, mysqlreader.BinlogStatus, error) {
	if ver, err := mysqlver.Cached(ctx, p.src); err != nil {
		return nil, mysqlreader.BinlogStatus{}, err
	} else if ver.IsMariaDB() {
		// MariaDB reports the binlog position of a consistent snapshot, so no
		// global read lock (and no RELOAD privilege) is needed.
		for attempt := 0; attempt < 5; attempt++ {
			conn, pos, again, err := p.openMariaDBSnapshot(ctx, ids)
			if err != nil || !again {
				return conn, pos, err
			}
			p.log.Warn("auto-seed: source wrote DDL while the snapshot was opening; retrying")
		}
		return nil, mysqlreader.BinlogStatus{}, autoSeedRejected("source kept writing DDL while the snapshot was opening; retry when the schema is stable")
	}
	return p.openFlushLockSnapshot(ctx, ids)
}

// openMariaDBSnapshot opens a consistent snapshot and reads its binlog
// position from Binlog_snapshot_file/position. Table definitions are pinned
// after the snapshot; again=true when the binlog between the snapshot and the
// pin holds a statement that may be DDL, so the caller retries.
func (p *Pipeline) openMariaDBSnapshot(ctx context.Context, ids []model.TableID) (conn *sql.Conn, pos mysqlreader.BinlogStatus, again bool, err error) {
	setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	c, err := p.src.Conn(setupCtx)
	if err != nil {
		return nil, pos, false, err
	}
	conn = c
	defer func() {
		if err != nil || again {
			closeAutoSeedConn(c)
			conn = nil
		}
	}()
	if _, err = conn.ExecContext(setupCtx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return nil, pos, false, err
	}
	if _, err = conn.ExecContext(setupCtx, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {
		return nil, pos, false, err
	}
	rows, err := conn.QueryContext(setupCtx, "SHOW SESSION STATUS LIKE 'binlog_snapshot_%'")
	if err != nil {
		return nil, pos, false, fmt.Errorf("auto-seed: read snapshot binlog position: %w", err)
	}
	for rows.Next() {
		var name, value string
		if err = rows.Scan(&name, &value); err != nil {
			rows.Close()
			return nil, pos, false, err
		}
		switch strings.ToLower(name) {
		case "binlog_snapshot_file":
			pos.File = value
		case "binlog_snapshot_position":
			if _, err = fmt.Sscan(value, &pos.Position); err != nil {
				rows.Close()
				return nil, pos, false, fmt.Errorf("auto-seed: parse Binlog_snapshot_position %q: %w", value, err)
			}
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, pos, false, err
	}
	if pos.File == "" || pos.Position == 0 {
		return nil, pos, false, autoSeedRejected("source did not report Binlog_snapshot_file/position; binary logging must be enabled")
	}
	if err = pinAutoSeedTables(setupCtx, conn, ids); err != nil {
		return nil, pos, false, err
	}
	head, err := mysqlreader.ReadBinlogStatus(setupCtx, p.src)
	if err != nil {
		return nil, pos, false, err
	}
	if head.File != pos.File {
		return conn, pos, true, nil // binlog rotated in between; a fresh attempt is simpler than following it
	}
	for from := pos.Position; from < head.Position; {
		events, err := readBinlogEvents(setupCtx, p.src, pos.File, from)
		if err != nil {
			return nil, pos, false, fmt.Errorf("auto-seed: check source binlog after the snapshot: %w", err)
		}
		if len(events) == 0 {
			break
		}
		for _, e := range events {
			if e.end > head.Position {
				break
			}
			if strings.EqualFold(e.kind, "Query") && !transactionControlQuery(e.info) {
				return conn, pos, true, nil
			}
		}
		next := events[len(events)-1].end
		if next <= from {
			return nil, pos, false, fmt.Errorf("auto-seed: binlog cursor did not advance at %s:%d", pos.File, from)
		}
		from = next
	}
	return conn, pos, false, nil
}

// transactionControlQuery: Query events that every row-based transaction writes.
func transactionControlQuery(info string) bool {
	q := strings.ToUpper(strings.TrimSpace(info))
	for _, prefix := range []string{"BEGIN", "COMMIT", "ROLLBACK", "XA ", "SAVEPOINT", "RELEASE SAVEPOINT"} {
		if strings.HasPrefix(q, prefix) {
			return true
		}
	}
	return false
}

func pinAutoSeedTables(ctx context.Context, source *sql.Conn, ids []model.TableID) error {
	// Pin several definitions per round trip. Stay below the 61-table MySQL join limit.
	for batch := range slices.Chunk(ids, 32) {
		refs := make([]string, 0, len(batch))
		for i, id := range batch {
			refs = append(refs, fmt.Sprintf("%s AS t%d", autoSeedTableSQL(id), i))
		}
		rows, err := source.QueryContext(ctx, "SELECT 1 FROM "+strings.Join(refs, " CROSS JOIN ")+" LIMIT 0")
		if err != nil {
			return fmt.Errorf("pin source table definitions starting at %s: %w", batch[0], err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) openFlushLockSnapshot(ctx context.Context, ids []model.TableID) (*sql.Conn, mysqlreader.BinlogStatus, error) {
	// Bound the lock wait and the lock interval. UNLOCK TABLES releases the
	// global flush lock without committing this session's InnoDB snapshot.
	lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	source, err := p.src.Conn(lockCtx)
	if err != nil {
		return nil, mysqlreader.BinlogStatus{}, err
	}
	var opened bool
	defer func() {
		if !opened {
			closeAutoSeedConn(source)
		}
	}()
	if _, err := source.ExecContext(lockCtx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		return nil, mysqlreader.BinlogStatus{}, fmt.Errorf("auto-seed: acquire short source read lock (MySQL sources need the RELOAD privilege for FLUSH TABLES WITH READ LOCK; MariaDB sources do not): %w", err)
	}
	if _, err := source.ExecContext(lockCtx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return nil, mysqlreader.BinlogStatus{}, err
	}
	if _, err := source.ExecContext(lockCtx, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {
		return nil, mysqlreader.BinlogStatus{}, err
	}
	// Batched pinning keeps the global read lock short even for a remote source.
	if err := pinAutoSeedTables(lockCtx, source, ids); err != nil {
		return nil, mysqlreader.BinlogStatus{}, err
	}
	pos, err := mysqlreader.ReadBinlogStatus(lockCtx, source)
	if err != nil {
		return nil, mysqlreader.BinlogStatus{}, err
	}
	if _, err := source.ExecContext(lockCtx, "UNLOCK TABLES"); err != nil {
		return nil, mysqlreader.BinlogStatus{}, err
	}
	opened = true
	return source, pos, nil
}

func (p *Pipeline) checkAutoSeedTable(ctx context.Context, id model.TableID) error {
	var engine, kind string
	if err := p.src.QueryRowContext(ctx, "SELECT COALESCE(ENGINE,''), TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
		id.Schema, id.Table).Scan(&engine, &kind); err != nil {
		return err
	}
	if !strings.EqualFold(engine, "InnoDB") || kind != "BASE TABLE" {
		return autoSeedRejected(fmt.Sprintf("source table %s must be a regular InnoDB table for a consistent snapshot", id))
	}
	dst := p.filter.Target(id)
	var triggers int
	if err := p.dst.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?",
		dst.Schema, dst.Table).Scan(&triggers); err != nil {
		return err
	}
	if triggers != 0 {
		return autoSeedRejected(fmt.Sprintf("target %s has triggers that could change the initial copy or other tables", dst))
	}
	return nil
}

func (p *Pipeline) copyAutoSeed(ctx context.Context, source *sql.Conn, group []model.TableID, pos mysqlreader.BinlogStatus, set *checkpoint.MySQLPosAppliedSet) error {
	budget, err := p.autoSeedCommitBudget(ctx)
	if err != nil {
		return err
	}
	conn, err := p.dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer closeAutoSeedConn(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode=?", mysqlTargetSessionSQLMode+",NO_AUTO_VALUE_ON_ZERO"); err != nil {
		return err
	}
	target := &autoSeedTarget{p: p, conn: conn, budget: budget, group: group}
	if err := target.begin(ctx); err != nil {
		return err
	}
	defer target.rollback()
	for _, id := range group {
		dst := p.filter.Target(id)
		var exists int
		err := target.tx.QueryRowContext(ctx, "SELECT 1 FROM "+autoSeedTableSQL(dst)+" LIMIT 1 FOR UPDATE").Scan(&exists)
		if err == nil {
			return autoSeedRejected(fmt.Sprintf("target %s already contains data", dst))
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if budget > 0 {
		p.log.Info("auto-seed target uses Group Replication: committing the copy in parts", "part_bytes", budget)
	}
	iops := p.cfg.Resources.Source.IOPS.Soft
	if iops <= 0 {
		iops = 600
	}
	binlogGuard := &autoSeedBinlogGuard{check: func(ctx context.Context) error {
		return p.checkBinlogPosition(ctx, pos.File, pos.Position)
	}}
	if err := binlogGuard.checkIfDue(ctx); err != nil {
		return err
	}
	p.autoSeedTotal.Store(int64(len(group)))
	names := make([]string, len(group))
	for i, id := range group {
		names[i] = id.String()
	}
	p.autoSeedMu.Lock()
	p.autoSeedGroup, p.autoSeedCopied = names, map[string]int64{}
	p.autoSeedMu.Unlock()
	noteCopied := func(name string, rows int64) {
		p.autoSeedMu.Lock()
		p.autoSeedCopied[name] = rows
		p.autoSeedMu.Unlock()
	}
	for _, id := range group {
		name := id.String()
		p.autoSeedTable.Store(&name)
		lastLog, base := time.Now(), p.autoSeedRows.Load()
		progress := func(copied int64) {
			p.autoSeedRows.Store(base + copied)
			noteCopied(name, copied)
			if time.Since(lastLog) >= autoSeedProgressLogEvery {
				lastLog = time.Now()
				p.log.Info("auto-seed copy in progress", "table", name, "rows", copied,
					"tables_done", p.autoSeedDone.Load(), "tables_total", len(group))
			}
		}
		rows, err := copyAutoSeedTable(ctx, source, target, id, p.filter.Target(id), writableColumns(p.tables[id]), float64(iops)*16*1024, binlogGuard, progress)
		if err != nil {
			var me *drivermysql.MySQLError
			if errors.As(err, &me) && me.Number == 3098 {
				return autoSeedRejected(fmt.Sprintf("target %s rejects rows: Group Replication requires a primary key (or a NOT NULL unique key) on every table: %v", p.filter.Target(id), err))
			}
			return fmt.Errorf("auto-seed copy %s: %w", id, err)
		}
		noteCopied(name, rows)
		p.autoSeedDone.Add(1)
		p.log.Info("auto-seed table copied (awaiting checkpoint commit)", "table", name, "rows", rows)
	}
	p.autoSeedTable.Store(nil)
	if err := p.checkBinlogPosition(ctx, pos.File, pos.Position); err != nil {
		return err
	}
	store := p.store.(*checkpoint.MySQLStore)
	if err := store.WriteGlobalTx(ctx, target.tx, checkpoint.Global{PipelineID: p.id, Epoch: p.epoch, Applied: set.String()}); err != nil {
		return err
	}
	if err := p.state.prepareAutoSeed(group); err != nil {
		return err
	}
	if err := target.tx.Commit(); err != nil {
		return fmt.Errorf("commit auto-seed and its checkpoint: %w", err)
	}
	target.tx = nil
	if err := p.state.clearAutoSeedPartial(); err != nil {
		return err
	}
	p.log.Info("auto-seed committed", "tables", len(group), "parts", target.parts+1, "file", pos.File, "position", pos.Position)
	return nil
}

// Group Replication rejects, at commit, a transaction larger than
// group_replication_transaction_size_limit (error 3100 in the before_commit
// hook), and very large transactions stall the group even below it.
const autoSeedMaxPartBytes = 64 << 20

// autoSeedCommitBudget returns how many copied bytes one target transaction
// may hold: 0 (the whole copy and its checkpoint in one transaction) unless
// the target has the Group Replication plugin.
func (p *Pipeline) autoSeedCommitBudget(ctx context.Context) (int64, error) {
	var limit sql.NullInt64
	err := p.dst.QueryRowContext(ctx, "SELECT @@GLOBAL.group_replication_transaction_size_limit").Scan(&limit)
	var me *drivermysql.MySQLError
	if errors.As(err, &me) && me.Number == 1193 { // unknown system variable: no plugin
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("auto-seed: read target group_replication_transaction_size_limit: %w", err)
	}
	budget := int64(autoSeedMaxPartBytes)
	// The copy counts value bytes; row events and write sets add to them.
	if limit.Valid && limit.Int64 > 0 && limit.Int64/4 < budget {
		budget = max(limit.Int64/4, 1)
	}
	return budget, nil
}

// targetGroupReplicationActive reports whether the MySQL target is a member of
// a running Group Replication group.
func targetGroupReplicationActive(ctx context.Context, db *sql.DB) (bool, error) {
	var name sql.NullString
	err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.group_replication_group_name").Scan(&name)
	var me *drivermysql.MySQLError
	if errors.As(err, &me) && me.Number == 1193 { // unknown system variable: no plugin
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read target group_replication_group_name: %w", err)
	}
	if !name.Valid || name.String == "" {
		return false, nil
	}
	var state string
	err = db.QueryRowContext(ctx, "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return true, nil // performance_schema not readable: the plugin is configured with a group
	}
	return state != "OFFLINE", nil
}

// checkGroupReplicationKeyless refuses, before any target change, source
// tables without a key whose target table would have none: Group Replication
// rejects their rows (error 3098), so the copy or CDC would fail later.
func (p *Pipeline) checkGroupReplicationKeyless(ctx context.Context) error {
	if p.dstEngine != model.EngineMySQL {
		return nil
	}
	var keyless []model.TableID
	for id, m := range p.tables {
		if m.Keyless {
			keyless = append(keyless, id)
		}
	}
	blocked, err := groupReplicationKeylessBlocked(ctx, p.dst, p.cfg.Target.Kind, keyless, p.filter.Target, p.cfg.Bootstrap.CreateSchemaAuto())
	if err != nil || len(blocked) == 0 {
		return err
	}
	names := make([]string, len(blocked))
	for i, id := range blocked {
		names[i] = id.String()
	}
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
		"the target uses Group Replication, which rejects rows of tables without a primary key (error 3098), and these source tables have no key: %s. "+
			"Before Strand creates their target tables, set sql_generate_invisible_primary_key=ON on every group member (MySQL 8.0.30+, SET PERSIST) "+
			"so they get an invisible primary key; or add a key on the source, add one to an existing target table, or exclude the tables",
		strings.Join(names, ", ")), nil)
}

// groupReplicationKeylessBlocked trả các bảng nguồn keyless (đã sắp xếp) mà bảng target sẽ không có khóa trong khi
// target đang chạy Group Replication.
func groupReplicationKeylessBlocked(ctx context.Context, dst *sql.DB, targetKind string, keyless []model.TableID, mapTarget func(model.TableID) model.TableID, createSchemaAuto bool) ([]model.TableID, error) {
	if len(keyless) == 0 {
		return nil, nil
	}
	active, err := targetGroupReplicationActive(ctx, dst)
	if err != nil || !active {
		return nil, err
	}
	var gipk sql.NullInt64
	if err := dst.QueryRowContext(ctx, "SELECT @@SESSION.sql_generate_invisible_primary_key").Scan(&gipk); err != nil {
		var me *drivermysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1193 { // before MySQL 8.0.30 the variable does not exist
			return nil, fmt.Errorf("read target sql_generate_invisible_primary_key: %w", err)
		}
	}
	var blocked []model.TableID
	for _, id := range keyless {
		to := mapTarget(id)
		present, err := schemagen.Exists(ctx, dst, targetKind, to)
		if err != nil {
			return nil, err
		}
		if !present {
			if gipk.Int64 == 0 || !createSchemaAuto {
				blocked = append(blocked, id)
			}
			continue
		}
		// Group Replication accepts a primary key or a unique key on NOT NULL columns.
		var one int
		err = dst.QueryRowContext(ctx, "SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0 GROUP BY INDEX_NAME HAVING SUM(NULLABLE='YES')=0 LIMIT 1",
			to.Schema, to.Table).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			blocked = append(blocked, id)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect keys of target %s: %w", to, err)
		}
	}
	sort.Slice(blocked, func(i, j int) bool { return blocked[i].String() < blocked[j].String() })
	return blocked, nil
}

// autoSeedTarget is the target side of an auto-seed copy. Without a budget it
// is one transaction that also commits the checkpoint, so an interrupted copy
// leaves the target empty. With a budget it commits every budget bytes; before
// the first such commit the local state records the group's target tables, and
// the next attempt empties them (clearAutoSeedPartial) before copying again.
type autoSeedTarget struct {
	p       *Pipeline
	conn    *sql.Conn
	tx      *sql.Tx
	budget  int64
	pending int64
	parts   int
	group   []model.TableID
}

func (t *autoSeedTarget) begin(ctx context.Context) error {
	tx, err := t.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	t.tx = tx
	return checkpoint.CheckFence(ctx, tx, false)
}

func (t *autoSeedTarget) rollback() {
	if t.tx != nil {
		_ = t.tx.Rollback()
	}
}

// wrote accounts for bytes inserted in the open transaction and commits it
// once the budget is reached.
func (t *autoSeedTarget) wrote(ctx context.Context, bytes int64) error {
	if t.budget == 0 {
		return nil
	}
	if t.pending += bytes; t.pending < t.budget {
		return nil
	}
	if t.parts == 0 {
		targets := make([]string, 0, len(t.group))
		for _, id := range t.group {
			targets = append(targets, t.p.filter.Target(id).String())
		}
		if err := t.p.state.markAutoSeedPartial(targets); err != nil {
			return err
		}
	}
	if err := t.tx.Commit(); err != nil {
		t.tx = nil
		return fmt.Errorf("commit auto-seed part: %w", err)
	}
	t.parts++
	t.pending = 0
	t.p.log.Info("auto-seed part committed", "parts", t.parts)
	return t.begin(ctx)
}

// clearPartialAutoSeed empties the target tables an interrupted chunked
// auto-seed committed rows into. It runs only without a checkpoint, so CDC has
// never applied to them; TRUNCATE is DDL and so not bound by the Group
// Replication transaction size limit.
func (p *Pipeline) clearPartialAutoSeed(ctx context.Context) error {
	tables := p.state.autoSeedPartial()
	if len(tables) == 0 {
		return nil
	}
	// The journal describes the previous copy. Validate every entry before the
	// first TRUNCATE: an edited scope or rename may no longer own one of them.
	currentTargets := make(map[string]model.TableID, len(p.tables))
	for id := range p.tables {
		dst := p.filter.Target(id)
		currentTargets[dst.String()] = dst
	}
	var outside []string
	for _, name := range tables {
		if _, ok := currentTargets[name]; !ok {
			outside = append(outside, name)
		}
	}
	if len(outside) > 0 {
		sort.Strings(outside)
		return autoSeedRejected(fmt.Sprintf("interrupted auto-seed has target tables outside the current replication scope: %s (excluded, mapped to another name, or dropped at the source); Strand does not empty a table it no longer replicates. Restore the previous table selection and mappings to finish that copy, or abandon it: empty the target tables it copied and drop the checkpoint database, so the next Start begins over", strings.Join(outside, ", ")))
	}
	conn, err := p.dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer closeAutoSeedConn(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks=0"); err != nil {
		return err
	}
	for _, name := range tables {
		_, err := conn.ExecContext(ctx, "TRUNCATE TABLE "+autoSeedTableSQL(currentTargets[name]))
		var me *drivermysql.MySQLError
		if errors.As(err, &me) && me.Number == 1146 { // table no longer exists
			continue
		}
		if err != nil {
			return fmt.Errorf("auto-seed: empty %s left partly copied by an interrupted attempt: %w", name, err)
		}
	}
	p.log.Warn("auto-seed: emptied target tables partly copied by an interrupted attempt", "tables", strings.Join(tables, ", "))
	return p.state.clearAutoSeedPartial()
}

// autoSeedProgressLogEvery spaces the progress lines of a long table copy.
const autoSeedProgressLogEvery = 30 * time.Second

// copyAutoSeedTable copies one table inside the seed transaction. progress,
// when set, receives the rows copied so far after each batch.
func copyAutoSeedTable(ctx context.Context, source *sql.Conn, target *autoSeedTarget, from, to model.TableID, columns []string, bytesPerSecond float64, binlogGuard *autoSeedBinlogGuard, progress func(int64)) (int64, error) {
	if len(columns) == 0 {
		return 0, autoSeedRejected("table has no writable columns")
	}
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, mysqlFKQuote(col))
	}
	list := strings.Join(names, ",")
	rows, err := source.QueryContext(ctx, "SELECT "+list+" FROM "+autoSeedTableSQL(from))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	const maxRows, maxBytes = 128, 256 * 1024
	valueList := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
	insert := "INSERT INTO " + autoSeedTableSQL(to) + " (" + list + ") VALUES "
	batch := make([]any, 0, len(columns)*maxRows)
	values, scans := make([]any, len(columns)), make([]any, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}
	var n int64
	var size int
	start := time.Now()
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if pause := time.Duration(float64(size)/bytesPerSecond*float64(time.Second)) - time.Since(start); pause > 0 {
			if err := binlogGuard.waitUntil(ctx, time.Now().Add(pause)); err != nil {
				return err
			}
		} else if err := binlogGuard.checkIfDue(ctx); err != nil {
			return err
		}
		count := len(batch) / len(columns)
		if _, err := target.tx.ExecContext(ctx, insert+valueList+strings.Repeat(","+valueList, count-1), batch...); err != nil {
			return err
		}
		n += int64(count)
		if progress != nil {
			progress(n)
		}
		if err := target.wrote(ctx, int64(size)); err != nil {
			return err
		}
		clear(batch)
		batch, size, start = batch[:0], 0, time.Now()
		return nil
	}
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return n, err
		}
		for _, v := range values {
			switch value := v.(type) {
			case []byte:
				size += len(value) + 8
			case string:
				size += len(value) + 8
			default:
				size += 16
			}
		}
		batch = append(batch, values...)
		if len(batch)/len(columns) >= maxRows || len(batch)+len(columns) > 65535 || size >= maxBytes {
			if err := flush(); err != nil {
				return n, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return n, err
	}
	err = flush()
	return n, err
}
