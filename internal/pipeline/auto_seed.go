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

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/mysqlver"
	mysqlreader "strand/internal/reader/mysql"
)

func autoSeedRejected(msg string) error {
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "auto-seed: "+msg, nil)
}

type seedDependency struct {
	child, parent model.TableID
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

// autoSeedTables closes the selected tables over foreign keys in both databases.
// A connected group cannot straddle online snapshot and the consistent initial copy.
func (p *Pipeline) autoSeedTables(ctx context.Context) ([]model.TableID, error) {
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
		if table.Keyless || p.cascade.parents[id] || p.cascade.children[id] {
			roots = append(roots, id)
		}
	}
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
		if p.tables[id] == nil {
			return nil, autoSeedRejected(fmt.Sprintf("foreign-key group requires source table %s in the replication scope", id))
		}
		if outside, ok := externalTarget[id]; ok {
			return nil, autoSeedRejected(fmt.Sprintf("target %s has a foreign-key dependency on %s outside the replication scope", p.filter.Target(id), outside))
		}
		seen[id] = true
		result = append(result, id)
		roots = append(roots, neighbors[id]...)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	return result, nil
}

// autoSeed runs before CDC and only with an empty checkpoint. Rows and the new
// binlog boundary become visible in the same transaction on the destination.
func (p *Pipeline) autoSeed(ctx context.Context, empty checkpoint.AppliedSet) (checkpoint.AppliedSet, error) {
	scope := make(map[model.TableID]bool, len(p.tables))
	var ids []model.TableID
	for id := range p.tables {
		scope[id] = true
		ids = append(ids, id)
	}
	cascades, err := readSourceCascadingFKs(ctx, p.src, scope)
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
	p.tables = fresh
	if p.cfg.Bootstrap.CreateSchemaAuto() {
		if err := p.createTargetTables(ctx, ids); err != nil {
			return nil, err
		}
		if err := p.addMissingTargetCascades(ctx, scope); err != nil {
			return nil, err
		}
	}
	if err := p.initMySQLCascade(ctx); err != nil {
		return nil, err
	}
	group, err := p.autoSeedTables(ctx)
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

// addMissingTargetCascades recreates source cascading foreign keys that an
// existing target table lacks. It only touches empty target tables: the
// auto-seed copy fills them and requires them to be empty anyway.
func (p *Pipeline) addMissingTargetCascades(ctx context.Context, scope map[model.TableID]bool) error {
	if p.srcEngine != model.EngineMySQL || p.dstEngine != model.EngineMySQL {
		return nil
	}
	source, err := readSourceCascadingFKs(ctx, p.src, scope)
	if err != nil {
		return fmt.Errorf("inspect source cascading foreign keys: %w", err)
	}
	target, err := readCascadingFKs(ctx, p.dst)
	if err != nil {
		return fmt.Errorf("inspect target cascading foreign keys: %w", err)
	}
	pin := checkCascadeOrder(source) != nil
	for _, fk := range source {
		if !scope[fk.Child] || !scope[fk.Parent] || fk.constraint == "" || fk.OnDelete == "SET DEFAULT" || fk.OnUpdate == "SET DEFAULT" {
			continue // planMySQLCascade reports these
		}
		sourceChild := fk.Child
		if pin {
			fk.Name = fk.constraint
		}
		fk.Child, fk.Parent = p.filter.Target(fk.Child), p.filter.Target(fk.Parent)
		present := false
		for _, t := range target {
			// Any constraint on the same columns stays as it is; a mismatch is
			// reported by planMySQLCascade instead of being changed here.
			if t.Child == fk.Child && t.Parent == fk.Parent && slices.Equal(t.Columns, fk.Columns) {
				present = true
				break
			}
		}
		if present {
			continue
		}
		empty := true
		for _, id := range []model.TableID{fk.Child, fk.Parent} {
			var one int
			err := p.dst.QueryRowContext(ctx, "SELECT 1 FROM "+autoSeedTableSQL(id)+" LIMIT 1").Scan(&one)
			if err == nil {
				empty = false
			} else if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("auto-seed: inspect target %s: %w", id, err)
			}
		}
		if !empty {
			continue
		}
		stmt := cascadeAddSQL(fk, p.cascadeTargetName(sourceChild, fk))
		if _, err := p.dst.ExecContext(ctx, stmt); err != nil {
			return autoSeedRejected(fmt.Sprintf("add missing cascading foreign key on target %s: %v (statement: %s)", fk.Child, err, stmt))
		}
		p.log.Warn("auto-seed: added a cascading foreign key missing on the empty target table", "table", fk.Child.String(), "references", fk.Parent.String(), "constraint", p.cascadeTargetName(sourceChild, fk))
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
	conn, err := p.dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer closeAutoSeedConn(conn)
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode=?", mysqlTargetSessionSQLMode+",NO_AUTO_VALUE_ON_ZERO"); err != nil {
		return err
	}
	target, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer target.Rollback()
	if err := checkpoint.CheckFence(ctx, target, false); err != nil {
		return err
	}
	for _, id := range group {
		dst := p.filter.Target(id)
		var exists int
		err := target.QueryRowContext(ctx, "SELECT 1 FROM "+autoSeedTableSQL(dst)+" LIMIT 1 FOR UPDATE").Scan(&exists)
		if err == nil {
			return autoSeedRejected(fmt.Sprintf("target %s already contains data", dst))
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	iops := p.cfg.Resources.Source.IOPS.Soft
	if iops <= 0 {
		iops = 600
	}
	for _, id := range group {
		rows, err := copyAutoSeedTable(ctx, source, target, id, p.filter.Target(id), writableColumns(p.tables[id]), float64(iops)*16*1024)
		if err != nil {
			return fmt.Errorf("auto-seed copy %s: %w", id, err)
		}
		p.log.Info("auto-seed table copied (awaiting checkpoint commit)", "table", id.String(), "rows", rows)
	}
	if err := p.checkBinlogPosition(ctx, pos.File, pos.Position); err != nil {
		return err
	}
	store := p.store.(*checkpoint.MySQLStore)
	if err := store.WriteGlobalTx(ctx, target, checkpoint.Global{PipelineID: p.id, Epoch: p.epoch, Applied: set.String()}); err != nil {
		return err
	}
	if err := p.state.prepareAutoSeed(group); err != nil {
		return err
	}
	if err := target.Commit(); err != nil {
		return fmt.Errorf("commit auto-seed and its checkpoint: %w", err)
	}
	p.log.Info("auto-seed committed", "tables", len(group), "file", pos.File, "position", pos.Position)
	return nil
}

func copyAutoSeedTable(ctx context.Context, source *sql.Conn, target *sql.Tx, from, to model.TableID, columns []string, bytesPerSecond float64) (int64, error) {
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
			timer := time.NewTimer(pause)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
		count := len(batch) / len(columns)
		if _, err := target.ExecContext(ctx, insert+valueList+strings.Repeat(","+valueList, count-1), batch...); err != nil {
			return err
		}
		n += int64(count)
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
