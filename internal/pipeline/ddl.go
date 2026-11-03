package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// spec: 02 §12 — "P1–P2: gặp DDL trên bảng thuộc pipeline thì dừng với mã schema_change_pending.
// resume chỉ chạy tiếp khi schema ở target đã khớp."

var (
	ddlIdent      = "(`[^`]+`|[A-Za-z0-9_$]+)(?:\\s*\\.\\s*(`[^`]+`|[A-Za-z0-9_$]+))?"
	reCreateTable = regexp.MustCompile(`(?is)^\s*(?:/\*.*?\*/\s*)?create\s+table\s+(?:if\s+not\s+exists\s+)?` + ddlIdent)
	reRenameTable = regexp.MustCompile(`(?is)^\s*(?:/\*.*?\*/\s*)?rename\s+tables?\s+(.+)$`)
	reRenamePair  = regexp.MustCompile(`(?is)` + ddlIdent + `\s+to\s+` + ddlIdent)
	reAlterRename = regexp.MustCompile(`(?is)^\s*(?:/\*.*?\*/\s*)?alter\s+table\s+` + ddlIdent + `\s+rename\s+(?:to\s+|as\s+)?` + ddlIdent)
)

func unquoteIdent(s string) string { return strings.Trim(strings.TrimSpace(s), "`") }

// qualify ghép (a, b) từ regex thành TableID: "a.b" hoặc "b" với schema mặc định.
func qualify(a, b, defSchema string) model.TableID {
	if b != "" {
		return model.TableID{Schema: unquoteIdent(a), Table: unquoteIdent(b)}
	}
	return model.TableID{Schema: defSchema, Table: unquoteIdent(a)}
}

// ddlNewTables trả về các bảng mà DDL tạo ra hoặc đổi tên thành (CREATE TABLE, RENAME TABLE, ALTER … RENAME).
// CREATE TEMPORARY TABLE không khớp (không ghi row vào binlog ở chế độ ROW).
func ddlNewTables(ddl, defSchema string) (created, renamedTo []model.TableID) {
	if m := reCreateTable.FindStringSubmatch(ddl); m != nil {
		created = append(created, qualify(m[1], m[2], defSchema))
	}
	if m := reRenameTable.FindStringSubmatch(ddl); m != nil {
		for _, pm := range reRenamePair.FindAllStringSubmatch(m[1], -1) {
			renamedTo = append(renamedTo, qualify(pm[3], pm[4], defSchema))
		}
	}
	if m := reAlterRename.FindStringSubmatch(ddl); m != nil {
		renamedTo = append(renamedTo, qualify(m[3], m[4], defSchema))
	}
	return created, renamedTo
}

// handleDDL quyết định một Txn DDL: không đụng bảng nào của pipeline thì bỏ qua; có đụng thì chỉ đi
// tiếp khi cột của mọi bảng liên quan ở target đã khớp source (DBA đã áp DDL ở target), và nạp lại
// metadata bảng. Bảng MỚI khớp bộ lọc:
//   - CREATE TABLE: bảng rỗng tại thời điểm tạo, mọi row sau đó đi qua stream → thêm vào pipeline, coi như đã snapshot.
//   - RENAME từ bảng KHÔNG thuộc pipeline: row cũ chưa có ở target → dừng; khởi động lại thì bảng được snapshot.
func (p *Pipeline) handleDDL(ctx context.Context, t *model.Txn) error {
	created, renamedTo := ddlNewTables(t.DDL, t.DDLSchema)
	affected := map[model.TableID]bool{}
	newCreated := map[model.TableID]bool{}
	q := strings.ToLower(t.DDL)
	p.tablesMu.RLock()
	for id := range p.tables {
		if strings.Contains(q, strings.ToLower(id.Table)) {
			affected[id] = true
		}
	}
	p.tablesMu.RUnlock()
	for _, id := range created {
		if p.filter.Included(id) && p.table(id) == nil {
			affected[id] = true
			newCreated[id] = true
		}
	}
	var renamedIn []model.TableID
	for _, id := range renamedTo {
		if p.filter.Included(id) && p.table(id) == nil {
			affected[id] = true
			renamedIn = append(renamedIn, id)
		}
	}
	if len(affected) == 0 {
		return nil
	}
	// Router đang giữ routeMu: chờ mọi Txn đã nhận (theo schema cũ) commit xong trước khi so schema và
	// nạp lại metadata, để không Txn cũ nào bị apply theo metadata mới.
	if !p.drain(10 * time.Minute) {
		if err := p.failed(); err != nil {
			return err
		}
		return fmt.Errorf("DDL at %s: pipeline did not drain", t.SourcePos.GTID)
	}
	ids := make([]model.TableID, 0, len(affected))
	for id := range affected {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	var diffs []string
	for _, id := range ids {
		d, err := p.schemaDiff(ctx, id)
		if err != nil {
			return err
		}
		if d != "" {
			diffs = append(diffs, d)
		}
	}
	if len(diffs) > 0 {
		return exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
			fmt.Sprintf("DDL at %s changes pipeline table(s) and the target schema does not match yet: %s. "+
				"Apply the change on the target, then resume. DDL: %q", t.SourcePos.GTID, strings.Join(diffs, "; "), t.DDL), nil)
	}
	if len(renamedIn) > 0 {
		names := make([]string, len(renamedIn))
		for i, id := range renamedIn {
			names[i] = id.String()
		}
		return exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
			fmt.Sprintf("DDL at %s renames a table that was not replicated into the pipeline filter (%s): its existing rows are not on the target. "+
				"Restart strand to snapshot it (or exclude it in tables.exclude). DDL: %q", t.SourcePos.GTID, strings.Join(names, ", "), t.DDL), nil)
	}
	// Schema khớp: nạp lại metadata (cột, PK, unique, FK) của các bảng liên quan.
	for _, id := range ids {
		one, err := newTableFilter([]string{id.String()}, nil, p.cfg.Tables.Rename)
		if err != nil {
			return err
		}
		metas, err := loadSourceTables(ctx, p.src, one)
		if err != nil {
			return err
		}
		m := metas[id]
		p.tablesMu.Lock()
		if m == nil {
			delete(p.tables, id) // bảng đã bị DROP/RENAME đi ở source
		} else {
			p.tables[id] = m
		}
		p.tablesMu.Unlock()
		if m == nil {
			continue
		}
		for _, w := range p.writers {
			w.SetPrimaryKey(p.filter.Target(id), m.PK)
		}
		if newCreated[id] {
			if err := p.state.SetDone(id.String(), true); err != nil {
				return err
			}
			p.log.Info("new table created on source joins the pipeline", "table", id.String(), "gtid", t.SourcePos.GTID)
		}
	}
	return nil
}

// schemaDiff so cột (tên, kiểu, NULL) của bảng ở source và ở target theo thứ tự. Bảng không còn ở
// source thì target cũng không được còn. Trả "" khi khớp.
func (p *Pipeline) schemaDiff(ctx context.Context, id model.TableID) (string, error) {
	srcCols, srcOK, err := tableColumns(ctx, p.src, id)
	if err != nil {
		return "", fmt.Errorf("read source schema of %s: %w", id, err)
	}
	tid := p.filter.Target(id)
	dstCols, dstOK, err := tableColumns(ctx, p.dst, tid)
	if err != nil {
		return "", fmt.Errorf("read target schema of %s: %w", tid, err)
	}
	switch {
	case !srcOK && !dstOK:
		return "", nil
	case !srcOK:
		return fmt.Sprintf("%s no longer exists on source but %s still exists on target", id, tid), nil
	case !dstOK:
		return fmt.Sprintf("%s does not exist on target", tid), nil
	}
	if strings.Join(srcCols, ", ") != strings.Join(dstCols, ", ") {
		return fmt.Sprintf("%s columns differ: source (%s) vs target (%s)", id, strings.Join(srcCols, ", "), strings.Join(dstCols, ", ")), nil
	}
	return "", nil
}

func tableColumns(ctx context.Context, db *sql.DB, id model.TableID) ([]string, bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT COLUMN_NAME, LOWER(COLUMN_TYPE), IS_NULLABLE FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, id.Schema, id.Table)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var n, typ, null string
		if err := rows.Scan(&n, &typ, &null); err != nil {
			return nil, false, err
		}
		c := n + " " + typ
		if null == "NO" {
			c += " not null"
		}
		cols = append(cols, c)
	}
	return cols, len(cols) > 0, rows.Err()
}
