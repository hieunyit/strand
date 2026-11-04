package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/schemagen"
)

func (p *Pipeline) createTargetTables(ctx context.Context, ids []model.TableID) error {
	plan, err := schemagen.Build(ctx, schemagen.Request{Source: p.src, Target: p.dst, SourceKind: p.cfg.Source.Kind, TargetKind: p.cfg.Target.Kind, Tables: ids, Rename: p.cfg.Tables.Rename})
	if err != nil {
		return err
	}
	for _, w := range plan.Warnings {
		p.log.Warn(w)
	}
	if err := plan.Apply(ctx, p.dst); err != nil {
		return err
	}
	p.log.Info("target schema initialized", "created_tables", len(plan.Tables))
	return nil
}

// invariant: SD-3, CP-5 — catalog hiện tại có thể vượt vị trí CREATE trong stream.
// Giữ metadata lock đọc trên source, kiểm log từ GTID CREATE tới head đã chụp rồi
// mới đọc catalog/sinh DDL. Nếu có DDL sau đó hoặc không chứng minh được log phủ
// đủ, dừng exit 10; không đánh dấu DDL là applied. Không đọc row tương lai.
// invariant: CO-1, SG-1 — caller giữ routeMu, đã drain worker và giữ khóa pipeline.
func (p *Pipeline) createAtDDL(ctx context.Context, t *model.Txn, ids []model.TableID) error {
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := p.src.Conn(checkCtx)
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(checkCtx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range ids {
		q := "SELECT 1 FROM `" + strings.ReplaceAll(id.Schema, "`", "``") + "`.`" + strings.ReplaceAll(id.Table, "`", "``") + "` LIMIT 0"
		rows, err := tx.QueryContext(checkCtx, q)
		if err != nil {
			return exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision, fmt.Sprintf("source table %s cannot be locked at CREATE position; source may have changed", id), err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	if err := p.checkCreateSchemaAtPosition(checkCtx, t, ids); err != nil {
		return exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
			fmt.Sprintf("cannot prove source schema at CREATE %s: %v; initialize target schema manually and resume", t.SourcePos.GTID, err), err)
	}
	// Lock đọc giữ nguyên cho tới sau Apply; ALTER không thể chen vào giữa kiểm và sinh DDL.
	if err := p.createTargetTables(checkCtx, ids); err != nil {
		return err
	}
	return tx.Commit()
}

type binlogEvent struct {
	kind, info string
	end        uint64
}

func (e binlogEvent) hasGTID(gtid string) bool {
	kind := strings.ToLower(e.kind)
	return strings.Contains(kind, "gtid") && !strings.HasPrefix(kind, "previous") && strings.Contains(strings.ToLower(e.info), "'"+strings.ToLower(gtid)+"'")
}

func readBinlogEvents(ctx context.Context, db *sql.DB, file string, from uint64) ([]binlogEvent, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' FROM %d LIMIT 1000", strings.ReplaceAll(file, "'", "''"), from))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []binlogEvent
	for rows.Next() {
		var log string
		var pos, server uint64
		var e binlogEvent
		if err := rows.Scan(&log, &pos, &e.kind, &server, &e.end, &e.info); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
func (p *Pipeline) checkCreateSchemaAtPosition(ctx context.Context, t *model.Txn, ids []model.TableID) error {
	rows, err := p.src.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return err
	}
	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return err
	}
	type logFile struct {
		name string
		size uint64
	}
	var files []logFile
	for rows.Next() {
		values := make([]sql.RawBytes, len(cols))
		ptr := make([]any, len(cols))
		for i := range ptr {
			ptr[i] = &values[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			rows.Close()
			return err
		}
		var size uint64
		if _, err := fmt.Sscan(string(values[1]), &size); err != nil {
			rows.Close()
			return err
		}
		files = append(files, logFile{string(values[0]), size})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	found := false
	for i := len(files) - 1; i >= 0; i-- {
		// Tìm file chứa GTID, từ file mới nhất; log bị purge thì không đoán schema.
		targetFound := false
		for from := uint64(4); from < files[i].size; {
			events, err := readBinlogEvents(ctx, p.src, files[i].name, from)
			if err != nil {
				return err
			}
			if len(events) == 0 {
				break
			}
			for _, e := range events {
				if e.hasGTID(t.SourcePos.GTID) {
					targetFound = true
					found = true
					break
				}
			}
			if targetFound {
				break
			}
			next := events[len(events)-1].end
			if next <= from {
				return fmt.Errorf("binlog cursor did not advance")
			}
			from = next
		}
		if !targetFound {
			continue
		}
		afterCreate := false
		own := false
		for j := i; j < len(files); j++ {
			for from := uint64(4); from < files[j].size; {
				events, err := readBinlogEvents(ctx, p.src, files[j].name, from)
				if err != nil {
					return err
				}
				if len(events) == 0 {
					break
				}
				for _, e := range events {
					if e.hasGTID(t.SourcePos.GTID) {
						own = true
						continue
					}
					if own && strings.EqualFold(e.kind, "Query") {
						own = false
						afterCreate = true
						continue
					}
					if afterCreate && strings.EqualFold(e.kind, "Query") {
						q := strings.ToLower(e.info)
						for _, id := range ids {
							if ddlMentionsTable(q, strings.ToLower(id.Table)) {
								return fmt.Errorf("later DDL mentions %s", id)
							}
						}
					}
				}
				next := events[len(events)-1].end
				if next <= from {
					return fmt.Errorf("binlog cursor did not advance")
				}
				from = next
			}
		}
		if !afterCreate {
			return fmt.Errorf("CREATE query not found after GTID")
		}
		return nil
	}
	if !found {
		return fmt.Errorf("CREATE GTID is not available in source binlogs")
	}
	return nil
}

// orderSnapshotTables giữ FK đang có hiệu lực: snapshot cha trước con. Khi con
// chưa đọc, stream của con vẫn đi qua range-state như cũ; trước flush con, barrier
// đã drain mọi event cha tới high watermark (RS-1/2, CO-1). Không đổi applied set,
// writer hay ranh giới transaction (CP-1..5). FK vòng/tự tham chiếu không có thứ tự
// này: từ chối trước khi tạo schema, thay vì chết giữa snapshot hoặc tắt constraint.
func orderSnapshotTables(ids []model.TableID, metas map[model.TableID]*tableMeta) ([]model.TableID, error) {
	pending := map[model.TableID]bool{}
	for _, id := range ids {
		pending[id] = true
	}
	state := map[model.TableID]int{}
	var out []model.TableID
	var visit func(model.TableID) error
	visit = func(id model.TableID) error {
		if state[id] == 2 {
			return nil
		}
		if state[id] == 1 {
			return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected, fmt.Sprintf("logical snapshot has a cyclic/self-referencing foreign key at %s; initialize data using a consistent seed", id), nil)
		}
		state[id] = 1
		for _, fk := range metas[id].Keys.ForeignKeys {
			if pending[fk.ParentTable] {
				if err := visit(fk.ParentTable); err != nil {
					return err
				}
			}
		}
		state[id] = 2
		out = append(out, id)
		return nil
	}
	for _, id := range ids {
		if err := visit(id); err != nil {
			return nil, err
		}
	}
	return out, nil
}
