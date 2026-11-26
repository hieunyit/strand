package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/findings"
	"strand/internal/model"
	"strand/internal/schemagen"
)

// Các hàm ở đây phơi các kiểm tra lúc khởi động pipeline cho `strand doctor` dưới dạng chỉ đọc: chúng dùng
// chung logic với Start (compareMySQLForeignKeys, closeAutoSeedGroup, groupReplicationKeylessBlocked) nhưng ghi
// mọi vấn đề vào một findings.Collector thay vì dừng ở vấn đề đầu tiên. Không DDL, không ghi, không khóa.

// ReportMySQLForeignKeys so FK nguồn/target như Start làm khi bootstrap.auto_seed tắt (hoặc pipeline auto-seed
// đã có checkpoint): FK cascade chồng nhau, FK target thiếu/thừa/khác, trùng tên constraint. tables là phạm vi
// nguồn, keyless là bảng chạy keyless, mapTarget áp tables.rename.
func ReportMySQLForeignKeys(ctx context.Context, src, dst *sql.DB, tables, keyless []model.TableID, mapTarget func(model.TableID) model.TableID, c *findings.Collector) error {
	if mapTarget == nil {
		mapTarget = func(id model.TableID) model.TableID { return id }
	}
	scope := make(map[model.TableID]bool, len(tables))
	for _, id := range tables {
		scope[id] = true
	}
	isKeyless := make(map[model.TableID]bool, len(keyless))
	for _, id := range keyless {
		isKeyless[id] = true
	}
	plan, err := compareMySQLForeignKeys(ctx, src, dst, scope, mapTarget, func(id model.TableID) bool { return isKeyless[id] }, false, c)
	if err != nil {
		return err
	}
	return reportCreateNameClashes(ctx, dst, plan, mapTarget, c)
}

// reportCreateNameClashes: bảng target chưa có sẽ được tạo bằng DDL của nguồn (schemagen giữ tên FK, hoặc
// "<bảng>_<tên>" khi đổi tên bảng). MySQL giữ tên FK duy nhất trong một schema (không phân biệt hoa thường), nên
// FK cùng tên của một bảng khác trên target làm CREATE TABLE lỗi (errno 121) giữa chừng.
func reportCreateNameClashes(ctx context.Context, dst *sql.DB, plan mysqlCascadePlan, mapTarget func(model.TableID) model.TableID, c *findings.Collector) error {
	exists := map[model.TableID]bool{}
	for _, fk := range plan.sourceFKs {
		if fk.constraint == "" {
			continue
		}
		child := mapTarget(fk.Child)
		present, ok := exists[child]
		if !ok {
			var err error
			if present, err = schemagen.Exists(ctx, dst, "mysql", child); err != nil {
				return err
			}
			exists[child] = present
		}
		if present {
			continue // bảng có sẵn: compareMySQLForeignKeys đã so FK của nó
		}
		name := fk.constraint
		if child != fk.Child {
			name = child.Table + "_" + fk.constraint
		}
		for _, t := range plan.targetFKs {
			if t.Child.Schema != child.Schema || t.Child == child || !strings.EqualFold(t.constraint, name) {
				continue
			}
			if c.Has("target_fk_name_clash", fkObject(fk.Child, fk.constraint)) {
				break // compareMySQLForeignKeys đã báo
			}
			c.Errorf("target_fk_name_clash", fkObject(fk.Child, fk.constraint), fmt.Sprintf(
				"creating target table %s needs foreign key %s, but %s on the target already uses that name (MySQL foreign-key names are unique per schema, ignoring case): CREATE TABLE would fail",
				child, name, t.Child),
				fmt.Sprintf("rename the constraint on one side, e.g. on the target: ALTER TABLE %s DROP FOREIGN KEY %s; then add it back under another name with %s",
					autoSeedTableSQL(t.Child), mysqlFKQuote(t.constraint), cascadeAddSQL(t, t.Child.Table+"_"+t.constraint)+";"))
			break
		}
	}
	return nil
}

// reportAutoSeedRepairs (doctor): FK của nguồn còn thiếu trên bảng target đã có. Auto-seed (create_schema: auto)
// tự thêm chúng khi bảng còn rỗng và collation cột FK khớp; báo trước điều sẽ xảy ra và điều sẽ bị từ chối.
func reportAutoSeedRepairs(ctx context.Context, dst *sql.DB, plan mysqlCascadePlan, existing map[model.TableID]bool, c *findings.Collector) {
	for _, fk := range plan.missing {
		for _, id := range []model.TableID{fk.Child, fk.Parent} {
			if _, checked := existing[id]; checked {
				continue
			}
			present, err := schemagen.Exists(ctx, dst, "mysql", id)
			if err != nil {
				c.Err(findings.Warning, "check_failed", id.String(), err, "")
				continue
			}
			existing[id] = present
			if present {
				if err := requireEmptyForeignKeyTarget(ctx, dst, id); err != nil {
					c.Err(findings.Error, "target_not_empty", id.String(), err,
						fmt.Sprintf("auto-seed adds missing foreign keys only to empty tables: add it yourself (%s;) or empty %s if its rows are disposable", cascadeAddSQL(fk, missingCascadeName(fk)), id))
				}
			}
		}
		collationOK := true
		for _, side := range []struct {
			table               model.TableID
			columns, collations []string
		}{
			{fk.Child, fk.Columns, fk.ChildCollations}, {fk.Parent, fk.References, fk.ParentCollations},
		} {
			if !existing[side.table] {
				continue
			}
			collations, err := readFKCollations(ctx, dst, side.table, side.columns)
			if err != nil {
				c.Err(findings.Warning, "check_failed", side.table.String(), err, "")
				continue
			}
			if !equivalentFKCollations(collations, side.collations) {
				collationOK = false
				c.Errorf("target_fk_collation_mismatch", side.table.String(),
					fmt.Sprintf("auto-seed: target %s foreign-key columns (%s) must use the source collations (%s) before automatic schema creation, found %s",
						side.table, strings.Join(side.columns, ", "), strings.Join(side.collations, ", "), strings.Join(collations, ", ")),
					fmt.Sprintf("ALTER TABLE %s MODIFY each of (%s) with COLLATE as on the source (%s)", autoSeedTableSQL(side.table), strings.Join(side.columns, ", "), strings.Join(side.collations, ", ")))
			}
		}
		if existing[fk.Child] && collationOK {
			c.Info("target_fk_will_be_added", fkObject(fk.Child, fk.constraint),
				fmt.Sprintf("target table %s exists without the source foreign key %s %s; auto-seed will add it while the table is empty", fk.Child, missingCascadeName(fk), describeCascadeFK(fk)),
				cascadeAddSQL(fk, missingCascadeName(fk))+"; (to add it yourself beforehand)")
		}
	}
}

// GroupReplicationKeylessReport ghi target_gr_keyless cho mỗi bảng keyless mà Group Replication của target sẽ
// từ chối (error 3098) — cùng điều kiện Start kiểm. active cho biết target có đang chạy Group Replication.
func GroupReplicationKeylessReport(ctx context.Context, dst *sql.DB, keyless []model.TableID, mapTarget func(model.TableID) model.TableID, createSchemaAuto bool, c *findings.Collector) error {
	if mapTarget == nil {
		mapTarget = func(id model.TableID) model.TableID { return id }
	}
	blocked, err := groupReplicationKeylessBlocked(ctx, dst, "mysql", keyless, mapTarget, createSchemaAuto)
	if err != nil {
		return err
	}
	for _, id := range blocked {
		c.Errorf("target_gr_keyless", id.String(),
			fmt.Sprintf("the target uses Group Replication, which rejects rows of tables without a primary key (error 3098), and source table %s has no key (target %s)", id, mapTarget(id)),
			"SET PERSIST sql_generate_invisible_primary_key=ON; on every group member (MySQL 8.0.30+) before Strand creates the table (bootstrap.create_schema: auto); or add a key on the source, add one to the existing target table, or add the table to tables.exclude")
	}
	return nil
}

// TargetGroupReplicationActive: target MySQL là thành viên một nhóm Group Replication đang chạy.
func TargetGroupReplicationActive(ctx context.Context, db *sql.DB) (bool, error) {
	return targetGroupReplicationActive(ctx, db)
}

// AutoSeedResumed cho biết pipeline name đã có checkpoint đã công bố trên target (auto-seed sẽ không chạy lại).
// Target DSN phải chọn database checkpoint (lỗi exit 3 nếu không).
func AutoSeedResumed(ctx context.Context, dst *sql.DB, name string) (bool, error) {
	var targetSchema sql.NullString
	if err := dst.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&targetSchema); err != nil {
		return false, err
	}
	if !targetSchema.Valid || targetSchema.String == "" {
		return false, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "target DSN must select the MySQL checkpoint database for auto-seed", nil)
	}
	hasCheckpointTable, err := schemagen.Exists(ctx, dst, "mysql", model.TableID{Schema: targetSchema.String, Table: "_strand_checkpoint"})
	if err != nil || !hasCheckpointTable {
		return false, err
	}
	var applied string
	err = dst.QueryRowContext(ctx, "SELECT applied FROM _strand_checkpoint WHERE pipeline_id=?", name).Scan(&applied)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	// Start creates an empty checkpoint row before attempting the copy;
	// only a published applied position means this is a real resume.
	return err == nil && applied != "", nil
}
