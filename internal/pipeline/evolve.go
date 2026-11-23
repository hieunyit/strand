package pipeline

import (
	"context"
	"fmt"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/schemachange"
	"strand/internal/schemagen"
)

// P3-01 — schema evolution tại barrier DDL (đã drain, router giữ routeMu).
//
// Giới hạn đã biết: schema source được đọc lúc xử lý DDL này, không phải đúng tại vị trí DDL trong log.
// Nếu source đã có thêm DDL sau đó thì diff có thể gồm cả thay đổi sau: thay đổi cộng thêm áp sớm vô hại,
// thay đổi phá hủy làm pipeline dừng chờ duyệt (đúng hướng an toàn).
// Nguồn PostgreSQL: phát hiện qua message Relation của pgoutput và barrier DDL tại row đầu tiên (P3-01b).

// evolveSchema so schema các bảng ids ở source/target. Trả changed=true khi đã áp DDL lên target (caller so lại
// schema); lỗi exit 10 khi cần người duyệt; (false, nil) khi không có gì phải làm hoặc không phân loại được.
func (p *Pipeline) evolveSchema(ctx context.Context, t *model.Txn, ids []model.TableID, hasColumnDiff bool) (bool, error) {
	if p.srcEngine != model.EngineMySQL && p.srcEngine != model.EnginePG {
		return false, nil
	}
	req := schemagen.Request{Source: p.src, Target: p.dst, SourceKind: p.cfg.Source.Kind, TargetKind: p.cfg.Target.Kind, Rename: p.cfg.Tables.Rename}
	var all schemagen.Evolution
	for _, id := range ids {
		msg, both, err := p.schemaDiffInfo(ctx, id)
		if err != nil {
			return false, err
		}
		if !both {
			if msg != "" {
				return false, nil // bảng bị DROP/thiếu ở target: không phải ALTER, giữ hành vi dừng
			}
			continue
		}
		ev, err := schemagen.PlanEvolution(ctx, req, id)
		if err != nil {
			if hasColumnDiff {
				p.log.Warn("cannot classify schema change", "table", id.String(), "err", err)
			}
			return false, nil
		}
		all.Changes = append(all.Changes, ev.Changes...)
	}
	if all.Empty() {
		return false, nil
	}
	if p.state != nil {
		if err := p.state.invalidateVerification(); err != nil {
			return false, err
		}
	}
	if all.Additive() && !strings.EqualFold(p.cfg.Schema.Additive, "pause") {
		if err := p.applyTargetDDL(ctx, all.Statements()); err != nil {
			return false, err
		}
		p.log.Info("additive schema change applied to target", "position", t.SourcePos.Label(), "changes", all.Describe(), "ddl", all.Statements())
		return true, nil
	}
	store := schemachange.NewStore(p.dst, p.dstEngine == model.EnginePG)
	id, created, err := store.Record(ctx, schemachange.Change{
		Pipeline: p.id, SourcePos: t.SourcePos.Label(), SourceDDL: t.DDL, Statements: all.Statements(),
		Classification: all.Classification(), Reason: all.Describe(),
	})
	if err != nil {
		return false, fmt.Errorf("record pending schema change: %w", err)
	}
	why := "destructive"
	if all.Classification() == "additive" {
		why = "additive but schema.additive=pause"
	}
	if created {
		p.log.Warn("schema change recorded, waiting for approval", "change_id", id, "classification", all.Classification())
	}
	return false, exitcode.New(exitcode.ErrSchemaChangePending, exitcode.NeedsDecision,
		fmt.Sprintf("DDL at %s needs approval (%s): %s. Proposed target DDL recorded as schema change #%d. "+
			"Run `strand schema diff -c <config>` to review and `strand schema approve %d -c <config>` to apply it on the target, then run strand sync again. DDL: %q",
			t.SourcePos.Label(), why, all.Describe(), id, id, t.DDL), nil)
}

// applyTargetDDL chạy các câu DDL (đã sinh bởi schemagen) lên target theo thứ tự. PG: một transaction.
func (p *Pipeline) applyTargetDDL(ctx context.Context, stmts []string) error {
	for _, st := range stmts {
		if schemagen.IsManualStatement(st) {
			return fmt.Errorf("internal: manual statement in auto-apply plan: %s", st)
		}
	}
	if p.dstEngine == model.EnginePG {
		tx, err := p.dst.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, st := range stmts {
			if _, err := tx.ExecContext(ctx, st); err != nil {
				return fmt.Errorf("apply schema change on target: %w (%s)", err, st)
			}
		}
		return tx.Commit()
	}
	for _, st := range stmts {
		if _, err := p.dst.ExecContext(ctx, st); err != nil {
			return fmt.Errorf("apply schema change on target: %w (%s)", err, st)
		}
	}
	return nil
}

// resolveSchemaChanges: schema target đã khớp tại DDL này (DBA áp tay hoặc approve) → đóng các bản ghi chờ.
func (p *Pipeline) resolveSchemaChanges(ctx context.Context, t *model.Txn) {
	if err := schemachange.NewStore(p.dst, p.dstEngine == model.EnginePG).ResolveAt(ctx, p.id, t.SourcePos.Label()); err != nil {
		p.log.Warn("cannot close schema change records", "err", err)
	}
}
