package pipeline

import (
	"context"
	"fmt"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/validate"
)

// Bất biến phạm vi (review DBA): một bảng chỉ được ghi sau khi qua cùng kiểm tra như lúc khởi động, dù nó vào
// pipeline lúc khởi động, qua `snapshot add` hay qua DDL tạo bảng mới ở source:
//   - không nằm trong tables.exclude (exclude luôn thắng, như lúc khởi động hợp added_tables);
//   - bảng đích không phải một bảng nguồn của pipeline trên cùng database (CheckTableOverlap);
//   - target MySQL: mọi unique ở bảng đích được unique của nguồn bao hàm: nếu không, upsert MySQL khớp unique
//     phụ và gộp hai row nguồn thành một mà không báo lỗi. Target PG không gộp âm thầm (ON CONFLICT chỉ theo
//     khóa; unique khác vi phạm thì dừng với lỗi), và kiểm tra đầy đủ (gồm collation khác engine) từ chối cả
//     cặp mà runtime hỗ trợ (vd. khóa chuỗi _ci → "C"): ở đó vẫn do validate/doctor báo.

// excludedTableError: bảng bị tables.exclude loại không được thêm lúc chạy.
func excludedTableError(id model.TableID) error {
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
		fmt.Sprintf("table %s is excluded by tables.exclude; remove it from tables.exclude to add it to the pipeline", id), nil)
}

// checkAdmission kiểm tra các bảng ids trước khi chúng vào pipeline lúc chạy (trước DDL ở target, state và
// p.tables).
func (p *Pipeline) checkAdmission(ctx context.Context, ids []model.TableID) error {
	for _, id := range ids {
		if p.filter != nil && p.filter.excluded(id) {
			return excludedTableError(id)
		}
	}
	if p.srcEngine == p.dstEngine {
		p.tablesMu.RLock()
		all := make([]model.TableID, 0, len(p.tables)+len(ids))
		for id := range p.tables {
			all = append(all, id)
		}
		p.tablesMu.RUnlock()
		for _, id := range ids {
			if p.table(id) == nil {
				all = append(all, id)
			}
		}
		if err := validate.CheckTableOverlap(ctx, p.src, p.dst, p.srcEngine, all, p.filter.Target); err != nil {
			return err
		}
	}
	return p.checkTargetConstraints(ctx, ids)
}

// checkTargetConstraints (target MySQL): unique của bảng đích (nếu đã có) phải được unique của nguồn bao hàm (như
// `strand validate` / `doctor`). Bảng đích chưa có thì Strand tạo theo nguồn: không có gì để kiểm.
func (p *Pipeline) checkTargetConstraints(ctx context.Context, ids []model.TableID) error {
	if p.dstEngine != model.EngineMySQL {
		return nil
	}
	opts := validate.ConstraintCheckOptions{TableRename: p.cfg.Tables.Rename}
	for _, id := range ids {
		if err := validate.CheckTableConstraintCompatibility(ctx, p.src, p.srcEngine, p.dst, p.dstEngine, id, p.filter.Target(id), opts); err != nil {
			return err
		}
	}
	return nil
}
