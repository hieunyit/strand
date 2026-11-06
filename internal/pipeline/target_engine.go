package pipeline

import (
	"context"

	"strand/internal/model"
	"strand/internal/validate"
)

// checkTargetEngines (review R26): target MySQL phải dùng InnoDB cho mọi bảng đích (tên sau rename)
// và mọi bảng _strand_*, nếu không ghi dữ liệu và checkpoint không nguyên tử (CP-1). Gọi một lần lúc
// khởi động, sau khi schema đích đã được tạo; target PG luôn giao dịch nên bỏ qua.
func (p *Pipeline) checkTargetEngines(ctx context.Context) error {
	if p.dstEngine == model.EnginePG {
		return nil
	}
	targets := make([]model.TableID, 0, len(p.tables))
	for id := range p.tables {
		targets = append(targets, p.filter.Target(id))
	}
	return validate.CheckMySQLTargetEngines(ctx, p.dst, targets)
}
