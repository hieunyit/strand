package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"strand/internal/model"
	"strand/internal/validate"
)

// Bảng partitioned ở source PG (review R02). Pipeline theo dõi bảng gốc (relkind 'p'); snapshot đọc
// qua bảng gốc nên thấy mọi partition. Nhưng với publication mặc định, pgoutput phát thay đổi dưới tên
// PARTITION LÁ — router không biết bảng đó và bỏ row, trong khi checkpoint vẫn tiến qua Txn: mất dữ
// liệu âm thầm. Hai lớp bảo vệ:
//   - lúc khởi động / snapshot add: bảng partitioned thuộc pipeline mà publication không bật
//     publish_via_partition_root thì từ chối chạy (báo lệnh ALTER PUBLICATION cần chạy);
//   - lúc stream: row của một partition lá có gốc thuộc pipeline → dừng với lỗi, không bỏ qua
//     (phòng publication bị đổi khi đang chạy).

func (p *Pipeline) checkPGPublication(ctx context.Context, ids []model.TableID) error {
	if p.srcEngine != model.EnginePG {
		return nil
	}
	_, pub := p.pgSlotNames()
	return validate.CheckPGPublication(ctx, p.src, pub, ids)
}

// pgPartitionRoots: cache bảng ngoài pipeline → bảng gốc partition (rỗng nếu không phải partition).
type pgPartitionRoots struct {
	mu sync.Mutex
	m  map[model.TableID]model.TableID
}

// leafOfPipelineTable trả về bảng gốc nếu id là partition lá (ở mọi cấp) của một bảng thuộc pipeline.
func (p *Pipeline) leafOfPipelineTable(ctx context.Context, id model.TableID) (model.TableID, bool, error) {
	if p.srcEngine != model.EnginePG {
		return model.TableID{}, false, nil
	}
	p.partRoots.mu.Lock()
	root, cached := p.partRoots.m[id]
	p.partRoots.mu.Unlock()
	if !cached {
		err := p.src.QueryRowContext(ctx, `
			SELECT rn.nspname, r.relname FROM pg_class c
			  JOIN pg_namespace n ON n.oid = c.relnamespace
			  JOIN pg_class r ON r.oid = pg_partition_root(c.oid)
			  JOIN pg_namespace rn ON rn.oid = r.relnamespace
			 WHERE n.nspname = $1 AND c.relname = $2 AND c.relispartition`, id.Schema, id.Table).Scan(&root.Schema, &root.Table)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return model.TableID{}, false, fmt.Errorf("look up partition root of %s: %w", id, err)
		}
		p.partRoots.mu.Lock()
		if p.partRoots.m == nil {
			p.partRoots.m = map[model.TableID]model.TableID{}
		}
		p.partRoots.m[id] = root
		p.partRoots.mu.Unlock()
	}
	if root.Table == "" || p.table(root) == nil {
		return model.TableID{}, false, nil
	}
	return root, true, nil
}
