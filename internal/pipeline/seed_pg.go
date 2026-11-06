package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/seed"
)

// spec: 02 §9 (strand sync --start-at seed, PG)
//
// Các quyết định thuần (không cần DB) của startFromSeedPG, tách ra để kiểm bằng unit test.

// pgSeedTablesDone: bảng nào của pipeline đã có dữ liệu trong seed (Done=true) và bảng nào phải
// snapshot logic (Done=false). Dùng seed.SeedCovers — cùng cách hiểu "tables" với planner/apply;
// seed vật lý mặc định ghi ["*"] = mọi bảng (review N06).
func pgSeedTablesDone(m *seed.SeedManifest, ids []model.TableID) map[model.TableID]bool {
	out := make(map[model.TableID]bool, len(ids))
	for _, id := range ids {
		out[id] = seed.SeedCovers(m.Tables, id)
	}
	return out
}

// checkPGSeedSourceIdentity (SD-1, review N09): seed.json phải ghi source_identity và nó phải đúng
// bằng system_identifier của source mà pipeline đang nối tới. Trước đây chỉ ghi log → seed của một
// cluster khác (hoặc manifest không có danh tính) vẫn được dùng làm điểm bắt đầu catch-up.
func checkPGSeedSourceIdentity(manifestID, sourceID string) error {
	manifestID, sourceID = strings.TrimSpace(manifestID), strings.TrimSpace(sourceID)
	if manifestID == "" {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			"SD-1: seed manifest has no source_identity: cannot prove the seed was taken from this source cluster (re-run strand seed plan)", nil)
	}
	if sourceID == "" {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			"SD-1: cannot read the source system_identifier to check the seed manifest source_identity", nil)
	}
	if manifestID != sourceID {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: seed was taken from cluster system_identifier %s but the source is %s", manifestID, sourceID), nil)
	}
	return nil
}

// pgSourceSystemID đọc system_identifier của source: pg_control_system() qua kết nối SQL; nếu không
// được (quyền), dùng IDENTIFY_SYSTEM trên kết nối replication — user CDC luôn có quyền REPLICATION.
func (p *Pipeline) pgSourceSystemID(ctx context.Context) (string, error) {
	var id string
	err := p.src.QueryRowContext(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&id)
	if err == nil && strings.TrimSpace(id) != "" {
		return strings.TrimSpace(id), nil
	}
	cfg, perr := pgconn.ParseConfig(p.srcDSN)
	if perr != nil {
		return "", fmt.Errorf("read source system_identifier: pg_control_system(): %v; parse DSN: %w", err, perr)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, cerr := pgconn.ConnectConfig(ctx, cfg)
	if cerr != nil {
		return "", fmt.Errorf("read source system_identifier: pg_control_system(): %v; replication connection: %w", err, cerr)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	res, ierr := pglogrepl.IdentifySystem(ctx, conn)
	if ierr != nil {
		return "", fmt.Errorf("read source system_identifier: pg_control_system(): %v; IDENTIFY_SYSTEM: %w", err, ierr)
	}
	return res.SystemID, nil
}
