package pipeline

import (
	"context"
	"fmt"
	"strings"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// checkMySQLSeedSourceIdentity (SD-1, cùng lỗ với review N09 phía PG): seed.json của MySQL phải được
// lấy từ đúng topology replication của source đang nối.
//
// Quy tắc: không so thẳng source_identity với @@server_uuid — seed plan ghi source_identity là UUID
// đầu tiên của GTID set P (hoặc server_uuid của server bị backup), mà backup thường lấy từ replica,
// còn CDC có thể đọc từ một member khác của cùng topology (sau failover): UUID khác nhau hợp lệ.
// Danh tính MySQL được chứng minh bằng lịch sử GTID:
//  1. source_identity không rỗng;
//  2. P ⊆ @@GLOBAL.gtid_executed của source (gtid_executed đã gồm gtid_purged) — mọi Txn trong seed
//     source đều đã thực thi; seed từ cluster khác (UUID khác) hoặc đi trước source thì vi phạm;
//  3. source_identity = @@server_uuid của source, hoặc là một UUID có trong gtid_executed của source.
func checkMySQLSeedSourceIdentity(manifestID, startGTID, sourceUUID, sourceExecuted string) error {
	id := strings.ToLower(strings.TrimSpace(manifestID))
	if id == "" {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			"SD-1: seed manifest has no source_identity: cannot prove the seed was taken from this source (re-run strand seed plan)", nil)
	}
	pset, err := checkpoint.ParseMySQLGTIDSet(startGTID)
	if err != nil || pset.String() == "" {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: seed manifest start_position.gtid_set %q is not a valid non-empty GTID set", startGTID), err)
	}
	exec, err := checkpoint.ParseMySQLGTIDSet(sourceExecuted)
	if err != nil {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: cannot parse the source gtid_executed: %v", err), err)
	}
	if !pset.IsSubsetOf(exec) {
		missing, _ := checkpoint.ParseMySQLGTIDSet(pset.String())
		missing.Subtract(exec)
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: the seed position contains transactions the source never executed (%s): the seed was not taken from this source's replication topology (or the source is behind the seed)", missing.String()), nil)
	}
	if id == strings.ToLower(strings.TrimSpace(sourceUUID)) || gtidSetUUIDs(sourceExecuted)[id] {
		return nil
	}
	return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
		fmt.Sprintf("SD-1: seed source_identity %s is neither the source server_uuid (%s) nor a server in the source GTID history", id, strings.TrimSpace(sourceUUID)), nil)
}

// gtidSetUUIDs: tập UUID (chữ thường, bỏ tag) xuất hiện trong một GTID set.
func gtidSetUUIDs(set string) map[string]bool {
	out := map[string]bool{}
	for _, seg := range strings.Split(set, ",") {
		u, _, _ := strings.Cut(strings.TrimSpace(seg), ":")
		if u = strings.ToLower(strings.TrimSpace(u)); u != "" {
			out[u] = true
		}
	}
	return out
}

// checkMySQLSeedSource đọc @@server_uuid và gtid_executed của source rồi kiểm SD-1 cho seed.json,
// trước khi ghi bất kỳ trạng thái nào.
func (p *Pipeline) checkMySQLSeedSource(ctx context.Context, manifestID, startGTID string) error {
	var uuid, executed string
	if err := p.src.QueryRowContext(ctx, "SELECT @@server_uuid, @@GLOBAL.gtid_executed").Scan(&uuid, &executed); err != nil {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: cannot read the source server_uuid / gtid_executed to check the seed manifest source_identity: %v", err), err)
	}
	return checkMySQLSeedSourceIdentity(manifestID, startGTID, uuid, executed)
}
