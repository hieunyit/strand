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
// Không so thẳng source_identity với @@server_uuid — backup thường lấy từ replica, CDC có thể đọc từ một
// member khác của cùng topology (sau failover). Danh tính MySQL được chứng minh bằng lịch sử GTID:
//  1. source_identity không rỗng, P là GTID set hợp lệ khác rỗng;
//  2. với mọi UUID mà source có trong gtid_executed (đã gồm gtid_purged): phần của P theo UUID đó ⊆
//     gtid_executed — không thì source đi sau seed / lịch sử đã rẽ nhánh: từ chối;
//  3. P có ít nhất một UUID thuộc lịch sử GTID của source (hoặc source_identity là @@server_uuid) — không
//     thì seed thuộc cluster khác: từ chối;
//  4. UUID của P mà source chưa từng thấy (Txn "lạc" ghi thẳng trên replica được backup, như CREATE USER
//     cục bộ): KHÔNG từ chối — replica có Txn lạc là chuyện thường và seed từ replica là đường được hỗ
//     trợ — mà trả cảnh báo: dữ liệu seed có thể chứa thay đổi không có ở source; chạy verify.
func checkMySQLSeedSourceIdentity(manifestID, startGTID, sourceUUID, sourceExecuted string) ([]string, error) {
	reject := func(msg string, err error) ([]string, error) {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, msg, err)
	}
	id := strings.ToLower(strings.TrimSpace(manifestID))
	if id == "" {
		return reject("SD-1: seed manifest has no source_identity: cannot prove the seed was taken from this source (re-run strand seed plan)", nil)
	}
	pset, err := checkpoint.ParseMySQLGTIDSet(startGTID)
	if err != nil || pset.String() == "" {
		return reject(fmt.Sprintf("SD-1: seed manifest start_position.gtid_set %q is not a valid non-empty GTID set", startGTID), err)
	}
	exec, err := checkpoint.ParseMySQLGTIDSet(sourceExecuted)
	if err != nil {
		return reject(fmt.Sprintf("SD-1: cannot parse the source gtid_executed: %v", err), err)
	}
	known := gtidSetUUIDs(sourceExecuted)
	shared := id == strings.ToLower(strings.TrimSpace(sourceUUID))
	var errant []string
	for _, seg := range strings.Split(pset.String(), ",") {
		seg = strings.TrimSpace(seg)
		u, _, _ := strings.Cut(seg, ":")
		u = strings.ToLower(strings.TrimSpace(u))
		if u == "" {
			continue
		}
		if !known[u] {
			errant = append(errant, seg)
			continue
		}
		shared = true
		part, err := checkpoint.ParseMySQLGTIDSet(seg)
		if err != nil {
			return reject(fmt.Sprintf("SD-1: cannot parse seed GTID segment %q: %v", seg, err), err)
		}
		if !part.IsSubsetOf(exec) {
			part.Subtract(exec)
			return reject(fmt.Sprintf("SD-1: the seed position contains transactions of server %s that the source has not executed (%s): the source is behind the seed or its history diverged", u, part.String()), nil)
		}
	}
	if !shared {
		return reject(fmt.Sprintf("SD-1: no server of the seed position (%s) appears in the source GTID history and source_identity %s is not the source server_uuid (%s): the seed belongs to another cluster",
			pset.String(), id, strings.TrimSpace(sourceUUID)), nil)
	}
	if len(errant) > 0 {
		return []string{fmt.Sprintf("seed position contains transactions of servers the source has never seen (%s), e.g. local writes on the replica the backup was taken from: the seeded data may contain changes that are not on the source; run strand verify", strings.Join(errant, ","))}, nil
	}
	return nil, nil
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
	warns, err := checkMySQLSeedSourceIdentity(manifestID, startGTID, uuid, executed)
	for _, w := range warns {
		p.log.Warn(w)
	}
	return err
}
