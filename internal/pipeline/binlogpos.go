package pipeline

// Chế độ vị trí binlog (source MySQL KHÔNG có GTID: 5.5, hoặc 5.6/5.7 với gtid_mode=OFF).
//
// Mỗi Txn được định danh bằng (file binlog, end_log_pos của event kết thúc) — xem
// checkpoint.MySQLPosAppliedSet. Tập đã apply, bộ theo dõi vị trí (lsnTracker.pointPos) và router dùng
// lại đúng khung của source PG (mốc base được Vouch + extra + holes), thay LSN bằng khóa vị trí.
//
// Lập luận bất biến (chi tiết ở từng hàm):
//   - CP-1: dữ liệu và vị trí vẫn cùng một transaction ở target (row delta của worker / row toàn cục ở
//     strict); mốc base chỉ do coordinator Vouch khi scheduler xác nhận mọi Txn trước đó đã commit.
//   - CP-2: router bỏ Txn có vị trí kết thúc ∈ applied0; reader đọc lại từ Lwd (≤ Txn chưa apply sớm
//     nhất) nên có thể thấy lại Txn đã apply, và chúng bị bỏ.
//   - CP-3: chỉ (file, end_log_pos) được ghi xuống; seq nội bộ không bao giờ.
//   - CP-4: |holes| + |extra| bị chặn bởi cùng cơ chế backpressure của scheduler như PG.
//   - CP-5: strict commit tuần tự theo seq = thứ tự binlog; checkpoint là một mốc ghi trong commit cuối.
//
// Ngoài phạm vi (từ chối rõ ràng): seed, snapshot logic (watermark theo GTID), đổi/failover server,
// đọc từ replica, promote/rewind/verify theo GTID — xem rejectBinlogPos.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	mysqlreader "strand/internal/reader/mysql"
)

const posModeUnsupported = "not yet supported in binlog position mode (source MySQL without GTID)"

// rejectBinlogPos: lỗi "chưa hỗ trợ" cho chức năng cần GTID. Luôn fail closed.
func rejectBinlogPos(what string) error {
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf("%s: %s", what, posModeUnsupported), nil)
}

// detectBinlogPosMode quyết định chế độ định danh Txn của source MySQL: gtid_mode = OFF, hoặc biến
// không tồn tại (MySQL 5.5) thì dùng vị trí binlog. Mọi giá trị khác (ON, ON_PERMISSIVE...) giữ đường
// GTID như cũ. Ghi log một lần, rõ ràng.
func (p *Pipeline) detectBinlogPosMode(ctx context.Context) error {
	if p.srcEngine != model.EngineMySQL {
		return nil
	}
	var mode string
	err := p.src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&mode)
	if err != nil {
		var me *drivermysql.MySQLError
		if errors.As(err, &me) && me.Number == 1193 { // ER_UNKNOWN_SYSTEM_VARIABLE: MySQL 5.5
			mode = "OFF"
		} else {
			return fmt.Errorf("read source gtid_mode: %w", err)
		}
	}
	if !strings.EqualFold(strings.TrimSpace(mode), "OFF") {
		return nil
	}
	p.posMode = true
	p.lsns = &lsnTracker{}
	if strings.EqualFold(p.cfg.Source.ReadFrom, "replica") {
		return rejectBinlogPos("source.read_from=replica: binlog positions differ on every server, so switching or reading from a replica")
	}
	var logBin int
	if err := p.src.QueryRowContext(ctx, "SELECT @@GLOBAL.log_bin").Scan(&logBin); err != nil || logBin != 1 {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "source log_bin is OFF: binary logging must be enabled", err)
	}
	var format string
	if err := p.src.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_format").Scan(&format); err != nil {
		return fmt.Errorf("read source binlog_format: %w", err)
	}
	if !strings.EqualFold(format, "ROW") {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf("binlog_format must be ROW, got %q", format), nil)
	}
	// Định danh server: vị trí binlog chỉ có nghĩa trên đúng server tạo ra chúng (CP-3). server_uuid chỉ có từ 5.6.
	var serverID int64
	if err := p.src.QueryRowContext(ctx, "SELECT @@GLOBAL.server_id").Scan(&serverID); err != nil {
		return fmt.Errorf("read source server_id: %w", err)
	}
	ident := fmt.Sprintf("server_id=%d", serverID)
	var uuid string
	if err := p.src.QueryRowContext(ctx, "SELECT @@GLOBAL.server_uuid").Scan(&uuid); err == nil && uuid != "" {
		ident += ";uuid=" + strings.ToLower(uuid)
	}
	p.srcIdent = ident
	p.log.Warn("source has no GTID (gtid_mode OFF or absent): running in BINLOG POSITION MODE; transactions are identified by "+
		"(binlog file, end position); seeds, logical snapshots, promote and replica switching are not supported in this mode",
		"source", ident)
	return nil
}

// binlogFiles liệt kê SHOW BINARY LOGS: tên file → kích thước.
func (p *Pipeline) binlogFiles(ctx context.Context) (names []string, sizes map[string]uint64, err error) {
	rows, err := p.src.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, nil, fmt.Errorf("SHOW BINARY LOGS: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	sizes = map[string]uint64{}
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptr := make([]any, len(cols))
		for i := range ptr {
			ptr[i] = &vals[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			return nil, nil, err
		}
		var size uint64
		if _, err := fmt.Sscan(string(vals[1]), &size); err != nil {
			return nil, nil, fmt.Errorf("SHOW BINARY LOGS: bad file size %q: %w", vals[1], err)
		}
		names = append(names, string(vals[0]))
		sizes[string(vals[0])] = size
	}
	return names, sizes, rows.Err()
}

// checkBinlogPosition kiểm (file, offset) là một vị trí đọc được ở source: file còn trên server, offset
// ≤ kích thước file và là ranh giới event (SHOW BINLOG EVENTS FROM offset báo lỗi nếu cắt giữa event).
// notFound: lỗi trả khi file không còn (resume: log_gap; start-at: từ chối).
func (p *Pipeline) checkBinlogPosition(ctx context.Context, file string, off uint64) error {
	_, sizes, err := p.binlogFiles(ctx)
	if err != nil {
		return err
	}
	size, ok := sizes[file]
	if !ok {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("log gap: binlog %s is no longer on the source (purged or index replaced); the transactions after %s:%d cannot be proven to be applied: re-seed required", file, file, off), nil)
	}
	if off > size {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("position %s:%d is beyond the end of the file (%d bytes): the binlog was replaced or the position is wrong", file, off, size), nil)
	}
	if off == size {
		return nil
	}
	notBoundary := func(err error) error {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("position %s:%d is not an event boundary of the source binlog: %v", file, off, err), err)
	}
	rows, err := p.src.QueryContext(ctx, fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' FROM %d LIMIT 1", strings.ReplaceAll(file, "'", "''"), off))
	if err != nil {
		return notBoundary(err)
	}
	defer rows.Close()
	rows.Next() // MySQL báo lỗi vị trí sai khi đọc dòng đầu, không phải lúc Query
	if err := rows.Err(); err != nil {
		return notBoundary(err)
	}
	return nil
}

// initialPositionPos quyết định điểm bắt đầu ở chế độ vị trí binlog (thay initialPosition). Chỉ có
// --start-at <file>:<position> (dữ liệu ở target đã khớp source tại vị trí đó); không có seed và
// snapshot logic (cần watermark GTID): từ chối.
func (p *Pipeline) initialPositionPos(ctx context.Context, loaded checkpoint.AppliedSet) (*checkpoint.MySQLPosAppliedSet, error) {
	var set *checkpoint.MySQLPosAppliedSet
	switch l := loaded.(type) {
	case *checkpoint.MySQLPosAppliedSet:
		if !l.IsEmpty() {
			set = l
		}
	case *checkpoint.MySQLAppliedSet:
		if l.String() != "" {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				"the checkpoint holds a GTID set but the source now has gtid_mode OFF (or no GTID): positions of the two modes are not comparable; "+
					"restore gtid_mode=ON or re-seed the pipeline", nil)
		}
	default:
		return nil, fmt.Errorf("unexpected applied set type %T", loaded)
	}

	if p.opts.StartAt != "" {
		if set != nil {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
				"--start-at is only valid for a new pipeline; a checkpoint already exists (use strand checkpoint rewind)", nil)
		}
		if p.opts.StartAt == "seed" {
			return nil, rejectBinlogPos("--start-at seed")
		}
		base, key, err := checkpoint.ParseBinlogStartAt(p.opts.StartAt)
		if err != nil {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
				fmt.Sprintf("invalid start position %q for a source without GTID: %v", p.opts.StartAt, err), err)
		}
		pos := checkpoint.PosOfKey(base, key)
		if err := p.checkBinlogPosition(ctx, pos.File, pos.BinlogPos); err != nil {
			return nil, err
		}
		// Mọi Txn kết thúc ≤ vị trí này coi như đã apply (dữ liệu target khớp source tại đó); đọc từ đúng vị trí.
		set = checkpoint.NewMySQLPosAppliedSet(base, key)
		set.SetSource(p.srcIdent)
		if err := p.state.SetVerifyPending(true); err != nil {
			return nil, err
		}
		for id := range p.tables {
			if err := p.state.SetDone(id.String(), true); err != nil {
				return nil, err
			}
		}
		p.log.Warn("start-at: target data is assumed to match the source at this binlog position; post-seed verify runs after catch-up",
			"position", pos.Label())
		p.binlogBase = base
		return set, p.writeGlobal(ctx, set.String())
	}
	if set == nil {
		// Pipeline mới, không --start-at: chế độ A cần snapshot logic với watermark GTID.
		return nil, rejectBinlogPos("a new pipeline needs --start-at <binlog file>:<position>; logical snapshot (bootstrap.default=logical)")
	}
	// Resume từ checkpoint. Định danh server phải khớp: vị trí chỉ có nghĩa trên server đã tạo ra chúng.
	if set.Source() != "" && set.Source() != p.srcIdent {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
			"the checkpoint was made against source %q but the DSN now reaches %q: binlog positions differ per server (failover/replica switch is not supported without GTID); re-seed the pipeline",
			set.Source(), p.srcIdent), nil)
	}
	p.binlogBase = set.FileBase()
	lw := set.Lwd()
	if err := p.checkBinlogPosition(ctx, lw.File, lw.BinlogPos); err != nil {
		return nil, err
	}
	return set, nil
}

// checkBinlogTxn kiểm Txn mà router nhận trước khi dùng vị trí của nó (CP-2/CP-5): cùng tên gốc file
// với checkpoint, vị trí bắt đầu/kết thúc hợp lệ và luồng đơn điệu — lsnTracker.pointPos và Vouch chỉ
// đúng khi các Txn đến theo thứ tự binlog, không chồng lấn. Vi phạm là lỗi dừng pipeline (fail closed).
func (p *Pipeline) checkBinlogTxn(t *model.Txn) (start, end uint64, err error) {
	p.curStart, p.curEnd = 0, 0
	end, err = checkpoint.KeyOfPos(p.binlogBase, t.SourcePos)
	if err != nil {
		return 0, 0, err
	}
	if t.BinlogFile != t.SourcePos.File {
		return 0, 0, fmt.Errorf("transaction ending at %s has no start position in the same binlog file", t.SourcePos.Label())
	}
	start, err = checkpoint.KeyOfPos(p.binlogBase, model.Pos{Engine: model.EngineMySQL, File: t.BinlogFile, BinlogPos: t.BinlogPos})
	if err != nil {
		return 0, 0, err
	}
	if start >= end {
		return 0, 0, fmt.Errorf("transaction at %s starts at or after its end", t.SourcePos.Label())
	}
	if start < p.lastRouteEnd {
		return 0, 0, fmt.Errorf("transaction %s starts at %s, before the end %s of the previous one: binlog positions are not monotonic (RESET MASTER or replaced binlog?)",
			t.SourcePos.Label(), checkpoint.FormatBinlogKey(start), checkpoint.FormatBinlogKey(p.lastRouteEnd))
	}
	p.lastRouteEnd = end
	p.curStart, p.curEnd = start, end
	return start, end, nil
}

// posCaughtUp: reader đã theo kịp đầu binlog của source. Router đã nhận mọi Txn kết thúc ≤ seen; phần
// binlog sau seen tới đầu (SHOW MASTER STATUS) chỉ được coi là "đã kịp" khi không còn event nào của một
// Txn — Rotate/Format_desc/Previous_gtids/Stop (sau FLUSH LOGS hoặc restart) không phải Txn, nếu không
// thì source rảnh sẽ không bao giờ "kịp".
func (p *Pipeline) posCaughtUp(ctx context.Context) bool {
	if p.lsns == nil {
		return false
	}
	seen := p.lsns.seenLSN()
	if seen == 0 {
		// Chưa nhận Txn nào: so từ Lwd khởi động.
		seen = p.applied0.(*checkpoint.MySQLPosAppliedSet).LwdKey()
	}
	ok, err := mysqlreader.TxnFreeToHead(ctx, p.src, p.binlogBase, seen)
	return err == nil && ok
}
