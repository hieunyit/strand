package pipeline

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

// stopGate: điểm dừng CDC của `sync --stop-at` / `--stop-at-time` (như CdcStopPosition của AWS DMS).
// Router không nhận Txn đầu tiên nằm SAU điểm dừng; khi mọi Txn đã nhận được apply, pipeline dừng êm
// (checkpoint như SIGTERM, exit 0). Txn chưa nhận không vào tập đã apply, nên lần chạy sau tiếp tục
// đúng từ đó.
type stopGate struct {
	desc string
	// beyond: t nằm sau điểm dừng, không được route.
	beyond func(t *model.Txn) bool
	// reachedAfter: sau khi route t, mọi Txn tới điểm dừng đã được route (không cần chờ Txn kế tiếp).
	reachedAfter func(t *model.Txn) bool
	// timeLimit (chỉ --stop-at-time, giây Unix): watchStopIdle dừng khi source đứng yên sau mốc này.
	timeLimit int64
}

// newStopGate dựng điểm dừng từ Options. applied: tập đã apply lúc bắt đầu. Trả (nil, true) nếu không có
// điểm dừng; (gate, true) nếu tập đã apply đã chứa điểm dừng thì reached = true (dừng ngay).
func (p *Pipeline) newStopGate(applied checkpoint.AppliedSet) (g *stopGate, reached bool, err error) {
	at := strings.TrimSpace(p.opts.StopAt)
	if at == "" && p.opts.StopAtTime.IsZero() {
		return nil, false, nil
	}
	bad := func(msg string) error {
		return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, msg, nil)
	}
	if at != "" && !p.opts.StopAtTime.IsZero() {
		return nil, false, bad("use either --stop-at or --stop-at-time, not both")
	}
	if !p.opts.StopAtTime.IsZero() {
		limit := p.opts.StopAtTime.Unix()
		return &stopGate{
			desc: "source commit time " + p.opts.StopAtTime.UTC().Format(time.RFC3339),
			// SourceTS là giây: Txn commit trong cùng giây với mốc được tính là trước mốc. Txn rỗng nội bộ
			// (watermark, heartbeat) không có SourceTS và không bao giờ là "sau mốc".
			beyond:       func(t *model.Txn) bool { return t.SourceTS > limit },
			reachedAfter: func(*model.Txn) bool { return false },
			timeLimit:    limit,
		}, false, nil
	}
	switch {
	case p.srcEngine == model.EnginePG:
		lsn, err := checkpoint.ParseLSN(at)
		if err != nil {
			return nil, false, bad(fmt.Sprintf("invalid --stop-at %q for a PostgreSQL source (want an LSN such as 0/1A2B3C4): %v", at, err))
		}
		pa, _ := applied.(*checkpoint.PGAppliedSet)
		if pa != nil && pa.Lwd().LSN >= lsn {
			reached = true
		}
		return &stopGate{
			desc:         "LSN " + at,
			beyond:       func(t *model.Txn) bool { return t.SourcePos.LSN > lsn },
			reachedAfter: func(t *model.Txn) bool { return t.SourcePos.LSN == lsn },
		}, reached, nil
	case p.posMode:
		base, key, err := checkpoint.ParseBinlogStartAt(at)
		if err != nil {
			return nil, false, bad(fmt.Sprintf("invalid --stop-at %q for a source without GTID (want <binlog file>:<position>): %v", at, err))
		}
		keyOf := func(t *model.Txn) (uint64, bool) {
			b, seq, err := checkpoint.SplitBinlogName(t.SourcePos.File)
			if err != nil || b != base {
				return 0, false
			}
			k, err := checkpoint.BinlogKey(seq, t.SourcePos.BinlogPos)
			return k, err == nil
		}
		if pa, ok := applied.(*checkpoint.MySQLPosAppliedSet); ok && pa.Base() >= key {
			reached = true
		}
		return &stopGate{
			desc: "binlog position " + at,
			beyond: func(t *model.Txn) bool {
				k, ok := keyOf(t)
				return ok && k > key
			},
			reachedAfter: func(t *model.Txn) bool {
				k, ok := keyOf(t)
				return ok && k == key
			},
		}, reached, nil
	default:
		want, err := checkpoint.ParseMySQLGTIDSet(at)
		if err != nil {
			return nil, false, bad(fmt.Sprintf("invalid --stop-at %q for a MySQL source with GTID (want a GTID set such as uuid:1-100): %v", at, err))
		}
		routed, _ := applied.Clone().(*checkpoint.MySQLAppliedSet)
		if routed == nil {
			return nil, false, bad("--stop-at with a GTID set needs a GTID checkpoint")
		}
		covered := func() bool {
			miss := want.Clone().(*checkpoint.MySQLAppliedSet)
			miss.Subtract(routed)
			return miss.String() == ""
		}
		// Mọi GTID của tập đã được route thì Txn kế tiếp nằm sau điểm dừng (binlog có thứ tự).
		return &stopGate{
			desc:   "GTID set " + at,
			beyond: func(*model.Txn) bool { return covered() },
			reachedAfter: func(t *model.Txn) bool {
				routed.Add(t.SourcePos)
				return covered()
			},
		}, covered(), nil
	}
}

// stopGateHold: router gặp Txn sau điểm dừng — không route nó (file tràn đĩa do process tạo được xóa).
func dropUnrouted(t *model.Txn) {
	if t.SpillOwned && t.SpillPath != "" {
		_ = os.Remove(t.SpillPath)
	}
}

// finishAtStop chờ mọi Txn đã route được apply rồi yêu cầu dừng êm. Gọi một lần.
func (p *Pipeline) finishAtStop(ctx context.Context, why string) {
	if !p.stopTripped.CompareAndSwap(false, true) {
		return
	}
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			if p.failed() != nil || ctx.Err() != nil {
				return
			}
			if p.sched == nil || p.sched.Lwd() >= p.admitted.Load() {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
		p.log.Info("stop position reached: every source change up to it is applied; stopping", "stop_at", p.stopGate.desc, "reason", why)
		if p.opts.RequestStop != nil {
			p.opts.RequestStop()
		}
	}()
}

// stopClockMargin: đồng hồ source phải qua mốc thêm chừng này giây trước khi lấy đầu log. Thời điểm commit
// được lấy ngay trước khi bản ghi commit vào log, nên một Txn commit "đúng mốc" có thể vào log muộn hơn
// một chút; khoảng đệm bảo đảm nó đã nằm trước đầu log được lấy.
const stopClockMargin = 1

// stopIdleState: trạng thái của watchStopIdle giữa các lần thăm dò.
type stopIdleState struct {
	pgMark uint64 // source PG: đầu WAL lấy khi đồng hồ source đã qua mốc (0 = chưa lấy)
}

// stopIdleReached: source đứng yên sau mốc thời gian VÀ router đã tự xử lý mọi Txn commit trước mốc.
// Trước đây dựa vào cờ đuổi kịp của monitor (PG: sent_lsn cách đầu WAL ≤ 16 MiB, mẫu trễ tới 1s, đồng hồ
// máy Strand): cờ bật khi Txn trước mốc còn ở reader hoặc trong channel, rồi router bỏ chúng khi dừng.
// Giờ mốc được đo bằng đồng hồ source và đầu log lấy SAU khi đồng hồ đó qua mốc; chỉ dừng khi vị trí
// router đã xử lý phủ đầu log đó. Mọi Txn tới sau đều commit sau mốc (beyond).
func (p *Pipeline) stopIdleReached(ctx context.Context, st *stopIdleState) (bool, error) {
	limit := p.stopGate.timeLimit + stopClockMargin
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	switch {
	case p.srcEngine == model.EnginePG:
		if st.pgMark == 0 {
			var now int64
			var head string
			if err := p.src.QueryRowContext(qctx, "SELECT floor(extract(epoch FROM clock_timestamp()))::bigint, pg_current_wal_insert_lsn()::text").Scan(&now, &head); err != nil {
				return false, err
			}
			if now <= limit {
				return false, nil
			}
			mark, err := checkpoint.ParseLSN(head)
			if err != nil {
				return false, err
			}
			// Message sau đầu WAL: reader phát Txn rỗng ở vị trí > mark nên router thấy mark cả khi các
			// bảng của pipeline không có ghi.
			if _, err := p.src.ExecContext(qctx, pgHeartbeatSQL); err != nil {
				return false, fmt.Errorf("write a WAL message to confirm the source is idle: %w", err)
			}
			st.pgMark = mark
		}
		return p.lsns != nil && p.lsns.seenLSN() >= st.pgMark, nil
	case p.srcEngine != model.EngineMySQL:
		return false, nil
	case p.posMode:
		var now int64
		if err := p.src.QueryRowContext(qctx, "SELECT UNIX_TIMESTAMP()").Scan(&now); err != nil {
			return false, err
		}
		if now <= limit {
			return false, nil
		}
		// Đầu binlog đọc SAU đồng hồ source.
		caught, known := p.posCaughtUpState(qctx)
		if !known {
			return false, fmt.Errorf("cannot read the source binlog head")
		}
		return caught, nil
	default:
		var executed string
		var now int64
		if err := p.src.QueryRowContext(qctx, "SELECT UNIX_TIMESTAMP(), @@GLOBAL.gtid_executed").Scan(&now, &executed); err != nil {
			return false, err
		}
		if now <= limit {
			return false, nil
		}
		set, err := checkpoint.ParseMySQLGTIDSet(executed)
		if err != nil {
			return false, err
		}
		p.seenMu.Lock()
		defer p.seenMu.Unlock()
		return set.IsSubsetOf(p.seen.(*checkpoint.MySQLAppliedSet)), nil
	}
}

// watchStopIdle: điểm dừng theo thời gian khi source đứng yên (không có Txn mới để chạm cổng).
func (p *Pipeline) watchStopIdle(ctx context.Context) {
	if p.stopGate == nil || p.stopGate.timeLimit == 0 {
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var st stopIdleState
	var lastErr string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if p.stopTripped.Load() {
			return
		}
		ok, err := p.stopIdleReached(ctx, &st)
		if err != nil {
			if ctx.Err() == nil && err.Error() != lastErr {
				lastErr = err.Error()
				p.log.Warn("cannot confirm that the source is idle past the stop time; waiting for the next source transaction after it", "error", err)
			}
			continue
		}
		lastErr = ""
		if ok {
			p.routeMu.Lock()
			p.finishAtStop(ctx, "the source is idle past the stop time")
			p.routeMu.Unlock()
			return
		}
	}
}
