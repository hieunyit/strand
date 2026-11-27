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
	// reachedIdle (có thể nil): đã tới điểm dừng dù không có Txn mới (source đứng yên).
	reachedIdle func() bool
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
			// Source đứng yên: đã qua mốc theo đồng hồ và reader đã đọc tới đầu log thì mọi Txn còn lại
			// (nếu có) commit sau mốc.
			reachedIdle: func() bool {
				return time.Now().Unix() > limit && p.sourceHead.Load() == sourceHeadCaughtUp
			},
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

// watchStopIdle: điểm dừng theo thời gian khi source đứng yên (không có Txn mới để chạm cổng).
func (p *Pipeline) watchStopIdle(ctx context.Context) {
	if p.stopGate == nil || p.stopGate.reachedIdle == nil {
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if p.stopTripped.Load() {
			return
		}
		if p.stopGate.reachedIdle() {
			p.routeMu.Lock()
			p.finishAtStop(ctx, "the source is idle past the stop time")
			p.routeMu.Unlock()
			return
		}
	}
}
