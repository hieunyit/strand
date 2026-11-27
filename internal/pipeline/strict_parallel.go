package pipeline

import (
	"context"
	"strings"
	"sync"

	"strand/internal/model"
	"strand/internal/txntrace"
)

// Strict song song (target.strict_parallel: auto, source MySQL): như Parallel Replicat của GoldenGate và
// replica_preserve_commit_order của MySQL. Bộ cắt batch (strictBatcher) lấy Txn đúng thứ tự seq từ
// strictDispatcher, cắt thành batch LIÊN TIẾP (CP-5: row toàn cục của batch là vị trí Txn cuối), rồi chỉ
// giao một batch cho writer rảnh khi nó không chung khóa writeset (PK, unique key cũ/mới, khóa cha FK —
// 02 §6) với batch nào đang ghi. Writer ghi dữ liệu song song, chờ lượt (CommitOrderer) để ghi row toàn
// cục và commit theo đúng thứ tự batch: target vẫn luôn là một prefix của thứ tự commit ở source.
// Batch chung khóa với batch đang ghi thì chờ batch đó commit: row nóng vẫn được gom thành batch lớn và
// ghi tuần tự như trước. Khóa ngoài writeset (gap lock, ràng buộc chỉ có ở target) xử lý bằng nhường
// lượt (CommitOrderer.WaitTurnOrYield).

// strictParallelOn: strict song song áp dụng cho pipeline này.
func (p *Pipeline) strictParallelOn() bool {
	if !p.strict || p.srcEngine != model.EngineMySQL || len(p.cascade.signatures) > 0 {
		// Source PG: walsender là trần (một luồng giải mã), ghi song song không thêm gì. Cascade FK gốc
		// ở target MySQL chạm row con mà writeset không thấy.
		return false
	}
	return !strings.EqualFold(strings.TrimSpace(p.cfg.Target.StrictParallel), "off")
}

// batchKeys: khóa writeset của một batch strict.
type batchKeys struct {
	all    bool                           // chặn mọi batch khác (DDL)
	tables map[string]bool                // bảng bị khóa cả bảng
	keys   map[string]map[uint64]struct{} // bảng → hash khóa
}

func (b *batchKeys) touches(tbl string) bool {
	return b.tables[tbl] || len(b.keys[tbl]) > 0
}

// conflicts: hai batch có thể chạm cùng row.
func (b *batchKeys) conflicts(o *batchKeys) bool {
	if b.all || o.all {
		return true
	}
	for tbl := range b.tables {
		if o.touches(tbl) {
			return true
		}
	}
	for tbl := range o.tables {
		if b.touches(tbl) {
			return true
		}
	}
	for tbl, ks := range b.keys {
		oks := o.keys[tbl]
		if len(oks) == 0 {
			continue
		}
		small, big := ks, oks
		if len(big) < len(small) {
			small, big = big, small
		}
		for k := range small {
			if _, ok := big[k]; ok {
				return true
			}
		}
	}
	return false
}

// keysOfBatch dựng khóa writeset của batch. tableLevel: số khóa một bảng trong một Txn mà từ đó khóa cả
// bảng (như scheduler, table_level_threshold).
func keysOfBatch(batch []*model.Txn, tableLevel int) *batchKeys {
	b := &batchKeys{tables: map[string]bool{}, keys: map[string]map[uint64]struct{}{}}
	for _, t := range batch {
		if t.DDL != "" {
			b.all = true
			return b
		}
		if t.IsSpilled() {
			for _, tid := range t.SpilledTables {
				name := tid.String()
				if _, ok := t.SpillWriteset[name]; !ok {
					b.tables[name] = true
				}
			}
		}
		for i := range t.Rows {
			if len(t.Rows[i].Key) == 0 {
				// Bảng keyless: row khớp theo mọi cột, không có khóa để so.
				b.tables[t.Rows[i].Table.String()] = true
			}
		}
		for tbl, ks := range t.WritesetByTable() {
			if b.tables[tbl] {
				continue
			}
			if tableLevel > 0 && len(ks) > tableLevel {
				b.tables[tbl] = true
				delete(b.keys, tbl)
				continue
			}
			m := b.keys[tbl]
			if m == nil {
				m = make(map[uint64]struct{}, len(ks))
				b.keys[tbl] = m
			}
			for _, k := range ks {
				m[k] = struct{}{}
			}
		}
	}
	return b
}

// strictBatch: một batch đã cắt, giao cho writer.
type strictBatch struct {
	txns []*model.Txn
	rows int64
	keys *batchKeys
}

// inflightSet: các batch đã giao mà chưa commit (hoặc đã lỗi). applying = true khi batch còn đang ghi dữ
// liệu (có thể xin thêm lock); false khi đã ghi xong và chỉ chờ lượt commit (không xin thêm lock nào).
// Batch mới chỉ phải tránh batch CÒN ĐANG GHI chung khóa: chạm row của batch đã ghi xong thì chỉ chờ lock
// tới khi batch đó commit (nó đi trước theo seq và không chờ gì từ batch mới), không có vòng chờ.
type inflightSet struct {
	mu   sync.Mutex
	cond *sync.Cond
	set  map[*batchKeys]bool // → applying
	dead bool
}

func newInflightSet() *inflightSet {
	s := &inflightSet{set: map[*batchKeys]bool{}}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// acquire chờ tới khi k không xung đột với batch nào đang ghi rồi đánh dấu k đang ghi. false: đã dừng.
func (s *inflightSet) acquire(k *batchKeys) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for !s.dead {
		clash := false
		for o, applying := range s.set {
			if applying && k.conflicts(o) {
				clash = true
				break
			}
		}
		if !clash {
			s.set[k] = true
			return true
		}
		s.cond.Wait()
	}
	return false
}

// applied: k đã ghi xong dữ liệu, chỉ còn chờ lượt commit.
func (s *inflightSet) applied(k *batchKeys) {
	s.mu.Lock()
	if _, ok := s.set[k]; ok {
		s.set[k] = false
	}
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *inflightSet) release(k *batchKeys) {
	s.mu.Lock()
	delete(s.set, k)
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *inflightSet) stop() {
	s.mu.Lock()
	s.dead = true
	s.mu.Unlock()
	s.cond.Broadcast()
}

// strictBatcher cắt Txn (đúng thứ tự seq) thành batch liên tiếp và giao batch không xung đột cho writer.
func (p *Pipeline) strictBatcher(ctx context.Context, in <-chan *model.Txn, out chan<- strictBatch, inflight *inflightSet, lim batchLimits) {
	defer close(out)
	go func() {
		<-ctx.Done()
		inflight.stop()
	}()
	tableLevel := p.sched.Config().TableLevelThreshold
	var stash *model.Txn
	for {
		first := stash
		stash = nil
		if first == nil {
			select {
			case <-ctx.Done():
				return
			case first = <-in:
			}
		}
		if first == nil {
			return
		}
		if txntrace.On {
			txntrace.Put(txntrace.GatherStart, 0, 0, first.Seq(), 0, 0)
		}
		batch, rows, next := gatherBatch(ctx, first, in, lim)
		if txntrace.On {
			txntrace.Put(txntrace.GatherEnd, 0, 0, first.Seq(), int64(len(batch)), 0)
		}
		stash = next
		k := keysOfBatch(batch, tableLevel)
		if !inflight.acquire(k) {
			return
		}
		select {
		case out <- strictBatch{txns: batch, rows: rows, keys: k}:
		case <-ctx.Done():
			return
		}
	}
}

// strictParallelWorker ghi các batch strict do strictBatcher giao.
func (p *Pipeline) strictParallelWorker(ctx context.Context, id int, w batchWriter, in <-chan strictBatch, inflight *inflightSet) {
	for {
		var b strictBatch
		var ok bool
		select {
		case <-ctx.Done():
			return
		case b, ok = <-in:
		}
		if !ok {
			return
		}
		k := b.keys
		cont := p.applyStrictWith(ctx, id, w, b.txns, b.rows, func() { inflight.applied(k) })
		inflight.release(b.keys)
		if !cont {
			return
		}
	}
}
