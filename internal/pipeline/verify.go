package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/snapshot"
	"strand/internal/verify"
)

// touchedKeys ghi lại PK (số nguyên) mà CDC đã chạm tới, theo bảng nguồn (verify delta).
type touchedKeys struct {
	mu    sync.Mutex
	on    bool
	limit int
	keys  map[model.TableID]map[int64]struct{}
	over  map[model.TableID]bool
	// gen: khóa đã chạm của bảng PK tổng quát (việc C), theo khóa mã hóa.
	gen map[model.TableID]map[snapshot.Key]snapshot.Bound
}

func newTouchedKeys(limit int) *touchedKeys {
	return &touchedKeys{limit: limit, keys: map[model.TableID]map[int64]struct{}{}, over: map[model.TableID]bool{},
		gen: map[model.TableID]map[snapshot.Key]snapshot.Bound{}}
}

func (tk *touchedKeys) enable(on bool) {
	tk.mu.Lock()
	tk.on = on
	tk.mu.Unlock()
}

// addKey: như add; codec khác nil (bảng PK tổng quát) thì lưu khóa mã hóa.
func (tk *touchedKeys) addKey(t model.TableID, key []model.Value, codec *snapshot.PKCodec) {
	if codec == nil {
		tk.add(t, key)
		return
	}
	b, err := codec.BoundOf(key)
	tk.mu.Lock()
	defer tk.mu.Unlock()
	if !tk.on || err != nil {
		return
	}
	m := tk.gen[t]
	if m == nil {
		m = map[snapshot.Key]snapshot.Bound{}
		tk.gen[t] = m
	}
	if len(m) >= tk.limit {
		tk.over[t] = true
		return
	}
	m[b.K] = b
}

// takeGeneric lấy (và xóa) khóa đã chạm của bảng PK tổng quát.
func (tk *touchedKeys) takeGeneric() map[model.TableID][]snapshot.Bound {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	out := map[model.TableID][]snapshot.Bound{}
	for t, m := range tk.gen {
		bs := make([]snapshot.Bound, 0, len(m))
		for _, b := range m {
			bs = append(bs, b)
		}
		sort.Slice(bs, func(i, j int) bool { return bs[i].K < bs[j].K })
		out[t] = bs
	}
	tk.gen = map[model.TableID]map[snapshot.Key]snapshot.Bound{}
	return out
}

func (tk *touchedKeys) add(t model.TableID, key []model.Value) {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	if !tk.on || len(key) != 1 {
		return
	}
	k, ok := toInt64(key[0])
	if !ok {
		return
	}
	m := tk.keys[t]
	if m == nil {
		m = map[int64]struct{}{}
		tk.keys[t] = m
	}
	if len(m) >= tk.limit {
		tk.over[t] = true
		return
	}
	m[k] = struct{}{}
}

// take trả về và xóa các khóa đã ghi; overflow = có khóa bị bỏ do vượt giới hạn.
func (tk *touchedKeys) take() (map[model.TableID][]int64, map[model.TableID]bool) {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	out := map[model.TableID][]int64{}
	for t, m := range tk.keys {
		ks := make([]int64, 0, len(m))
		for k := range m {
			ks = append(ks, k)
		}
		sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
		out[t] = ks
	}
	over := tk.over
	tk.keys = map[model.TableID]map[int64]struct{}{}
	tk.over = map[model.TableID]bool{}
	return out, over
}

func toInt64(v model.Value) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > 1<<63-1 {
			return 0, false
		}
		return int64(x), true
	}
	return 0, false
}

func (p *Pipeline) liveVerifier() *verify.LiveVerifier {
	return verify.NewLiveVerifier(verify.LiveConfig{
		Source: p.src, Target: p.dst, Meta: p.dst, PipelineID: p.id, TargetName: p.filter.Target,
	})
}

// verifyTables chạy sample (rate) cho mọi bảng và verify các khóa trong keys; trả lỗi
// verify_mismatch nếu có lệch thật.
func (p *Pipeline) verifyTables(ctx context.Context, rate float64, keys map[model.TableID][]int64) error {
	return p.verifyTablesKeys(ctx, rate, keys, nil)
}

// verifyTablesKeys như verifyTables, kèm khóa đã chạm của bảng PK tổng quát (gen).
func (p *Pipeline) verifyTablesKeys(ctx context.Context, rate float64, keys map[model.TableID][]int64, gen map[model.TableID][]snapshot.Bound) error {
	lv := p.liveVerifier()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	p.tablesMu.RLock()
	ids := make([]model.TableID, 0, len(p.tables))
	var genIDs []model.TableID
	for id, m := range p.tables {
		switch {
		case m.PKNumeric:
			ids = append(ids, id)
		case m.PKCodec != nil:
			genIDs = append(genIDs, id) // việc C: verify theo tuple PK
		default:
			p.log.Warn("verify skipped: primary key type is not supported", "table", id.String(), "reason", fmt.Sprint(m.PKCodecErr))
		}
	}
	p.tablesMu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	var bad []string
	for _, id := range ids {
		m := p.table(id)
		var ranges [][2]int64
		if rate > 0 {
			r, err := verify.SampleRanges(ctx, p.src, id, m.PK[0], rate, 1000, rng)
			if err != nil {
				return err
			}
			ranges = append(ranges, r...)
		}
		ranges = append(ranges, verify.KeyRanges(keys[id])...)
		if len(ranges) == 0 {
			continue
		}
		rep, err := lv.VerifyRanges(ctx, id, m.PK[0], ranges)
		if err != nil {
			return err
		}
		p.log.Info("verify", "table", id.String(), "ranges", rep.RangesChecked, "rows", rep.RowsChecked,
			"mismatches", len(rep.Mismatches), "unverifiable_hot", len(rep.UnverifiableHot))
		if n := len(rep.UnverifiableHot); n > 0 {
			p.metrics.VerifyUnverifiableHotTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
		}
		if n := len(rep.Mismatches); n > 0 {
			p.metrics.VerifyMismatchTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
			pks := rep.Mismatches
			if len(pks) > 10 {
				pks = pks[:10]
			}
			bad = append(bad, fmt.Sprintf("%s: %d rows (pk %s)", id, n, strings.Join(pks, ",")))
		}
	}
	sort.Slice(genIDs, func(i, j int) bool { return genIDs[i].String() < genIDs[j].String() })
	for _, id := range genIDs {
		m := p.table(id)
		var ranges []verify.KeyRange
		if rate > 0 {
			r, err := verify.SampleKeyRanges(ctx, p.src, id, m.PKCodec, rate, 1000, rng)
			if err != nil {
				return err
			}
			ranges = append(ranges, r...)
		}
		ranges = append(ranges, verify.PointRanges(gen[id])...)
		if len(ranges) == 0 {
			continue
		}
		rep, err := lv.VerifyKeyRanges(ctx, id, m.PKCodec, ranges)
		if err != nil {
			return err
		}
		p.log.Info("verify", "table", id.String(), "ranges", rep.RangesChecked, "rows", rep.RowsChecked,
			"mismatches", len(rep.Mismatches), "unverifiable_hot", len(rep.UnverifiableHot))
		if n := len(rep.UnverifiableHot); n > 0 {
			p.metrics.VerifyUnverifiableHotTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
		}
		if n := len(rep.Mismatches); n > 0 {
			p.metrics.VerifyMismatchTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
			pks := rep.Mismatches
			if len(pks) > 10 {
				pks = pks[:10]
			}
			bad = append(bad, fmt.Sprintf("%s: %d rows (key %s)", id, n, strings.Join(pks, "; ")))
		}
	}
	if len(bad) > 0 {
		return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError, "verify found mismatches: "+strings.Join(bad, "; "), nil)
	}
	return nil
}

// postSeedVerify (02 §9): sau seed / --start-at, chỉ chuyển sang synced khi checksum ngẫu nhiên
// ~1% range PK và verify mọi range xuất hiện trong cửa sổ CDC đầu tiên đều 0 lệch.
func (p *Pipeline) postSeedVerify(ctx context.Context) {
	if p.srcEngine != model.EngineMySQL || p.dstEngine != model.EngineMySQL {
		// LiveVerifier chỉ so MySQL→MySQL. Cặp khác engine bắt buộc verify --mode full (02 §8).
		p.log.Warn("automatic post-seed verify is MySQL→MySQL only; run `strand verify --mode full` for this pipeline")
		if err := p.state.SetVerifyPending(false); err != nil {
			p.fail(err)
			return
		}
		p.verifying.Store(false)
		return
	}
	window := durationOr(p.cfg.Verify.PostSeedWindow, 30*time.Minute)
	p.touched.enable(true)
	// Verify chỉ có nghĩa khi target đã theo kịp phần log từ P (catch-up xong).
	for !(p.caughtUpWithSource(ctx) && p.sched.Lwd() >= p.admitted.Load()) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	p.log.Info("post-seed verify started", "window", window.String())
	if err := p.verifyTables(ctx, 0.01, nil); err != nil {
		if ctx.Err() == nil {
			p.fail(err)
		}
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(window):
	}
	keys, over, gen := p.takePostSeedKeys()
	for t := range over {
		p.log.Warn("post-seed verify: too many changed keys, verifying a sample of them", "table", t.String())
	}
	if err := p.verifyTablesKeys(ctx, 0, keys, gen); err != nil {
		if ctx.Err() == nil {
			p.fail(err)
		}
		return
	}
	if err := p.state.SetVerifyPending(false); err != nil {
		p.fail(err)
		return
	}
	p.verifying.Store(false)
	p.log.Info("post-seed verify passed: pipeline synced")
}

// takePostSeedKeys lấy khóa đã chạm trong cửa sổ post-seed rồi tắt ghi khóa — trừ khi verify.delta: on
// (pipeline đã bật từ lúc khởi động cho `strand verify --mode delta`; tắt ở đây làm verify delta
// về sau không còn thấy khóa nào, review R21).
func (p *Pipeline) takePostSeedKeys() (map[model.TableID][]int64, map[model.TableID]bool, map[model.TableID][]snapshot.Bound) {
	keys, over := p.touched.take()
	gen := p.touched.takeGeneric()
	if p.cfg == nil || !strings.EqualFold(p.cfg.Verify.Delta, "on") {
		p.touched.enable(false)
	}
	return keys, over, gen
}

// VerifyDelta (strand verify --mode delta): verify các khóa CDC đã chạm từ lần gọi trước
// (cần verify.delta: on), trả báo cáo theo bảng.
func (p *Pipeline) VerifyDelta(ctx context.Context) (string, error) {
	if p.srcEngine != model.EngineMySQL || p.dstEngine != model.EngineMySQL {
		return "", fmt.Errorf("verify --mode delta is supported for MySQL → MySQL pipelines only; use verify --mode full")
	}
	keys, _ := p.touched.take()
	gen := p.touched.takeGeneric()
	if err := p.verifyTablesKeys(ctx, 0, keys, gen); err != nil {
		return "", err
	}
	n := 0
	for _, ks := range keys {
		n += len(ks)
	}
	for _, ks := range gen {
		n += len(ks)
	}
	return fmt.Sprintf("delta verify: %d changed keys in %d tables, 0 mismatches", n, len(keys)+len(gen)), nil
}
