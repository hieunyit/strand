package pipeline

import (
	"context"
	"errors"
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

// Bên tiêu thụ khóa đã chạm (review N12): post-seed verify và `strand verify --mode delta` mỗi bên có
// tập riêng, take của bên này không lấy mất khóa của bên kia.
const (
	touchPostSeed = "postseed"
	touchDelta    = "delta"
)

// touchedSet: khóa đã chạm của một bên tiêu thụ.
type touchedSet struct {
	keys map[model.TableID]map[int64]struct{}
	over map[model.TableID]bool
	// gen: khóa đã chạm của bảng PK tổng quát (việc C), theo khóa mã hóa.
	gen map[model.TableID]map[snapshot.Key]snapshot.Bound
}

func newTouchedSet() *touchedSet {
	return &touchedSet{keys: map[model.TableID]map[int64]struct{}{}, over: map[model.TableID]bool{},
		gen: map[model.TableID]map[snapshot.Key]snapshot.Bound{}}
}

func (s *touchedSet) addInt(t model.TableID, k int64, limit int) {
	m := s.keys[t]
	if m == nil {
		m = map[int64]struct{}{}
		s.keys[t] = m
	}
	if _, ok := m[k]; ok {
		return
	}
	if len(m) >= limit {
		s.over[t] = true
		return
	}
	m[k] = struct{}{}
}

func (s *touchedSet) addBound(t model.TableID, b snapshot.Bound, limit int) {
	m := s.gen[t]
	if m == nil {
		m = map[snapshot.Key]snapshot.Bound{}
		s.gen[t] = m
	}
	if _, ok := m[b.K]; ok {
		return
	}
	if len(m) >= limit {
		s.over[t] = true
		return
	}
	m[b.K] = b
}

// touchedTake: kết quả take của một bên tiêu thụ. over = bảng có khóa bị bỏ do vượt giới hạn hoặc lỗi mã hóa.
type touchedTake struct {
	keys map[model.TableID][]int64
	over map[model.TableID]bool
	gen  map[model.TableID][]snapshot.Bound
}

// touchedKeys ghi lại PK mà CDC đã chạm tới, theo bảng nguồn, cho từng bên tiêu thụ đang bật
// (post-seed verify, verify delta — review N12).
type touchedKeys struct {
	mu    sync.Mutex
	limit int
	sets  map[string]*touchedSet // bên tiêu thụ đang bật → tập khóa riêng
}

func newTouchedKeys(limit int) *touchedKeys {
	return &touchedKeys{limit: limit, sets: map[string]*touchedSet{}}
}

// enable bật/tắt ghi khóa cho một bên tiêu thụ. Tắt thì bỏ tập của bên đó; bật lại bắt đầu tập rỗng.
func (tk *touchedKeys) enable(consumer string, on bool) {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	if !on {
		delete(tk.sets, consumer)
		return
	}
	if tk.sets[consumer] == nil {
		tk.sets[consumer] = newTouchedSet()
	}
}

func (tk *touchedKeys) active(consumer string) bool {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	return tk.sets[consumer] != nil
}

// addKey: như add; codec khác nil (bảng PK tổng quát) thì lưu khóa mã hóa.
func (tk *touchedKeys) addKey(t model.TableID, key []model.Value, codec *snapshot.PKCodec) {
	if codec == nil {
		tk.add(t, key)
		return
	}
	// Kiểm có bên tiêu thụ nào bật TRƯỚC khi mã hóa: codec PKWeight gọi WEIGHT_STRING trên source cho mỗi
	// khóa mới, không được chạy trên luồng CDC thường trực khi delta tracking tắt (review N05). BoundOf
	// chạy ngoài khóa (có thể là truy vấn mạng), rồi kiểm lại dưới khóa vì enable có thể đổi giữa chừng.
	tk.mu.Lock()
	on := len(tk.sets) > 0
	tk.mu.Unlock()
	if !on {
		return
	}
	b, err := codec.BoundOf(key)
	if err != nil {
		// Không đổi apply/checkpoint (CP-1..5, CO-1/2), nhưng verify phải biết phạm vi bị thiếu.
		// Không nuốt lỗi weight rồi báo sạch: dùng cùng đường incomplete như overflow cho từng consumer.
		tk.mu.Lock()
		for _, s := range tk.sets {
			s.over[t] = true
		}
		tk.mu.Unlock()
		return
	}
	tk.mu.Lock()
	defer tk.mu.Unlock()
	for _, s := range tk.sets {
		s.addBound(t, b, tk.limit)
	}
}

func (tk *touchedKeys) add(t model.TableID, key []model.Value) {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	if len(tk.sets) == 0 || len(key) != 1 {
		return
	}
	k, ok := toInt64(key[0])
	if !ok {
		return
	}
	for _, s := range tk.sets {
		s.addInt(t, k, tk.limit)
	}
}

// take trả về và xóa các khóa đã ghi của một bên tiêu thụ (khóa số + khóa PK tổng quát + cờ overflow,
// lấy cùng lúc dưới một khóa). Tập của bên khác không đổi.
func (tk *touchedKeys) take(consumer string) touchedTake {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	out := touchedTake{keys: map[model.TableID][]int64{}, over: map[model.TableID]bool{}, gen: map[model.TableID][]snapshot.Bound{}}
	s := tk.sets[consumer]
	if s == nil {
		return out
	}
	for t, m := range s.keys {
		ks := make([]int64, 0, len(m))
		for k := range m {
			ks = append(ks, k)
		}
		sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
		out.keys[t] = ks
	}
	for t, m := range s.gen {
		bs := make([]snapshot.Bound, 0, len(m))
		for _, b := range m {
			bs = append(bs, b)
		}
		sort.Slice(bs, func(i, j int) bool { return bs[i].K < bs[j].K })
		out.gen[t] = bs
	}
	out.over = s.over
	tk.sets[consumer] = newTouchedSet()
	return out
}

// restore gộp lại khóa đã take khi verify lỗi (lỗi kết nối, hủy — không phải lệch) để lần sau verify
// lại, không mất (review N12). Bên tiêu thụ đã tắt thì bỏ qua.
func (tk *touchedKeys) restore(consumer string, tt touchedTake) {
	tk.mu.Lock()
	defer tk.mu.Unlock()
	s := tk.sets[consumer]
	if s == nil {
		return
	}
	for t, ks := range tt.keys {
		for _, k := range ks {
			s.addInt(t, k, tk.limit)
		}
	}
	for t, bs := range tt.gen {
		for _, b := range bs {
			s.addBound(t, b, tk.limit)
		}
	}
	for t, o := range tt.over {
		if o {
			s.over[t] = true
		}
	}
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

// liveMySQL: LiveVerifier (so ảnh MySQL nguyên văn) chỉ dùng được cho MySQL→MySQL; cặp khác so chuỗi
// chính tắc (canonicalVerifier).
func (p *Pipeline) liveMySQL() bool {
	return p.srcEngine == model.EngineMySQL && p.dstEngine == model.EngineMySQL
}

// canonicalVerifier so từng row bằng chuỗi chính tắc (mọi cặp engine) và kiểm lại row lệch, chờ target
// apply tới vị trí log của source (gtid_executed / WAL insert LSN + watermark) như LiveVerifier.
func (p *Pipeline) canonicalVerifier() *verify.MerkleVerifier {
	return verify.NewMerkleVerifier(verify.FullVerifyConfig{
		Source: p.src, Target: p.dst, Meta: p.dst, PipelineID: p.id,
		SourceEngine: p.srcEngine, TargetEngine: p.dstEngine, TargetName: p.filter.Target,
		ApplyWaiter: &verify.LogApplyWaiter{Source: p.src, Meta: p.dst, PipelineID: p.id,
			SourceEngine: p.srcEngine, TargetEngine: p.dstEngine},
	})
}

// targetKeyCodec: codec PK của bảng đích, đã kiểm cùng thứ tự với source (như checkSnapshotKey, không
// ghi vào meta). exact: cặp MySQL→PG sắp khóa khác thứ tự nhưng thỏa snapshot.CrossEngineKeySet (Gate 2
// "_ci → C") — range theo khóa source không đọc được ở target, verify theo range/điểm không áp dụng.
func (p *Pipeline) targetKeyCodec(ctx context.Context, id model.TableID, m *tableMeta) (dc *snapshot.PKCodec, exact bool, err error) {
	p.tablesMu.RLock()
	dc, exact = m.TargetPKCodec, m.ExactKeys
	p.tablesMu.RUnlock()
	if dc != nil {
		return dc, exact, nil
	}
	tgt := p.filter.Target(id)
	if p.dstEngine == model.EnginePG {
		dc, err = pgPKCodec(ctx, p.dst, tgt, m.PK)
	} else {
		dc, err = mysqlPKCodec(ctx, p.dst, tgt, m.PK)
	}
	if err != nil {
		return nil, false, fmt.Errorf("verify %s: target primary key: %w", id, err)
	}
	if p.srcEngine == p.dstEngine {
		err = m.PKCodec.SameOrder(dc)
	} else {
		err = snapshot.CrossEngineSameOrder(m.PKCodec, dc)
		if err != nil && p.srcEngine == model.EngineMySQL && p.dstEngine == model.EnginePG && snapshot.CrossEngineKeySet(m.PKCodec, dc) == nil {
			return dc, true, nil
		}
	}
	if err != nil {
		return nil, false, fmt.Errorf("verify %s: target orders the primary key differently from the source: %w", id, err)
	}
	return dc, false, nil
}

// genVerifyMode: cách verify một bảng PK tổng quát (việc C) — "live" (LiveVerifier, MySQL→MySQL, so ảnh
// nguyên văn theo range tuple), "canonical" (MerkleVerifier.VerifyKeyRanges, PG→PG / MySQL→PG, so chuỗi
// chính tắc), "skip" (bảng khóa chính xác sắp khác thứ tự: chỉ `strand verify --mode full` so được).
func genVerifyMode(liveMySQL, exact bool) string {
	switch {
	case liveMySQL:
		return "live"
	case exact:
		return "skip"
	}
	return "canonical"
}

// tableVerdict là kết quả verify một bảng (chung cho LiveVerifier và canonicalVerifier).
type tableVerdict struct {
	ranges, rows    int
	mismatches, hot []string
}

// verifyTables chạy sample (rate) cho mọi bảng và verify các khóa trong keys; trả lỗi
// verify_mismatch nếu có lệch thật. skipped: bảng không verify được theo range/khóa (genVerifyMode
// "skip") — bảng có khóa cần verify (hoặc có mẫu) bị bỏ.
func (p *Pipeline) verifyTables(ctx context.Context, rate float64, keys map[model.TableID][]int64) (skipped []model.TableID, err error) {
	return p.verifyTablesKeys(ctx, rate, keys, nil)
}

// verifyTablesKeys như verifyTables, kèm khóa đã chạm của bảng PK tổng quát (gen).
func (p *Pipeline) verifyTablesKeys(ctx context.Context, rate float64, keys map[model.TableID][]int64, gen map[model.TableID][]snapshot.Bound) (skipped []model.TableID, err error) {
	lv := p.liveVerifier()
	var cv *verify.MerkleVerifier
	if !p.liveMySQL() {
		cv = p.canonicalVerifier()
	}
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
			r, err := verify.SampleRangesEngine(ctx, p.src, p.srcEngine, id, m.PK[0], rate, 1000, rng)
			if err != nil {
				return nil, err
			}
			ranges = append(ranges, r...)
		}
		ranges = append(ranges, verify.KeyRanges(keys[id])...)
		if len(ranges) == 0 {
			continue
		}
		var rep tableVerdict
		if cv != nil {
			r, err := cv.VerifyRanges(ctx, id, m.PK[0], ranges)
			if err != nil {
				return nil, err
			}
			rep = tableVerdict{r.RangesChecked, r.RowsChecked, r.Mismatches, r.UnverifiableHot}
		} else {
			r, err := lv.VerifyRanges(ctx, id, m.PK[0], ranges)
			if err != nil {
				return nil, err
			}
			rep = tableVerdict{r.RangesChecked, r.RowsChecked, r.Mismatches, r.UnverifiableHot}
		}
		p.log.Info("verify", "table", id.String(), "ranges", rep.ranges, "rows", rep.rows,
			"mismatches", len(rep.mismatches), "unverifiable_hot", len(rep.hot))
		if n := len(rep.hot); n > 0 {
			p.metrics.VerifyUnverifiableHotTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
		}
		if n := len(rep.mismatches); n > 0 {
			p.metrics.VerifyMismatchTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
			pks := rep.mismatches
			if len(pks) > 10 {
				pks = pks[:10]
			}
			bad = append(bad, fmt.Sprintf("%s: %d rows (pk %s)", id, n, strings.Join(pks, ",")))
		}
	}
	sort.Slice(genIDs, func(i, j int) bool { return genIDs[i].String() < genIDs[j].String() })
	for _, id := range genIDs {
		m := p.table(id)
		if rate <= 0 && len(gen[id]) == 0 {
			continue
		}
		var dc *snapshot.PKCodec
		exact := false
		if cv != nil {
			if dc, exact, err = p.targetKeyCodec(ctx, id, m); err != nil {
				return nil, err
			}
		}
		var ranges []verify.KeyRange
		var rep tableVerdict
		switch genVerifyMode(cv == nil, exact) {
		case "skip":
			p.log.Warn("verify skipped: the target orders the primary key differently from the source (exact-key table); run `strand verify --mode full` for it",
				"table", id.String(), "changed_keys", len(gen[id]))
			skipped = append(skipped, id)
			continue
		case "live":
			if rate > 0 {
				r, err := verify.SampleKeyRanges(ctx, p.src, id, m.PKCodec, rate, 1000, rng)
				if err != nil {
					return nil, err
				}
				ranges = append(ranges, r...)
			}
			ranges = append(ranges, verify.PointRanges(gen[id])...)
			if len(ranges) == 0 {
				continue
			}
			r, err := lv.VerifyKeyRanges(ctx, id, m.PKCodec, ranges)
			if err != nil {
				return nil, err
			}
			rep = tableVerdict{r.RangesChecked, r.RowsChecked, r.Mismatches, r.UnverifiableHot}
		default:
			// PG→PG, MySQL→PG: mẫu theo tuple PK của source (SampleKeyRangesEngine, chặn theo rate) + khóa
			// đã chạm, so chuỗi chính tắc và kiểm lại row lệch.
			if rate > 0 {
				r, err := verify.SampleKeyRangesEngine(ctx, p.src, p.srcEngine, id, m.PKCodec, rate, 1000, rng)
				if err != nil {
					return nil, err
				}
				ranges = append(ranges, r...)
			}
			ranges = append(ranges, verify.PointRanges(gen[id])...)
			if len(ranges) == 0 {
				continue
			}
			r, err := cv.VerifyKeyRanges(ctx, id, m.PKCodec, dc, ranges)
			if err != nil {
				return nil, err
			}
			rep = tableVerdict{r.RangesChecked, r.RowsChecked, r.Mismatches, r.UnverifiableHot}
		}
		p.log.Info("verify", "table", id.String(), "ranges", rep.ranges, "rows", rep.rows,
			"mismatches", len(rep.mismatches), "unverifiable_hot", len(rep.hot))
		if n := len(rep.hot); n > 0 {
			p.metrics.VerifyUnverifiableHotTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
		}
		if n := len(rep.mismatches); n > 0 {
			p.metrics.VerifyMismatchTotal.WithLabelValues(p.id, id.String()).Add(float64(n))
			pks := rep.mismatches
			if len(pks) > 10 {
				pks = pks[:10]
			}
			bad = append(bad, fmt.Sprintf("%s: %d rows (key %s)", id, n, strings.Join(pks, "; ")))
		}
	}
	if len(bad) > 0 {
		return skipped, exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError, "verify found mismatches: "+strings.Join(bad, "; "), nil)
	}
	return skipped, nil
}

// postSeedVerify (02 §9): sau seed / --start-at, chỉ chuyển sang synced khi checksum ngẫu nhiên
// ~1% range PK và verify mọi range xuất hiện trong cửa sổ CDC đầu tiên đều 0 lệch. Mọi cặp engine:
// MySQL→MySQL so bằng LiveVerifier, PG→PG / MySQL→PG so chuỗi chính tắc (canonicalVerifier, kiểm lại row
// lệch chờ target apply tới vị trí log của source). Lệch thật: dừng với verify_mismatch (exit 5).
func (p *Pipeline) postSeedVerify(ctx context.Context) {
	window := durationOr(p.cfg.Verify.PostSeedWindow, 30*time.Minute)
	p.touched.enable(touchPostSeed, true)
	// Verify chỉ có nghĩa khi target đã theo kịp phần log từ P (catch-up xong) và bảng không có trong
	// seed (seed PG một phần: snapshot logic) đã snapshot xong.
	for !postSeedReady(p.caughtUpWithSource(ctx), p.sched.Lwd(), p.admitted.Load(), p.bootstrap.Load()) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	p.log.Info("post-seed verify started", "window", window.String(), "source", string(p.srcEngine), "target", string(p.dstEngine))
	skipped, err := p.verifyTables(ctx, 0.01, nil)
	if err != nil {
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
	// SD-1/CP-1 không thay đổi; chỉ trạng thái verify được siết: thiếu khóa không chứng minh đủ
	// cửa sổ CDC nên giữ VerifyPending, không được báo synced sau khi chỉ kiểm phần còn lại.
	if _, err := deltaVerifyResult(touchedTake{over: over}); err != nil {
		p.fail(err)
		return
	}
	more, err := p.verifyTablesKeys(ctx, 0, keys, gen)
	if err != nil {
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
	if s := skippedNames(append(skipped, more...)); len(s) > 0 {
		p.log.Warn("post-seed verify passed except for tables it cannot verify by key range; run `strand verify --mode full` for them",
			"tables", strings.Join(s, ","))
	}
	p.log.Info("post-seed verify passed: pipeline synced")
}

// postSeedReady: điều kiện bắt đầu post-seed verify — reader đã theo kịp source, mọi Txn đã nhận đã
// apply (lwd ≥ admitted), và không còn bảng đang snapshot logic (bootstrap).
func postSeedReady(caughtUp bool, lwd, admitted uint64, bootstrapping bool) bool {
	return caughtUp && lwd >= admitted && !bootstrapping
}

// skippedNames: tên bảng (đã sắp, không trùng).
func skippedNames(ids []model.TableID) []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range ids {
		if n := id.String(); !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// takePostSeedKeys lấy khóa đã chạm trong cửa sổ post-seed rồi tắt ghi khóa của post-seed. Tập của
// verify delta (verify.delta: on) là tập riêng, không bị lấy hay tắt ở đây (review R21, N12).
func (p *Pipeline) takePostSeedKeys() (map[model.TableID][]int64, map[model.TableID]bool, map[model.TableID][]snapshot.Bound) {
	tt := p.touched.take(touchPostSeed)
	p.touched.enable(touchPostSeed, false)
	return tt.keys, tt.over, tt.gen
}

// VerifyDelta (strand verify --mode delta): verify các khóa CDC đã chạm từ lần gọi trước
// (cần verify.delta: on), trả báo cáo theo bảng. Verify lỗi (không phải lệch) thì gộp lại khóa đã lấy
// để lần sau verify lại; có bảng vượt giới hạn ghi khóa thì không báo sạch (review N12).
func (p *Pipeline) VerifyDelta(ctx context.Context) (string, error) {
	// MySQL→MySQL so bằng LiveVerifier; PG→PG, MySQL→PG so chuỗi chính tắc (canonicalVerifier) — cả hai
	// kiểm lại row lệch, row đổi liên tục báo unverifiable_hot.
	// Đang pause: router dừng nên không apply gì — kiểm lại row lệch chờ apply chỉ hết hạn rồi lỗi
	// "target did not apply". Báo ngay, trước khi lấy tập khóa đã đổi (giữ nguyên cho lần verify sau).
	if p.paused.Load() {
		return "", exitcode.New(exitcode.ErrPipelinePaused, exitcode.Fenced, fmt.Sprintf(
			"pipeline %s is paused: resume it (`strand resume`) or stop it before verify", p.id), nil)
	}
	if !p.touched.active(touchDelta) {
		return "", fmt.Errorf("verify --mode delta needs verify.delta: on in the pipeline config (no changed keys are being tracked)")
	}
	tt := p.touched.take(touchDelta)
	skipped, err := p.verifyTablesKeys(ctx, 0, tt.keys, tt.gen)
	if err != nil {
		var xe *exitcode.Error
		if !errors.As(err, &xe) || xe.Code != exitcode.ErrVerifyMismatch {
			p.touched.restore(touchDelta, tt)
		}
		return "", err
	}
	return deltaVerifyResult(tt, skipped...)
}

// deltaVerifyResult dựng kết quả verify delta sau khi các khóa đã lấy đều 0 lệch. Bảng vượt giới hạn
// ghi khóa chỉ được verify một phần (mẫu) nên kết quả là incomplete — lỗi verify_incomplete (exit 5),
// không phải "0 mismatches" cho toàn bộ tập thay đổi (review N12). Bảng skipped (khóa chính xác sắp khác
// thứ tự, genVerifyMode "skip"): khóa đã chạm không được verify — cũng incomplete.
func deltaVerifyResult(tt touchedTake, skipped ...model.TableID) (string, error) {
	n := 0
	tables := map[model.TableID]bool{}
	for t, ks := range tt.keys {
		n += len(ks)
		tables[t] = true
	}
	for t, ks := range tt.gen {
		n += len(ks)
		tables[t] = true
	}
	var over []string
	for t, o := range tt.over {
		if o {
			over = append(over, t.String())
			tables[t] = true
		}
	}
	var why []string
	if len(over) > 0 {
		sort.Strings(over)
		why = append(why, fmt.Sprintf("%d tables have incomplete changed-key tracking (limit or encoding failure): %s", len(over), strings.Join(over, ", ")))
	}
	if sk := skippedNames(skipped); len(sk) > 0 {
		why = append(why, fmt.Sprintf("%d tables cannot be verified by key (the target orders the primary key differently): %s", len(sk), strings.Join(sk, ", ")))
	}
	if len(why) > 0 {
		return "", exitcode.New(exitcode.ErrVerifyIncomplete, exitcode.DataError,
			fmt.Sprintf("delta verify incomplete: %s; %d changed keys in %d tables verified, 0 mismatches; run `strand verify --mode sample` or `--mode full` on those tables",
				strings.Join(why, "; "), n, len(tables)), nil)
	}
	return fmt.Sprintf("delta verify: %d changed keys in %d tables, 0 mismatches", n, len(tables)), nil
}
