package verify

// Full verify khi target sắp PK KHÁC thứ tự source (Gate 2 "_ci → C", snapshot.CrossEngineKeySet), ví dụ
// VARCHAR utf8mb4_0900_ai_ci ở MySQL → text COLLATE "C" ở PG. Merkle theo khóa (merkle_keys.go) hỏi CÙNG
// range khóa ở hai phía; ở đây range theo thứ tự source không biểu diễn được bằng SQL ở target. Cách làm:
//
//  1. Chunk theo khóa SOURCE như VerifyTableFullKeys (keyChunks: "< min", các range keyset, "> max" — phủ
//     toàn bộ không gian khóa, kể cả hai đầu mở); hash từng chunk ở source bằng SQL như cũ
//     ("count:sum 60 bit đầu MD5 của row chính tắc").
//  2. Target: MỘT lượt quét toàn bảng theo trang (snapshot.ScanPGKeys). Mỗi row: khóa source = codec SOURCE
//     trên giá trị khóa target (WEIGHT_STRING của chính MySQL source, PAD SPACE qua PadSpaceWeightKey);
//     chunk = chunkOf (tìm nhị phân trên cận đã mã hóa); cộng count và giá trị 60 bit MD5 của row chính
//     tắc (PG tính trong SQL, cùng biểu thức với hash chunk) vào chunk đó. Tổng là phép cộng — không phụ
//     thuộc thứ tự — nên kết quả bằng đúng hash SQL của chunk nếu target có đúng các row đó. Bộ nhớ:
//     một bộ đếm 128 bit mỗi chunk (O(số chunk)), không giữ row.
//  3. So từng chunk (số học: count và tổng, không so chuỗi). Chunk lệch: đọc row source của chunk bằng SQL
//     range; row target của các chunk lệch lấy bằng một lượt quét nữa (mỗi lượt tối đa crossBadGroup
//     chunk, để bộ nhớ chặn ở crossBadGroup × cỡ chunk), rồi so từng row theo khóa chính tắc dạng
//     "len:value" (review F6: không trùng giữa hai tuple khác nhau).
//
// Vì sao không có va chạm "row target rơi vào chunk khác row source tương ứng": hai phía cùng một codec
// source cho cùng một giá trị khóa (byte khóa ở target = byte ở source, CrossEngineKeySet điều kiện 2–3),
// nên row khớp nằm cùng chunk. Row chỉ có ở target (kể cả biến thể 'A' của 'a' cùng khóa source) làm count
// / tổng của chunk của nó lệch và được liệt kê ở bước 3 — khóa chính tắc khác byte thì khác nhau.

import (
	"context"
	"fmt"
	"math/big"
	"math/bits"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// crossBadGroup: số chunk lệch tối đa được liệt kê row trong một lượt quét target.
const crossBadGroup = 16

// crossScanPage: số row target mỗi trang quét.
const crossScanPage = 1000

// chunkAcc: count và tổng 128 bit của các giá trị 60 bit của một chunk.
type chunkAcc struct {
	n      int64
	hi, lo uint64
}

func (a *chunkAcc) add(h uint64) {
	var c uint64
	a.lo, c = bits.Add64(a.lo, h, 0)
	a.hi += c
	a.n++
}

func (a chunkAcc) sum() *big.Int {
	s := new(big.Int).SetUint64(a.hi)
	s.Lsh(s, 64)
	return s.Add(s, new(big.Int).SetUint64(a.lo))
}

// sameHash so hash SQL "count:sum" của source với bộ đếm target theo giá trị số (không theo chuỗi).
func sameHash(src string, a chunkAcc) (bool, error) {
	cs, ss, ok := strings.Cut(src, ":")
	if !ok {
		return false, fmt.Errorf("malformed chunk hash %q", src)
	}
	n, err := strconv.ParseInt(cs, 10, 64)
	if err != nil {
		return false, fmt.Errorf("malformed chunk hash %q: %w", src, err)
	}
	s, ok := new(big.Int).SetString(ss, 10)
	if !ok {
		return false, fmt.Errorf("malformed chunk hash %q", src)
	}
	return n == a.n && s.Cmp(a.sum()) == 0, nil
}

// chunkOf trả chỉ số chunk chứa khóa source k trong danh sách keyChunks (thứ tự source, phủ kín không gian
// khóa). Lỗi nếu k không thuộc chunk nào — chỉ xảy ra khi danh sách không liền mạch (lỗi lập trình),
// không được "đoán" chunk.
func chunkOf(chunks []keyChunk, k snapshot.Key) (int, error) {
	n := len(chunks)
	if n == 1 && chunks[0].all {
		return 0, nil
	}
	if n < 3 || !chunks[0].below || !chunks[n-1].above {
		return 0, fmt.Errorf("malformed key chunk list")
	}
	if k < chunks[0].lo.K {
		return 0, nil
	}
	if k > chunks[n-1].hi.K {
		return n - 1, nil
	}
	r := chunks[1 : n-1]
	i := sort.Search(len(r), func(i int) bool { return r[i].hi.K >= k })
	if i == len(r) || !inLo(k, r[i].lo) {
		return 0, fmt.Errorf("key %x falls between verify chunks", string(k))
	}
	return i + 1, nil
}

func inLo(k snapshot.Key, lo snapshot.Bound) bool {
	if lo.Excl {
		return k > lo.K
	}
	return k >= lo.K
}

// VerifyTableFullKeysExact: full verify của bảng t khi target (PG) sắp PK khác thứ tự source (MySQL) —
// xem đầu file. srcCodec phải tính được khóa source (Weigh trên DB source); caller đã kiểm
// snapshot.CrossEngineKeySet(srcCodec, dstCodec).
func (mv *MerkleVerifier) VerifyTableFullKeysExact(ctx context.Context, t model.TableID, srcCodec, dstCodec *snapshot.PKCodec) (*FullVerifyReport, error) {
	if mv.cfg.Resume {
		return nil, fmt.Errorf("table %s: --resume is not supported for a primary key that is not a single integer column", t)
	}
	if mv.cfg.SourceEngine != model.EngineMySQL || mv.cfg.TargetEngine != model.EnginePG {
		return nil, fmt.Errorf("table %s: exact-key full verify needs a MySQL source and a PostgreSQL target", t)
	}
	if err := snapshot.CrossEngineKeySet(srcCodec, dstCodec); err != nil {
		return nil, fmt.Errorf("table %s: %w", t, err)
	}
	src, dst, srcCols, dstCols, srcPK, dstPK, err := mv.keySides(ctx, t, srcCodec, dstCodec)
	if err != nil {
		return nil, err
	}
	chunks, err := mv.keyChunks(ctx, src)
	if err != nil {
		return nil, err
	}
	rep := &FullVerifyReport{Table: t.String(), TotalChunks: len(chunks)}

	// 1. Hash source theo chunk.
	var tick <-chan time.Time
	if mv.cfg.Rate > 0 {
		tk := time.NewTicker(time.Duration(float64(time.Second) / mv.cfg.Rate))
		defer tk.Stop()
		tick = tk.C
	}
	srcHash := make([]string, len(chunks))
	for i, c := range chunks {
		if mv.cfg.StopAfter > 0 && rep.HashedChunks >= mv.cfg.StopAfter {
			return rep, fmt.Errorf("full verify of %s stopped after %d chunks: %w", t, rep.HashedChunks, ErrVerifyInterrupted)
		}
		if tick != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick:
			}
		}
		if srcHash[i], err = src.hash(ctx, c, srcCols); err != nil {
			return nil, fmt.Errorf("source: %w", err)
		}
		rep.HashedChunks++
	}

	// 2. Một lượt quét target, gom vào chunk theo khóa source.
	dq := snapshot.PGKeySQL{Table: dst.table, Codec: dstCodec}
	rowHash := fmt.Sprintf("(('x' || substr(md5(%s), 1, 15))::bit(60)::bigint)::text", rowExpression(model.EnginePG, dstCols))
	acc := make([]chunkAcc, len(chunks))
	err = snapshot.ScanPGKeys(ctx, dst.db, dq, []string{rowHash}, crossScanPage, func(keys [][]model.Value, ex [][]string) error {
		ks, err := srcCodec.EncodeMany(keys)
		if err != nil {
			return fmt.Errorf("source key of a target row of %s: %w", dst.table, err)
		}
		for i, k := range ks {
			ci, err := chunkOf(chunks, k)
			if err != nil {
				return err
			}
			h, err := strconv.ParseUint(ex[i][0], 10, 64)
			if err != nil || h >= 1<<60 {
				return fmt.Errorf("row hash %q of %s is not a 60-bit value", ex[i][0], dst.table)
			}
			acc[ci].add(h)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("target: %w", err)
	}
	var bad []int
	for i := range chunks {
		ok, err := sameHash(srcHash[i], acc[i])
		if err != nil {
			return nil, fmt.Errorf("source: %w", err)
		}
		if !ok {
			bad = append(bad, i)
		}
	}
	rep.MismatchedChunks = len(bad)

	// 3. Liệt kê row lệch, từng nhóm chunk.
	canon := make([]string, 0, len(dstPK)+1)
	for _, pc := range dstPK {
		canon = append(canon, canonicalValueExpr(model.EnginePG, pc))
	}
	canon = append(canon, rowExpression(model.EnginePG, dstCols))
	for g := 0; g < len(bad); g += crossBadGroup {
		group := bad[g:min(g+crossBadGroup, len(bad))]
		want := make(map[int]map[string]string, len(group))
		for _, ci := range group {
			want[ci] = map[string]string{}
		}
		err := snapshot.ScanPGKeys(ctx, dst.db, dq, canon, crossScanPage, func(keys [][]model.Value, ex [][]string) error {
			ks, err := srcCodec.EncodeMany(keys)
			if err != nil {
				return fmt.Errorf("source key of a target row of %s: %w", dst.table, err)
			}
			for i, k := range ks {
				ci, err := chunkOf(chunks, k)
				if err != nil {
					return err
				}
				m, ok := want[ci]
				if !ok {
					continue
				}
				parts := make([]string, len(dstPK))
				for j := range dstPK {
					parts[j] = fmt.Sprintf("%d:%s", len(ex[i][j]), ex[i][j])
				}
				m[strings.Join(parts, ",")] = ex[i][len(dstPK)]
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("target: %w", err)
		}
		for _, ci := range group {
			s, sb, err := src.rowsBounds(ctx, chunks[ci], srcCols, srcPK)
			if err != nil {
				return nil, fmt.Errorf("source: %w", err)
			}
			d := want[ci]
			rep.RangesChecked++
			rep.RowsChecked += len(s)
			for k, sv := range s {
				if dv, ok := d[k]; !ok || dv != sv {
					rep.Mismatches = append(rep.Mismatches, k)
					if b, ok := sb[k]; ok {
						rep.MismatchKeys = append(rep.MismatchKeys, b)
					}
				}
			}
			for k := range d {
				if _, ok := s[k]; !ok {
					rep.Mismatches = append(rep.Mismatches, k)
				}
			}
		}
	}
	return rep, nil
}
