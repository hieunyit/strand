package snapshot

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// planBounds dựng các cận khóa của plan khóa tổng quát: min, max, khóa cuối đã flush (nếu có).
func planBounds(kq keySQL, p *SnapshotPlan) (minB, maxB, last Bound, hasLast bool, err error) {
	if minB, err = kq.bound(fromKeyVals(p.MinKey)); err != nil {
		return
	}
	if maxB, err = kq.bound(fromKeyVals(p.MaxKey)); err != nil {
		return
	}
	if len(p.LastKey) > 0 {
		if last, err = kq.bound(fromKeyVals(p.LastKey)); err != nil {
			return
		}
		hasLast = true
	}
	return
}

// registerGeneric: phần đăng ký range của SnapshotAddKeys cho khóa tổng quát (chạy trong barrier). Plan
// mới đã được lưu (biên đọc sau barrier); plan chạy tiếp thì xóa phần chưa đọc ở target trước (như
// đường số nguyên, xem resume-clear), rồi đăng ký [min, last] "Xong" và (last, max] "Chưa đọc".
func (s *MySQLSnapshotter) registerGeneric(ctx context.Context, dstDB *sql.DB, table model.TableID, pkCol string,
	kq keySQL, plan *SnapshotPlan, resumed bool, cur *SnapshotPlan) error {
	*cur = *plan
	if plan.Empty {
		s.Manager.RegisterTableKeys(table, pkCol, kq.codec, nil, nil)
		return nil
	}
	minB, maxB, last, hasLast, err := planBounds(kq, plan)
	if err != nil {
		return fmt.Errorf("snapshot plan of %s: %w", table, err)
	}
	pendLo := minB
	if hasLast {
		var ok bool
		if pendLo, ok = after(kq.codec, last); !ok {
			plan.Finished = true
		}
	}
	unread := !plan.Finished && geLo(maxB.K, pendLo)
	var keys []Bound // khóa chờ đọc lại từ lần chạy trước / lúc bảng chờ đăng ký: nghĩa vụ vẫn còn
	for _, kv := range plan.RereadKeyVals {
		b, err := kq.bound(fromKeyVals(kv))
		if err != nil {
			return err
		}
		keys = append(keys, b)
	}
	if resumed {
		if unread || len(keys) > 0 {
			if err := s.clearUnreadKeys(ctx, dstDB, kq, pendLo, maxB, unread, keys); err != nil {
				return fmt.Errorf("clear unread part of resumed snapshot of %s: %w", table, err)
			}
		}
	}
	var chunks []*Chunk
	if hasLast {
		chunks = append(chunks, &Chunk{ID: 0, Table: table, PKCol: pkCol, Lo: minB, Hi: last, State: StateDone,
			BufferedChanges: map[Key]*model.RowChange{}})
	}
	if unread {
		chunks = append(chunks, &Chunk{ID: 1, Table: table, PKCol: pkCol, Lo: pendLo, Hi: maxB, State: StatePending,
			BufferedChanges: map[Key]*model.RowChange{}})
	}
	s.Manager.RegisterTableKeys(table, pkCol, kq.codec, chunks, &maxB)
	if len(keys) > 0 {
		s.Manager.AddRereadKeys(table, keys, time.Now())
	}
	return nil
}

// walkGeneric: keyset walk của khóa tổng quát — cắt chunk (lo, khóa thứ ChunkRows] ngay trước khi đọc.
func (s *MySQLSnapshotter) walkGeneric(ctx context.Context, srcDB *sql.DB, table model.TableID, kq keySQL,
	cur SnapshotPlan, processChunk func(*Chunk) error) error {
	if cur.Empty {
		return nil
	}
	minB, maxB, last, hasLast, err := planBounds(kq, &cur)
	if err != nil {
		return err
	}
	next := minB
	mainDone := cur.Finished
	if hasLast && !mainDone {
		var ok bool
		if next, ok = after(kq.codec, last); !ok {
			mainDone = true
		}
	}
	if !mainDone && !geLo(maxB.K, next) {
		mainDone = true
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if k := s.Manager.TakeReread(table, time.Now(), mainDone); k != nil {
			if err := processChunk(k); err != nil {
				return err
			}
			continue
		}
		if mainDone {
			return nil
		}
		end, err := s.keysetEndKeys(ctx, srcDB, kq, next, maxB, cur.ChunkRows)
		if err != nil {
			return fmt.Errorf("keyset walk %s: %w", table, err)
		}
		chunk, err := s.Manager.CarveChunkKeys(table, next, end)
		if err != nil {
			return err
		}
		chunk.EstimatedRows = cur.ChunkRows
		if end.K >= maxB.K {
			mainDone = true
		} else {
			next, _ = after(kq.codec, end)
		}
		if err := processChunk(chunk); err != nil {
			return err
		}
	}
}

// clearUnreadKeys: phần "Chưa đọc" của plan khóa tổng quát ở target MySQL — range [lo, hi] (khi
// hasRange) và các khóa keys — xóa với FOREIGN_KEY_CHECKS=0 (không cascade), trong transaction có fence.
func (s *MySQLSnapshotter) clearUnreadKeys(ctx context.Context, dstDB *sql.DB, kq keySQL, lo, hi Bound, hasRange bool, keys []Bound) error {
	if s.ClearUnreadRange != nil {
		return s.ClearUnreadRange(ctx, s.target(kq.table), lo, hi, hasRange, keys)
	}
	if dstDB == nil {
		return fmt.Errorf("no target to clear")
	}
	tq := keySQL{table: s.target(kq.table), codec: kq.codec}
	var stmts []string
	var args [][]any
	if hasRange {
		cond, a, err := tq.rangeCond(lo, hi)
		if err != nil {
			return err
		}
		stmts, args = append(stmts, "DELETE FROM "+quoteTable(tq.table)+" WHERE "+cond), append(args, a)
	}
	for _, k := range keys {
		cond, a, err := tq.cmp("=", k)
		if err != nil {
			return err
		}
		stmts, args = append(stmts, "DELETE FROM "+quoteTable(tq.table)+" WHERE "+cond), append(args, a)
	}
	return execNoFKChecks(ctx, dstDB, stmts, args)
}

// execNoFKChecks chạy các câu lệnh trên một kết nối riêng với FOREIGN_KEY_CHECKS=0, trong một transaction
// có kiểm fence; trả kết nối về pool với đúng giá trị cũ của phiên (bỏ kết nối nếu không khôi phục được).
func execNoFKChecks(ctx context.Context, dstDB *sql.DB, stmts []string, args [][]any) error {
	conn, err := dstDB.Conn(ctx)
	if err != nil {
		return err
	}
	var prev int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.foreign_key_checks").Scan(&prev); err != nil {
		conn.Close()
		return err
	}
	restored := false
	defer func() {
		if !restored {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS = 0"); err != nil {
		return err
	}
	err = func() error {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := checkpoint.CheckFence(ctx, tx, false); err != nil {
			return err
		}
		for i, q := range stmts {
			if _, err := tx.ExecContext(ctx, q, args[i]...); err != nil {
				return err
			}
		}
		return tx.Commit()
	}()
	if _, rerr := conn.ExecContext(context.Background(), fmt.Sprintf("SET SESSION FOREIGN_KEY_CHECKS = %d", prev)); rerr == nil {
		restored = true
	}
	return err
}

// keyBounds / keysetEndKeys: biên và keyset walk theo tuple, ở source MySQL (srcDB) hoặc qua ChunkSource
// (PG) khi nó hỗ trợ khóa tổng quát.
func (s *MySQLSnapshotter) keyBounds(ctx context.Context, srcDB *sql.DB, kq keySQL) (Bound, Bound, bool, error) {
	if s.Chunks != nil {
		kc, ok := s.Chunks.(KeyChunkSource)
		if !ok {
			return Bound{}, Bound{}, false, fmt.Errorf("chunk source of %s does not support this primary key", kq.table)
		}
		return kc.KeyBounds(ctx, kq.table, kq.codec)
	}
	return kq.mysqlKeyBounds(ctx, srcDB)
}

func (s *MySQLSnapshotter) keysetEndKeys(ctx context.Context, srcDB *sql.DB, kq keySQL, lo, max Bound, n int64) (Bound, error) {
	if s.Chunks != nil {
		kc, ok := s.Chunks.(KeyChunkSource)
		if !ok {
			return Bound{}, fmt.Errorf("chunk source of %s does not support this primary key", kq.table)
		}
		return kc.KeyKeysetEnd(ctx, kq.table, kq.codec, lo, max, n)
	}
	return kq.mysqlKeysetEnd(ctx, srcDB, lo, max, n)
}

// PendingKey (plan khóa tổng quát): khóa thuộc phần sẽ được snapshot đọc — chưa flush (sau LastKey,
// ≤ MaxKey, plan chưa Finished) hoặc đang chờ đọc lại.
func (p SnapshotPlan) PendingKey(codec *PKCodec, key []model.Value) (bool, error) {
	if p.Empty {
		return false, nil
	}
	kq := newKeySQL(model.TableID{}, codec)
	k, err := kq.codec.Encode(key)
	if err != nil {
		return false, err
	}
	for _, kv := range p.RereadKeyVals {
		if rk, err := kq.codec.Encode(fromKeyVals(kv)); err == nil && rk == k {
			return true, nil
		}
	}
	if p.Finished {
		return false, nil
	}
	minB, maxB, last, hasLast, err := planBounds(kq, &p)
	if err != nil {
		return false, err
	}
	lo := minB
	if hasLast {
		lo = Bound{K: last.K, Excl: true}
	}
	return geLo(k, lo) && k <= maxB.K, nil
}

// AddPlanRereadKey thêm khóa (tổng quát) vào RereadKeyVals của plan đã lưu (ok=false nếu bảng không có plan).
func (s *MySQLSnapshotter) AddPlanRereadKey(table model.TableID, codec *PKCodec, vals []model.Value) (bool, error) {
	b, err := codecOrInt(codec).BoundOf(vals)
	if err != nil {
		return false, err
	}
	found := false
	err = s.updatePlan(table, func(p *SnapshotPlan) {
		found = true
		for _, kv := range p.RereadKeyVals {
			if k, err := codecOrInt(codec).Encode(fromKeyVals(kv)); err == nil && k == b.K {
				return
			}
		}
		p.RereadKeyVals = append(p.RereadKeyVals, toKeyVals(b.V))
	})
	return found, err
}
