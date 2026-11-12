package pipeline

// Snapshot logic ở chế độ vị trí binlog (source MySQL không GTID): watermark theo (file, end_log_pos).
// Lập luận RS-1'/RS-2' nằm ở snapshot.PosWatermarks; ở đây là phần SQL, điểm hẹn và khởi tạo.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	mysqlreader "strand/internal/reader/mysql"
)

// posLockWaitSeconds: thời gian tối đa chờ MDL READ của table (Txn đang ghi table phải xong). Hết giờ →
// lỗi 1205 → đường retry của snapshot (backoff, tối đa waitGTIDMaxAttempts lần ở tầng đọc chunk).
const posLockWaitSeconds = 10

// posWatermarks cài snapshot.PosWatermarks trên kết nối tới source.
type posWatermarks struct {
	db   *sql.DB
	base string // tên gốc file binlog của pipeline
}

func (w *posWatermarks) key(st mysqlreader.BinlogStatus) (uint64, error) {
	return checkpoint.KeyOfPos(w.base, model.Pos{Engine: model.EngineMySQL, File: st.File, BinlogPos: st.Position})
}

func (w *posWatermarks) set(st mysqlreader.BinlogStatus) (checkpoint.AppliedSet, error) {
	k, err := w.key(st)
	if err != nil {
		return nil, err
	}
	return checkpoint.NewMySQLPosAppliedSet(w.base, k), nil
}

// Low: LOCK TABLES t READ → SHOW MASTER STATUS → UNLOCK, tất cả trên MỘT kết nối (RS-2').
func (w *posWatermarks) Low(ctx context.Context, table model.TableID) (set checkpoint.AppliedSet, err error) {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	locked := false
	defer func() {
		if locked {
			uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, uerr := conn.ExecContext(uctx, "UNLOCK TABLES"); uerr != nil {
				// Không chắc khóa đã nhả: bỏ kết nối (đóng kết nối thật nhả mọi khóa).
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
		_ = conn.Close()
	}()
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", posLockWaitSeconds)); err != nil {
		return nil, err
	}
	locked = true // đặt trước LOCK: nếu LOCK dở dang thì UNLOCK vô hại
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("LOCK TABLES `%s`.`%s` READ",
		strings.ReplaceAll(table.Schema, "`", "``"), strings.ReplaceAll(table.Table, "`", "``"))); err != nil {
		var me *drivermysql.MySQLError
		if errors.As(err, &me) && (me.Number == 1044 || me.Number == 1142 || me.Number == 1227) {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, fmt.Sprintf(
				"logical snapshot in binlog position mode needs the LOCK TABLES privilege on %s (without GTID, the chunk's low position can only be proven consistent under a metadata lock): %v",
				table, err), err)
		}
		return nil, err
	}
	st, err := mysqlreader.ReadBinlogStatus(ctx, conn)
	if err != nil {
		return nil, err
	}
	return w.set(st)
}

func (w *posWatermarks) High(ctx context.Context) (checkpoint.AppliedSet, error) {
	st, err := mysqlreader.ReadBinlogStatus(ctx, w.db)
	if err != nil {
		return nil, err
	}
	return w.set(st)
}

// posRendezvousDone (thuần): router đã xử lý mọi Txn kết thúc ≤ high. seen = khóa kết thúc lớn nhất
// router đã xử lý; txnFree = không còn event Txn nào trong binlog từ seen tới đầu hiện tại (nguồn rảnh,
// phần đuôi chỉ là Rotate/Format_desc... nên seen không bao giờ chạm high).
func posRendezvousDone(seen, high uint64, txnFree func() bool) bool {
	if seen >= high {
		return true
	}
	return txnFree()
}

// rendezvousPos chờ router xử lý xong mọi Txn có vị trí kết thúc ≤ high. Router xử lý tuần tự theo thứ tự
// binlog nên "đã thấy key ≥ high" ⇒ mọi Txn ≤ high đã qua range-state. Nguồn rảnh: head có thể vượt Txn
// cuối (Rotate...) → dùng TxnFreeToHead từ seen: không còn Txn nào trong (seen, head hiện tại] mà
// head hiện tại ≥ high, nên mọi Txn ≤ high đã ≤ seen. Poll định kỳ vì không có event nào đánh thức khi rảnh.
func (p *Pipeline) rendezvousPos(ctx context.Context, high checkpoint.AppliedSet) error {
	hs, ok := high.(*checkpoint.MySQLPosAppliedSet)
	if !ok {
		return fmt.Errorf("unexpected high watermark type %T", high)
	}
	hk := hs.Base()
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ctx.Done():
				p.seenMu.Lock()
				p.seenCond.Broadcast()
				p.seenMu.Unlock()
				return
			case <-t.C:
				p.seenMu.Lock()
				p.seenCond.Broadcast()
				p.seenMu.Unlock()
			}
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.failed(); err != nil {
			return err
		}
		seen := p.lsns.seenLSN()
		from := seen
		if from == 0 {
			from = p.applied0.(*checkpoint.MySQLPosAppliedSet).LwdKey()
		}
		if posRendezvousDone(seen, hk, func() bool {
			qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			free, err := mysqlreader.TxnFreeToHead(qctx, p.src, p.binlogBase, from)
			return err == nil && free
		}) {
			return nil
		}
		p.seenMu.Lock()
		if p.lsns.seenLSN() == seen { // không có Txn mới từ lúc đọc: ngủ tới tick/broadcast kế
			p.seenCond.Wait()
		}
		p.seenMu.Unlock()
	}
}

// initialPositionPosHead: pipeline mới không --start-at (bootstrap logical): stream bắt đầu ở đầu binlog
// hiện tại, mọi bảng được snapshot sau đó (như chế độ A của GTID: event từ vị trí này được stream, snapshot
// đọc dữ liệu từ lúc sau; RS-1'/RS-2' lo phần chồng lấn).
func (p *Pipeline) initialPositionPosHead(ctx context.Context) (*checkpoint.MySQLPosAppliedSet, error) {
	mode := strings.ToLower(p.cfg.Bootstrap.Default)
	switch mode {
	case "", "logical":
	case "seed", "start_at":
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
			fmt.Sprintf("bootstrap.default=%s requires --start-at for a new pipeline", mode), nil)
	default:
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, "unknown bootstrap.default "+mode, nil)
	}
	st, err := mysqlreader.ReadBinlogStatus(ctx, p.src)
	if err != nil {
		return nil, err
	}
	base, seq, err := checkpoint.SplitBinlogName(st.File)
	if err != nil {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, err.Error(), err)
	}
	key, err := checkpoint.BinlogKey(seq, st.Position)
	if err != nil {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, err.Error(), err)
	}
	set := checkpoint.NewMySQLPosAppliedSet(base, key)
	set.SetSource(p.srcIdent)
	for id := range p.tables {
		if err := p.state.SetDone(id.String(), false); err != nil {
			return nil, err
		}
	}
	p.binlogBase = base
	p.log.Info("logical bootstrap in binlog position mode: streaming from the current head", "position", fmt.Sprintf("%s:%d", st.File, st.Position))
	return set, p.writeGlobal(ctx, set.String())
}
