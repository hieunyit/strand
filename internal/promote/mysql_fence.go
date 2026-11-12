package promote

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/mysqlver"
	readermysql "strand/internal/reader/mysql"
)

// FenceMode là cách chặn ghi trên source MySQL, quyết định theo phiên bản.
type FenceMode int

const (
	// FenceSuperReadOnly: SET GLOBAL super_read_only = ON (5.7.8+ và 8.x): chặn cả user có SUPER.
	FenceSuperReadOnly FenceMode = iota
	// FenceFTWRL: 5.5, 5.6, 5.7 < 5.7.8 không có super_read_only. read_only=ON không chặn user SUPER nên
	// phải giữ thêm FLUSH TABLES WITH READ LOCK trên một kết nối riêng suốt promote.
	FenceFTWRL
)

func (m FenceMode) String() string {
	if m == FenceFTWRL {
		return "read_only + FLUSH TABLES WITH READ LOCK"
	}
	return "super_read_only"
}

// ChooseFenceMode: hàm thuần quyết định cách fence theo phiên bản source.
func ChooseFenceMode(v mysqlver.Version) FenceMode {
	if v.HasSuperReadOnly() {
		return FenceSuperReadOnly
	}
	return FenceFTWRL
}

// DefaultFenceTimeout: thời gian tối đa chờ FTWRL (chờ câu lệnh đang chạy xong) và chờ session ứng dụng thoát.
const DefaultFenceTimeout = 60 * time.Second

// SessionInfo là một dòng PROCESSLIST của source.
type SessionInfo struct {
	ID   int64
	User string
	Host string
}

// appSessions lọc các session thuộc user ứng dụng cần kill (so khớp không phân biệt hoa thường? Không:
// tên user MySQL phân biệt hoa thường, nên so khớp chính xác).
func appSessions(all []SessionInfo, appUsers []string) []SessionInfo {
	want := map[string]bool{}
	for _, u := range appUsers {
		want[u] = true
	}
	var out []SessionInfo
	for _, s := range all {
		if want[s.User] {
			out = append(out, s)
		}
	}
	return out
}

// superSessions trả các session (trừ user của chính Strand và user hệ thống) thuộc user có SUPER.
func superSessions(all []SessionInfo, supers map[string]bool, self string) []SessionInfo {
	var out []SessionInfo
	for _, s := range all {
		if s.User == self || s.User == "system user" || s.User == "event_scheduler" {
			continue
		}
		if supers[s.User] {
			out = append(out, s)
		}
	}
	return out
}

// PosCaughtUp: hàm thuần cho bước 3 ở chế độ vị trí binlog. lwdKey là mốc bền vững của pipeline
// (MySQLPosAppliedSet.LwdKey: mọi Txn kết thúc ≤ lwdKey đã apply), headKey là vị trí đầu binlog nguồn
// ĐỌC SAU KHI FENCE (SHOW MASTER STATUS), không đổi nữa vì không còn ai ghi được.
//
// Không so với kích thước file: sau Txn cuối cùng binlog còn có thể có event không thuộc Txn (Rotate sau
// FLUSH LOGS, Format_desc của file mới, Stop...), nên head có thể lớn hơn mọi end_log_pos của Txn và
// lwdKey sẽ không bao giờ tới head. Vì vậy: đã kịp nếu lwdKey ≥ headKey, hoặc txnFree() báo từ lwdKey tới
// head không còn event của Txn nào (nghĩa là mọi Txn đã có đều kết thúc ≤ lwdKey). lwdKey = 0 (tập rỗng,
// chưa có mốc) không bao giờ coi là kịp.
func PosCaughtUp(lwdKey, headKey uint64, txnFree func() (bool, error)) (bool, error) {
	if lwdKey == 0 {
		return false, nil
	}
	if lwdKey >= headKey {
		return true, nil
	}
	return txnFree()
}

// WithFenceTimeout đặt thời gian chờ FTWRL / chờ session ứng dụng thoát (chỉ nhánh FenceFTWRL).
func (d *MySQLSourceDriver) WithFenceTimeout(t time.Duration) *MySQLSourceDriver {
	d.fenceTimeout = t
	return d
}

// WithAppUsers khai báo user ứng dụng trên source: fence kill session của các user này sau FTWRL.
func (d *MySQLSourceDriver) WithAppUsers(users ...string) *MySQLSourceDriver {
	d.appUsers = users
	return d
}

// WithLogger đặt logger.
func (d *MySQLSourceDriver) WithLogger(l *slog.Logger) *MySQLSourceDriver {
	d.logger = l
	return d
}

func (d *MySQLSourceDriver) log() *slog.Logger {
	if d.logger != nil {
		return d.logger
	}
	return slog.Default()
}

// ftwrlHold là kết nối riêng đang giữ FTWRL.
type ftwrlHold struct {
	conn *sql.Conn
	stop chan struct{}
	done chan struct{}
	mu   sync.Mutex
	err  error // lỗi keepalive đầu tiên: kết nối mất nghĩa là FTWRL đã nhả
}

// fenceFTWRL: read_only=ON → FTWRL trên kết nối riêng → kill session app → chờ chúng thoát.
//
// Giới hạn (cũng in ra cho người vận hành):
//   - read_only một mình KHÔNG chặn user SUPER; FTWRL chặn mọi ghi (kể cả SUPER) nhưng chỉ tồn tại
//     chừng nào kết nối này còn sống. Kết nối bị kill (user SUPER có thể KILL) hoặc đứt thì khóa nhả:
//     keepalive phát hiện và FenceHealthy báo lỗi, promote dừng trước khi mở target.
//   - FTWRL chờ câu lệnh/transaction đang chạy; quá hạn (lock_wait_timeout / fenceTimeout) thì thất bại
//     và read_only được trả về giá trị cũ.
//   - Khi tiến trình promote thoát, FTWRL nhả; read_only=ON vẫn giữ. Người vận hành phải giữ source tắt
//     hoặc read_only (và không cấp SUPER cho app) cho đến khi cut-over xong.
func (d *MySQLSourceDriver) fenceFTWRL(ctx context.Context) (retErr error) {
	if d.hold != nil {
		return nil // đã giữ FTWRL trong tiến trình này
	}
	timeout := d.fenceTimeout
	if timeout <= 0 {
		timeout = DefaultFenceTimeout
	}
	var prevRO int
	if err := d.db.QueryRowContext(ctx, "SELECT @@GLOBAL.read_only").Scan(&prevRO); err != nil {
		return fmt.Errorf("read source read_only: %w", err)
	}
	if _, err := d.db.ExecContext(ctx, "SET GLOBAL read_only = ON"); err != nil {
		return fmt.Errorf("set source read_only: %w", err)
	}
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open dedicated fence connection: %w", err)
	}
	locked := false
	defer func() {
		if retErr == nil {
			return
		}
		if locked {
			_, _ = conn.ExecContext(context.Background(), "UNLOCK TABLES")
		}
		_ = conn.Close()
		if prevRO == 0 {
			_, _ = d.db.ExecContext(context.Background(), "SET GLOBAL read_only = OFF")
		}
	}()
	secs := int(timeout / time.Second)
	if secs < 1 {
		secs = 1
	}
	// wait_timeout lớn: kết nối giữ khóa không được bị server đóng vì rảnh.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION wait_timeout = 31536000, lock_wait_timeout = %d", secs)); err != nil {
		return fmt.Errorf("configure fence connection: %w", err)
	}
	fctx, cancel := context.WithTimeout(ctx, timeout+5*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(fctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		return fmt.Errorf("FLUSH TABLES WITH READ LOCK failed within %s (it waits for running statements and open transactions to finish; "+
			"a long-running query on the source blocks it; read_only was restored): %w", timeout, err)
	}
	locked = true

	if err := d.warnSuperSessions(ctx, conn); err != nil {
		d.log().Warn("cannot enumerate SUPER sessions on the source", "error", err)
	}
	if len(d.appUsers) > 0 {
		if err := d.killAppSessions(ctx, conn, timeout); err != nil {
			return err
		}
	} else {
		d.log().Warn("no --source-app-roles: application sessions are not killed; they stay connected but their writes block on the global read lock")
	}

	h := &ftwrlHold{conn: conn, stop: make(chan struct{}), done: make(chan struct{})}
	d.hold = h
	go h.keepalive()
	d.log().Warn("source fenced with FLUSH TABLES WITH READ LOCK held by this process: the lock is released when promote exits; "+
		"keep the old source down or read_only (and away from SUPER users) until cut-over is complete",
		"mode", FenceFTWRL.String())
	return nil
}

// keepalive: một lệnh nhẹ mỗi 5 giây; lỗi nghĩa là kết nối (và FTWRL) đã mất.
func (h *ftwrlHold) keepalive() {
	defer close(h.done)
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			var one int
			err := h.conn.QueryRowContext(cctx, "SELECT 1").Scan(&one)
			cancel()
			if err != nil {
				h.mu.Lock()
				if h.err == nil {
					h.err = err
				}
				h.mu.Unlock()
				return
			}
		}
	}
}

// FenceHealthy báo lỗi nếu FTWRL đã nhả (kết nối giữ khóa mất). Nhánh super_read_only luôn nil.
func (d *MySQLSourceDriver) FenceHealthy(ctx context.Context) error {
	h := d.hold
	if h == nil {
		return nil
	}
	h.mu.Lock()
	err := h.err
	h.mu.Unlock()
	if err != nil {
		return fmt.Errorf("source fence lost: the connection holding FLUSH TABLES WITH READ LOCK dropped (%v); writes to the source may have resumed", err)
	}
	return nil
}

// ReleaseFence nhả FTWRL và đóng kết nối giữ khóa (read_only vẫn ON). Gọi khi tiến trình thoát hoặc trong test.
func (d *MySQLSourceDriver) ReleaseFence() {
	h := d.hold
	if h == nil {
		return
	}
	d.hold = nil
	close(h.stop)
	<-h.done
	_, _ = h.conn.ExecContext(context.Background(), "UNLOCK TABLES")
	_ = h.conn.Close()
}

func listSessions(ctx context.Context, conn *sql.Conn) ([]SessionInfo, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id, IFNULL(user,''), IFNULL(host,'') FROM information_schema.PROCESSLIST WHERE id <> CONNECTION_ID()")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionInfo
	for rows.Next() {
		var s SessionInfo
		if err := rows.Scan(&s.ID, &s.User, &s.Host); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// warnSuperSessions liệt kê user có SUPER đang kết nối: họ không bị read_only chặn và có thể KILL
// kết nối giữ FTWRL; FTWRL vẫn chặn ghi của họ chừng nào kết nối còn sống.
func (d *MySQLSourceDriver) warnSuperSessions(ctx context.Context, conn *sql.Conn) error {
	sessions, err := listSessions(ctx, conn)
	if err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, "SELECT DISTINCT User FROM mysql.user WHERE Super_priv = 'Y'")
	if err != nil {
		return err
	}
	supers := map[string]bool{}
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			rows.Close()
			return err
		}
		supers[u] = true
	}
	rows.Close()
	var self string
	_ = conn.QueryRowContext(ctx, "SELECT SUBSTRING_INDEX(USER(), '@', 1)").Scan(&self)
	sup := superSessions(sessions, supers, self)
	if len(sup) == 0 {
		return nil
	}
	var names []string
	for _, s := range sup {
		names = append(names, fmt.Sprintf("%s@%s(id=%d)", s.User, s.Host, s.ID))
	}
	sort.Strings(names)
	d.log().Warn("SUPER users are connected to the source: read_only does not block them, only the held global read lock does, "+
		"and a SUPER user can KILL the connection holding it. Disconnect them or revoke SUPER before cut-over",
		"sessions", strings.Join(names, ", "))
	return nil
}

// killAppSessions kill session của user ứng dụng rồi chờ chúng thoát (kill lại session mới kết nối).
func (d *MySQLSourceDriver) killAppSessions(ctx context.Context, conn *sql.Conn, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		sessions, err := listSessions(ctx, conn)
		if err != nil {
			return fmt.Errorf("list source sessions: %w", err)
		}
		victims := appSessions(sessions, d.appUsers)
		if len(victims) == 0 {
			return nil
		}
		for _, s := range victims {
			// Session có thể đã thoát giữa chừng (lỗi 1094 unknown thread): bỏ qua.
			_, _ = conn.ExecContext(ctx, fmt.Sprintf("KILL CONNECTION %d", s.ID))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("application sessions of %v still connected to the source after %s (%d remaining)", d.appUsers, timeout, len(victims))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// posHead là vị trí đầu binlog nguồn đọc sau fence, dạng khóa.
type posHead struct {
	file string
	key  uint64
}

// readPosHead đọc SHOW MASTER STATUS / SHOW BINARY LOG STATUS (readermysql.ReadBinlogStatus) và kiểm tên
// gốc file khớp với tập đã apply.
func readPosHead(ctx context.Context, source *sql.DB, ps *checkpoint.MySQLPosAppliedSet) (posHead, error) {
	st, err := readermysql.ReadBinlogStatus(ctx, source)
	if err != nil {
		return posHead{}, err
	}
	base, seq, err := checkpoint.SplitBinlogName(st.File)
	if err != nil {
		return posHead{}, err
	}
	if ps.FileBase() != "" && base != ps.FileBase() {
		return posHead{}, fmt.Errorf("source binlog base name is %q but the pipeline checkpoint was made for %q", base, ps.FileBase())
	}
	key, err := checkpoint.BinlogKey(seq, st.Position)
	if err != nil {
		return posHead{}, err
	}
	return posHead{file: st.File, key: key}, nil
}

// posMissing: "" nếu lwd bền vững đã bao phủ đầu binlog (xem PosCaughtUp), ngược lại mô tả phần còn thiếu.
func posMissing(ctx context.Context, source *sql.DB, ps *checkpoint.MySQLPosAppliedSet, head posHead) (string, error) {
	lwd := ps.LwdKey()
	ok, err := PosCaughtUp(lwd, head.key, func() (bool, error) {
		return readermysql.TxnFreeToHead(ctx, source, ps.FileBase(), lwd)
	})
	if err != nil {
		return "", fmt.Errorf("apply remaining: compare checkpoint with source binlog: %w", err)
	}
	if ok {
		return "", nil
	}
	return fmt.Sprintf("binlog up to %s:%d (pipeline durable checkpoint is at %s; is the pipeline running?)",
		head.file, head.key&0xFFFFFFFF, checkpoint.FormatBinlogKey(lwd)), nil
}

// applyRemainingPos: bước 3 cho MySQL target ở chế độ vị trí binlog. Vị trí đầu binlog được đọc MỘT LẦN
// sau fence: không còn ghi nào (super_read_only, hoặc FTWRL giữ), nên đó là vị trí cuối cùng.
func (d *MySQLTargetDriver) applyRemainingPos(ctx context.Context, first *checkpoint.MySQLPosAppliedSet) error {
	head, err := readPosHead(ctx, d.source, first)
	if err != nil {
		return fmt.Errorf("apply remaining: %w", err)
	}
	deadline := time.Now().Add(d.catchUpTimeout)
	var ps = first
	for {
		missing, err := posMissing(ctx, d.source, ps, head)
		if err != nil {
			return err
		}
		if missing == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("apply remaining: target still missing %s after %s", missing, d.catchUpTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.catchUpInterval):
		}
		applied, err := checkpoint.LoadAppliedSnapshotMySQL(ctx, d.db, d.pipelineID, model.EngineMySQL)
		if err != nil {
			return fmt.Errorf("apply remaining: load target applied set: %w", err)
		}
		next, ok := applied.(*checkpoint.MySQLPosAppliedSet)
		if !ok {
			return fmt.Errorf("apply remaining: checkpoint is no longer a binlog position set")
		}
		ps = next
	}
}

// FenceHeld: nhánh super_read_only luôn nil (cờ toàn cục, không phụ thuộc kết nối). Nhánh FTWRL: kết nối
// giữ khóa phải đang sống trong tiến trình này.
func (d *MySQLSourceDriver) FenceHeld(ctx context.Context) error {
	if d.db == nil {
		return nil
	}
	ver, err := mysqlver.Cached(ctx, d.db)
	if err != nil || ChooseFenceMode(ver) != FenceFTWRL {
		return nil
	}
	if d.hold == nil {
		return fmt.Errorf("FLUSH TABLES WITH READ LOCK is not held by this process")
	}
	return d.FenceHealthy(ctx)
}
