package promote

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"strand/internal/checkpoint"
	"strand/internal/model"
	pgreader "strand/internal/reader/pg"
)

// quoteIdent bao bọc định danh PostgreSQL trong dấu ngoặc kép.
func quoteIdent(s string) string {
	s = strings.Trim(strings.TrimSpace(s), `"`)
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteQualified bao bọc tên schema-qualified (ví dụ "public.orders") trong dấu ngoặc kép từng phần.
func quoteQualified(name string) string {
	name = strings.TrimSpace(name)
	if strings.Contains(name, ".") {
		parts := strings.Split(name, ".")
		quoted := make([]string, len(parts))
		for i, p := range parts {
			quoted[i] = quoteIdent(p)
		}
		return strings.Join(quoted, ".")
	}
	return quoteIdent(name)
}

func cleanList(in []string) []string {
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// =============================================================================
// PGSourceDriver
// =============================================================================

// PGSourceDriver điều khiển source PostgreSQL trong promote theo spec/02 §8.
type PGSourceDriver struct {
	db        *sql.DB
	appRoles  []string
	reachable bool
	// exitWait: thời gian tối đa Fence chờ các session vừa bị ngắt thoát hẳn (mặc định 30s).
	exitWait time.Duration
}

// NewPGSourceDriver khởi tạo PGSourceDriver với DB kết nối và danh sách role ứng dụng cần fence.
func NewPGSourceDriver(db *sql.DB, appRoles ...string) *PGSourceDriver {
	return &PGSourceDriver{db: db, appRoles: cleanList(appRoles), reachable: true, exitWait: 30 * time.Second}
}

// SetReachable cho phép giả lập ngắt kết nối source khi test failover khẩn cấp.
func (d *PGSourceDriver) SetReachable(r bool) {
	d.reachable = r
}

// Fence chặn ghi trên PostgreSQL source (spec/02 §8 bước 1):
//  1. ALTER SYSTEM SET default_transaction_read_only = on, rồi pg_reload_conf() để mọi session
//     (kể cả session đang mở) nhận giá trị mới ở transaction kế tiếp.
//  2. Ngắt các session client đang mở để transaction read-write đang dở không commit thêm được:
//     theo role ứng dụng nếu có cấu hình, nếu không thì mọi client backend trừ user của Strand.
//     Walsender (backend_type khác 'client backend') không bị ngắt để pipeline apply nốt log ở bước 3.
//  3. Chờ các session vừa bị ngắt thoát hẳn: pg_terminate_backend chỉ gửi SIGTERM rồi trả về ngay, một
//     backend đang dở COMMIT vẫn có thể ghi commit record SAU khi Fence trả về. Bước 3 (ApplyRemaining)
//     dựa vào việc không còn commit nào của ứng dụng sau marker, nên Fence chỉ thành công khi chúng đã thoát.
func (d *PGSourceDriver) Fence(ctx context.Context) error {
	if !d.reachable || d.db == nil {
		return fmt.Errorf("source db unreachable")
	}
	if _, err := d.db.ExecContext(ctx, "ALTER SYSTEM SET default_transaction_read_only = on"); err != nil {
		return fmt.Errorf("fence source: alter system set default_transaction_read_only: %w", err)
	}
	if _, err := d.db.ExecContext(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("fence source: pg_reload_conf: %w", err)
	}
	var killed []int64
	terminate := func(q string, args ...any) error {
		rows, err := d.db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var pid int64
			if err := rows.Scan(&pid); err != nil {
				return err
			}
			killed = append(killed, pid)
		}
		return rows.Err()
	}
	if len(d.appRoles) > 0 {
		// Hàng rào cứng cho role ứng dụng đã khai báo (review R06): default_transaction_read_only chỉ là
		// mặc định — role thường tự đặt lại được (SET default_transaction_read_only = off). NOLOGIN trước
		// khi ngắt session: role không kết nối lại được, nên không còn session nào để ghi. Gỡ fence là
		// việc của người vận hành (ALTER ROLE … LOGIN), như với super_read_only của MySQL.
		for _, role := range d.appRoles {
			if _, err := d.db.ExecContext(ctx, "ALTER ROLE "+quoteIdent(role)+" NOLOGIN"); err != nil {
				return fmt.Errorf("fence source: ALTER ROLE %s NOLOGIN: %w", role, err)
			}
			q := `SELECT pid FROM pg_stat_activity
				WHERE usename = $1 AND pid <> pg_backend_pid() AND backend_type = 'client backend'
				  AND pg_terminate_backend(pid)`
			if err := terminate(q, role); err != nil {
				return fmt.Errorf("fence source: terminate sessions for role %s: %w", role, err)
			}
		}
	} else {
		q := `SELECT pid FROM pg_stat_activity
			WHERE pid <> pg_backend_pid() AND backend_type = 'client backend' AND usename <> current_user
			  AND pg_terminate_backend(pid)`
		if err := terminate(q); err != nil {
			return fmt.Errorf("fence source: terminate client sessions: %w", err)
		}
	}
	return d.waitExited(ctx, killed)
}

// waitExited chờ đến khi mọi backend trong pids đã thoát (không còn trong pg_stat_activity).
func (d *PGSourceDriver) waitExited(ctx context.Context, pids []int64) error {
	if len(pids) == 0 {
		return nil
	}
	wait := d.exitWait
	if wait <= 0 {
		wait = 30 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		var alive int
		if err := d.db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity WHERE pid = ANY($1::int8[])", pids).Scan(&alive); err != nil {
			return fmt.Errorf("fence source: check terminated sessions: %w", err)
		}
		if alive == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("fence source: %d terminated session(s) still running after %s; they may still commit — retry promote", alive, wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// IsReachable kiểm tra kết nối tới source PostgreSQL.
func (d *PGSourceDriver) IsReachable(ctx context.Context) bool {
	if !d.reachable || d.db == nil {
		return false
	}
	return d.db.PingContext(ctx) == nil
}

// ReadSequenceLastValue đọc last_value của sequence trên source (primary hoặc physical standby).
// Trả về last_value thô, kể cả khi is_called = false: giá trị đó cao hơn hoặc bằng mọi giá trị
// đã cấp nên an toàn (thận trọng) cho setval. Trên primary last_value đã bao gồm cả phần cache
// mà các session đã giữ; trên standby nó đi trước giá trị đã cấp vì PG ghi WAL trước (02 §8).
func (d *PGSourceDriver) ReadSequenceLastValue(ctx context.Context, seqName string) (int64, error) {
	if !d.reachable || d.db == nil {
		return 0, fmt.Errorf("source db unreachable")
	}
	var lastVal int64
	q := fmt.Sprintf("SELECT last_value FROM %s", quoteQualified(seqName))
	if err := d.db.QueryRowContext(ctx, q).Scan(&lastVal); err != nil {
		return 0, fmt.Errorf("read sequence last_value for %s: %w", seqName, err)
	}
	return lastVal, nil
}

// =============================================================================
// PGTargetDriver
// =============================================================================

// pgTableWritePrivs và pgSeqWritePrivs là các quyền cho phép ứng dụng ghi: thu hồi ở bước 2,
// cấp lại ở bước 5. Với sequence, USAGE/UPDATE cho phép nextval (và UPDATE cho phép setval).
var (
	pgTableWritePrivs = []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}
	pgSeqWritePrivs   = []string{"USAGE", "UPDATE"}
)

// PGTargetDriver điều khiển target PostgreSQL trong promote theo spec/02 §8.
type PGTargetDriver struct {
	db       *sql.DB
	appRoles []string
	schemas  []string

	// Kiểm tra bước 3 (apply nốt log): source đã fence, pipeline trên target.
	source          *sql.DB
	sourceEngine    model.Engine
	pipelineID      string
	catchUpTimeout  time.Duration
	catchUpInterval time.Duration
	logger          *slog.Logger
}

// NewPGTargetDriver khởi tạo PGTargetDriver. appRoles là role của ứng dụng bị chặn ghi đến bước 5.
func NewPGTargetDriver(db *sql.DB, appRoles ...string) *PGTargetDriver {
	return &PGTargetDriver{
		db:           db,
		appRoles:     cleanList(appRoles),
		schemas:      []string{"public"},
		sourceEngine: model.EnginePG,
		logger:       slog.Default(),
	}
}

// WithSchemas gán danh sách schema chứa bảng và sequence của ứng dụng.
func (d *PGTargetDriver) WithSchemas(schemas ...string) *PGTargetDriver {
	if list := cleanList(schemas); len(list) > 0 {
		d.schemas = list
	}
	return d
}

// WithPipeline gán pipelineID để đọc được checkpoint của pipeline ngay cả khi không cấu hình source.
func (d *PGTargetDriver) WithPipeline(id string) *PGTargetDriver {
	d.pipelineID = id
	return d
}

// WithSourceEngine khai báo engine của source (mặc định PG; MySQL cho pipeline MySQL→PG),
// quyết định cách bước 3 so vị trí log của source với tập đã apply ở target.
func (d *PGTargetDriver) WithSourceEngine(e model.Engine) *PGTargetDriver {
	if e != "" {
		d.sourceEngine = e
	}
	return d
}

// WithCatchUpCheck cấu hình cách bước 3 xác minh target đã apply hết log của source
// (cùng hợp đồng với MySQLTargetDriver.WithCatchUpCheck).
func (d *PGTargetDriver) WithCatchUpCheck(source *sql.DB, pipelineID string, timeout time.Duration, logger *slog.Logger) *PGTargetDriver {
	d.source = source
	d.pipelineID = pipelineID
	d.catchUpTimeout = timeout
	d.catchUpInterval = 500 * time.Millisecond
	if logger == nil {
		logger = slog.Default()
	}
	d.logger = logger
	return d
}

// ensureGrantsTable tạo bảng lưu các quyền ghi của role ứng dụng đã bị thu hồi ở bước 2,
// để bước 5 (có thể ở lần chạy promote khác) cấp lại đúng như cũ.
func (d *PGTargetDriver) ensureGrantsTable(ctx context.Context) error {
	_, err := d.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _strand_promote_grants (
		pipeline_id text NOT NULL,
		role_name   text NOT NULL,
		schema_name text NOT NULL,
		rel_name    text NOT NULL,
		rel_kind    text NOT NULL,
		privilege   text NOT NULL,
		PRIMARY KEY (pipeline_id, role_name, schema_name, rel_name, privilege)
	)`)
	return err
}

// HoldReadOnly (bước 2) chặn ghi của ứng dụng nhưng để Strand vẫn apply được ở bước 3 và setval
// ở bước 4 (spec/02 §8, spec/03 §1): với mỗi role ứng dụng
//   - ALTER ROLE ... SET default_transaction_read_only = on (session mới mặc định read-only);
//   - ghi lại rồi REVOKE các quyền ghi trên bảng (INSERT/UPDATE/DELETE/TRUNCATE) và sequence
//     (USAGE/UPDATE) trong các schema; REVOKE có hiệu lực ngay cả với session đang mở và chặn
//     cả trường hợp ứng dụng tự SET default_transaction_read_only = off;
//   - kiểm lại bằng has_*_privilege: còn quyền ghi (qua PUBLIC hoặc role cha) thì trả lỗi.
//
// Role của Strand không bị đụng tới nên vẫn ghi được. Idempotent: chạy lại không làm mất bản ghi quyền.
func (d *PGTargetDriver) HoldReadOnly(ctx context.Context) error {
	if d.db == nil {
		return fmt.Errorf("target db not configured")
	}
	if len(d.appRoles) == 0 {
		return fmt.Errorf("hold read-only: no application role configured (--app-role)")
	}
	if err := d.ensureGrantsTable(ctx); err != nil {
		return fmt.Errorf("hold read-only: create _strand_promote_grants: %w", err)
	}
	for _, role := range d.appRoles {
		if _, err := d.db.ExecContext(ctx, fmt.Sprintf("ALTER ROLE %s SET default_transaction_read_only = on", quoteIdent(role))); err != nil {
			return fmt.Errorf("hold read-only: alter role %s: %w", role, err)
		}
		// Ghi lại quyền ghi đang được cấp trực tiếp cho role trước khi thu hồi.
		snap := `INSERT INTO _strand_promote_grants (pipeline_id, role_name, schema_name, rel_name, rel_kind, privilege)
			SELECT $1::text, $2::text, n.nspname, c.relname, c.relkind::text, a.privilege_type
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			CROSS JOIN LATERAL aclexplode(c.relacl) a
			WHERE n.nspname = ANY($3::text[])
			  AND c.relkind IN ('r', 'p', 'S')
			  AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $2::text)
			  AND ((c.relkind IN ('r', 'p') AND a.privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'))
			    OR (c.relkind = 'S' AND a.privilege_type IN ('USAGE', 'UPDATE')))
			ON CONFLICT DO NOTHING`
		if _, err := d.db.ExecContext(ctx, snap, d.pipelineID, role, d.schemas); err != nil {
			return fmt.Errorf("hold read-only: record grants of %s: %w", role, err)
		}
		for _, schema := range d.schemas {
			qs := quoteIdent(schema)
			qr := quoteIdent(role)
			for _, q := range []string{
				fmt.Sprintf("REVOKE %s ON ALL TABLES IN SCHEMA %s FROM %s", strings.Join(pgTableWritePrivs, ", "), qs, qr),
				fmt.Sprintf("REVOKE %s ON ALL SEQUENCES IN SCHEMA %s FROM %s", strings.Join(pgSeqWritePrivs, ", "), qs, qr),
			} {
				if _, err := d.db.ExecContext(ctx, q); err != nil {
					return fmt.Errorf("hold read-only: revoke on schema %s from %s: %w", schema, role, err)
				}
			}
		}
		if obj, err := d.firstWritableObject(ctx, role); err != nil {
			return fmt.Errorf("hold read-only: check privileges of %s: %w", role, err)
		} else if obj != "" {
			return fmt.Errorf("hold read-only: role %s can still write %s (privilege via PUBLIC, an inherited role or ownership); revoke it before promote", role, obj)
		}
	}
	return nil
}

// firstWritableObject trả về tên một bảng/sequence trong các schema mà role vẫn ghi được
// (tính cả quyền thừa kế và PUBLIC), "" nếu không còn.
func (d *PGTargetDriver) firstWritableObject(ctx context.Context, role string) (string, error) {
	q := `SELECT n.nspname || '.' || c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = ANY($2::text[])
		  AND ((c.relkind IN ('r', 'p') AND has_table_privilege($1::name, c.oid, 'INSERT, UPDATE, DELETE, TRUNCATE'))
		    OR (c.relkind = 'S' AND has_sequence_privilege($1::name, c.oid, 'USAGE, UPDATE')))
		ORDER BY 1 LIMIT 1`
	var obj string
	err := d.db.QueryRowContext(ctx, q, role, d.schemas).Scan(&obj)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return obj, err
}

// IsReadOnly trả về true khi mọi role ứng dụng vừa có default_transaction_read_only=on
// vừa không còn quyền ghi trên bảng/sequence nào trong các schema.
func (d *PGTargetDriver) IsReadOnly(ctx context.Context) (bool, error) {
	if d.db == nil {
		return false, fmt.Errorf("target db not configured")
	}
	if len(d.appRoles) == 0 {
		return false, nil
	}
	for _, role := range d.appRoles {
		var ro bool
		q := `SELECT EXISTS (
			SELECT 1 FROM pg_db_role_setting s JOIN pg_roles r ON s.setrole = r.oid
			WHERE r.rolname = $1 AND s.setdatabase IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
			  AND 'default_transaction_read_only=on' = ANY(s.setconfig))`
		if err := d.db.QueryRowContext(ctx, q, role).Scan(&ro); err != nil {
			return false, err
		}
		if !ro {
			return false, nil
		}
		obj, err := d.firstWritableObject(ctx, role)
		if err != nil {
			return false, err
		}
		if obj != "" {
			return false, nil
		}
	}
	return true, nil
}

// ApplyRemaining (bước 3) chờ đến khi tập đã apply của pipeline trên target bao phủ mọi commit của
// source (đã fence). Không xác minh được thì trả lỗi để promote không bao giờ mở ghi khi target còn
// thiếu dữ liệu (PR-1). Source không truy cập được (failover khẩn cấp, 05 §5): chỉ cảnh báo và đi
// tiếp, mất dữ liệu bằng lag — giống MySQLTargetDriver.
//
// Source PG: KHÔNG so lwd với pg_current_wal_lsn(). lwd chỉ tiến tới commit LSN (đầu commit record)
// của các Txn pipeline đã thấy, còn pg_current_wal_lsn() nằm ở/sau CUỐI commit record cuối cùng và
// còn cộng WAL không thuộc transaction mà pgoutput không bao giờ gửi (RUNNING_XACTS, checkpoint…):
// sau fence không còn commit nào nên điều kiện đó không bao giờ đúng. Thay vào đó (fenceMarker):
//  1. chờ source yên: không còn backend nào trong database đang giữ xid, không còn prepared
//     transaction — mọi Txn đã bắt đầu trước fence đều đã commit hoặc abort;
//  2. ghi một marker qua stream replication: pg_logical_emit_message(false, 'strand_watermark', …),
//     cùng loại message mà snapshot logic dùng; reader phát nó thành một Txn rỗng có LSN = LSN mà
//     hàm trả về (cuối record), router ghi nhận nó như mọi Txn;
//  3. chờ lwd bền vững trên target ≥ H, vị trí chèn WAL đọc ngay trước marker (record của marker bắt
//     đầu ở LSN ≥ H, mọi commit record khác nằm trước H).
//
// Đúng vì: logical decoding giải mã WAL theo thứ tự, nên mọi Txn có commit record đứng trước marker
// được phát (và được router xử lý) trước marker; lwd ≥ marker nghĩa là mọi Txn đó đã commit ở target.
// Không có commit nào của ứng dụng đứng SAU marker: Fence đã đặt NOLOGIN (role ứng dụng), ngắt session
// và chờ chúng thoát; default_transaction_read_only chặn transaction mới; bước 1 ở trên chờ hết các
// Txn còn dở (kể cả của session không bị ngắt). Marker cần pgoutput option messages (PG14+) và
// --fence phải trỏ vào đúng database của slot (message chỉ được giải mã trong database của nó).
//
// Source MySQL (MySQL→PG): so gtid_executed với tập GTID đã apply như MySQLTargetDriver.
func (d *PGTargetDriver) ApplyRemaining(ctx context.Context) error {
	if d.source == nil || d.pipelineID == "" || d.db == nil {
		return fmt.Errorf("apply remaining: cannot verify the target caught up (no source connection / pipeline id configured)")
	}
	if err := d.source.PingContext(ctx); err != nil {
		d.logger.Warn("source unreachable: cannot verify remaining log was applied (failover; data loss bounded by lag)", "error", err)
		return nil
	}
	deadline := time.Now().Add(d.catchUpTimeout)

	var caughtUp func(applied checkpoint.AppliedSet) (string, error)
	switch d.sourceEngine {
	case model.EngineMySQL:
		var executed string
		if err := d.source.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
			return fmt.Errorf("apply remaining: read source gtid_executed: %w", err)
		}
		caughtUp = func(applied checkpoint.AppliedSet) (string, error) {
			return MissingFromApplied(executed, applied)
		}
	default:
		marker, err := d.fenceMarker(ctx, deadline)
		if err != nil {
			return err
		}
		caughtUp = func(applied checkpoint.AppliedSet) (string, error) {
			if applied != nil && applied.Lwd().LSN >= marker {
				return "", nil
			}
			return fmt.Sprintf("WAL up to the promote fence marker (lwd must reach %s; is the pipeline running, reading this database, on PostgreSQL 14+?)",
				checkpoint.FormatLSN(marker)), nil
		}
	}

	interval := d.catchUpInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	for {
		applied, err := checkpoint.LoadAppliedSnapshotPG(ctx, d.db, d.pipelineID, d.sourceEngine)
		if err != nil {
			return fmt.Errorf("apply remaining: load target applied set: %w", err)
		}
		missing, err := caughtUp(applied)
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
		case <-time.After(interval):
		}
	}
}

// pgSourceBusyQuery trả mô tả một transaction còn có thể commit trên source ("" = source đã yên):
// backend trong database hiện tại đang giữ xid (đã ghi gì đó), hoặc prepared transaction chưa
// COMMIT/ROLLBACK PREPARED (COMMIT PREPARED được phép cả khi default_transaction_read_only = on).
const pgSourceBusyQuery = `SELECT COALESCE(
	(SELECT 'transaction ' || backend_xid::text || ' of pid ' || pid::text || ' (user ' || COALESCE(usename, '?') || ')'
	   FROM pg_stat_activity
	  WHERE pid <> pg_backend_pid() AND datname = current_database() AND backend_xid IS NOT NULL
	  ORDER BY pid LIMIT 1),
	(SELECT 'prepared transaction ' || quote_literal(gid)
	   FROM pg_prepared_xacts WHERE database = current_database() ORDER BY prepared LIMIT 1),
	'')`

// fenceMarker chờ source PG yên (xem pgSourceBusyQuery) rồi ghi marker vào WAL. Trả mốc H mà lwd
// phải đạt: chỉ Txn có LSN ≥ H (marker hoặc sau nó) mới đưa được lwd tới H.
func (d *PGTargetDriver) fenceMarker(ctx context.Context, deadline time.Time) (uint64, error) {
	var standby bool
	if err := d.source.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&standby); err != nil {
		return 0, fmt.Errorf("apply remaining: check source recovery state: %w", err)
	}
	if standby {
		return 0, fmt.Errorf("apply remaining: the source is a standby: cannot write the promote fence marker (pg_logical_emit_message); point --fence at the primary")
	}
	interval := d.catchUpInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	for {
		var busy string
		if err := d.source.QueryRowContext(ctx, pgSourceBusyQuery).Scan(&busy); err != nil {
			return 0, fmt.Errorf("apply remaining: check open source transactions: %w", err)
		}
		if busy == "" {
			break
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("apply remaining: source still has %s after %s: it could commit after the fence; end it (or COMMIT/ROLLBACK PREPARED) and retry",
				busy, d.catchUpTimeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(interval):
		}
	}
	// Session riêng tắt read-only ở mức session: sau fence, mặc định cấp cluster là read-only.
	conn, err := d.source.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("apply remaining: open source session: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET default_transaction_read_only = off"); err != nil {
		return 0, fmt.Errorf("apply remaining: prepare marker session: %w", err)
	}
	// H = vị trí chèn WAL đọc SAU khi source đã yên và TRƯỚC marker: mọi commit record đã nằm trước H,
	// record của marker bắt đầu ở LSN ≥ H (như mốc high của snapshot logic). So lwd với H thay vì với LSN
	// hàm trả về, để không phụ thuộc việc pgoutput báo đầu hay cuối record của message.
	var hStr string
	var version int
	if err := conn.QueryRowContext(ctx, "SELECT pg_current_wal_insert_lsn()::text, current_setting('server_version_num')::int").Scan(&hStr, &version); err != nil {
		return 0, fmt.Errorf("apply remaining: read source WAL insert position: %w", err)
	}
	h, err := checkpoint.ParseLSN(hStr)
	if err != nil {
		return 0, fmt.Errorf("apply remaining: parse source LSN %q: %w", hStr, err)
	}
	q := "SELECT pg_logical_emit_message(false, $1::text, $2::text)::text"
	if version >= 170000 {
		q = "SELECT pg_logical_emit_message(false, $1::text, $2::text, true)::text" // PG17+: flush ngay
	}
	var markerStr string
	if err := conn.QueryRowContext(ctx, q, pgreader.WatermarkPrefix, "promote_fence:"+d.pipelineID).Scan(&markerStr); err != nil {
		return 0, fmt.Errorf("apply remaining: write promote fence marker: %w", err)
	}
	d.logger.Info("promote fence marker written to the source WAL; waiting for the pipeline to apply up to it",
		"wal_insert_lsn", hStr, "marker_lsn", markerStr)
	return h, nil
}

// FindColumnMax tìm giá trị MAX của cột trong bảng target PostgreSQL (0 nếu bảng rỗng).
func (d *PGTargetDriver) FindColumnMax(ctx context.Context, table, column string) (int64, error) {
	if d.db == nil {
		return 0, fmt.Errorf("target db not configured")
	}
	q := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0)::bigint FROM %s", quoteIdent(column), quoteQualified(table))
	var maxVal int64
	if err := d.db.QueryRowContext(ctx, q).Scan(&maxVal); err != nil {
		return 0, fmt.Errorf("find max column for %s.%s: %w", table, column, err)
	}
	return maxVal, nil
}

// FindColumnMin tìm giá trị MIN của cột trong bảng target PostgreSQL (0 nếu bảng rỗng), cho sequence
// giảm dần (đối xứng với FindColumnMax).
func (d *PGTargetDriver) FindColumnMin(ctx context.Context, table, column string) (int64, error) {
	if d.db == nil {
		return 0, fmt.Errorf("target db not configured")
	}
	q := fmt.Sprintf("SELECT COALESCE(MIN(%s), 0)::bigint FROM %s", quoteIdent(column), quoteQualified(table))
	var minVal int64
	if err := d.db.QueryRowContext(ctx, q).Scan(&minVal); err != nil {
		return 0, fmt.Errorf("find min column for %s.%s: %w", table, column, err)
	}
	return minVal, nil
}

// Setval (bước 4) đặt sequence trên target bằng một câu lệnh duy nhất, không bao giờ lùi:
// setval(seq, GREATEST(val, last_value hiện tại), true) cho sequence tăng; sequence giảm dần
// (INCREMENT âm) dùng LEAST để không quay về phía các giá trị đã cấp. Áp dụng cho cả identity.
func (d *PGTargetDriver) Setval(ctx context.Context, seq SequenceInfo, val int64) error {
	if d.db == nil {
		return fmt.Errorf("target db not configured")
	}
	name := strings.TrimSpace(seq.Name)
	if name == "" {
		return fmt.Errorf("setval: sequence name is empty")
	}
	qn := quoteQualified(name)
	fn := "GREATEST"
	if isDescending(seq) {
		fn = "LEAST"
	}
	q := fmt.Sprintf("SELECT setval($1::regclass, %s($2::bigint, (SELECT last_value FROM %s)), true)", fn, qn)
	if _, err := d.db.ExecContext(ctx, q, qn, val); err != nil {
		return fmt.Errorf("setval %s to %d: %w", name, val, err)
	}
	return nil
}

// OpenWrites (bước 5) mở quyền ghi cho role ứng dụng: RESET default_transaction_read_only,
// cấp INSERT/UPDATE/DELETE trên mọi bảng của ứng dụng và USAGE/SELECT trên mọi sequence trong các
// schema (spec/03 §2: role ứng dụng chỉ được cấp quyền ghi lúc promote; bảng _strand_* bị loại trừ),
// rồi cấp lại đúng các quyền đã ghi lại ở bước 2 (ví dụ TRUNCATE, UPDATE trên sequence), và ngắt
// các session của role ứng dụng còn mang mặc định read-only.
func (d *PGTargetDriver) OpenWrites(ctx context.Context) error {
	if d.db == nil {
		return fmt.Errorf("target db not configured")
	}
	if len(d.appRoles) == 0 {
		return fmt.Errorf("open writes: no application role configured (--app-role)")
	}
	if err := d.ensureGrantsTable(ctx); err != nil {
		return fmt.Errorf("open writes: create _strand_promote_grants: %w", err)
	}
	for _, role := range d.appRoles {
		qr := quoteIdent(role)
		if _, err := d.db.ExecContext(ctx, fmt.Sprintf("ALTER ROLE %s RESET default_transaction_read_only", qr)); err != nil {
			return fmt.Errorf("open writes: reset default_transaction_read_only on role %s: %w", role, err)
		}

		type grant struct{ schema, rel, kind, privs string }
		var grants []grant
		rows, err := d.db.QueryContext(ctx, `SELECT n.nspname, c.relname, c.relkind::text
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r', 'p', 'S') AND c.relname NOT LIKE '\_strand\_%'
			ORDER BY 1, 2`, d.schemas)
		if err != nil {
			return fmt.Errorf("open writes: list relations: %w", err)
		}
		for rows.Next() {
			var g grant
			if err := rows.Scan(&g.schema, &g.rel, &g.kind); err != nil {
				rows.Close()
				return err
			}
			if g.kind == "S" {
				g.privs = "USAGE, SELECT"
			} else {
				g.privs = "INSERT, UPDATE, DELETE"
			}
			grants = append(grants, g)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		// Các quyền đã thu hồi ở bước 2 (chỉ những object còn tồn tại).
		rows, err = d.db.QueryContext(ctx, `SELECT g.schema_name, g.rel_name, g.rel_kind, g.privilege
			FROM _strand_promote_grants g
			JOIN pg_namespace n ON n.nspname = g.schema_name
			JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = g.rel_name
			WHERE g.pipeline_id = $1 AND g.role_name = $2`, d.pipelineID, role)
		if err != nil {
			return fmt.Errorf("open writes: read recorded grants: %w", err)
		}
		for rows.Next() {
			var g grant
			if err := rows.Scan(&g.schema, &g.rel, &g.kind, &g.privs); err != nil {
				rows.Close()
				return err
			}
			grants = append(grants, g)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, g := range grants {
			obj := quoteIdent(g.schema) + "." + quoteIdent(g.rel)
			kw := "TABLE"
			if g.kind == "S" {
				kw = "SEQUENCE"
			}
			if _, err := d.db.ExecContext(ctx, fmt.Sprintf("GRANT %s ON %s %s TO %s", g.privs, kw, obj, qr)); err != nil {
				return fmt.Errorf("open writes: grant %s on %s to %s: %w", g.privs, obj, role, err)
			}
		}
		if _, err := d.db.ExecContext(ctx, "DELETE FROM _strand_promote_grants WHERE pipeline_id = $1 AND role_name = $2", d.pipelineID, role); err != nil {
			return fmt.Errorf("open writes: clear recorded grants: %w", err)
		}
		// Session ứng dụng mở trong lúc giữ đã nhận default_transaction_read_only=on lúc kết nối và giữ
		// nguyên sau RESET; ngắt chúng để pool kết nối lại với mặc định read-write.
		if _, err := d.db.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE usename = $1 AND pid <> pg_backend_pid() AND backend_type = 'client backend'`, role); err != nil {
			return fmt.Errorf("open writes: terminate read-only sessions of %s: %w", role, err)
		}
	}
	return nil
}

// FailoverSummary đọc thông tin checkpoint target để tóm tắt cho người vận hành (quyết định P1-17, câu 3).
func (d *PGTargetDriver) FailoverSummary(ctx context.Context) (string, error) {
	if d.db == nil || d.pipelineID == "" {
		return "", fmt.Errorf("target db or pipeline id not configured")
	}
	var applied string
	var updatedAt time.Time
	q := `SELECT applied, updated_at FROM _strand_checkpoint WHERE pipeline_id = $1`
	err := d.db.QueryRowContext(ctx, q, d.pipelineID).Scan(&applied, &updatedAt)
	if err == sql.ErrNoRows {
		return fmt.Sprintf("target has no checkpoint recorded for pipeline %q", d.pipelineID), nil
	}
	if err != nil {
		return "", fmt.Errorf("query target checkpoint: %w", err)
	}
	ageSec := int64(time.Since(updatedAt).Seconds())
	if ageSec < 0 {
		ageSec = 0
	}
	return fmt.Sprintf(
		"target applied up to %s (checkpoint updated %s, %ds ago); transactions committed on the old source after this are LOST if you continue",
		applied,
		updatedAt.UTC().Format(time.RFC3339),
		ageSec,
	), nil
}

// =============================================================================
// DiscoverPGSequences
// =============================================================================

// DiscoverPGSequences liệt kê mọi sequence và identity trong các schema chỉ định (rỗng = mọi schema
// không phải hệ thống). Sequence gắn cột là sequence có pg_depend deptype 'a' (serial / OWNED BY)
// hoặc 'i' (identity) trỏ tới một cột; còn lại là sequence độc lập (02 §8). Đọc cache và increment.
// Tên trả về không kèm schema nếu schema là public.
func DiscoverPGSequences(ctx context.Context, db *sql.DB, schemas []string) ([]SequenceInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("discover pg sequences: db is nil")
	}
	schemas = cleanList(schemas)
	q := `
		SELECT n.nspname, c.relname, s.seqcache, s.seqincrement,
			COALESCE(tn.nspname, ''), COALESCE(t.relname, ''), COALESCE(a.attname, '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_sequence s ON s.seqrelid = c.oid
		LEFT JOIN LATERAL (
			SELECT dep.refobjid, dep.refobjsubid FROM pg_depend dep
			WHERE dep.classid = 'pg_class'::regclass AND dep.objid = c.oid
			  AND dep.refclassid = 'pg_class'::regclass AND dep.refobjsubid > 0
			  AND dep.deptype IN ('a', 'i')
			LIMIT 1
		) d ON true
		LEFT JOIN pg_class t ON t.oid = d.refobjid
		LEFT JOIN pg_namespace tn ON tn.oid = t.relnamespace
		LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
		WHERE c.relkind = 'S'
		  AND (cardinality($1::text[]) = 0 OR n.nspname = ANY($1::text[]))
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname`
	if schemas == nil {
		schemas = []string{}
	}
	rows, err := db.QueryContext(ctx, q, schemas)
	if err != nil {
		return nil, fmt.Errorf("discover pg sequences query failed: %w", err)
	}
	defer rows.Close()

	qualify := func(schema, name string) string {
		if schema == "" || schema == "public" {
			return name
		}
		return schema + "." + name
	}

	var results []SequenceInfo
	for rows.Next() {
		var seqSchema, seqName, tblSchema, tblName, colName string
		var cacheSize, incrementBy int64
		if err := rows.Scan(&seqSchema, &seqName, &cacheSize, &incrementBy, &tblSchema, &tblName, &colName); err != nil {
			return nil, fmt.Errorf("scan sequence metadata: %w", err)
		}
		info := SequenceInfo{
			Name:        qualify(seqSchema, seqName),
			CacheSize:   cacheSize,
			IncrementBy: incrementBy,
			Type:        SequenceTypeIndependent,
		}
		if tblName != "" && colName != "" {
			info.Type = SequenceTypeColumn
			info.Table = qualify(tblSchema, tblName)
			info.Column = colName
		}
		results = append(results, info)
	}
	return results, rows.Err()
}

// =============================================================================
// Đồng bộ định kỳ last_value của sequence độc lập
// =============================================================================

// SyncIndependentSequences đọc last_value của mọi sequence độc lập từ source một lần và lưu vào
// LastSyncedValue trong _strand_promote (qua StateStore). Đây là "lần đồng bộ cuối" mà promote dùng
// để gợi ý giá trị khi source không đọc được sau fence (02 §8). LastSyncedValue không bao giờ giảm.
// Khi promote đã bắt đầu (state khác validating) thì không ghi gì: tiến trình promote là của promoter.
func SyncIndependentSequences(
	ctx context.Context,
	source SourceDriver,
	store StateStore,
	pipelineID string,
	sequences []SequenceInfo,
) ([]SequenceInfo, error) {
	if source == nil || store == nil {
		return nil, fmt.Errorf("sync independent sequences: source or store is nil")
	}
	progress, err := store.LoadProgress(ctx, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("load progress for sequence sync: %w", err)
	}
	if progress != nil && progress.State != "" && progress.State != StateValidating {
		return progress.Sequences, nil
	}
	if progress == nil {
		progress = &Progress{
			PipelineID:         pipelineID,
			State:              StateValidating,
			CurrentStep:        StepFenceSource,
			CompletedSteps:     []PromoteStep{},
			SuggestedSequences: make(map[string]int64),
		}
	}
	progress.Sequences = mergeSequences(progress.Sequences, sequences)

	updated := false
	for i, seq := range progress.Sequences {
		if seq.Type != SequenceTypeIndependent {
			continue
		}
		lastVal, err := source.ReadSequenceLastValue(ctx, seq.Name)
		if err != nil {
			continue
		}
		// Không bao giờ lùi theo chiều của sequence (giảm dần: chỉ nhận giá trị nhỏ hơn).
		progress.Sequences[i].LastSyncedValue = further(seq, seq.LastSyncedValue, lastVal)
		progress.Sequences[i].CurrentValue = lastVal
		updated = true
	}
	if updated {
		if err := store.SaveProgress(ctx, progress); err != nil {
			return nil, fmt.Errorf("save synced sequence progress: %w", err)
		}
	}
	return progress.Sequences, nil
}

// StartPeriodicSequenceSync chạy SyncIndependentSequences mỗi syncInterval (Δt_sync) trong goroutine
// nền. Trả về hàm stop và kênh done đóng khi goroutine đã thoát. Pipeline PG gọi hàm này khi sync
// (chưa nối: cmd/strand/sync.go chưa có pipeline PG).
func StartPeriodicSequenceSync(
	ctx context.Context,
	source SourceDriver,
	store StateStore,
	pipelineID string,
	sequences []SequenceInfo,
	syncInterval time.Duration,
	logger *slog.Logger,
) (stop func(), done <-chan struct{}) {
	if syncInterval <= 0 {
		syncInterval = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctxSync, cancel := context.WithCancel(ctx)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(syncInterval)
		defer ticker.Stop()
		for {
			if _, err := SyncIndependentSequences(ctxSync, source, store, pipelineID, sequences); err != nil && ctxSync.Err() == nil {
				logger.Warn("independent sequence sync failed", "pipeline", pipelineID, "error", err)
			}
			select {
			case <-ctxSync.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return cancel, doneCh
}
