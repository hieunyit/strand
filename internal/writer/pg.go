package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// spec: 02 §3 (Writer, DeltaRow), §4 (quy tắc epoch, metadata), §6 (COPY vào bảng tạm rồi MERGE), §8 (PG TOAST)
// invariant: CP-1, SG-1, CP-5

const (
	// pgCopyBlockRows: số row tối đa của một khối COPY + MERGE.
	pgCopyBlockRows = 5000
	// pgDefaultCopyMinRows: khối ít row hơn ngưỡng này dùng một câu INSERT … ON CONFLICT nhiều row
	// thay cho COPY + MERGE (xem pgUpsertBlock.flush).
	pgDefaultCopyMinRows = 8
	// pgDeleteMaxKeys: số khóa tối đa của một câu DELETE gom (lũy thừa của 2, xem pgDeleteBatch).
	pgDeleteMaxKeys = 1024
	// pgMaxParams: trần số tham số của một câu lệnh trong giao thức PG (uint16).
	pgMaxParams = 65535
	// pgBlockMaxBytes: ngân sách byte payload (ước lượng theo argBytes) của một khối COPY + MERGE
	// hoặc một lần gửi pipeline (review N13). PG không có trần gói như max_allowed_packet, nhưng
	// 5000 row hay 1000 câu chứa blob lớn bị giữ trọn trong bộ nhớ trước khi gửi. Một row lớn hơn
	// ngân sách vẫn đi nguyên (một mình), không bao giờ tách giữa row.
	pgBlockMaxBytes = 16 * 1024 * 1024
)

// ToastRowMissingError: partial UPDATE (cột TOAST Unchanged) không tìm thấy row ở target.
// Không thể dựng lại row vì thiếu giá trị cột TOAST, nên writer không ghi gì và trả lỗi
// (bọc trong exitcode verify_mismatch, exit 5) kèm bảng/PK để pipeline đưa range vào verify.
// spec: 02 §8: "Row không tồn tại ở target thì báo lỗi và đưa range vào verify."
type ToastRowMissingError struct {
	Table model.TableID
	Key   []model.Value
}

func (e *ToastRowMissingError) Error() string {
	return fmt.Sprintf("partial (toast) update: row missing at target: table=%s key=%v", e.Table, e.Key)
}

func newToastRowMissing(t model.TableID, key []model.Value) error {
	te := &ToastRowMissingError{Table: t, Key: append([]model.Value(nil), key...)}
	// Chọn verify_mismatch (exit 5) thay vì thêm mã mới: bảng mã lỗi ở spec 02 §12 ổn định giữa
	// các bản, và bản chất lỗi là target lệch source — đúng nghĩa verify_mismatch. Kiểu
	// *ToastRowMissingError (errors.As) mang bảng/PK cho pipeline.
	return exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError,
		fmt.Sprintf("table %s: partial update for key %v but row missing at target", t, key), te)
}

// PGWriter cài đặt Writer cho PostgreSQL target: COPY vào bảng tạm rồi MERGE (PG 15+).
// db phải mở bằng driver pgx stdlib ("pgx") vì cần pgx.Conn để COPY.
type PGWriter struct {
	db          *sql.DB
	pipelineID  string
	workerID    int
	epoch       int64
	pk          pkRegistry
	mu          sync.Mutex
	orderer     *CommitOrderer
	copyMinRows int
	ordered     map[model.TableID]bool // bảng có UNIQUE ngoài PK / ràng buộc loại trừ (cache từ catalog)
	// txBegin (seam kiểm thử, SetTxBeginner): mở transaction thay cho kết nối lấy từ db.
	txBegin        func(ctx context.Context) (pgx.Tx, error)
	MergeCount     int64
	TotalMergeRows int64
}

// NewPGWriter khởi tạo PGWriter mới cho PostgreSQL target.
func NewPGWriter(db *sql.DB, pipelineID string, workerID int, epoch int64) *PGWriter {
	return &PGWriter{
		db:          db,
		pipelineID:  pipelineID,
		workerID:    workerID,
		epoch:       epoch,
		copyMinRows: pgDefaultCopyMinRows,
	}
}

// SetCopyMinRows đặt ngưỡng số row tối thiểu để một khối upsert đi qua COPY + MERGE; khối nhỏ hơn
// dùng INSERT … ON CONFLICT nhiều row. n <= 1: mọi khối đều đi qua COPY + MERGE.
func (w *PGWriter) SetCopyMinRows(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.copyMinRows = n
}

// SetTxBeginner (chỉ dùng cho kiểm thử/mô phỏng): ApplyBatch mở transaction bằng fn thay vì lấy kết nối
// pgx từ db; phần áp dữ liệu, row delta và commit giữ nguyên. nil = đường thật.
func (w *PGWriter) SetTxBeginner(fn func(ctx context.Context) (pgx.Tx, error)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.txBegin = fn
}

// SetPrimaryKey khai báo tên các cột khóa chính của bảng (theo thứ tự của RowChange.Key).
func (w *PGWriter) SetPrimaryKey(t model.TableID, cols []string) { w.pk.set(t, cols) }

// SetCommitOrderer gán CommitOrderer để kiểm soát thứ tự commit (CP-5).
func (w *PGWriter) SetCommitOrderer(orderer *CommitOrderer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.orderer = orderer
}

// GetStats trả về số câu MERGE đã chạy và tổng số row đã đi qua MERGE.
func (w *PGWriter) GetStats() (mergeCount, totalMergeRows int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.MergeCount, w.TotalMergeRows
}

// EnsureWorkerRow đảm bảo row cho worker_id đã tồn tại trong _strand_worker_delta trên Postgres.
// Không bao giờ hạ epoch: instance có epoch cũ hơn nhận lỗi fenced (SG-1).
func (w *PGWriter) EnsureWorkerRow(ctx context.Context) error {
	query := `INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied)
	VALUES ($1, $2, $3, 1, '')
	ON CONFLICT (pipeline_id, worker_id) DO UPDATE SET epoch = EXCLUDED.epoch
	WHERE _strand_worker_delta.epoch <= EXCLUDED.epoch`
	if _, err := w.db.ExecContext(ctx, query, w.pipelineID, w.workerID, w.epoch); err != nil {
		return fmt.Errorf("failed to ensure pg worker row: %w", err)
	}
	var cur int64
	if err := w.db.QueryRowContext(ctx,
		"SELECT epoch FROM _strand_worker_delta WHERE pipeline_id = $1 AND worker_id = $2",
		w.pipelineID, w.workerID).Scan(&cur); err != nil {
		return fmt.Errorf("failed to read pg worker row epoch: %w", err)
	}
	if cur != w.epoch {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced,
			fmt.Sprintf("worker %d fenced: row epoch %d, instance epoch %d", w.workerID, cur, w.epoch), nil)
	}
	return nil
}

// ApplyBatch ghi dữ liệu của batch và row delta (hoặc row toàn cục ở strict) trong CÙNG một
// transaction trên Postgres (CP-1). Cả batch là một commit: một Txn nguồn không bao giờ bị tách.
// spec: 02 §4 quy tắc 3, 5; §6; §8
func (w *PGWriter) ApplyBatch(ctx context.Context, batch []*model.Txn, delta DeltaRow) (err error) {
	w.mu.Lock()
	orderer := w.orderer
	w.mu.Unlock()

	strict := orderer != nil && orderer.Mode() == "strict" && len(batch) > 0
	if strict {
		if err := validateStrictBatch(batch); err != nil {
			return err
		}
		// Chờ đến lượt TRƯỚC khi mở transaction (CP-5), giống MySQLWriter: nếu ghi trước rồi mới
		// chờ, batch seq lớn giữ row lock mà batch seq nhỏ cần, hai bên chờ nhau.
		if err := orderer.WaitTurn(ctx, batch[0].Seq()); err != nil {
			return err
		}
	}

	committing := false
	if strict {
		// Lỗi TRƯỚC commit: batch chưa commit, lượt vẫn thuộc về nó (caller có thể retry cùng seq).
		// Lỗi KHI commit: kết quả không chắc chắn, dừng orderer để không seq nào commit sau nó.
		defer func() {
			if err != nil && committing {
				orderer.Fail(err)
			}
		}()
	}

	w.mu.Lock()
	begin := w.txBegin
	w.mu.Unlock()
	if begin != nil {
		// Seam kiểm thử (SetTxBeginner): transaction do bên gọi cung cấp, cùng đường áp dữ liệu.
		tx, berr := begin(ctx)
		if berr != nil {
			return fmt.Errorf("failed to begin transaction: %w", berr)
		}
		if err = w.applyTx(ctx, tx, batch, delta, &committing); err != nil {
			return err
		}
		if strict {
			orderer.CommitDone(batch[len(batch)-1].Seq())
		}
		return nil
	}

	// COPY cần pgx.Conn. Toàn bộ transaction (dữ liệu, COPY, MERGE, row delta, commit) chạy trên
	// đúng một kết nối vật lý, bên trong conn.Raw — cách dùng driver conn được database/sql cho phép.
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire pg connection: %w", err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn any) error {
		sc, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("pg writer requires the pgx stdlib driver, got %T", driverConn)
		}
		pc := sc.Conn()
		rerr := w.applyOnConn(ctx, pc, batch, delta, &committing)
		if pc.PgConn().TxStatus() != 'I' {
			// Transaction còn mở (rollback không gửi được): đóng kết nối để pool loại bỏ nó,
			// không trả về pool một kết nối đang ở giữa transaction.
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_ = pc.Close(cctx)
			cancel()
		}
		return rerr
	})
	if err != nil {
		return err
	}

	if strict {
		orderer.CommitDone(batch[len(batch)-1].Seq())
	}
	return nil
}

func (w *PGWriter) applyOnConn(ctx context.Context, pc *pgx.Conn, batch []*model.Txn, delta DeltaRow, committing *bool) error {
	tx, err := pc.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	return w.applyTx(ctx, tx, batch, delta, committing)
}

// applyTx: dữ liệu của batch + row delta (hoặc row toàn cục) + commit trên transaction tx đã mở.
func (w *PGWriter) applyTx(ctx context.Context, tx pgx.Tx, batch []*model.Txn, delta DeltaRow, committing *bool) error {
	defer func() {
		// Rollback bằng context không bị hủy để transaction không treo trên kết nối khi ctx đã hủy.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_ = tx.Rollback(rctx) // no-op sau commit
		cancel()
	}()

	// 1. Dữ liệu, theo đúng thứ tự row trong Txn và thứ tự Txn trong batch.
	w.mu.Lock()
	minRows := w.copyMinRows
	w.mu.Unlock()
	a := &pgApplier{w: w, ctx: ctx, tx: tx, tmp: make(map[model.TableID]string), copyMinRows: minRows}
	var routed int64
	for _, txn := range batch {
		routed += txn.TotalRows()
		if err := txn.ForEachRow(a.apply); err != nil {
			return fmt.Errorf("failed to apply row changes: %w", err)
		}
	}
	if err := a.flushAll(); err != nil {
		return fmt.Errorf("failed to apply row changes: %w", err)
	}
	// F11: mọi row đã định tuyến phải nằm trong một câu lệnh đã chạy thành công — kiểm TRƯỚC commit,
	// để batch thiếu row rollback (cùng row delta) thay vì được đánh dấu đã apply.
	if err := checkRowsWritten(batch, routed, a.handed, a.written); err != nil {
		return err
	}
	if delta.Stats != nil {
		delta.Stats.Rows = a.written
	}

	pid, wid, ep := resolveDelta(delta, w.pipelineID, w.workerID, w.epoch)
	if delta.Global == nil {
		// 2. Row delta có điều kiện epoch trong CÙNG transaction (CP-1, SG-1).
		tag, err := tx.Exec(ctx,
			"UPDATE _strand_worker_delta SET applied = $1, batch_no = batch_no + 1 WHERE pipeline_id = $2 AND worker_id = $3 AND epoch = $4",
			delta.Applied, pid, wid, ep)
		if err != nil {
			return fmt.Errorf("failed to update pg worker delta: %w", err)
		}
		if n := tag.RowsAffected(); n != 1 {
			// spec: 02 §4 quy tắc 3: số row != 1 -> rollback và dừng với mã fenced
			return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("writer fenced: expected 1 row affected, got %d", n), nil)
		}
	} else {
		// 2b. Strict, quy tắc 5: row toàn cục ghi trong transaction commit của batch, có điều kiện epoch.
		applied, gerr := delta.Global()
		if gerr != nil {
			return gerr
		}
		tag, gerr := tx.Exec(ctx,
			"UPDATE _strand_checkpoint SET applied = $1, updated_at = now() WHERE pipeline_id = $2 AND epoch = $3",
			applied, pid, ep)
		if gerr != nil {
			return fmt.Errorf("failed to update pg global checkpoint: %w", gerr)
		}
		if n := tag.RowsAffected(); n != 1 {
			return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("writer fenced: global checkpoint update affected %d rows", n), nil)
		}
	}

	// 3. Commit dữ liệu cùng delta.
	*committing = true
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// pgApplier: áp các row của một batch trong một transaction
// -----------------------------------------------------------------------------

// pgApplier gom row INSERT/UPDATE liên tiếp (cùng bảng, cùng tập cột) thành một khối COPY + MERGE
// và DELETE liên tiếp cùng bảng thành một câu DELETE. Mọi loại thao tác khác cắt khối, nên thứ tự
// tương đối giữa các khối và các câu lệnh lẻ luôn đúng thứ tự nguồn.
type pgApplier struct {
	w   *PGWriter
	ctx context.Context
	tx  pgx.Tx
	up  *pgUpsertBlock
	del *pgDeleteBatch
	tmp map[model.TableID]string // bảng tạm đã tạo trong transaction này, theo bảng đích

	// q: câu lệnh từng row của bảng "ordered" chờ gửi, đúng thứ tự nguồn, pipeline một round-trip.
	q       *pgx.Batch
	qTables []model.TableID
	qRows   []int64 // số row nguồn mà từng câu trong q phủ (câu DELETE khóa cũ của đổi PK: 0)
	qBytes  int     // ước lượng byte tham số của q (review N13)

	copyMinRows int

	// Bộ đếm phát hiện mất row (F11): handed = số row đã nhận qua apply; written = số row nằm trong
	// câu lệnh đã chạy thành công (khối upsert tính cả row bị ảnh sau cùng khóa thay thế). Cuối batch
	// hai số phải bằng nhau và bằng tổng row đã định tuyến (checkRowsWritten).
	handed  int64
	written int64
}

func (a *pgApplier) flushUpsert() error {
	if a.up == nil {
		return nil
	}
	err := a.up.flush(a)
	if err == nil {
		a.written += a.up.covered
	}
	a.up = nil
	return err
}

func (a *pgApplier) flushDelete() error {
	if a.del == nil {
		return nil
	}
	err := a.del.flush(a.ctx, a.tx)
	if err == nil {
		a.written += int64(len(a.del.keys))
	}
	a.del = nil
	return err
}

func (a *pgApplier) flushAll() error {
	if err := a.flushQueue(); err != nil {
		return err
	}
	if err := a.flushUpsert(); err != nil {
		return err
	}
	return a.flushDelete()
}

// pgOrderedQueueMax: số câu lệnh tối đa trong một lần gửi pipeline.
const pgOrderedQueueMax = 1000

func (a *pgApplier) enqueue(t model.TableID, q string, args []any, rows int64) error {
	if a.q == nil {
		a.q = &pgx.Batch{}
	}
	a.q.Queue(q, args...)
	a.qTables = append(a.qTables, t)
	a.qRows = append(a.qRows, rows)
	a.qBytes += len(q) + argsBytes(args)
	if a.queueFull() {
		return a.flushQueue()
	}
	return nil
}

// queueFull: hàng đợi pipeline đủ số câu hoặc đủ ngân sách byte (review N13).
func (a *pgApplier) queueFull() bool {
	return a.q != nil && (a.q.Len() >= pgOrderedQueueMax || a.qBytes >= pgBlockMaxBytes)
}

// flushQueue gửi các câu lệnh đang chờ trong một pipeline; PG thực thi tuần tự đúng thứ tự xếp hàng.
func (a *pgApplier) flushQueue() error {
	if a.q == nil || a.q.Len() == 0 {
		return nil
	}
	b, tables, rows := a.q, a.qTables, a.qRows
	a.q, a.qTables, a.qRows, a.qBytes = nil, nil, nil, 0
	br := a.tx.SendBatch(a.ctx, b)
	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("ordered apply on %s: %w", tables[i], err)
		}
	}
	if err := br.Close(); err != nil {
		return err
	}
	for _, n := range rows {
		a.written += n
	}
	return nil
}

// isOrdered cho biết bảng có UNIQUE index (kể cả partial) ngoài PK hoặc ràng buộc loại trừ.
// Với các bảng này, ràng buộc được PG kiểm ngay ở từng row (không deferrable), nên gộp ảnh cuối
// hoặc MERGE (thứ tự row không xác định) có thể bỏ qua trạng thái trung gian hợp lệ của nguồn
// (ví dụ hoán đổi giá trị UNIQUE giữa hai row) và gây lỗi trùng khóa giả. Tra catalog một lần mỗi
// bảng và lưu cache trong PGWriter (đổi DDL trên target cần khởi tạo lại writer).
func (a *pgApplier) isOrdered(t model.TableID) (bool, error) {
	a.w.mu.Lock()
	v, ok := a.w.ordered[t]
	a.w.mu.Unlock()
	if ok {
		return v, nil
	}
	const q = `SELECT EXISTS (SELECT 1 FROM pg_index WHERE indrelid = $1::text::regclass AND indisunique AND NOT indisprimary)
	    OR EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = $1::text::regclass AND contype = 'x')`
	if err := a.tx.QueryRow(a.ctx, q, quotePGTable(t)).Scan(&v); err != nil {
		return false, fmt.Errorf("inspect constraints of %s: %w", t, err)
	}
	a.w.mu.Lock()
	if a.w.ordered == nil {
		a.w.ordered = make(map[model.TableID]bool)
	}
	a.w.ordered[t] = v
	a.w.mu.Unlock()
	return v, nil
}

// applyOrdered áp một row của bảng "ordered": từng row một, đúng thứ tự nguồn, không gộp ảnh cuối,
// không COPY/MERGE. Quy tắc TOAST giữ nguyên như đường nhanh.
func (a *pgApplier) applyOrdered(row *model.RowChange) error {
	if err := a.flushUpsert(); err != nil {
		return err
	}
	if err := a.flushDelete(); err != nil {
		return err
	}
	switch row.Op {
	case model.OpInsert, model.OpUpdate:
		if len(row.Cols) == 0 {
			return fmt.Errorf("table %s: %s without column values", row.Table, row.Op)
		}
		pkCols, err := a.w.pk.get(row.Table, len(row.Key))
		if err != nil {
			return err
		}
		if hasUnchanged(row) {
			if row.Op == model.OpInsert {
				return fmt.Errorf("table %s: insert with unchanged (toast) column", row.Table)
			}
			if err := a.flushQueue(); err != nil {
				return err
			}
			return a.partialUpdate(row, pkCols)
		}
		if row.Op == model.OpUpdate && len(row.OldKey) > 0 && keysDiffer(row.OldKey, row.Key) {
			oldCols, err := a.w.pk.get(row.Table, len(row.OldKey))
			if err != nil {
				return err
			}
			q, args, err := buildDelete(row.Table, oldCols, [][]model.Value{row.OldKey})
			if err != nil {
				return err
			}
			if err := a.enqueue(row.Table, q, args, 0); err != nil { // nửa DELETE của đổi PK: row tính ở upsert
				return err
			}
		}
		names := make([]string, len(row.Cols))
		vals := make([]any, len(row.Cols))
		for i, c := range row.Cols {
			names[i] = c.Name
			vals[i] = c.Value
		}
		for _, pc := range pkCols {
			if !containsString(names, pc) {
				return fmt.Errorf("table %s: row image lacks primary key column %q", row.Table, pc)
			}
		}
		q, args := buildUpsert(row.Table, names, pkCols, [][]any{vals})
		return a.enqueue(row.Table, q, args, 1)
	case model.OpDelete:
		pkCols, err := a.w.pk.get(row.Table, len(row.Key))
		if err != nil {
			return err
		}
		q, args, err := buildDelete(row.Table, pkCols, [][]model.Value{row.Key})
		if err != nil {
			return err
		}
		return a.enqueue(row.Table, q, args, 1)
	default:
		return fmt.Errorf("table %s: unknown op %q", row.Table, row.Op)
	}
}

func (a *pgApplier) apply(row *model.RowChange) error {
	a.handed++
	ordered, err := a.isOrdered(row.Table)
	if err != nil {
		return err
	}
	if ordered {
		return a.applyOrdered(row)
	}
	// Đường nhanh (bảng chỉ có PK): các câu lệnh tuần tự còn chờ phải đi trước.
	if err := a.flushQueue(); err != nil {
		return err
	}
	switch row.Op {
	case model.OpInsert, model.OpUpdate:
		if len(row.Cols) == 0 {
			return fmt.Errorf("table %s: %s without column values", row.Table, row.Op)
		}
		pkCols, err := a.w.pk.get(row.Table, len(row.Key))
		if err != nil {
			return err
		}
		if hasUnchanged(row) {
			if row.Op == model.OpInsert {
				// INSERT luôn mang đủ giá trị; cột Unchanged ở đây nghĩa là ảnh hỏng — ghi sẽ ra NULL.
				return fmt.Errorf("table %s: insert with unchanged (toast) column", row.Table)
			}
			// Partial TOAST (spec 02 §8): không đi qua MERGE (MERGE INSERT sẽ ghi NULL cho cột thiếu).
			if err := a.flushAll(); err != nil {
				return err
			}
			return a.partialUpdate(row, pkCols)
		}

		if row.Op == model.OpUpdate && len(row.OldKey) > 0 && keysDiffer(row.OldKey, row.Key) {
			// Đổi PK với ảnh đầy đủ: DELETE khóa cũ rồi upsert khóa mới (như MySQLWriter).
			if err := a.flushAll(); err != nil {
				return err
			}
			oldCols, err := a.w.pk.get(row.Table, len(row.OldKey))
			if err != nil {
				return err
			}
			if err := execDelete(a.ctx, a.tx, row.Table, oldCols, [][]model.Value{row.OldKey}); err != nil {
				return err
			}
		}
		if err := a.flushDelete(); err != nil {
			return err
		}

		names := make([]string, len(row.Cols))
		vals := make([]any, len(row.Cols))
		for i, c := range row.Cols {
			names[i] = c.Name
			vals[i] = c.Value
		}
		if a.up != nil && a.up.full(row.Table, names, argsBytes(vals)) {
			if err := a.flushUpsert(); err != nil {
				return err
			}
		}
		if a.up == nil {
			for _, pc := range pkCols {
				if !containsString(names, pc) {
					return fmt.Errorf("table %s: row image lacks primary key column %q", row.Table, pc)
				}
			}
			a.up = &pgUpsertBlock{table: row.Table, cols: names, pkCols: pkCols, idx: make(map[string]int)}
		}
		a.up.add(row.Key, vals)
		return nil

	case model.OpDelete:
		if err := a.flushUpsert(); err != nil {
			return err
		}
		pkCols, err := a.w.pk.get(row.Table, len(row.Key))
		if err != nil {
			return err
		}
		if a.del != nil && (a.del.table != row.Table || len(a.del.keys) >= a.del.maxKeys()) {
			if err := a.flushDelete(); err != nil {
				return err
			}
		}
		if a.del == nil {
			a.del = &pgDeleteBatch{table: row.Table, pkCols: pkCols}
		}
		a.del.keys = append(a.del.keys, row.Key)
		return nil

	default:
		return fmt.Errorf("table %s: unknown op %q", row.Table, row.Op)
	}
}

// partialUpdate áp một UPDATE có cột Unchanged (TOAST): cột Unchanged không bao giờ nằm trong SET.
// spec: 02 §8 (partial UPDATE; đổi PK khi cột TOAST không đổi). Row thiếu ở target → ToastRowMissingError.
func (a *pgApplier) partialUpdate(row *model.RowChange, pkCols []string) error {
	if err := a.partialUpdateRow(row, pkCols); err != nil {
		return err
	}
	a.written++
	return nil
}

func (a *pgApplier) partialUpdateRow(row *model.RowChange, pkCols []string) error {
	tbl := quotePGTable(row.Table)
	isPK := make(map[string]bool, len(pkCols))
	for _, c := range pkCols {
		isPK[c] = true
	}
	var sets []string
	var args []any
	addSet := func(col string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s = $%d", quotePG(col), len(args)))
	}
	keyCond := func(key []model.Value) string {
		conds := make([]string, len(pkCols))
		for i, c := range pkCols {
			args = append(args, key[i])
			conds[i] = fmt.Sprintf("%s = $%d", quotePG(c), len(args))
		}
		return strings.Join(conds, " AND ")
	}

	pkChanged := len(row.OldKey) > 0 && keysDiffer(row.OldKey, row.Key)
	if !pkChanged {
		for _, c := range row.Cols {
			if c.Unchanged || isPK[c.Name] {
				continue
			}
			addSet(c.Name, c.Value)
		}
		if len(sets) == 0 {
			// Mọi cột không-PK đều Unchanged: không có gì để ghi, nhưng row vẫn phải tồn tại.
			var one int
			err := a.tx.QueryRow(a.ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE %s", tbl, keyCond(row.Key)), args...).Scan(&one)
			if errors.Is(err, pgx.ErrNoRows) {
				return newToastRowMissing(row.Table, row.Key)
			}
			if err != nil {
				return fmt.Errorf("partial update on %s: %w", row.Table, err)
			}
			return nil
		}
		q := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tbl, strings.Join(sets, ", "), keyCond(row.Key))
		tag, err := a.tx.Exec(a.ctx, q, args...)
		if err != nil {
			return fmt.Errorf("partial update on %s: %w", row.Table, err)
		}
		if tag.RowsAffected() != 1 {
			return newToastRowMissing(row.Table, row.Key)
		}
		return nil
	}

	// Đổi PK khi cột TOAST không đổi, range đã đọc/Xong (spec 02 §8):
	//   UPDATE t SET pk = :new, <các cột đổi> WHERE pk = :old AND NOT EXISTS (SELECT 1 FROM t WHERE pk = :new);
	//   DELETE FROM t WHERE pk = :old;
	// Trường hợp range "chưa đọc" do pipeline quyết định trước khi giao cho writer.
	for i, c := range pkCols {
		addSet(c, row.Key[i])
	}
	for _, c := range row.Cols {
		if c.Unchanged || isPK[c.Name] {
			continue
		}
		addSet(c.Name, c.Value)
	}
	oldCond := keyCond(row.OldKey)
	newCond := keyCond(row.Key)
	q := fmt.Sprintf("UPDATE %s SET %s WHERE %s AND NOT EXISTS (SELECT 1 FROM %s WHERE %s)",
		tbl, strings.Join(sets, ", "), oldCond, tbl, newCond)
	tag, err := a.tx.Exec(a.ctx, q, args...)
	if err != nil {
		return fmt.Errorf("partial pk-change update on %s: %w", row.Table, err)
	}
	if tag.RowsAffected() == 0 {
		// Hoặc PK mới đã có ở target (giữ nguyên, chỉ xóa PK cũ), hoặc thiếu row PK cũ để dựng row mới.
		args = args[:0]
		var one int
		err := a.tx.QueryRow(a.ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE %s", tbl, keyCond(row.Key)), args...).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return newToastRowMissing(row.Table, row.OldKey)
		}
		if err != nil {
			return fmt.Errorf("partial pk-change update on %s: %w", row.Table, err)
		}
	}
	return execDelete(a.ctx, a.tx, row.Table, pkCols, [][]model.Value{row.OldKey})
}

// tempTable trả tên bảng tạm cho bảng đích, tạo nếu chưa có trong transaction này.
// Một bảng tạm dùng lại cho mọi khối của cùng bảng đích trong transaction (TRUNCATE sau MERGE),
// tránh tạo/xóa catalog cho mỗi khối; ON COMMIT DROP dọn khi transaction kết thúc.
func (a *pgApplier) tempTable(t model.TableID) (string, error) {
	if name, ok := a.tmp[t]; ok {
		return name, nil
	}
	name := "_strand_tmp_" + strconv.Itoa(len(a.tmp)+1)
	q := fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", quotePG(name), quotePGTable(t))
	if _, err := a.tx.Exec(a.ctx, q); err != nil {
		return "", fmt.Errorf("create temp table for %s: %w", t, err)
	}
	a.tmp[t] = name
	return name, nil
}

// -----------------------------------------------------------------------------
// pgUpsertBlock: COPY vào bảng tạm rồi MERGE INTO (PG 15+)
// -----------------------------------------------------------------------------

// pgUpsertBlock là một khối row INSERT/UPDATE ảnh đầy đủ liên tiếp, cùng bảng, cùng tập cột.
//
// Cùng PK xuất hiện nhiều lần trong khối: CHỈ GIỮ ẢNH CUỐI (ghi đè tại chỗ). MERGE từ chối bảng
// nguồn có hai row khớp cùng một row đích, và giữa các lần xuất hiện không có DELETE/partial
// UPDATE/đổi PK nào (những thao tác đó cắt khối). Hai upsert ảnh đầy đủ cùng tập cột liên tiếp
// cho cùng PK tương đương upsert ảnh sau, nên trạng thái cuối giữ nguyên mà không phải tách khối
// (tách khối với khóa nóng sẽ thành nhiều MERGE nhỏ).
type pgUpsertBlock struct {
	table  model.TableID
	cols   []string
	pkCols []string
	rows   [][]any
	idx    map[string]int // khóa PK -> vị trí trong rows
	bytes  int            // tổng argsBytes của rows (review N13)
	// covered: số row nguồn đã vào khối, kể cả row có ảnh bị ảnh sau cùng khóa thay thế (F11).
	covered int64
}

// full cho biết row (bảng, tập cột, kích thước rowBytes) không được thêm vào khối này mà phải mở
// khối mới: khác bảng/tập cột, đủ số row, hoặc vượt ngân sách byte (review N13). Khối rỗng không bao
// giờ "đầy" theo byte, nên row lớn hơn ngân sách vẫn đi nguyên một mình.
func (b *pgUpsertBlock) full(table model.TableID, cols []string, rowBytes int) bool {
	return b.table != table || !sameStringSlice(b.cols, cols) || len(b.rows) >= pgCopyBlockRows ||
		(len(b.rows) > 0 && b.bytes+rowBytes > pgBlockMaxBytes)
}

func (b *pgUpsertBlock) add(key []model.Value, vals []any) {
	b.covered++
	k := pkString(key)
	if i, ok := b.idx[k]; ok {
		b.bytes += argsBytes(vals) - argsBytes(b.rows[i])
		b.rows[i] = vals
		return
	}
	b.idx[k] = len(b.rows)
	b.rows = append(b.rows, vals)
	b.bytes += argsBytes(vals)
}

// flush ghi khối. Khối lớn: COPY vào bảng tạm rồi MERGE (02 §6). Khối nhỏ (< copyMinRows):
// COPY + MERGE tốn 3–4 round-trip (pgx CopyFrom tự describe cột, rồi COPY, rồi MERGE) cho vài row,
// nên dùng một câu INSERT … ON CONFLICT (pk) DO UPDATE nhiều row (1 round-trip). Với ảnh đầy đủ
// đã khử trùng PK, hai cách cho cùng trạng thái cuối. Tải OLTP nhỏ xen nhiều bảng (khối 1–3 row)
// nếu luôn dùng COPY + MERGE sẽ chậm hơn nhiều lần so với upsert từng row.
func (b *pgUpsertBlock) flush(a *pgApplier) error {
	if len(b.rows) == 0 {
		return nil
	}
	if len(b.rows) < a.copyMinRows {
		return b.insertOnConflict(a)
	}
	tmp, err := a.tempTable(b.table)
	if err != nil {
		return err
	}
	if _, err := a.tx.CopyFrom(a.ctx, pgx.Identifier{"pg_temp", tmp}, b.cols, pgx.CopyFromRows(b.rows)); err != nil {
		return fmt.Errorf("copy into temp table for %s: %w", b.table, err)
	}

	isPK := make(map[string]bool, len(b.pkCols))
	on := make([]string, len(b.pkCols))
	for i, c := range b.pkCols {
		isPK[c] = true
		on[i] = fmt.Sprintf("_strand_t.%s = _strand_s.%s", quotePG(c), quotePG(c))
	}
	quoted := make([]string, len(b.cols))
	src := make([]string, len(b.cols))
	var sets []string
	for i, c := range b.cols {
		quoted[i] = quotePG(c)
		src[i] = "_strand_s." + quoted[i]
		if !isPK[c] {
			sets = append(sets, fmt.Sprintf("%s = _strand_s.%s", quoted[i], quoted[i]))
		}
	}
	matched := "WHEN MATCHED THEN DO NOTHING"
	if len(sets) > 0 {
		matched = "WHEN MATCHED THEN UPDATE SET " + strings.Join(sets, ", ")
	}
	tmpQ := "pg_temp." + quotePG(tmp)
	// MERGE và TRUNCATE bảng tạm gửi chung một round-trip (simple protocol, không tham số).
	q := fmt.Sprintf("MERGE INTO %s AS _strand_t USING %s AS _strand_s ON (%s) %s WHEN NOT MATCHED THEN INSERT (%s) OVERRIDING SYSTEM VALUE VALUES (%s); TRUNCATE %s",
		quotePGTable(b.table), tmpQ, strings.Join(on, " AND "), matched,
		strings.Join(quoted, ", "), strings.Join(src, ", "), tmpQ)
	if _, err := a.tx.Exec(a.ctx, q, pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("merge into %s: %w", b.table, err)
	}

	a.w.mu.Lock()
	a.w.MergeCount++
	a.w.TotalMergeRows += int64(len(b.rows))
	a.w.mu.Unlock()
	return nil
}

func (b *pgUpsertBlock) insertOnConflict(a *pgApplier) error {
	q, args := buildUpsert(b.table, b.cols, b.pkCols, b.rows)
	if _, err := a.tx.Exec(a.ctx, q, args...); err != nil {
		return fmt.Errorf("upsert into %s: %w", b.table, err)
	}
	return nil
}

// invariant: CP-1/2, CP-5 — OVERRIDING SYSTEM VALUE giữ ID tường minh từ source
// cho cả identity ALWAYS; không gọi nextval sinh ID khác. Không đổi ranh giới Txn,
// checkpoint, epoch hoặc thứ tự commit (SG-1, CO-1/2).
// buildUpsert dựng INSERT … VALUES (…),(…) ON CONFLICT (pk) DO UPDATE SET <cột không-PK>.
func buildUpsert(table model.TableID, cols, pkCols []string, rows [][]any) (string, []any) {
	isPK := make(map[string]bool, len(pkCols))
	quotedPK := make([]string, len(pkCols))
	for i, c := range pkCols {
		isPK[c] = true
		quotedPK[i] = quotePG(c)
	}
	quoted := make([]string, len(cols))
	var sets []string
	for i, c := range cols {
		quoted[i] = quotePG(c)
		if !isPK[c] {
			sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", quoted[i], quoted[i]))
		}
	}
	conflict := "DO NOTHING"
	if len(sets) > 0 {
		conflict = "DO UPDATE SET " + strings.Join(sets, ", ")
	}
	args := make([]any, 0, len(rows)*len(cols))
	tuples := make([]string, len(rows))
	for i, r := range rows {
		phs := make([]string, len(r))
		for j, v := range r {
			args = append(args, v)
			phs[j] = "$" + strconv.Itoa(len(args))
		}
		tuples[i] = "(" + strings.Join(phs, ", ") + ")"
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE VALUES %s ON CONFLICT (%s) %s", quotePGTable(table),
		strings.Join(quoted, ", "), strings.Join(tuples, ", "), strings.Join(quotedPK, ", "), conflict)
	return q, args
}

// -----------------------------------------------------------------------------
// pgDeleteBatch: DELETE … WHERE (pk) IN (…)
// -----------------------------------------------------------------------------

// pgDeleteBatch gom DELETE liên tiếp cùng bảng. Danh sách IN được đệm (lặp khóa cuối) tới lũy
// thừa của 2 để số câu lệnh khác nhau trong statement cache của pgx có giới hạn (≤ 11 mỗi bảng).
type pgDeleteBatch struct {
	table  model.TableID
	pkCols []string
	keys   [][]model.Value
}

func (b *pgDeleteBatch) maxKeys() int {
	n := pgDeleteMaxKeys
	for n > 1 && n*len(b.pkCols) > pgMaxParams {
		n /= 2
	}
	return n
}

func (b *pgDeleteBatch) flush(ctx context.Context, tx pgx.Tx) error {
	if len(b.keys) == 0 {
		return nil
	}
	return execDelete(ctx, tx, b.table, b.pkCols, b.keys)
}

func execDelete(ctx context.Context, tx pgx.Tx, t model.TableID, pkCols []string, keys [][]model.Value) error {
	q, args, err := buildDelete(t, pkCols, keys)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("delete on %s: %w", t, err)
	}
	return nil
}

// buildDelete dựng DELETE … WHERE (pk) IN (…), danh sách IN đệm tới lũy thừa của 2.
// Không có khóa nào (hoặc khóa không đủ cột) là lỗi: keys[len(keys)-1] sẽ panic, và DELETE rỗng là dấu
// hiệu lỗi ở phía gọi chứ không phải thao tác hợp lệ.
func buildDelete(t model.TableID, pkCols []string, keys [][]model.Value) (string, []any, error) {
	if len(keys) == 0 {
		return "", nil, fmt.Errorf("delete on %s: no keys", t)
	}
	if len(pkCols) == 0 {
		return "", nil, fmt.Errorf("delete on %s: no primary key columns", t)
	}
	for _, k := range keys {
		if len(k) != len(pkCols) {
			return "", nil, fmt.Errorf("delete on %s: key has %d value(s), primary key has %d column(s)", t, len(k), len(pkCols))
		}
	}
	n := 1
	for n < len(keys) {
		n *= 2
	}
	args := make([]any, 0, n*len(pkCols))
	tuples := make([]string, n)
	for i := 0; i < n; i++ {
		k := keys[len(keys)-1]
		if i < len(keys) {
			k = keys[i]
		}
		phs := make([]string, len(pkCols))
		for j := range pkCols {
			args = append(args, k[j])
			phs[j] = "$" + strconv.Itoa(len(args))
		}
		tuples[i] = "(" + strings.Join(phs, ", ") + ")"
	}
	quoted := make([]string, len(pkCols))
	for i, c := range pkCols {
		quoted[i] = quotePG(c)
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)", quotePGTable(t), strings.Join(quoted, ", "), strings.Join(tuples, ", "))
	return q, args, nil
}

// -----------------------------------------------------------------------------
// Tiện ích
// -----------------------------------------------------------------------------

// pkString mã hóa khóa thành chuỗi không nhập nhằng (độ dài + kiểu + giá trị cho từng phần).
func pkString(key []model.Value) string {
	var sb strings.Builder
	for _, v := range key {
		s := fmt.Sprintf("%T:%v", v, v)
		sb.WriteString(strconv.Itoa(len(s)))
		sb.WriteByte('|')
		sb.WriteString(s)
	}
	return sb.String()
}

func containsString(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
