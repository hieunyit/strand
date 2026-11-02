package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// spec: 02 §3, §4 quy tắc 3, 6; invariant: CP-1, SG-1

// DeltaRow đại diện cho dữ liệu delta của một worker tại thời điểm commit batch.
// spec: 02 §3
type DeltaRow struct {
	PipelineID string
	WorkerID   int
	Epoch      int64
	BatchNo    int64
	Applied    string

	// Global (chế độ strict, 02 §4 quy tắc 5): không ghi row delta; thay vào đó ghi row toàn cục
	// trong chính transaction commit, có điều kiện epoch. Hàm được gọi SAU khi đến lượt commit
	// (mọi batch có seq nhỏ hơn đã commit), trả chuỗi applied mới của row toàn cục.
	Global func() (string, error)
}

// Writer là interface áp dụng các batch transaction vào target database.
// invariant: CP-1, SG-1
type Writer interface {
	// ApplyBatch áp dụng batch transaction cùng row delta trong cùng một transaction ở target (CP-1).
	ApplyBatch(ctx context.Context, batch []*model.Txn, delta DeltaRow) error
}

// -----------------------------------------------------------------------------
// Khóa chính và quote identifier
// -----------------------------------------------------------------------------

// pkRegistry lưu tên các cột khóa chính theo bảng. Writer không đoán khóa chính:
// bảng chưa đăng ký chỉ được chấp nhận khi khóa có đúng 1 cột (mặc định tên "id",
// giữ tương thích cũ); khóa nhiều cột mà chưa đăng ký thì từ chối.
type pkRegistry struct {
	mu  sync.RWMutex
	pks map[model.TableID][]string
}

func (r *pkRegistry) set(t model.TableID, cols []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pks == nil {
		r.pks = make(map[model.TableID][]string)
	}
	r.pks[t] = append([]string(nil), cols...)
}

// registered trả về khóa chính đã đăng ký (nil nếu chưa).
func (r *pkRegistry) registered(t model.TableID) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pks[t]
}

func (r *pkRegistry) get(t model.TableID, nKey int) ([]string, error) {
	r.mu.RLock()
	cols, ok := r.pks[t]
	r.mu.RUnlock()
	if ok {
		if nKey != len(cols) {
			return nil, fmt.Errorf("table %s: key has %d values but primary key has %d columns", t, nKey, len(cols))
		}
		return cols, nil
	}
	if nKey <= 1 {
		return []string{"id"}, nil
	}
	return nil, fmt.Errorf("table %s: primary key columns unknown for %d-column key; register with SetPrimaryKey", t, nKey)
}

func quoteMySQL(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quoteMySQLTable(t model.TableID) string {
	if t.Schema == "" {
		return quoteMySQL(t.Table)
	}
	return quoteMySQL(t.Schema) + "." + quoteMySQL(t.Table)
}

func quotePG(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quotePGTable(t model.TableID) string {
	if t.Schema == "" {
		return quotePG(t.Table)
	}
	return quotePG(t.Schema) + "." + quotePG(t.Table)
}

func toArgs(vs []model.Value) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = v
	}
	return out
}

func hasUnchanged(rc *model.RowChange) bool {
	for _, c := range rc.Cols {
		if c.Unchanged {
			return true
		}
	}
	return false
}

func keysDiffer(a, b []model.Value) bool {
	if len(a) != len(b) {
		return true
	}
	for i := range a {
		if fmt.Sprint(a[i]) != fmt.Sprint(b[i]) {
			return true
		}
	}
	return false
}

// validateStrictBatch kiểm tra batch ở chế độ strict gồm các seq liên tiếp (CP-5).
func validateStrictBatch(batch []*model.Txn) error {
	for i, t := range batch {
		if t.Seq() == 0 {
			return fmt.Errorf("strict commit order: txn %v has no seq", t.SourcePos)
		}
		if i > 0 && t.Seq() != batch[i-1].Seq()+1 {
			return fmt.Errorf("strict commit order: batch seqs not consecutive (%d then %d)", batch[i-1].Seq(), t.Seq())
		}
	}
	return nil
}

func resolveDelta(delta DeltaRow, pipelineID string, workerID int, epoch int64) (string, int, int64) {
	pid, wid, ep := delta.PipelineID, delta.WorkerID, delta.Epoch
	if pid == "" {
		pid = pipelineID
	}
	if wid == 0 {
		wid = workerID
	}
	if ep == 0 {
		ep = epoch
	}
	return pid, wid, ep
}

// -----------------------------------------------------------------------------
// MySQLWriter
// -----------------------------------------------------------------------------

// MySQLWriter cài đặt Writer cho MySQL target.
type MySQLWriter struct {
	db                  *sql.DB
	pipelineID          string
	workerID            int
	epoch               int64
	orderer             *CommitOrderer
	pk                  pkRegistry
	mu                  sync.Mutex
	MultiRowUpsertCount int64
	TotalUpsertRows     int64
}

// NewMySQLWriter khởi tạo MySQLWriter mới.
func NewMySQLWriter(db *sql.DB, pipelineID string, workerID int, epoch int64) *MySQLWriter {
	return &MySQLWriter{
		db:         db,
		pipelineID: pipelineID,
		workerID:   workerID,
		epoch:      epoch,
	}
}

// SetPrimaryKey khai báo tên các cột khóa chính của bảng (theo thứ tự của RowChange.Key).
func (w *MySQLWriter) SetPrimaryKey(t model.TableID, cols []string) { w.pk.set(t, cols) }

// SetCommitOrderer gán CommitOrderer để kiểm soát thứ tự commit (CP-5).
func (w *MySQLWriter) SetCommitOrderer(orderer *CommitOrderer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.orderer = orderer
}

// GetStats trả về số lượng câu lệnh multi-row upsert và tổng số row đã upsert.
func (w *MySQLWriter) GetStats() (multiRowUpserts, totalUpsertRows int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.MultiRowUpsertCount, w.TotalUpsertRows
}

// EnsureWorkerRow đảm bảo row cho worker_id đã tồn tại trong _strand_worker_delta.
// Không bao giờ hạ epoch: instance có epoch cũ hơn row hiện tại nhận lỗi fenced (SG-1).
func (w *MySQLWriter) EnsureWorkerRow(ctx context.Context) error {
	query := `INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied)
	VALUES (?, ?, ?, 1, '')
	ON DUPLICATE KEY UPDATE epoch = GREATEST(epoch, VALUES(epoch))`
	if _, err := w.db.ExecContext(ctx, query, w.pipelineID, w.workerID, w.epoch); err != nil {
		return fmt.Errorf("failed to ensure mysql worker row: %w", err)
	}
	var cur int64
	if err := w.db.QueryRowContext(ctx,
		"SELECT epoch FROM _strand_worker_delta WHERE pipeline_id = ? AND worker_id = ?",
		w.pipelineID, w.workerID).Scan(&cur); err != nil {
		return fmt.Errorf("failed to read mysql worker row epoch: %w", err)
	}
	if cur != w.epoch {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced,
			fmt.Sprintf("worker %d fenced: row epoch %d, instance epoch %d", w.workerID, cur, w.epoch), nil)
	}
	return nil
}

// ApplyBatch ghi dữ liệu của batch và cập nhật row delta trong cùng transaction.
// spec: 02 §4 quy tắc 3: nếu số row bị ảnh hưởng khác 1 thì rollback và trả về lỗi fenced.
func (w *MySQLWriter) ApplyBatch(ctx context.Context, batch []*model.Txn, delta DeltaRow) (err error) {
	w.mu.Lock()
	orderer := w.orderer
	w.mu.Unlock()

	strict := orderer != nil && orderer.Mode() == "strict" && len(batch) > 0
	if strict {
		if err := validateStrictBatch(batch); err != nil {
			return err
		}
		// Chờ đến lượt TRƯỚC khi mở transaction (CP-5). Nếu thực thi câu lệnh trước rồi mới chờ,
		// batch đang chờ giữ row/gap lock mà batch seq nhỏ hơn cần: seq nhỏ chờ lock, seq lớn chờ
		// lượt — InnoDB không thấy vòng chờ này (chỉ hết hạn lock wait rồi lặp lại).
		if err := orderer.WaitTurn(ctx, batch[0].Seq()); err != nil {
			return err
		}
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	committing := false

	// 1. Ghi dữ liệu vào các bảng ứng dụng (kể cả Txn đã tràn đĩa)
	for _, txn := range batch {
		if err := w.applyTxnMySQL(ctx, tx, txn); err != nil {
			return fmt.Errorf("failed to apply row changes: %w", err)
		}
	}

	pid, wid, ep := resolveDelta(delta, w.pipelineID, w.workerID, w.epoch)
	if delta.Global == nil {
		// 2. Ghi row delta có điều kiện epoch trong CÙNG transaction (CP-1)
		query := "UPDATE _strand_worker_delta SET applied = ?, batch_no = batch_no + 1 WHERE pipeline_id = ? AND worker_id = ? AND epoch = ?"
		res, err := tx.ExecContext(ctx, query, delta.Applied, pid, wid, ep)
		if err != nil {
			return fmt.Errorf("failed to update worker delta: %w", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		if affected != 1 {
			// spec: 02 §4 quy tắc 3: số row != 1 -> rollback và dừng với mã fenced
			return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("writer fenced: expected 1 row affected, got %d", affected), nil)
		}
	}

	// 3. Chờ đến lượt commit ở chế độ strict (CP-5)
	if strict {
		// Lỗi TRƯỚC commit: batch chưa commit, lượt vẫn thuộc về nó (caller có thể retry cùng seq).
		// Lỗi KHI commit: kết quả không chắc chắn, dừng orderer để không seq nào commit sau nó.
		defer func() {
			if err != nil && committing {
				orderer.Fail(err)
			}
		}()
	}

	// 3b. Strict, quy tắc 5: row toàn cục ghi trong transaction commit của batch, có điều kiện epoch.
	if delta.Global != nil {
		applied, gerr := delta.Global()
		if gerr != nil {
			return gerr
		}
		res, gerr := tx.ExecContext(ctx,
			"UPDATE _strand_checkpoint SET applied = ?, updated_at = NOW(6) WHERE pipeline_id = ? AND epoch = ?",
			applied, pid, ep)
		if gerr != nil {
			return fmt.Errorf("failed to update global checkpoint: %w", gerr)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("writer fenced: global checkpoint update affected %d rows", n), nil)
		}
	}

	// 4. Commit cả dữ liệu và delta
	committing = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	// 5. Thông báo commit xong cho orderer (CP-5)
	if strict {
		orderer.CommitDone(batch[len(batch)-1].Seq())
	}
	return nil
}

// mysqlUpsertBatch gom các upsert liên tiếp cùng bảng, cùng tập cột thành một câu multi-row.
type mysqlUpsertBatch struct {
	w      *MySQLWriter
	table  model.TableID
	cols   []string
	pkCols []string
	vals   [][]any
}

func (b *mysqlUpsertBatch) flush(ctx context.Context, tx *sql.Tx) error {
	if len(b.vals) == 0 {
		return nil
	}
	b.w.mu.Lock()
	if len(b.vals) > 1 {
		b.w.MultiRowUpsertCount++
	}
	b.w.TotalUpsertRows += int64(len(b.vals))
	b.w.mu.Unlock()

	isPK := make(map[string]bool, len(b.pkCols))
	for _, c := range b.pkCols {
		isPK[c] = true
	}
	quoted := make([]string, len(b.cols))
	var updates []string
	for i, c := range b.cols {
		quoted[i] = quoteMySQL(c)
		if !isPK[c] {
			updates = append(updates, fmt.Sprintf("%s=VALUES(%s)", quoted[i], quoted[i]))
		}
	}
	if len(updates) == 0 {
		updates = append(updates, fmt.Sprintf("%s=%s", quoted[0], quoted[0]))
	}
	rowPH := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(b.cols)), ", ") + ")"
	phs := make([]string, len(b.vals))
	args := make([]any, 0, len(b.vals)*len(b.cols))
	for i, v := range b.vals {
		phs[i] = rowPH
		args = append(args, v...)
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		quoteMySQLTable(b.table), strings.Join(quoted, ", "), strings.Join(phs, ", "), strings.Join(updates, ", "))
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("multi-row upsert on %s: %w", b.table, err)
	}
	b.vals = nil
	return nil
}

func (w *MySQLWriter) deleteByKey(ctx context.Context, tx *sql.Tx, t model.TableID, pkCols []string, key []model.Value) error {
	conds := make([]string, len(pkCols))
	for i, c := range pkCols {
		conds[i] = quoteMySQL(c) + " = ?"
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE %s", quoteMySQLTable(t), strings.Join(conds, " AND "))
	_, err := tx.ExecContext(ctx, q, toArgs(key)...)
	return err
}

// applyTxnMySQL áp dụng mọi row của Txn. Với row image FULL (bắt buộc ở source MySQL,
// spec 02 §8), INSERT và UPDATE đều là upsert theo khóa chính, nên UPDATE của row
// chưa có ở target không bị mất im lặng. UPDATE đổi PK = DELETE khóa cũ + upsert khóa mới.
func (w *MySQLWriter) applyTxnMySQL(ctx context.Context, tx *sql.Tx, txn *model.Txn) error {
	var cur *mysqlUpsertBatch
	flush := func() error {
		if cur == nil {
			return nil
		}
		err := cur.flush(ctx, tx)
		cur = nil
		return err
	}

	err := txn.ForEachRow(func(row *model.RowChange) error {
		switch row.Op {
		case model.OpInsert, model.OpUpdate:
			// Upsert MySQL dựa trên mọi unique key của bảng; tên cột PK chỉ cần để
			// loại khỏi danh sách UPDATE (nếu đã đăng ký) và cho DELETE khi đổi PK.
			pkCols := w.pk.registered(row.Table)
			if len(row.Cols) == 0 {
				return fmt.Errorf("table %s: %s without column values", row.Table, row.Op)
			}
			if hasUnchanged(row) {
				return fmt.Errorf("table %s: partial row image is not supported on a MySQL target", row.Table)
			}
			if row.Op == model.OpUpdate && len(row.OldKey) > 0 && keysDiffer(row.OldKey, row.Key) {
				if err := flush(); err != nil {
					return err
				}
				delCols, err := w.pk.get(row.Table, len(row.OldKey))
				if err != nil {
					return err
				}
				if err := w.deleteByKey(ctx, tx, row.Table, delCols, row.OldKey); err != nil {
					return err
				}
			}
			names := make([]string, len(row.Cols))
			vals := make([]any, len(row.Cols))
			for i, c := range row.Cols {
				names[i] = c.Name
				vals[i] = c.Value
			}
			if cur != nil && (cur.table != row.Table || !sameStringSlice(cur.cols, names) || len(cur.vals) >= 500) {
				if err := flush(); err != nil {
					return err
				}
			}
			if cur == nil {
				cur = &mysqlUpsertBatch{w: w, table: row.Table, cols: names, pkCols: pkCols}
			}
			cur.vals = append(cur.vals, vals)
			return nil
		case model.OpDelete:
			if err := flush(); err != nil {
				return err
			}
			pkCols, err := w.pk.get(row.Table, len(row.Key))
			if err != nil {
				return err
			}
			return w.deleteByKey(ctx, tx, row.Table, pkCols, row.Key)
		default:
			return fmt.Errorf("table %s: unknown op %q", row.Table, row.Op)
		}
	})
	if err != nil {
		return err
	}
	return flush()
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// PGWriter
// -----------------------------------------------------------------------------

// PGWriter cài đặt Writer cho PostgreSQL target.
type PGWriter struct {
	db         *sql.DB
	pipelineID string
	workerID   int
	epoch      int64
	pk         pkRegistry
	mu         sync.Mutex
	orderer    *CommitOrderer
}

// NewPGWriter khởi tạo PGWriter mới.
func NewPGWriter(db *sql.DB, pipelineID string, workerID int, epoch int64) *PGWriter {
	return &PGWriter{
		db:         db,
		pipelineID: pipelineID,
		workerID:   workerID,
		epoch:      epoch,
	}
}

// SetPrimaryKey khai báo tên các cột khóa chính của bảng (theo thứ tự của RowChange.Key).
func (w *PGWriter) SetPrimaryKey(t model.TableID, cols []string) { w.pk.set(t, cols) }

// SetCommitOrderer gán CommitOrderer để kiểm soát thứ tự commit (CP-5).
func (w *PGWriter) SetCommitOrderer(orderer *CommitOrderer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.orderer = orderer
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

// ApplyBatch ghi dữ liệu của batch và cập nhật row delta trong cùng transaction trên Postgres.
func (w *PGWriter) ApplyBatch(ctx context.Context, batch []*model.Txn, delta DeltaRow) (err error) {
	w.mu.Lock()
	orderer := w.orderer
	w.mu.Unlock()
	strict := orderer != nil && orderer.Mode() == "strict" && len(batch) > 0
	if strict {
		if err := validateStrictBatch(batch); err != nil {
			return err
		}
		// Chờ đến lượt TRƯỚC khi mở transaction (CP-5). Nếu thực thi câu lệnh trước rồi mới chờ,
		// batch đang chờ giữ row/gap lock mà batch seq nhỏ hơn cần: seq nhỏ chờ lock, seq lớn chờ
		// lượt — InnoDB không thấy vòng chờ này (chỉ hết hạn lock wait rồi lặp lại).
		if err := orderer.WaitTurn(ctx, batch[0].Seq()); err != nil {
			return err
		}
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	committing := false

	for _, txn := range batch {
		if err := w.applyTxnPG(ctx, tx, txn); err != nil {
			return fmt.Errorf("failed to apply row changes: %w", err)
		}
	}

	pid, wid, ep := resolveDelta(delta, w.pipelineID, w.workerID, w.epoch)
	query := "UPDATE _strand_worker_delta SET applied = $1, batch_no = batch_no + 1 WHERE pipeline_id = $2 AND worker_id = $3 AND epoch = $4"
	res, err := tx.ExecContext(ctx, query, delta.Applied, pid, wid, ep)
	if err != nil {
		return fmt.Errorf("failed to update pg worker delta: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if affected != 1 {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("writer fenced: expected 1 row affected, got %d", affected), nil)
	}

	if strict {
		defer func() {
			if err != nil && committing {
				orderer.Fail(err)
			}
		}()
	}

	committing = true
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}
	if strict {
		orderer.CommitDone(batch[len(batch)-1].Seq())
	}
	return nil
}

func (w *PGWriter) applyTxnPG(ctx context.Context, tx *sql.Tx, txn *model.Txn) error {
	return txn.ForEachRow(func(row *model.RowChange) error {
		pkCols, err := w.pk.get(row.Table, len(row.Key))
		if err != nil {
			return err
		}
		tbl := quotePGTable(row.Table)
		where := func(start int) string {
			conds := make([]string, len(pkCols))
			for i, c := range pkCols {
				conds[i] = fmt.Sprintf("%s = $%d", quotePG(c), start+i)
			}
			return strings.Join(conds, " AND ")
		}

		switch row.Op {
		case model.OpDelete:
			_, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s", tbl, where(1)), toArgs(row.Key)...)
			return err

		case model.OpInsert, model.OpUpdate:
			if len(row.Cols) == 0 {
				return fmt.Errorf("table %s: %s without column values", row.Table, row.Op)
			}
			oldKey := row.Key
			pkChanged := row.Op == model.OpUpdate && len(row.OldKey) > 0 && keysDiffer(row.OldKey, row.Key)
			if pkChanged {
				oldKey = row.OldKey
			}

			if row.Op == model.OpUpdate && hasUnchanged(row) {
				// Partial UPDATE (TOAST, spec 02 §8): cột unchanged không nằm trong SET.
				// Row phải tồn tại ở target, nếu không thì báo lỗi (không ghi đè NULL, không mất im lặng).
				var sets []string
				var args []any
				for _, c := range row.Cols {
					if c.Unchanged {
						continue
					}
					args = append(args, c.Value)
					sets = append(sets, fmt.Sprintf("%s = $%d", quotePG(c.Name), len(args)))
				}
				if len(sets) == 0 {
					return nil
				}
				q := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tbl, strings.Join(sets, ", "), where(len(args)+1))
				args = append(args, toArgs(oldKey)...)
				res, err := tx.ExecContext(ctx, q, args...)
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if n != 1 {
					return fmt.Errorf("table %s: partial update matched %d rows (row missing at target)", row.Table, n)
				}
				return nil
			}

			if pkChanged {
				if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s", tbl, where(1)), toArgs(row.OldKey)...); err != nil {
					return err
				}
			}
			isPK := make(map[string]bool, len(pkCols))
			quotedPK := make([]string, len(pkCols))
			for i, c := range pkCols {
				isPK[c] = true
				quotedPK[i] = quotePG(c)
			}
			names := make([]string, len(row.Cols))
			phs := make([]string, len(row.Cols))
			vals := make([]any, len(row.Cols))
			var updates []string
			for i, c := range row.Cols {
				names[i] = quotePG(c.Name)
				phs[i] = fmt.Sprintf("$%d", i+1)
				vals[i] = c.Value
				if !isPK[c.Name] {
					updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", names[i], names[i]))
				}
			}
			conflict := "DO NOTHING"
			if len(updates) > 0 {
				conflict = "DO UPDATE SET " + strings.Join(updates, ", ")
			}
			q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) %s",
				tbl, strings.Join(names, ", "), strings.Join(phs, ", "), strings.Join(quotedPK, ", "), conflict)
			_, err := tx.ExecContext(ctx, q, vals...)
			return err
		default:
			return fmt.Errorf("table %s: unknown op %q", row.Table, row.Op)
		}
	})
}
