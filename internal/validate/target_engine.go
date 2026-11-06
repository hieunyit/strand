package validate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// review R26: dữ liệu và checkpoint được commit trong cùng một transaction trên target (CP-1, 02 §4).
// Trên MySQL điều đó chỉ đúng khi mọi bảng đích và mọi bảng _strand_* dùng engine giao dịch InnoDB;
// MyISAM/MEMORY/ARCHIVE... ghi ngay không rollback được, crash giữa chừng để lại dữ liệu đã ghi mà
// checkpoint không ghi nhận (hoặc ngược lại). Bảng như vậy bị từ chối từ đầu (exit 3).

// TableEngine là một dòng information_schema.TABLES: Engine rỗng = NULL (view, hoặc không xác định).
type TableEngine struct {
	Schema string
	Table  string
	Engine string
}

// strandMetaPrefix: tiền tố bảng metadata của Strand trên target (_strand_pipeline, _strand_checkpoint,
// _strand_worker_delta, _strand_promote...).
const strandMetaPrefix = "_strand_"

// selectTargetEngines lọc các dòng cần kiểm: bảng đích trong want (so khớp chính xác, rồi không phân
// biệt hoa thường cho lower_case_table_names) và mọi bảng _strand_* trong metaSchema.
// Bảng đích chưa có trên target không được báo ở đây (bước tạo schema/kiểm tồn tại lo phần đó).
func selectTargetEngines(want []model.TableID, metaSchema string, rows []TableEngine) []TableEngine {
	exact := make(map[model.TableID]bool, len(want))
	folded := make(map[string]bool, len(want))
	for _, t := range want {
		exact[t] = true
		folded[strings.ToLower(t.String())] = true
	}
	var out []TableEngine
	for _, r := range rows {
		id := model.TableID{Schema: r.Schema, Table: r.Table}
		isMeta := metaSchema != "" && r.Schema == metaSchema && strings.HasPrefix(strings.ToLower(r.Table), strandMetaPrefix)
		if exact[id] || folded[strings.ToLower(id.String())] || isMeta {
			out = append(out, r)
		}
	}
	return out
}

// TargetEngineError trả lỗi target_not_transactional (exit 3) nếu có dòng nào không phải InnoDB; nil nếu
// mọi bảng đều InnoDB. Hàm thuần, không truy cập DB.
func TargetEngineError(rows []TableEngine) error {
	var bad []string
	for _, r := range rows {
		if !strings.EqualFold(strings.TrimSpace(r.Engine), "InnoDB") {
			engine := r.Engine
			if engine == "" {
				engine = "NULL"
			}
			bad = append(bad, fmt.Sprintf("%s.%s (ENGINE=%s)", r.Schema, r.Table, engine))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad)
	return exitcode.New(exitcode.ErrTargetNotTransactional, exitcode.Rejected,
		fmt.Sprintf("target tables must use the transactional InnoDB engine so data and checkpoint commit atomically: %s; "+
			"convert with ALTER TABLE ... ENGINE=InnoDB", strings.Join(bad, ", ")), nil)
}

// CheckMySQLTargetEngines đọc ENGINE của các bảng đích (tên SAU rename) và của mọi bảng _strand_* trong
// database mặc định của kết nối target, rồi từ chối nếu có bảng không phải InnoDB (review R26).
func CheckMySQLTargetEngines(ctx context.Context, db *sql.DB, targets []model.TableID) error {
	var metaSchema sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&metaSchema); err != nil {
		return fmt.Errorf("read target default database: %w", err)
	}
	schemas := map[string]bool{}
	for _, t := range targets {
		schemas[t.Schema] = true
	}
	if metaSchema.Valid {
		schemas[metaSchema.String] = true
	}
	if len(schemas) == 0 {
		return nil
	}
	args := make([]any, 0, len(schemas))
	for s := range schemas {
		args = append(args, s)
	}
	q := "SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA IN (?" +
		strings.Repeat(",?", len(args)-1) + ")"
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("read target table engines: %w", err)
	}
	defer rows.Close()
	var all []TableEngine
	for rows.Next() {
		var r TableEngine
		var engine sql.NullString
		if err := rows.Scan(&r.Schema, &r.Table, &engine); err != nil {
			return err
		}
		r.Engine = engine.String
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return TargetEngineError(selectTargetEngines(targets, metaSchema.String, all))
}
