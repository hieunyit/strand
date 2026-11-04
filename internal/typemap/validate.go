package typemap

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/exitcode"
)

// spec: 02 §8, §11, §12; TASK P2-04

// Warning đại diện cho cảnh báo trong quá trình validate.
type Warning = string

// CheckMySQLToPGTables quét và kiểm tra tính tương thích giữa bảng nguồn MySQL và đích PostgreSQL:
//  1. Kiểu không map được (type_unmappable).
//  2. Collation của cột khóa (trả collation_looser khi target lỏng hơn source hoặc dùng citext).
//  3. Quét giá trị thực tế gây lỗi (zero date, unsigned bigint vượt int8 khi dùng int8-checked, ENUM ngoài danh sách)
//     bằng truy vấn đếm có giới hạn (LIMIT) nhằm tránh quét toàn bảng khi không cần thiết.
func CheckMySQLToPGTables(ctx context.Context, src *sql.DB, dst *sql.DB, tables []string, opts Options) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if src == nil || dst == nil {
		return nil, fmt.Errorf("CheckMySQLToPGTables: source and target connections are required")
	}
	if opts.ScanLimit <= 0 {
		opts.ScanLimit = 1000
	}
	if opts.ZeroDatePolicy == "" {
		opts.ZeroDatePolicy = "reject"
	}
	if opts.BigIntUnsignedPolicy == "" {
		opts.BigIntUnsignedPolicy = "numeric"
	}
	if opts.EnumPolicy == "" {
		opts.EnumPolicy = "check"
	}

	var warnings []string

	// Lấy schema mặc định của source nếu bảng không chỉ định schema
	var defaultSourceSchema string
	_ = src.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&defaultSourceSchema)

	for _, fullTable := range tables {
		select {
		case <-ctx.Done():
			return warnings, ctx.Err()
		default:
		}

		schema, table, hasSchema := strings.Cut(fullTable, ".")
		if !hasSchema {
			table = schema
			schema = defaultSourceSchema
		}
		if schema == "" {
			schema = "strand"
		}

		// 1. Kiểm tra tồn tại bảng nguồn
		var exists int
		err := src.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			schema, table).Scan(&exists)
		if err != nil {
			return warnings, fmt.Errorf("check table %s.%s existence: %w", schema, table, err)
		}
		if exists == 0 {
			return warnings, exitcode.New(
				exitcode.ErrBinlogConfigInvalid,
				exitcode.Rejected,
				fmt.Sprintf("table %s.%s does not exist on source", schema, table),
				nil,
			)
		}

		// 2. Đọc metadata các cột từ source
		cols, err := InspectMySQLColumns(ctx, src, schema, table)
		if err != nil {
			return warnings, err
		}

		// 3. Đọc khóa chính từ source
		pks, err := InspectMySQLPrimaryKey(ctx, src, schema, table)
		if err != nil {
			return warnings, err
		}
		if len(pks) == 0 {
			return warnings, exitcode.New(
				exitcode.ErrMissingPK,
				exitcode.Rejected,
				fmt.Sprintf("table %s.%s has no primary key (spec: 02 §8)", schema, table),
				nil,
			)
		}
		pkMap := make(map[string]bool, len(pks))
		for _, pk := range pks {
			pkMap[pk] = true
		}

		colMap := make(map[string]MySQLColumn, len(cols))
		for _, col := range cols {
			colMap[col.Name] = col

			// 4. Kiểm tra kiểu không map được (type_unmappable)
			mapping, mapErr := MapMySQLColumn(col, opts)
			if mapErr != nil {
				return warnings, mapErr
			}

			// Thêm cảnh báo thông tin nếu cần
			if col.DataType == "bigint" && col.Unsigned && opts.BigIntUnsignedPolicy == "numeric" {
				warnings = append(warnings, fmt.Sprintf("table %s.%s column %s: unsigned bigint mapped to numeric(20,0); consider int8-checked if values fit in int8", schema, table, col.Name))
			}
			if col.DataType == "time" && opts.TimePolicy == "time" {
				warnings = append(warnings, fmt.Sprintf("table %s.%s column %s: time mapped to time(without time zone); values outside 00:00:00-24:00:00 require interval policy", schema, table, col.Name))
			}
			_ = mapping
		}

		// 5. Kiểm tra target PostgreSQL (nếu có kết nối target)
		if dst != nil {
			// Xác định target schema: ưu tiên schema gốc, sau đó thử "public"
			targetSchema := schema
			var dstTableExists int
			err := dst.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
				targetSchema, table).Scan(&dstTableExists)
			if err != nil {
				// Không nuốt lỗi: lỗi ở đây trước kia dẫn tới bỏ qua toàn bộ kiểm tra collation target.
				return warnings, fmt.Errorf("check target table %s.%s existence: %w", targetSchema, table, err)
			}
			if dstTableExists == 0 {
				targetSchema = "public"
				if err := dst.QueryRowContext(ctx,
					"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
					targetSchema, table).Scan(&dstTableExists); err != nil {
					return warnings, fmt.Errorf("check target table %s.%s existence: %w", targetSchema, table, err)
				}
			}

			if dstTableExists > 0 {
				dstCols, err := InspectPGColumns(ctx, dst, targetSchema, table)
				if err != nil {
					return warnings, err
				}
				dstPKs, err := InspectPGPrimaryKey(ctx, dst, targetSchema, table)
				if err != nil {
					return warnings, err
				}
				dstPKMap := make(map[string]bool, len(dstPKs))
				for _, pk := range dstPKs {
					dstPKMap[pk] = true
				}

				for _, dc := range dstCols {
					// Nếu cột này thuộc khóa chính của target hoặc source
					if dstPKMap[dc.Name] || pkMap[dc.Name] {
						// a) Cấm citext trên cột khóa
						if strings.EqualFold(dc.DataType, "citext") || strings.EqualFold(dc.UDTName, "citext") {
							return warnings, exitcode.New(
								exitcode.ErrCollationLooser,
								exitcode.Rejected,
								fmt.Sprintf("table %s.%s key column %s on target uses citext: citext is forbidden for key columns (spec: 02 §8)", targetSchema, table, dc.Name),
								nil,
							)
						}

						// b) Kiểm tra collation deterministic trong PG
						// Resolve theo pg_attribute.attcollation (OID) chứ không theo tên: tên collation có thể
						// trùng giữa các schema, tra theo tên + LIMIT 1 có thể chọn nhầm bản deterministic.
						// Luôn kiểm tra (kể cả khi information_schema.collation_name rỗng, ví dụ cột kiểu domain
						// mang collation của domain): attcollation là collation hiệu lực của cột.
						{
							cm, err := InspectPGColumnCollation(ctx, dst, targetSchema, table, dc.Name)
							if err != nil {
								// Không được bỏ qua: không xác định được collation thì không kết luận "an toàn".
								return warnings, err
							}
							if cm != nil {
								if !cm.IsDeterministic {
									return warnings, exitcode.New(
										exitcode.ErrCollationLooser,
										exitcode.Rejected,
										fmt.Sprintf("table %s.%s key column %s on target uses non-deterministic collation %s (spec: 02 §8)", targetSchema, table, dc.Name, cm.Name),
										nil,
									)
								}
								if isPGCaseInsensitive(cm.Name) || isPGCaseInsensitive(cm.ICULocale) {
									return warnings, exitcode.New(
										exitcode.ErrCollationLooser,
										exitcode.Rejected,
										fmt.Sprintf("table %s.%s key column %s on target uses case-insensitive collation %s (spec: 02 §8)", targetSchema, table, dc.Name, cm.Name),
										nil,
									)
								}
							}
						}

						// c) Đối chiếu collation source -> target
						srcCol, ok := colMap[dc.Name]
						if ok && srcCol.Collation != "" && dc.CollationName != "" {
							if err := CheckKeyCollation(srcCol.Collation, dc.CollationName); err != nil {
								return warnings, err
							}
						}
					}
				}
			} else {
				warnings = append(warnings, fmt.Sprintf("table %s not found on target database, skipping target collation checks", table))
			}
		}

		// 6. Quét giá trị thực tế gây lỗi trên source MySQL (LIMIT)
		for _, col := range cols {
			// 6.1. Zero Date: khi policy là reject (mặc định)
			if (col.DataType == "date" || col.DataType == "datetime" || col.DataType == "timestamp") && opts.ZeroDatePolicy == "reject" {
				// Chỉ so sánh trên chuỗi CAST(col AS CHAR): KHÔNG dùng literal '0000-00-00' vì dưới sql_mode
				// mặc định (strict + NO_ZERO_DATE) MySQL trả ERROR 1525 "Incorrect DATE/TIMESTAMP value" cho
				// `col = '0000-00-00'`. Đọc giá trị zero rồi đổi sang chuỗi không phụ thuộc sql_mode.
				q := fmt.Sprintf(`
					SELECT COUNT(*) FROM (
						SELECT 1 FROM `+"`%s`.`%s`"+`
						 WHERE CAST(`+"`%s`"+` AS CHAR) LIKE '0000-00-00%%'
						    OR CAST(`+"`%s`"+` AS CHAR) LIKE '%%-00-%%'
						    OR CAST(`+"`%s`"+` AS CHAR) LIKE '%%-00'
						    OR CAST(`+"`%s`"+` AS CHAR) LIKE '%%-00 %%'
						 LIMIT %d
					) _cnt`, schema, table, col.Name, col.Name, col.Name, col.Name, opts.ScanLimit)

				var zeroCount int
				if err := src.QueryRowContext(ctx, q).Scan(&zeroCount); err != nil {
					// Không được nuốt lỗi: quét thất bại thì không thể kết luận bảng "sạch".
					return warnings, fmt.Errorf("validate %s.%s column %s: zero-date scan failed (cannot conclude table is safe): %w", schema, table, col.Name, err)
				}
				if zeroCount > 0 {
					return warnings, exitcode.New(
						exitcode.ErrTypeUnmappable,
						exitcode.Rejected,
						fmt.Sprintf("table %s.%s column %s: found %d row(s) with zero date ('0000-00-00') (policy=reject, spec: 02 §8)", schema, table, col.Name, zeroCount),
						nil,
					)
				}
			}

			// 6.2. Unsigned BigInt vượt int8 max khi dùng int8-checked
			if col.DataType == "bigint" && col.Unsigned && opts.BigIntUnsignedPolicy == "int8-checked" {
				q := fmt.Sprintf(`
					SELECT COUNT(*) FROM (
						SELECT 1 FROM `+"`%s`.`%s`"+`
						 WHERE `+"`%s`"+` > 9223372036854775807
						 LIMIT %d
					) _cnt`, schema, table, col.Name, opts.ScanLimit)

				var overCount int
				if err := src.QueryRowContext(ctx, q).Scan(&overCount); err != nil {
					return warnings, fmt.Errorf("validate %s.%s column %s: unsigned bigint range scan failed (cannot conclude table is safe): %w", schema, table, col.Name, err)
				}
				if overCount > 0 {
					return warnings, exitcode.New(
						exitcode.ErrTypeUnmappable,
						exitcode.Rejected,
						fmt.Sprintf("table %s.%s column %s: found %d row(s) with BIGINT UNSIGNED exceeding 2^63-1 (policy=int8-checked, spec: 02 §8)", schema, table, col.Name, overCount),
						nil,
					)
				}
			}

			// 6.3. ENUM ngoài danh sách hợp lệ
			if col.DataType == "enum" && len(col.EnumValues) > 0 && opts.EnumPolicy == "check" {
				var quotedVals []string
				for _, ev := range col.EnumValues {
					quotedVals = append(quotedVals, fmt.Sprintf("'%s'", strings.ReplaceAll(ev, "'", "''")))
				}
				inList := strings.Join(quotedVals, ", ")
				q := fmt.Sprintf(`
					SELECT COUNT(*) FROM (
						SELECT 1 FROM `+"`%s`.`%s`"+`
						 WHERE `+"`%s`"+` IS NOT NULL
						   AND `+"`%s`"+` NOT IN (%s)
						 LIMIT %d
					) _cnt`, schema, table, col.Name, col.Name, inList, opts.ScanLimit)

				var invalidEnumCount int
				if err := src.QueryRowContext(ctx, q).Scan(&invalidEnumCount); err != nil {
					return warnings, fmt.Errorf("validate %s.%s column %s: enum value scan failed (cannot conclude table is safe): %w", schema, table, col.Name, err)
				}
				if invalidEnumCount > 0 {
					return warnings, exitcode.New(
						exitcode.ErrTypeUnmappable,
						exitcode.Rejected,
						fmt.Sprintf("table %s.%s column %s: found %d row(s) with ENUM value outside defined list %v (spec: 02 §8)", schema, table, col.Name, invalidEnumCount, col.EnumValues),
						nil,
					)
				}
			}
		}
	}

	return warnings, nil
}
