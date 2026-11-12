// Package mysqlver nhận diện phiên bản server MySQL nguồn một lần và quyết định các câu SQL / thông điệp
// phụ thuộc phiên bản (nguồn MySQL 5.7 → đích 8.x). Mọi quyết định là hàm thuần trên Version để test không
// cần DB; nhánh 8.x giữ nguyên hành vi cũ, chỉ 5.7 đi nhánh riêng.
package mysqlver

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"strand/internal/exitcode"
)

// Flavor là nhánh phân phối của server.
type Flavor string

const (
	FlavorMySQL   Flavor = "mysql"
	FlavorPercona Flavor = "percona"
	FlavorMariaDB Flavor = "mariadb"
)

// Version là phiên bản server đã phân tích từ SELECT VERSION().
type Version struct {
	Major, Minor, Patch int
	Flavor              Flavor
	Raw                 string
}

var versionRe = regexp.MustCompile(`^\s*(\d+)\.(\d+)(?:\.(\d+))?`)

// Parse phân tích chuỗi VERSION() như "5.7.44-log", "5.7.44-48-log" (Percona), "8.0.36-0ubuntu0.22.04.1",
// "10.6.12-MariaDB-1:10.6.12+maria~ubu2004", "5.5.5-10.3.0-MariaDB" (tiền tố tương thích của MariaDB).
func Parse(raw string) (Version, error) {
	v := Version{Raw: raw, Flavor: FlavorMySQL}
	low := strings.ToLower(raw)
	switch {
	case strings.Contains(low, "mariadb"):
		v.Flavor = FlavorMariaDB
		// MariaDB cũ báo "5.5.5-10.x.y-MariaDB": số thật nằm sau tiền tố.
		if rest, ok := strings.CutPrefix(strings.TrimSpace(raw), "5.5.5-"); ok {
			raw = rest
		}
	case strings.Contains(low, "percona"):
		v.Flavor = FlavorPercona
	}
	m := versionRe.FindStringSubmatch(raw)
	if m == nil {
		return Version{}, fmt.Errorf("cannot parse MySQL version %q", v.Raw)
	}
	v.Major, _ = strconv.Atoi(m[1])
	v.Minor, _ = strconv.Atoi(m[2])
	if m[3] != "" {
		v.Patch, _ = strconv.Atoi(m[3])
	}
	return v, nil
}

// AtLeast: version >= major.minor.patch.
func (v Version) AtLeast(major, minor, patch int) bool {
	if v.Major != major {
		return v.Major > major
	}
	if v.Minor != minor {
		return v.Minor > minor
	}
	return v.Patch >= patch
}

// Line là dòng version dạng "5.7", "8.0".
func (v Version) Line() string { return fmt.Sprintf("%d.%d", v.Major, v.Minor) }

func (v Version) String() string { return v.Raw }

// IsMariaDB: nguồn MariaDB (GTID và binlog khác MySQL): chỉ chạy chế độ vị trí binlog.
func (v Version) IsMariaDB() bool { return v.Flavor == FlavorMariaDB }

// SyncerFlavor là Flavor cho go-mysql BinlogSyncer: "mariadb" để server gửi event MariaDB gốc (GTID,
// ANNOTATE_ROWS...) thay vì event giả thay thế; mọi bản khác là "mysql".
func (v Version) SyncerFlavor() string {
	if v.IsMariaDB() {
		return "mariadb"
	}
	return "mysql"
}

// Legacy57 là nguồn 5.7 (hoặc cũ hơn nhưng đã bị CheckSupported từ chối): không có các tính năng 8.0.
func (v Version) Legacy57() bool { return !v.IsMariaDB() && !v.AtLeast(8, 0, 0) }

// HasBinlogRowValueOptions: @@binlog_row_value_options có từ 8.0.3.
func (v Version) HasBinlogRowValueOptions() bool { return !v.IsMariaDB() && v.AtLeast(8, 0, 3) }

// HasBinlogRowMetadata: @@binlog_row_metadata (TABLE_MAP có tên cột/charset/PK) có từ 8.0.1.
// MariaDB có biến này từ 10.5 nhưng Strand đọc cấu trúc bảng MariaDB qua information_schema, không cần FULL.
func (v Version) HasBinlogRowMetadata() bool { return !v.IsMariaDB() && v.AtLeast(8, 0, 1) }

// HasPadAttribute: information_schema.COLLATIONS.PAD_ATTRIBUTE có từ 8.0; 5.7 mọi collation là PAD SPACE.
// MariaDB không có cột này (collation NO PAD có tên chứa "nopad").
func (v Version) HasPadAttribute() bool { return !v.IsMariaDB() && v.AtLeast(8, 0, 0) }

// HasBinlogExpireLogsSeconds: @@binlog_expire_logs_seconds có từ 8.0.1; 5.7 dùng expire_logs_days.
func (v Version) HasBinlogExpireLogsSeconds() bool { return v.AtLeast(8, 0, 1) }

// HasClone: clone plugin có từ 8.0.17.
func (v Version) HasClone() bool { return !v.IsMariaDB() && v.AtLeast(8, 0, 17) }

// HasSuperReadOnly: @@super_read_only có từ 5.7.8.
// MariaDB 10.6 không có super_read_only.
func (v Version) HasSuperReadOnly() bool { return !v.IsMariaDB() && v.AtLeast(5, 7, 8) }

// HasCastAsDouble: CAST(x AS DOUBLE) có từ 8.0.17 (5.7 báo lỗi cú pháp).
func (v Version) HasCastAsDouble() bool { return v.AtLeast(8, 0, 17) }

// PersistAdvice là lời khuyên đặt biến cấu hình lâu dài: SET PERSIST từ 8.0, 5.7 dùng SET GLOBAL + my.cnf.
func (v Version) PersistAdvice(variable, value string) string {
	if !v.IsMariaDB() && v.AtLeast(8, 0, 0) {
		return fmt.Sprintf("SET PERSIST %s = %s", variable, value)
	}
	return fmt.Sprintf("SET GLOBAL %s = %s and add \"%s=%s\" to my.cnf ([mysqld] section; SET GLOBAL alone is lost on restart)", variable, value, variable, value)
}

// CheckSupported từ chối nguồn không được hỗ trợ: MariaDB ngoài 10.6 và MySQL < 5.5 (exit 3).
func CheckSupported(v Version) error {
	if v.IsMariaDB() {
		// MariaDB 10.6 (yêu cầu chuyển MariaDB 10.6 → MySQL 8.x, 2026-11-12): chế độ vị trí binlog. Bản khác
		// chưa kiểm (kiểu dữ liệu/collation mới từ 10.7: UUID, uca1400...): từ chối rõ.
		if v.Major != 10 || v.Minor != 6 {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("MariaDB source %q is not supported: only MariaDB 10.6 is supported", v.Raw), nil)
		}
		return nil
	}
	// 5.5/5.6 được hỗ trợ (yêu cầu chuyển đổi từ phiên bản cũ, 2026-11-12): không có GTID thì đi chế độ vị trí
	// binlog. Dưới 5.5 (5.1, 5.0): ROWS event và catalog quá khác, chưa kiểm — từ chối rõ.
	if !v.AtLeast(5, 5, 0) {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("MySQL source 5.5 or later is required, got %q", v.Raw), nil)
	}
	return nil
}

// CheckTargetForSource từ chối (exit 3) cặp nguồn/đích chưa hỗ trợ: MariaDB chỉ sang MySQL 8.x (sang
// PostgreSQL chưa kiểm). targetKind: "mysql" | "postgres".
func CheckTargetForSource(v Version, targetKind string) error {
	if v.IsMariaDB() && !strings.EqualFold(targetKind, "mysql") {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("MariaDB source %q is supported only with a MySQL 8.x target, not %s", v.Raw, targetKind), nil)
	}
	return nil
}

// CheckClone từ chối seed clone khi donor (nguồn) không có clone plugin (< 8.0.17).
func CheckClone(v Version) error {
	if v.HasClone() {
		return nil
	}
	return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
		fmt.Sprintf("clone seed requires a MySQL 8.0.17+ donor, source is %q: use mysqldump or mydumper for MySQL 5.x", v.Raw), nil)
}

// Detect đọc SELECT VERSION() từ db và phân tích.
func Detect(ctx context.Context, db *sql.DB) (Version, error) {
	var raw string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&raw); err != nil {
		return Version{}, fmt.Errorf("read source server version: %w", err)
	}
	return Parse(raw)
}

var cache sync.Map // *sql.DB -> Version

// Cached như Detect nhưng chỉ hỏi server một lần cho mỗi *sql.DB (thành công mới nhớ).
func Cached(ctx context.Context, db *sql.DB) (Version, error) {
	if v, ok := cache.Load(db); ok {
		return v.(Version), nil
	}
	v, err := Detect(ctx, db)
	if err != nil {
		return Version{}, err
	}
	cache.Store(db, v)
	return v, nil
}

// Forget bỏ cache của db (gọi khi đóng db để không giữ con trỏ).
func Forget(db *sql.DB) { cache.Delete(db) }

// BinlogSettingsQuery trả câu SELECT binlog settings; 5.7 thay binlog_row_value_options bằng ”.
func BinlogSettingsQuery(v Version) string {
	opts := "@@GLOBAL.binlog_row_value_options"
	if !v.HasBinlogRowValueOptions() {
		opts = "''"
	}
	return "SELECT @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image, @@GLOBAL.gtid_mode, " + opts
}

// BinlogRetentionQuery trả câu SELECT thời gian giữ binlog tính bằng giây (0 = không giới hạn).
func BinlogRetentionQuery(v Version) string {
	if v.HasBinlogExpireLogsSeconds() {
		return "SELECT @@GLOBAL.binlog_expire_logs_seconds"
	}
	return "SELECT @@GLOBAL.expire_logs_days * 86400"
}

// BinlogRetentionSeconds đọc thời gian giữ binlog (giây) trên server; <= 0 là không giới hạn.
func BinlogRetentionSeconds(ctx context.Context, db *sql.DB) (int64, error) {
	v, err := Cached(ctx, db)
	if err != nil {
		return 0, err
	}
	var s int64
	if err := db.QueryRowContext(ctx, BinlogRetentionQuery(v)).Scan(&s); err != nil {
		return 0, err
	}
	return s, nil
}

// NormalizeCharset đưa tên charset về dạng của 8.x: 5.7 báo "utf8", 8.x báo "utf8mb3".
func NormalizeCharset(cs string) string {
	if strings.EqualFold(cs, "utf8") {
		return "utf8mb3"
	}
	return cs
}

// NormalizeCollation đưa tên collation về dạng của 8.x: "utf8_general_ci" → "utf8mb3_general_ci".
func NormalizeCollation(coll string) string {
	if len(coll) >= 5 && strings.EqualFold(coll[:5], "utf8_") {
		return "utf8mb3_" + coll[5:]
	}
	return coll
}
