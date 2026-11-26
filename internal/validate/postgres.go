package validate

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/findings"
	"strand/internal/model"
)

// CheckPGSource kiểm tra catalog chỉ đọc; không tạo slot hay publication.
// Khi slot không khai báo, seed có thể cung cấp tên khác qua state nên không
// khẳng định slot mặc định là slot sẽ được dùng.
func CheckPGSource(ctx context.Context, db *sql.DB, cfg *config.Config, tables []model.TableID) ([]string, error) {
	return CheckPGSourceReport(ctx, db, cfg, tables, nil)
}

// CheckPGSourceReport là CheckPGSource với bộ gom c: c != nil thì mỗi điều kiện bị từ chối thành một vấn đề
// trong c và kiểm tiếp; lỗi trả về chỉ còn là lỗi không đọc được catalog.
func CheckPGSourceReport(ctx context.Context, db *sql.DB, cfg *config.Config, tables []model.TableID, c *findings.Collector) ([]string, error) {
	reject := func(msg string) error {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, msg, nil)
	}
	var stop error
	fail := func(code, object, fix string, e error) bool {
		if c == nil {
			stop = e
			return true
		}
		c.Err(findings.Error, code, object, e, fix)
		return false
	}
	var wal, version, feedback, role string
	var recovery, canReplicate bool
	if err := db.QueryRowContext(ctx, `SELECT current_setting('wal_level'), current_setting('server_version_num'), pg_is_in_recovery(), current_setting('hot_standby_feedback'), (SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user), current_user`).Scan(&wal, &version, &recovery, &feedback, &canReplicate, &role); err != nil {
		return nil, fmt.Errorf("read PostgreSQL replication settings: %w", err)
	}
	if wal != "logical" {
		if fail("source_pg_wal_level", "source", "ALTER SYSTEM SET wal_level = logical; then restart PostgreSQL", reject(fmt.Sprintf("wal_level must be logical, got %s", wal))) {
			return nil, stop
		}
	}
	if !canReplicate {
		if fail("source_privilege_missing", "source", fmt.Sprintf(`ALTER ROLE "%s" WITH REPLICATION;`, strings.ReplaceAll(role, `"`, `""`)), reject("source role needs REPLICATION or SUPERUSER")) {
			return nil, stop
		}
	}
	ver, err := strconv.Atoi(version)
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL version %q: %w", version, err)
	}
	if recovery && (ver < 160000 || feedback != "on") {
		if fail("source_pg_standby", "source", "use PostgreSQL 16+ with hot_standby_feedback = on on the standby, or point source.dsn at the primary",
			reject("logical decoding on standby requires PostgreSQL 16+ and hot_standby_feedback=on")) {
			return nil, stop
		}
	}
	norm := func(name string) string {
		var b strings.Builder
		for _, r := range strings.ToLower(name) {
			if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
				b.WriteRune(r)
			} else {
				b.WriteByte('_')
			}
		}
		s := "strand_" + b.String()
		if len(s) > 63 {
			s = s[:63]
		}
		return s
	}
	pub := cfg.Source.Publication
	if pub == "" {
		pub = norm(cfg.Name)
	}
	if err := CheckPGPublicationReport(ctx, db, pub, tables, c); err != nil {
		if exitcode.GetExitCode(err) != exitcode.Rejected || fail("source_pg_publication", "source", "", err) {
			return nil, err
		}
	}
	var warnings []string
	slot := cfg.Source.Slot
	if slot == "" {
		return append(warnings, "slot not explicitly configured: seed state may choose the slot; validate did not inspect it"), nil
	}
	var kind, plugin, database, status, invalidation, conflicting string
	var active bool
	err = db.QueryRowContext(ctx, "SELECT slot_type, coalesce(plugin,''), coalesce(database,''), coalesce(wal_status,''), active, coalesce(to_jsonb(s)->>'invalidation_reason',''), coalesce(to_jsonb(s)->>'conflicting','false') FROM pg_replication_slots s WHERE slot_name=$1", slot).Scan(&kind, &plugin, &database, &status, &active, &invalidation, &conflicting)
	if err == sql.ErrNoRows {
		if cfg.Source.CreateSlot {
			if recovery && cfg.Source.FailoverSlot {
				if fail("source_pg_slot", slot, "set source.failover_slot: false, or point source.dsn at the primary", reject("cannot create a failover slot on a standby")) {
					return nil, stop
				}
			}
			return warnings, nil
		}
		e := exitcode.New(exitcode.ErrLogGap, exitcode.DataError, fmt.Sprintf("replication slot %q does not exist (create_slot=false)", slot), nil)
		if c == nil {
			return nil, e
		}
		c.Err(findings.Error, "source_pg_slot", slot, e, "set source.create_slot: true so sync creates it, or create it: SELECT pg_create_logical_replication_slot('"+strings.ReplaceAll(slot, "'", "''")+"', 'pgoutput');")
		return warnings, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect replication slot %q: %w", slot, err)
	}
	var currentDB string
	if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&currentDB); err != nil {
		return nil, err
	}
	if kind != "logical" || plugin != "pgoutput" || database != currentDB || status == "lost" || invalidation != "" || conflicting == "true" {
		if fail("source_pg_slot", slot, "drop the slot and let sync recreate it (a lost slot means the data must be copied again), or set source.slot to a logical pgoutput slot of this database",
			reject(fmt.Sprintf("replication slot %q incompatible: type=%s plugin=%s database=%s wal_status=%s invalidation=%s conflicting=%s", slot, kind, plugin, database, status, invalidation, conflicting))) {
			return nil, stop
		}
	}
	if active {
		warnings = append(warnings, fmt.Sprintf("replication slot %q is active; validate cannot determine its owner", slot))
	}
	return warnings, nil
}

// CheckPGPublication checks that pgoutput will publish all row changes in the snapshot scope.
func CheckPGPublication(ctx context.Context, db *sql.DB, pub string, tables []model.TableID) error {
	return CheckPGPublicationReport(ctx, db, pub, tables, nil)
}

// CheckPGPublicationReport là CheckPGPublication với bộ gom c: c != nil thì mỗi bảng có vấn đề thành một vấn đề
// source_pg_publication trong c.
func CheckPGPublicationReport(ctx context.Context, db *sql.DB, pub string, tables []model.TableID, c *findings.Collector) error {
	reject := func(message string) error {
		return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, message, nil)
	}
	quoted := `"` + strings.ReplaceAll(pub, `"`, `""`) + `"`
	var insert, update, deleteRows, viaRoot bool
	err := db.QueryRowContext(ctx, "SELECT pubinsert, pubupdate, pubdelete, pubviaroot FROM pg_publication WHERE pubname=$1", pub).
		Scan(&insert, &update, &deleteRows, &viaRoot)
	if err == sql.ErrNoRows {
		e := reject(fmt.Sprintf("publication %q does not exist", pub))
		if c == nil {
			return e
		}
		c.Err(findings.Error, "source_pg_publication", pub, e, "CREATE PUBLICATION "+quoted+" FOR TABLE <the tables in scope>; (or set source.publication to an existing publication)")
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect publication %q: %w", pub, err)
	}
	if !insert || !update || !deleteRows {
		e := reject(fmt.Sprintf("publication %q must publish insert, update and delete", pub))
		if c == nil {
			return e
		}
		c.Err(findings.Error, "source_pg_publication", pub, e, "ALTER PUBLICATION "+quoted+" SET (publish = 'insert, update, delete, truncate');")
	}
	for _, table := range tables {
		var partitioned, included, filtered, missingColumns bool
		// Use JSON field lookup for columns introduced in PostgreSQL 15.
		err := db.QueryRowContext(ctx, `
			SELECT c.relkind = 'p', p.pubname IS NOT NULL, to_jsonb(p)->>'rowfilter' IS NOT NULL,
			       EXISTS (SELECT 1 FROM pg_attribute a
			                WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
			                  AND a.attgenerated='' AND to_jsonb(p)->'attnames' IS NOT NULL
			                  AND NOT ((to_jsonb(p)->'attnames') ? a.attname::text))
			  FROM pg_namespace n
			  JOIN pg_class c ON c.relnamespace=n.oid
			  LEFT JOIN pg_publication_tables p
			    ON p.pubname=$1 AND p.schemaname=n.nspname AND p.tablename=c.relname
			 WHERE n.nspname=$2 AND c.relname=$3`,
			pub, table.Schema, table.Table).Scan(&partitioned, &included, &filtered, &missingColumns)
		tableSQL := `"` + strings.ReplaceAll(table.Schema, `"`, `""`) + `"."` + strings.ReplaceAll(table.Table, `"`, `""`) + `"`
		var e error
		fix := ""
		switch {
		case err == sql.ErrNoRows:
			e = reject(fmt.Sprintf("publication %q does not include %s", pub, table))
			fix = "ALTER PUBLICATION " + quoted + " ADD TABLE " + tableSQL + ";"
		case err != nil:
			return fmt.Errorf("inspect publication %q for %s: %w", pub, table, err)
		case partitioned && !viaRoot:
			e = reject(fmt.Sprintf("publication %q needs publish_via_partition_root for %s. Run: ALTER PUBLICATION %s SET (publish_via_partition_root = true)", pub, table, quoted))
			fix = "ALTER PUBLICATION " + quoted + " SET (publish_via_partition_root = true);"
		case !included:
			e = reject(fmt.Sprintf("publication %q does not include %s", pub, table))
			fix = "ALTER PUBLICATION " + quoted + " ADD TABLE " + tableSQL + ";"
		case filtered:
			e = reject(fmt.Sprintf("publication %q has a row filter for %s; the snapshot copies all rows", pub, table))
			fix = "ALTER PUBLICATION " + quoted + " SET TABLE ... without the WHERE clause for " + tableSQL
		case missingColumns:
			e = reject(fmt.Sprintf("publication %q omits columns of %s", pub, table))
			fix = "ALTER PUBLICATION " + quoted + " SET TABLE ... without a column list for " + tableSQL
		}
		if e == nil {
			continue
		}
		if c == nil {
			return e
		}
		c.Err(findings.Error, "source_pg_publication", table.String(), e, fix)
	}
	return nil
}
