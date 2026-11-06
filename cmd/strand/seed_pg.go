package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/spf13/cobra"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/seed"
)

// spec: 02 §9 (PG 16–18: pgBackRest / WAL-G, P = LSN của slot kiểm bằng pg_last_wal_replay_lsn()); design "PG seed"

// isPGKind cho biết --type là seed PostgreSQL.
func isPGKind(kind string) bool {
	switch kind {
	case "pgbackrest", "walg", "wal-g", "strand-export", "strand_export":
		return true
	}
	return false
}

// manifestP trả P dạng chuỗi (GTID set với MySQL, LSN với PG).
func manifestP(m *seed.SeedManifest) string {
	if m.StartPosition.LSN != "" {
		return m.StartPosition.LSN
	}
	return m.StartPosition.GTIDSet
}

// pgPlanFlags là các cờ của `seed plan` dành cho PG.
type pgPlanFlags struct {
	slot, backupLabel, archiveInfo, toolVersion string
	eta, fullInterval                           time.Duration
	retentionFull                               int
}

func (f *pgPlanFlags) register(c *cobra.Command) {
	c.Flags().StringVar(&f.slot, "slot", "", "PG: slot logical (đã tạo trên --source) — P = consistent_point/confirmed_flush_lsn của slot")
	c.Flags().StringVar(&f.backupLabel, "backup-label", "", "PG: label pgBackRest / backup_name WAL-G cần dùng (mặc định: backup mới nhất kết thúc trước P)")
	c.Flags().StringVar(&f.archiveInfo, "archive-info", "", "inventory archive WAL (SD-2): WAL-G = JSON của `wal-g wal-show --detailed-json`; pgBackRest = JSON của `pgbackrest repo-ls archive/<stanza>/<archive-id> --recurse --output=json` (bắt buộc: `info` chỉ có min/max, review N08)")
	c.Flags().StringVar(&f.toolVersion, "tool-version", "", "PG: phiên bản công cụ khi metadata không ghi (WAL-G)")
	c.Flags().DurationVar(&f.eta, "eta", 2*time.Hour, "PG: thời gian restore dự kiến; backup không được expire trong khoảng này")
	c.Flags().IntVar(&f.retentionFull, "retention-full", 0, "PG: repo1-retention-full (pgBackRest) / số FULL giữ lại (wal-g delete retain FULL N)")
	c.Flags().DurationVar(&f.fullInterval, "full-interval", 0, "PG: chu kỳ full backup (mặc định suy từ các full trong metadata)")
}

// fillPGSeedSource đọc từ source: systemid (IDENTIFY_SYSTEM), server_version, wal_segment_size và P từ slot;
// từ target (nếu có): server_version và pg_last_wal_replay_lsn() khi target đã dừng (pause) ở recovery target.
func fillPGSeedSource(ctx context.Context, src *seed.SeedSource, sourceDSN, targetDSN string, f pgPlanFlags) error {
	src.Engine = "pg"
	src.Slot = f.slot
	src.BackupLabel = f.backupLabel
	src.ArchiveInfoRef = f.archiveInfo
	src.ToolVersion = f.toolVersion
	src.ETA = f.eta
	src.RetentionFull = f.retentionFull
	src.FullBackupInterval = f.fullInterval
	if src.Kind == "wal-g" {
		src.Kind = "walg"
	}
	// SD-1: P không nhận gõ tay — phải đọc từ slot trên source.
	if sourceDSN == "" || f.slot == "" {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			"SD-1: PostgreSQL seed needs --source and --slot (P is the consistent_point of the CDC slot)", nil)
	}
	sysID, err := identifySystem(ctx, sourceDSN)
	if err != nil {
		return err
	}
	src.SourceSystemID = sysID

	conn, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var segSize int64
	if err := conn.QueryRow(ctx, `SELECT current_setting('server_version'),
		(SELECT setting::bigint FROM pg_settings WHERE name = 'wal_segment_size')`).Scan(&src.SourceVersion, &segSize); err != nil {
		return fmt.Errorf("read source version: %w", err)
	}
	if segSize > 0 {
		src.WALSegmentSize = uint64(segSize)
	}
	var slotType, plugin string
	var flush *string
	err = conn.QueryRow(ctx, `SELECT slot_type, coalesce(plugin, ''), confirmed_flush_lsn::text
		FROM pg_replication_slots WHERE slot_name = $1`, f.slot).Scan(&slotType, &plugin, &flush)
	if errors.Is(err, pgx.ErrNoRows) {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: slot %q does not exist on source (create it after the standby has caught up)", f.slot), nil)
	}
	if err != nil {
		return fmt.Errorf("read slot %s: %w", f.slot, err)
	}
	if slotType != "logical" || plugin != "pgoutput" || flush == nil {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: slot %q must be a logical pgoutput slot with a consistent point (type=%s plugin=%s)", f.slot, slotType, plugin), nil)
	}
	src.ConsistentPointLSN = *flush

	if targetDSN != "" {
		tc, err := pgx.Connect(ctx, targetDSN)
		if err != nil {
			return fmt.Errorf("connect target: %w", err)
		}
		defer func() { _ = tc.Close(context.Background()) }()
		var inRecovery bool
		if err := tc.QueryRow(ctx, "SELECT current_setting('server_version'), pg_is_in_recovery()").
			Scan(&src.TargetVersion, &inRecovery); err != nil {
			return fmt.Errorf("read target version: %w", err)
		}
		if inRecovery {
			// Chỉ đối chiếu khi target đã dừng tại recovery target; standby đang đuổi archive thì chưa.
			var paused string
			var replay *string
			if err := tc.QueryRow(ctx, "SELECT pg_get_wal_replay_pause_state(), pg_last_wal_replay_lsn()::text").
				Scan(&paused, &replay); err != nil {
				return fmt.Errorf("read target replay state: %w", err)
			}
			if paused == "paused" && replay != nil {
				src.TargetRestoredLSN = *replay
			}
		}
	}
	return nil
}

// identifySystem chạy IDENTIFY_SYSTEM trên kết nối replication và trả systemid.
func identifySystem(ctx context.Context, dsn string) (string, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, "invalid PostgreSQL DSN", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("replication connection to source: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	res, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return "", fmt.Errorf("IDENTIFY_SYSTEM: %w", err)
	}
	return res.SystemID, nil
}

// pgRestoreCommands in các bước restore cho seed PG (Strand không tự chạy pgbackrest / wal-g / pg_restore).
func pgRestoreCommands(m *seed.SeedManifest) []string {
	p := m.StartPosition.LSN
	recovery := fmt.Sprintf("đặt recovery_target_lsn = '%s', recovery_target_inclusive = on, recovery_target_action = 'pause' rồi khởi động lại target", p)
	check := "strand seed apply --target <dsn>: kiểm pg_last_wal_replay_lsn() = P rồi pg_promote()"
	after := "trước khi mở ghi: archive_mode = off (hoặc stanza khác), xóa publication/subscription thừa kế từ source"
	switch m.Kind {
	case "pgbackrest":
		stanza, label := "main", ""
		if rest, ok := strings.CutPrefix(m.BackupRef, "pgbackrest://"); ok {
			stanza, label, _ = strings.Cut(rest, "/")
		}
		return []string{
			fmt.Sprintf("pgbackrest --stanza=%s --set=%s --type=standby --delta restore   # trên target, để standby đuổi archive", stanza, label),
			recovery,
			"nếu segment chứa P chưa được archive: SELECT pg_switch_wal() trên primary, hoặc đặt primary_conninfo",
			check, after,
		}
	case "walg", "wal-g":
		name := strings.TrimPrefix(m.BackupRef, "wal-g://")
		return []string{
			fmt.Sprintf("wal-g backup-fetch $PGDATA %s   # restore_command = 'wal-g wal-fetch %%f %%p', standby.signal", name),
			recovery,
			"nếu segment chứa P chưa được archive: SELECT pg_switch_wal() trên primary, hoặc đặt primary_conninfo",
			check, after,
		}
	default: // strand-export
		return []string{fmt.Sprintf("pg_restore -j 8 -d <target_dsn> %s", m.BackupRef)}
	}
}

// applyPGSeed: chạy --restore-cmd (nếu có), rồi kiểm/điều phối target. Không tự gọi pgbackrest/wal-g.
func applyPGSeed(ctx context.Context, m *seed.SeedManifest, targetDSN, restoreCmd string) error {
	if restoreCmd != "" {
		if err := runRestoreShellCmd(ctx, restoreCmd); err != nil {
			return fmt.Errorf("restore-cmd failed: %w", err)
		}
	}
	if targetDSN == "" {
		return exitcode.New("missing_target", exitcode.InvalidSyntax, "missing --target", nil)
	}
	conn, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if m.Kind == "strand-export" || m.Kind == "strand_export" {
		// Dump logic: dữ liệu đúng tại P theo snapshot; chỉ kiểm các bảng đã được nạp. "tables" là pattern
		// `pg_dump -t` (có thể có `*`, `?`, "..."): đối chiếu với bảng thật ở target bằng cùng cách hiểu
		// seed.SeedCovers thay vì to_regclass (không hiểu pattern) — review N06.
		rows, err := conn.Query(ctx, `SELECT n.nspname, c.relname FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind IN ('r', 'p') AND n.nspname NOT IN ('pg_catalog', 'information_schema')`)
		if err != nil {
			return fmt.Errorf("list tables on target: %w", err)
		}
		var have []model.TableID
		for rows.Next() {
			var id model.TableID
			if err := rows.Scan(&id.Schema, &id.Table); err != nil {
				rows.Close()
				return fmt.Errorf("list tables on target: %w", err)
			}
			have = append(have, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("list tables on target: %w", err)
		}
		if miss := missingSeedTables(m.Tables, have); len(miss) > 0 {
			return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
				fmt.Sprintf("table(s) %s from the export are missing on target (run pg_restore first)", strings.Join(miss, ", ")), nil)
		}
		return nil
	}
	return prepareRestoredPGTarget(ctx, conn, m)
}

// missingSeedTables trả các phần tử "tables" của seed.json không khớp bảng nào trong have (review N06).
func missingSeedTables(patterns []string, have []model.TableID) []string {
	var miss []string
	for _, t := range patterns {
		found := false
		for _, id := range have {
			if seed.SeedCovers([]string{t}, id) {
				found = true
				break
			}
		}
		if !found {
			miss = append(miss, t)
		}
	}
	return miss
}

// prepareRestoredPGTarget (idempotent): target phải đang dừng ở recovery với pg_last_wal_replay_lsn() = P
// (SD-1); khớp thì promote. Chạy lại sau khi đã promote: chấp nhận nếu lần trước đã ghi nhận khớp.
func prepareRestoredPGTarget(ctx context.Context, conn *pgx.Conn, m *seed.SeedManifest) error {
	p, err := checkpoint.ParseLSN(m.StartPosition.LSN)
	if err != nil || p == 0 {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "invalid start_position.lsn in manifest", err)
	}
	pStr := checkpoint.FormatLSN(p)
	var inRecovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return fmt.Errorf("read pg_is_in_recovery: %w", err)
	}
	if !inRecovery {
		if cc := m.PositionCrossCheck; cc != nil && cc.Match && cc.RestoredLSN == pStr {
			fmt.Printf("target already promoted at P = %s\n", pStr)
			return nil
		}
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			"SD-1: target is not in recovery: cannot prove it stopped exactly at P (restore as standby with recovery_target_lsn = P, recovery_target_action = pause)", nil)
	}
	var replay *string
	var pause string
	if err := conn.QueryRow(ctx, "SELECT pg_last_wal_replay_lsn()::text, pg_get_wal_replay_pause_state()").Scan(&replay, &pause); err != nil {
		return fmt.Errorf("read pg_last_wal_replay_lsn: %w", err)
	}
	if replay == nil {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "SD-1: target has not replayed any WAL yet", nil)
	}
	got, err := checkpoint.ParseLSN(*replay)
	if err != nil {
		return fmt.Errorf("parse target replay LSN %q: %w", *replay, err)
	}
	if got != p {
		hint := "target replayed past P: restore again with recovery_target_lsn = P"
		if got < p {
			hint = "target has not reached P yet (paused=" + pause + "); wait and run seed apply again"
		}
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-1: target pg_last_wal_replay_lsn() = %s != P = %s: %s", checkpoint.FormatLSN(got), pStr, hint), nil)
	}
	var promoted bool
	if err := conn.QueryRow(ctx, "SELECT pg_promote(true, 120)").Scan(&promoted); err != nil {
		return fmt.Errorf("pg_promote: %w", err)
	}
	if !promoted {
		return fmt.Errorf("pg_promote did not finish within 120s; run seed apply again")
	}
	m.PositionCrossCheck = &seed.PositionCrossCheck{RestoredLSN: pStr, Match: true}
	fmt.Printf("target replayed exactly to P = %s and was promoted\n", pStr)

	// design bước 5: cảnh báo những thứ thừa kế từ source cần xử lý trước khi mở ghi.
	var archiveMode string
	var pubs, subs int
	if err := conn.QueryRow(ctx, `SELECT current_setting('archive_mode'),
		(SELECT count(*) FROM pg_publication), (SELECT count(*) FROM pg_subscription)`).Scan(&archiveMode, &pubs, &subs); err == nil {
		if archiveMode != "off" {
			fmt.Printf("WARNING: archive_mode = %s on target: the new timeline may push WAL into the source's repository; turn it off or use another stanza\n", archiveMode)
		}
		if pubs > 0 || subs > 0 {
			fmt.Printf("WARNING: target inherited %d publication(s) and %d subscription(s) from the source; drop them before opening for writes\n", pubs, subs)
		}
	}
	return nil
}
