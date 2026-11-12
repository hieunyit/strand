package seed

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// binlogResolveTimeout giới hạn thời gian đọc binlog của source để tính GTID tại vị trí.
const binlogResolveTimeout = 5 * time.Minute

// BinlogPositionResolver tính GTID set đã thực thi tại đúng một vị trí binlog của source.
// gtidEnabled=false nghĩa là source không dùng GTID (5.5 / gtid_mode=OFF): khi đó chỉ có vị trí file/pos.
type BinlogPositionResolver func(ctx context.Context, file string, pos uint64) (gtid string, gtidEnabled bool, err error)

// NewSourceBinlogResolver tạo resolver đọc binlog của source (DSN go-sql-driver) qua BinlogSyncer.
func NewSourceBinlogResolver(dsn string) BinlogPositionResolver {
	return func(ctx context.Context, file string, pos uint64) (string, bool, error) {
		cfg, err := syncerConfigFromDSN(dsn)
		if err != nil {
			return "", false, err
		}
		on, err := sourceGTIDEnabled(ctx, dsn)
		if err != nil || !on {
			return "", false, err
		}
		cfg.ServerID = uint32(2_000_000_000 + rand.Intn(1_000_000_000))
		cfg.RawModeEnabled = true
		cfg.DisableRetrySync = true
		syncer := replication.NewBinlogSyncer(cfg)
		defer syncer.Close()
		ctx, cancel := context.WithTimeout(ctx, binlogResolveTimeout)
		defer cancel()
		streamer, err := syncer.StartSync(gomysql.Position{Name: file, Pos: 4})
		if err != nil {
			return "", true, wrapBinlogErr(file, err)
		}
		g, err := GTIDAtBinlogPosition(file, pos, func() (*replication.BinlogEvent, error) {
			return streamer.GetEvent(ctx)
		})
		if err != nil {
			return "", true, wrapBinlogErr(file, err)
		}
		return g, true, nil
	}
}

// sourceGTIDEnabled: @@gtid_mode ON/ON_PERMISSIVE. 5.5 không có biến này (lỗi 1193) -> coi là tắt.
func sourceGTIDEnabled(ctx context.Context, dsn string) (bool, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, err
	}
	defer db.Close()
	var mode string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
		var me *drivermysql.MySQLError
		if errors.As(err, &me) && me.Number == 1193 {
			return false, nil
		}
		return false, fmt.Errorf("read source gtid_mode: %w", err)
	}
	mode = strings.ToUpper(mode)
	return mode == "ON" || mode == "ON_PERMISSIVE", nil
}

func wrapBinlogErr(file string, err error) error {
	var me *gomysql.MyError
	if errors.As(err, &me) && me.Code == 1236 {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("log gap: source binlog file %s (the dump's consistent position) no longer exists (purged); "+
				"cannot compute the GTID set at the dump position: take a new dump", file), err)
	}
	var se *exitcode.Error
	if errors.As(err, &se) {
		return err
	}
	return fmt.Errorf("read source binlog %s: %w", file, err)
}

func syncerConfigFromDSN(dsn string) (replication.BinlogSyncerConfig, error) {
	c, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return replication.BinlogSyncerConfig{}, exitcode.New("invalid_connection", exitcode.InvalidSyntax, fmt.Sprintf("invalid --source: %v", err), err)
	}
	host, port := c.Addr, 3306
	if h, p, err := net.SplitHostPort(c.Addr); err == nil {
		host = h
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	cfg := replication.BinlogSyncerConfig{Flavor: "mysql", Host: host, Port: uint16(port), User: c.User, Password: c.Passwd}
	if c.TLS != nil {
		cfg.TLSConfig = c.TLS.Clone()
		if cfg.TLSConfig.ServerName == "" && !cfg.TLSConfig.InsecureSkipVerify {
			cfg.TLSConfig.ServerName = host
		}
	}
	return cfg, nil
}

// GTIDAtBinlogPosition tính GTID set đã thực thi tại (file, pos), đọc file từ đầu qua next (event raw):
// Previous_gtids_log_event cho tập trước file, cộng mọi transaction có GTID event bắt đầu trước pos
// (pos là ranh giới transaction: SHOW MASTER STATUS dưới FTWRL). Dừng khi event kết thúc tại pos.
func GTIDAtBinlogPosition(file string, pos uint64, next func() (*replication.BinlogEvent, error)) (string, error) {
	set := checkpoint.NewMySQLAppliedSet()
	checksum := false
	for {
		ev, err := next()
		if err != nil {
			return "", err
		}
		h := ev.Header
		switch h.EventType {
		case replication.ROTATE_EVENT:
			if h.LogPos != 0 && h.Timestamp != 0 {
				return "", exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("binlog position %s:%d is beyond the end of the file", file, pos), nil)
			}
			continue // rotate giả đầu stream
		case replication.FORMAT_DESCRIPTION_EVENT:
			if fde, ok := ev.Event.(*replication.FormatDescriptionEvent); ok {
				checksum = fde.ChecksumAlgorithm == replication.BINLOG_CHECKSUM_ALG_CRC32
			}
		case replication.PREVIOUS_GTIDS_EVENT:
			raw := ev.RawData
			if len(raw) < replication.EventHeaderSize {
				return "", fmt.Errorf("short Previous_gtids event")
			}
			body := raw[replication.EventHeaderSize:]
			if checksum && len(body) >= 4 {
				body = body[:len(body)-4]
			}
			pg := &replication.PreviousGTIDsEvent{}
			if err := pg.Decode(body); err != nil {
				return "", fmt.Errorf("decode Previous_gtids event: %w", err)
			}
			if pg.GTIDSets != "" {
				if err := set.AddGTIDSet(pg.GTIDSets); err != nil {
					return "", err
				}
			}
		case replication.GTID_EVENT:
			if uint64(h.LogPos)-uint64(h.EventSize) >= pos && h.LogPos != 0 {
				return finishGTID(set)
			}
			raw := ev.RawData
			off := replication.EventHeaderSize
			if len(raw) < off+25 {
				return "", fmt.Errorf("short GTID event (%d bytes)", len(raw))
			}
			sid, err := uuid.FromBytes(raw[off+1 : off+17])
			if err != nil {
				return "", err
			}
			gno := int64(leUint64(raw[off+17 : off+25]))
			if err := set.AddGTIDSet(fmt.Sprintf("%s:%d", sid.String(), gno)); err != nil {
				return "", err
			}
		case replication.GTID_TAGGED_LOG_EVENT:
			return "", exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				"tagged GTIDs (MySQL 8.4 gtid tags) are not supported by strand", nil)
		}
		if h.LogPos != 0 && uint64(h.LogPos) >= pos && h.EventType != replication.HEARTBEAT_EVENT && h.EventType != replication.HEARTBEAT_LOG_EVENT_V2 {
			return finishGTID(set)
		}
	}
}

func leUint64(b []byte) uint64 {
	var v uint64
	for i := 7; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v
}

func finishGTID(set *checkpoint.MySQLAppliedSet) (string, error) {
	return strings.ReplaceAll(set.String(), "\n", ""), nil
}
