package main

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"strand/internal/exitcode"
	"strand/internal/reader/archive"
)

// `strand archive binlog --from <replica> --to <dir>` (02 §10, P3): lưu binlog liên tục, thay cho
// script mysqlbinlog --raw --stop-never. Thư mục đích dùng trực tiếp cho `seed apply --archive`.
func newArchiveCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "archive", Short: "Lưu trữ log của source (binlog MySQL)"}
	var from, to, startGTID string
	var serverID uint32
	var heartbeat time.Duration
	bin := &cobra.Command{
		Use:   "binlog",
		Short: "Lưu binlog liên tục từ một MySQL (nên là replica) vào thư mục archive",
		Long: `Ghi binlog thô của server vào --to; file chỉ xuất hiện (đủ, đã fsync) khi server rotate, file dở
mang tên .partial-* và bị bỏ qua. Chạy lại tiếp tục theo GTID, không lặp, không hở. Lần chạy đầu bắt đầu
từ gtid_purged của source (mọi binlog còn trên server), hoặc --start-gtid.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if from == "" || to == "" {
				return exitcode.New("missing_flag", exitcode.InvalidSyntax, "--from and --to are required", nil)
			}
			if strings.Contains(to, "://") {
				return exitcode.New("invalid_flag", exitcode.InvalidSyntax,
					"--to must be a directory (object storage upload is not implemented; sync the directory to a bucket separately)", nil)
			}
			if err := normalizeDSNFlags(&from); err != nil {
				return err
			}
			dir, err := filepath.Abs(to)
			if err != nil {
				return err
			}
			scfg, err := syncerConfigFromDSN(from)
			if err != nil {
				return err
			}
			if serverID == 0 {
				h := fnv.New32a()
				_, _ = h.Write([]byte("strand-archive:" + dir))
				serverID = h.Sum32()%1_000_000_000 + 1_000_000_000
			}
			scfg.ServerID = serverID
			log := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("dir", dir, "server_id", serverID)

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			start := startGTID
			if start == "" {
				if start, err = sourceGTIDPurged(ctx, from); err != nil {
					return err
				}
			}
			a, err := archive.OpenArchiver(archive.ArchiverConfig{Dir: dir, Syncer: scfg, StartGTID: start, Heartbeat: heartbeat, Log: log})
			if err != nil {
				return err
			}
			defer a.Close()
			log.Info("archive: started", "executed", a.Executed())
			if dryRun {
				fmt.Printf("archive %s: would stream binlog after %s (server_id %d)\n", dir, a.Executed(), serverID)
				return nil
			}
			return a.Run(ctx)
		},
	}
	bin.Flags().StringVar(&from, "from", "", "DSN của MySQL nguồn (nên là replica), mysql://… hoặc user:pass@tcp(host:3306)/")
	bin.Flags().StringVar(&to, "to", "", "Thư mục archive (dùng cho seed apply --archive)")
	bin.Flags().Uint32Var(&serverID, "server-id", 0, "server_id khi đọc binlog; phải duy nhất trong cụm (mặc định suy từ --to)")
	bin.Flags().StringVar(&startGTID, "start-gtid", "", "Lần chạy đầu: tập GTID không cần lưu (mặc định gtid_purged của source)")
	bin.Flags().DurationVar(&heartbeat, "heartbeat", 30*time.Second, "Chu kỳ heartbeat để phát hiện kết nối chết")
	cmd.AddCommand(bin)
	return cmd
}

// syncerConfigFromDSN: host/port/user/password và TLS (tham số tls= của DSN, như reader — review R15).
func syncerConfigFromDSN(dsn string) (replication.BinlogSyncerConfig, error) {
	c, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return replication.BinlogSyncerConfig{}, exitcode.New("invalid_connection", exitcode.InvalidSyntax, fmt.Sprintf("invalid --from: %v", err), err)
	}
	host, port := c.Addr, 3306
	if h, p, err := net.SplitHostPort(c.Addr); err == nil {
		host = h
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	scfg := replication.BinlogSyncerConfig{Flavor: "mysql", Host: host, Port: uint16(port), User: c.User, Password: c.Passwd}
	if c.TLS != nil {
		scfg.TLSConfig = c.TLS.Clone()
		if scfg.TLSConfig.ServerName == "" && !scfg.TLSConfig.InsecureSkipVerify {
			scfg.TLSConfig.ServerName = host
		}
	}
	return scfg, nil
}

func sourceGTIDPurged(ctx context.Context, dsn string) (string, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", err
	}
	defer db.Close()
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var purged string
	if err := db.QueryRowContext(qctx, "SELECT @@GLOBAL.gtid_purged").Scan(&purged); err != nil {
		return "", fmt.Errorf("read gtid_purged of the source: %w", err)
	}
	return strings.ReplaceAll(purged, "\n", ""), nil
}
