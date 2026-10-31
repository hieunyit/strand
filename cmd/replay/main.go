package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/model"
	"strand/internal/sched"
)

type ReplayMetrics struct {
	Workers      int            `json:"workers"`
	TxnCount     int            `json:"txn_count"`
	RowCount     int            `json:"row_count"`
	DurationSec  float64        `json:"duration_seconds"`
	TxnPerSec    float64        `json:"txn_per_sec"`
	RowsPerSec   float64        `json:"rows_per_sec"`
	DAGWidthHist map[string]int `json:"dag_width_histogram"`
	HotRowRatio  float64        `json:"hot_row_ratio"`
}

func main() {
	src := flag.String("src", "root:dev@tcp(127.0.0.1:3306)/app?parseTime=true", "MySQL Source DSN")
	dst := flag.String("dst", "root:dev@tcp(127.0.0.1:3307)/app?parseTime=true", "MySQL Target DSN")
	workers := flag.Int("workers", 4, "Number of concurrent replay workers")
	maxTxns := flag.Int("max-txns", 200, "Maximum transactions to replay")
	startFile := flag.String("start-file", "", "Binlog start file")
	startPos := flag.Uint("start-pos", 4, "Binlog start pos")
	out := flag.String("out", "", "Output JSON benchmark file")
	flag.Parse()

	metrics, err := RunReplay(*src, *dst, *workers, *maxTxns, *startFile, uint32(*startPos))
	if err != nil {
		log.Fatalf("Replay failed: %v", err)
	}

	data, _ := json.MarshalIndent(metrics, "", "  ")
	fmt.Println(string(data))

	if *out != "" {
		_ = os.WriteFile(*out, data, 0644)
	}
}

func parseDSN(dsn string) (user, password, host string, port int, dbname string, err error) {
	// Format: user:password@tcp(host:port)/dbname?...
	parts := strings.Split(dsn, "@tcp(")
	if len(parts) != 2 {
		return "", "", "", 0, "", fmt.Errorf("unsupported DSN format: %s", dsn)
	}
	userPass := strings.Split(parts[0], ":")
	user = userPass[0]
	if len(userPass) > 1 {
		password = userPass[1]
	}

	hostPortDB := parts[1]
	closeParenIdx := strings.Index(hostPortDB, ")")
	if closeParenIdx == -1 {
		return "", "", "", 0, "", fmt.Errorf("missing ) in DSN: %s", dsn)
	}
	hostPort := hostPortDB[:closeParenIdx]
	h, pStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", "", "", 0, "", err
	}
	p, _ := strconv.Atoi(pStr)

	rest := hostPortDB[closeParenIdx+1:]
	if strings.HasPrefix(rest, "/") {
		rest = rest[1:]
	}
	qIdx := strings.Index(rest, "?")
	if qIdx != -1 {
		dbname = rest[:qIdx]
	} else {
		dbname = rest
	}

	return user, password, h, p, dbname, nil
}

// RunReplay đọc stream binlog từ src, đưa qua Scheduler và apply vào dst với số lượng worker chỉ định.
func RunReplay(srcDSN, dstDSN string, numWorkers, targetTxns int, startFile string, startPos uint32) (*ReplayMetrics, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open src db: %w", err)
	}
	defer srcDB.Close()

	dstDB, err := sql.Open("mysql", dstDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open dst db: %w", err)
	}
	defer dstDB.Close()

	// Đồng bộ lược đồ bảng gen_orders sang target nếu chưa có
	var createDDL string
	var tblName string
	err = srcDB.QueryRowContext(ctx, "SHOW CREATE TABLE gen_orders").Scan(&tblName, &createDDL)
	if err == nil {
		_, _ = dstDB.ExecContext(ctx, createDDL)
	}

	user, pass, host, port, _, err := parseDSN(srcDSN)
	if err != nil {
		return nil, err
	}

	syncerConfig := replication.BinlogSyncerConfig{
		ServerID: uint32(10000 + rand.Intn(50000)),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: pass,
	}
	syncer := replication.NewBinlogSyncer(syncerConfig)
	defer syncer.Close()

	if startFile == "" {
		// Thử lấy vị trí hiện tại
		var curFile string
		var curPos uint32
		var d1, d2, d3 sql.NullString
		err = srcDB.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&curFile, &curPos, &d1, &d2, &d3)
		if err == nil && curFile != "" {
			startFile = curFile
			startPos = 4
		} else {
			startFile = "binlog.000001"
			startPos = 4
		}
	}

	streamer, err := syncer.StartSync(mysql.Position{Name: startFile, Pos: startPos})
	if err != nil {
		return nil, fmt.Errorf("failed to start binlog sync from %s:%d: %w", startFile, startPos, err)
	}

	scheduler := sched.NewScheduler(sched.DefaultConfig())

	var wg sync.WaitGroup
	var completedTxns int
	var completedRows int
	var metricsMu sync.Mutex

	// Khởi chạy worker pool
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for t := range scheduler.Ready() {
				rowCount, err := applyTxn(ctx, dstDB, t)
				if err != nil {
					log.Printf("worker %d failed to apply txn %d: %v", workerID, t.Seq(), err)
				}
				scheduler.Done(t.Seq(), err)

				metricsMu.Lock()
				completedTxns++
				completedRows += rowCount
				metricsMu.Unlock()
			}
		}(w)
	}

	start := time.Now()
	var currentTxn *model.Txn
	var admittedTxns int

	// Loop đọc binlog events và lắp Txn
	for admittedTxns < targetTxns {
		select {
		case <-ctx.Done():
			break
		default:
		}

		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			break
		}

		switch e := ev.Event.(type) {
		case *replication.GTIDEvent:
			currentTxn = &model.Txn{
				SourcePos: model.Pos{
					Engine: model.EngineMySQL,
					GTID:   fmt.Sprintf("%s:%d", e.SID, e.GNO),
				},
			}
		case *replication.GtidTaggedLogEvent:
			currentTxn = &model.Txn{
				SourcePos: model.Pos{
					Engine: model.EngineMySQL,
					GTID:   fmt.Sprintf("%s:%s:%d", e.SID, e.Tag.String(), e.GNO),
				},
			}
		case *replication.RowsEvent:
			if currentTxn == nil {
				currentTxn = &model.Txn{
					SourcePos: model.Pos{Engine: model.EngineMySQL},
				}
			}
			tbl := model.TableID{
				Schema: string(e.Table.Schema),
				Table:  string(e.Table.Table),
			}

			// Chỉ replay các bảng ứng dụng test (bỏ qua hệ thống mysql)
			if tbl.Schema == "mysql" || tbl.Schema == "sys" || tbl.Schema == "information_schema" || tbl.Schema == "performance_schema" {
				continue
			}

			var op model.Op
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				op = model.OpInsert
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				op = model.OpUpdate
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				op = model.OpDelete
			}

			// Parse các rows
			if op == model.OpUpdate {
				for i := 0; i < len(e.Rows); i += 2 {
					oldRow := e.Rows[i]
					newRow := e.Rows[i+1]
					var key, oldKey []model.Value
					if len(newRow) > 0 {
						key = []model.Value{newRow[0]}
					}
					if len(oldRow) > 0 {
						oldKey = []model.Value{oldRow[0]}
					}
					var cols []model.ColValue
					for cIdx, cVal := range newRow {
						cols = append(cols, model.ColValue{
							Name:  fmt.Sprintf("col_%d", cIdx),
							Value: cVal,
						})
					}
					currentTxn.Rows = append(currentTxn.Rows, model.RowChange{
						Table:  tbl,
						Op:     op,
						Key:    key,
						OldKey: oldKey,
						Cols:   cols,
					})
				}
			} else {
				for _, row := range e.Rows {
					var key []model.Value
					if len(row) > 0 {
						key = []model.Value{row[0]}
					}
					var cols []model.ColValue
					for cIdx, cVal := range row {
						cols = append(cols, model.ColValue{
							Name:  fmt.Sprintf("col_%d", cIdx),
							Value: cVal,
						})
					}
					currentTxn.Rows = append(currentTxn.Rows, model.RowChange{
						Table: tbl,
						Op:    op,
						Key:   key,
						Cols:  cols,
					})
				}
			}

		case *replication.XIDEvent:
			if currentTxn != nil && len(currentTxn.Rows) > 0 {
				_ = scheduler.Admit(ctx, currentTxn)
				admittedTxns++
				currentTxn = nil
			}
		case *replication.QueryEvent:
			if string(e.Query) == "COMMIT" {
				if currentTxn != nil && len(currentTxn.Rows) > 0 {
					_ = scheduler.Admit(ctx, currentTxn)
					admittedTxns++
					currentTxn = nil
				}
			}
		}
	}

	// Đợi các worker hoàn thành
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for workers to complete (%d/%d txns): %w", completedTxns, admittedTxns, ctx.Err())
		case <-ticker.C:
			metricsMu.Lock()
			done := completedTxns >= admittedTxns
			metricsMu.Unlock()
			if done {
				goto finished
			}
		}
	}

finished:
	scheduler.Close()
	close(scheduler.ReadyCh())
	wg.Wait()

	duration := time.Since(start).Seconds()
	if duration <= 0 {
		duration = 0.001
	}

	dagHist, hotRatio := scheduler.Stats()

	return &ReplayMetrics{
		Workers:      numWorkers,
		TxnCount:     completedTxns,
		RowCount:     completedRows,
		DurationSec:  duration,
		TxnPerSec:    float64(completedTxns) / duration,
		RowsPerSec:   float64(completedRows) / duration,
		DAGWidthHist: dagHist,
		HotRowRatio:  hotRatio,
	}, nil
}

func applyTxn(ctx context.Context, db *sql.DB, t *model.Txn) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	count := 0
	for _, row := range t.Rows {
		tbl := row.Table.String()
		switch row.Op {
		case model.OpInsert:
			if len(row.Cols) >= 4 {
				id := row.Cols[0].Value
				acc := row.Cols[1].Value
				amount := row.Cols[2].Value
				note := row.Cols[3].Value
				query := fmt.Sprintf("INSERT INTO %s (id, account_id, amount, note) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE account_id=?, amount=?, note=?", tbl)
				_, err = tx.ExecContext(ctx, query, id, acc, amount, note, acc, amount, note)
			}
		case model.OpUpdate:
			if len(row.Cols) >= 4 && len(row.Key) > 0 {
				id := row.Cols[0].Value
				acc := row.Cols[1].Value
				amount := row.Cols[2].Value
				note := row.Cols[3].Value
				pk := row.Key[0]
				if len(row.OldKey) > 0 {
					pk = row.OldKey[0]
				}
				query := fmt.Sprintf("UPDATE %s SET id=?, account_id=?, amount=?, note=? WHERE id=?", tbl)
				_, err = tx.ExecContext(ctx, query, id, acc, amount, note, pk)
			}
		case model.OpDelete:
			if len(row.Key) > 0 {
				pk := row.Key[0]
				query := fmt.Sprintf("DELETE FROM %s WHERE id=?", tbl)
				_, err = tx.ExecContext(ctx, query, pk)
			}
		}
		if err != nil {
			return count, err
		}
		count++
	}

	return count, tx.Commit()
}
