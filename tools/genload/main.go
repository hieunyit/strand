package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type GenloadResult struct {
	TxnCount    int     `json:"txn_count"`
	RowCount    int     `json:"row_count"`
	HotRowRatio float64 `json:"hot_row_ratio"`
	DurationSec float64 `json:"duration_seconds"`
}

func main() {
	dsn := flag.String("dsn", "root:dev@tcp(127.0.0.1:3306)/app?parseTime=true", "MySQL Source DSN")
	count := flag.Int("count", 200, "Number of transactions to generate")
	hotRatio := flag.Float64("hot-ratio", 0.2, "Hot row ratio (0.0 - 1.0)")
	out := flag.String("out", "", "Output JSON path")
	flag.Parse()

	res, err := RunGenload(*dsn, *count, *hotRatio)
	if err != nil {
		log.Fatalf("Genload failed: %v", err)
	}

	data, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(data))

	if *out != "" {
		_ = os.WriteFile(*out, data, 0644)
	}
}

// RunGenload thực thi sinh tải có kiểm soát trên MySQL source với hot row.
func RunGenload(dsn string, count int, hotRatio float64) (*GenloadResult, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %w", err)
	}
	defer db.Close()

	// Khởi tạo bảng kiểm tra
	schemaDDL := `
	CREATE TABLE IF NOT EXISTS gen_orders (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		account_id INT NOT NULL,
		amount DECIMAL(10, 2) NOT NULL,
		note VARCHAR(255) NOT NULL,
		INDEX idx_account (account_id)
	) ENGINE=InnoDB;`
	if _, err := db.Exec(schemaDDL); err != nil {
		return nil, fmt.Errorf("failed to create table gen_orders: %w", err)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	totalRows := 0
	hotCount := 0

	numHotRows := 5
	if count < numHotRows {
		numHotRows = count
	}

	// 1. Khởi tạo các hot row (mỗi cái trong 1 transaction)
	for h := 1; h <= numHotRows; h++ {
		tx, err := db.Begin()
		if err != nil {
			return nil, fmt.Errorf("failed to begin init tx %d: %w", h, err)
		}
		_, err = tx.Exec("INSERT INTO gen_orders (id, account_id, amount, note) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE note = VALUES(note)",
			h, h, 100.00, fmt.Sprintf("hot_order_%d", h))
		if err != nil {
			_ = tx.Rollback()
			return nil, fmt.Errorf("init hot row %d failed: %w", h, err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("init hot commit %d failed: %w", h, err)
		}
		totalRows++
	}

	// 2. Chạy các transaction còn lại
	for i := numHotRows + 1; i <= count; i++ {
		tx, err := db.Begin()
		if err != nil {
			return nil, fmt.Errorf("failed to begin tx %d: %w", i, err)
		}

		isHot := r.Float64() < hotRatio
		if isHot {
			// Cập nhật trên hot row ngẫu nhiên từ 1 đến numHotRows
			hotID := 1 + r.Intn(numHotRows)
			delta := float64(1 + r.Intn(20))
			note := fmt.Sprintf("update_hot_%d_by_tx_%d", hotID, i)
			_, err = tx.Exec("UPDATE gen_orders SET amount = amount + ?, note = ? WHERE id = ?", delta, note, hotID)
			if err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("update hot row failed in tx %d: %w", i, err)
			}
			hotCount++
			totalRows++
		} else {
			accountID := 100 + r.Intn(10000)
			amount := float64(10+r.Intn(1000)) + 0.50
			note := fmt.Sprintf("order_%d_acc_%d", i, accountID)
			res, err := tx.Exec("INSERT INTO gen_orders (account_id, amount, note) VALUES (?, ?, ?)", accountID, amount, note)
			if err != nil {
				_ = tx.Rollback()
				return nil, fmt.Errorf("insert failed in tx %d: %w", i, err)
			}
			totalRows++

			// Thỉnh thoảng UPDATE trên cùng transaction để tạo row delta
			if i%4 == 0 {
				lastID, _ := res.LastInsertId()
				_, _ = tx.Exec("UPDATE gen_orders SET amount = amount + 5.0 WHERE id = ?", lastID)
				totalRows++
			}
		}

		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit failed in tx %d: %w", i, err)
		}
	}

	duration := time.Since(start).Seconds()
	actualHotRatio := float64(hotCount) / float64(count)

	return &GenloadResult{
		TxnCount:    count,
		RowCount:    totalRows,
		HotRowRatio: actualHotRatio,
		DurationSec: duration,
	}, nil
}
