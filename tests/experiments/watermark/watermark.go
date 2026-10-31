package watermark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// Config chứa các cấu hình cho thí nghiệm watermark RS-2.
type Config struct {
	Addr            string // host:port
	User            string
	Password        string
	DBName          string
	Iterations      int  // Mặc định: 10.000
	NumWriters      int  // Số worker ghi đồng thời (mặc định: 4)
	NegativeControl bool // True nếu cố ý đọc low SAU câu SELECT (AC2)
	OutputPath      string
}

// Result chứa kết quả báo cáo nghiệm thu dưới dạng JSON.
type Result struct {
	Iterations   int     `json:"iterations"`
	Violations   int     `json:"violations"`
	DurationSec  float64 `json:"duration_seconds"`
	MySQLVersion string  `json:"mysql_version"`
	TotalWrites  int     `json:"total_writes"`
}

type record struct {
	id   int64
	gtid mysql.GTIDSet
}

type writeStore struct {
	mu          sync.RWMutex
	records     []record
	verifiedIdx int
}

func (s *writeStore) add(id int64, gtid mysql.GTIDSet) {
	s.mu.Lock()
	s.records = append(s.records, record{id: id, gtid: gtid})
	s.mu.Unlock()
}

func (s *writeStore) getWindow() (int, []record) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	start := s.verifiedIdx
	if start >= len(s.records) {
		return start, nil
	}
	res := make([]record, len(s.records)-start)
	copy(res, s.records[start:])
	return start, res
}

func (s *writeStore) advanceVerified(newVerified int) {
	s.mu.Lock()
	if newVerified > s.verifiedIdx {
		s.verifiedIdx = newVerified
	}
	s.mu.Unlock()
}

// cloneBytes tạo bản sao byte slice mới để thoát khỏi buffer pool dùng chung của go-mysql.
func cloneBytes(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return string(b)
}

// RunWatermarkExperiment thực hiện thí nghiệm RS-2 theo S02 §2, §5 và D §6.1.
func RunWatermarkExperiment(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Iterations <= 0 {
		cfg.Iterations = 10000
	}
	if cfg.NumWriters <= 0 {
		cfg.NumWriters = 4
	}
	if cfg.User == "" {
		cfg.User = "root"
	}
	if cfg.DBName == "" {
		cfg.DBName = "test_watermark"
	}

	opt := func(c *client.Conn) error {
		return c.SetCapability(mysql.CLIENT_SESSION_TRACK)
	}

	// 1. Kết nối admin khởi tạo schema riêng cho watermark
	initConn, err := client.Connect(cfg.Addr, cfg.User, cfg.Password, "", opt)
	if err != nil {
		return nil, fmt.Errorf("failed to connect for initialization: %w", err)
	}
	defer initConn.Close()

	var mysqlVersion string
	rVer, err := initConn.Execute("SELECT @@version;")
	if err == nil && len(rVer.Values) > 0 {
		rawVer, _ := rVer.GetString(0, 0)
		mysqlVersion = cloneBytes(rawVer)
	}

	// Bật session_track_gtids cấp GLOBAL
	_, _ = initConn.Execute("SET GLOBAL session_track_gtids = 'OWN_GTID';")

	_, _ = initConn.Execute(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", cfg.DBName))
	_, _ = initConn.Execute(fmt.Sprintf("USE %s;", cfg.DBName))
	_, _ = initConn.Execute("DROP TABLE IF EXISTS wm_watermark_records;")
	schemaSQL := `CREATE TABLE wm_watermark_records (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		writer_id INT NOT NULL,
		val INT NOT NULL,
		INDEX idx_writer (writer_id)
	) ENGINE=InnoDB;`
	if _, err := initConn.Execute(schemaSQL); err != nil {
		return nil, fmt.Errorf("failed to create table wm_watermark_records: %w", err)
	}

	store := &writeStore{}
	var stopWriters atomic.Bool
	var wg sync.WaitGroup

	// BufferPoolMu bảo vệ tránh buffer pool race trong go-mysql
	var poolMu sync.Mutex

	// 2. Khởi chạy concurrent writers liên tục ghi và theo dõi GTID chính mình qua session_track_gtids
	for w := 1; w <= cfg.NumWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			wConn, err := client.Connect(cfg.Addr, cfg.User, cfg.Password, cfg.DBName, opt)
			if err != nil {
				return
			}
			defer wConn.Close()

			poolMu.Lock()
			_, _ = wConn.Execute("SET SESSION session_track_gtids = 'OWN_GTID';")
			poolMu.Unlock()

			v := 0
			for !stopWriters.Load() {
				select {
				case <-ctx.Done():
					return
				default:
				}

				v++
				var insertID uint64
				var gtidCopy string

				poolMu.Lock()
				r, err := wConn.Execute("INSERT INTO wm_watermark_records (writer_id, val) VALUES (?, ?);", writerID, v)
				if err == nil && r != nil {
					insertID = r.InsertId
					if r.SessionTracking != nil && len(r.SessionTracking.GTID) > 0 {
						gtidCopy = cloneBytes(r.SessionTracking.GTID)
					}
				}
				poolMu.Unlock()

				if gtidCopy != "" {
					gSet, err := mysql.ParseMysqlGTIDSet(gtidCopy)
					if err == nil {
						store.add(int64(insertID), gSet)
					}
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	// Đợi writers ghi một số rows ban đầu
	time.Sleep(50 * time.Millisecond)

	// 3. Reader chạy N vòng thí nghiệm
	readerConn, err := client.Connect(cfg.Addr, cfg.User, cfg.Password, cfg.DBName, opt)
	if err != nil {
		stopWriters.Store(true)
		wg.Wait()
		return nil, fmt.Errorf("failed to connect reader: %w", err)
	}
	defer readerConn.Close()

	start := time.Now()
	violations := 0

	for i := 0; i < cfg.Iterations; i++ {
		select {
		case <-ctx.Done():
			stopWriters.Store(true)
			wg.Wait()
			return nil, ctx.Err()
		default:
		}

		var lowSet mysql.GTIDSet
		var seenMap map[int64]bool

		if !cfg.NegativeControl {
			// [QUY CHUẨN ĐÚNG: S02 §2 (RS-2), §5; D §6.1]
			// Bước 1: Đọc low = @@GLOBAL.gtid_executed TRƯỚC
			poolMu.Lock()
			rLow, err := readerConn.Execute("SELECT @@GLOBAL.gtid_executed;")
			var rawLow string
			if err == nil && len(rLow.Values) > 0 {
				raw, _ := rLow.GetString(0, 0)
				rawLow = cloneBytes(raw)
			}
			poolMu.Unlock()

			if err != nil {
				stopWriters.Store(true)
				wg.Wait()
				return nil, fmt.Errorf("read low failed at iter %d: %w", i, err)
			}
			lowSet, err = mysql.ParseMysqlGTIDSet(rawLow)
			if err != nil {
				stopWriters.Store(true)
				wg.Wait()
				return nil, fmt.Errorf("parse low failed: %w", err)
			}

			// Bước 2: Chạy SELECT đọc snapshot dữ liệu
			poolMu.Lock()
			rSel, err := readerConn.Execute("SELECT id FROM wm_watermark_records;")
			var ids []int64
			if err == nil && rSel != nil {
				ids = make([]int64, len(rSel.Values))
				for idx, row := range rSel.Values {
					ids[idx] = row[0].AsInt64()
				}
			}
			poolMu.Unlock()

			if err != nil {
				stopWriters.Store(true)
				wg.Wait()
				return nil, fmt.Errorf("select failed at iter %d: %w", i, err)
			}
			seenMap = make(map[int64]bool, len(ids))
			for _, id := range ids {
				seenMap[id] = true
			}
		} else {
			// [NEGATIVE CONTROL: AC2 - Cố ý đọc SELECT TRƯỚC, rồi mới đọc low]
			poolMu.Lock()
			rSel, err := readerConn.Execute("SELECT id FROM wm_watermark_records;")
			var ids []int64
			if err == nil && rSel != nil {
				ids = make([]int64, len(rSel.Values))
				for idx, row := range rSel.Values {
					ids[idx] = row[0].AsInt64()
				}
			}
			poolMu.Unlock()

			if err != nil {
				stopWriters.Store(true)
				wg.Wait()
				return nil, fmt.Errorf("select failed at iter %d: %w", i, err)
			}
			seenMap = make(map[int64]bool, len(ids))
			for _, id := range ids {
				seenMap[id] = true
			}

			// Đọc low SAU câu SELECT
			poolMu.Lock()
			rLow, err := readerConn.Execute("SELECT @@GLOBAL.gtid_executed;")
			var rawLow string
			if err == nil && len(rLow.Values) > 0 {
				raw, _ := rLow.GetString(0, 0)
				rawLow = cloneBytes(raw)
			}
			poolMu.Unlock()

			if err != nil {
				stopWriters.Store(true)
				wg.Wait()
				return nil, fmt.Errorf("read low failed at iter %d: %w", i, err)
			}
			lowSet, err = mysql.ParseMysqlGTIDSet(rawLow)
			if err != nil {
				stopWriters.Store(true)
				wg.Wait()
				return nil, fmt.Errorf("parse low failed: %w", err)
			}
		}

		// Bước 3: Kiểm tra bất biến RS-2 trên cửa sổ chưa xác minh
		windowStart, windowRecs := store.getWindow()
		firstUnverifiedOffset := 0
		allPrefixVerified := true

		for idx, rec := range windowRecs {
			if lowSet.Contain(rec.gtid) {
				if !seenMap[rec.id] {
					violations++
					allPrefixVerified = false
				} else if allPrefixVerified && idx == firstUnverifiedOffset {
					firstUnverifiedOffset++
				}
			} else {
				allPrefixVerified = false
			}
		}

		if firstUnverifiedOffset > 0 && !cfg.NegativeControl {
			store.advanceVerified(windowStart + firstUnverifiedOffset)
		}

		if !cfg.NegativeControl && i > 0 && i%50 == 0 {
			var cutoffID int64
			store.mu.RLock()
			if store.verifiedIdx > 0 && store.verifiedIdx < len(store.records) {
				cutoffID = store.records[store.verifiedIdx].id
			}
			store.mu.RUnlock()
			if cutoffID > 0 {
				poolMu.Lock()
				_, _ = readerConn.Execute(fmt.Sprintf("DELETE FROM wm_watermark_records WHERE id < %d;", cutoffID))
				poolMu.Unlock()
			}
		}
	}

	stopWriters.Store(true)
	wg.Wait()

	duration := time.Since(start).Seconds()
	res := &Result{
		Iterations:   cfg.Iterations,
		Violations:   violations,
		DurationSec:  duration,
		MySQLVersion: mysqlVersion,
		TotalWrites:  len(store.records),
	}

	if cfg.OutputPath != "" {
		data, err := json.MarshalIndent(res, "", "  ")
		if err == nil {
			_ = os.WriteFile(cfg.OutputPath, data, 0644)
		}
	}

	return res, nil
}
