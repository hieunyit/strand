// Package pg cung cấp bộ đọc PostgreSQL logical replication (pgoutput) theo spec/02-specs.md §3, §8.
package pg

import (
	"fmt"
	"regexp"
	"time"

	"strand/internal/exitcode"
)

// Config chứa cấu hình kết nối và replication của PostgreSQL Reader.
// spec: 02 §3, §8
type Config struct {
	DSN            string
	Slot           string
	Publication    string
	CreateSlot     bool
	Failover       bool          /* PG17+ */
	StatusInterval time.Duration // chu kỳ gửi StandbyStatusUpdate, mặc định 10s
	BufferSize     int           // dung lượng kênh Event, mặc định 1024
}

// slotNameRe theo quy tắc tên slot của PostgreSQL (ReplicationSlotValidateName).
var slotNameRe = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

// validate kiểm tra và chuẩn hóa cấu hình Config.
func (c *Config) validate() error {
	if c.DSN == "" {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			"PostgreSQL DSN is required",
			nil,
		)
	}
	if c.Slot == "" {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			"PostgreSQL replication slot name is required",
			nil,
		)
	}
	if !slotNameRe.MatchString(c.Slot) {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			fmt.Sprintf("tên replication slot %q không hợp lệ (chỉ a-z, 0-9, _; tối đa 63 ký tự)", c.Slot),
			nil,
		)
	}
	if c.Publication == "" {
		return exitcode.New(
			exitcode.ErrBinlogConfigInvalid,
			exitcode.Rejected,
			"PostgreSQL publication name is required",
			nil,
		)
	}
	if c.StatusInterval <= 0 {
		c.StatusInterval = 10 * time.Second
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 1024
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("pg.Config{Slot:%s, Publication:%s, CreateSlot:%v, Failover:%v, StatusInterval:%v}",
		c.Slot, c.Publication, c.CreateSlot, c.Failover, c.StatusInterval)
}
