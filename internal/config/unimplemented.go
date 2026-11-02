package config

import (
	"fmt"
	"sort"
	"strings"
)

// UnimplementedKeys trả về các key cấu hình đang được đặt trong cfg mà bản này nhận (để file cấu hình
// hợp lệ theo spec không bị từ chối) nhưng chưa có hành vi. Không bao giờ bỏ qua im lặng:
// validate in cảnh báo cho từng key, sync ghi log cảnh báo.
func (c *Config) UnimplementedKeys() []string {
	if c == nil {
		return nil
	}
	var keys []string
	if strings.EqualFold(c.Source.ReadFrom, "standby") {
		keys = append(keys, "source.read_from=standby")
	}
	if len(c.Tables.Mask) > 0 {
		keys = append(keys, "tables.mask")
	}
	if c.Resources.Source.Bandwidth.isSet() {
		keys = append(keys, "resources.source.bandwidth")
	}
	if c.Resources.Target.Bandwidth.isSet() {
		keys = append(keys, "resources.target.bandwidth")
	}
	if c.Resources.Network.BackupRead.isSet() {
		keys = append(keys, "resources.network.backup_read")
	}
	if len(c.Resources.Schedule) > 0 {
		keys = append(keys, "resources.schedule")
	}

	tables := make([]string, 0, len(c.Bootstrap.PerTable))
	for t := range c.Bootstrap.PerTable {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	for _, t := range tables {
		s := c.Bootstrap.PerTable[t].Seed
		if s == nil {
			continue
		}
		prefix := fmt.Sprintf("bootstrap.per_table.%q.seed.", t)
		if s.Credentials != "" {
			keys = append(keys, prefix+"credentials")
		}
		if s.EncryptionKey != "" {
			keys = append(keys, prefix+"encryption_key")
		}
		if s.Load != nil {
			keys = append(keys, prefix+"load")
		}
		if s.VerifyAfter != nil {
			keys = append(keys, prefix+"verify_after")
		}
	}
	return keys
}

// UnimplementedWarning là câu cảnh báo chuẩn cho một key đã nhận nhưng chưa triển khai.
func UnimplementedWarning(key string) string {
	return fmt.Sprintf("config key %s is accepted but not implemented in this version", key)
}
