package web

import (
	"strings"
	"testing"

	"strand/internal/admin"
)

// Trang pipeline cảnh báo "Source WAL" khi replication slot của source PG giữ WAL đáng lo (vượt giới hạn,
// từ 50% max_slot_wal_keep_size, hoặc >= 1 GiB khi không giới hạn); mức bình thường thì không.
func TestPipelinePageSlotWALWarning(t *testing.T) {
	const mb = int64(1) << 20
	for _, c := range []struct {
		name string
		wal  *admin.SlotWAL
		want string // rỗng: không có cảnh báo
	}{
		{"no slot", nil, ""},
		{"small, limited", &admin.SlotWAL{RetainedBytes: 10 * mb, KeepBytes: 1024 * mb, Status: "reserved"}, ""},
		{"small, unlimited", &admin.SlotWAL{RetainedBytes: 512 * mb, KeepBytes: -1, Status: "reserved"}, ""},
		{"half of limit", &admin.SlotWAL{RetainedBytes: 600 * mb, KeepBytes: 1024 * mb, Status: "extended"}, "keeps 600 MB of WAL, 58% of max_slot_wal_keep_size (1024 MB)"},
		{"unlimited, large", &admin.SlotWAL{RetainedBytes: 2048 * mb, KeepBytes: -1, Status: "extended"}, "max_slot_wal_keep_size = -1 (no limit)"},
		{"past limit", &admin.SlotWAL{RetainedBytes: 1100 * mb, KeepBytes: 1024 * mb, Status: "unreserved"}, "wal_status=unreserved"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newEnv(t, nil)
			e.mock.mockPipeline.slotWAL = c.wal
			e.sampleNow()
			cl := e.client()
			e.login(cl, testPassword)
			_, body := e.do(cl, "GET", "/pipelines/orders", "", nil)
			has := strings.Contains(body, "data-slot-wal")
			if c.want == "" {
				if has {
					t.Fatalf("unexpected Source WAL warning")
				}
				return
			}
			if !has || !strings.Contains(body, c.want) {
				t.Fatalf("want Source WAL warning containing %q", c.want)
			}
			_, js := e.do(cl, "GET", "/api/v1/pipelines/orders", "", nil)
			if !strings.Contains(js, `"slot_wal"`) || !strings.Contains(js, `"keep_bytes"`) {
				t.Fatalf("API lacks top.slot_wal: %s", js)
			}
		})
	}
}
