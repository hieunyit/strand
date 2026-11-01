package verify

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

// spec: design.md §7:
// "Với PK có event trong cửa sổ đọc [low, high]: kiểm tra giá trị đọc được ở source
// có nằm trong tập {trạng thái trước cửa sổ, các image của event trong cửa sổ} hay không (image-membership check).
// Ở phía đích, row chắc chắn khớp image cuối, vì đó là cách writer apply.
// Row vẫn không kiểm được, vì event quá dày đến mức không có cửa sổ nào yên, được báo riêng là unverifiable_hot, không báo là lệch."

type rowRecord struct {
	pk         any
	images     []map[string]any
	eventCount int
	lastSeen   time.Time
}

// ImageTracker theo dõi các image của row trong cửa sổ CDC để phục vụ image-membership check.
type ImageTracker struct {
	mu           sync.RWMutex
	records      map[string]*rowRecord
	hotThreshold int // Ngưỡng số event để coi là hot row (mặc định 5)
	windowTTL    time.Duration
}

// NewImageTracker khởi tạo ImageTracker mới.
func NewImageTracker(hotThreshold int, windowTTL time.Duration) *ImageTracker {
	if hotThreshold <= 0 {
		hotThreshold = 5
	}
	if windowTTL <= 0 {
		windowTTL = 30 * time.Second
	}
	return &ImageTracker{
		records:      make(map[string]*rowRecord),
		hotThreshold: hotThreshold,
		windowTTL:    windowTTL,
	}
}

func makeKey(schema, table string, pk any) string {
	return fmt.Sprintf("%s.%s:%v", schema, table, pk)
}

// RecordRowImage ghi nhận một trạng thái image mới của row từ luồng CDC.
func (t *ImageTracker) RecordRowImage(schema, table string, pk any, image map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := makeKey(schema, table, pk)
	rec, ok := t.records[k]
	now := time.Now()
	if !ok {
		rec = &rowRecord{
			pk:         pk,
			images:     make([]map[string]any, 0, 8),
			eventCount: 0,
			lastSeen:   now,
		}
		t.records[k] = rec
	}

	rec.eventCount++
	rec.lastSeen = now
	// Sao chép map image để tránh bị sửa đổi từ bên ngoài
	cp := make(map[string]any, len(image))
	for k, v := range image {
		cp[k] = v
	}
	rec.images = append(rec.images, cp)
}

// GetDeltaRanges trả về các range PK chứa các row vừa có event trong cửa sổ CDC (cho ModeDelta).
func (t *ImageTracker) GetDeltaRanges(schema, table string) [][2]any {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var ranges [][2]any
	prefix := fmt.Sprintf("%s.%s:", schema, table)
	for k, rec := range t.records {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			ranges = append(ranges, [2]any{rec.pk, rec.pk})
		}
	}
	return ranges
}

// Check thực hiện image-membership check và hot-row check cho một row.
func (t *ImageTracker) Check(schema, table string, pk any, srcVal, dstVal map[string]any) (isMember bool, isHot bool, eventCount int) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	k := makeKey(schema, table, pk)
	rec, ok := t.records[k]
	if !ok {
		return false, false, 0
	}

	eventCount = rec.eventCount

	// Nếu số event vượt ngưỡng hotThreshold -> đây là hot row nhận update liên tục
	if rec.eventCount >= t.hotThreshold {
		isHot = true
	}

	// Image-membership check: kiểm tra srcVal hoặc dstVal có khớp với bất kỳ image nào trong cửa sổ không
	for _, img := range rec.images {
		if matchRow(srcVal, img) || matchRow(dstVal, img) {
			isMember = true
			break
		}
	}

	return isMember, isHot, eventCount
}

func matchRow(a, b map[string]any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	for k, vA := range a {
		vB, ok := b[k]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(vA, vB) && fmt.Sprintf("%v", vA) != fmt.Sprintf("%v", vB) {
			return false
		}
	}
	return true
}
