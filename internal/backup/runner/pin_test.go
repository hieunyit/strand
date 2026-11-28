package runner

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// A restore pins what it reads; retention running meanwhile (possibly in another process) skips the
// pinned backups instead of deleting them mid-restore, and expires them once the pin is released.
func TestPruneSkipsPinnedRestorePoints(t *testing.T) {
	vars := map[string]string{"STRAND_BACKUP_S3_ENDPOINT": "http://127.0.0.1:1", "STRAND_BACKUP_S3_ACCESS_KEY": "k",
		"STRAND_BACKUP_S3_SECRET_KEY": "s", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true"}
	st := store.NewMem()
	e := testEnv(t, vars, st)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	e.Now = func() time.Time { return now }
	mk := func(id string, at time.Time) {
		r := backup.Record{ID: id, Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central-backups",
			ObjectKey: "prod/" + id, Status: backup.StatusQueued, CreatedAt: at, Method: backup.MethodPGDump, JobID: "bjob-pin"}
		if _, _, err := e.Catalog.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		_, _, _ = e.Catalog.UpdateStatus(ctx, id, backup.StatusUploading, nil)
		_, _, _ = e.Catalog.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
		if _, _, err := e.Catalog.Finalize(ctx, id, backup.StatusVerifying, 10, strings.Repeat("a", 64), "s3://central-backups/prod/"+id, at); err != nil {
			t.Fatal(err)
		}
	}
	mk("bkp-old", now.Add(-48*time.Hour))
	mk("bkp-new", now.Add(-time.Hour))
	old, _, _ := e.Catalog.Get(ctx, "bkp-old")
	_, release, err := e.PinPlan(ctx, RestorePlan{Chain: []backup.Record{old}})
	if err != nil {
		t.Fatal(err)
	}
	job := backup.Job{ID: "bjob-pin", JobOptions: backup.JobOptions{Retention: backup.RetentionPolicy{KeepLast: 1}}}
	res, err := e.PruneJob(ctx, job, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Expired) != 0 || len(res.Errors) != 0 || len(res.Skipped) != 1 || res.Skipped[0] != "bkp-old" {
		t.Fatalf("prune during a restore = %+v", res)
	}
	if r, _, _ := e.Catalog.Get(ctx, "bkp-old"); r.Status != backup.StatusComplete {
		t.Fatalf("pinned restore point = %s", r.Status)
	}
	release()
	release() // idempotent
	// Released: retention claims it now (the storage delete then fails against the fake endpoint, but the
	// claim proves the pin is gone).
	cctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	res, _ = e.PruneJob(cctx, job, false)
	if len(res.Skipped) != 0 {
		t.Fatalf("prune after release = %+v", res)
	}
	if r, _, _ := e.Catalog.Get(ctx, "bkp-old"); r.Status == backup.StatusComplete {
		t.Fatalf("released restore point still complete: %+v", res)
	}
	// A record retention already claimed cannot be pinned for a restore.
	if _, _, err := e.PinRecords(ctx, []string{"bkp-old"}); err == nil {
		t.Fatal("pinned a restore point that retention is deleting")
	}
}

type flakyPins struct {
	store.Store
	fail atomic.Bool
}

func (f *flakyPins) PinBackups(ctx context.Context, holder string, ids []string, ttl time.Duration) error {
	if f.fail.Load() {
		return errors.New("catalog unreachable")
	}
	return f.Store.PinBackups(ctx, holder, ids, ttl)
}

// Review DBA: gia hạn pin lỗi kéo dài thì reader phải dừng TRƯỚC khi pin hết hạn — nếu không, retention ở
// tiến trình khác có thể xóa đúng backup đang được restore.
func TestPinLapseCancelsTheReaderBeforeThePinExpires(t *testing.T) {
	old := PinTTL
	PinTTL = 800 * time.Millisecond
	t.Cleanup(func() { PinTTL = old })
	vars := map[string]string{"STRAND_BACKUP_S3_ENDPOINT": "http://127.0.0.1:1", "STRAND_BACKUP_S3_ACCESS_KEY": "k",
		"STRAND_BACKUP_S3_SECRET_KEY": "s", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true"}
	st := &flakyPins{Store: store.NewMem()}
	e := testEnv(t, vars, st)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	r := backup.Record{ID: "bkp-lapse", Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central-backups",
		ObjectKey: "prod/bkp-lapse", Status: backup.StatusQueued, CreatedAt: now, Method: backup.MethodPGDump, JobID: "bjob-lapse"}
	if _, _, err := e.Catalog.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	_, _, _ = e.Catalog.UpdateStatus(ctx, r.ID, backup.StatusUploading, nil)
	_, _, _ = e.Catalog.UpdateStatus(ctx, r.ID, backup.StatusVerifying, nil)
	if _, _, err := e.Catalog.Finalize(ctx, r.ID, backup.StatusVerifying, 10, strings.Repeat("a", 64), "s3://central-backups/prod/bkp-lapse", now); err != nil {
		t.Fatal(err)
	}
	pinned, release, err := e.PinRecords(ctx, []string{r.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// Gia hạn được: reader chạy quá TTL mà không bị dừng.
	time.Sleep(PinTTL * 3 / 2)
	if pinned.Err() != nil {
		t.Fatalf("reader cancelled while renewals succeed: %v", context.Cause(pinned))
	}
	st.fail.Store(true)
	start := time.Now()
	select {
	case <-pinned.Done():
	case <-time.After(PinTTL * 3):
		t.Fatal("renewals keep failing but the reader still runs past the pin's expiry")
	}
	if !errors.Is(context.Cause(pinned), ErrPinLapsed) {
		t.Fatalf("cause = %v, want ErrPinLapsed", context.Cause(pinned))
	}
	// Dừng trước khi pin (gia hạn thành công cuối cùng + TTL) có thể hết hạn.
	if waited := time.Since(start); waited >= PinTTL {
		t.Fatalf("reader stopped %v after renewals began failing; the pin may already have expired (TTL %v)", waited, PinTTL)
	}
}

// hangingPins: sau khi bật hang, PinBackups không trả về (bỏ qua cả context) cho tới khi unblock đóng —
// catalog ngừng trả lời hoặc chờ row lock.
type hangingPins struct {
	store.Store
	hang    atomic.Bool
	unblock chan struct{}
}

func (h *hangingPins) PinBackups(ctx context.Context, holder string, ids []string, ttl time.Duration) error {
	if h.hang.Load() {
		<-h.unblock
		return errors.New("catalog stopped answering")
	}
	return h.Store.PinBackups(ctx, holder, ids, ttl)
}

// Nghiệm thu lại: lệnh gia hạn treo không được giữ reader chạy qua hạn của pin.
func TestPinLapseCancelsTheReaderWhenARenewalHangs(t *testing.T) {
	old := PinTTL
	PinTTL = 800 * time.Millisecond
	t.Cleanup(func() { PinTTL = old })
	vars := map[string]string{"STRAND_BACKUP_S3_ENDPOINT": "http://127.0.0.1:1", "STRAND_BACKUP_S3_ACCESS_KEY": "k",
		"STRAND_BACKUP_S3_SECRET_KEY": "s", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true"}
	st := &hangingPins{Store: store.NewMem(), unblock: make(chan struct{})}
	e := testEnv(t, vars, st)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	r := backup.Record{ID: "bkp-hang", Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central-backups",
		ObjectKey: "prod/bkp-hang", Status: backup.StatusQueued, CreatedAt: now, Method: backup.MethodPGDump, JobID: "bjob-hang"}
	if _, _, err := e.Catalog.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	_, _, _ = e.Catalog.UpdateStatus(ctx, r.ID, backup.StatusUploading, nil)
	_, _, _ = e.Catalog.UpdateStatus(ctx, r.ID, backup.StatusVerifying, nil)
	if _, _, err := e.Catalog.Finalize(ctx, r.ID, backup.StatusVerifying, 10, strings.Repeat("a", 64), "s3://central-backups/prod/bkp-hang", now); err != nil {
		t.Fatal(err)
	}
	pinned, release, err := e.PinRecords(ctx, []string{r.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	defer close(st.unblock)
	time.Sleep(PinTTL * 3 / 2)
	if pinned.Err() != nil {
		t.Fatalf("reader cancelled while renewals succeed: %v", context.Cause(pinned))
	}
	st.hang.Store(true)
	start := time.Now()
	select {
	case <-pinned.Done():
	case <-time.After(PinTTL * 3):
		t.Fatal("a renewal hangs but the reader still runs past the pin's expiry")
	}
	if !errors.Is(context.Cause(pinned), ErrPinLapsed) {
		t.Fatalf("cause = %v, want ErrPinLapsed", context.Cause(pinned))
	}
	if waited := time.Since(start); waited >= PinTTL {
		t.Fatalf("reader stopped %v after renewals began hanging; the pin may already have expired (TTL %v)", waited, PinTTL)
	}
}
