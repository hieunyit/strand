package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"strand/internal/backup"
)

type memBackupPro struct {
	checks   []backup.CheckResult
	checkSeq int64
	repos    map[string]backup.Repo
	notified map[string]time.Time
	missed   map[string]time.Time
	verified map[string]time.Time
	pruned   map[string]time.Time
	pins     map[string]map[string]time.Time // backup id -> holder -> expiry
}

func (p *memBackupPro) init() {
	if p.repos == nil {
		p.repos, p.notified, p.missed = map[string]backup.Repo{}, map[string]time.Time{}, map[string]time.Time{}
		p.verified, p.pruned = map[string]time.Time{}, map[string]time.Time{}
	}
}

func optTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := *t
	return &v
}

func cloneBackupPro(r backup.Record) backup.Record {
	r.StartTime, r.EndTime, r.RetainUntil = optTime(r.StartTime), optTime(r.EndTime), optTime(r.RetainUntil)
	r.CheckedAt, r.ExpiredAt = optTime(r.CheckedAt), optTime(r.ExpiredAt)
	r.Kind, r.Format = kindOrFull(r.Kind), formatOrRaw(r.Format)
	if r.Details != nil {
		d := make(map[string]string, len(r.Details))
		for k, v := range r.Details {
			d[k] = v
		}
		r.Details = d
	}
	return r
}

func memBackupProFilter(r backup.Record, f backup.Filter) bool {
	kind := kindOrFull(r.Kind)
	return (f.JobID == "" || r.JobID == f.JobID) && (f.Kind == "" || kind == f.Kind) &&
		(f.NotKind == "" || kind != f.NotKind) && (f.Repository == "" || r.Repository == f.Repository)
}

func (m *Mem) UpdateBackupMeta(_ context.Context, id string, meta backup.Meta) (backup.Record, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	if !ok {
		return backup.Record{}, false, nil
	}
	if err := backup.ValidateMeta(r.Engine, meta); err != nil {
		return backup.Record{}, false, err
	}
	if r.Status != backup.StatusUploading && r.Status != backup.StatusVerifying {
		return backup.Record{}, false, fmt.Errorf("%w: metadata can change only while uploading or verifying", backup.ErrTransition)
	}
	r.ApplyMeta(meta)
	m.backups[id] = cloneBackup(r)
	return cloneBackup(r), true, nil
}

func (m *Mem) ClaimBackupDeletion(_ context.Context, id string, at time.Time) (backup.Record, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	if !ok || r.Status == backup.StatusExpired {
		return cloneBackup(r), false, nil
	}
	if r.Status == backup.StatusDeleting {
		return cloneBackup(r), true, nil
	}
	if r.Status == backup.StatusComplete && m.pinnedLocked(id) {
		return backup.Record{}, false, fmt.Errorf("%w: %s", backup.ErrPinned, id)
	}
	// Like the PG store, the Object Lock is checked against the store's clock, not the caller's.
	if r.Status != backup.StatusComplete || r.LegalHold || (r.RetainUntil != nil && r.RetainUntil.After(m.dbNow())) {
		return cloneBackup(r), false, fmt.Errorf("%w: backup is not eligible for deletion (status, legal hold or retention lock)", backup.ErrInvalid)
	}
	r.Status = backup.StatusDeleting
	m.backups[id] = cloneBackup(r)
	return cloneBackup(r), true, nil
}

func (m *Mem) ExpireBackup(_ context.Context, id string, at time.Time) (backup.Record, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	if !ok {
		return backup.Record{}, false, nil
	}
	switch {
	case r.Status == backup.StatusExpired:
		return cloneBackup(r), false, nil
	case r.Status != backup.StatusComplete && r.Status != backup.StatusDeleting:
		return backup.Record{}, false, backup.ErrTransition
	case r.LegalHold:
		return backup.Record{}, false, fmt.Errorf("%w: legal-hold backup cannot be expired", backup.ErrInvalid)
	case r.Status == backup.StatusComplete && m.pinnedLocked(id):
		return backup.Record{}, false, fmt.Errorf("%w: %s", backup.ErrPinned, id)
	case r.RetainUntil != nil && r.RetainUntil.After(m.dbNow()):
		return backup.Record{}, false, fmt.Errorf("%w: backup is object-locked until %s", backup.ErrInvalid, r.RetainUntil.UTC().Format(time.RFC3339))
	}
	r.Status = backup.StatusExpired
	t := at.UTC()
	r.ExpiredAt = &t
	m.backups[id] = cloneBackup(r)
	return cloneBackup(r), true, nil
}

// pinnedLocked reports a live pin on id (m.mu held).
func (m *Mem) pinnedLocked(id string) bool {
	now := m.dbNow()
	for _, until := range m.pro.pins[id] {
		if until.After(now) {
			return true
		}
	}
	return false
}

func (m *Mem) PinBackups(_ context.Context, holder string, ids []string, ttl time.Duration) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	for _, id := range ids {
		if err := pinnableErr(id, string(m.backups[id].Status)); err != nil {
			return err
		}
	}
	if m.pro.pins == nil {
		m.pro.pins = map[string]map[string]time.Time{}
	}
	until := m.dbNow().Add(ttl)
	for _, id := range ids {
		if m.pro.pins[id] == nil {
			m.pro.pins[id] = map[string]time.Time{}
		}
		m.pro.pins[id][holder] = until
	}
	return nil
}

func (m *Mem) ReleaseBackupPins(_ context.Context, holder string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	for id, hs := range m.pro.pins {
		delete(hs, holder)
		if len(hs) == 0 {
			delete(m.pro.pins, id)
		}
	}
	return nil
}

func (m *Mem) SetBackupLegalHold(_ context.Context, id string, hold bool) (backup.Record, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	if !ok {
		return backup.Record{}, false, nil
	}
	if r.Status == backup.StatusExpired || r.Status == backup.StatusDeleting {
		return backup.Record{}, false, fmt.Errorf("%w: a backup pending deletion or expired cannot have its hold changed", backup.ErrInvalid)
	}
	changed := r.LegalHold != hold
	r.LegalHold = hold
	m.backups[id] = cloneBackup(r)
	return cloneBackup(r), changed, nil
}

func (m *Mem) DeleteBackup(_ context.Context, id string, now time.Time) (backup.Record, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	if !ok {
		return backup.Record{}, false, nil
	}
	if err := backup.DeletableErr(r, now); err != nil {
		return backup.Record{}, false, err
	}
	if err := backup.DeletableErr(r, m.dbNow()); err != nil { // the store's clock decides the Object Lock
		return backup.Record{}, false, err
	}
	delete(m.backups, id)
	delete(m.pro.pins, id)
	kept := m.pro.checks[:0]
	for _, c := range m.pro.checks {
		if c.BackupID != id {
			kept = append(kept, c)
		}
	}
	m.pro.checks = kept
	return cloneBackup(r), true, nil
}

func (m *Mem) AddBackupCheck(_ context.Context, c backup.CheckResult) (int64, error) {
	if err := m.enter(); err != nil {
		return 0, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[c.BackupID]
	if !ok {
		return 0, fmt.Errorf("%w: unknown backup", backup.ErrInvalid)
	}
	m.pro.checkSeq++
	c.ID = m.pro.checkSeq
	m.pro.checks = append(m.pro.checks, c)
	t := c.FinishedAt
	r.CheckedAt, r.CheckStatus, r.CheckDetail = &t, c.Status, truncate(c.Detail, 2000)
	m.backups[c.BackupID] = cloneBackup(r)
	return c.ID, nil
}

func (m *Mem) ListBackupChecks(_ context.Context, backupID string, limit int) ([]backup.CheckResult, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	out := []backup.CheckResult{}
	for i := len(m.pro.checks) - 1; i >= 0 && len(out) < limit; i-- {
		if backupID == "" || m.pro.checks[i].BackupID == backupID {
			out = append(out, m.pro.checks[i])
		}
	}
	return out, nil
}

func (m *Mem) BackupSummaries(_ context.Context) ([]backup.SourceSummary, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	by := map[string]*backup.SourceSummary{}
	var points []backup.Record
	for _, r := range m.backups {
		s := by[r.Source]
		if s == nil {
			s = &backup.SourceSummary{Source: r.Source, Engine: r.Engine}
			by[r.Source] = s
		}
		if r.JobID > s.JobID {
			s.JobID = r.JobID
		}
		if r.Status == backup.StatusFailed && (s.LastFailedAt == nil || r.CreatedAt.After(*s.LastFailedAt)) {
			s.LastFailedAt = optTime(&r.CreatedAt)
		}
		if r.Status != backup.StatusComplete {
			continue
		}
		s.TotalBytes += r.Bytes
		if kindOrFull(r.Kind) == backup.KindLog {
			s.LogSegments++
			start, end := r.CreatedAt, r.CreatedAt
			if r.StartTime != nil {
				start = *r.StartTime
			}
			if r.EndTime != nil {
				end = *r.EndTime
			}
			if s.LogStart == nil || start.Before(*s.LogStart) {
				s.LogStart = optTime(&start)
			}
			if s.LogEnd == nil || end.After(*s.LogEnd) {
				s.LogEnd = optTime(&end)
			}
			continue
		}
		s.Complete++
		points = append(points, cloneBackup(r))
	}
	backup.SortOldestFirst(points)
	for _, r := range points {
		s := by[r.Source]
		rc := r
		s.LastSuccess = &rc
		s.RecentSizes = append(s.RecentSizes, r.Bytes)
		if len(s.RecentSizes) > 10 {
			s.RecentSizes = s.RecentSizes[1:]
		}
	}
	out := make([]backup.SourceSummary, 0, len(by))
	for _, s := range by {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Source < out[j].Source })
	return out, nil
}

func (m *Mem) CreateBackupRepo(_ context.Context, r backup.Repo) error {
	if err := backup.ValidateRepo(r); err != nil {
		return err
	}
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.pro.init()
	for _, o := range m.pro.repos {
		if o.ID == r.ID || o.Name == r.Name {
			return backup.ErrConflict
		}
	}
	r.CreatedAt = time.Now().UTC()
	r.UpdatedAt = r.CreatedAt
	r.LastTestAt, r.LastTestStatus, r.LastTestDetail, r.LastTestSteps = nil, "", "", nil
	m.pro.repos[r.ID] = cloneRepo(r)
	return nil
}

func cloneRepo(r backup.Repo) backup.Repo {
	r.AccessKeySealed = append([]byte(nil), r.AccessKeySealed...)
	r.SecretKeySealed = append([]byte(nil), r.SecretKeySealed...)
	r.AccessKeySet, r.SecretKeySet = len(r.AccessKeySealed) > 0, len(r.SecretKeySealed) > 0
	if !r.AccessKeySet {
		r.AccessKeySealed = nil
	}
	if !r.SecretKeySet {
		r.SecretKeySealed = nil
	}
	r.LastTestAt = optTime(r.LastTestAt)
	r.LastTestSteps = append([]byte(nil), r.LastTestSteps...)
	if r.Kind == "" {
		r.Kind = backup.RepoKindS3
	}
	if r.SecretMode == "" {
		r.SecretMode = backup.RepoSecretEnv
	}
	return r
}

func (m *Mem) SetBackupRepoTest(_ context.Context, id string, at time.Time, status, detail string, steps []byte) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	r, ok := m.pro.repos[id]
	if !ok {
		return false, nil
	}
	r.LastTestAt, r.LastTestStatus, r.LastTestDetail, r.LastTestSteps = &at, status, detail, append([]byte(nil), steps...)
	m.pro.repos[id] = r
	return true, nil
}

func (m *Mem) GetBackupRepo(_ context.Context, id string) (backup.Repo, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Repo{}, false, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	r, ok := m.pro.repos[id]
	return cloneRepo(r), ok, nil
}

func (m *Mem) ListBackupRepos(_ context.Context) ([]backup.Repo, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	out := []backup.Repo{}
	for _, r := range m.pro.repos {
		out = append(out, cloneRepo(r))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *Mem) UpdateBackupRepo(_ context.Context, r backup.Repo) (bool, error) {
	if err := backup.ValidateRepo(r); err != nil {
		return false, err
	}
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	old, ok := m.pro.repos[r.ID]
	if !ok {
		return false, nil
	}
	for _, o := range m.pro.repos {
		if o.ID != r.ID && o.Name == r.Name {
			return false, backup.ErrConflict
		}
	}
	r.CreatedAt, r.UpdatedAt = old.CreatedAt, time.Now().UTC()
	r.LastTestAt, r.LastTestStatus, r.LastTestDetail, r.LastTestSteps = old.LastTestAt, old.LastTestStatus, old.LastTestDetail, old.LastTestSteps
	m.pro.repos[r.ID] = cloneRepo(r)
	return true, nil
}

func (m *Mem) DeleteBackupRepo(_ context.Context, id string) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	used := 0
	for _, b := range m.backups {
		if b.Repository == id && b.Status != backup.StatusExpired {
			used++
		}
	}
	if used > 0 {
		return false, fmt.Errorf("%w: repository still holds %d backups", backup.ErrInvalid, used)
	}
	if _, ok := m.pro.repos[id]; !ok {
		return false, nil
	}
	delete(m.pro.repos, id)
	return true, nil
}

func (m *Mem) MarkBackupJobNotified(_ context.Context, id string, runFinishedAt time.Time) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	j, ok := m.backupJobs[id]
	if !ok || j.RunFinishedAt == nil || !j.RunFinishedAt.Equal(runFinishedAt) {
		return false, nil
	}
	if t, ok := m.pro.notified[id]; ok && t.Equal(runFinishedAt) {
		return false, nil
	}
	m.pro.notified[id] = runFinishedAt
	return true, nil
}

func (m *Mem) MarkBackupJobMissed(_ context.Context, id string, dueAt time.Time) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	j, ok := m.backupJobs[id]
	if !ok || j.NextRunAt == nil || !j.NextRunAt.Equal(dueAt) {
		return false, nil
	}
	if t, ok := m.pro.missed[id]; ok && t.Equal(dueAt) {
		return false, nil
	}
	m.pro.missed[id] = dueAt
	return true, nil
}

func (m *Mem) ClaimBackupJobVerify(_ context.Context, id string, now time.Time, every time.Duration) (bool, error) {
	return m.claim(func(p *memBackupPro) map[string]time.Time { return p.verified }, id, now, every)
}

func (m *Mem) ClaimBackupJobPrune(_ context.Context, id string, now time.Time, every time.Duration) (bool, error) {
	return m.claim(func(p *memBackupPro) map[string]time.Time { return p.pruned }, id, now, every)
}

func (m *Mem) claim(pick func(*memBackupPro) map[string]time.Time, id string, now time.Time, every time.Duration) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.pro.init()
	if _, ok := m.backupJobs[id]; !ok {
		return false, nil
	}
	mp := pick(&m.pro)
	if last, ok := mp[id]; ok && last.After(now.Add(-every)) {
		return false, nil
	}
	mp[id] = now
	return true, nil
}
