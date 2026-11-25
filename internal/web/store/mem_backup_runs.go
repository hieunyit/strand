package store

import (
	"context"
	"errors"
	"sort"
	"time"

	"strand/internal/backup"
)

func (m *Mem) CreateBackupRun(_ context.Context, r backup.Run) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if r.ID == "" || r.JobID == "" {
		return errors.New("backup run id and job_id are required")
	}
	if _, ok := m.backupJobs[r.JobID]; !ok {
		return errors.New("foreign key violation: backup job does not exist")
	}
	if m.backupRuns == nil {
		m.backupRuns = make(map[string]backup.Run)
	}
	if _, exists := m.backupRuns[r.ID]; exists {
		return errors.New("backup run already exists")
	}
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now().UTC()
	}
	if r.Attempt <= 0 {
		r.Attempt = 1
	}
	if r.Status == "" {
		r.Status = string(backup.RunStatusRunning)
	}
	m.backupRuns[r.ID] = r
	return nil
}

func (m *Mem) UpdateBackupRun(_ context.Context, r backup.Run) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	cur, ok := m.backupRuns[r.ID]
	if !ok {
		return false, nil
	}
	r.JobID = cur.JobID
	r.Trigger = cur.Trigger
	r.StartedAt = cur.StartedAt
	m.backupRuns[r.ID] = r
	return true, nil
}

func (m *Mem) FinishBackupRun(_ context.Context, id string, finishedAt time.Time, status string, bytesRead, bytesWritten int64, duration time.Duration, backupID, errorCode, errorMessage string) (backup.Run, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Run{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backupRuns[id]
	if !ok {
		return backup.Run{}, false, nil
	}
	r.FinishedAt = &finishedAt
	r.Status = status
	r.BytesRead = bytesRead
	r.BytesWritten = bytesWritten
	r.Duration = duration
	r.BackupID = backupID
	r.ErrorCode = errorCode
	r.ErrorMessage = errorMessage
	m.backupRuns[id] = r
	return r, true, nil
}

func (m *Mem) GetBackupRun(_ context.Context, id string) (backup.Run, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Run{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backupRuns[id]
	return r, ok, nil
}

func (m *Mem) ListBackupRuns(_ context.Context, f BackupRunFilter) ([]backup.Run, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var all []backup.Run
	for _, r := range m.backupRuns {
		if (f.JobID != "" && r.JobID != f.JobID) || (f.Status != "" && r.Status != f.Status) ||
			(f.ProcessID != "" && r.ProcessID != f.ProcessID) {
			continue
		}
		all = append(all, r)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].StartedAt.Equal(all[j].StartedAt) {
			return all[i].ID > all[j].ID
		}
		return all[i].StartedAt.After(all[j].StartedAt)
	})
	var out []backup.Run
	for _, r := range all {
		if f.After != nil {
			if r.StartedAt.After(f.After.CreatedAt) {
				continue
			}
			if r.StartedAt.Equal(f.After.CreatedAt) && r.ID >= f.After.ID {
				continue
			}
		}
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}
