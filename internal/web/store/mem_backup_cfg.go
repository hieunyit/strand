package store

import (
	"context"
	"sort"
	"time"

	"strand/internal/backup"
)

type memSlot struct {
	host    string
	renewed time.Time
}

type memBackupCfg struct {
	conns    map[string]backup.Connection
	policies map[string]backup.Policy
	slots    map[string]memSlot
}

func (c *memBackupCfg) init() {
	if c.conns == nil {
		c.conns, c.policies, c.slots = map[string]backup.Connection{}, map[string]backup.Policy{}, map[string]memSlot{}
	}
}

func cloneConn(c backup.Connection) backup.Connection {
	c.PasswordSealed = append([]byte(nil), c.PasswordSealed...)
	c.URISealed = append([]byte(nil), c.URISealed...)
	if len(c.PasswordSealed) == 0 {
		c.PasswordSealed = nil
	}
	if len(c.URISealed) == 0 {
		c.URISealed = nil
	}
	if c.SSHTunnel != nil {
		t := *c.SSHTunnel
		c.SSHTunnel = &t
	}
	if c.Labels != nil {
		l := make(map[string]string, len(c.Labels))
		for k, v := range c.Labels {
			l[k] = v
		}
		c.Labels = l
	}
	if c.LastTestAt != nil {
		t := *c.LastTestAt
		c.LastTestAt = &t
	}
	return c
}

func clonePolicy(p backup.Policy) backup.Policy {
	p.Schedule.Days = append([]int(nil), p.Schedule.Days...)
	if len(p.Schedule.Days) == 0 {
		p.Schedule.Days = nil
	}
	return p
}

func (m *Mem) CreateBackupConnection(_ context.Context, c backup.Connection) (backup.Connection, error) {
	c.PasswordSet, c.URISet = len(c.PasswordSealed) > 0, len(c.URISealed) > 0
	if err := c.Validate(); err != nil {
		return backup.Connection{}, err
	}
	if err := m.enter(); err != nil {
		return backup.Connection{}, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	for _, o := range m.cfg.conns {
		if o.ID == c.ID || o.Name == c.Name {
			return backup.Connection{}, backup.ErrConflict
		}
	}
	c.CreatedAt = time.Now().UTC()
	c.UpdatedAt = c.CreatedAt
	c.LastTestAt, c.LastTestStatus, c.LastTestDetail = nil, "", ""
	m.cfg.conns[c.ID] = cloneConn(c)
	return cloneConn(c), nil
}

func (m *Mem) GetBackupConnection(_ context.Context, id string) (backup.Connection, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Connection{}, false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	c, ok := m.cfg.conns[id]
	return cloneConn(c), ok, nil
}

func (m *Mem) ListBackupConnections(_ context.Context) ([]backup.Connection, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	out := make([]backup.Connection, 0, len(m.cfg.conns))
	for _, c := range m.cfg.conns {
		out = append(out, cloneConn(c))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name || (out[i].Name == out[j].Name && out[i].ID < out[j].ID)
	})
	return out, nil
}

func (m *Mem) UpdateBackupConnection(_ context.Context, c backup.Connection) (bool, error) {
	c.PasswordSet, c.URISet = len(c.PasswordSealed) > 0, len(c.URISealed) > 0
	if err := c.Validate(); err != nil {
		return false, err
	}
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	cur, ok := m.cfg.conns[c.ID]
	if !ok {
		return false, nil
	}
	for id, o := range m.cfg.conns {
		if id != c.ID && o.Name == c.Name {
			return false, backup.ErrConflict
		}
	}
	c.CreatedAt, c.CreatedBy = cur.CreatedAt, cur.CreatedBy
	c.LastTestAt, c.LastTestStatus, c.LastTestDetail = cur.LastTestAt, cur.LastTestStatus, cur.LastTestDetail
	c.UpdatedAt = time.Now().UTC()
	m.cfg.conns[c.ID] = cloneConn(c)
	return true, nil
}

func (m *Mem) DeleteBackupConnection(_ context.Context, id string) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	if _, ok := m.cfg.conns[id]; !ok {
		return false, nil
	}
	var jobs []backup.Job
	for _, j := range m.backupJobs {
		jobs = append(jobs, j)
	}
	if used := jobsUsing(jobs, "connection", id); len(used) > 0 {
		return false, &backup.InUseError{Jobs: used}
	}
	delete(m.cfg.conns, id)
	return true, nil
}

func (m *Mem) SetBackupConnectionTest(_ context.Context, id string, at time.Time, status, detail string) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	c, ok := m.cfg.conns[id]
	if !ok {
		return false, nil
	}
	t := at.UTC()
	c.LastTestAt, c.LastTestStatus, c.LastTestDetail = &t, status, detail
	m.cfg.conns[id] = c
	return true, nil
}

func (m *Mem) CreateBackupPolicy(_ context.Context, p backup.Policy) (backup.Policy, error) {
	if err := p.Validate(); err != nil {
		return backup.Policy{}, err
	}
	if err := m.enter(); err != nil {
		return backup.Policy{}, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	for _, o := range m.cfg.policies {
		if o.ID == p.ID || o.Name == p.Name {
			return backup.Policy{}, backup.ErrConflict
		}
	}
	p.CreatedAt = time.Now().UTC()
	p.UpdatedAt = p.CreatedAt
	m.cfg.policies[p.ID] = clonePolicy(p)
	return clonePolicy(p), nil
}

func (m *Mem) GetBackupPolicy(_ context.Context, id string) (backup.Policy, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Policy{}, false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	p, ok := m.cfg.policies[id]
	return clonePolicy(p), ok, nil
}

func (m *Mem) ListBackupPolicies(_ context.Context) ([]backup.Policy, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	out := make([]backup.Policy, 0, len(m.cfg.policies))
	for _, p := range m.cfg.policies {
		out = append(out, clonePolicy(p))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Builtin != out[j].Builtin {
			return out[i].Builtin
		}
		return out[i].Name < out[j].Name || (out[i].Name == out[j].Name && out[i].ID < out[j].ID)
	})
	return out, nil
}

func (m *Mem) UpdateBackupPolicy(_ context.Context, p backup.Policy) (bool, error) {
	if err := p.Validate(); err != nil {
		return false, err
	}
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	cur, ok := m.cfg.policies[p.ID]
	if !ok {
		return false, nil
	}
	for id, o := range m.cfg.policies {
		if id != p.ID && o.Name == p.Name {
			return false, backup.ErrConflict
		}
	}
	p.Builtin, p.CreatedAt, p.CreatedBy = cur.Builtin, cur.CreatedAt, cur.CreatedBy
	p.UpdatedAt = time.Now().UTC()
	m.cfg.policies[p.ID] = clonePolicy(p)
	return true, nil
}

func (m *Mem) DeleteBackupPolicy(_ context.Context, id string) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	if _, ok := m.cfg.policies[id]; !ok {
		return false, nil
	}
	var jobs []backup.Job
	for _, j := range m.backupJobs {
		jobs = append(jobs, j)
	}
	if used := jobsUsing(jobs, "policy", id); len(used) > 0 {
		return false, &backup.InUseError{Jobs: used}
	}
	delete(m.cfg.policies, id)
	return true, nil
}

func (m *Mem) AcquireBackupSlot(_ context.Context, r SlotRequest, now time.Time) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	for id, s := range m.cfg.slots {
		if s.renewed.Before(now.Add(-r.TTL)) {
			delete(m.cfg.slots, id)
		}
	}
	if _, ok := m.cfg.slots[r.JobID]; ok {
		m.cfg.slots[r.JobID] = memSlot{host: r.Host, renewed: now}
		return true, nil
	}
	host := 0
	for _, s := range m.cfg.slots {
		if r.Host != "" && s.host == r.Host {
			host++
		}
	}
	if (r.MaxTotal > 0 && len(m.cfg.slots) >= r.MaxTotal) || (r.MaxPerHost > 0 && r.Host != "" && host >= r.MaxPerHost) {
		return false, nil
	}
	m.cfg.slots[r.JobID] = memSlot{host: r.Host, renewed: now}
	return true, nil
}

func (m *Mem) RenewBackupSlot(_ context.Context, jobID string, now time.Time) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	if s, ok := m.cfg.slots[jobID]; ok {
		s.renewed = now
		m.cfg.slots[jobID] = s
	}
	return nil
}

func (m *Mem) ReleaseBackupSlot(_ context.Context, jobID string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.cfg.init()
	delete(m.cfg.slots, jobID)
	return nil
}

func (m *Mem) RecordBackupJobMissed(_ context.Context, id string, expectedUpdatedAt, nextRunAt time.Time) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	j, ok := m.backupJobs[id]
	if !ok || !j.UpdatedAt.Equal(expectedUpdatedAt) || j.LastRunStatus == backup.RunStatusRunning {
		return false, nil
	}
	now := time.Now().UTC()
	st := now
	j.RunStartedAt, j.RunFinishedAt = &st, &now
	j.LastRunStatus = backup.RunStatusMissed
	n := nextRunAt
	j.NextRunAt = &n
	m.backupJobs[id] = cloneBackupJob(j)
	return true, nil
}
