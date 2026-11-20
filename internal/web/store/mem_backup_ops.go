package store

import (
	"context"
	"sort"
	"strings"
	"time"

	"strand/internal/backup"
)

type memBackupOps struct {
	channels map[string]backup.NotifyChannel
	rules    map[string]backup.NotifyRule
	breaches map[string]RPOBreach
}

func (o *memBackupOps) init() {
	if o.channels == nil {
		o.channels, o.rules, o.breaches = map[string]backup.NotifyChannel{}, map[string]backup.NotifyRule{}, map[string]RPOBreach{}
	}
}

func cloneChannel(c backup.NotifyChannel) backup.NotifyChannel {
	c.SecretSealed = append([]byte(nil), c.SecretSealed...)
	c.SecretSet = len(c.SecretSealed) > 0
	if !c.SecretSet {
		c.SecretSealed = nil
	}
	c.To = append([]string(nil), c.To...)
	if len(c.To) == 0 {
		c.To = nil
	}
	c.LastTestAt = optTime(c.LastTestAt)
	c.Source = "stored"
	return c
}

func (m *Mem) CreateNotifyChannel(_ context.Context, c backup.NotifyChannel) (backup.NotifyChannel, error) {
	if err := c.Validate(); err != nil {
		return backup.NotifyChannel{}, err
	}
	if err := m.enter(); err != nil {
		return backup.NotifyChannel{}, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	for _, o := range m.ops.channels {
		if o.ID == c.ID || o.Name == c.Name {
			return backup.NotifyChannel{}, backup.ErrConflict
		}
	}
	c.CreatedAt = time.Now().UTC()
	c.UpdatedAt = c.CreatedAt
	c.LastTestAt, c.LastTestStatus, c.LastTestDetail = nil, "", ""
	m.ops.channels[c.ID] = cloneChannel(c)
	return cloneChannel(c), nil
}

func (m *Mem) GetNotifyChannel(_ context.Context, id string) (backup.NotifyChannel, bool, error) {
	if err := m.enter(); err != nil {
		return backup.NotifyChannel{}, false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	c, ok := m.ops.channels[id]
	return cloneChannel(c), ok, nil
}

func (m *Mem) ListNotifyChannels(_ context.Context) ([]backup.NotifyChannel, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	out := []backup.NotifyChannel{}
	for _, c := range m.ops.channels {
		out = append(out, cloneChannel(c))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name || (out[i].Name == out[j].Name && out[i].ID < out[j].ID)
	})
	return out, nil
}

func (m *Mem) UpdateNotifyChannel(_ context.Context, c backup.NotifyChannel) (bool, error) {
	if err := c.Validate(); err != nil {
		return false, err
	}
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	old, ok := m.ops.channels[c.ID]
	if !ok {
		return false, nil
	}
	for _, o := range m.ops.channels {
		if o.ID != c.ID && o.Name == c.Name {
			return false, backup.ErrConflict
		}
	}
	c.CreatedAt, c.CreatedBy, c.UpdatedAt = old.CreatedAt, old.CreatedBy, time.Now().UTC()
	c.LastTestAt, c.LastTestStatus, c.LastTestDetail = old.LastTestAt, old.LastTestStatus, old.LastTestDetail
	m.ops.channels[c.ID] = cloneChannel(c)
	return true, nil
}

func (m *Mem) DeleteNotifyChannel(_ context.Context, id string) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	if _, ok := m.ops.channels[id]; !ok {
		return false, nil
	}
	delete(m.ops.channels, id)
	for rid, r := range m.ops.rules {
		if r.ChannelID == id {
			delete(m.ops.rules, rid)
		}
	}
	return true, nil
}

func (m *Mem) SetNotifyChannelTest(_ context.Context, id string, at time.Time, status, detail string) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	c, ok := m.ops.channels[id]
	if !ok {
		return false, nil
	}
	c.LastTestAt, c.LastTestStatus, c.LastTestDetail = &at, status, detail
	m.ops.channels[id] = c
	return true, nil
}

func (m *Mem) CreateNotifyRule(_ context.Context, r backup.NotifyRule) (backup.NotifyRule, error) {
	if err := r.Validate(); err != nil {
		return backup.NotifyRule{}, err
	}
	if err := m.enter(); err != nil {
		return backup.NotifyRule{}, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	if _, ok := m.ops.channels[r.ChannelID]; !ok {
		return backup.NotifyRule{}, backup.ErrInvalid
	}
	for _, o := range m.ops.rules {
		if o.ID == r.ID || (o.Event == r.Event && o.ScopeKind == r.ScopeKind && o.ScopeID == r.ScopeID && o.ChannelID == r.ChannelID) {
			return backup.NotifyRule{}, backup.ErrConflict
		}
	}
	r.CreatedAt = time.Now().UTC()
	m.ops.rules[r.ID] = r
	return r, nil
}

func (m *Mem) GetNotifyRule(_ context.Context, id string) (backup.NotifyRule, bool, error) {
	if err := m.enter(); err != nil {
		return backup.NotifyRule{}, false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	r, ok := m.ops.rules[id]
	return r, ok, nil
}

func (m *Mem) ListNotifyRules(_ context.Context) ([]backup.NotifyRule, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	out := []backup.NotifyRule{}
	for _, r := range m.ops.rules {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		ka := strings.Join([]string{a.Event, a.ScopeKind, a.ScopeID, a.ChannelID, a.ID}, "\x00")
		kb := strings.Join([]string{b.Event, b.ScopeKind, b.ScopeID, b.ChannelID, b.ID}, "\x00")
		return ka < kb
	})
	return out, nil
}

func (m *Mem) SetNotifyRuleEnabled(_ context.Context, id string, enabled bool) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	r, ok := m.ops.rules[id]
	if !ok {
		return false, nil
	}
	r.Enabled = enabled
	m.ops.rules[id] = r
	return true, nil
}

func (m *Mem) DeleteNotifyRule(_ context.Context, id string) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	if _, ok := m.ops.rules[id]; !ok {
		return false, nil
	}
	delete(m.ops.rules, id)
	return true, nil
}

func (m *Mem) ClaimBackupRPOBreach(_ context.Context, source string, now time.Time, renotify time.Duration) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	b, ok := m.ops.breaches[source]
	if !ok {
		m.ops.breaches[source] = RPOBreach{Source: source, Since: now, NotifiedAt: now}
		return true, nil
	}
	if b.NotifiedAt.After(now.Add(-renotify)) {
		return false, nil
	}
	b.NotifiedAt = now
	m.ops.breaches[source] = b
	return true, nil
}

func (m *Mem) ClearBackupRPOBreach(_ context.Context, source string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.ops.init()
	delete(m.ops.breaches, source)
	return nil
}

func (m *Mem) ListBackupRPOBreaches(_ context.Context) ([]RPOBreach, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	m.ops.init()
	var out []RPOBreach
	for _, b := range m.ops.breaches {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Source < out[j].Source })
	return out, nil
}

func (m *Mem) BackupRepoUsage(_ context.Context) ([]RepoUsage, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	by := map[string]*RepoUsage{}
	for _, b := range m.backups {
		if b.Status == backup.StatusExpired {
			continue
		}
		k := b.Repository + "\x00" + b.Source
		u := by[k]
		if u == nil {
			u = &RepoUsage{Repository: b.Repository, Source: b.Source}
			by[k] = u
		}
		u.Count++
		u.Bytes += b.Bytes
		if b.CopyOf != "" {
			u.Copies++
		}
	}
	out := make([]RepoUsage, 0, len(by))
	for _, u := range by {
		out = append(out, *u)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Repository < out[j].Repository || (out[i].Repository == out[j].Repository && out[i].Source < out[j].Source)
	})
	return out, nil
}

func (m *Mem) BackupKeyUsage(_ context.Context) ([]KeyUsage, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	type rec struct {
		id string
		at time.Time
	}
	by := map[string]*KeyUsage{}
	recs := map[string][]rec{}
	for _, b := range m.backups {
		if b.Status == backup.StatusExpired {
			continue
		}
		u := by[b.KeyID]
		if u == nil {
			u = &KeyUsage{KeyID: b.KeyID}
			by[b.KeyID] = u
		}
		u.Count++
		u.Bytes += b.Bytes
		recs[b.KeyID] = append(recs[b.KeyID], rec{b.ID, b.CreatedAt})
	}
	out := make([]KeyUsage, 0, len(by))
	for k, u := range by {
		rs := recs[k]
		sort.Slice(rs, func(i, j int) bool {
			return rs[i].at.After(rs[j].at) || (rs[i].at.Equal(rs[j].at) && rs[i].id > rs[j].id)
		})
		for i := 0; i < len(rs) && i < 5; i++ {
			u.SampleIDs = append(u.SampleIDs, rs[i].id)
		}
		out = append(out, *u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].KeyID < out[j].KeyID })
	return out, nil
}

func (m *Mem) BackupRunCounts(_ context.Context, since, until time.Time) ([]RunCount, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	by := map[string]int{}
	for _, r := range m.backupRuns {
		if r.StartedAt.Before(since) || !r.StartedAt.Before(until) {
			continue
		}
		by[r.JobID+"\x00"+r.Status]++
	}
	var out []RunCount
	for k, n := range by {
		parts := strings.SplitN(k, "\x00", 2)
		out = append(out, RunCount{JobID: parts[0], Status: parts[1], N: n})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].JobID < out[j].JobID || (out[i].JobID == out[j].JobID && out[i].Status < out[j].Status)
	})
	return out, nil
}

func (m *Mem) ListAuditFiltered(_ context.Context, f AuditFilter) ([]AuditRow, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	var out []AuditRow
	for i := len(m.audit) - 1; i >= 0 && len(out) < f.Limit; i-- {
		r := m.audit[i]
		if f.ActionPrefix != "" && !strings.HasPrefix(r.Action, f.ActionPrefix) {
			continue
		}
		if f.BeforeID > 0 && r.ID >= f.BeforeID {
			continue
		}
		if (f.Since != nil && r.Time.Before(*f.Since)) || (f.Until != nil && !r.Time.Before(*f.Until)) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}
