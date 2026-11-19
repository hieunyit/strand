package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"strand/internal/backup"
)

// Mem is an in-memory Store for tests (several web Servers can share one Mem to model a restart).
type Mem struct {
	mu       sync.Mutex
	admin    *Admin
	sessions map[string]Session
	pipes    map[string]Pipeline
	desired  map[string]string
	jobs     map[string]Job
	wizards  map[string][]byte
	audit    []AuditRow
	meta     map[string]string
	backups  map[string]backup.Record
	locks    map[string]*sync.Mutex
	// Down makes every call fail (simulates a database outage).
	down bool
}

func NewMem() *Mem {
	return &Mem{sessions: map[string]Session{}, pipes: map[string]Pipeline{}, desired: map[string]string{},
		jobs: map[string]Job{}, wizards: map[string][]byte{}, meta: map[string]string{}, backups: map[string]backup.Record{}, locks: map[string]*sync.Mutex{}}
}

var errDown = errors.New("store: database is down")

// SetDown simulates an outage.
func (m *Mem) SetDown(v bool) { m.mu.Lock(); m.down = v; m.mu.Unlock() }

func (m *Mem) enter() error {
	m.mu.Lock()
	if m.down {
		m.mu.Unlock()
		return errDown
	}
	return nil
}

func (m *Mem) Ping(context.Context) error {
	if err := m.enter(); err != nil {
		return err
	}
	m.mu.Unlock()
	return nil
}
func (m *Mem) Close() error { return nil }

func (m *Mem) Admin(context.Context) (Admin, bool, error) {
	if err := m.enter(); err != nil {
		return Admin{}, false, err
	}
	defer m.mu.Unlock()
	if m.admin == nil {
		return Admin{}, false, nil
	}
	return *m.admin, true, nil
}

func (m *Mem) setAdmin(f func(a *Admin)) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.admin == nil {
		m.admin = &Admin{}
	}
	f(m.admin)
	return nil
}
func (m *Mem) SetPasswordHash(_ context.Context, h string) error {
	return m.setAdmin(func(a *Admin) { a.PasswordHash = h })
}
func (m *Mem) SetAPITokenHash(_ context.Context, h string) error {
	return m.setAdmin(func(a *Admin) { a.APITokenHash = h })
}

func (m *Mem) PutSession(_ context.Context, s Session) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.sessions[s.IDHash] = s
	return nil
}
func (m *Mem) GetSession(_ context.Context, id string) (Session, bool, error) {
	if err := m.enter(); err != nil {
		return Session{}, false, err
	}
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	return s, ok, nil
}
func (m *Mem) TouchSession(_ context.Context, id string, t time.Time) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if s, ok := m.sessions[id]; ok {
		s.LastSeen = t
		m.sessions[id] = s
	}
	return nil
}
func (m *Mem) DeleteSession(_ context.Context, id string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	delete(m.sessions, id)
	return nil
}
func (m *Mem) PurgeSessions(_ context.Context, idle, created time.Time) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	for k, s := range m.sessions {
		if s.LastSeen.Before(idle) || s.Created.Before(created) {
			delete(m.sessions, k)
		}
	}
	return nil
}
func (m *Mem) CountSessions(context.Context) (int, error) {
	if err := m.enter(); err != nil {
		return 0, err
	}
	defer m.mu.Unlock()
	return len(m.sessions), nil
}

func (m *Mem) UpsertPipeline(_ context.Context, p Pipeline) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	now := time.Now()
	if old, ok := m.pipes[p.Name]; ok {
		p.Created = old.Created
	} else {
		p.Created = now
	}
	p.Updated = now
	p.EnvEnc = append([]byte(nil), p.EnvEnc...)
	m.pipes[p.Name] = p
	return nil
}
func (m *Mem) GetPipeline(_ context.Context, name string) (Pipeline, bool, error) {
	if err := m.enter(); err != nil {
		return Pipeline{}, false, err
	}
	defer m.mu.Unlock()
	p, ok := m.pipes[name]
	return p, ok, nil
}
func (m *Mem) ListPipelines(context.Context) ([]Pipeline, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	var out []Pipeline
	for _, p := range m.pipes {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (m *Mem) DeletePipeline(_ context.Context, name string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	delete(m.pipes, name)
	delete(m.desired, name)
	for k := range m.wizards {
		if strings.HasSuffix(k, "/"+name) {
			delete(m.wizards, k)
		}
	}
	return nil
}

func (m *Mem) SetDesired(_ context.Context, name, d string) error {
	if d != DesiredRunning && d != DesiredStopped {
		return errors.New("invalid desired state")
	}
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.desired[name] = d
	return nil
}
func (m *Mem) Desired(context.Context) (map[string]string, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	out := map[string]string{}
	for k, v := range m.desired {
		out[k] = v
	}
	return out, nil
}

func (m *Mem) UpsertJob(_ context.Context, j Job) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	return nil
}
func (m *Mem) ListJobs(context.Context) ([]Job, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	var out []Job
	for _, j := range m.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Started.After(out[k].Started) })
	return out, nil
}
func (m *Mem) DeleteJob(_ context.Context, id string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	delete(m.jobs, id)
	return nil
}

func (m *Mem) GetWizard(_ context.Context, kind, pipeline string) ([]byte, bool, error) {
	if err := m.enter(); err != nil {
		return nil, false, err
	}
	defer m.mu.Unlock()
	d, ok := m.wizards[kind+"\x00"+pipeline]
	return d, ok, nil
}
func (m *Mem) PutWizard(_ context.Context, kind, pipeline string, data []byte) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.wizards[kind+"\x00"+pipeline] = append([]byte(nil), data...)
	return nil
}

func (m *Mem) AppendAudit(_ context.Context, r AuditRow) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	r.ID = int64(len(m.audit) + 1)
	m.audit = append(m.audit, r)
	return nil
}
func (m *Mem) AppendAuditBatch(ctx context.Context, rows []AuditRow) error {
	for _, r := range rows {
		if err := m.AppendAudit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}
func (m *Mem) ListAudit(_ context.Context, n int) ([]AuditRow, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	var out []AuditRow
	for i := len(m.audit) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, m.audit[i])
	}
	return out, nil
}

func (m *Mem) MetaGet(_ context.Context, k string) (string, bool, error) {
	if err := m.enter(); err != nil {
		return "", false, err
	}
	defer m.mu.Unlock()
	v, ok := m.meta[k]
	return v, ok, nil
}
func (m *Mem) MetaSet(_ context.Context, k, v string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	m.meta[k] = v
	return nil
}

func cloneBackup(r backup.Record) backup.Record {
	if r.VerifiedAt != nil {
		t := *r.VerifiedAt
		r.VerifiedAt = &t
	}
	if r.ExpiresAt != nil {
		t := *r.ExpiresAt
		r.ExpiresAt = &t
	}
	return r
}

func (m *Mem) CreateBackup(_ context.Context, r backup.Record) (backup.Record, bool, error) {
	r.SHA256 = strings.ToLower(r.SHA256)
	if err := backup.ValidateCreate(r); err != nil {
		return backup.Record{}, false, err
	}
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	if old, ok := m.backups[r.ID]; ok {
		if !backup.SameIdentity(old, r) {
			return backup.Record{}, false, backup.ErrConflict
		}
		return cloneBackup(old), false, nil
	}
	for _, old := range m.backups {
		if old.Bucket == r.Bucket && old.ObjectKey == r.ObjectKey {
			return backup.Record{}, false, backup.ErrConflict
		}
	}
	r.SHA256 = strings.ToLower(r.SHA256)
	m.backups[r.ID] = cloneBackup(r)
	return cloneBackup(r), true, nil
}

func (m *Mem) GetBackup(_ context.Context, id string) (backup.Record, bool, error) {
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	return cloneBackup(r), ok, nil
}

func (m *Mem) ListBackups(_ context.Context, f backup.Filter) ([]backup.Record, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	limit := f.Limit
	if limit <= 0 || limit > backup.MaxListLimit {
		limit = backup.MaxListLimit
	}
	out := make([]backup.Record, 0)
	for _, r := range m.backups {
		if (f.Source != "" && r.Source != f.Source) || (f.Engine != "" && r.Engine != f.Engine) || (f.Status != "" && r.Status != f.Status) {
			continue
		}
		if (f.CreatedAfter != nil && r.CreatedAt.Before(*f.CreatedAfter)) || (f.CreatedBefore != nil && r.CreatedAt.After(*f.CreatedBefore)) {
			continue
		}
		out = append(out, cloneBackup(r))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Mem) UpdateBackupStatus(_ context.Context, id string, expected, next backup.Status, verifiedAt *time.Time) (backup.Record, bool, error) {
	if next == backup.StatusComplete {
		return backup.Record{}, false, fmt.Errorf("%w: use finalization to complete a backup", backup.ErrInvalid)
	}
	if !backup.CanTransition(expected, next) {
		return backup.Record{}, false, backup.ErrTransition
	}
	if verifiedAt != nil {
		return backup.Record{}, false, fmt.Errorf("%w: verified_at is set by finalization", backup.ErrInvalid)
	}
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	if !ok {
		return backup.Record{}, false, nil
	}
	if r.Status != expected {
		return backup.Record{}, false, backup.ErrTransition
	}
	if r.LegalHold && next == backup.StatusExpired {
		return backup.Record{}, false, fmt.Errorf("%w: legal-hold backup cannot be expired", backup.ErrInvalid)
	}
	r.Status = next
	if next == backup.StatusUploading {
		r.VerifiedAt = nil
	}
	m.backups[id] = cloneBackup(r)
	return cloneBackup(r), true, nil
}

func (m *Mem) FinalizeBackup(_ context.Context, id string, expected backup.Status, bytes int64, sha256, manifestURI string, verifiedAt time.Time) (backup.Record, bool, error) {
	if expected != backup.StatusVerifying {
		return backup.Record{}, false, fmt.Errorf("%w: invalid expected status", backup.ErrInvalid)
	}
	if err := backup.ValidateFinalization(bytes, sha256, manifestURI, verifiedAt); err != nil {
		return backup.Record{}, false, err
	}
	if err := m.enter(); err != nil {
		return backup.Record{}, false, err
	}
	defer m.mu.Unlock()
	r, ok := m.backups[id]
	if !ok {
		return backup.Record{}, false, nil
	}
	sha256 = strings.ToLower(sha256)
	if r.Status == backup.StatusComplete && r.Bytes == bytes && r.SHA256 == sha256 && r.ManifestURI == manifestURI {
		return cloneBackup(r), false, nil
	}
	if r.Status != expected {
		return backup.Record{}, false, backup.ErrTransition
	}
	if verifiedAt.Before(r.CreatedAt) {
		return backup.Record{}, false, fmt.Errorf("%w: verified_at precedes created_at", backup.ErrInvalid)
	}
	r.Bytes = bytes
	r.SHA256 = sha256
	r.ManifestURI = manifestURI
	r.Status = backup.StatusComplete
	t := verifiedAt.UTC()
	r.VerifiedAt = &t
	m.backups[id] = cloneBackup(r)
	return cloneBackup(r), true, nil
}

func (m *Mem) Locked(ctx context.Context, key string, fn func(ctx context.Context) error) error {
	m.mu.Lock()
	l := m.locks[key]
	if l == nil {
		l = &sync.Mutex{}
		m.locks[key] = l
	}
	m.mu.Unlock()
	l.Lock()
	defer l.Unlock()
	return fn(ctx)
}
