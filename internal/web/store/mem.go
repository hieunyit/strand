package store

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// Mem is an in-memory Store for tests (several web Servers can share one Mem to model a restart).
type Mem struct {
	mu       sync.Mutex
	admin    *Admin
	sessions map[string]Session
	pipes    map[string]Pipeline
	desired  map[string]string
	jobs     map[string]Job
	jobOut   map[string]map[int64][]byte
	// metrics, metrics1m: pipeline -> UnixNano(ts) -> mẫu.
	metrics   map[string]map[int64]MetricSample
	metrics1m map[string]map[int64]MetricSample
	settings  map[string]string
	wizards   map[string][]byte
	audit     []AuditRow
	meta      map[string]string
	locks     map[string]*sync.Mutex
	// Down makes every call fail (simulates a database outage).
	down bool
}

func NewMem() *Mem {
	return &Mem{sessions: map[string]Session{}, pipes: map[string]Pipeline{}, desired: map[string]string{},
		jobs: map[string]Job{}, wizards: map[string][]byte{}, meta: map[string]string{}, locks: map[string]*sync.Mutex{}}
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
	p.normalize()
	m.pipes[p.Name] = p
	return nil
}

func (m *Mem) CreatePipeline(ctx context.Context, p Pipeline) (bool, error) {
	if err := m.enter(); err != nil {
		return false, err
	}
	_, exists := m.pipes[p.Name]
	m.mu.Unlock()
	if exists {
		return false, nil
	}
	return true, m.UpsertPipeline(ctx, p)
}

func (m *Mem) UpdatePipelineMeta(_ context.Context, r Pipeline) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	cur, ok := m.pipes[r.Name]
	if !ok {
		return nil
	}
	r.normalize()
	cur.ConfigPath, cur.EnvPath, cur.SystemdUnit, cur.LogFile, cur.PidFile = r.ConfigPath, r.EnvPath, r.SystemdUnit, r.LogFile, r.PidFile
	cur.Origin, cur.MissingSince, cur.Updated = r.Origin, r.MissingSince, time.Now()
	m.pipes[r.Name] = cur
	return nil
}

func (m *Mem) SetPipelineConflicts(_ context.Context, name, conflicts string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if conflicts == "" {
		conflicts = "[]"
	}
	if cur, ok := m.pipes[name]; ok {
		cur.Conflicts = conflicts
		m.pipes[name] = cur
	}
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
		if strings.HasSuffix(k, "\x00"+name) { // khóa: kind + "\x00" + pipeline
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
	delete(m.jobOut, id)
	return nil
}

func (m *Mem) AppendJobOutput(_ context.Context, id string, off int64, data []byte) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if _, ok := m.jobs[id]; !ok {
		return errors.New("store: job_output for unknown job " + id)
	}
	if m.jobOut == nil {
		m.jobOut = map[string]map[int64][]byte{}
	}
	if m.jobOut[id] == nil {
		m.jobOut[id] = map[int64][]byte{}
	}
	if _, ok := m.jobOut[id][off]; !ok {
		m.jobOut[id][off] = append([]byte(nil), data...)
	}
	return nil
}

func (m *Mem) ReadJobOutput(_ context.Context, id string, off int64, max int) ([]byte, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	return readChunks(m.jobOut[id], off, max), nil
}

func (m *Mem) JobOutputSize(_ context.Context, id string) (int64, error) {
	if err := m.enter(); err != nil {
		return 0, err
	}
	defer m.mu.Unlock()
	return contiguous(m.jobOut[id]), nil
}

// contiguous: số byte liên tục từ offset 0.
func contiguous(ch map[int64][]byte) int64 {
	var n int64
	for {
		d, ok := ch[n]
		if !ok || len(d) == 0 {
			return n
		}
		n += int64(len(d))
	}
}

func readChunks(ch map[int64][]byte, off int64, max int) []byte {
	offs := make([]int64, 0, len(ch))
	for o := range ch {
		offs = append(offs, o)
	}
	sort.Slice(offs, func(i, k int) bool { return offs[i] < offs[k] })
	var out []byte
	for _, o := range offs {
		d := ch[o]
		end := o + int64(len(d))
		if end <= off {
			continue
		}
		start := off + int64(len(out))
		if o > start {
			break // lỗ: dừng ở phần liên tục
		}
		out = append(out, d[start-o:]...)
		if len(out) >= max {
			return out[:max]
		}
	}
	return out
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
func (m *Mem) AppendAuditBatch(_ context.Context, rows []AuditRow, progress *MetaKV) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	for _, r := range rows {
		r.ID = int64(len(m.audit) + 1)
		m.audit = append(m.audit, r)
	}
	if progress != nil {
		m.meta[progress.Key] = progress.Value
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
