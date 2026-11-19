package store

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"
)

// Lease is the state of the single active-instance lease (web_lease).
type Lease struct {
	Holder   string
	Info     string // JSON: host, listen, version, started
	Epoch    int64
	Acquired time.Time
	Renewed  time.Time
	Expires  time.Time
	// Held: the caller of TryLease holds the lease now. Remaining: time left until expiry, measured with the
	// database clock (the caller adds it to its own monotonic clock: no clock-skew issue).
	Held      bool
	Remaining time.Duration
}

// Instance is one web_instances heartbeat row.
type Instance struct {
	ID       string
	Info     string
	Role     string
	Started  time.Time
	LastSeen time.Time
}

// LeaseStore is the lease part of Store.
type LeaseStore interface {
	// TryLease acquires the lease for holder (free, expired, or already held by holder: renewed) and returns
	// the lease as it is after the call (Held=false: another holder has it).
	TryLease(ctx context.Context, holder, info string, ttl time.Duration) (Lease, error)
	// ReleaseLease expires the lease now if holder has it (clean shutdown: a standby takes over at once).
	ReleaseLease(ctx context.Context, holder string) error
	// HeartbeatInstance upserts the heartbeat row of an instance and removes rows not seen for a day.
	HeartbeatInstance(ctx context.Context, id, info, role string) error
	// RemoveInstance deletes the heartbeat row (clean shutdown).
	RemoveInstance(ctx context.Context, id string) error
	ListInstances(ctx context.Context) ([]Instance, error)
}

const leaseCols = `holder, info::text, epoch, acquired_at, renewed_at, expires_at, GREATEST(0, EXTRACT(EPOCH FROM (expires_at - now())) * 1000)::bigint`

func scanLease(sc interface{ Scan(...any) error }, holder string) (Lease, error) {
	var l Lease
	var ms int64
	err := sc.Scan(&l.Holder, &l.Info, &l.Epoch, &l.Acquired, &l.Renewed, &l.Expires, &ms)
	l.Remaining = time.Duration(ms) * time.Millisecond
	l.Held = err == nil && l.Holder == holder && ms > 0
	return l, err
}

func (p *PG) TryLease(ctx context.Context, holder, info string, ttl time.Duration) (Lease, error) {
	if info == "" {
		info = "{}"
	}
	row := p.db.QueryRowContext(ctx, `INSERT INTO web_lease AS l (id, holder, info, epoch, acquired_at, renewed_at, expires_at)
		VALUES (1, $1, $2::jsonb, 1, now(), now(), now() + $3 * interval '1 millisecond')
		ON CONFLICT (id) DO UPDATE SET
		  holder = EXCLUDED.holder, info = EXCLUDED.info,
		  epoch = CASE WHEN l.holder = EXCLUDED.holder THEN l.epoch ELSE l.epoch + 1 END,
		  acquired_at = CASE WHEN l.holder = EXCLUDED.holder THEN l.acquired_at ELSE now() END,
		  renewed_at = now(), expires_at = EXCLUDED.expires_at
		WHERE l.holder = EXCLUDED.holder OR l.expires_at <= now()
		RETURNING `+leaseCols, holder, info, ttl.Milliseconds())
	l, err := scanLease(row, holder)
	if errors.Is(err, sql.ErrNoRows) {
		l, err = scanLease(p.db.QueryRowContext(ctx, `SELECT `+leaseCols+` FROM web_lease WHERE id = 1`), holder)
	}
	return l, p.wrap(err)
}

func (p *PG) ReleaseLease(ctx context.Context, holder string) error {
	_, err := p.db.ExecContext(ctx, `UPDATE web_lease SET expires_at = now() WHERE id = 1 AND holder = $1`, holder)
	return p.wrap(err)
}

func (p *PG) HeartbeatInstance(ctx context.Context, id, info, role string) error {
	if info == "" {
		info = "{}"
	}
	if _, err := p.db.ExecContext(ctx, `INSERT INTO web_instances (id, info, role) VALUES ($1, $2::jsonb, $3)
		ON CONFLICT (id) DO UPDATE SET info = EXCLUDED.info, role = EXCLUDED.role, last_seen = now()`, id, info, role); err != nil {
		return p.wrap(err)
	}
	_, err := p.db.ExecContext(ctx, `DELETE FROM web_instances WHERE last_seen < now() - interval '1 day'`)
	return p.wrap(err)
}

func (p *PG) RemoveInstance(ctx context.Context, id string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM web_instances WHERE id = $1`, id)
	return p.wrap(err)
}

func (p *PG) ListInstances(ctx context.Context) ([]Instance, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, info::text, role, started_at, last_seen FROM web_instances ORDER BY started_at, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		var i Instance
		if err := rows.Scan(&i.ID, &i.Info, &i.Role, &i.Started, &i.LastSeen); err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, i)
	}
	return out, p.wrap(rows.Err())
}

// ---- Mem ----

func (m *Mem) TryLease(_ context.Context, holder, info string, ttl time.Duration) (Lease, error) {
	if err := m.enter(); err != nil {
		return Lease{}, err
	}
	defer m.mu.Unlock()
	now := time.Now()
	l := m.lease
	switch {
	case l == nil:
		l = &Lease{Holder: holder, Epoch: 1, Acquired: now}
		m.lease = l
	case l.Holder == holder:
	case !l.Expires.After(now):
		l.Holder, l.Epoch, l.Acquired = holder, l.Epoch+1, now
	default:
		out := *l
		out.Held, out.Remaining = false, l.Expires.Sub(now)
		return out, nil
	}
	l.Info, l.Renewed, l.Expires = info, now, now.Add(ttl)
	out := *l
	out.Held, out.Remaining = true, ttl
	return out, nil
}

func (m *Mem) ReleaseLease(_ context.Context, holder string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.lease != nil && m.lease.Holder == holder {
		m.lease.Expires = time.Now()
	}
	return nil
}

func (m *Mem) HeartbeatInstance(_ context.Context, id, info, role string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.instances == nil {
		m.instances = map[string]Instance{}
	}
	now := time.Now()
	i, ok := m.instances[id]
	if !ok {
		i = Instance{ID: id, Started: now}
	}
	i.Info, i.Role, i.LastSeen = info, role, now
	m.instances[id] = i
	for k, v := range m.instances {
		if now.Sub(v.LastSeen) > 24*time.Hour {
			delete(m.instances, k)
		}
	}
	return nil
}

func (m *Mem) RemoveInstance(_ context.Context, id string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	delete(m.instances, id)
	return nil
}

func (m *Mem) ListInstances(context.Context) ([]Instance, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	var out []Instance
	for _, i := range m.instances {
		out = append(out, i)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}
