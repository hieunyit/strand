package store

import "context"

// SettingsStore keeps the web settings edited in the UI (key -> JSON value).
type SettingsStore interface {
	Settings(ctx context.Context) (map[string]string, error)
	PutSetting(ctx context.Context, key, valueJSON string) error
	DeleteSetting(ctx context.Context, key string) error
}

func (p *PG) Settings(ctx context.Context) (map[string]string, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT key, value::text FROM web_settings`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, p.wrap(err)
		}
		out[k] = v
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) PutSetting(ctx context.Context, key, v string) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO web_settings (key, value) VALUES ($1, $2::jsonb)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, key, v)
	return p.wrap(err)
}

func (p *PG) DeleteSetting(ctx context.Context, key string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM web_settings WHERE key = $1`, key)
	return p.wrap(err)
}

func (m *Mem) Settings(context.Context) (map[string]string, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	out := map[string]string{}
	for k, v := range m.settings {
		out[k] = v
	}
	return out, nil
}

func (m *Mem) PutSetting(_ context.Context, key, v string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.settings == nil {
		m.settings = map[string]string{}
	}
	m.settings[key] = v
	return nil
}

func (m *Mem) DeleteSetting(_ context.Context, key string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()
	delete(m.settings, key)
	return nil
}
