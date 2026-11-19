package store

import (
	"context"
	"sort"
)

// Wizard is one stored wizard state.
type Wizard struct {
	Kind, Pipeline string
	Data           []byte
}

// ExportStore lists everything for `strand web db export` and the migration report.
type ExportStore interface {
	ListWizards(ctx context.Context) ([]Wizard, error)
	// AuditAfter returns up to n rows with id > afterID, oldest first (paging through the whole log).
	AuditAfter(ctx context.Context, afterID int64, n int) ([]AuditRow, error)
	CountAudit(ctx context.Context) (int64, error)
}

func (p *PG) ListWizards(ctx context.Context) ([]Wizard, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT kind, pipeline, data::text FROM wizards ORDER BY kind, pipeline`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []Wizard
	for rows.Next() {
		var w Wizard
		var d string
		if err := rows.Scan(&w.Kind, &w.Pipeline, &d); err != nil {
			return nil, p.wrap(err)
		}
		w.Data = []byte(d)
		out = append(out, w)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) AuditAfter(ctx context.Context, afterID int64, n int) ([]AuditRow, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, ts, ip, action, pipeline, result, data::text FROM audit_log WHERE id > $1 ORDER BY id LIMIT $2`, afterID, n)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []AuditRow
	for rows.Next() {
		var r AuditRow
		var d string
		if err := rows.Scan(&r.ID, &r.Time, &r.IP, &r.Action, &r.Pipeline, &r.Result, &d); err != nil {
			return nil, p.wrap(err)
		}
		r.Data = []byte(d)
		out = append(out, r)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) CountAudit(ctx context.Context) (int64, error) {
	var n int64
	err := p.db.QueryRowContext(ctx, `SELECT count(*) FROM audit_log`).Scan(&n)
	return n, p.wrap(err)
}

func (m *Mem) ListWizards(context.Context) ([]Wizard, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	var out []Wizard
	for k, v := range m.wizards {
		kind, pipe := splitWizKey(k)
		out = append(out, Wizard{Kind: kind, Pipeline: pipe, Data: append([]byte(nil), v...)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Pipeline < out[j].Pipeline
	})
	return out, nil
}

func splitWizKey(k string) (string, string) {
	for i := 0; i < len(k); i++ {
		if k[i] == 0 {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}

func (m *Mem) AuditAfter(_ context.Context, afterID int64, n int) ([]AuditRow, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	var out []AuditRow
	for _, r := range m.audit {
		if r.ID > afterID && len(out) < n {
			out = append(out, r)
		}
	}
	return out, nil
}

func (m *Mem) CountAudit(context.Context) (int64, error) {
	if err := m.enter(); err != nil {
		return 0, err
	}
	defer m.mu.Unlock()
	return int64(len(m.audit)), nil
}
