package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// AuditArchiveAction is the audit_log action recorded by every archive; such rows are never archived.
const AuditArchiveAction = "audit.archive"

// AuditArchivePlan describes the audit rows older than a cutoff (audit.archive entries excluded).
type AuditArchivePlan struct {
	Before time.Time
	Count  int64
	MinID  int64
	MaxID  int64
	MinTS  time.Time
	MaxTS  time.Time
}

// AuditArchive is a request to remove the rows of a plan after they were exported to File (sha256 SHA256).
type AuditArchive struct {
	Before time.Time
	MaxID  int64
	Count  int64
	File   string
	SHA256 string
	Detail map[string]any
}

// AuditArchiveRec is one audit_archives manifest row.
type AuditArchiveRec struct {
	ID      int64
	Created time.Time
	Before  time.Time
	Count   int64
	MinID   int64
	MaxID   int64
	MinTS   time.Time
	MaxTS   time.Time
	File    string
	SHA256  string
}

// AuditArchiveStore is the retention part of Store.
type AuditArchiveStore interface {
	PlanAuditArchive(ctx context.Context, before time.Time) (AuditArchivePlan, error)
	// ScanAudit calls fn for every row with ts < before and id <= maxID (audit.archive excluded), by id.
	ScanAudit(ctx context.Context, before time.Time, maxID int64, fn func(AuditRow) error) error
	// ArchiveAudit deletes exactly the planned rows (all or nothing: the count must still match) and records the
	// manifest row and an audit.archive entry, in one transaction. Returns the number of rows removed.
	ArchiveAudit(ctx context.Context, a AuditArchive) (int64, error)
	ListAuditArchives(ctx context.Context, n int) ([]AuditArchiveRec, error)
}

// MinAuditArchiveAge: rows younger than this are never archived (also enforced by strand_audit_archive).
const MinAuditArchiveAge = 24 * time.Hour

func (p *PG) PlanAuditArchive(ctx context.Context, before time.Time) (AuditArchivePlan, error) {
	pl := AuditArchivePlan{Before: before}
	var minID, maxID sql.NullInt64
	var minTS, maxTS sql.NullTime
	err := p.db.QueryRowContext(ctx, `SELECT count(*), min(id), max(id), min(ts), max(ts) FROM audit_log WHERE ts < $1 AND action <> $2`,
		before, AuditArchiveAction).Scan(&pl.Count, &minID, &maxID, &minTS, &maxTS)
	pl.MinID, pl.MaxID, pl.MinTS, pl.MaxTS = minID.Int64, maxID.Int64, minTS.Time, maxTS.Time
	return pl, p.wrap(err)
}

func (p *PG) ScanAudit(ctx context.Context, before time.Time, maxID int64, fn func(AuditRow) error) error {
	// Keyset pages (no long-lived cursor holding a connection while the caller writes a file).
	last := int64(0)
	for {
		rows, err := p.db.QueryContext(ctx, `SELECT id, ts, ip, action, pipeline, result, data::text FROM audit_log
			WHERE ts < $1 AND id <= $2 AND id > $3 AND action <> $4 ORDER BY id LIMIT 5000`, before, maxID, last, AuditArchiveAction)
		if err != nil {
			return p.wrap(err)
		}
		n := 0
		for rows.Next() {
			var r AuditRow
			var d string
			if err := rows.Scan(&r.ID, &r.Time, &r.IP, &r.Action, &r.Pipeline, &r.Result, &d); err != nil {
				rows.Close()
				return p.wrap(err)
			}
			r.Data = []byte(d)
			if err := fn(r); err != nil {
				rows.Close()
				return err
			}
			last, n = r.ID, n+1
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return p.wrap(err)
		}
		if n == 0 {
			return nil
		}
	}
}

func (p *PG) ArchiveAudit(ctx context.Context, a AuditArchive) (int64, error) {
	det, err := json.Marshal(a.Detail)
	if err != nil {
		return 0, err
	}
	if a.Detail == nil {
		det = []byte(`{}`)
	}
	var n int64
	err = p.db.QueryRowContext(ctx, `SELECT strand_audit_archive($1, $2, $3, $4, $5, $6::jsonb)`,
		a.Before, a.MaxID, a.Count, a.File, a.SHA256, string(det)).Scan(&n)
	return n, p.wrap(err)
}

func (p *PG) ListAuditArchives(ctx context.Context, n int) ([]AuditArchiveRec, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, created_at, before_ts, row_count, coalesce(min_id, 0), max_id,
		coalesce(min_ts, before_ts), coalesce(max_ts, before_ts), file_name, file_sha256 FROM audit_archives ORDER BY id DESC LIMIT $1`, n)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []AuditArchiveRec
	for rows.Next() {
		var r AuditArchiveRec
		if err := rows.Scan(&r.ID, &r.Created, &r.Before, &r.Count, &r.MinID, &r.MaxID, &r.MinTS, &r.MaxTS, &r.File, &r.SHA256); err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, r)
	}
	return out, p.wrap(rows.Err())
}

// ---- Mem ----

func (m *Mem) archivable(r AuditRow, before time.Time, maxID int64) bool {
	return r.Time.Before(before) && r.ID <= maxID && r.Action != AuditArchiveAction
}

func (m *Mem) PlanAuditArchive(_ context.Context, before time.Time) (AuditArchivePlan, error) {
	if err := m.enter(); err != nil {
		return AuditArchivePlan{}, err
	}
	defer m.mu.Unlock()
	pl := AuditArchivePlan{Before: before}
	for _, r := range m.audit {
		if !m.archivable(r, before, 1<<62) {
			continue
		}
		if pl.Count == 0 || r.ID < pl.MinID {
			pl.MinID = r.ID
		}
		if r.ID > pl.MaxID {
			pl.MaxID = r.ID
		}
		if pl.Count == 0 || r.Time.Before(pl.MinTS) {
			pl.MinTS = r.Time
		}
		if r.Time.After(pl.MaxTS) {
			pl.MaxTS = r.Time
		}
		pl.Count++
	}
	return pl, nil
}

func (m *Mem) ScanAudit(_ context.Context, before time.Time, maxID int64, fn func(AuditRow) error) error {
	if err := m.enter(); err != nil {
		return err
	}
	var rows []AuditRow
	for _, r := range m.audit {
		if m.archivable(r, before, maxID) {
			rows = append(rows, r)
		}
	}
	m.mu.Unlock()
	for _, r := range rows {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mem) ArchiveAudit(_ context.Context, a AuditArchive) (int64, error) {
	if err := m.enter(); err != nil {
		return 0, err
	}
	defer m.mu.Unlock()
	if a.Before.After(time.Now().Add(-MinAuditArchiveAge)) {
		return 0, fmt.Errorf("audit archive: the cutoff %s is less than one day old", a.Before)
	}
	var keep []AuditRow
	var n int64
	rec := AuditArchiveRec{ID: int64(len(m.archives) + 1), Created: time.Now(), Before: a.Before, MaxID: a.MaxID, File: a.File, SHA256: a.SHA256}
	for _, r := range m.audit {
		if m.archivable(r, a.Before, a.MaxID) {
			if n == 0 || r.ID < rec.MinID {
				rec.MinID = r.ID
			}
			if n == 0 || r.Time.Before(rec.MinTS) {
				rec.MinTS = r.Time
			}
			if r.Time.After(rec.MaxTS) {
				rec.MaxTS = r.Time
			}
			n++
			continue
		}
		keep = append(keep, r)
	}
	if n != a.Count {
		return 0, fmt.Errorf("audit archive: %d rows match in the database but the export file holds %d; nothing deleted", n, a.Count)
	}
	if n == 0 {
		return 0, nil
	}
	rec.Count = n
	m.archives = append(m.archives, rec)
	det := map[string]any{}
	for k, v := range a.Detail {
		det[k] = v
	}
	det["rows"], det["min_id"], det["max_id"], det["from"], det["to"] = n, rec.MinID, a.MaxID, rec.MinTS, rec.MaxTS
	det["before"], det["file"], det["sha256"] = a.Before, a.File, a.SHA256
	now := time.Now()
	data, _ := json.Marshal(map[string]any{"time": now, "ip": "", "action": AuditArchiveAction, "result": "ok", "detail": det})
	m.audit = append(keep, AuditRow{ID: m.nextAuditID(), Time: now, Action: AuditArchiveAction, Result: "ok", Data: data})
	return n, nil
}

func (m *Mem) ListAuditArchives(_ context.Context, n int) ([]AuditArchiveRec, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	var out []AuditArchiveRec
	for i := len(m.archives) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, m.archives[i])
	}
	return out, nil
}
