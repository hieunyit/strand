package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"strand/internal/admin"
	"strand/internal/model"
)

// tableActivity counts the CDC row changes this process applied to the target, per source table: inserts,
// updates and deletes, when the last one was committed on the target and the source commit time of that
// change. Snapshot copies are not counted. It lives in RAM: it starts empty when the pipeline starts.
type tableActivity struct {
	mu sync.Mutex
	m  map[model.TableID]*tableAct
}

type tableAct struct {
	inserts, updates, deletes int64
	last                      time.Time // committed on the target
	lastSource                int64     // source commit time of the last change (Unix seconds; 0 unknown)
}

// record adds the changes of a committed batch (one lock per batch).
func (a *tableActivity) record(batch []*model.Txn, now time.Time) {
	n := 0
	for _, t := range batch {
		n += len(t.TableOps.Counts)
	}
	if n == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.m == nil {
		a.m = make(map[model.TableID]*tableAct)
	}
	for _, t := range batch {
		for _, c := range t.TableOps.Counts {
			x := a.m[c.Table]
			if x == nil {
				x = &tableAct{}
				a.m[c.Table] = x
			}
			x.inserts += c.Inserts
			x.updates += c.Updates
			x.deletes += c.Deletes
			x.last = now
			if t.SourceTS > x.lastSource {
				x.lastSource = t.SourceTS
			}
		}
	}
}

// fill sets the activity fields of the tables in out (keyed by source table name).
func (a *tableActivity) fill(out []admin.TableInfo, ids []model.TableID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range out {
		x := a.m[ids[i]]
		if x == nil {
			continue
		}
		ti := &out[i]
		ti.Inserts, ti.Updates, ti.Deletes = x.inserts, x.updates, x.deletes
		last := x.last.UTC()
		ti.LastChangeAt = &last
		if x.lastSource > 0 {
			src := time.Unix(x.lastSource, 0).UTC()
			ti.LastSourceAt = &src
		}
	}
}

// rowCountCache holds the source row estimates (table statistics) of every pipeline table. The Tables view
// is polled every few seconds, so it never queries the source itself: a stale cache (rowCountsTTL) starts
// one background refresh (one query per schema batch, not one per table) and the view uses what it has.
type rowCountCache struct {
	mu      sync.Mutex
	m       map[model.TableID]int64
	at      time.Time
	running bool
}

var (
	rowCountsTTL     = 5 * time.Minute
	rowCountsTimeout = 30 * time.Second
)

// tableRowCounts returns the cached estimates and refreshes them in the background when stale.
func (p *Pipeline) tableRowCounts(ids []model.TableID) map[model.TableID]int64 {
	c := &p.rowCounts
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.src != nil && !c.running && time.Since(c.at) > rowCountsTTL && len(ids) > 0 {
		c.running = true
		want := append([]model.TableID(nil), ids...)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rowCountsTimeout)
			defer cancel()
			m, err := p.queryRowCounts(ctx, want)
			c.mu.Lock()
			defer c.mu.Unlock()
			c.running = false
			c.at = time.Now()
			if err != nil {
				if p.log != nil {
					p.log.Debug("refresh table row estimates", "error", err)
				}
				return
			}
			c.m = m
		}()
	}
	return c.m
}

// queryRowCounts reads the statistics row estimate of the tables, a few schemas per query.
func (p *Pipeline) queryRowCounts(ctx context.Context, ids []model.TableID) (map[model.TableID]int64, error) {
	want := make(map[model.TableID]bool, len(ids))
	var schemas []string
	seen := map[string]bool{}
	for _, id := range ids {
		want[id] = true
		if !seen[id.Schema] {
			seen[id.Schema] = true
			schemas = append(schemas, id.Schema)
		}
	}
	out := make(map[model.TableID]int64, len(ids))
	const per = 50
	for lo := 0; lo < len(schemas); lo += per {
		part := schemas[lo:min(lo+per, len(schemas))]
		args := make([]any, len(part))
		ph := make([]string, len(part))
		for i, s := range part {
			args[i] = s
			if p.srcEngine == model.EnginePG {
				ph[i] = fmt.Sprintf("$%d", i+1)
			} else {
				ph[i] = "?"
			}
		}
		q := `SELECT TABLE_SCHEMA, TABLE_NAME, IFNULL(TABLE_ROWS, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA IN (` +
			strings.Join(ph, ",") + `)`
		if p.srcEngine == model.EnginePG {
			q = `SELECT n.nspname, c.relname, GREATEST(c.reltuples, 0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace ` +
				`WHERE c.relkind IN ('r', 'p') AND n.nspname IN (` + strings.Join(ph, ",") + `)`
		}
		rows, err := p.src.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id model.TableID
			var n int64
			if err := rows.Scan(&id.Schema, &id.Table, &n); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if want[id] {
				out[id] = n
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
