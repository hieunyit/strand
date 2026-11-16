package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

var migrationRE = regexp.MustCompile(`^(\d{3})_([a-z0-9_]+)\.sql$`)

// Migration is one embedded, versioned SQL file (NNN_name.sql).
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Migrations returns the embedded migrations in version order.
func Migrations() ([]Migration, error) {
	ents, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range ents {
		m := migrationRE.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration file %q does not match NNN_name.sql", e.Name())
		}
		v, _ := strconv.Atoi(m[1])
		b, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, Migration{Version: v, Name: m[2], SQL: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i, m := range out {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migrations must be numbered 001.. without gaps (got %03d at position %d)", m.Version, i+1)
		}
	}
	return out, nil
}

// migrateLockName is the advisory-lock name shared by all web instances: two instances starting at once
// serialise here, the second finds everything applied.
const migrateLockName = "migrate"

// Migrate applies pending migrations, each in its own transaction (together with its schema_migrations row),
// under a cross-instance advisory lock. Idempotent. Returns the "NNN_name" labels applied by this call.
func Migrate(ctx context.Context, db *sql.DB) ([]string, error) {
	ms, err := Migrations()
	if err != nil {
		return nil, err
	}
	var applied []string
	err = withAdvisoryLock(ctx, db, migrateLockName, func(ctx context.Context) error {
		if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
			version int PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		have := map[int]bool{}
		rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var v int
			if err := rows.Scan(&v); err != nil {
				rows.Close()
				return err
			}
			have[v] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, m := range ms {
			if have[m.Version] {
				continue
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("migration %03d_%s: %w", m.Version, m.Name, err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.Version, m.Name); err != nil {
				_ = tx.Rollback()
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			applied = append(applied, fmt.Sprintf("%03d_%s", m.Version, m.Name))
		}
		return nil
	})
	return applied, err
}
