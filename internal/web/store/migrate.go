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

// SchemaStatus returns the number of applied migrations and the number embedded in this binary (0 applied when
// the schema_migrations table does not exist yet).
func SchemaStatus(ctx context.Context, db *sql.DB) (applied, latest int, err error) {
	ms, err := Migrations()
	if err != nil {
		return 0, 0, err
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('schema_migrations') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, 0, err
	}
	if exists {
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
			return 0, 0, err
		}
	}
	latest = len(ms)
	if len(ms) > 0 && ms[len(ms)-1].Version > latest {
		latest = ms[len(ms)-1].Version
	}
	return applied, latest, nil
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
		have := map[int]string{}
		rows, err := db.QueryContext(ctx, `SELECT version, name FROM schema_migrations`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var v int
			var name string
			if err := rows.Scan(&v, &name); err != nil {
				rows.Close()
				return err
			}
			have[v] = name
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, m := range ms {
			name, done := have[m.Version]
			if done && name == m.Name {
				continue
			}
			if done {
				// The version was applied by a different migration (an unmerged branch reused the number).
				// Only known, reversible collisions are healed; anything else must not be silently skipped,
				// or the schema this binary expects would never be created.
				undo, ok := supersededMigrations[supersededKey{m.Version, name}]
				if !ok {
					return fmt.Errorf("schema_migrations version %03d is %q but this binary's migration %03d is %q: "+
						"the database was migrated by a different build; restore it from a backup or migrate with the build that created it",
						m.Version, name, m.Version, m.Name)
				}
				if err := applyMigration(ctx, db, m, undo, true); err != nil {
					return err
				}
				applied = append(applied, fmt.Sprintf("%03d_%s (replaced %03d_%s)", m.Version, m.Name, m.Version, name))
				continue
			}
			if err := applyMigration(ctx, db, m, "", false); err != nil {
				return err
			}
			applied = append(applied, fmt.Sprintf("%03d_%s", m.Version, m.Name))
		}
		return nil
	})
	return applied, err
}

type supersededKey struct {
	version int
	name    string
}

// supersededMigrations: migrations that unmerged development builds applied under a version number main later
// used for something else, with the SQL that undoes them. Migrate undoes the stray migration and applies the
// real one in the same transaction.
var supersededMigrations = map[supersededKey]string{
	// feat/web-db-first (WIP) added pipelines.config_json as 010; main's 010 is backups. The column was never
	// written by any released code path (its writer had no callers), so dropping it loses nothing.
	{10, "pipeline_config_json"}: `DROP INDEX IF EXISTS pipelines_config_json_present_idx;
ALTER TABLE pipelines DROP COLUMN IF EXISTS config_json;`,
}

// applyMigration runs undo (if any) and m.SQL, then records m, all in one transaction.
func applyMigration(ctx context.Context, db *sql.DB, m Migration, undo string, replace bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if undo != "" {
		if _, err := tx.ExecContext(ctx, undo); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("undo superseded migration %03d: %w", m.Version, err)
		}
	}
	if _, err := tx.ExecContext(ctx, m.SQL); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("migration %03d_%s: %w", m.Version, m.Name, err)
	}
	q := `INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`
	if replace {
		q = `UPDATE schema_migrations SET name = $2, applied_at = now() WHERE version = $1`
	}
	if _, err := tx.ExecContext(ctx, q, m.Version, m.Name); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
