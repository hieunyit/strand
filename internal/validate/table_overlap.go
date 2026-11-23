package validate

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// CheckTableOverlap rejects a destination that is also in the selected source
// set on the same database. The lock probes identify the server even if the two
// connections use different addresses, credentials, or default schemas.
func CheckTableOverlap(ctx context.Context, src, dst *sql.DB, engine model.Engine, tables []model.TableID, targetOf func(model.TableID) model.TableID) error {
	normalize := func(id model.TableID) string { return id.String() }
	// MySQL servers can fold table names depending on lower_case_table_names.
	if engine == model.EngineMySQL {
		var mode int
		if err := src.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&mode); err != nil {
			return fmt.Errorf("check source table name case rules: %w", err)
		}
		if mode != 0 {
			normalize = func(id model.TableID) string { return strings.ToLower(id.String()) }
		}
	}
	selected := make(map[string]struct{}, len(tables))
	for _, id := range tables {
		selected[normalize(id)] = struct{}{}
	}
	for _, id := range tables {
		target := targetOf(id)
		if _, overlaps := selected[normalize(target)]; !overlaps {
			continue
		}
		same, err := sharedLockNamespace(ctx, src, dst, engine)
		if err != nil {
			return fmt.Errorf("check whether source and target share the same database: %w", err)
		}
		if same {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				fmt.Sprintf("target table %s for %s is also a source table on the same database", target, id), nil)
		}
		return nil // A disjoint lock namespace applies to every table on these connections.
	}
	return nil
}

func sharedLockNamespace(ctx context.Context, src, dst *sql.DB, engine model.Engine) (bool, error) {
	if engine == model.EnginePG {
		var keyBytes [8]byte
		rand.Read(keyBytes[:])
		key := int64(binary.BigEndian.Uint64(keyBytes[:]))
		tx, err := src.BeginTx(ctx, nil)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", key); err != nil {
			return false, err
		}
		// Run in an implicit transaction so any successfully acquired lock is
		// released as soon as Scan finishes.
		var acquired bool
		err = dst.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", key).Scan(&acquired)
		return !acquired, err
	}

	name := "strand-overlap-" + rand.Text()
	conn, err := src.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	var owner int64
	var acquired int
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID(), GET_LOCK(?, 0)", name).Scan(&owner, &acquired); err != nil {
		return false, err
	}
	if acquired != 1 {
		return false, fmt.Errorf("could not acquire the MySQL identity probe lock")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanup, "DO RELEASE_LOCK(?)", name)
	}()
	var observed sql.NullInt64
	err = dst.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", name).Scan(&observed)
	return observed.Valid && observed.Int64 == owner, err
}
