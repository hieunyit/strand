package mongocdc

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Database is one source database of a pipeline and the target database it is
// copied to.
type Database struct {
	Source string
	Target string
}

// checkpointSeparator joins the pipeline name and the source database in the
// checkpoint key of a database. MongoDB database names cannot contain it.
const checkpointSeparator = "/"

// DatabaseConfigs returns one Config per database of a pipeline. A pipeline with
// several databases runs one change stream, snapshot and checkpoint per database
// (RunAll): each gets the include/exclude patterns that select its database and a
// share of SnapshotParallel, so the source sees about the same number of copy
// workers as with one database.
//
// Checkpoint keys: a pipeline with one database uses the pipeline name, as it always
// has. With several, a database uses "<pipeline>/<database>", except the database
// whose checkpoint already exists under the pipeline name (a pipeline that started
// with one database keeps its progress when more are added). A pipeline cut back to
// one database keeps "<pipeline>/<database>" when only that checkpoint exists. The
// keys are read from the target; when it cannot be read, the configs come back with
// the keys of a new pipeline together with the error, so `validate` can still report
// every check.
func DatabaseConfigs(ctx context.Context, base Config, dbs []Database) ([]Config, error) {
	if len(dbs) == 0 {
		return nil, errors.New("mongodb cdc: at least one database is required")
	}
	name := base.Pipeline
	out := make([]Config, len(dbs))
	parallel := max(base.SnapshotParallel, 1)
	for i, d := range dbs {
		c := base
		c.SourceDB, c.TargetDB = d.Source, d.Target
		if len(dbs) > 1 {
			c.Observer = nil
			c.Include = patternsFor(base.Include, d.Source)
			c.Exclude = patternsFor(base.Exclude, d.Source)
			share := parallel / len(dbs)
			if i < parallel%len(dbs) {
				share++
			}
			c.SnapshotParallel = max(1, share)
			c.Pipeline = name + checkpointSeparator + d.Source
		}
		out[i] = c
	}
	if base.TargetURI == "" {
		return out, nil
	}
	target, err := connect(ctx, base.TargetURI, base.TargetDialer, nil)
	if err != nil {
		return out, fmt.Errorf("connect MongoDB target: %w", redactError(err, base.SourceURI, base.TargetURI))
	}
	defer disconnect(target)
	states := target.Database(stateDatabase).Collection(stateCollection)
	find := func(key string) (*syncState, error) {
		var st syncState
		err := states.FindOne(ctx, bson.D{{Key: "_id", Value: key}}).Decode(&st)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read MongoDB checkpoint %q: %w", key, redactError(err, base.SourceURI, base.TargetURI))
		}
		return &st, nil
	}
	legacy, err := find(name)
	if err != nil {
		return out, err
	}
	if len(dbs) == 1 {
		if legacy == nil {
			split, err := find(name + checkpointSeparator + dbs[0].Source)
			if err != nil {
				return out, err
			}
			if split != nil {
				out[0].Pipeline = split.ID
			}
		}
		return out, nil
	}
	for i := range out {
		if legacy != nil && legacy.SourceDB == out[i].SourceDB {
			out[i].Pipeline = name
		}
	}
	return out, nil
}

// patternsFor keeps the patterns whose database part matches db.
func patternsFor(patterns []string, db string) []string {
	var out []string
	for _, p := range patterns {
		dbPattern, _, ok := strings.Cut(p, ".")
		if !ok {
			continue
		}
		if m, _ := path.Match(dbPattern, db); m {
			out = append(out, p)
		}
	}
	return out
}

// RunAll runs the per-database configs of one pipeline (DatabaseConfigs) together.
// The pipeline stops as a whole: when one database stops with an error, the others
// are stopped (each at a committed checkpoint) and that error is returned, prefixed
// with the database.
func RunAll(ctx context.Context, cfgs []Config) error {
	if len(cfgs) == 1 {
		return runDatabase(ctx, cfgs[0])
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		db  string
		err error
	}
	done := make(chan result, len(cfgs))
	for _, c := range cfgs {
		go func() {
			err := runDatabase(runCtx, c)
			if err == nil {
				err = errors.New("stopped")
			}
			done <- result{c.SourceDB, err}
		}()
	}
	var first error
	for range cfgs {
		r := <-done
		if first == nil && !(errors.Is(r.err, context.Canceled) && runCtx.Err() != nil) {
			first = fmt.Errorf("MongoDB database %s: %w", r.db, r.err)
		}
		cancel()
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return first
}

// runDatabase runs one database of a pipeline (a variable for tests).
var runDatabase = Run
