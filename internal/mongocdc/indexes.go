package mongocdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection options passed to "create" on the target when indexes/options are
// replicated. Others (uuid, storage engine, encryption, views, time series) are not.
var creatableOptionKeys = map[string]bool{
	"validator": true, "validationLevel": true, "validationAction": true, "collation": true,
	"capped": true, "size": true, "max": true, "clusteredIndex": true, "expireAfterSeconds": true,
}

// Options kept in sync later with collMod.
var modifiableOptionKeys = []string{"validator", "validationLevel", "validationAction"}

func creatableOptions(opts bson.Raw) bson.D {
	var out bson.D
	elems, err := opts.Elements()
	if err != nil {
		return nil
	}
	for _, e := range elems {
		k := e.Key()
		if !creatableOptionKeys[k] {
			continue
		}
		v := e.Value()
		if k == "clusteredIndex" && v.Type == bson.TypeEmbeddedDocument {
			// listCollections reports {key, unique, name, v}; create accepts no "v".
			var ci bson.D
			if err := bson.Unmarshal(v.Value, &ci); err == nil {
				filtered := ci[:0]
				for _, f := range ci {
					if f.Key != "v" {
						filtered = append(filtered, f)
					}
				}
				out = append(out, bson.E{Key: k, Value: filtered})
				continue
			}
		}
		out = append(out, bson.E{Key: k, Value: v})
	}
	return out
}

// indexSpec is a listIndexes entry without the fields that do not define the index.
type indexSpec struct {
	name   string
	unique bool
	spec   bson.D
	canon  []byte
}

var ignoredIndexFields = map[string]bool{"v": true, "ns": true, "background": true}

func listIndexSpecs(ctx context.Context, coll *mongo.Collection) (map[string]indexSpec, error) {
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := map[string]indexSpec{}
	for cur.Next(ctx) {
		var d bson.D
		if err := bson.Unmarshal(cur.Current, &d); err != nil {
			return nil, err
		}
		s := indexSpec{}
		var fields []bson.E
		for _, e := range d {
			if ignoredIndexFields[e.Key] {
				continue
			}
			switch e.Key {
			case "name":
				s.name, _ = e.Value.(string)
			case "unique":
				s.unique, _ = e.Value.(bool)
			}
			fields = append(fields, e)
		}
		s.spec = fields
		sort.Slice(fields, func(i, j int) bool { return fields[i].Key < fields[j].Key })
		s.canon, _ = bson.Marshal(bson.D(fields))
		// spec keeps the original order for createIndexes.
		s.spec = nil
		for _, e := range d {
			if !ignoredIndexFields[e.Key] {
				s.spec = append(s.spec, e)
			}
		}
		out[s.name] = s
	}
	return out, cur.Err()
}

type managedIndexes struct {
	ID    progressID `bson:"_id"`
	Names []string   `bson:"names"`
}

func (r *runner) triggerReconcile() {
	if r.reconcileTrigger == nil {
		return
	}
	select {
	case r.reconcileTrigger <- struct{}{}:
	default:
	}
}

// reconcileLoop keeps target indexes and collection options equal to the source
// (ReplicateIndexes): at stream start, every IndexReconcileEvery, and after DDL events.
// Unique indexes are built only once the target is past the snapshot overlap.
func (r *runner) reconcileLoop(ctx context.Context, cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(r.cfg.IndexReconcileEvery)
	defer ticker.Stop()
	pendingUnique := true
	for {
		allowUnique := r.consistent.Load()
		err := r.reconcileAll(ctx, allowUnique)
		if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrDDLUncertain) {
			if ctx.Err() == nil || errors.Is(err, ErrDDLUncertain) {
				cancel(err)
			}
			return
		}
		if err != nil && ctx.Err() == nil {
			r.cfg.Logger.Warn("MongoDB index/option reconcile failed; retrying later", "pipeline", r.cfg.Pipeline, "error", err)
		}
		if allowUnique {
			pendingUnique = false
		}
		wait := r.cfg.IndexReconcileEvery
		if pendingUnique || err != nil {
			wait = min(wait, 2*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.reconcileTrigger:
		case <-time.After(wait):
		}
	}
}

func (r *runner) reconcileAll(ctx context.Context, allowUnique bool) error {
	infos, err := listSelectedCollections(ctx, r.source, r.cfg)
	if err != nil {
		return err
	}
	var errs []error
	for _, info := range infos {
		if err := r.reconcileCollection(ctx, info, allowUnique); err != nil {
			if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrDDLUncertain) {
				return err
			}
			errs = append(errs, fmt.Errorf("%s: %w", info.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (r *runner) reconcileCollection(ctx context.Context, info collectionInfo, allowUnique bool) error {
	names, err := r.target.ListCollectionNames(ctx, bson.D{{Key: "name", Value: info.Name}})
	if err != nil || len(names) == 0 {
		return err // the collection appears on the target with its first event or copy
	}
	if err := r.reconcileOptions(ctx, info); err != nil {
		return err
	}
	src, err := listIndexSpecs(ctx, r.source.Collection(info.Name))
	if err != nil {
		if serverErrorCode(err, codeNamespaceNotFound) {
			return nil
		}
		return err
	}
	dst, err := listIndexSpecs(ctx, r.target.Collection(info.Name))
	if err != nil {
		if serverErrorCode(err, codeNamespaceNotFound) {
			return nil
		}
		return err
	}
	mid := progressID{Pipeline: r.cfg.Pipeline, Collection: info.Name}
	var managed managedIndexes
	if err := r.indexes.FindOne(ctx, bson.D{{Key: "_id", Value: mid}}).Decode(&managed); err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}
	owned := map[string]bool{}
	for _, n := range managed.Names {
		owned[n] = true
	}
	changed := false
	srcNames := make([]string, 0, len(src))
	for n := range src {
		srcNames = append(srcNames, n)
	}
	sort.Strings(srcNames)
	for _, name := range srcNames {
		s := src[name]
		if name == "_id_" {
			continue
		}
		if d, ok := dst[name]; ok {
			if bytes.Equal(d.canon, s.canon) {
				if !owned[name] {
					owned[name], changed = true, true
				}
				continue
			}
			if err := r.dropReconciledIndex(ctx, info.Name, name); err != nil {
				return fmt.Errorf("drop changed index %s: %w", name, err)
			}
		}
		if s.unique && !allowUnique {
			continue // built once the target is past the snapshot overlap
		}
		err := r.reconcileDDL(ctx, "create index "+info.Name+"."+name, func() error {
			return r.target.RunCommand(ctx, bson.D{{Key: "createIndexes", Value: info.Name}, {Key: "indexes", Value: bson.A{s.spec}}, {Key: "writeConcern", Value: durableDDLWriteConcern()}}).Err()
		})
		if err != nil {
			return fmt.Errorf("create index %s: %w", name, err)
		}
		r.cfg.Logger.Info("MongoDB index created on target", "pipeline", r.cfg.Pipeline, "collection", info.Name, "index", name, "unique", s.unique)
		owned[name], changed = true, true
	}
	for name := range owned {
		if _, ok := src[name]; ok {
			continue
		}
		if _, ok := dst[name]; ok {
			if err := r.dropReconciledIndex(ctx, info.Name, name); err != nil {
				return fmt.Errorf("drop index %s: %w", name, err)
			}
			r.cfg.Logger.Info("MongoDB index dropped on target (dropped on source)", "pipeline", r.cfg.Pipeline, "collection", info.Name, "index", name)
		}
		delete(owned, name)
		changed = true
	}
	if !changed {
		return nil
	}
	list := make([]string, 0, len(owned))
	for n := range owned {
		list = append(list, n)
	}
	sort.Strings(list)
	_, err = r.indexes.ReplaceOne(ctx, bson.D{{Key: "_id", Value: mid}}, managedIndexes{ID: mid, Names: list}, options.Replace().SetUpsert(true))
	return err
}

// A definite command rejection is safe to retry after re-reading the catalog.
func (r *runner) reconcileDDL(ctx context.Context, description string, command func() error) error {
	var rejected error
	err := r.lease.withDDL(ctx, description, func() error {
		err := command()
		var commandErr mongo.CommandError
		if errors.As(err, &commandErr) && !retryable(err) {
			rejected = err
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return rejected
}

func (r *runner) dropReconciledIndex(ctx context.Context, collection, name string) error {
	return r.reconcileDDL(ctx, "drop index "+collection+"."+name, func() error {
		err := r.target.RunCommand(ctx, bson.D{{Key: "dropIndexes", Value: collection}, {Key: "index", Value: name}, {Key: "writeConcern", Value: durableDDLWriteConcern()}}).Err()
		if serverErrorCode(err, 27) {
			return nil
		}
		return err
	})
}

// reconcileOptions applies validator / validation level / action changes with collMod.
func (r *runner) reconcileOptions(ctx context.Context, info collectionInfo) error {
	srcInfo, err := r.sourceCollectionInfo(ctx, info.Name)
	if err != nil {
		return err
	}
	dstCur, err := r.target.ListCollections(ctx, bson.D{{Key: "name", Value: info.Name}})
	if err != nil {
		return err
	}
	var dstInfo collectionInfo
	if dstCur.Next(ctx) {
		_ = dstCur.Decode(&dstInfo)
	}
	_ = dstCur.Close(ctx)
	cmd := bson.D{{Key: "collMod", Value: info.Name}}
	for _, k := range modifiableOptionKeys {
		sv, se := srcInfo.Options.LookupErr(k)
		dv, de := dstInfo.Options.LookupErr(k)
		switch {
		case se != nil && de != nil:
		case se == nil && de == nil && sv.Type == dv.Type && bytes.Equal(sv.Value, dv.Value):
		case se != nil && k == "validator":
			cmd = append(cmd, bson.E{Key: k, Value: bson.D{}})
		case se != nil:
			// level/action back to defaults
			def := map[string]string{"validationLevel": "strict", "validationAction": "error"}[k]
			if de == nil && dv.StringValue() != def {
				cmd = append(cmd, bson.E{Key: k, Value: def})
			}
		default:
			cmd = append(cmd, bson.E{Key: k, Value: sv})
		}
	}
	if len(cmd) == 1 {
		return nil
	}
	cmd = append(cmd, bson.E{Key: "writeConcern", Value: durableDDLWriteConcern()})
	if err := r.reconcileDDL(ctx, "modify collection "+info.Name, func() error {
		return r.target.RunCommand(ctx, cmd).Err()
	}); err != nil {
		return fmt.Errorf("collMod: %w", err)
	}
	r.cfg.Logger.Info("MongoDB collection options updated on target", "pipeline", r.cfg.Pipeline, "collection", info.Name)
	return nil
}

// monitorOplog estimates the source oplog window (first to last entry) every minute
// for the retention-headroom metric. It needs read access to local.oplog.rs; without
// it the window stays unknown (0).
func (r *runner) monitorOplog(ctx context.Context) {
	for {
		if w, err := oplogWindow(ctx, r.srcClient); err == nil {
			r.obs.setOplogWindow(w)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
		}
	}
}

// oplogWindow returns the time span the source oplog covers (projected to its maximum
// size while it is still filling up).
func oplogWindow(ctx context.Context, client *mongo.Client) (time.Duration, error) {
	oplog := client.Database("local").Collection("oplog.rs")
	var first, last struct {
		TS bson.Timestamp `bson:"ts"`
	}
	opts := options.FindOne().SetProjection(bson.D{{Key: "ts", Value: 1}})
	if err := oplog.FindOne(ctx, bson.D{}, opts.SetSort(bson.D{{Key: "$natural", Value: 1}})).Decode(&first); err != nil {
		return 0, err
	}
	if err := oplog.FindOne(ctx, bson.D{}, options.FindOne().SetProjection(bson.D{{Key: "ts", Value: 1}}).SetSort(bson.D{{Key: "$natural", Value: -1}})).Decode(&last); err != nil {
		return 0, err
	}
	age := time.Duration(int64(last.TS.T)-int64(first.TS.T)) * time.Second
	// An oplog that has not filled up yet only shows its age: project the window it will
	// cover when full at the current write rate.
	var st struct {
		Size    float64 `bson:"size"`
		MaxSize float64 `bson:"maxSize"`
	}
	if err := client.Database("local").RunCommand(ctx, bson.D{{Key: "collStats", Value: "oplog.rs"}}).Decode(&st); err == nil &&
		st.Size > 0 && st.MaxSize > 0 && st.Size < 0.9*st.MaxSize {
		age = max(age, time.Second)
		return time.Duration(float64(age) * st.MaxSize / st.Size), nil
	}
	return age, nil
}
