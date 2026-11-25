package mongocdc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
)

// VerifyOptions selects what `strand verify` compares for a MongoDB pipeline.
type VerifyOptions struct {
	Collections  []string      // empty: every selected collection
	Sample       int           // > 0: compare this many random source documents instead of a full scan
	Rechecks     int           // re-read differing documents this many times (pipeline lag), default 3
	RecheckDelay time.Duration // default 2s
	MaxReported  int           // differing _ids listed per collection, default 10
}

// CollectionReport is the result for one collection.
type CollectionReport struct {
	Collection   string
	SourceCount  int64
	TargetCount  int64
	Compared     int64
	SourceHash   string // full scan: sha256 over documents in _id order
	TargetHash   string
	Missing      int64 // on source, not on target (after rechecks)
	Extra        int64 // on target, not on source
	Different    int64 // different bytes (field order and BSON types included)
	DifferingIDs []string
}

// OK reports a byte-identical collection.
func (r CollectionReport) OK() bool { return r.Missing+r.Extra+r.Different == 0 }

// Verify compares source and target collections document by document (bytes) in _id
// ranges, rechecking differences to absorb pipeline lag.
func Verify(ctx context.Context, cfg Config, opts VerifyOptions) (_ []CollectionReport, err error) {
	defer func() { err = redactError(err, cfg.SourceURI, cfg.TargetURI) }()
	if opts.Rechecks <= 0 {
		opts.Rechecks = 3
	}
	if opts.RecheckDelay <= 0 {
		opts.RecheckDelay = 2 * time.Second
	}
	if opts.MaxReported <= 0 {
		opts.MaxReported = 10
	}
	src, err := connect(ctx, cfg.SourceURI, cfg.SourceDialer, nil)
	if err != nil {
		return nil, fmt.Errorf("connect MongoDB source: %w", err)
	}
	defer disconnect(src)
	dst, err := connect(ctx, cfg.TargetURI, cfg.TargetDialer, nil)
	if err != nil {
		return nil, fmt.Errorf("connect MongoDB target: %w", err)
	}
	defer disconnect(dst)
	sdb := src.Database(cfg.SourceDB, options.Database().SetReadConcern(readconcern.Majority()))
	tdb := dst.Database(cfg.TargetDB, options.Database().SetReadConcern(readconcern.Majority()))
	names := opts.Collections
	if len(names) == 0 {
		infos, err := listSelectedCollections(ctx, sdb, cfg)
		if err != nil {
			return nil, err
		}
		for _, in := range infos {
			names = append(names, in.Name)
		}
	}
	var out []CollectionReport
	for _, name := range names {
		rep, err := verifyCollection(ctx, sdb.Collection(name), tdb.Collection(name), opts)
		if err != nil {
			return out, fmt.Errorf("verify %s: %w", name, err)
		}
		rep.Collection = name
		out = append(out, rep)
	}
	return out, nil
}

type diffSet map[string]bson.RawValue // exactKey -> _id

func verifyCollection(ctx context.Context, s, t *mongo.Collection, opts VerifyOptions) (CollectionReport, error) {
	var rep CollectionReport
	var err error
	if rep.SourceCount, err = s.CountDocuments(ctx, bson.D{}); err != nil {
		return rep, err
	}
	if rep.TargetCount, err = t.CountDocuments(ctx, bson.D{}); err != nil {
		return rep, err
	}
	diffs := diffSet{}
	if opts.Sample > 0 {
		cur, err := s.Aggregate(ctx, mongo.Pipeline{bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: opts.Sample}}}}})
		if err != nil {
			return rep, err
		}
		var batch []bson.Raw
		for cur.Next(ctx) {
			batch = append(batch, append(bson.Raw(nil), cur.Current...))
		}
		if err := cur.Err(); err != nil {
			return rep, err
		}
		_ = cur.Close(ctx)
		rep.Compared = int64(len(batch))
		if err := compareByID(ctx, t, batch, diffs); err != nil {
			return rep, err
		}
	} else if err := fullCompare(ctx, s, t, &rep, diffs); err != nil {
		return rep, err
	}
	for i := 0; i < opts.Rechecks && len(diffs) > 0; i++ {
		time.Sleep(opts.RecheckDelay)
		if err := recheck(ctx, s, t, diffs); err != nil {
			return rep, err
		}
	}
	if len(diffs) > 0 {
		srcDocs, err := fetchRaw(ctx, s, diffs)
		if err != nil {
			return rep, err
		}
		dstDocs, err := fetchRaw(ctx, t, diffs)
		if err != nil {
			return rep, err
		}
		for k, id := range diffs {
			switch a, b := srcDocs[k], dstDocs[k]; {
			case a != nil && b == nil:
				rep.Missing++
			case a == nil && b != nil:
				rep.Extra++
			default:
				rep.Different++
			}
			if len(rep.DifferingIDs) < opts.MaxReported {
				rep.DifferingIDs = append(rep.DifferingIDs, id.String())
			}
		}
	}
	return rep, nil
}

// fullCompare reads the source in _id order in batches; for each batch the target is
// read over the same _id range ([first, next batch's first) with index bounds, which
// order every BSON type like the server does) and compared by exact _id bytes.
func fullCompare(ctx context.Context, s, t *mongo.Collection, rep *CollectionReport, diffs diffSet) error {
	const batchSize = 1000
	hint := bson.D{{Key: "_id", Value: 1}}
	cur, err := s.Find(ctx, bson.D{}, options.Find().SetSort(hint).SetHint(hint).SetBatchSize(batchSize))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	sh, th := sha256.New(), sha256.New()
	var batch []bson.Raw
	first := true
	flush := func(next *bson.RawValue) error {
		fo := options.Find().SetSort(hint).SetHint(hint)
		if !first && len(batch) > 0 {
			fo.SetMin(bson.D{{Key: "_id", Value: batch[0].Lookup("_id")}})
		}
		if next != nil {
			fo.SetMax(bson.D{{Key: "_id", Value: *next}})
		}
		first = false
		tc, err := t.Find(ctx, bson.D{}, fo)
		if err != nil {
			return err
		}
		tgt := map[string]bson.Raw{}
		for tc.Next(ctx) {
			d := append(bson.Raw(nil), tc.Current...)
			th.Write(d)
			tgt[exactKey(d.Lookup("_id"))] = d
		}
		if err := tc.Err(); err != nil {
			return err
		}
		_ = tc.Close(ctx)
		for _, d := range batch {
			sh.Write(d)
			id := d.Lookup("_id")
			k := exactKey(id)
			if td, ok := tgt[k]; !ok || !bytes.Equal(td, d) {
				diffs[k] = id
			}
			delete(tgt, k)
		}
		for k, d := range tgt {
			diffs[k] = d.Lookup("_id")
		}
		rep.Compared += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	for cur.Next(ctx) {
		d := append(bson.Raw(nil), cur.Current...)
		if len(batch) == batchSize {
			next := d.Lookup("_id")
			if err := flush(&next); err != nil {
				return err
			}
		}
		batch = append(batch, d)
	}
	if err := cur.Err(); err != nil {
		return err
	}
	if err := flush(nil); err != nil {
		return err
	}
	rep.SourceHash = fmt.Sprintf("%x", sh.Sum(nil))
	rep.TargetHash = fmt.Sprintf("%x", th.Sum(nil))
	return nil
}

func compareByID(ctx context.Context, t *mongo.Collection, docs []bson.Raw, diffs diffSet) error {
	ids := diffSet{}
	for _, d := range docs {
		id := d.Lookup("_id")
		ids[exactKey(id)] = id
	}
	tgt, err := fetchRaw(ctx, t, ids)
	if err != nil {
		return err
	}
	for _, d := range docs {
		id := d.Lookup("_id")
		if td := tgt[exactKey(id)]; !bytes.Equal(td, d) {
			diffs[exactKey(id)] = id
		}
	}
	return nil
}

func recheck(ctx context.Context, s, t *mongo.Collection, diffs diffSet) error {
	a, err := fetchRaw(ctx, s, diffs)
	if err != nil {
		return err
	}
	b, err := fetchRaw(ctx, t, diffs)
	if err != nil {
		return err
	}
	for k := range diffs {
		if bytes.Equal(a[k], b[k]) {
			delete(diffs, k)
		}
	}
	return nil
}

func fetchRaw(ctx context.Context, c *mongo.Collection, ids diffSet) (map[string]bson.Raw, error) {
	out := map[string]bson.Raw{}
	list := make(bson.A, 0, len(ids))
	for _, id := range ids {
		list = append(list, id)
	}
	for start := 0; start < len(list); start += 1000 {
		cur, err := c.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: list[start:min(start+1000, len(list))]}}}})
		if err != nil {
			return nil, err
		}
		for cur.Next(ctx) {
			d := append(bson.Raw(nil), cur.Current...)
			out[exactKey(d.Lookup("_id"))] = d
		}
		err = cur.Err()
		_ = cur.Close(ctx)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ListCollections returns the selected source collections (for admin source_tables).
func ListCollections(ctx context.Context, cfg Config) (_ []string, err error) {
	defer func() { err = redactError(err, cfg.SourceURI) }()
	c, err := connect(ctx, cfg.SourceURI, cfg.SourceDialer, nil)
	if err != nil {
		return nil, err
	}
	defer disconnect(c)
	names, err := c.Database(cfg.SourceDB).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	out := names[:0]
	for _, n := range names {
		if len(n) < 7 || n[:7] != "system." {
			out = append(out, n)
		}
	}
	return out, nil
}

// ErrVerifyMismatch is returned by callers when a report is not OK.
var ErrVerifyMismatch = errors.New("mongodb verify: source and target differ")
