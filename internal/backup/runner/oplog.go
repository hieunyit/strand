package runner

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/backup"
)

// MongoDB continuous oplog slicing (like PBM): a tailable cursor on local.oplog.rs from the end of the
// previous slice; every log_flush_seconds (default 60 s) or 64 MiB the buffered entries are uploaded as one
// slice (concatenated BSON, the oplog.bson format mongorestore --oplogReplay reads). Slices are
// contiguous: the previous slice's last timestamp is a checkpoint, and the next slice starts after it.

const maxOplogSlice = 64 << 20

// ArchiveOplog runs until ctx ends.
func (e *Env) ArchiveOplog(ctx context.Context, job backup.Job) error {
	if err := checkRuntimeTLS(job.Engine(), job.Conn); err != nil {
		return err
	}
	repo, err := e.OpenJobRepository(ctx, job.Repository, job.Bucket, job.Prefix)
	if err != nil {
		return err
	}
	enc, err := e.encodeOptions(job)
	if err != nil {
		return err
	}
	uri, err := MongoURI(e.Getenv, job.Conn)
	if err != nil {
		return err
	}
	client, err := connectBackupMongo(uri, job.Conn)
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())
	sourceID, sourceTerm, committed, err := mongoHistory(ctx, client)
	if err != nil {
		return err
	}
	var from bson.Timestamp
	last, err := e.latestOriginalLog(ctx, job.ID)
	if err != nil {
		return err
	}
	if last != nil && last.Method == backup.MethodOplog {
		if last.Details["source_identity"] != sourceID || last.Details["oplog_boundary"] != "exclusive-v1" {
			return fmt.Errorf("cannot resume oplog archive without matching committed replica set history")
		}
		from, err = ParseOplogTS(last.EndPosition.OplogTS)
		if err != nil {
			return err
		}
	} else {
		from = committed
	}
	every := time.Duration(job.LogFlushSeconds) * time.Second
	if every <= 0 {
		every = time.Minute
	}
	coll := client.Database("local").Collection("oplog.rs")
	e.logf("oplog archive %s: tailing from %s", job.Source, FormatOplogTS(from))
	for ctx.Err() == nil {
		cur, err := coll.Find(ctx, bson.D{{Key: "ts", Value: bson.D{{Key: "$gte", Value: from}}}},
			options.Find().SetCursorType(options.TailableAwait).SetMaxAwaitTime(2*time.Second).SetSort(bson.D{{Key: "$natural", Value: 1}}))
		if err != nil {
			return err
		}
		var buf bytes.Buffer
		var lastTS bson.Timestamp
		n := 0
		sliceStart := time.Now()
		seenFrom := false
		flush := func() error {
			if n == 0 {
				return nil
			}
			if err := awaitMongoCommit(ctx, client, sourceID, sourceTerm, lastTS); err != nil {
				return err
			}
			start, end := time.Unix(int64(from.T), 0).UTC(), time.Unix(int64(lastTS.T), 0).UTC()
			segmentID := fmt.Sprintf("oplog-%s-%010d-%010d", safeSegment(job.ID), lastTS.T, lastTS.I)
			_, err := e.storeLog(ctx, job, repo, enc, logSegment{ID: segmentID, Name: fmt.Sprintf("oplog/%010d.%010d.bson", lastTS.T, lastTS.I),
				Data: bytes.NewReader(buf.Bytes()), Method: backup.MethodOplog, Seq: uint64(lastTS.T)<<32 | uint64(lastTS.I),
				Meta: backup.Meta{Position: backup.Position{OplogTS: FormatOplogTS(from)}, EndPosition: backup.Position{OplogTS: FormatOplogTS(lastTS)},
					StartTime: &start, EndTime: &end, Details: map[string]string{"entries": fmt.Sprint(n), "source_identity": sourceID, "oplog_boundary": "exclusive-v1"}}})
			if err != nil {
				return err
			}
			e.logf("oplog archive: slice up to %s archived (%d entries)", FormatOplogTS(lastTS), n)
			from, n = lastTS, 0
			buf.Reset()
			sliceStart = time.Now()
			return nil
		}
		for ctx.Err() == nil {
			if cur.TryNext(ctx) {
				var doc struct {
					TS bson.Timestamp `bson:"ts"`
				}
				if err := cur.Decode(&doc); err != nil {
					cur.Close(context.Background())
					return err
				}
				if doc.TS == from {
					seenFrom = true
					continue
				}
				if !seenFrom {
					cur.Close(context.Background())
					return fmt.Errorf("oplog no longer contains %s: archive gap", FormatOplogTS(from))
				}
				buf.Write(cur.Current)
				n++
				lastTS = doc.TS
				if buf.Len() >= maxOplogSlice {
					if err := flush(); err != nil {
						cur.Close(context.Background())
						return err
					}
				}
				continue
			}
			if err := cur.Err(); err != nil || cur.ID() == 0 {
				break
			}
			if n > 0 && time.Since(sliceStart) >= every {
				if err := flush(); err != nil {
					cur.Close(context.Background())
					return err
				}
			}
		}
		cur.Close(context.Background())
		if !seenFrom && ctx.Err() == nil {
			return fmt.Errorf("oplog no longer contains %s: the oplog rolled over, the archive has a gap (take a new base backup)", FormatOplogTS(from))
		}
		if n > 0 && ctx.Err() == nil {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return nil
}
