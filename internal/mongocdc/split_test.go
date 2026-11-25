package mongocdc

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestSplitParts(t *testing.T) {
	collated, _ := bson.Marshal(bson.D{{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}}}})
	for _, c := range []struct {
		parallel, minDocs int
		estimate          int64
		options           bson.Raw
		want              int
	}{
		{1, 100000, 10_000_000, nil, 1},      // one worker
		{8, 100000, 150_000, nil, 1},         // below 2 x split_min_docs
		{8, 100000, 200_000, nil, 2},         // two ranges of 100k
		{8, 100000, 10_000_000, nil, 8},      // one range per worker
		{8, -1, 10_000_000, nil, 1},          // never split
		{8, 100000, 10_000_000, collated, 1}, // collation orders _id differently
	} {
		r := &runner{cfg: Config{SnapshotParallel: c.parallel, SplitMinDocs: c.minDocs}}
		if got := r.splitParts(collectionInfo{Name: "c", Options: c.options}, c.estimate); got != c.want {
			t.Errorf("parallel %d min %d estimate %d: %d ranges, want %d", c.parallel, c.minDocs, c.estimate, got, c.want)
		}
	}
}
