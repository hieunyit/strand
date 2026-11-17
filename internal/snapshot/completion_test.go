package snapshot

import (
	"context"
	"errors"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

type completionSource struct {
	*PGChunkSource
	barrier   *bool
	markerErr error
	markers   int
}

func (c *completionSource) BeforeRegister(context.Context) error { return nil }
func (c *completionSource) Bounds(context.Context, model.TableID, string) (*TableBounds, error) {
	return &TableBounds{}, nil
}
func (c *completionSource) KeyBounds(context.Context, model.TableID, *PKCodec) (Bound, Bound, bool, error) {
	return Bound{}, Bound{}, false, nil
}
func (c *completionSource) CompletionWatermark(context.Context) (checkpoint.AppliedSet, error) {
	if *c.barrier {
		panic("completion marker emitted under router barrier")
	}
	c.markers++
	return checkpoint.NewPGAppliedSet(0x200), c.markerErr
}

func TestPGSnapshotCompletionWaitsWithBoundsGuard(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "t"}
	for _, generic := range []bool{false, true} {
		for _, kind := range []string{"new-empty", "resume-empty", "resume-finished"} {
			name := kind + "/int"
			if generic {
				name = kind + "/tuple"
			}
			t.Run(name, func(t *testing.T) {
				inBarrier := false
				bound := &PGSnapshot{Xmin: 100, Xmax: 100, BoundsLSN: 0x100}
				source := &completionSource{PGChunkSource: &PGChunkSource{boundSnap: bound}, barrier: &inBarrier}
				s := NewMySQLSnapshotter(DefaultChunkOptions())
				s.Chunks = source
				s.ClearTarget = func(context.Context, model.TableID) error { return nil }
				s.Barrier = func(_ context.Context, fn func() error) error {
					inBarrier = true
					defer func() { inBarrier = false }()
					return fn()
				}
				plan := SnapshotPlan{Empty: kind == "resume-empty", Finished: kind == "resume-finished", MinPK: 2, MaxPK: 100, NextPK: 100, BoundSnapshot: bound, GenericKey: generic}
				codec := &PKCodec{Cols: []PKColumn{{Name: "id", Kind: PKInt}}}
				if generic {
					codec = &PKCodec{Cols: []PKColumn{{Name: "id", Kind: PKBytes}}}
					plan.MinKey = toKeyVals([]model.Value{"a"})
					plan.MaxKey = toKeyVals([]model.Value{"z"})
				}
				s.LoadPlan = func(model.TableID) (SnapshotPlan, bool) { return plan, kind != "new-empty" }
				waits := 0
				partial := &model.RowChange{Table: table, Op: model.OpUpdate, Key: []model.Value{int64(1)}, Cols: []model.ColValue{{Name: "doc", Unchanged: true}}}
				if generic {
					partial.Key = []model.Value{"outside"}
				}
				err := s.SnapshotAddKeys(context.Background(), nil, nil, table, codec, []string{"id"}, func(_ context.Context, high checkpoint.AppliedSet) error {
					waits++
					if inBarrier || !s.Manager.IsRegistered(table) {
						t.Fatal("guard absent or router blocked during completion wait")
					}
					if high.High().LSN != 0x200 {
						t.Fatalf("wrong completion marker: %+v", high.High())
					}
					action, _ := s.Manager.HandleStreamEventXID(partial, model.Pos{Engine: model.EnginePG, LSN: 0x80}, 50)
					if action != ActionDiscard {
						t.Fatalf("historical partial event escaped before rendezvous: %v", action)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if waits != 1 || source.markers != 1 {
					t.Fatalf("completion markers=%d waits=%d", source.markers, waits)
				}
				// This is the pipeline's completion boundary: unregister only after the wait succeeded.
				s.Manager.RemoveTable(table)
			})
		}
	}
}

func TestPGSnapshotCompletionFailureKeepsGuard(t *testing.T) {
	for _, markerFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "rendezvous", true: "marker"}[markerFails], func(t *testing.T) {
			table := model.TableID{Schema: "app", Table: "empty"}
			blocked := false
			source := &completionSource{PGChunkSource: &PGChunkSource{boundSnap: &PGSnapshot{Xmin: 100, Xmax: 100, BoundsLSN: 0x100}}, barrier: &blocked}
			want := context.Canceled
			if markerFails {
				source.markerErr = want
			}
			s := NewMySQLSnapshotter(DefaultChunkOptions())
			s.Chunks = source
			s.ClearTarget = func(context.Context, model.TableID) error { return nil }
			waits := 0
			err := s.SnapshotAdd(context.Background(), nil, nil, table, "id", []string{"id"}, func(context.Context, checkpoint.AppliedSet) error { waits++; return want })
			if !errors.Is(err, want) {
				t.Fatalf("completion must propagate failure: %v", err)
			}
			if markerFails && waits != 0 {
				t.Fatal("wait invoked after marker failure")
			}
			if !markerFails && waits != 1 {
				t.Fatal("missing rendezvous")
			}
			if !s.Manager.IsRegistered(table) {
				t.Fatal("failed completion removed range guard")
			}
		})
	}
}

func TestPGEmptyPlanRestoresRereadObligations(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "empty"}
	for _, generic := range []bool{false, true} {
		t.Run(map[bool]string{false: "int", true: "tuple"}[generic], func(t *testing.T) {
			s := NewMySQLSnapshotter(DefaultChunkOptions())
			codec := &PKCodec{Cols: []PKColumn{{Name: "id", Kind: PKBytes}}}
			plan := SnapshotPlan{Empty: true, GenericKey: generic, Reread: []int64{7}, RereadKeyVals: [][]KeyVal{toKeyVals([]model.Value{"a"})}}
			clears := 0
			s.ClearUnread = func(_ context.Context, _ model.TableID, _ string, _, _ int64, hasRange bool, keys []int64) error {
				if hasRange || len(keys) != 1 {
					t.Fatal("clear must target only persisted reread")
				}
				clears++
				return nil
			}
			s.ClearUnreadRange = func(_ context.Context, _ model.TableID, _, _ Bound, hasRange bool, keys []Bound) error {
				if hasRange || len(keys) != 1 {
					t.Fatal("clear must target only persisted reread")
				}
				clears++
				return nil
			}

			stop := errors.New("inspect registered obligations")
			blocked := false
			s.Chunks = &completionSource{PGChunkSource: &PGChunkSource{}, barrier: &blocked}
			s.LoadPlan = func(model.TableID) (SnapshotPlan, bool) { return plan, true }
			s.Barrier = func(_ context.Context, register func() error) error {
				if err := register(); err != nil {
					return err
				}
				if clears != 1 {
					t.Fatalf("persisted unread row not cleared: %d", clears)
				}
				if got := s.Manager.RereadBounds(table); len(got) != 1 {
					t.Fatalf("lost empty-plan reread: %+v", got)
				}
				return stop
			}
			useCodec := codec
			if !generic {
				useCodec = &PKCodec{Cols: []PKColumn{{Name: "id", Kind: PKInt}}}
			}
			if err := s.SnapshotAddKeys(context.Background(), nil, nil, table, useCodec, []string{"id"}, func(context.Context, checkpoint.AppliedSet) error { return nil }); !errors.Is(err, stop) {
				t.Fatal(err)
			}
			if generic {
				if pending, err := plan.PendingKey(codec, []model.Value{"a"}); err != nil || !pending {
					t.Fatalf("empty plan reread not pending: %v %v", pending, err)
				}
				calls := 0
				err := s.walkGeneric(context.Background(), nil, table, newKeySQL(table, codec), plan, func(c *Chunk) error {
					calls++
					if len(c.KeySet) != 1 {
						t.Fatalf("unexpected reread: %+v", c)
					}
					return stop
				}, nil)
				if !errors.Is(err, stop) || calls != 1 {
					t.Fatalf("empty generic walk skipped reread: %v calls=%d", err, calls)
				}
			}
		})
	}
}
