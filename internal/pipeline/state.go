package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"strand/internal/snapshot"
)

// localState lưu cục bộ (thư mục resources.spool.path) những bảng đã snapshot xong.
//
// Trạng thái này chỉ là tối ưu: mất file thì mọi bảng được snapshot lại, và snapshot lại
// luôn đúng vì flush thay thế nguyên range ở target trong một transaction (RS-1).
// Không lưu trên target vì spec 02 §4 chỉ cho phép 3 bảng metadata với số row cố định.
type localState struct {
	mu   sync.Mutex
	path string
	data stateFile
}

type stateFile struct {
	PipelineID string                           `json:"pipeline_id"`
	Done       map[string]bool                  `json:"snapshot_done"`
	Plans      map[string]snapshot.SnapshotPlan `json:"snapshot_plans,omitempty"`
}

func (s *localState) Plan(table string) (snapshot.SnapshotPlan, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.data.Plans[table]
	return p, ok
}

func (s *localState) SetPlan(table string, p snapshot.SnapshotPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Plans == nil {
		s.data.Plans = map[string]snapshot.SnapshotPlan{}
	}
	s.data.Plans[table] = p
	return s.flushLocked()
}

func openLocalState(dir, pipelineID string) (*localState, error) {
	if dir == "" {
		return nil, fmt.Errorf("resources.spool.path is required (local pipeline state)")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	s := &localState{path: filepath.Join(dir, "pipeline-state.json"), data: stateFile{PipelineID: pipelineID, Done: map[string]bool{}}}
	raw, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, fmt.Errorf("read state: %w", err)
	}
	var f stateFile
	if err := json.Unmarshal(raw, &f); err != nil {
		// File hỏng: coi như mất, snapshot lại toàn bộ (an toàn).
		return s, nil
	}
	if f.PipelineID != pipelineID || f.Done == nil {
		return s, nil
	}
	s.data = f
	return s, nil
}

func (s *localState) IsDone(table string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Done[table]
}

func (s *localState) DoneTables() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for t, d := range s.data.Done {
		if d {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

func (s *localState) SetDone(table string, done bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if done {
		s.data.Done[table] = true
	} else {
		delete(s.data.Done, table)
	}
	// Bảng xong hoặc phải snapshot lại từ đầu: bỏ plan cũ.
	delete(s.data.Plans, table)
	return s.flushLocked()
}

func (s *localState) flushLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}
