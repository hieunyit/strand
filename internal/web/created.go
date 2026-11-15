package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// CreatedFile là tên file registry các pipeline tạo từ web, nằm trong state_dir (thư mục duy nhất
// luôn ghi được dưới unit systemd có ProtectSystem=strict). Mode 0600, ghi nguyên tử.
const CreatedFile = "pipelines.json"

type createdFileDoc struct {
	Version   int             `json:"version"`
	Pipelines []PipelineEntry `json:"pipelines"`
}

func (r *Registry) createdPath() string { return filepath.Join(r.cfg.StateDir, CreatedFile) }

// loadCreated đọc registry state_dir; file chưa có = rỗng. Lỗi parse trả error (Refresh ghi vào Warnings).
func (r *Registry) loadCreated() ([]PipelineEntry, error) {
	b, err := os.ReadFile(r.createdPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var d createdFileDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("parse %s: %w", r.createdPath(), err)
	}
	return d.Pipelines, nil
}

// AddCreated lưu entry vào <state_dir>/pipelines.json (thay entry cùng tên nếu có).
func (r *Registry) AddCreated(e PipelineEntry) error {
	r.createdMu.Lock()
	defer r.createdMu.Unlock()
	cur, err := r.loadCreated()
	if err != nil {
		return err
	}
	var out []PipelineEntry
	for _, c := range cur {
		if c.Name != e.Name {
			out = append(out, c)
		}
	}
	out = append(out, e)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	b, err := json.MarshalIndent(createdFileDoc{Version: 1, Pipelines: out}, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(r.cfg.StateDir, 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(r.cfg.StateDir, ".pipelines-json-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), r.createdPath())
}
