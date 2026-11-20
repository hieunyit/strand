package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// CreatedFile là registry các pipeline tạo từ web của bản cài cũ (chế độ file, <state_dir>/pipelines.json).
// Chỉ còn được ĐỌC khi nhập vào DB (`strand web db migrate`, lần khởi động đầu) và GHI bởi `strand web db export`
// (bản sao lưu / quay về bản cũ).
const CreatedFile = "pipelines.json"

type createdFileDoc struct {
	Version   int             `json:"version"`
	Pipelines []PipelineEntry `json:"pipelines"`
}

func (r *Registry) createdPath() string { return "the metadata database" }

// loadCreatedFile đọc <state_dir>/pipelines.json của bản cài cũ; file chưa có = rỗng.
func (r *Registry) loadCreatedFile() ([]PipelineEntry, error) {
	path := filepath.Join(r.cfg.StateDir, CreatedFile)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var d createdFileDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return d.Pipelines, nil
}

// AddCreated ghi (hoặc cập nhật đường dẫn của) pipeline do web tạo vào DB.
func (r *Registry) AddCreated(e PipelineEntry) error {
	ctx, cancel := dbCtx()
	defer cancel()
	row, _, err := r.db.st.GetPipeline(ctx, e.Name)
	if err != nil {
		return err
	}
	row.Name, row.ConfigPath, row.EnvPath, row.SystemdUnit, row.LogFile, row.PidFile = e.Name, e.Config, e.EnvFile, e.SystemdUnit, e.LogFile, e.PidFile
	return r.db.st.UpsertPipeline(ctx, row)
}

// RemovePipeline xóa pipeline khỏi danh sách khai báo (bản trong RAM của web.yaml) và khỏi DB.
func (r *Registry) RemovePipeline(name string) error {
	r.mu.Lock()
	if r.cfg != nil {
		var newPipes []PipelineEntry
		for _, p := range r.cfg.Pipelines {
			if p.Name != name {
				newPipes = append(newPipes, p)
			}
		}
		r.cfg.Pipelines = newPipes
	}
	r.mu.Unlock()
	return r.RemoveCreated(name)
}

// RemoveCreated xóa dòng pipeline khỏi DB.
func (r *Registry) RemoveCreated(name string) error {
	ctx, cancel := dbCtx()
	defer cancel()
	return r.db.st.DeletePipeline(ctx, name)
}
