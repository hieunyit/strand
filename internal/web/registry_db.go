package web

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"strand/internal/web/store"
)

// Chế độ DB: bảng pipelines là nguồn sự thật của DANH SÁCH pipeline và của nội dung cấu hình/.env.
//
//   - Pipeline khai báo trong web.yaml (`pipelines:`) và file tìm thấy bởi `scan` được **phát hiện** vào DB
//     (origin web.yaml | scan) ở lần quét đầu tiên thấy chúng: YAML (và .env mã hóa khi có khóa) chụp từ đĩa.
//   - Đường dẫn/unit của pipeline khai báo trong web.yaml lấy theo web.yaml (người vận hành quản lý); nội dung
//     cấu hình lấy theo DB.
//   - Entry web.yaml bị xóa hoặc file scan biến mất: pipeline **được giữ** và đánh dấu missing (cảnh báo +
//     banner); người dùng chọn Restore file (dựng lại từ DB) hoặc Remove from registry (xóa dòng DB).
//   - Trước validate/start, file trên đĩa khác bản DB: bản DB thắng, bản trên đĩa được cất sang
//     <file>.conflict-<thời gian> và pipeline có banner (Adopt disk version / Dismiss). Không ghi đè lặng lẽ.

// DiskConflict là một bản sửa trên đĩa đã bị đặt sang bên.
type DiskConflict struct {
	File string `json:"file"` // config | env
	Path string `json:"path"` // bản cất
	At   string `json:"at"`
}

func parseConflicts(s string) []DiskConflict {
	var out []DiskConflict
	_ = json.Unmarshal([]byte(s), &out)
	return out
}

func (r *Registry) refreshDB(ctx context.Context) {
	var warns []string
	declared := map[string]PipelineEntry{}
	for _, e := range r.cfg.Pipelines {
		if !ValidName(e.Name) {
			warns = append(warns, fmt.Sprintf("pipeline %q skipped: invalid name", e.Name))
			continue
		}
		if e.SystemdUnit != "" && !strings.HasSuffix(e.SystemdUnit, ".service") {
			e.SystemdUnit += ".service"
		}
		declared[e.Name] = e
	}
	scanned := map[string]PipelineEntry{}
	scan := r.cfg.Scan
	if r.scanFn != nil {
		scan = r.scanFn()
	}
	for _, pat := range scan {
		matches, err := filepath.Glob(pat)
		if err != nil {
			warns = append(warns, fmt.Sprintf("scan %q: %v", pat, err))
			continue
		}
		sort.Strings(matches)
		for _, m := range matches {
			base := filepath.Base(m)
			if abs, _ := filepath.Abs(m); (r.cfg.SourcePath != "" && abs == r.cfg.SourcePath) || base == "web.yaml" || base == "web.yml" {
				continue
			}
			name := strings.TrimSuffix(strings.TrimSuffix(base, ".yaml"), ".yml")
			if !ValidName(name) {
				warns = append(warns, fmt.Sprintf("scan: %s skipped: file name is not a valid pipeline name", m))
				continue
			}
			if _, dup := declared[name]; dup {
				continue
			}
			if _, dup := scanned[name]; dup {
				continue
			}
			e := PipelineEntry{Name: name, Config: m}
			if env := filepath.Join(filepath.Dir(m), name+".env"); fileExists(env) {
				e.EnvFile = env
			}
			if unit := "strand@" + name + ".service"; r.UnitExists(ctx, unit) {
				e.SystemdUnit = unit
			}
			scanned[name] = e
		}
	}

	dctx, cancel := dbCtx()
	defer cancel()
	rows, err := r.db.st.ListPipelines(dctx)
	r.createdMu.Lock()
	if err != nil {
		rows = r.lastRows
		warns = append(warns, "read pipelines from the metadata database failed (showing the last known list): "+err.Error())
	} else {
		r.lastRows = rows
	}
	r.createdMu.Unlock()
	byRow := map[string]store.Pipeline{}
	for _, row := range rows {
		byRow[row.Name] = row
	}
	dbOK := err == nil && (r.writable == nil || r.writable()) // standby: chỉ đọc registry, không ghi
	byName := map[string]*Pipeline{}
	now := r.now().UTC()

	add := func(row store.Pipeline, p *Pipeline) {
		p.Conflicts = parseConflicts(row.Conflicts)
		byName[p.Name] = p
	}
	for name, e := range declared {
		p := &Pipeline{Name: name, ConfigPath: e.Config, EnvFile: e.EnvFile, SystemdUnit: e.SystemdUnit, LogFile: e.LogFile, PidFile: e.PidFile, Origin: "web.yaml"}
		row, ok := byRow[name]
		if !ok {
			if dbOK {
				r.discover(dctx, e, store.OriginFile, &warns)
			}
			byName[name] = p
			continue
		}
		// Khai báo tường minh thắng scan và pipeline tạo từ web cùng tên.
		if row.Origin == store.OriginCreated {
			warns = append(warns, fmt.Sprintf("pipeline %q is in both web.yaml and %s: using the web.yaml entry", name, r.createdPath()))
		}
		if dbOK && (row.ConfigPath != e.Config || row.EnvPath != e.EnvFile || row.SystemdUnit != e.SystemdUnit || row.LogFile != e.LogFile ||
			row.PidFile != e.PidFile || row.Origin != store.OriginFile || !row.MissingSince.IsZero()) {
			row.ConfigPath, row.EnvPath, row.SystemdUnit, row.LogFile, row.PidFile = e.Config, e.EnvFile, e.SystemdUnit, e.LogFile, e.PidFile
			row.Origin, row.MissingSince = store.OriginFile, time0
			r.updateMeta(dctx, row, &warns)
		}
		add(row, p)
	}
	for name, e := range scanned {
		row, ok := byRow[name]
		if !ok {
			if dbOK {
				r.discover(dctx, e, store.OriginScan, &warns)
			}
			byName[name] = &Pipeline{Name: name, ConfigPath: e.Config, EnvFile: e.EnvFile, SystemdUnit: e.SystemdUnit, Origin: "scan"}
			continue
		}
		if dbOK && row.Origin == store.OriginScan && !row.MissingSince.IsZero() {
			row.MissingSince = time0
			r.updateMeta(dctx, row, &warns)
		}
		add(row, rowPipeline(row))
	}
	for _, row := range rows {
		if _, done := byName[row.Name]; done {
			continue
		}
		p := rowPipeline(row)
		switch row.Origin {
		case store.OriginFile:
			p.Missing = "no longer declared in web.yaml (pipelines:)"
		case store.OriginScan:
			if fileExists(row.ConfigPath) {
				p.Missing = "its config file is no longer matched by the scan globs"
			} else {
				p.Missing = "its config file " + row.ConfigPath + " was removed from disk"
			}
		}
		if p.Missing != "" {
			warns = append(warns, fmt.Sprintf("pipeline %q: %s; it is kept in the database (open it to restore the file or remove it from the registry)", row.Name, p.Missing))
			if dbOK && row.MissingSince.IsZero() {
				row.MissingSince = now
				r.updateMeta(dctx, row, &warns)
			}
		}
		add(row, p)
	}
	r.publish(byName, warns)
}

var time0 = store.Pipeline{}.MissingSince

func rowPipeline(row store.Pipeline) *Pipeline {
	origin := row.Origin
	if origin == "" {
		origin = store.OriginCreated
	}
	return &Pipeline{Name: row.Name, ConfigPath: row.ConfigPath, EnvFile: row.EnvPath, SystemdUnit: row.SystemdUnit,
		LogFile: row.LogFile, PidFile: row.PidFile, Origin: origin}
}

func (r *Registry) updateMeta(ctx context.Context, row store.Pipeline, warns *[]string) {
	if err := r.db.st.UpdatePipelineMeta(ctx, row); err != nil {
		*warns = append(*warns, fmt.Sprintf("pipeline %q: update registry row: %v", row.Name, err))
	}
}

// discover ghi pipeline mới thấy (web.yaml/scan) vào DB, chụp YAML và .env (mã hóa) từ đĩa. Không ghi đè dòng
// đã có (instance khác có thể vừa ghi).
func (r *Registry) discover(ctx context.Context, e PipelineEntry, origin string, warns *[]string) {
	b, err := os.ReadFile(e.Config)
	if err != nil {
		*warns = append(*warns, fmt.Sprintf("pipeline %q: config %s not readable, not registered in the database yet: %v", e.Name, e.Config, err))
		return
	}
	row := store.Pipeline{Name: e.Name, ConfigPath: e.Config, EnvPath: e.EnvFile, SystemdUnit: e.SystemdUnit, LogFile: e.LogFile,
		PidFile: e.PidFile, ConfigYAML: string(b), HasConfig: true, Origin: origin}
	if e.EnvFile != "" && r.db.box != nil {
		if eb, err := os.ReadFile(e.EnvFile); err == nil {
			if row.EnvEnc, err = r.db.box.Seal(eb, e.Name); err != nil {
				*warns = append(*warns, fmt.Sprintf("pipeline %q: seal .env: %v", e.Name, err))
				return
			}
		}
	}
	if ok, err := r.db.st.CreatePipeline(ctx, row); err != nil {
		*warns = append(*warns, fmt.Sprintf("pipeline %q: register in the database: %v", e.Name, err))
	} else if ok {
		r.db.log.Info("registered pipeline in the metadata database", "pipeline", e.Name, "origin", origin, "config", e.Config)
	}
}
