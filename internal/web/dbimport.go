package web

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/web/store"
)

// ---- nhập trạng thái file -> DB (lần khởi động đầu với DSN, hoặc `strand web db migrate`) ----

const (
	importedMetaKey        = "files_imported"
	auditImportProgressKey = "audit_import_lines"
)

// ImportSummary mô tả những gì đã nhập.
type ImportSummary struct {
	Admin     bool `json:"admin"`
	Token     bool `json:"api_token"`
	Pipelines int  `json:"pipelines"`
	Jobs      int  `json:"jobs"`
	JobOutput int  `json:"job_outputs"`
	Wizards   int  `json:"wizards"`
	Audit     int  `json:"audit_lines"`
}

// importer nhập trạng thái file vào kho. Mọi bước idempotent: thứ đã có trong DB được giữ nguyên (không ghi đè),
// audit.log tiếp tục từ dòng đã nhập cuối cùng.
type importer struct {
	st  store.Store
	cfg *Config
	box *store.Box
	log *slog.Logger
}

// fileItem là một thứ trong trạng thái file và tình trạng của nó so với DB.
type fileItem struct {
	Kind   string `json:"kind"` // admin | api_token | pipeline | job | job_output | wizard | audit
	Name   string `json:"name"`
	Status string `json:"status"` // new | present | conflict | skipped
	Note   string `json:"note,omitempty"`
	hash   string // sha256 của nội dung file (để kiểm sau khi nhập)
	path   string
	origin string
	entry  PipelineEntry
}

func sha(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func (im *importer) jobsDir() string    { return filepath.Join(im.cfg.StateDir, "jobs") }
func (im *importer) wizardsDir() string { return filepath.Join(im.cfg.StateDir, "wizards") }
func (im *importer) auditPath() string  { return filepath.Join(im.cfg.StateDir, "audit.log") }

// pipelineSources: pipelines.json (created), web.yaml pipelines:, scan (theo thứ tự ưu tiên; trùng tên bỏ).
func (im *importer) pipelineSources() ([]fileItem, []string) {
	var out []fileItem
	var warns []string
	seen := map[string]bool{}
	for _, e := range im.cfg.Pipelines {
		if ValidName(e.Name) && e.Config != "" && !seen[e.Name] {
			if e.SystemdUnit != "" && !strings.HasSuffix(e.SystemdUnit, ".service") {
				e.SystemdUnit += ".service"
			}
			seen[e.Name] = true
			out = append(out, fileItem{Kind: "pipeline", Name: e.Name, path: e.Config, origin: store.OriginFile, entry: e})
		}
	}
	reg := NewRegistry(im.cfg, nil)
	created, err := reg.loadCreatedFile()
	if err != nil {
		warns = append(warns, "pipelines.json skipped: "+err.Error())
	}
	for _, e := range created {
		if ValidName(e.Name) && e.Config != "" && !seen[e.Name] {
			seen[e.Name] = true
			out = append(out, fileItem{Kind: "pipeline", Name: e.Name, path: e.Config, origin: store.OriginCreated, entry: e})
		}
	}
	for _, pat := range im.cfg.Scan {
		ms, _ := filepath.Glob(pat)
		sort.Strings(ms)
		for _, m := range ms {
			base := filepath.Base(m)
			if abs, _ := filepath.Abs(m); (im.cfg.SourcePath != "" && abs == im.cfg.SourcePath) || base == "web.yaml" || base == "web.yml" {
				continue
			}
			name := strings.TrimSuffix(strings.TrimSuffix(base, ".yaml"), ".yml")
			if !ValidName(name) || seen[name] {
				continue
			}
			seen[name] = true
			e := PipelineEntry{Name: name, Config: m}
			if env := filepath.Join(filepath.Dir(m), name+".env"); fileExists(env) {
				e.EnvFile = env
			}
			out = append(out, fileItem{Kind: "pipeline", Name: name, path: m, origin: store.OriginScan, entry: e})
		}
	}
	return out, warns
}

// plan liệt kê trạng thái file và so với DB (không ghi gì). dbEmpty: lược đồ chưa có (dry-run trước khi migrate).
func (im *importer) plan(ctx context.Context, dbEmpty bool) ([]fileItem, []string, error) {
	var items []fileItem
	adm := store.Admin{}
	if !dbEmpty {
		a, _, err := im.st.Admin(ctx)
		if err != nil {
			return nil, nil, err
		}
		adm = a
	}
	cmp := func(kind, fileVal, dbVal string) {
		if fileVal == "" {
			return
		}
		it := fileItem{Kind: kind, Name: kind, hash: fileVal}
		switch {
		case dbVal == "":
			it.Status = "new"
		case dbVal == fileVal:
			it.Status = "present"
		default:
			it.Status, it.Note = "conflict", "the database already has a different value; the database value is kept"
		}
		items = append(items, it)
	}
	cmp("admin", im.cfg.PasswordHash, adm.PasswordHash)
	cmp("api_token", im.cfg.APITokenHash, adm.APITokenHash)

	pipes, warns := im.pipelineSources()
	for _, it := range pipes {
		b, err := os.ReadFile(it.path)
		if err != nil {
			it.Status, it.Note = "skipped", "config not readable: "+err.Error()
			items = append(items, it)
			continue
		}
		it.hash = sha(b)
		it.Status = "new"
		if !dbEmpty {
			row, ok, err := im.st.GetPipeline(ctx, it.Name)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				if row.HasConfig && sha([]byte(row.ConfigYAML)) == it.hash {
					it.Status = "present"
				} else {
					it.Status, it.Note = "conflict", "already in the database with a different config; the database copy is kept (the web sets the file aside on the next start)"
				}
			}
		}
		items = append(items, it)
	}

	haveJobs := map[string]bool{}
	if !dbEmpty {
		js, err := im.st.ListJobs(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, j := range js {
			haveJobs[j.ID] = true
		}
	}
	if ents, err := os.ReadDir(im.jobsDir()); err == nil {
		for _, e := range ents {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(im.jobsDir(), e.Name()))
			if err != nil {
				continue
			}
			var js JobStatus
			if json.Unmarshal(b, &js) != nil || !JobIDRE.MatchString(js.ID) || e.Name() != js.ID+".json" {
				continue
			}
			it := fileItem{Kind: "job", Name: js.ID, Status: "new", path: filepath.Join(im.jobsDir(), e.Name())}
			if haveJobs[js.ID] {
				it.Status = "present"
			}
			items = append(items, it)
			logp := filepath.Join(im.jobsDir(), js.ID+".log")
			if fi, err := os.Stat(logp); err == nil && fi.Size() > 0 {
				o := fileItem{Kind: "job_output", Name: js.ID, Status: "new", path: logp}
				if !dbEmpty && haveJobs[js.ID] {
					if n, err := im.st.JobOutputSize(ctx, js.ID); err == nil && n > 0 {
						o.Status = "present"
					}
				}
				items = append(items, o)
			}
		}
	}
	if ents, err := os.ReadDir(im.wizardsDir()); err == nil {
		for _, e := range ents {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(im.wizardsDir(), e.Name()))
			if err != nil {
				continue
			}
			var w wizardState
			if json.Unmarshal(b, &w) != nil || w.Kind == "" || w.Pipeline == "" {
				continue
			}
			it := fileItem{Kind: "wizard", Name: w.Kind + "/" + w.Pipeline, Status: "new", path: filepath.Join(im.wizardsDir(), e.Name())}
			if !dbEmpty {
				if _, ok, _ := im.st.GetWizard(ctx, w.Kind, w.Pipeline); ok {
					it.Status = "present"
				}
			}
			items = append(items, it)
		}
	}
	total, err := countAuditLines(im.auditPath())
	if err != nil {
		return nil, nil, err
	}
	if total > 0 {
		done := 0
		if !dbEmpty {
			if v, ok, err := im.st.MetaGet(ctx, auditImportProgressKey); err != nil {
				return nil, nil, err
			} else if ok {
				done, _ = strconv.Atoi(v)
			}
		}
		it := fileItem{Kind: "audit", Name: "audit.log", Status: "new", Note: fmt.Sprintf("%d lines, %d already imported", total, done), hash: strconv.Itoa(total)}
		if done >= total {
			it.Status = "present"
		}
		items = append(items, it)
	}
	return items, warns, nil
}

// apply nhập mọi thứ còn thiếu. Gọi trong khóa advisory "import".
func (im *importer) apply(ctx context.Context) (*ImportSummary, error) {
	st := im.st
	sum := &ImportSummary{}
	adm, have, err := st.Admin(ctx)
	if err != nil {
		return nil, err
	}
	if (!have || adm.PasswordHash == "") && im.cfg.PasswordHash != "" {
		if err := st.SetPasswordHash(ctx, im.cfg.PasswordHash); err != nil {
			return nil, err
		}
		sum.Admin = true
	}
	if adm.APITokenHash == "" && im.cfg.APITokenHash != "" {
		if err := st.SetAPITokenHash(ctx, im.cfg.APITokenHash); err != nil {
			return nil, err
		}
		sum.Token = true
	}
	pipes, warns := im.pipelineSources()
	for _, w := range warns {
		im.log.Warn("import: " + w)
	}
	for _, it := range pipes {
		if _, ok, err := st.GetPipeline(ctx, it.Name); err != nil {
			return nil, err
		} else if ok {
			continue
		}
		e := it.entry
		row := store.Pipeline{Name: e.Name, ConfigPath: e.Config, EnvPath: e.EnvFile, SystemdUnit: e.SystemdUnit, LogFile: e.LogFile, PidFile: e.PidFile, Origin: it.origin}
		b, err := os.ReadFile(e.Config)
		if err != nil {
			if it.origin != store.OriginCreated {
				continue // khai báo/scan mà file không đọc được: web đăng ký khi file có lại
			}
		} else {
			row.ConfigYAML, row.HasConfig = string(b), true
		}
		if e.EnvFile != "" && im.box != nil {
			if eb, err := os.ReadFile(e.EnvFile); err == nil {
				if row.EnvEnc, err = im.box.Seal(eb, e.Name); err != nil {
					return nil, err
				}
			}
		}
		if ok, err := st.CreatePipeline(ctx, row); err != nil {
			return nil, err
		} else if ok {
			sum.Pipelines++
		}
	}
	have2 := map[string]bool{}
	js, err := st.ListJobs(ctx)
	if err != nil {
		return nil, err
	}
	for _, j := range js {
		have2[j.ID] = true
	}
	if ents, err := os.ReadDir(im.jobsDir()); err == nil {
		for _, e := range ents {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(im.jobsDir(), e.Name()))
			if err != nil {
				continue
			}
			var j JobStatus
			if json.Unmarshal(b, &j) != nil || !JobIDRE.MatchString(j.ID) || e.Name() != j.ID+".json" {
				continue
			}
			if !have2[j.ID] {
				if err := st.UpsertJob(ctx, store.Job{ID: j.ID, Pipeline: j.Pipeline, Started: j.Started, Running: j.Running, Data: b}); err != nil {
					return nil, err
				}
				sum.Jobs++
			}
			n, err := im.importJobOutput(ctx, j.ID)
			if err != nil {
				return nil, err
			}
			if n {
				sum.JobOutput++
			}
		}
	}
	if ents, err := os.ReadDir(im.wizardsDir()); err == nil {
		for _, e := range ents {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(im.wizardsDir(), e.Name()))
			if err != nil {
				continue
			}
			var w wizardState
			if json.Unmarshal(b, &w) != nil || w.Kind == "" || w.Pipeline == "" {
				continue
			}
			if _, ok, _ := st.GetWizard(ctx, w.Kind, w.Pipeline); ok {
				continue
			}
			if err := st.PutWizard(ctx, w.Kind, w.Pipeline, b); err != nil {
				return nil, err
			}
			sum.Wizards++
		}
	}
	n, err := im.importAuditFile(ctx)
	if err != nil {
		return nil, err
	}
	sum.Audit = n
	return sum, nil
}

// importJobOutput chép <id>.log vào job_output nếu DB chưa có output của job đó (không xóa file).
func (im *importer) importJobOutput(ctx context.Context, id string) (bool, error) {
	f, err := os.Open(filepath.Join(im.jobsDir(), id+".log"))
	if err != nil {
		return false, nil
	}
	defer f.Close()
	if n, err := im.st.JobOutputSize(ctx, id); err != nil || n > 0 {
		return false, err
	}
	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		return false, err
	}
	var off int64
	buf := make([]byte, maxOutputChunk)
	for off < fi.Size() && off < MaxJobOutputDB {
		want := int64(len(buf))
		if rest := MaxJobOutputDB - off; rest < want {
			want = rest
		}
		m, err := f.ReadAt(buf[:want], off)
		if m > 0 {
			if err := im.st.AppendJobOutput(ctx, id, off, buf[:m]); err != nil {
				return false, err
			}
			off += int64(m)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return false, err
		}
	}
	if fi.Size() > MaxJobOutputDB {
		note := fmt.Sprintf("\n[strand web: output truncated, %d bytes in total; only the first %d MB are kept in the database]\n", fi.Size(), MaxJobOutputDB>>20)
		if err := im.st.AppendJobOutput(ctx, id, off, []byte(note)); err != nil {
			return false, err
		}
	}
	return true, nil
}

func countAuditLines(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	n := 0
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		var e AuditEntry
		if len(line) > 0 && json.Unmarshal(line, &e) == nil && e.Action != "" {
			n++
		}
	}
	return n, sc.Err()
}

func (im *importer) importAuditFile(ctx context.Context) (int, error) {
	f, err := os.Open(im.auditPath())
	if err != nil {
		return 0, nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	// Tiến độ (số dòng hợp lệ đã nhập) ghi cùng transaction với mỗi lô: lần nhập bị ngắt giữa chừng (mất DB,
	// container bị dừng) tiếp tục đúng chỗ, không nhân đôi dòng (audit_log chỉ-thêm, không xóa được).
	skip := 0
	if v, ok, err := im.st.MetaGet(ctx, auditImportProgressKey); err != nil {
		return 0, err
	} else if ok {
		skip, _ = strconv.Atoi(v)
	}
	var batch []store.AuditRow
	total, seen := 0, 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := im.st.AppendAuditBatch(ctx, batch, &store.MetaKV{Key: auditImportProgressKey, Value: strconv.Itoa(seen)}); err != nil {
			return err
		}
		total += len(batch)
		batch = batch[:0]
		return nil
	}
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e AuditEntry
		if json.Unmarshal(line, &e) != nil || e.Action == "" {
			continue
		}
		seen++
		if seen <= skip {
			continue
		}
		batch = append(batch, store.AuditRow{Time: e.Time, IP: e.IP, Action: e.Action, Pipeline: e.Pipeline, Result: e.Result, Data: append([]byte(nil), line...)})
		if len(batch) >= 1000 {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, sc.Err()
}

// importFiles (khởi động web có DSN): nhập trạng thái file đúng một lần (cờ files_imported, khóa advisory).
func (s *Server) importFiles(ctx context.Context) (*ImportSummary, error) {
	st := s.db.st
	var sum *ImportSummary
	err := st.Locked(ctx, "import", func(ctx context.Context) error {
		if _, done, err := st.MetaGet(ctx, importedMetaKey); err != nil || done {
			return err
		}
		im := &importer{st: st, cfg: s.cfg, box: s.db.box, log: s.log}
		var err error
		if sum, err = im.apply(ctx); err != nil {
			return err
		}
		b, _ := json.Marshal(sum)
		return st.MetaSet(ctx, importedMetaKey, time.Now().UTC().Format(time.RFC3339)+" "+string(b))
	})
	return sum, err
}
