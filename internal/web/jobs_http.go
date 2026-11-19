package web

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"strand/internal/admin"
)

// REST và trang của job (docs/WEB-API.md §4). Hủy job đi qua action `job-cancel` (có audit).

// SourceTablesView: bảng của source để chọn khi snapshot add / verify.
type SourceTablesView struct {
	Tables []admin.SourceTable `json:"tables"`
	Error  string              `json:"error,omitempty"`
}

type sourceTablesCacheEntry struct {
	tables    []admin.SourceTable
	expiresAt time.Time
}

const sourceTablesCacheTTL = 30 * time.Second

func (s *Server) sourceTablesView(ctx context.Context, p *Pipeline) SourceTablesView {
	if p == nil {
		return SourceTablesView{Tables: []admin.SourceTable{}, Error: "pipeline is unavailable"}
	}
	now := time.Now()
	s.sourceTablesMu.Lock()
	if cached, ok := s.sourceTablesCache[p.Name]; ok && now.Before(cached.expiresAt) {
		v := SourceTablesView{Tables: append([]admin.SourceTable(nil), cached.tables...)}
		s.sourceTablesMu.Unlock()
		return v
	}
	s.sourceTablesMu.Unlock()

	v := SourceTablesView{Tables: []admin.SourceTable{}}
	err := s.withAdminTimeout(ctx, p, 9*time.Second, func(ctx context.Context, c *admin.Client) (err error) {
		v.Tables, err = c.SourceTables(ctx)
		return
	})
	if err != nil {
		v.Error = notRunningMsg(err)
	}
	if v.Tables == nil {
		v.Tables = []admin.SourceTable{}
	}
	if err == nil {
		s.sourceTablesMu.Lock()
		s.sourceTablesCache[p.Name] = sourceTablesCacheEntry{
			tables: append([]admin.SourceTable(nil), v.Tables...), expiresAt: now.Add(sourceTablesCacheTTL),
		}
		s.sourceTablesMu.Unlock()
	}
	return v
}

// cachedSourceTables trả bảng của source nếu còn trong cache (không gọi admin socket); ok=false: trang để
// trình duyệt tải qua GET /api/v1/pipelines/{name}/source-tables khi người dùng cần.
func (s *Server) cachedSourceTables(p *Pipeline) (SourceTablesView, bool) {
	s.sourceTablesMu.Lock()
	defer s.sourceTablesMu.Unlock()
	if cached, ok := s.sourceTablesCache[p.Name]; ok && time.Now().Before(cached.expiresAt) {
		return SourceTablesView{Tables: append([]admin.SourceTable(nil), cached.tables...)}, true
	}
	return SourceTablesView{Tables: []admin.SourceTable{}}, false
}

func (s *Server) invalidateSourceTables(name string) {
	s.sourceTablesMu.Lock()
	delete(s.sourceTablesCache, name)
	s.sourceTablesMu.Unlock()
}

func (s *Server) listJobs(pipeline string) []JobStatus {
	if s.jobs == nil {
		return []JobStatus{}
	}
	out := s.jobs.List(pipeline)
	if out == nil {
		out = []JobStatus{}
	}
	return out
}

func (s *Server) apiSourceTables(w http.ResponseWriter, r *http.Request, _ *Session) {
	if p := s.lookup(w, r); p != nil {
		writeJSON(w, http.StatusOK, s.sourceTablesView(r.Context(), p))
	}
}

func (s *Server) apiPipelineJobs(w http.ResponseWriter, r *http.Request, _ *Session) {
	if p := s.lookup(w, r); p != nil {
		writeJSON(w, http.StatusOK, map[string]any{"jobs": s.listJobs(p.Name)})
	}
}

func (s *Server) apiJobs(w http.ResponseWriter, r *http.Request, _ *Session) {
	pl := r.URL.Query().Get("pipeline")
	if pl != "" && !ValidName(pl) {
		writeErr(w, http.StatusBadRequest, "invalid pipeline")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": s.listJobs(pl)})
}

func (s *Server) lookupJob(w http.ResponseWriter, r *http.Request) (JobStatus, bool) {
	id := r.PathValue("id")
	if s.jobs == nil || !JobIDRE.MatchString(id) {
		writeErr(w, http.StatusNotFound, "unknown job")
		return JobStatus{}, false
	}
	j, ok := s.jobs.Get(id)
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown job")
	}
	return j, ok
}

func (s *Server) apiJob(w http.ResponseWriter, r *http.Request, _ *Session) {
	if j, ok := s.lookupJob(w, r); ok {
		writeJSON(w, http.StatusOK, map[string]any{"job": j})
	}
}

// apiJobOutput: output mới kể từ ?offset=N (byte). Client lặp lại với "offset" trả về cho tới khi running=false.
func (s *Server) apiJobOutput(w http.ResponseWriter, r *http.Request, _ *Session) {
	j, ok := s.lookupJob(w, r)
	if !ok {
		return
	}
	off, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	out, _ := s.jobs.Output(j.ID, off)
	writeJSON(w, http.StatusOK, out)
}

// ---- trang ----

type jobsBody struct {
	Scope    string // "" = tổng; tên pipeline
	Jobs     []JobStatus
	Verify   bool // hiện form verify (tab Jobs của pipeline)
	Tables   SourceTablesView
	View     PipelineView
	Pipeline string
}

func (s *Server) pageJobs(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.render(w, "jobs", sess, pageData{Title: "Jobs", Active: "jobs", Refresh: 5, Body: jobsBody{Jobs: s.listJobs("")}})
}

type jobBody struct {
	Job   JobStatus
	Chunk JobOutput
}

func (s *Server) pageJob(w http.ResponseWriter, r *http.Request, sess *Session) {
	id := r.PathValue("id")
	if s.jobs == nil || !JobIDRE.MatchString(id) {
		http.NotFound(w, r)
		return
	}
	j, ok := s.jobs.Get(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	// Output ban đầu nạp sẵn (HTML), phần sau lấy bằng polling ?offset= từ app.js.
	out, _ := s.jobs.Output(id, 0)
	s.render(w, "job", sess, pageData{Title: "Job " + j.Kind + " " + j.Pipeline, Active: j.Pipeline, Refresh: 0, Body: jobBody{Job: j, Chunk: out}})
}
