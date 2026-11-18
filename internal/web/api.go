package web

import (
	"net/http"
	"strconv"
)

// REST /api/v1 (docs/WEB-API.md). Mọi route đã qua middleware auth.

func (s *Server) apiSession(w http.ResponseWriter, r *http.Request, sess *Session) {
	out := map[string]any{"authenticated": true, "version": s.version,
		"idle_timeout_s": int(SessionIdleTimeout.Seconds()), "max_age_s": int(SessionMaxAge.Seconds())}
	if sess.CSRF != "" {
		out["csrf"] = sess.CSRF
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) apiOverview(w http.ResponseWriter, r *http.Request, _ *Session) {
	ov := s.samp.BuildOverview()
	out := map[string]any{"counts": ov.Counts, "total": ov.Total, "max_lag_seconds": ov.MaxLagSeconds,
		"max_lag_pipeline": ov.MaxLagPipeline, "rows_per_sec": ov.RowsPerSec, "warnings": s.reg.Warnings()}
	views := make([]PipelineView, 0, len(ov.Pipelines))
	for _, p := range s.reg.List() {
		views = append(views, s.view(p))
	}
	out["pipelines"] = views
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) apiPipelines(w http.ResponseWriter, r *http.Request, _ *Session) {
	views := make([]PipelineView, 0)
	for _, p := range s.reg.List() {
		views = append(views, s.view(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"pipelines": views, "warnings": s.reg.Warnings()})
}

func (s *Server) lookup(w http.ResponseWriter, r *http.Request) *Pipeline {
	p := s.reg.Get(r.PathValue("name"))
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
	}
	return p
}

func (s *Server) apiPipeline(w http.ResponseWriter, r *http.Request, _ *Session) {
	p := s.lookup(w, r)
	if p == nil {
		return
	}
	out := map[string]any{"pipeline": s.view(p), "status": s.statusView(r.Context(), p)}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) apiPipelineDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	p := s.lookup(w, r)
	if p == nil {
		return
	}
	a, ok := s.actions.Get(ActDelete)
	if !ok {
		writeErr(w, http.StatusNotFound, "delete action not supported")
		return
	}
	res, err := a.Run(r.Context(), s, ActionRequest{
		Pipeline: p,
		Args:     map[string]string{"confirm_name": p.Name},
		IP:       s.clientIP(r),
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) apiPipelineTab(w http.ResponseWriter, r *http.Request, tab string) {
	p := s.lookup(w, r)
	if p == nil {
		return
	}
	ctx := r.Context()
	switch tab {
	case "status":
		writeJSON(w, http.StatusOK, s.statusView(ctx, p))
	case "tables":
		writeJSON(w, http.StatusOK, s.tablesView(ctx, p))
	case "checkpoint":
		writeJSON(w, http.StatusOK, s.checkpointView(ctx, p))
	case "errors":
		writeJSON(w, http.StatusOK, s.errorsView(ctx, p))
	case "config":
		// Cấu hình đã che bí mật nhưng vẫn nhạy cảm: ghi audit mỗi lần xem.
		_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "config.view", Pipeline: p.Name})
		writeJSON(w, http.StatusOK, s.configView(p))
	case "logs":
		n, _ := strconv.Atoi(r.URL.Query().Get("lines"))
		writeJSON(w, http.StatusOK, s.logsView(ctx, p, n))
	case "series":
		writeJSON(w, http.StatusOK, s.seriesView(p))
	}
}
