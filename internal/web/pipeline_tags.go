package web

import (
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

var pipelineTag = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)
var endpointLabel = regexp.MustCompile(`^[a-zA-Z0-9_.:\[\],-]+$`)

func pipelineDisplayHost(host, dsn string) string {
	if host == "" {
		switch {
		case strings.HasPrefix(dsn, "mysql://"), strings.HasPrefix(dsn, "postgres://"),
			strings.HasPrefix(dsn, "postgresql://"), strings.HasPrefix(dsn, "mongodb://"),
			strings.HasPrefix(dsn, "mongodb+srv://"):
			if u, err := url.Parse(dsn); err == nil {
				host = u.Host
			}
		}
	}
	if endpointLabel.MatchString(host) {
		return host
	}
	return ""
}

func (s *Server) pageSavePipelineTags(w http.ResponseWriter, r *http.Request, sess *Session) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	if err := r.ParseForm(); err != nil {
		s.renderError(w, r, sess, http.StatusBadRequest, "Invalid tag form")
		return
	}
	raw := r.PostForm.Get("tags")
	if len(raw) > 512 {
		s.renderError(w, r, sess, http.StatusBadRequest, "The tag list is too long.")
		return
	}
	seen := map[string]bool{}
	var tags []string
	for _, tag := range strings.Fields(strings.ReplaceAll(strings.ToLower(raw), ",", " ")) {
		if !pipelineTag.MatchString(tag) {
			s.renderError(w, r, sess, http.StatusBadRequest, "Use letters, digits, hyphens or underscores in each tag (up to 32 characters).")
			return
		}
		if !seen[tag] {
			seen[tag] = true
			tags = append(tags, tag)
		}
	}
	if len(tags) > 15 {
		s.renderError(w, r, sess, http.StatusBadRequest, "Use up to 15 tags per pipeline.")
		return
	}
	sort.Strings(tags)
	name := r.PathValue("name")
	ok, err := s.db.st.SetPipelineTags(r.Context(), name, strings.Join(tags, " "))
	if err != nil {
		s.renderError(w, r, sess, http.StatusServiceUnavailable, "Could not save pipeline tags.")
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.reg.Refresh(r.Context())
	http.Redirect(w, r, "/pipelines/"+url.PathEscape(name)+"/config", http.StatusSeeOther)
}
