package web

import (
	"net/http"
	"net/url"
	"time"
)

// Compliance report page (spec 2.7): success rates, RPO per source and the last test restore over a date
// range, with a CSV export of the same range. Readable by every role, like the report API.

type reportsPageBody struct {
	opsBody
	Report  complianceReport
	From    string // YYYY-MM-DD as typed / defaulted
	To      string
	CSVURL  string
	Message string
}

func (s *Server) pageBackupsReports(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil {
		http.Error(w, "backup reports require the metadata database", http.StatusServiceUnavailable)
		return
	}
	body := reportsPageBody{opsBody: roleBody(sess)}
	now := s.now().In(webTimeZone)
	to := now
	from := to.AddDate(0, 0, -30)
	q := r.URL.Query()
	parse := func(v string) (time.Time, bool) {
		t, err := time.ParseInLocation("2006-01-02", v, webTimeZone)
		return t, err == nil
	}
	if v := q.Get("from"); v != "" {
		if t, ok := parse(v); ok {
			from = t
		} else {
			body.Message = "From must be a date (YYYY-MM-DD)."
		}
	}
	if v := q.Get("to"); v != "" {
		if t, ok := parse(v); ok {
			to = t.Add(24 * time.Hour) // through the end of that day
		} else {
			body.Message = "To must be a date (YYYY-MM-DD)."
		}
	}
	if !from.Before(to) || to.Sub(from) > 366*24*time.Hour {
		body.Message = "From must be before To, at most 366 days apart; showing the last 30 days."
		to, from = now, now.AddDate(0, 0, -30)
	}
	body.From = from.Format("2006-01-02")
	body.To = to.Add(-time.Nanosecond).Format("2006-01-02")
	v := url.Values{}
	v.Set("from", from.Format(time.RFC3339Nano))
	v.Set("to", to.Format(time.RFC3339Nano))
	body.CSVURL = "/api/v1/backup-reports/compliance.csv?" + v.Encode()
	rep, err := s.buildCompliance(r.Context(), from, to)
	if err != nil {
		http.Error(w, "could not build the compliance report", http.StatusServiceUnavailable)
		return
	}
	body.Report = rep
	crumbs := append(opsCrumbs("", ""), BreadcrumbItem{Label: "Reports"})
	s.render(w, "backups_reports", sess, pageData{Title: "Compliance report", Subtitle: "Are all sources backed up on time, restorable, and to which point? Export the same range as CSV.",
		Active: "backups-reports", Breadcrumbs: crumbs, Body: body})
}
