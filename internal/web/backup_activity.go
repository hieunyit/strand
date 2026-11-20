package web

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"strand/internal/web/store"
)

// Audit of backup writes. Every mutating backup route records an entry BEFORE it takes effect (the request is
// refused when the audit log cannot be written) with the acting user, and for edits the changed fields
// ("changes": {field: {from, to}}); secret values never appear (sealed secrets are only flagged
// set/cleared/changed, and detail maps pass through maskAuditSecrets as a second line of defence).

// auditUser is the signed-in user of the request ("" when unknown).
func auditUser(r *http.Request) string {
	if u, ok := r.Context().Value(userCtxKey).(string); ok {
		return u
	}
	return ""
}

// secretDetailKeys: detail keys whose string values are replaced by "***".
var secretDetailKeys = map[string]bool{"password": true, "secret": true, "secret_key": true, "access_key": true, "token": true,
	"uri": true, "url": true, "webhook_url": true, "passphrase": true, "api_key": true, "smtp_password": true, "session_token": true}

// maskAuditSecrets returns a copy of detail with secret-named values masked (recursively).
func maskAuditSecrets(detail map[string]any) map[string]any {
	if detail == nil {
		return nil
	}
	out := make(map[string]any, len(detail))
	for k, v := range detail {
		out[k] = maskAuditValue(k, v)
	}
	return out
}

func maskAuditValue(key string, v any) any {
	switch x := v.(type) {
	case string:
		if secretDetailKeys[strings.ToLower(key)] && x != "" && x != "set" && x != "cleared" && x != "changed" {
			return "***"
		}
		return x
	case map[string]any:
		return maskAuditSecrets(x)
	case []any:
		c := make([]any, len(x))
		for i := range x {
			c[i] = maskAuditValue(key, x[i])
		}
		return c
	}
	return v
}

// changedFields compares the JSON forms of old and nw and returns {field: {"from": a, "to": b}} for every
// top-level field that differs. Types hide their secrets from JSON (json:"-"), so none can appear here.
func changedFields(old, nw any) map[string]any {
	om, nm := jsonMap(old), jsonMap(nw)
	out := map[string]any{}
	for k, nv := range nm {
		ov, had := om[k]
		if had && reflect.DeepEqual(ov, nv) {
			continue
		}
		switch k {
		case "created_at", "updated_at", "last_test_at", "last_test_status", "last_test_detail", "next_run_at":
			continue
		}
		out[k] = map[string]any{"from": ov, "to": maskAuditValue(k, nv)}
		if secretDetailKeys[strings.ToLower(k)] {
			out[k] = "changed"
		}
	}
	for k, ov := range om {
		if _, ok := nm[k]; !ok {
			out[k] = map[string]any{"from": maskAuditValue(k, ov), "to": nil}
		}
	}
	return out
}

func jsonMap(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr && rv.IsNil() {
		return map[string]any{}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if json.Unmarshal(b, &m) != nil || m == nil {
		return map[string]any{}
	}
	return m
}

// sealedChange describes a change of a sealed secret without revealing it.
func sealedChange(old, nw []byte) string {
	switch {
	case len(old) == 0 && len(nw) > 0:
		return "set"
	case len(old) > 0 && len(nw) == 0:
		return "cleared"
	case string(old) != string(nw):
		return "changed"
	}
	return ""
}

// activityItem is one row of the backup activity feed.
type activityItem struct {
	ID     int64          `json:"id"`
	Time   time.Time      `json:"time"`
	User   string         `json:"user,omitempty"`
	IP     string         `json:"ip,omitempty"`
	Action string         `json:"action"`
	Target string         `json:"target,omitempty"`
	Result string         `json:"result"`
	Detail map[string]any `json:"detail,omitempty"`
}

// backupActivity reads up to limit audit rows whose action starts with prefix (default "backup": the
// backup., backup_job., backup_connection., backup_policy., backup_repository., backup_notify_channel.,
// backup_run. ... families), newest first, older than cursor (an audit row id). user filters on the actor.
func (s *Server) backupActivity(r *http.Request, prefix, user string, since, until *time.Time, cursor int64, limit int) ([]activityItem, string, error) {
	var out []activityItem
	before := cursor
	for iter := 0; iter < 20 && len(out) < limit; iter++ {
		rows, err := s.db.st.ListAuditFiltered(r.Context(), store.AuditFilter{ActionPrefix: prefix, BeforeID: before, Limit: 200, Since: since, Until: until})
		if err != nil {
			return nil, "", err
		}
		for _, row := range rows {
			before = row.ID
			var e AuditEntry
			_ = json.Unmarshal(row.Data, &e)
			if user != "" && e.User != user {
				continue
			}
			out = append(out, activityItem{ID: row.ID, Time: row.Time, User: e.User, IP: row.IP, Action: row.Action, Target: row.Pipeline,
				Result: row.Result, Detail: maskAuditSecrets(e.Detail)})
			if len(out) == limit {
				break
			}
		}
		if len(rows) < 200 {
			break
		}
	}
	// A full page may be followed by more rows: the cursor is the id of its last row (an empty next page ends it).
	next := ""
	if len(out) == limit {
		next = strconv.FormatInt(out[len(out)-1].ID, 10)
	}
	return out, next, nil
}

func (s *Server) apiBackupActivity(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	q := r.URL.Query()
	prefix := q.Get("action")
	if prefix == "" {
		prefix = "backup"
	}
	if !strings.HasPrefix(prefix, "backup") {
		writeErr(w, http.StatusBadRequest, "action must start with backup")
		return
	}
	limit := 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeErr(w, http.StatusBadRequest, "limit must be between 1 and 200")
			return
		}
		limit = n
	}
	var cursor int64
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			writeErr(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		cursor = n
	}
	since, ok1 := parseTimeParam(w, q.Get("from"), "from")
	until, ok2 := parseTimeParam(w, q.Get("to"), "to")
	if !ok1 || !ok2 {
		return
	}
	items, next, err := s.backupActivity(r, prefix, q.Get("user"), since, until, cursor, limit)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the audit log")
		return
	}
	if items == nil {
		items = []activityItem{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"activity": items, "next_cursor": next})
}

// parseTimeParam reads an RFC 3339 timestamp or a YYYY-MM-DD date (UTC); "" yields nil.
func parseTimeParam(w http.ResponseWriter, v, name string) (*time.Time, bool) {
	if v == "" {
		return nil, true
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, v); err == nil {
			t = t.UTC()
			return &t, true
		}
	}
	writeErr(w, http.StatusBadRequest, name+" must be an RFC 3339 time or a YYYY-MM-DD date")
	return nil, false
}
