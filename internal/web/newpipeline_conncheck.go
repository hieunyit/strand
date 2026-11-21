package web

import (
	"net/http"
	"regexp"
	"strings"

	"strand/internal/backup"
	"strand/internal/backup/conncheck"
)

// POST /api/v1/pipeline-connections/test: the "Test connection" buttons of the New pipeline wizard. It probes
// a source or target the operator has typed (nothing is saved) with the same checker the backup connections
// use, and answers with its Result: version, latency, privileges and replication prerequisites.
//
// Operator or admin (writeRules in rbac.go): a probe changes nothing, creating the pipeline stays admin. Every
// probe is audited before it runs. The password and the URI are used for this one request only: they are never
// logged, audited or echoed (conncheck redacts them from its messages).

type pipelineConnRequest struct {
	Role     string `json:"role"`   // source | target
	Engine   string `json:"engine"` // mysql | mariadb | postgres | mongodb
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	URI      string `json:"uri"` // mongodb://... (secret); API clients only, the console sends the fields below
	// MongoDB without a URI: Host may list several host:port (replica set members).
	AuthDB     string `json:"auth_db"`
	ReplicaSet string `json:"replica_set"`
	TLS        bool   `json:"tls"`
}

func (s *Server) pipelineConnRoutes(mux *http.ServeMux) {
	mux.Handle("POST /api/v1/pipeline-connections/test", s.auth(true, s.apiPipelineConnTest))
}

func (s *Server) apiPipelineConnTest(w http.ResponseWriter, r *http.Request, sess *Session) {
	var req pipelineConnRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	req.Role, req.Engine = strings.ToLower(strings.TrimSpace(req.Role)), strings.ToLower(strings.TrimSpace(req.Engine))
	req.Host, req.User = strings.TrimSpace(req.Host), strings.TrimSpace(req.User)
	if req.Role != "source" && req.Role != "target" {
		writeErr(w, http.StatusBadRequest, "role must be source or target")
		return
	}
	switch req.Engine {
	case backup.ConnMySQL, backup.ConnMariaDB, backup.ConnPostgres:
		if req.Host == "" || req.User == "" {
			writeErr(w, http.StatusBadRequest, "host and user are required")
			return
		}
	case backup.ConnMongoDB:
		if strings.TrimSpace(req.URI) == "" {
			uri, err := buildMongoURI(req.Role, mongoEndpoint{Hosts: req.Host, Port: req.Port, User: req.User, Password: req.Password,
				AuthDB: req.AuthDB, ReplicaSet: req.ReplicaSet, TLS: req.TLS})
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			req.URI = uri
		}
	default:
		writeErr(w, http.StatusBadRequest, "unsupported engine")
		return
	}
	if req.Port < 0 || req.Port > 65535 {
		writeErr(w, http.StatusBadRequest, "port must be between 1 and 65535")
		return
	}
	user := ""
	if sess != nil {
		user = sess.Username
	}
	detail := map[string]any{"role": req.Role, "engine": req.Engine}
	if req.Host != "" {
		detail["host"], detail["port"] = req.Host, req.Port
	}
	if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), User: user, Action: "pipeline.test_connection", Result: "started", Detail: detail}); err != nil {
		s.log.Error("audit pipeline connection test", "error", err)
		writeErr(w, http.StatusInternalServerError, "audit log unavailable; refusing the connection test")
		return
	}
	res := s.connChecker().Check(r.Context(), conncheck.Target{
		Engine: req.Engine, Host: req.Host, Port: req.Port, User: req.User, Password: req.Password, URI: strings.TrimSpace(req.URI),
	})
	writeJSON(w, http.StatusOK, res)
}

// createFormFields are the form fields a create error can be attached to.
var createFormFields = map[string]bool{
	"name": true, "from": true, "to": true, "source_host": true, "source_port": true, "target_host": true, "target_port": true,
	"source_user": true, "target_user": true, "source_password": true, "target_password": true,
	"source_password_env": true, "target_password_env": true, "databases": true, "database": true, "target_database": true,
	"target_pg_database": true, "schema": true, "include": true, "exclude": true, "commit_order": true, "spool_dir": true,
	"workers": true, "iops_soft": true, "iops_hard": true, "chunk_rows": true, "max_parallel": true, "start_at": true,
	"windows": true, "breaker_max_ratio": true, "replica_lag_max": true, "governor_algorithm": true, "lag_slo": true,
	"source_uri": true, "target_uri": true, "source_database": true, "target_mongo_database": true, "snapshot_batch": true,
	"stream_batch": true, "lease_ttl": true, "source_auth_db": true, "target_auth_db": true, "source_replica_set": true,
	"target_replica_set": true,
}

var createErrFieldRE = regexp.MustCompile(`^([a-z_]+)(?::| ")`)

// createErrField names the form field a create error is about ("" when it is about the pipeline as a whole), so
// the wizard can show the message under that field instead of only in a banner.
func createErrField(msg string) string {
	switch {
	case strings.HasPrefix(msg, "invalid pipeline name"), strings.Contains(msg, "already exists"), strings.Contains(msg, "is currently running"):
		return "name"
	case strings.Contains(msg, "can only be copied to"):
		return "to"
	}
	if m := createErrFieldRE.FindStringSubmatch(msg); m != nil && createFormFields[m[1]] {
		return m[1]
	}
	if strings.Contains(msg, "set target_pg_database") {
		return "target_pg_database"
	}
	return ""
}

// writeCreateErr is writeErr plus {"fields": {field: message}} when the error belongs to one form field.
func writeCreateErr(w http.ResponseWriter, status int, msg string) {
	f := createErrField(msg)
	if f == "" {
		writeErr(w, status, msg)
		return
	}
	writeJSON(w, status, map[string]any{"error": msg, "fields": map[string]string{f: msg}})
}
