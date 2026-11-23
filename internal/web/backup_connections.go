package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/conncheck"
	"strand/internal/backup/runner"
	"strand/internal/backup/sshtun"
)

// Reusable backup connections (data sources). Stored secrets are sealed with the Web secret key
// (store.Box, the connection id as additional data) and never leave the process in any API response, HTML
// page, audit entry or log line: responses carry only password_set / uri_set. The Web process decrypts a
// stored secret only to hand it to the backup processes it starts, through their environment (never argv).

func (s *Server) backupConnectionRoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backup-connections", s.auth(true, s.apiConnList))
	mux.Handle("POST /api/v1/backup-connections", s.auth(true, s.apiConnSave(false)))
	mux.Handle("POST /api/v1/backup-connections/test", s.auth(true, s.apiConnTest(false)))
	mux.Handle("GET /api/v1/backup-connections/{id}", s.auth(true, s.apiConnGet))
	mux.Handle("PUT /api/v1/backup-connections/{id}", s.auth(true, s.apiConnSave(true)))
	mux.Handle("DELETE /api/v1/backup-connections/{id}", s.auth(true, s.apiConnDelete))
	mux.Handle("POST /api/v1/backup-connections/{id}/test", s.auth(true, s.apiConnTest(true)))
}

// connRequest is the write body. Password and URI are write-only: nil keeps the stored value on update.
type connRequest struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Engine      string            `json:"engine"`
	Host        string            `json:"host"`
	Port        int               `json:"port"`
	User        string            `json:"user"`
	SecretMode  string            `json:"secret_mode"`
	PasswordEnv string            `json:"password_env"`
	URIEnv      string            `json:"uri_env"`
	Password    *string           `json:"password"`
	URI         *string           `json:"uri"`
	TLSMode     string            `json:"tls_mode"`
	CACertPEM   string            `json:"ca_cert_pem"`
	SSHTunnel   *backup.SSHTunnel `json:"ssh_tunnel"`
	Labels      map[string]string `json:"labels"`
}

func randomConnID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "conn-" + hex.EncodeToString(b[:]), nil
}

// connFromRequest builds the connection to store: request fields replace the definition; secrets the
// request does not carry are kept from `cur` (update). Secrets are sealed under the connection id.
func (s *Server) connFromRequest(req connRequest, cur *backup.Connection) (backup.Connection, error) {
	c := backup.Connection{ID: req.ID, Name: req.Name, Engine: req.Engine, Host: req.Host, Port: req.Port, User: req.User,
		SecretMode: req.SecretMode, PasswordEnv: req.PasswordEnv, URIEnv: req.URIEnv, TLSMode: req.TLSMode,
		CACertPEM: req.CACertPEM, SSHTunnel: req.SSHTunnel, Labels: req.Labels}
	if cur != nil {
		c.ID = cur.ID
	}
	if c.SecretMode == "" && (req.PasswordEnv != "" || req.URIEnv != "") && (cur == nil || cur.SecretMode != backup.SecretEnv) {
		c.SecretMode = backup.SecretEnv // refused just below with the explanation
	}
	if c.SecretMode == "" {
		c.SecretMode = backup.SecretStored
		if cur != nil && cur.SecretMode == backup.SecretEnv && (req.PasswordEnv != "" || req.URIEnv != "") {
			c.SecretMode = backup.SecretEnv
		}
	}
	// Passwords and URIs are entered in the console and stored sealed in the metadata database. Reading them from
	// environment variables of the Web service is a legacy mode: existing connections keep working and stay
	// editable, but no connection can be created in, or switched to, that mode.
	if c.SecretMode == backup.SecretEnv && (cur == nil || cur.SecretMode != backup.SecretEnv) {
		return c, errors.New("passwords read from environment variables are no longer supported: enter the password (it is stored encrypted)")
	}
	if c.SecretMode == backup.SecretStored && s.db.box == nil {
		return c, errors.New(ErrNoSecretKey)
	}
	if c.SecretMode != backup.SecretStored && (req.Password != nil || req.URI != nil) {
		return c, errors.New("password and uri are only accepted with secret_mode stored (secret_mode env reads them from the Web environment)")
	}
	if c.SecretMode == backup.SecretStored {
		if cur != nil && cur.SecretMode == backup.SecretStored {
			c.PasswordSealed, c.URISealed = cur.PasswordSealed, cur.URISealed
		}
		seal := func(v *string, dst *[]byte) error {
			if v == nil {
				return nil
			}
			if *v == "" {
				*dst = nil
				return nil
			}
			if len(*v) > 4096 || strings.ContainsRune(*v, 0) {
				return errors.New("secret is too long or contains a NUL byte")
			}
			sealed, err := s.db.box.Seal([]byte(*v), c.ID)
			if err != nil {
				return errors.New("could not seal the secret")
			}
			*dst = sealed
			return nil
		}
		if err := seal(req.Password, &c.PasswordSealed); err != nil {
			return c, err
		}
		if err := seal(req.URI, &c.URISealed); err != nil {
			return c, err
		}
	}
	c.PasswordSet, c.URISet = len(c.PasswordSealed) > 0, len(c.URISealed) > 0
	return c, nil
}

// connChanges lists the changed non-secret fields (old, new) and flags secret changes without values.
func connChanges(old *backup.Connection, nw backup.Connection) map[string]any {
	out := map[string]any{}
	if old == nil {
		out["name"], out["engine"], out["host"], out["secret_mode"] = nw.Name, nw.Engine, nw.Host, nw.SecretMode
		if nw.PasswordSet {
			out["password"] = "set"
		}
		if nw.URISet {
			out["uri"] = "set"
		}
		return out
	}
	pair := func(k string, a, b any) {
		if !reflect.DeepEqual(a, b) {
			out[k] = map[string]any{"from": a, "to": b}
		}
	}
	pair("name", old.Name, nw.Name)
	pair("engine", old.Engine, nw.Engine)
	pair("host", old.Host, nw.Host)
	pair("port", old.Port, nw.Port)
	pair("user", old.User, nw.User)
	pair("secret_mode", old.SecretMode, nw.SecretMode)
	pair("password_env", old.PasswordEnv, nw.PasswordEnv)
	pair("uri_env", old.URIEnv, nw.URIEnv)
	pair("tls_mode", old.TLSMode, nw.TLSMode)
	pair("ssh_tunnel", old.SSHTunnel, nw.SSHTunnel)
	pair("labels", old.Labels, nw.Labels)
	if old.CACertPEM != nw.CACertPEM {
		out["ca_cert_pem"] = "changed"
	}
	secret := func(k string, a, b []byte) {
		switch {
		case len(a) == 0 && len(b) > 0:
			out[k] = "set"
		case len(a) > 0 && len(b) == 0:
			out[k] = "cleared"
		case string(a) != string(b):
			out[k] = "changed"
		}
	}
	secret("password", old.PasswordSealed, nw.PasswordSealed)
	secret("uri", old.URISealed, nw.URISealed)
	return out
}

func (s *Server) connsUsedBy(ctx context.Context, kind, id string) ([]string, error) {
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, j := range jobs {
		if (kind == "connection" && (j.ConnectionID == id || j.ScratchConnectionID == id)) || (kind == "policy" && j.PolicyID == id) {
			out = append(out, j.ID)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (s *Server) apiConnList(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	cs, err := s.db.st.ListBackupConnections(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup connections")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": cs})
}

func (s *Server) apiConnGet(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	c, ok, err := s.db.st.GetBackupConnection(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup connection")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "backup connection not found")
		return
	}
	used, _ := s.connsUsedBy(r.Context(), "connection", c.ID)
	writeJSON(w, http.StatusOK, map[string]any{"connection": c, "used_by": used})
}

func (s *Server) apiConnSave(update bool) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, sess *Session) {
		if !s.needDB(w) {
			return
		}
		var req connRequest
		if !decodeStrict(w, r, &req) {
			return
		}
		var cur *backup.Connection
		if update {
			req.ID = r.PathValue("id")
			c, ok, err := s.db.st.GetBackupConnection(r.Context(), req.ID)
			if err != nil {
				writeErr(w, http.StatusServiceUnavailable, "could not read backup connection")
				return
			}
			if !ok {
				writeErr(w, http.StatusNotFound, "backup connection not found")
				return
			}
			cur = &c
		} else if req.ID == "" {
			id, err := randomConnID()
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "could not create a connection id")
				return
			}
			req.ID = id
		}
		c, err := s.connFromRequest(req, cur)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if c.SSHTunnel != nil && (c.URISet || c.URIEnv != "" || (req.URI != nil && *req.URI != "")) {
			writeErr(w, http.StatusBadRequest, "ssh_tunnel cannot be used with a connection URI; use host/port/user")
			return
		}
		if err := c.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if c.SSHTunnel != nil {
			if strings.TrimSpace(c.SSHTunnel.KnownHostsRef) == "" {
				writeErr(w, http.StatusBadRequest, "ssh_tunnel needs known_hosts_ref")
				return
			}
			if err := sshtun.ValidateSecurity(c.SSHTunnel.IdentityRef, c.SSHTunnel.KnownHostsRef); err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		if update {
			// A change that makes the connection incompatible with a job's method is refused up front.
			if err := s.checkJobsAccept(r.Context(), c); err != nil {
				writeErr(w, http.StatusConflict, err.Error())
				return
			}
		}
		action := "backup_connection.create"
		if update {
			action = "backup_connection.update"
		}
		detail := map[string]any{"connection": c.ID, "changes": connChanges(cur, c)}
		if !s.auditBackupOp(w, r, action, detail) {
			return
		}
		if update {
			ok, err := s.db.st.UpdateBackupConnection(r.Context(), c)
			switch {
			case errors.Is(err, backup.ErrConflict):
				writeErr(w, http.StatusConflict, "a connection with this name already exists")
				return
			case errors.Is(err, backup.ErrInvalid):
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			case err != nil:
				writeErr(w, http.StatusInternalServerError, "could not update the backup connection")
				return
			case !ok:
				writeErr(w, http.StatusNotFound, "backup connection not found")
				return
			}
			refreshed, failed := s.refreshJobsUsing(r.Context(), "connection", c.ID)
			got, _, _ := s.db.st.GetBackupConnection(r.Context(), c.ID)
			used, _ := s.connsUsedBy(r.Context(), "connection", c.ID)
			writeJSON(w, http.StatusOK, map[string]any{"connection": got, "affected_jobs": used, "refreshed_jobs": refreshed, "refresh_failed": failed})
			return
		}
		c.CreatedBy = sess.Username
		got, err := s.db.st.CreateBackupConnection(r.Context(), c)
		switch {
		case errors.Is(err, backup.ErrConflict):
			writeErr(w, http.StatusConflict, "a connection with this id or name already exists")
		case errors.Is(err, backup.ErrInvalid):
			writeErr(w, http.StatusBadRequest, err.Error())
		case err != nil:
			writeErr(w, http.StatusInternalServerError, "could not create the backup connection")
		default:
			writeJSON(w, http.StatusCreated, map[string]any{"connection": got})
		}
	}
}

func (s *Server) apiConnDelete(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	id := r.PathValue("id")
	if used, err := s.connsUsedBy(r.Context(), "connection", id); err == nil && len(used) > 0 {
		writeJSON(w, http.StatusConflict, map[string]any{"error": "connection is used by backup jobs", "jobs": used})
		return
	}
	if !s.auditBackupOp(w, r, "backup_connection.delete", map[string]any{"connection": id}) {
		return
	}
	ok, err := s.db.st.DeleteBackupConnection(r.Context(), id)
	var inuse *backup.InUseError
	switch {
	case errors.As(err, &inuse):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "connection is used by backup jobs", "jobs": inuse.Jobs})
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "could not delete the backup connection")
	case !ok:
		writeErr(w, http.StatusNotFound, "backup connection not found")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// checkJobsAccept refuses a connection edit that no longer fits the jobs using it (engine change).
func (s *Server) checkJobsAccept(ctx context.Context, c backup.Connection) error {
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.ConnectionID == c.ID {
			if _, err := backup.ApplyConnection(j, c); err != nil {
				return errors.New("job " + j.ID + " uses this connection: " + err.Error())
			}
		}
		if j.ScratchConnectionID == c.ID && j.TestRestore {
			if _, err := backup.ApplyScratchConnection(j, c); err != nil {
				return errors.New("job " + j.ID + " uses this connection for test restores: " + err.Error())
			}
		}
	}
	return nil
}

// secretOf returns the plaintext password and URI of a connection for a connection test. Environment-mode
// connections read them from the Web process environment; stored ones are decrypted.
func (s *Server) connSecrets(c backup.Connection) (password, uri string, err error) {
	if c.SecretMode == backup.SecretStored {
		if len(c.PasswordSealed) > 0 {
			b, err := s.db.box.Open(c.PasswordSealed, c.ID)
			if err != nil {
				return "", "", errors.New("the stored password cannot be decrypted (wrong STRAND_WEB_SECRET_KEY?)")
			}
			password = string(b)
		}
		if len(c.URISealed) > 0 {
			b, err := s.db.box.Open(c.URISealed, c.ID)
			if err != nil {
				return "", "", errors.New("the stored URI cannot be decrypted (wrong STRAND_WEB_SECRET_KEY?)")
			}
			uri = string(b)
		}
		return password, uri, nil
	}
	if c.PasswordEnv != "" {
		password = os.Getenv(c.PasswordEnv)
	}
	if c.URIEnv != "" {
		uri = os.Getenv(c.URIEnv)
	}
	return password, uri, nil
}

func (s *Server) connChecker() conncheck.Checker {
	if s.checker != nil {
		return s.checker
	}
	return conncheck.Live{}
}

// apiConnTest uses saved credentials only with the saved connection definition.
// An unsaved probe uses credentials supplied in the request alone.
func (s *Server) apiConnTest(saved bool) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, sess *Session) {
		if !s.needDB(w) {
			return
		}
		var c backup.Connection
		var password, uri string
		if saved {
			cur, ok, err := s.db.st.GetBackupConnection(r.Context(), r.PathValue("id"))
			if err != nil {
				writeErr(w, http.StatusServiceUnavailable, "could not read backup connection")
				return
			}
			if !ok {
				writeErr(w, http.StatusNotFound, "backup connection not found")
				return
			}
			c = cur
			var err2 error
			if password, uri, err2 = s.connSecrets(c); err2 != nil {
				writeErr(w, http.StatusConflict, err2.Error())
				return
			}
		} else {
			var req connRequest
			if !decodeStrict(w, r, &req) {
				return
			}
			if req.ID == "" {
				req.ID = "conn-unsaved"
			}
			built, err := s.connFromRequest(req, nil)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			if req.Name == "" {
				built.Name = "unsaved"
			}
			if err := built.Validate(); err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			c = built
			if req.Password != nil {
				password = *req.Password
			}
			if req.URI != nil {
				uri = *req.URI
			}
		}
		var restoreEndpoint string
		if saved && sess != nil && RoleAllows(sess.Role, PermAdmin) && c.Engine == backup.ConnMongoDB {
			original := uri
			if original == "" {
				original, _ = runner.MongoURI(os.Getenv, backup.ConnSpec{Host: c.Host, Port: c.Port})
			}
			destination, err := runner.MongoRestoreDestination(original, "")
			if err == nil {
				restoreEndpoint = strings.TrimSuffix(destination, "/*")
			}
		}
		if !s.auditBackupOp(w, r, "backup_connection.test", map[string]any{"connection": c.ID, "saved": saved, "engine": c.Engine, "host": c.Host}) {
			return
		}
		if c.SSHTunnel != nil {
			if c.URISet || c.URIEnv != "" || uri != "" {
				res := conncheck.Result{
					OK:                false,
					Engine:            c.Engine,
					MissingPrivileges: []string{},
					Databases:         []string{},
					Checks: []conncheck.Check{
						{Name: "ssh_tunnel", Status: "fail", Detail: "ssh_tunnel cannot be used with a connection URI; use host/port/user"},
					},
					Error: "ssh_tunnel cannot be used with a connection URI; use host/port/user",
				}
				if saved {
					_, _ = s.db.st.SetBackupConnectionTest(r.Context(), c.ID, time.Now().UTC(), "failed", "ssh tunnel: ssh_tunnel cannot be used with a connection URI; use host/port/user")
				}
				writeJSON(w, http.StatusOK, res)
				return
			}
			addrHost, addrPort, closeTunnel, err := sshtun.OpenForConnection(r.Context(), c)
			if err != nil {
				res := conncheck.Result{
					OK:                false,
					Engine:            c.Engine,
					MissingPrivileges: []string{},
					Databases:         []string{},
					Checks: []conncheck.Check{
						{Name: "ssh_tunnel", Status: "fail", Detail: err.Error()},
					},
					Error: err.Error(),
				}
				if saved {
					_, _ = s.db.st.SetBackupConnectionTest(r.Context(), c.ID, time.Now().UTC(), "failed", "ssh tunnel: "+err.Error())
				}
				writeJSON(w, http.StatusOK, res)
				return
			}
			defer closeTunnel()
			origHost := c.Host
			c.Host = addrHost
			c.Port = addrPort
			res := s.connChecker().Check(r.Context(), conncheck.Target{
				Engine:        c.Engine,
				Host:          c.Host,
				Port:          c.Port,
				User:          c.User,
				Password:      password,
				URI:           uri,
				TLSMode:       c.TLSMode,
				CACertPEM:     c.CACertPEM,
				TLSServerName: origHost,
			})
			res.RestoreEndpoint = restoreEndpoint
			res.Checks = append([]conncheck.Check{
				{Name: "ssh_tunnel", Status: "ok", Detail: "connected via " + c.SSHTunnel.Host},
			}, res.Checks...)
			if saved {
				status, detail := "ok", "connected"
				if !res.OK {
					status, detail = "failed", res.Error
				} else if len(res.MissingPrivileges) > 0 {
					status, detail = "warning", "missing privileges: "+strings.Join(res.MissingPrivileges, ", ")
				}
				if len(detail) > 500 {
					detail = detail[:500]
				}
				_, _ = s.db.st.SetBackupConnectionTest(r.Context(), c.ID, time.Now().UTC(), status, detail)
			}
			writeJSON(w, http.StatusOK, res)
			return
		}
		res := s.connChecker().Check(r.Context(), conncheck.Target{Engine: c.Engine, Host: c.Host, Port: c.Port, User: c.User,
			Password: password, URI: uri, TLSMode: c.TLSMode, CACertPEM: c.CACertPEM})
		res.RestoreEndpoint = restoreEndpoint
		if saved {
			status, detail := "ok", "connected"
			if !res.OK {
				status, detail = "failed", res.Error
			} else if len(res.MissingPrivileges) > 0 {
				status, detail = "warning", "missing privileges: "+strings.Join(res.MissingPrivileges, ", ")
			}
			if len(detail) > 500 {
				detail = detail[:500]
			}
			_, _ = s.db.st.SetBackupConnectionTest(r.Context(), c.ID, time.Now().UTC(), status, detail)
		}
		writeJSON(w, http.StatusOK, res)
	}
}
