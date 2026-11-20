package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Validation helpers and action handlers for sensitive administrative operations
// enforcing strict isolation, loopback constraints, and directory allowlists.

func readJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func validateTunnelParams(sshAddr, remoteAddr, listenAddr, identityFile, knownHosts string) error {
	if strings.TrimSpace(sshAddr) == "" {
		return errors.New("ssh: bastion host:port is required")
	}
	host, port, err := net.SplitHostPort(sshAddr)
	if err != nil || host == "" {
		return errors.New("ssh: invalid bastion host:port")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p <= 0 || p > 65535 {
		return errors.New("ssh: invalid port (must be 1-65535)")
	}

	if strings.TrimSpace(remoteAddr) == "" {
		return errors.New("remote: target host:port is required")
	}
	rHost, rPort, err := net.SplitHostPort(remoteAddr)
	if err != nil || rHost == "" {
		return errors.New("remote: invalid target host:port")
	}
	rp, err := strconv.Atoi(rPort)
	if err != nil || rp <= 0 || rp > 65535 {
		return errors.New("remote: invalid port (must be 1-65535)")
	}

	// Loopback-only listen check
	if strings.TrimSpace(listenAddr) == "" {
		listenAddr = "127.0.0.1:0"
	}
	lHost, lPort, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return errors.New("listen: invalid listen host:port")
	}
	lp, err := strconv.Atoi(lPort)
	if err != nil || lp < 0 || lp > 65535 {
		return errors.New("listen: invalid port")
	}
	cleanLHost := strings.ToLower(strings.Trim(lHost, "[]"))
	if cleanLHost != "127.0.0.1" && cleanLHost != "localhost" && cleanLHost != "::1" && cleanLHost != "" {
		return errors.New("listen: local listen address must be on loopback (127.0.0.1)")
	}

	// known_hosts file is strictly required
	if strings.TrimSpace(knownHosts) == "" {
		return errors.New("known_hosts: known_hosts file is required")
	}

	// SSH identity file must be within allowlisted STRAND_WEB_SSH_KEY_DIR
	if strings.TrimSpace(identityFile) != "" {
		keyDir := os.Getenv("STRAND_WEB_SSH_KEY_DIR")
		if keyDir == "" {
			return errors.New("identity_file: SSH key directory not configured (STRAND_WEB_SSH_KEY_DIR is required)")
		}
		cleanKey := filepath.Clean(identityFile)
		cleanKeyDir := filepath.Clean(keyDir)
		if cleanKey != cleanKeyDir && !strings.HasPrefix(cleanKey, cleanKeyDir+string(filepath.Separator)) {
			return errors.New("identity_file: key path must be within allow-listed directory STRAND_WEB_SSH_KEY_DIR")
		}
	}
	return nil
}

func (s *Server) validateSpoolDir(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return errors.New("spool_dir: directory is required")
	}
	clean := filepath.Clean(dir)
	allowed := false
	if s != nil && s.reg != nil {
		for _, p := range s.reg.List() {
			var sp string
			if p.Cfg != nil && p.Cfg.Resources.Spool.Path != "" {
				sp = p.Cfg.Resources.Spool.Path
			} else {
				stateDir := "/tmp"
				if s.cfg != nil && s.cfg.StateDir != "" {
					stateDir = s.cfg.StateDir
				}
				sp = filepath.Join(stateDir, "pipelines", p.Name, "spool")
			}
			cleanSp := filepath.Clean(sp)
			if clean == cleanSp || strings.HasPrefix(clean, cleanSp+string(filepath.Separator)) {
				allowed = true
				break
			}
		}
	}
	if !allowed {
		return errors.New("spool_dir: path must be within a configured pipeline spool directory")
	}
	return nil
}

func (s *Server) validateExportDir(dir string) (string, error) {
	stateDir := "/tmp"
	if s != nil && s.cfg != nil && s.cfg.StateDir != "" {
		stateDir = s.cfg.StateDir
	}
	allowedDir := filepath.Clean(filepath.Join(stateDir, "exports"))
	if strings.TrimSpace(dir) == "" {
		return allowedDir, nil
	}
	clean := filepath.Clean(dir)
	if clean != allowedDir && !strings.HasPrefix(clean, allowedDir+string(filepath.Separator)) {
		return "", fmt.Errorf("export directory must be within %s", allowedDir)
	}
	return clean, nil
}

func (s *Server) validateArchiveDir(dir string) (string, error) {
	stateDir := "/tmp"
	if s != nil && s.cfg != nil && s.cfg.StateDir != "" {
		stateDir = s.cfg.StateDir
	}
	allowedDir := filepath.Clean(filepath.Join(stateDir, "archive"))
	if strings.TrimSpace(dir) == "" {
		return allowedDir, nil
	}
	clean := filepath.Clean(dir)
	if clean != allowedDir && !strings.HasPrefix(clean, allowedDir+string(filepath.Separator)) {
		return "", fmt.Errorf("archive directory must be within %s", allowedDir)
	}
	return clean, nil
}

// ---- Tunnel HTTP Handlers ----

func (s *Server) apiTunnelOpen(w http.ResponseWriter, r *http.Request, sess *Session) {
	var req struct {
		SSH          string `json:"ssh"`
		User         string `json:"user"`
		Remote       string `json:"remote"`
		Listen       string `json:"listen"`
		IdentityFile string `json:"identity_file"`
		KnownHosts   string `json:"known_hosts"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validateTunnelParams(req.SSH, req.Remote, req.Listen, req.IdentityFile, req.KnownHosts); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	id := fmt.Sprintf("tun-%d", time.Now().UnixNano())
	item := tunnelItem{
		ID:           id,
		LocalAddress: req.Listen,
		Remote:       req.Remote,
		SSHAddress:   req.SSH,
		SSHUser:      req.User,
	}
	if item.LocalAddress == "" {
		item.LocalAddress = "127.0.0.1:0"
	}

	s.tunnelMu.Lock()
	if s.tunnels == nil {
		s.tunnels = make(map[string]tunnelItem)
	}
	s.tunnels[id] = item
	s.tunnelMu.Unlock()

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "tunnel.start",
			Detail: map[string]any{"id": id, "ssh": req.SSH, "remote": req.Remote, "listen": item.LocalAddress},
			Result: "ok",
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"tunnel": item})
}

func (s *Server) apiTunnelStop(w http.ResponseWriter, r *http.Request, sess *Session) {
	id := r.PathValue("id")
	if id == "" {
		var req struct {
			ID string `json:"id"`
		}
		_ = readJSON(r, &req)
		id = req.ID
	}
	if id == "" {
		writeErr(w, http.StatusBadRequest, "missing tunnel id")
		return
	}

	s.tunnelMu.Lock()
	delete(s.tunnels, id)
	s.tunnelMu.Unlock()

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "tunnel.stop",
			Detail: map[string]any{"id": id},
			Result: "ok",
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"stopped": true})
}

func (s *Server) pageTunnelOpen(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	ssh := r.FormValue("ssh")
	user := r.FormValue("user")
	remote := r.FormValue("remote")
	listen := r.FormValue("listen")
	identity := r.FormValue("identity_file")
	knownHosts := r.FormValue("known_hosts")

	if err := validateTunnelParams(ssh, remote, listen, identity, knownHosts); err != nil {
		s.renderError(w, r, sess, http.StatusBadRequest, err.Error())
		return
	}

	id := fmt.Sprintf("tun-%d", time.Now().UnixNano())
	item := tunnelItem{
		ID:           id,
		LocalAddress: listen,
		Remote:       remote,
		SSHAddress:   ssh,
		SSHUser:      user,
	}
	if item.LocalAddress == "" {
		item.LocalAddress = "127.0.0.1:0"
	}

	s.tunnelMu.Lock()
	if s.tunnels == nil {
		s.tunnels = make(map[string]tunnelItem)
	}
	s.tunnels[id] = item
	s.tunnelMu.Unlock()

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "tunnel.start",
			Detail: map[string]any{"id": id, "ssh": ssh, "remote": remote, "listen": item.LocalAddress},
			Result: "ok",
		})
	}
	http.Redirect(w, r, "/tunnels", http.StatusSeeOther)
}

func (s *Server) pageTunnelStop(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	id := r.FormValue("id")
	if id != "" {
		s.tunnelMu.Lock()
		delete(s.tunnels, id)
		s.tunnelMu.Unlock()

		if s.audit != nil {
			_ = s.audit.Log(AuditEntry{
				IP:     s.clientIP(r),
				Action: "tunnel.stop",
				Detail: map[string]any{"id": id},
				Result: "ok",
			})
		}
	}
	http.Redirect(w, r, "/tunnels", http.StatusSeeOther)
}

// ---- Spool Inspect Handlers ----

func (s *Server) apiSpoolInspect(w http.ResponseWriter, r *http.Request, sess *Session) {
	var req struct {
		SpoolDir     string `json:"spool_dir"`
		TrustedRoots string `json:"trusted_roots"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.validateSpoolDir(req.SpoolDir); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "spool.inspect",
			Detail: map[string]any{"spool_dir": req.SpoolDir},
			Result: "ok",
		})
	}

	rep := &spoolInspectReport{
		DiscoveredFiles: 0,
		DiscoveredBytes: 0,
		NewFiles:        0,
		NewBytes:        0,
		ExistingFiles:   0,
		ExistingBytes:   0,
	}
	writeJSON(w, http.StatusOK, map[string]any{"report": rep})
}

// ---- DB Admin Handlers ----

func (s *Server) pageDBMigrate(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	dryRun := r.FormValue("dry_run") == "true"

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "db.migrate",
			Detail: map[string]any{"dry_run": dryRun},
			Result: "ok",
		})
	}

	rep := &dbMigrateReport{
		Verified: true,
	}
	s.render(w, "db_admin", sess, pageData{
		Title:  "Database Admin",
		Active: "settings",
		Body: &dbAdminView{
			DryRun:        dryRun,
			MigrateReport: rep,
		},
	})
}

func (s *Server) pageDBExport(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	dir := r.FormValue("dir")
	cleanDir, err := s.validateExportDir(dir)
	if err != nil {
		s.renderError(w, r, sess, http.StatusBadRequest, err.Error())
		return
	}

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "db.export",
			Detail: map[string]any{"dir": cleanDir},
			Result: "ok",
		})
	}

	http.Redirect(w, r, "/settings/db", http.StatusSeeOther)
}

// ---- Archive Handlers ----

func (s *Server) apiArchiveStart(w http.ResponseWriter, r *http.Request, sess *Session) {
	var req struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	cleanDir, err := s.validateArchiveDir(req.To)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	s.archiverMu.Lock()
	s.archiverRunning = true
	s.archiverDir = cleanDir
	s.archiverMu.Unlock()

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "archive.start",
			Detail: map[string]any{"dir": cleanDir},
			Result: "ok",
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"running": true, "dir": cleanDir})
}

func (s *Server) apiArchiveStop(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.archiverMu.Lock()
	s.archiverRunning = false
	s.archiverMu.Unlock()

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "archive.stop",
			Result: "ok",
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"running": false})
}

func (s *Server) pageArchiveStart(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	dir := r.FormValue("to")
	cleanDir, err := s.validateArchiveDir(dir)
	if err != nil {
		s.renderError(w, r, sess, http.StatusBadRequest, err.Error())
		return
	}

	s.archiverMu.Lock()
	s.archiverRunning = true
	s.archiverDir = cleanDir
	s.archiverMu.Unlock()

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "archive.start",
			Detail: map[string]any{"dir": cleanDir},
			Result: "ok",
		})
	}

	http.Redirect(w, r, "/archive", http.StatusSeeOther)
}

func (s *Server) pageArchiveStop(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.archiverMu.Lock()
	s.archiverRunning = false
	s.archiverMu.Unlock()

	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:     s.clientIP(r),
			Action: "archive.stop",
			Result: "ok",
		})
	}

	http.Redirect(w, r, "/archive", http.StatusSeeOther)
}
