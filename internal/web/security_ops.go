package web

import (
	"bytes"
	"context"
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

	"strand/internal/spool"
	"strand/internal/tunnel"
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
	if cleanLHost != "127.0.0.1" && cleanLHost != "localhost" && cleanLHost != "::1" {
		return errors.New("listen: local listen address must be on loopback (127.0.0.1)")
	}

	if err := tunnel.ValidateSecurity(identityFile, knownHosts); err != nil {
		return err
	}
	return nil
}

func withinCanonicalRoot(path, root string) (string, error) {
	absRoot, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", err
	}
	rootInfo, err := os.Lstat(absRoot)
	if err != nil {
		return "", err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("allow-listed root must not be a symlink")
	}
	rootReal, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", err
	}
	rootReal, err = filepath.Abs(rootReal)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	// Resolve existing paths fully. For not-yet-created leaves, resolve the nearest existing ancestor,
	// then append the unresolved suffix so a symlinked parent cannot escape the allowlist.
	probe, suffix := absPath, []string{}
	for {
		if _, statErr := os.Lstat(probe); statErr == nil {
			break
		}
		parent := filepath.Dir(probe)
		if parent == probe {
			return "", errors.New("path has no existing parent")
		}
		suffix = append(suffix, filepath.Base(probe))
		probe = parent
	}
	probeReal, err := filepath.EvalSymlinks(probe)
	if err != nil {
		return "", err
	}
	for i := len(suffix) - 1; i >= 0; i-- {
		probeReal = filepath.Join(probeReal, suffix[i])
	}
	probeReal, err = filepath.Abs(filepath.Clean(probeReal))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootReal, probeReal)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path must resolve within %s", rootReal)
	}
	return probeReal, nil
}

func (s *Server) validateSpoolDir(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return errors.New("spool_dir: directory is required")
	}
	clean := filepath.Clean(dir)
	if s != nil && s.reg != nil {
		for _, p := range s.reg.List() {
			sp, err := s.spoolPath(p)
			if err == nil && clean == sp {
				return nil
			}
		}
	}
	return errors.New("spool_dir: path must exactly match a configured pipeline spool directory")
}

func (s *Server) spoolPipelineNames() []string {
	if s == nil || s.reg == nil {
		return nil
	}
	ps := s.reg.List()
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		if p != nil {
			out = append(out, p.Name)
		}
	}
	return out
}

func (s *Server) spoolPath(p *Pipeline) (string, error) {
	if p == nil {
		return "", errors.New("pipeline is required")
	}
	dir := ""
	if p.Cfg != nil {
		dir = p.Cfg.Resources.Spool.Path
	}
	if dir == "" {
		stateDir := "/tmp"
		if s != nil && s.cfg != nil && s.cfg.StateDir != "" {
			stateDir = s.cfg.StateDir
		}
		dir = filepath.Join(stateDir, "pipelines", p.Name, "spool")
	}
	abs, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("spool directory unavailable: %w", err)
	}
	return filepath.Clean(canonical), nil
}

func (s *Server) scanPipelineSpool(ctx context.Context, pipeline string) (*spool.InventoryReport, string, error) {
	if s == nil || s.reg == nil {
		return nil, "", errors.New("pipeline registry is unavailable")
	}
	if !ValidName(pipeline) {
		return nil, "", errors.New("pipeline is required")
	}
	p := s.reg.Get(pipeline)
	if p == nil {
		return nil, "", errors.New("unknown pipeline")
	}
	if s.isRunning(p) {
		return nil, "", errors.New("stop the pipeline before offline spool inspection")
	}
	dir, err := s.spoolPath(p)
	if err != nil {
		return nil, "", err
	}
	spill := filepath.Join(dir, "spill")
	if _, err := os.Stat(spill); err != nil {
		return nil, dir, fmt.Errorf("spool spill directory unavailable: %w", err)
	}
	report, err := spool.ScanInventoryOffline(dir, []string{spill})
	return report, dir, err
}

func (s *Server) auditSensitive(r *http.Request, action string, detail map[string]any, result string) error {
	if s == nil || s.audit == nil {
		return errors.New("audit log unavailable")
	}
	if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: action, Detail: detail, Result: result}); err != nil {
		return fmt.Errorf("audit %s: %w", action, err)
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
		dir = allowedDir
	}
	if !filepath.IsAbs(dir) {
		return "", errors.New("export directory must be an absolute path")
	}
	if err := os.MkdirAll(allowedDir, 0o700); err != nil {
		return "", fmt.Errorf("create export root: %w", err)
	}
	clean, err := withinCanonicalRoot(dir, allowedDir)
	if err != nil {
		return "", fmt.Errorf("export directory must resolve within %s: %w", allowedDir, err)
	}
	if st, err := os.Stat(clean); err == nil && !st.IsDir() {
		return "", errors.New("export path exists and is not a directory")
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
		dir = allowedDir
	}
	if !filepath.IsAbs(dir) {
		return "", errors.New("archive directory must be an absolute path")
	}
	if err := os.MkdirAll(allowedDir, 0o700); err != nil {
		return "", fmt.Errorf("create archive root: %w", err)
	}
	clean, err := withinCanonicalRoot(dir, allowedDir)
	if err != nil {
		return "", fmt.Errorf("archive directory must resolve within %s: %w", allowedDir, err)
	}
	if st, err := os.Stat(clean); err == nil && !st.IsDir() {
		return "", errors.New("archive path exists and is not a directory")
	}
	return clean, nil
}

// ---- Tunnel HTTP Handlers ----

func (s *Server) startTunnel(ctx context.Context, r *http.Request, sshAddr, user, remote, listen, identity, knownHosts string, profile string) (tunnelItem, error) {
	var privateKey []byte
	if profile != "" {
		saved, err := s.savedTunnel(ctx, profile)
		if err != nil {
			return tunnelItem{}, err
		}
		sshAddr, user, identity, knownHosts = saved.SSHAddress, saved.SSHUser, saved.IdentityFile, ""
		privateKey = saved.PrivateKey
		defer clear(privateKey)
	}
	if strings.TrimSpace(user) == "" {
		return tunnelItem{}, errors.New("ssh user is required")
	}
	if strings.TrimSpace(listen) == "" {
		listen = "127.0.0.1:0"
	}
	useServerTrust := strings.TrimSpace(knownHosts) == ""
	if useServerTrust {
		var err error
		knownHosts, err = tunnel.ConfiguredKnownHosts()
		if err != nil {
			return tunnelItem{}, fmt.Errorf("find server SSH host keys: %w", err)
		}
	}
	if err := validateTunnelParams(sshAddr, remote, listen, identity, knownHosts); err != nil {
		if useServerTrust && strings.HasPrefix(err.Error(), "known_hosts:") {
			return tunnelItem{}, fmt.Errorf("configure the Web server's SSH trust file with STRAND_WEB_SSH_KNOWN_HOSTS or ~/.ssh/known_hosts: %w", err)
		}
		return tunnelItem{}, err
	}
	parent := s.tunnelCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	if err := s.auditSensitive(r, "tunnel.start", map[string]any{"ssh": sshAddr, "remote": remote, "listen": listen, "user": user}, "started"); err != nil {
		cancel()
		return tunnelItem{}, err
	}
	fwd, err := tunnel.Start(ctx, tunnel.Config{SSHAddress: sshAddr, SSHUser: user, Remote: remote, LocalAddress: listen, IdentityFile: identity, HostKeysFile: knownHosts, PrivateKey: privateKey})
	if err != nil {
		cancel()
		_ = s.auditSensitive(r, "tunnel.start", map[string]any{"ssh": sshAddr, "remote": remote, "error": err.Error()}, "error")
		return tunnelItem{}, err
	}
	id := fmt.Sprintf("tun-%d", s.now().UnixNano())
	item := tunnelItem{ID: id, LocalAddress: fwd.Address(), Remote: remote, SSHAddress: sshAddr, SSHUser: user}
	if err := s.auditSensitive(r, "tunnel.start", map[string]any{"id": id, "ssh": sshAddr, "remote": remote, "listen": item.LocalAddress, "user": user}, "ok"); err != nil {
		cancel()
		_ = fwd.Close()
		return tunnelItem{}, fmt.Errorf("audit tunnel start: %w", err)
	}
	s.tunnelMu.Lock()
	if s.tunnels == nil {
		s.tunnels = make(map[string]activeTunnel)
	}
	s.tunnels[id] = activeTunnel{Item: item, Forward: fwd, Cancel: cancel}
	s.tunnelMu.Unlock()
	return item, nil
}

func (s *Server) stopTunnel(r *http.Request, id string) error {
	s.tunnelMu.Lock()
	active, ok := s.tunnels[id]
	if ok {
		delete(s.tunnels, id)
	}
	s.tunnelMu.Unlock()
	if !ok {
		return os.ErrNotExist
	}
	active.Cancel()
	err := active.Forward.Close()
	result := "ok"
	detail := map[string]any{"id": id}
	if err != nil {
		result = "error"
		detail["error"] = err.Error()
	}
	if auditErr := s.auditSensitive(r, "tunnel.stop", detail, result); auditErr != nil {
		return errors.Join(err, fmt.Errorf("audit tunnel stop: %w", auditErr))
	}
	return err
}

func (s *Server) closeTunnels() {
	s.tunnelMu.Lock()
	active := s.tunnels
	s.tunnels = make(map[string]activeTunnel)
	s.tunnelMu.Unlock()
	for _, t := range active {
		t.Cancel()
		_ = t.Forward.Close()
	}
}

func (s *Server) apiTunnelOpen(w http.ResponseWriter, r *http.Request, sess *Session) {
	var req struct {
		SSH          string `json:"ssh"`
		User         string `json:"user"`
		Remote       string `json:"remote"`
		Listen       string `json:"listen"`
		IdentityFile string `json:"identity_file"`
		KnownHosts   string `json:"known_hosts"`
		Profile      string `json:"profile"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	item, err := s.startTunnel(r.Context(), r, req.SSH, req.User, req.Remote, req.Listen, req.IdentityFile, req.KnownHosts, req.Profile)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
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

	if err := s.stopTunnel(r, id); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		writeErr(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stopped": true})
}

func (s *Server) pageTunnelOpen(w http.ResponseWriter, r *http.Request, sess *Session) {
	r.Body = http.MaxBytesReader(w, r.Body, 128<<10)
	if err := r.ParseForm(); err != nil {
		s.renderError(w, r, sess, http.StatusBadRequest, "invalid SSH tunnel form")
		return
	}
	if r.FormValue("intent") == "save-profile" {
		form := sshProfileFormFromRequest(r)
		if err := s.saveSSHProfile(r, form); err != nil {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusBadRequest)
			s.renderTunnel(w, sess, tunnelView{Profiles: s.sshProfileViews(r.Context()), Form: form, Error: err.Error()})
			return
		}
		http.Redirect(w, r, "/tunnels?saved=1", http.StatusSeeOther)
		return
	}
	ssh := r.FormValue("ssh")
	user := r.FormValue("user")
	remote := r.FormValue("remote")
	listen := r.FormValue("listen")
	identity := r.FormValue("identity_file")

	if _, err := s.startTunnel(r.Context(), r, ssh, user, remote, listen, identity, "", r.FormValue("profile")); err != nil {
		s.renderError(w, r, sess, http.StatusBadRequest, err.Error())
		return
	}
	http.Redirect(w, r, "/tunnels", http.StatusSeeOther)
}

func (s *Server) pageTunnelStop(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	id := r.FormValue("id")
	if id != "" {
		if err := s.stopTunnel(r, id); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, os.ErrNotExist) {
				status = http.StatusNotFound
			}
			s.renderError(w, r, sess, status, err.Error())
			return
		}
	}
	http.Redirect(w, r, "/tunnels", http.StatusSeeOther)
}

// ---- Spool Inspect Handlers ----

func (s *Server) apiSpoolInspect(w http.ResponseWriter, r *http.Request, sess *Session) {
	var req struct {
		Pipeline string `json:"pipeline"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.auditSensitive(r, "spool.inspect", map[string]any{"pipeline": req.Pipeline}, "started"); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	rep, dir, err := s.scanPipelineSpool(r.Context(), req.Pipeline)
	if err != nil {
		_ = s.auditSensitive(r, "spool.inspect", map[string]any{"pipeline": req.Pipeline, "error": err.Error()}, "error")
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.auditSensitive(r, "spool.inspect", map[string]any{"pipeline": req.Pipeline, "discovered_files": rep.DiscoveredFiles, "new_files": rep.NewFiles}, "ok"); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = dir
	writeJSON(w, http.StatusOK, map[string]any{"report": rep})
}

// ---- DB Admin Handlers ----

func (s *Server) pageDBMigrate(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	dryValue := r.FormValue("dry_run")
	if dryValue != "true" && dryValue != "false" {
		s.renderError(w, r, sess, http.StatusBadRequest, "dry_run must be true or false")
		return
	}
	dryRun := dryValue == "true"
	if s.cfg.SourcePath == "" {
		s.renderError(w, r, sess, http.StatusBadRequest, "web config file path is not available; migration requires the legacy file-mode web config")
		return
	}
	detail := map[string]any{"dry_run": dryRun}
	if err := s.auditSensitive(r, "db.migrate", detail, "started"); err != nil {
		s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
		return
	}
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	rep, err := MigrateFilesFromStore(ctx, s.db.st, MigrateOptions{ConfigPath: s.cfg.SourcePath, SecretKey: os.Getenv(EnvSecretKey), DryRun: dryRun, Out: &out, Log: s.log})
	if err != nil {
		_ = s.auditSensitive(r, "db.migrate", map[string]any{"dry_run": dryRun, "error": err.Error()}, "error")
		s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.auditSensitive(r, "db.migrate", map[string]any{"dry_run": dryRun, "verified": rep.Verified}, "ok"); err != nil {
		s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
		return
	}
	view := &dbMigrateReport{Verified: rep.Verified, Problems: rep.Problems}
	for _, item := range rep.Items {
		view.Summary.Total++
		switch item.Status {
		case "new":
			view.Summary.New++
		case "present":
			view.Summary.Present++
		case "conflict":
			view.Summary.Conflict++
		}
	}
	s.render(w, "db_admin", sess, pageData{
		Title:  "Database Admin",
		Active: "db-admin",
		Body: &dbAdminView{
			DryRun:        dryRun,
			MigrateReport: view,
			MigrateOutput: out.String(),
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

	if err := s.auditSensitive(r, "db.export", map[string]any{"dir": cleanDir}, "started"); err != nil {
		s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
		return
	}
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	err = ExportDBFromStore(ctx, s.db.st, ExportOptions{Dir: cleanDir, ConfigPath: s.cfg.SourcePath, SecretKey: os.Getenv(EnvSecretKey), Out: &out, Log: s.log})
	if err != nil {
		_ = s.auditSensitive(r, "db.export", map[string]any{"dir": cleanDir, "error": err.Error()}, "error")
		s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.auditSensitive(r, "db.export", map[string]any{"dir": cleanDir}, "ok"); err != nil {
		s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
		return
	}
	s.render(w, "db_admin", sess, pageData{Title: "Database Admin", Active: "db-admin", Body: &dbAdminView{ExportOutput: out.String()}})
}

// ---- Archive Handlers ----

func (s *Server) archivePipelineNames() []string {
	if s.reg == nil {
		return nil
	}
	var names []string
	for _, p := range s.reg.List() {
		if p.Cfg != nil && strings.EqualFold(p.Cfg.Source.Kind, "mysql") {
			names = append(names, p.Name)
		}
	}
	return names
}

func (s *Server) activeArchiveJob() *JobStatus {
	if s.jobs == nil {
		return nil
	}
	s.archiverMu.Lock()
	id := s.archiverJobID
	s.archiverMu.Unlock()
	return s.archiveJobByID(id)
}

func (s *Server) latestArchiveJob() *JobStatus {
	if s.jobs == nil {
		return nil
	}
	jobs := s.jobs.List("")
	var latest *JobStatus
	for i := range jobs {
		j := &jobs[i]
		if j.Kind == "archive" && (latest == nil || j.Started.After(latest.Started)) {
			latest = j
		}
	}
	return latest
}

func (s *Server) archiveJobByID(id string) *JobStatus {
	if s.jobs == nil {
		return nil
	}
	if id != "" {
		if j, ok := s.jobs.Get(id); ok && j.Kind == "archive" && j.Running {
			return &j
		}
	}
	for _, j := range s.jobs.List("") {
		if j.Kind == "archive" && j.Running {
			return &j
		}
	}
	return nil
}

func (s *Server) archiveOutputDir(j *JobStatus) string {
	for i := 0; i+1 < len(j.Args); i++ {
		if j.Args[i] == "--to" {
			return j.Args[i+1]
		}
	}
	return ""
}

func (s *Server) startArchive(r *http.Request, pipeline, dir string) (JobStatus, error) {
	if s.jobs == nil {
		return JobStatus{}, errors.New("job runner unavailable")
	}
	p := s.reg.Get(pipeline)
	if p == nil || p.Cfg == nil || !strings.EqualFold(p.Cfg.Source.Kind, "mysql") {
		return JobStatus{}, errors.New("select a pipeline with a MySQL source; MariaDB binlog archiving is not supported")
	}
	if p.ConfigPath == "" {
		return JobStatus{}, errors.New("pipeline config is unavailable")
	}
	cleanDir, err := s.validateArchiveDir(dir)
	if err != nil {
		return JobStatus{}, err
	}
	s.archiverMu.Lock()
	defer s.archiverMu.Unlock()
	if current := s.archiveJobByID(s.archiverJobID); current != nil {
		return JobStatus{}, fmt.Errorf("archive job %s is already running for pipeline %s", current.ID, current.Pipeline)
	}
	for _, current := range s.jobs.List("") {
		if current.Kind == "archive" && current.Running {
			return JobStatus{}, fmt.Errorf("archive job %s is already running for pipeline %s", current.ID, current.Pipeline)
		}
	}
	if err := s.auditSensitive(r, "archive.start", map[string]any{"pipeline": pipeline, "dir": cleanDir}, "started"); err != nil {
		return JobStatus{}, err
	}
	s.materialize(p.Name)
	env, err := p.Env()
	if err != nil {
		_ = s.auditSensitive(r, "archive.start", map[string]any{"pipeline": pipeline, "error": err.Error()}, "error")
		return JobStatus{}, fmt.Errorf("pipeline env: %w", err)
	}
	args := []string{"archive", "binlog", "--config", p.ConfigPath, "--to", cleanDir}
	id, err := s.jobs.Start(r.Context(), JobSpec{Pipeline: p.Name, Kind: "archive", Args: args, Env: env})
	if err != nil {
		_ = s.auditSensitive(r, "archive.start", map[string]any{"pipeline": pipeline, "error": err.Error()}, "error")
		return JobStatus{}, err
	}
	s.archiverRunning, s.archiverDir, s.archiverJobID = true, cleanDir, id
	if err := s.auditSensitive(r, "archive.start", map[string]any{"pipeline": pipeline, "dir": cleanDir, "job_id": id}, "ok"); err != nil {
		_ = s.jobs.Cancel(id)
		return JobStatus{}, err
	}
	j, _ := s.jobs.Get(id)
	return j, nil
}

func (s *Server) stopArchive(r *http.Request, id string) error {
	if s.jobs == nil {
		return errors.New("job runner unavailable")
	}
	if id == "" {
		if j := s.activeArchiveJob(); j != nil {
			id = j.ID
		}
	}
	if id == "" {
		return os.ErrNotExist
	}
	j, ok := s.jobs.Get(id)
	if !ok || j.Kind != "archive" {
		return os.ErrNotExist
	}
	if err := s.auditSensitive(r, "archive.stop", map[string]any{"pipeline": j.Pipeline, "job_id": id}, "started"); err != nil {
		return err
	}
	if j.Running {
		if err := s.jobs.Cancel(id); err != nil {
			_ = s.auditSensitive(r, "archive.stop", map[string]any{"job_id": id, "error": err.Error()}, "error")
			return err
		}
	}
	s.archiverMu.Lock()
	if s.archiverJobID == id {
		s.archiverRunning = false
		s.archiverJobID = ""
	}
	s.archiverMu.Unlock()
	return s.auditSensitive(r, "archive.stop", map[string]any{"pipeline": j.Pipeline, "job_id": id}, "ok")
}

func (s *Server) apiArchiveStart(w http.ResponseWriter, r *http.Request, _ *Session) {
	var req struct {
		Pipeline string `json:"pipeline"`
		To       string `json:"to"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	j, err := s.startArchive(r, req.Pipeline, req.To)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"running": true, "job_id": j.ID, "pipeline": j.Pipeline})
}

func (s *Server) apiArchiveStop(w http.ResponseWriter, r *http.Request, _ *Session) {
	var req struct {
		JobID string `json:"job_id"`
	}
	if r.Body != nil {
		_ = readJSON(r, &req)
	}
	if err := s.stopArchive(r, req.JobID); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		writeErr(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"running": false})
}

func (s *Server) pageArchiveStart(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	if _, err := s.startArchive(r, r.FormValue("pipeline"), r.FormValue("to")); err != nil {
		s.renderError(w, r, sess, http.StatusBadRequest, err.Error())
		return
	}
	http.Redirect(w, r, "/archive", http.StatusSeeOther)
}

func (s *Server) pageArchiveStop(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	if err := s.stopArchive(r, r.FormValue("job_id")); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		s.renderError(w, r, sess, status, err.Error())
		return
	}
	http.Redirect(w, r, "/archive", http.StatusSeeOther)
}
