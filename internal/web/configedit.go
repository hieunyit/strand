package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Sửa cấu hình (M5): xem/sửa file YAML và .env qua Web UI/API.
// - Lưu YAML: ghi file tạm, chạy strand validate -c <tmp> (với env), hiện diff, sao lưu <file>.bak.<ts>, ghi đè nguyên tử.
// - Sửa .env: riêng biệt, giá trị bí mật chỉ ghi (write-only, hiện '********'), mode 0600.

func isSecretKey(k string) bool {
	lower := strings.ToLower(k)
	if secretKeyRE.MatchString(lower) {
		return true
	}
	if strings.Contains(lower, "password") || strings.Contains(lower, "passwd") ||
		strings.Contains(lower, "secret") || strings.Contains(lower, "token") ||
		strings.Contains(lower, "cred") || strings.Contains(lower, "private") ||
		strings.HasSuffix(lower, "_pw") || strings.HasPrefix(lower, "pw_") || lower == "pw" {
		return true
	}
	return false
}

// unifiedDiff tính diff thống nhất giữa hai chuỗi văn bản.
func unifiedDiff(oldName, newName, oldText, newText string) string {
	if oldText == newText {
		return ""
	}
	oldLines := splitLines(oldText)
	newLines := splitLines(newText)

	n, m := len(oldLines), len(newLines)
	// Bound LCS memory and CPU, including configurations read from disk.
	if n > 0 && m > 1_000_000/n {
		return fmt.Sprintf("--- %s\n+++ %s\n(diff omitted: configuration too large)\n", oldName, newName)
	}
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < m; j++ {
			if oldLines[i] == newLines[j] {
				dp[i+1][j+1] = dp[i][j] + 1
			} else if dp[i+1][j] >= dp[i][j+1] {
				dp[i+1][j+1] = dp[i+1][j]
			} else {
				dp[i+1][j+1] = dp[i][j+1]
			}
		}
	}

	type edit struct {
		op   rune
		text string
	}
	var edits []edit
	i, j := n, m
	for i > 0 || j > 0 {
		if i > 0 && j > 0 && oldLines[i-1] == newLines[j-1] {
			edits = append(edits, edit{op: ' ', text: oldLines[i-1]})
			i--
			j--
		} else if j > 0 && (i == 0 || dp[i][j-1] >= dp[i-1][j]) {
			edits = append(edits, edit{op: '+', text: newLines[j-1]})
			j--
		} else if i > 0 && (j == 0 || dp[i][j-1] < dp[i-1][j]) {
			edits = append(edits, edit{op: '-', text: oldLines[i-1]})
			i--
		}
	}
	for l, r := 0, len(edits)-1; l < r; l, r = l+1, r-1 {
		edits[l], edits[r] = edits[r], edits[l]
	}

	const ctxLen = 3
	var changed []int
	for idx, e := range edits {
		if e.op != ' ' {
			changed = append(changed, idx)
		}
	}
	if len(changed) == 0 {
		return ""
	}

	type hunkRange struct{ start, end int }
	var ranges []hunkRange
	st0 := changed[0] - ctxLen
	if st0 < 0 {
		st0 = 0
	}
	en0 := changed[0] + ctxLen + 1
	if en0 > len(edits) {
		en0 = len(edits)
	}
	cur := hunkRange{start: st0, end: en0}
	for k := 1; k < len(changed); k++ {
		ch := changed[k]
		st := ch - ctxLen
		if st < 0 {
			st = 0
		}
		en := ch + ctxLen + 1
		if en > len(edits) {
			en = len(edits)
		}
		if st <= cur.end {
			cur.end = en
		} else {
			ranges = append(ranges, cur)
			cur = hunkRange{start: st, end: en}
		}
	}
	ranges = append(ranges, cur)

	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("--- %s\n", oldName))
	buf.WriteString(fmt.Sprintf("+++ %s\n", newName))

	for _, hr := range ranges {
		oldStart, newStart := 1, 1
		for k := 0; k < hr.start; k++ {
			if edits[k].op == ' ' || edits[k].op == '-' {
				oldStart++
			}
			if edits[k].op == ' ' || edits[k].op == '+' {
				newStart++
			}
		}
		oldCount, newCount := 0, 0
		for k := hr.start; k < hr.end; k++ {
			if edits[k].op == ' ' || edits[k].op == '-' {
				oldCount++
			}
			if edits[k].op == ' ' || edits[k].op == '+' {
				newCount++
			}
		}
		buf.WriteString(fmt.Sprintf("@@ -%d,%d +%d,%d @@\n", oldStart, oldCount, newStart, newCount))
		for k := hr.start; k < hr.end; k++ {
			buf.WriteRune(edits[k].op)
			buf.WriteString(edits[k].text)
			buf.WriteByte('\n')
		}
	}
	return buf.String()
}

func (s *Server) getOriginalConfig(p *Pipeline) []byte {
	if p == nil {
		return nil
	}
	// Bản trong DB là nguồn sự thật; file trên đĩa chỉ là bản vật chất hóa (dự phòng khi DB chưa có bản).
	ctx, cancel := dbCtx()
	defer cancel()
	row, ok, err := s.db.st.GetPipeline(ctx, p.Name)
	if err == nil && ok && row.HasConfig && len(row.ConfigYAML) > 0 {
		return []byte(row.ConfigYAML)
	}
	if p.ConfigPath != "" {
		if b, err := os.ReadFile(p.ConfigPath); err == nil && len(b) > 0 {
			return b
		}
	}
	return nil
}

// unmaskConfigYAML khôi phục giá trị bí mật từ file gốc nếu người dùng gửi lại chuỗi '********'
// (theo dòng, giữ nguyên định dạng). Lỗi nếu không khôi phục được: không bao giờ ghi '********' như mật khẩu thật.
func unmaskConfigYAML(newYAML []byte, origBytes []byte) ([]byte, error) {
	out, err := UnmaskConfigText(string(newYAML), origBytes)
	if err != nil {
		return nil, BadRequest("%s", err.Error())
	}
	return []byte(out), nil
}

// ConfigValidateResult trả kết quả kiểm tra cú pháp và diff.
type ConfigValidateResult struct {
	Valid    bool   `json:"valid"`
	ExitCode int    `json:"exit_code"`
	Output   string `json:"output"`
	Diff     string `json:"diff"`
}

// ConfigSaveResult trả kết quả sau khi ghi file cấu hình.
type ConfigSaveResult struct {
	OK        bool   `json:"ok"`
	Backup    string `json:"backup,omitempty"`
	Restarted bool   `json:"restarted"`
	Message   string `json:"message"`
}

// validateConfigContent ghi ra file tạm cùng thư mục, chạy `strand validate -c <tmp>`, tính diff, rồi dọn dẹp.
func (s *Server) validateConfigContent(ctx context.Context, p *Pipeline, yamlContent string) (*ConfigValidateResult, error) {
	if p == nil || p.ConfigPath == "" {
		return nil, errors.New("pipeline has no config path")
	}
	dir := filepath.Dir(p.ConfigPath)
	_ = os.MkdirAll(dir, 0o750)
	tmp, err := os.CreateTemp(dir, ".cfg-val-*")
	if err != nil {
		tmp, err = os.CreateTemp("", ".cfg-val-*")
		if err != nil {
			return nil, fmt.Errorf("create temp config file in %s: %w", dir, err)
		}
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	origBytes := s.getOriginalConfig(p)
	unmasked, err := unmaskConfigYAML([]byte(yamlContent), origBytes)
	if err != nil {
		tmp.Close()
		return nil, err
	}
	if _, err := tmp.Write(unmasked); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("write temp config file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("close temp config file: %w", err)
	}

	out, code, runErr := s.runStrand(ctx, p, "validate", "-c", tmpName)
	exitCode := 0
	if code != nil {
		exitCode = *code
	}
	valid := runErr == nil && exitCode == 0

	origText, _ := MaskConfigText(origBytes)
	newText, _ := MaskConfigText(unmasked)
	diff := unifiedDiff(p.ConfigPath, p.ConfigPath+" (new)", origText, newText)

	return &ConfigValidateResult{
		Valid:    valid,
		ExitCode: exitCode,
		Output:   out,
		Diff:     diff,
	}, nil
}

// saveConfigContent thực hiện các bước lưu cấu hình:
// (a) ghi ra file tạm cùng thư mục
// (b) chạy strand validate -c <tmp> (với env của pipeline); thất bại thì dừng ngay và không ghi đè trừ khi force=true
// (c) ghi bản sao lưu <file>.bak.<timestamp>
// (d) ghi đè nguyên tử (rename)
// (e) restart nếu được yêu cầu (và không có lỗi validate).
func (s *Server) saveConfigContent(ctx context.Context, p *Pipeline, yamlContent string, restart bool, force bool, ip string) (*ConfigSaveResult, error) {
	s.configEditMu.Lock()
	defer s.configEditMu.Unlock()
	return s.saveConfigContentLocked(ctx, p, yamlContent, restart, force, ip)
}

// Caller holds configEditMu through any read-modify-write of the config file.
func (s *Server) saveConfigContentLocked(ctx context.Context, p *Pipeline, yamlContent string, restart bool, force bool, ip string) (*ConfigSaveResult, error) {
	if p == nil || p.ConfigPath == "" {
		return nil, errors.New("pipeline has no config path")
	}
	dir := filepath.Dir(p.ConfigPath)
	_ = os.MkdirAll(dir, 0o750)
	tmp, err := os.CreateTemp(dir, ".cfg-save-*")
	if err != nil {
		tmp, err = os.CreateTemp("", ".cfg-save-*")
		if err != nil {
			return nil, fmt.Errorf("create temp file in %s: %w", dir, err)
		}
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	perm := os.FileMode(0o640)
	if fi, err := os.Stat(p.ConfigPath); err == nil {
		perm = fi.Mode().Perm()
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return nil, err
	}

	origBytes := s.getOriginalConfig(p)
	unmasked, err := unmaskConfigYAML([]byte(yamlContent), origBytes)
	if err != nil {
		tmp.Close()
		return nil, err
	}
	if _, err := tmp.Write(unmasked); err != nil {
		tmp.Close()
		return nil, fmt.Errorf("write temp config file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}

	// (b) Validate trước khi lưu
	out, code, runErr := s.runStrand(ctx, p, "validate", "-c", tmpName)
	exitCode := 0
	if code != nil {
		exitCode = *code
	}
	if (runErr != nil || exitCode != 0) && !force {
		_ = s.audit.Log(AuditEntry{
			IP:       ip,
			Action:   "config.edit",
			Pipeline: p.Name,
			Result:   "error",
			ExitCode: code,
			Detail:   map[string]any{"error": out},
		})
		return nil, &ActionError{
			Status: http.StatusBadGateway,
			Msg:    fmt.Sprintf("validation failed (exit %d): %s", exitCode, out),
		}
	}

	// (c) Sao lưu <file>.bak.<timestamp>
	ts := s.now().UTC().Format("20060102T150405Z")
	bakPath := p.ConfigPath + ".bak." + ts
	if len(origBytes) > 0 {
		if err := writeBackup(bakPath, origBytes, p.ConfigPath, perm); err != nil {
			s.log.Warn("write backup failed", "path", bakPath, "err", err)
		}
	}

	// Coordinate the final disk write and metadata update with materializeOpt.
	// Validation ran before this lock, so runStrand's materialization cannot
	// deadlock by trying to acquire it recursively.
	unlockFiles, err := lockPipelineFiles(p.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("lock pipeline config files: %w", err)
	}
	locked := true
	unlock := func() {
		if locked {
			unlockFiles()
			locked = false
		}
	}
	defer unlock()
	if err := s.checkPipelineConfigSnapshot(p, origBytes); err != nil {
		return nil, err
	}

	// (d) Ghi đè nguyên tử (rename)
	copyOwner(tmpName, p.ConfigPath)
	if err := os.Rename(tmpName, p.ConfigPath); err != nil {
		if werr := writeFilePreserving(p.ConfigPath, unmasked, false, false); werr != nil {
			return nil, fmt.Errorf("atomic rename %s -> %s: %w (fallback write: %v)", tmpName, p.ConfigPath, err, werr)
		}
	}

	// Chế độ DB: lưu bản mới vào DB ngay (DB là nguồn sự thật của pipeline do web tạo). Không lưu được thì
	// trả file về bản cũ và báo lỗi: chỉ giữ bản trên đĩa thì lần start/khởi động sau DB sẽ ghi đè lặng lẽ.
	if err := s.persistPipelineConfigSnapshot(p, origBytes, unmasked); err != nil {
		var actionErr *ActionError
		if errors.As(err, &actionErr) && actionErr.Status == http.StatusConflict {
			unlock()
			// A concurrent editor won the database compare-and-swap. Restore this
			// host's disk from the winning DB version; never roll it back to the
			// stale bytes that this request originally read.
			s.materialize(p.Name)
			return nil, err
		}
		rollbackErr := writeFilePreserving(p.ConfigPath, origBytes, false, false)
		unlock()
		_ = s.audit.Log(AuditEntry{IP: ip, Action: "config.edit", Pipeline: p.Name, Result: "error",
			Detail: map[string]any{"error": "metadata database: " + err.Error()}})
		msg := "config NOT saved: storing it in the metadata database failed (" + err.Error() + "); the file was left unchanged, try again"
		if rollbackErr != nil {
			msg = "config NOT stored in the metadata database (" + err.Error() + ") and restoring the previous file failed (" + rollbackErr.Error() + "): the file on disk has the new content"
		}
		return nil, &ActionError{Status: http.StatusServiceUnavailable, Msg: msg}
	}
	unlock()

	// Làm mới cấu hình trong registry
	s.reg.Refresh(ctx)

	restarted := false
	if restart && exitCode == 0 {
		_, rErr := s.runLifecycle(ctx, p, ActRestart)
		if rErr == nil {
			restarted = true
		} else {
			s.log.Warn("config saved but restart failed", "pipeline", p.Name, "err", rErr)
		}
	}

	zero := 0
	_ = s.audit.Log(AuditEntry{
		IP:       ip,
		Action:   "config.edit",
		Pipeline: p.Name,
		Result:   "ok",
		ExitCode: &zero,
		Detail:   map[string]any{"backup": bakPath, "restarted": restarted, "force": force, "val_exit": exitCode},
	})

	msg := "Configuration updated successfully"
	if exitCode != 0 {
		msg = fmt.Sprintf("Configuration saved with validation warnings/errors (exit %d) — please review before Start: %s", exitCode, out)
	} else if restarted {
		msg += " and pipeline restarted"
	}

	return &ConfigSaveResult{
		OK:        true,
		Backup:    bakPath,
		Restarted: restarted,
		Message:   msg,
	}, nil
}

// rollbackFile đặt lại nội dung cũ của path (giữ quyền/chủ) sau khi lưu vào DB thất bại.
func (s *Server) rollbackFile(path string, orig []byte) {
	if err := writeFilePreserving(path, orig, false, false); err != nil {
		s.log.Error("rollback of file after database failure failed", "path", path, "err", err)
	}
}

// EnvView thể hiện các biến môi trường của pipeline (.env), các khóa bí mật được che bằng '********'.
type EnvView struct {
	Path     string            `json:"path"`
	Database bool              `json:"database,omitempty"`
	Exists   bool              `json:"exists"`
	Entries  map[string]string `json:"entries"`
	Error    string            `json:"error,omitempty"`
}

func (s *Server) envPathFor(p *Pipeline) string {
	if p.EnvFile != "" {
		return p.EnvFile
	}
	dir := filepath.Dir(p.ConfigPath)
	return filepath.Join(dir, p.Name+".env")
}

func (s *Server) getEnvView(p *Pipeline) EnvView {
	s.materialize(p.Name)
	v := EnvView{Entries: map[string]string{}, Database: p.dbEnv != nil}
	var lines []string
	var err error
	if v.Database {
		lines, err = p.dbEnv.pipelineEnv(p.Name)
	} else {
		v.Path = s.envPathFor(p)
		lines, err = ReadEnvFile(v.Path)
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			v.Exists = false
			return v
		}
		v.Error = err.Error()
		return v
	}
	v.Exists = true
	for _, l := range lines {
		k, val, ok := strings.Cut(l, "=")
		if ok {
			if v.Database || isSecretKey(k) {
				v.Entries[k] = maskStr
			} else {
				v.Entries[k] = val
			}
		}
	}
	return v
}

// saveEnvContent lưu credentials, giữ nguyên giá trị cũ nếu người dùng gửi '********'.
func (s *Server) saveEnvContent(ctx context.Context, p *Pipeline, entries map[string]string, ip string) (string, error) {
	s.configEditMu.Lock()
	defer s.configEditMu.Unlock()
	s.materialize(p.Name)
	var existing []string
	var envPath string
	if p.dbEnv != nil {
		var err error
		existing, err = p.dbEnv.pipelineEnv(p.Name)
		if err != nil {
			return "", err
		}
	} else {
		envPath = s.envPathFor(p)
		existing, _ = ReadEnvFile(envPath)
	}
	existingVals := map[string]string{}
	for _, l := range existing {
		if k, v, ok := strings.Cut(l, "="); ok {
			existingVals[k] = v
		}
	}

	merged := map[string]string{}
	for k, v := range existingVals {
		merged[k] = v
	}
	for k, v := range entries {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		if v == maskStr || v == "" {
			// Giữ nguyên giá trị cũ nếu đã có
			if _, ok := existingVals[k]; ok {
				continue
			}
			if v == maskStr {
				continue // không bao giờ ghi '********' vào file nếu không có giá trị cũ
			}
		}
		merged[k] = v
	}

	var keys []string
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("# Environment variables for pipeline %s\n", p.Name))
	for _, k := range keys {
		v := merged[k]
		line, err := formatEnvLine(k, v)
		if err != nil {
			return "", err
		}
		b.WriteString(line)
	}

	if p.dbEnv != nil {
		row, found, err := s.db.st.GetPipeline(ctx, p.Name)
		if err != nil {
			return "", err
		}
		if !found || row.EnvPath != "" {
			return "", Conflict("pipeline credential storage changed; reload Config")
		}
		sealed, err := s.db.box.Seal(b.Bytes(), p.Name)
		if err != nil {
			return "", err
		}
		// Write env_enc only: a full-row upsert could replace a config that
		// another Web instance saved after the read above.
		updated, err := s.db.st.UpdatePipelineEnv(ctx, p.Name, "", sealed)
		if err == nil && !updated {
			return "", Conflict("pipeline was removed; reload Config")
		}
		if err != nil {
			s.log.Warn("store pipeline credentials failed", "pipeline", p.Name, "err", err)
			return "", &ActionError{Status: http.StatusServiceUnavailable, Msg: "Could not save credentials to the metadata database. Try again."}
		}
		_ = s.audit.Log(AuditEntry{IP: ip, Action: "env.edit", Pipeline: p.Name, Result: "ok",
			Detail: map[string]any{"storage": "encrypted metadata database"}})
		return "", nil
	}
	origEnv, origEnvErr := os.ReadFile(envPath)
	dir := filepath.Dir(envPath)
	_ = os.MkdirAll(dir, 0o750)

	tmp, err := os.CreateTemp(dir, ".env-save-*")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	// Preserve the original .env mode (e.g. 0640 root:strand) but never leave
	// it accessible to others; new files are 0600.
	envPerm := os.FileMode(0o600)
	if fi, err := os.Stat(envPath); err == nil {
		envPerm = fi.Mode().Perm() & secretPermMask
	}
	if err := tmp.Chmod(envPerm); err != nil {
		tmp.Close()
		return "", err
	}
	if _, err := tmp.Write(b.Bytes()); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}

	// Sao lưu .env cũ nếu có
	bakPath := ""
	if origBytes, err := os.ReadFile(envPath); err == nil && len(origBytes) > 0 {
		ts := s.now().UTC().Format("20060102T150405Z")
		bakPath = envPath + ".bak." + ts
		_ = writeBackup(bakPath, origBytes, envPath, envPerm)
	}

	copyOwner(tmpName, envPath)
	if err := os.Rename(tmpName, envPath); err != nil {
		if werr := writeFilePreserving(envPath, b.Bytes(), true, false); werr != nil {
			return "", fmt.Errorf("write %s: %w (rename err: %v)", envPath, werr, err)
		}
	}

	if err := s.persistPipelineEnv(p, envPath); err != nil {
		var rollbackErr error
		if errors.Is(origEnvErr, os.ErrNotExist) {
			rollbackErr = os.Remove(envPath)
		} else {
			rollbackErr = writeFilePreserving(envPath, origEnv, true, false)
		}
		_ = s.audit.Log(AuditEntry{IP: ip, Action: "env.edit", Pipeline: p.Name, Result: "error",
			Detail: map[string]any{"error": "metadata database: " + err.Error()}})
		msg := ".env NOT saved: storing it in the metadata database failed (" + err.Error() + "); the file was left unchanged, try again"
		if rollbackErr != nil {
			msg += " (restoring the previous file failed: " + rollbackErr.Error() + ")"
		}
		return "", &ActionError{Status: http.StatusServiceUnavailable, Msg: msg}
	}
	s.reg.Refresh(ctx)

	zero := 0
	_ = s.audit.Log(AuditEntry{
		IP:       ip,
		Action:   "env.edit",
		Pipeline: p.Name,
		Result:   "ok",
		ExitCode: &zero,
		Detail:   map[string]any{"path": envPath, "backup": bakPath},
	})

	return bakPath, nil
}

// ---- HTTP handlers ----

type configEditReq struct {
	YAML    string `json:"yaml"`
	Restart bool   `json:"restart"`
	Force   bool   `json:"force,omitempty"`
}

func (s *Server) apiConfigValidate(w http.ResponseWriter, r *http.Request, sess *Session) {
	name := r.PathValue("name")
	p := s.reg.Get(name)
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req configEditReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	res, err := s.validateConfigContent(r.Context(), p, req.YAML)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) apiConfigSave(w http.ResponseWriter, r *http.Request, sess *Session) {
	name := r.PathValue("name")
	p := s.reg.Get(name)
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req configEditReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	res, err := s.saveConfigContent(r.Context(), p, req.YAML, req.Restart, req.Force, s.clientIP(r))
	if err != nil {
		var ae *ActionError
		if errors.As(err, &ae) {
			writeErr(w, ae.Status, ae.Msg)
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) apiEnvGet(w http.ResponseWriter, r *http.Request, sess *Session) {
	name := r.PathValue("name")
	p := s.reg.Get(name)
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	v := s.getEnvView(p)
	writeJSON(w, http.StatusOK, v)
}

type envSaveReq struct {
	Entries map[string]string `json:"entries"`
}

func (s *Server) apiEnvSave(w http.ResponseWriter, r *http.Request, sess *Session) {
	name := r.PathValue("name")
	p := s.reg.Get(name)
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req envSaveReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	bak, err := s.saveEnvContent(r.Context(), p, req.Entries, s.clientIP(r))
	if err != nil {
		status := http.StatusInternalServerError
		var ae *ActionError
		if errors.As(err, &ae) {
			status = ae.Status
		}
		writeErr(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"backup":  bak,
		"message": ".env updated successfully",
	})
}

// registerConfigEditOps đăng ký các action liên quan tới sửa cấu hình.
func (s *Server) registerConfigEditOps() {
	s.actions.Register(Action{
		Name:       "config-validate",
		SecretArgs: []string{"yaml"},
		Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			res, err := srv.validateConfigContent(ctx, req.Pipeline, req.Args["yaml"])
			if err != nil {
				return ActionResult{}, err
			}
			return ActionResult{
				Message: res.Output,
				Data: map[string]any{
					"valid":     res.Valid,
					"diff":      res.Diff,
					"output":    res.Output,
					"exit_code": res.ExitCode,
				},
			}, nil
		},
	})
	s.actions.Register(Action{
		Name:       "config-save",
		SecretArgs: []string{"yaml"},
		Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			restart := req.Args["restart"] == "true" || req.Args["restart"] == "1"
			force := req.Args["force"] == "true" || req.Args["force"] == "1"
			res, err := srv.saveConfigContent(ctx, req.Pipeline, req.Args["yaml"], restart, force, req.IP)
			if err != nil {
				return ActionResult{}, err
			}
			return ActionResult{
				Message: res.Message,
				Data: map[string]any{
					"ok":        res.OK,
					"backup":    res.Backup,
					"restarted": res.Restarted,
				},
			}, nil
		},
	})
	s.actions.Register(Action{
		Name:          "env-save",
		SecretAllArgs: true,
		Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			var entries map[string]string
			if raw := req.Args["entries"]; raw != "" {
				_ = json.Unmarshal([]byte(raw), &entries)
			}
			if entries == nil {
				entries = map[string]string{}
				for k, v := range req.Args {
					if k != "confirm_name" && k != "entries" {
						entries[k] = v
					}
				}
			}
			bak, err := srv.saveEnvContent(ctx, req.Pipeline, entries, req.IP)
			if err != nil {
				return ActionResult{}, err
			}
			return ActionResult{
				Message: ".env updated successfully",
				Data:    map[string]any{"backup": bak},
			}, nil
		},
	})
}
