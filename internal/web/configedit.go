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

// unmaskConfigYAML khôi phục giá trị bí mật từ file gốc nếu người dùng gửi lại chuỗi '********'
// (theo dòng, giữ nguyên định dạng). Lỗi nếu không khôi phục được: không bao giờ ghi '********' như mật khẩu thật.
func unmaskConfigYAML(newYAML []byte, origPath string) ([]byte, error) {
	origBytes, err := os.ReadFile(origPath)
	if err != nil {
		origBytes = nil
	}
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
	tmp, err := os.CreateTemp(dir, ".cfg-val-*")
	if err != nil {
		return nil, fmt.Errorf("create temp config file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	unmasked, err := unmaskConfigYAML([]byte(yamlContent), p.ConfigPath)
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

	origBytes, _ := os.ReadFile(p.ConfigPath)
	origText, err := MaskConfigText(origBytes)
	if err != nil {
		return nil, fmt.Errorf("mask original config: %w", err)
	}
	newText, err := MaskConfigText(unmasked)
	if err != nil {
		return nil, fmt.Errorf("mask edited config: %w", err)
	}
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
// (b) chạy strand validate -c <tmp> (với env của pipeline); thất bại thì dừng ngay và không ghi đè
// (c) ghi bản sao lưu <file>.bak.<timestamp>
// (d) ghi đè nguyên tử (rename)
// (e) restart nếu được yêu cầu.
func (s *Server) saveConfigContent(ctx context.Context, p *Pipeline, yamlContent string, restart bool, ip string) (*ConfigSaveResult, error) {
	s.configEditMu.Lock()
	defer s.configEditMu.Unlock()
	if p == nil || p.ConfigPath == "" {
		return nil, errors.New("pipeline has no config path")
	}
	dir := filepath.Dir(p.ConfigPath)
	tmp, err := os.CreateTemp(dir, ".cfg-save-*")
	if err != nil {
		return nil, fmt.Errorf("create temp file in %s: %w", dir, err)
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

	unmasked, err := unmaskConfigYAML([]byte(yamlContent), p.ConfigPath)
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
	if runErr != nil || exitCode != 0 {
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
	origBytes, _ := os.ReadFile(p.ConfigPath)
	ts := s.now().UTC().Format("20060102T150405Z")
	bakPath := p.ConfigPath + ".bak." + ts
	if len(origBytes) > 0 {
		if err := writeBackup(bakPath, origBytes, p.ConfigPath, perm); err != nil {
			return nil, fmt.Errorf("write backup %s: %w", bakPath, err)
		}
	}

	// (d) Ghi đè nguyên tử (rename)
	copyOwner(tmpName, p.ConfigPath)
	if err := os.Rename(tmpName, p.ConfigPath); err != nil {
		return nil, fmt.Errorf("atomic rename %s -> %s: %w", tmpName, p.ConfigPath, err)
	}

	// Chế độ DB: lưu bản mới vào DB ngay (DB là nguồn sự thật của pipeline do web tạo).
	if err := s.persistPipelineFiles(p); err != nil {
		rollbackErr := writeFilePreserving(p.ConfigPath, origBytes, false, false)
		return nil, errors.Join(fmt.Errorf("persist config: %w", err), rollbackErr)
	}

	// Làm mới cấu hình trong registry
	s.reg.Refresh(ctx)

	restarted := false
	if restart {
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
		Detail:   map[string]any{"backup": bakPath, "restarted": restarted},
	})

	return &ConfigSaveResult{
		OK:        true,
		Backup:    bakPath,
		Restarted: restarted,
		Message:   "Configuration updated successfully",
	}, nil
}

// EnvView thể hiện các biến môi trường của pipeline (.env), các khóa bí mật được che bằng '********'.
type EnvView struct {
	Path    string            `json:"path"`
	Exists  bool              `json:"exists"`
	Entries map[string]string `json:"entries"`
	Error   string            `json:"error,omitempty"`
}

func (s *Server) envPathFor(p *Pipeline) string {
	if p.EnvFile != "" {
		return p.EnvFile
	}
	dir := filepath.Dir(p.ConfigPath)
	return filepath.Join(dir, p.Name+".env")
}

func (s *Server) getEnvView(p *Pipeline) EnvView {
	path := s.envPathFor(p)
	v := EnvView{Path: path, Entries: map[string]string{}}
	lines, err := ReadEnvFile(path)
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
			if isSecretKey(k) {
				v.Entries[k] = maskStr
			} else {
				v.Entries[k] = val
			}
		}
	}
	return v
}

// saveEnvContent lưu file .env (mode 0600), giữ nguyên giá trị cũ nếu người dùng gửi '********'.
func (s *Server) saveEnvContent(ctx context.Context, p *Pipeline, entries map[string]string, ip string) (string, error) {
	s.configEditMu.Lock()
	defer s.configEditMu.Unlock()
	envPath := s.envPathFor(p)
	origEnv, origEnvErr := os.ReadFile(envPath)
	dir := filepath.Dir(envPath)
	_ = os.MkdirAll(dir, 0o750)

	// Đọc các giá trị hiện có để giữ nguyên khi người dùng giữ '********'
	existingVals := map[string]string{}
	if origLines, err := ReadEnvFile(envPath); err == nil {
		for _, l := range origLines {
			k, v, ok := strings.Cut(l, "=")
			if ok {
				existingVals[k] = v
			}
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
		return "", fmt.Errorf("rename to %s: %w", envPath, err)
	}

	updated := *p
	updated.EnvFile = envPath
	if err := s.persistPipelineFiles(&updated); err != nil {
		var rollbackErr error
		if errors.Is(origEnvErr, os.ErrNotExist) {
			rollbackErr = os.Remove(envPath)
		} else {
			rollbackErr = writeFilePreserving(envPath, origEnv, true, false)
		}
		return "", errors.Join(fmt.Errorf("persist env: %w", err), rollbackErr)
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
	res, err := s.saveConfigContent(r.Context(), p, req.YAML, req.Restart, s.clientIP(r))
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
			res, err := srv.saveConfigContent(ctx, req.Pipeline, req.Args["yaml"], restart, req.IP)
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
