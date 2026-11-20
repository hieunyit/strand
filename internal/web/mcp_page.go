package web

import (
	"net/http"
	"strings"
	"time"

	"strand/internal/mcp"
)

// Trang MCP (chỉ đọc): `strand mcp` là server MCP stdio do client (Claude Desktop/Code, IDE...) tự khởi động trên
// máy có binary strand và truy cập được DB metadata; web không chạy nó. Trang cho biết cách kết nối, các tool
// (đều chỉ đọc, allowlist cố định trong internal/mcp) và các lần gọi gần đây (audit "mcp.<tool>").

// MCPView là nội dung trang /mcp và GET /api/v1/mcp.
type MCPView struct {
	Command     string         `json:"command"`
	ConfigPath  string         `json:"config_path"`
	ClientJSON  string         `json:"client_json"`
	ClaudeCode  string         `json:"claude_code"`
	Tools       []mcp.ToolInfo `json:"tools"`
	RecentCalls []MCPCall      `json:"recent_calls"`
	CallsShown  int            `json:"calls_shown"`
}

// MCPCall là một lần gọi tool MCP (từ audit).
type MCPCall struct {
	Time   time.Time `json:"time"`
	Tool   string    `json:"tool"`
	Result string    `json:"result"`
}

func (s *Server) mcpView(r *http.Request) MCPView {
	bin := s.strandBinary()
	cfg := s.cfg.SourcePath
	if cfg == "" {
		cfg = "/etc/strand/web.yaml"
	}
	v := MCPView{Command: bin + " mcp --config " + cfg, ConfigPath: cfg, Tools: mcp.Tools(), RecentCalls: []MCPCall{}}
	v.ClientJSON = `{
  "mcpServers": {
    "strand-backup": {
      "command": "` + bin + `",
      "args": ["mcp", "--config", "` + cfg + `"]
    }
  }
}`
	v.ClaudeCode = "claude mcp add strand-backup -- " + bin + " mcp --config " + cfg
	ctx, cancel := dbCtx()
	defer cancel()
	rows, err := s.db.st.ListAudit(ctx, 2000)
	if err == nil {
		for _, a := range rows {
			if !strings.HasPrefix(a.Action, "mcp.") {
				continue
			}
			v.RecentCalls = append(v.RecentCalls, MCPCall{Time: a.Time, Tool: strings.TrimPrefix(a.Action, "mcp."), Result: a.Result})
			if len(v.RecentCalls) >= 50 {
				break
			}
		}
	}
	v.CallsShown = len(v.RecentCalls)
	return v
}

func (s *Server) pageMCP(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.render(w, "mcp", sess, pageData{Title: "MCP", Active: "mcp", Body: s.mcpView(r)})
}

func (s *Server) apiMCP(w http.ResponseWriter, r *http.Request, _ *Session) {
	writeJSON(w, http.StatusOK, s.mcpView(r))
}

// apiConfigDownload trả cấu hình (bản trong DB, bí mật đã che) dạng file YAML để tải về. Tải lên lại qua ô
// "Upload YAML" của trình sửa: các '********' được khôi phục từ bản lưu khi Save.
func (s *Server) apiConfigDownload(w http.ResponseWriter, r *http.Request, _ *Session) {
	p := s.lookup(w, r)
	if p == nil {
		return
	}
	raw := s.getOriginalConfig(p)
	if len(raw) == 0 {
		writeErr(w, http.StatusNotFound, "no stored configuration")
		return
	}
	masked, err := MaskConfigText(raw)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "config is not valid YAML")
		return
	}
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "config.download", Pipeline: p.Name, Result: "ok"})
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+p.Name+`.yaml"`)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(masked))
}
