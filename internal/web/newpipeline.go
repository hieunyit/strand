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
	"strings"
	texttmpl "text/template"

	"gopkg.in/yaml.v3"
)

// Tạo pipeline mới (M5):
// - Form cấu hình (tên whitelist, kind source/target, DSN không mật khẩu, user, biến môi trường mật khẩu, tables...)
// - Sinh YAML bằng logic config init + file .env (mode 0600) trong config_dir (mặc định /etc/strand nếu ghi được, nếu không thì state_dir/pipelines)
// - Chạy `validate` và `plan`, hiện kết quả
// - Thêm pipeline vào registry (web.yaml hoặc scan) rồi Registry.Refresh
// - Có nút Start (dùng action start sẵn có, chế độ background)

// NewPipelineOpts là tham số tạo pipeline mới.
type NewPipelineOpts struct {
	Name string `json:"name"`
	From string `json:"from"` // mysql | mariadb | postgres
	To   string `json:"to"`   // mysql | postgres

	SourceHost string `json:"source_host"`
	SourcePort int    `json:"source_port"`
	TargetHost string `json:"target_host"`
	TargetPort int    `json:"target_port"`

	SourceUser string `json:"source_user"`
	TargetUser string `json:"target_user"`

	SourcePassword string `json:"source_password"` // chỉ ghi vào .env, không bao giờ vào YAML
	TargetPassword string `json:"target_password"`

	SourcePasswordEnv string `json:"source_password_env"` // mặc định SRC_PW
	TargetPasswordEnv string `json:"target_password_env"` // mặc định DST_PW

	SourceDSN string `json:"source_dsn,omitempty"` // PG: dsn không có mật khẩu
	TargetDSN string `json:"target_dsn,omitempty"`

	Databases []string `json:"databases,omitempty"`
	Database  string   `json:"database,omitempty"`
	PGSchema  string   `json:"schema,omitempty"`

	Includes []string `json:"include,omitempty"`
	Excludes []string `json:"exclude,omitempty"`

	ServerID    int    `json:"server_id,omitempty"`
	CommitOrder string `json:"commit_order,omitempty"`

	SpoolDir string `json:"spool_dir,omitempty"`
	Workers  int    `json:"workers,omitempty"`
	IOPSSoft int    `json:"iops_soft,omitempty"`
	IOPSHard int    `json:"iops_hard,omitempty"`

	ChunkRows       int      `json:"chunk_rows,omitempty"`
	Windows         []string `json:"windows,omitempty"`
	BreakerMaxRatio float64  `json:"breaker_max_ratio,omitempty"`
	ReplicaLagMax   string   `json:"replica_lag_max,omitempty"`

	Metrics   string `json:"metrics,omitempty"`
	LogFormat string `json:"log_format,omitempty"`
}

// CreatePipelineResult chứa kết quả sau khi tạo pipeline và chạy validate + plan.
type CreatePipelineResult struct {
	OK         bool   `json:"ok"`
	Pipeline   string `json:"pipeline"`
	ConfigPath string `json:"config_path"`
	EnvPath    string `json:"env_path"`
	Validate   struct {
		ExitCode int    `json:"exit_code"`
		Output   string `json:"output"`
	} `json:"validate"`
	Plan struct {
		ExitCode int    `json:"exit_code"`
		Output   string `json:"output"`
	} `json:"plan"`
	Message string `json:"message"`
}

// isWritableDir kiểm tra quyền ghi của thư mục bằng file tạm.
func isWritableDir(dir string) bool {
	fi, err := os.Stat(dir)
	if err != nil || !fi.IsDir() {
		return false
	}
	tmp, err := os.CreateTemp(dir, ".probe-*")
	if err != nil {
		return false
	}
	_ = tmp.Close()
	_ = os.Remove(tmp.Name())
	return true
}

// ConfigDir trả thư mục chứa cấu hình pipeline theo thứ tự ưu tiên:
// 1. web.yaml `config_dir`
// 2. /etc/strand nếu ghi được
// 3. <state_dir>/pipelines
func (s *Server) ConfigDir() string {
	if s.cfg.ConfigDir != "" {
		return s.cfg.ConfigDir
	}
	if isWritableDir("/etc/strand") {
		return "/etc/strand"
	}
	return filepath.Join(s.cfg.StateDir, "pipelines")
}

const pipelineTmplText = `# Cấu hình Strand: {{.From}} -> {{.To}} (tạo qua Web UI M5)
version: 1
name: {{.Name}}

source:
{{- if .SourceMySQL}}
  kind: mysql
  host: {{.SourceHost}}
  port: {{.SourcePort}}
  user: {{.SourceUser}}
  password_env: {{.SourcePasswordEnv}}
  server_id: {{.ServerID}}
{{- else}}
  kind: postgres
  dsn: "{{.SourceDSN}}"
  create_slot: true
  publication: strand_{{.Name | slot}}
{{- end}}

target:
{{- if .TargetMySQL}}
  kind: mysql
  host: {{.TargetHost}}
  port: {{.TargetPort}}
  user: {{.TargetUser}}
  password_env: {{.TargetPasswordEnv}}
  database: {{.Database}}
  commit_order: {{.CommitOrder}}
{{- else}}
  kind: postgres
  dsn: "{{.TargetDSN}}"
  commit_order: {{.CommitOrder}}
{{- end}}

tables:
  include: [{{range $i, $e := .Includes}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
{{- if .Excludes}}
  exclude: [{{range $i, $e := .Excludes}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
{{- end}}

bootstrap:
  default: logical
{{- if or .ChunkRows .Windows}}
  logical: { {{if .ChunkRows}}chunk_rows: {{.ChunkRows}}{{if .Windows}}, {{end}}{{end}}{{if .Windows}}windows: [{{range $i, $e := .Windows}}{{if $i}}, {{end}}"{{$e}}"{{end}}]{{end}} }
{{- end}}

resources:
  spool: { path: {{.SpoolDir}} }
{{- if .IOPSSoft}}
  source: { iops: { soft: {{.IOPSSoft}}, hard: {{.IOPSHard}} } }
{{- end}}
{{- if .Workers}}
  workers: { max: {{.Workers}} }
{{- end}}

governor:
{{- if or .BreakerMaxRatio .ReplicaLagMax}}
  breaker: { {{if .BreakerMaxRatio}}strand_query_p99: { max_ratio: {{.BreakerMaxRatio}} }{{if .ReplicaLagMax}}, {{end}}{{end}}{{if .ReplicaLagMax}}replica_lag: { max: {{.ReplicaLagMax}} }{{end}} }
{{- end}}

observability:
  admin_socket: {{.Socket}}
  metrics: "{{.Metrics}}"
  log: { level: info, format: {{.LogFormat}} }
`

type tmplData struct {
	Name              string
	From, To          string
	SourceHost        string
	SourcePort        int
	TargetHost        string
	TargetPort        int
	SourceUser        string
	TargetUser        string
	SourcePasswordEnv string
	TargetPasswordEnv string
	SourceDSN         string
	TargetDSN         string
	Database          string
	Includes          []string
	Excludes          []string
	ServerID          int
	CommitOrder       string
	SpoolDir          string
	Socket            string
	Workers           int
	IOPSSoft          int
	IOPSHard          int
	ChunkRows         int
	Windows           []string
	BreakerMaxRatio   float64
	ReplicaLagMax     string
	Metrics           string
	LogFormat         string
	SourceMySQL       bool
	TargetMySQL       bool
	MariaDB           bool
}

var pipelineTmpl = texttmpl.Must(texttmpl.New("pipe").Funcs(texttmpl.FuncMap{
	"slot": func(s string) string { return strings.ReplaceAll(s, "-", "_") },
}).Parse(pipelineTmplText))

// buildNewPipelineYAML sinh chuỗi YAML từ NewPipelineOpts.
func buildNewPipelineYAML(opts NewPipelineOpts, spoolDir, socketPath string) (string, error) {
	from := strings.ToLower(strings.TrimSpace(opts.From))
	to := strings.ToLower(strings.TrimSpace(opts.To))
	td := tmplData{
		Name:              opts.Name,
		From:              from,
		To:                to,
		SourceHost:        opts.SourceHost,
		SourcePort:        opts.SourcePort,
		TargetHost:        opts.TargetHost,
		TargetPort:        opts.TargetPort,
		SourceUser:        opts.SourceUser,
		TargetUser:        opts.TargetUser,
		SourcePasswordEnv: opts.SourcePasswordEnv,
		TargetPasswordEnv: opts.TargetPasswordEnv,
		Database:          opts.Database,
		Includes:          opts.Includes,
		Excludes:          opts.Excludes,
		ServerID:          opts.ServerID,
		CommitOrder:       opts.CommitOrder,
		SpoolDir:          spoolDir,
		Socket:            socketPath,
		Workers:           opts.Workers,
		IOPSSoft:          opts.IOPSSoft,
		IOPSHard:          opts.IOPSHard,
		ChunkRows:         opts.ChunkRows,
		Windows:           opts.Windows,
		BreakerMaxRatio:   opts.BreakerMaxRatio,
		ReplicaLagMax:     opts.ReplicaLagMax,
		Metrics:           opts.Metrics,
		LogFormat:         opts.LogFormat,
	}

	switch from {
	case "mysql", "mariadb":
		td.SourceMySQL = true
		td.MariaDB = from == "mariadb"
	case "postgres", "postgresql", "pg":
		td.From = "postgres"
	default:
		return "", fmt.Errorf("source engine must be mysql, mariadb or postgres, got %q", from)
	}

	switch to {
	case "mysql":
		td.TargetMySQL = true
	case "postgres", "postgresql", "pg":
		td.To = "postgres"
	default:
		return "", fmt.Errorf("target engine must be mysql or postgres, got %q", to)
	}

	if td.MariaDB && !td.TargetMySQL {
		return "", errors.New("a MariaDB source is supported only with a MySQL target")
	}
	if !td.SourceMySQL && td.TargetMySQL {
		return "", errors.New("a PostgreSQL source is supported only with a PostgreSQL target")
	}

	if td.SourceHost == "" {
		td.SourceHost = "127.0.0.1"
	}
	if td.TargetHost == "" {
		td.TargetHost = "127.0.0.1"
	}
	if td.SourcePort == 0 {
		if td.SourceMySQL {
			td.SourcePort = 3306
		} else {
			td.SourcePort = 5432
		}
	}
	if td.TargetPort == 0 {
		if td.TargetMySQL {
			td.TargetPort = 3306
		} else {
			td.TargetPort = 5432
		}
	}
	if td.SourceUser == "" {
		td.SourceUser = "strand"
	}
	if td.TargetUser == "" {
		td.TargetUser = "strand"
	}
	if td.SourcePasswordEnv == "" {
		td.SourcePasswordEnv = "SRC_PW"
	}
	if td.TargetPasswordEnv == "" {
		td.TargetPasswordEnv = "DST_PW"
	}
	if td.Database == "" {
		td.Database = "app"
	}
	if len(td.Includes) == 0 {
		if td.SourceMySQL {
			td.Includes = []string{td.Database + ".*"}
		} else {
			schema := opts.PGSchema
			if schema == "" {
				schema = "public"
			}
			td.Includes = []string{schema + ".*"}
		}
	}
	if td.ServerID == 0 {
		td.ServerID = 47001
	}
	if td.CommitOrder == "" {
		td.CommitOrder = "relaxed"
	}
	if td.Metrics == "" {
		td.Metrics = "127.0.0.1:9464"
	}
	if td.LogFormat == "" {
		td.LogFormat = "json"
	}

	// DSN PostgreSQL
	if !td.SourceMySQL {
		if opts.SourceDSN != "" {
			td.SourceDSN = opts.SourceDSN
		} else {
			td.SourceDSN = fmt.Sprintf("postgres://%s:${%s}@%s:%d/%s?sslmode=prefer",
				td.SourceUser, td.SourcePasswordEnv, td.SourceHost, td.SourcePort, td.Database)
		}
	}
	if !td.TargetMySQL {
		if opts.TargetDSN != "" {
			td.TargetDSN = opts.TargetDSN
		} else {
			td.TargetDSN = fmt.Sprintf("postgres://%s:${%s}@%s:%d/%s?sslmode=prefer",
				td.TargetUser, td.TargetPasswordEnv, td.TargetHost, td.TargetPort, td.Database)
		}
	}

	var buf bytes.Buffer
	if err := pipelineTmpl.Execute(&buf, td); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// AddPipelineToWebConfig thêm một PipelineEntry vào file web.yaml, giữ nguyên cấu trúc và chú thích.
func AddPipelineToWebConfig(path string, entry PipelineEntry) error {
	var doc yaml.Node
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(bytes.TrimSpace(b)) > 0 {
		if err := yaml.Unmarshal(b, &doc); err != nil {
			return err
		}
	}
	if doc.Kind == 0 {
		doc = yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode}}}
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("%s: top level must be a mapping", path)
	}

	var pipeSeq *yaml.Node
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "pipelines" {
			pipeSeq = root.Content[i+1]
			break
		}
	}
	if pipeSeq == nil {
		pipeSeq = &yaml.Node{Kind: yaml.SequenceNode}
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "pipelines"},
			pipeSeq)
	}

	// Kiểm tra xem entry đã tồn tại trong sequence chưa
	for _, item := range pipeSeq.Content {
		if item.Kind == yaml.MappingNode {
			for j := 0; j+1 < len(item.Content); j += 2 {
				if item.Content[j].Value == "name" && item.Content[j+1].Value == entry.Name {
					return nil // đã có
				}
			}
		}
	}

	entryMap := &yaml.Node{Kind: yaml.MappingNode}
	entryMap.Content = append(entryMap.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "name"},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: entry.Name},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "config"},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: entry.Config},
	)
	if entry.EnvFile != "" {
		entryMap.Content = append(entryMap.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "env_file"},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: entry.EnvFile},
		)
	}
	pipeSeq.Content = append(pipeSeq.Content, entryMap)

	var out bytes.Buffer
	enc := yaml.NewEncoder(&out)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return err
	}
	_ = enc.Close()

	tmp, err := os.CreateTemp(filepath.Dir(path), ".web-yaml-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(out.Bytes()); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// createPipeline thực hiện tạo pipeline mới:
// 1. Kiểm tra whitelist tên và trùng lặp
// 2. Sinh file cấu hình YAML và file .env (mode 0600) trong config_dir
// 3. Thêm pipeline vào web.yaml / scan
// 4. Registry.Refresh
// 5. Chạy validate và plan
// 6. Ghi audit log
func (s *Server) createPipeline(ctx context.Context, opts NewPipelineOpts, ip string) (*CreatePipelineResult, error) {
	opts.Name = strings.TrimSpace(opts.Name)
	if !ValidName(opts.Name) {
		return nil, BadRequest("invalid pipeline name %q (allowed: letters, digits, _ . - ; up to 63 chars)", opts.Name)
	}
	if s.reg.Get(opts.Name) != nil {
		return nil, Conflict("pipeline %q already exists in registry", opts.Name)
	}

	configDir := s.ConfigDir()
	if err := os.MkdirAll(configDir, 0o750); err != nil {
		return nil, fmt.Errorf("create config dir %s: %w", configDir, err)
	}

	yamlPath := filepath.Join(configDir, opts.Name+".yaml")
	envPath := filepath.Join(configDir, opts.Name+".env")

	// Chặn path traversal: đảm bảo file nằm trong configDir
	if filepath.Dir(yamlPath) != filepath.Clean(configDir) || filepath.Dir(envPath) != filepath.Clean(configDir) {
		return nil, BadRequest("path traversal detected")
	}

	if fileExists(yamlPath) {
		return nil, Conflict("config file %s already exists", yamlPath)
	}
	if fileExists(envPath) {
		return nil, Conflict("env file %s already exists", envPath)
	}

	spoolDir := opts.SpoolDir
	if spoolDir == "" {
		spoolDir = filepath.Join(s.cfg.StateDir, "pipelines", opts.Name, "spool")
	}
	socketPath := filepath.Join(spoolDir, "admin.sock")

	yamlContent, err := buildNewPipelineYAML(opts, spoolDir, socketPath)
	if err != nil {
		return nil, BadRequest("%s", err.Error())
	}

	// Ghi file .env với mode 0600 (trước khi ghi YAML)
	srcEnv := opts.SourcePasswordEnv
	if srcEnv == "" {
		srcEnv = "SRC_PW"
	}
	dstEnv := opts.TargetPasswordEnv
	if dstEnv == "" {
		dstEnv = "DST_PW"
	}
	var envBuf bytes.Buffer
	envBuf.WriteString(fmt.Sprintf("# Environment variables for pipeline %s\n", opts.Name))
	envBuf.WriteString(fmt.Sprintf("export %s=%q\n", srcEnv, opts.SourcePassword))
	envBuf.WriteString(fmt.Sprintf("export %s=%q\n", dstEnv, opts.TargetPassword))

	tmpEnv, err := os.CreateTemp(configDir, ".env-init-*")
	if err != nil {
		return nil, fmt.Errorf("create temp env file: %w", err)
	}
	tmpEnvName := tmpEnv.Name()
	defer os.Remove(tmpEnvName)

	if err := tmpEnv.Chmod(0o600); err != nil {
		tmpEnv.Close()
		return nil, err
	}
	if _, err := tmpEnv.Write(envBuf.Bytes()); err != nil {
		tmpEnv.Close()
		return nil, err
	}
	if err := tmpEnv.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpEnvName, envPath); err != nil {
		return nil, fmt.Errorf("write %s: %w", envPath, err)
	}
	_ = os.Chmod(envPath, 0o600)

	// Ghi file YAML
	tmpYaml, err := os.CreateTemp(configDir, ".yaml-init-*")
	if err != nil {
		_ = os.Remove(envPath)
		return nil, fmt.Errorf("create temp yaml file: %w", err)
	}
	tmpYamlName := tmpYaml.Name()
	defer os.Remove(tmpYamlName)

	if err := tmpYaml.Chmod(0o640); err != nil {
		tmpYaml.Close()
		_ = os.Remove(envPath)
		return nil, err
	}
	if _, err := tmpYaml.Write([]byte(yamlContent)); err != nil {
		tmpYaml.Close()
		_ = os.Remove(envPath)
		return nil, err
	}
	if err := tmpYaml.Close(); err != nil {
		_ = os.Remove(envPath)
		return nil, err
	}
	if err := os.Rename(tmpYamlName, yamlPath); err != nil {
		_ = os.Remove(envPath)
		return nil, fmt.Errorf("write %s: %w", yamlPath, err)
	}

	// Đăng ký pipeline vào registry:
	// Nếu configDir không khớp bất kỳ mẫu scan nào, thêm vào s.cfg.Pipelines và web.yaml
	coveredByScan := false
	for _, pat := range s.cfg.Scan {
		if matched, _ := filepath.Match(pat, yamlPath); matched {
			coveredByScan = true
			break
		}
	}
	entry := PipelineEntry{
		Name:    opts.Name,
		Config:  yamlPath,
		EnvFile: envPath,
	}
	if !coveredByScan {
		s.cfg.Pipelines = append(s.cfg.Pipelines, entry)
		if s.cfg.SourcePath != "" && isWritableDir(filepath.Dir(s.cfg.SourcePath)) {
			_ = AddPipelineToWebConfig(s.cfg.SourcePath, entry)
		}
	}
	s.reg.Refresh(ctx)

	p := s.reg.Get(opts.Name)
	if p == nil {
		// Tạo đối tượng tạm nếu refresh chưa kịp nhận
		p = &Pipeline{
			Name:       opts.Name,
			ConfigPath: yamlPath,
			EnvFile:    envPath,
			Origin:     "created",
		}
	}

	// Chạy validate
	vOut, vCode, _ := s.runStrand(ctx, p, "validate", "-c", yamlPath)
	valExit := 0
	if vCode != nil {
		valExit = *vCode
	}

	// Chạy plan
	pOut, pCode, _ := s.runStrand(ctx, p, "plan", "-c", yamlPath)
	planExit := 0
	if pCode != nil {
		planExit = *pCode
	}

	_ = s.audit.Log(AuditEntry{
		IP:       ip,
		Action:   "pipeline.create",
		Pipeline: opts.Name,
		Result:   "ok",
		ExitCode: vCode,
		Detail: map[string]any{
			"config":   yamlPath,
			"env":      envPath,
			"val_exit": valExit,
		},
	})

	res := &CreatePipelineResult{
		OK:         true,
		Pipeline:   opts.Name,
		ConfigPath: yamlPath,
		EnvPath:    envPath,
		Message:    "Pipeline created successfully",
	}
	res.Validate.ExitCode = valExit
	res.Validate.Output = vOut
	res.Plan.ExitCode = planExit
	res.Plan.Output = pOut

	return res, nil
}

// ---- HTTP Handlers ----

func (s *Server) pageNewPipeline(w http.ResponseWriter, r *http.Request, sess *Session) {
	data := pageData{
		Title:   "New pipeline",
		Active:  "new-pipeline",
		Refresh: 0,
		Body: map[string]any{
			"ConfigDir": s.ConfigDir(),
			"StateDir":  s.cfg.StateDir,
		},
	}
	s.render(w, "new_pipeline", sess, data)
}

func (s *Server) apiPipelineCreate(w http.ResponseWriter, r *http.Request, sess *Session) {
	var opts NewPipelineOpts
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/json") {
		if err := json.NewDecoder(r.Body).Decode(&opts); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
	} else {
		_ = r.ParseForm()
		opts.Name = r.PostForm.Get("name")
		opts.From = r.PostForm.Get("from")
		opts.To = r.PostForm.Get("to")
		opts.SourceHost = r.PostForm.Get("source_host")
		opts.TargetHost = r.PostForm.Get("target_host")
		opts.SourceUser = r.PostForm.Get("source_user")
		opts.TargetUser = r.PostForm.Get("target_user")
		opts.SourcePassword = r.PostForm.Get("source_password")
		opts.TargetPassword = r.PostForm.Get("target_password")
		opts.SourcePasswordEnv = r.PostForm.Get("source_password_env")
		opts.TargetPasswordEnv = r.PostForm.Get("target_password_env")
		opts.Database = r.PostForm.Get("database")
		opts.PGSchema = r.PostForm.Get("schema")
		opts.CommitOrder = r.PostForm.Get("commit_order")
		opts.SpoolDir = r.PostForm.Get("spool_dir")
		if dbs := r.PostForm.Get("databases"); dbs != "" {
			for _, d := range strings.Split(dbs, ",") {
				if d = strings.TrimSpace(d); d != "" {
					opts.Databases = append(opts.Databases, d)
				}
			}
		}
		if inc := r.PostForm.Get("include"); inc != "" {
			for _, item := range strings.Split(inc, ",") {
				if item = strings.TrimSpace(item); item != "" {
					opts.Includes = append(opts.Includes, item)
				}
			}
		}
	}

	res, err := s.createPipeline(r.Context(), opts, s.clientIP(r))
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
