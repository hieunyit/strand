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
	"regexp"
	"strconv"
	"strings"
	texttmpl "text/template"

	"strand/internal/config"

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
	// TargetDatabase: database chứa bảng _strand_* của Strand (chỉ target MySQL); mặc định "strand".
	// Khác với Database/Databases (database DỮ LIỆU cần chuyển).
	TargetDatabase string `json:"target_database,omitempty"`
	// TargetPGDatabase: database PostgreSQL ĐÍCH (chỉ target PG) để chép vào; rỗng = cùng tên database nguồn.
	TargetPGDatabase string `json:"target_pg_database,omitempty"`
	PGSchema         string `json:"schema,omitempty"`

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
	GovAlgorithm    string   `json:"governor_algorithm,omitempty"`
	LagSLO          string   `json:"lag_slo,omitempty"`

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
  database: {{.TargetDatabase}}   # database chứa bảng _strand_* (checkpoint); cần CREATE DATABASE {{.TargetDatabase}}; trước khi sync
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
{{- if or .IOPSSoft .IOPSHard}}
  source: { iops: { {{if .IOPSSoft}}soft: {{.IOPSSoft}}{{end}}{{if and .IOPSSoft .IOPSHard}}, {{end}}{{if .IOPSHard}}hard: {{.IOPSHard}}{{end}} } }
{{- end}}
{{- if .Workers}}
  workers: { max: {{.Workers}} }
{{- end}}

governor:
{{- if .GovAlgorithm}}
  algorithm: {{.GovAlgorithm}}
{{- end}}
{{- if .LagSLO}}
  lag_slo: {{.LagSLO}}
{{- end}}
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
	TargetDatabase    string
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
	GovAlgorithm      string
	LagSLO            string
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
		TargetDatabase:    strings.TrimSpace(opts.TargetDatabase),
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
		GovAlgorithm:      opts.GovAlgorithm,
		LagSLO:            opts.LagSLO,
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
	dbs := dataDatabases(opts)
	if len(dbs) == 0 {
		dbs = []string{"app"}
	}
	td.Database = dbs[0] // PG: tên database; MySQL: database đầu tiên
	// Database chứa _strand_* tách khỏi database dữ liệu (giống `strand config init`).
	if td.TargetDatabase == "" {
		td.TargetDatabase = "strand"
	}
	if len(td.Includes) == 0 {
		if td.SourceMySQL {
			for _, d := range dbs {
				if d == "*" {
					td.Includes = append(td.Includes, "*.*")
				} else {
					td.Includes = append(td.Includes, d+".*")
				}
			}
		} else {
			schema := opts.PGSchema
			if schema == "" {
				schema = "public"
			}
			td.Includes = []string{schema + ".*"}
		}
	}
	if td.IOPSSoft > 0 && td.IOPSHard == 0 {
		td.IOPSHard = td.IOPSSoft * 3 / 2 // chỉ đặt soft: hard mặc định = 1.5 lần soft
	}
	if td.ServerID == 0 {
		td.ServerID = 47001
	}
	if td.CommitOrder == "" {
		td.CommitOrder = "relaxed"
	}
	if td.Metrics == "" {
		td.Metrics = defaultMetricsAddr
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
			tdb := strings.TrimSpace(opts.TargetPGDatabase)
			if tdb == "" {
				tdb = td.Database
			}
			td.TargetDSN = fmt.Sprintf("postgres://%s:${%s}@%s:%d/%s?sslmode=prefer",
				td.TargetUser, td.TargetPasswordEnv, td.TargetHost, td.TargetPort, tdb)
		}
	}

	var buf bytes.Buffer
	if err := pipelineTmpl.Execute(&buf, td); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// defaultMetricsAddr chỉ là giá trị dự phòng của hàm sinh YAML; createPipeline luôn chọn cổng riêng cho từng pipeline.
const defaultMetricsAddr = "127.0.0.1:9464"

// dataDatabases gộp `databases` (danh sách) và `database` (một tên hoặc nhiều tên cách nhau dấu phẩy).
func dataDatabases(opts NewPipelineOpts) []string {
	var out []string
	seen := map[string]bool{}
	add := func(d string) {
		if d = strings.TrimSpace(d); d != "" && !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	for _, d := range opts.Databases {
		for _, x := range strings.Split(d, ",") {
			add(x)
		}
	}
	for _, x := range strings.Split(opts.Database, ",") {
		add(x)
	}
	return out
}

// splitList tách chuỗi phân cách bằng dấu phẩy, bỏ phần tử rỗng.
func splitList(s string) []string {
	var out []string
	for _, x := range strings.Split(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

var plainDBNameRE = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)

// checkOpts từ chối giá trị làm hỏng YAML sinh ra (dấu nháy, xuống dòng, ...) và tên database metadata lạ.
func checkOpts(opts NewPipelineOpts) error {
	bad := func(field, v string) error {
		if strings.ContainsAny(v, "\"\\\r\n") {
			return fmt.Errorf("%s: value %q contains a quote, backslash or newline", field, v)
		}
		return nil
	}
	for field, vals := range map[string][]string{
		"databases": opts.Databases, "include": opts.Includes, "exclude": opts.Excludes, "windows": opts.Windows,
		"database": {opts.Database}, "schema": {opts.PGSchema}, "source_host": {opts.SourceHost},
		"target_host": {opts.TargetHost}, "source_user": {opts.SourceUser}, "target_user": {opts.TargetUser},
	} {
		for _, v := range vals {
			if err := bad(field, v); err != nil {
				return err
			}
		}
	}
	// Tuning snapshot: khoảng giá trị khớp với tab Config; rỗng/0 = mặc định.
	if err := validateWindowList(opts.Windows); err != nil {
		return fmt.Errorf("windows: %v", err)
	}
	if opts.GovAlgorithm != "" {
		if err := validateAlgorithm(opts.GovAlgorithm); err != nil {
			return fmt.Errorf("governor_algorithm: %v", err)
		}
	}
	for field, v := range map[string]string{"lag_slo": opts.LagSLO, "replica_lag_max": opts.ReplicaLagMax} {
		if v != "" {
			if err := validateDuration(v); err != nil {
				return fmt.Errorf("%s: %v", field, err)
			}
		}
	}
	if opts.BreakerMaxRatio != 0 {
		if err := validateRatio(opts.BreakerMaxRatio); err != nil {
			return fmt.Errorf("breaker_max_ratio: %v", err)
		}
	}
	for field, n := range map[string]int{"iops_soft": opts.IOPSSoft, "iops_hard": opts.IOPSHard} {
		if n < 0 || n > 10000000 {
			return fmt.Errorf("%s: %d is not an integer between 1 and 10000000", field, n)
		}
	}
	if err := validateIOPSPair(opts.IOPSSoft, opts.IOPSHard); err != nil {
		return err
	}
	if opts.ChunkRows < 0 || opts.ChunkRows > 10000000 {
		return fmt.Errorf("chunk_rows: %d is not an integer between 1 and 10000000", opts.ChunkRows)
	}
	if td := strings.TrimSpace(opts.TargetDatabase); td != "" && !plainDBNameRE.MatchString(td) {
		return fmt.Errorf("target_database %q is not a plain database name (letters, digits, _ and $ only)", td)
	}
	if td := strings.TrimSpace(opts.TargetPGDatabase); td != "" && !plainDBNameRE.MatchString(td) {
		return fmt.Errorf("target_pg_database %q is not a plain database name (letters, digits, _ and $ only)", td)
	}
	if opts.To == "postgres" && opts.From != "postgres" && strings.TrimSpace(opts.TargetPGDatabase) == "" && strings.TrimSpace(opts.TargetDSN) == "" {
		if dbs := dataDatabases(opts); len(dbs) > 0 && dbs[0] == "*" {
			return fmt.Errorf("copying all MySQL databases (*) to PostgreSQL: set target_pg_database (the PostgreSQL database to copy into)")
		}
	}
	if opts.From == "postgres" && opts.To == "mysql" {
		return fmt.Errorf("PostgreSQL can only be copied to PostgreSQL (postgres -> mysql is not supported)")
	}
	if opts.From == "mariadb" && opts.To == "postgres" {
		return fmt.Errorf("MariaDB can only be copied to MySQL (mariadb -> postgres is not supported)")
	}
	return nil
}

// socketPathLimit: giới hạn thực dùng cho socket mặc định (chừa một chút dư).
func socketFits(path string) bool { return len(path) <= config.MaxUnixSocketPath() }

// defaultAdminSocket chọn đường dẫn admin socket mặc định cho pipeline: <spool>/admin.sock nếu đủ ngắn
// (giới hạn sun_path ~107 byte), nếu không thì lần lượt <state_dir>/sock/<name>.sock, /run/strand/<name>.sock
// (khi thư mục đó tạo được/ghi được), cuối cùng <os.TempDir()>/strand-<name>.sock.
func defaultAdminSocket(spoolDir, name, stateDir string) string {
	c := filepath.Join(spoolDir, "admin.sock")
	if socketFits(c) {
		return c
	}
	c = filepath.Join(stateDir, "sock", name+".sock")
	if socketFits(c) {
		return c
	}
	c = filepath.Join("/run/strand", name+".sock")
	if socketFits(c) && (isWritableDir("/run/strand") || (isWritableDir("/run") && os.MkdirAll("/run/strand", 0o750) == nil)) {
		return c
	}
	return filepath.Join(os.TempDir(), "strand-"+name+".sock")
}

// metricsPort lấy số cổng từ địa chỉ metrics ("127.0.0.1:9464", ":9464"); 0 nếu không đọc được.
func metricsPort(addr string) int {
	_, ps, err := net.SplitHostPort(strings.TrimSpace(addr))
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(ps)
	return n
}

// usedMetricsPorts đọc observability.metrics của mọi pipeline đã đăng ký (đọc YAML lỏng, không cần cấu hình hợp lệ).
func (s *Server) usedMetricsPorts() map[int]string {
	used := map[int]string{}
	for _, p := range s.reg.List() {
		b, err := os.ReadFile(p.ConfigPath)
		if err != nil {
			continue
		}
		var c struct {
			Observability struct {
				Metrics string `yaml:"metrics"`
			} `yaml:"observability"`
		}
		if yaml.Unmarshal(b, &c) == nil {
			if port := metricsPort(c.Observability.Metrics); port > 0 {
				used[port] = p.Name
			}
		}
	}
	return used
}

// pickMetricsAddr chọn cổng /metrics riêng: tìm từ 9464 trở lên, bỏ cổng đã dùng trong cấu hình của pipeline khác
// và cổng đang có tiến trình lắng nghe.
func (s *Server) pickMetricsAddr() (string, error) {
	used := s.usedMetricsPorts()
	for port := 9464; port < 9464+1000; port++ {
		if _, taken := used[port]; taken {
			continue
		}
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			continue
		}
		_ = ln.Close()
		return addr, nil
	}
	return "", errors.New("no free metrics port in 9464-10463")
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
	// Socket mặc định nằm trong spool; nếu đường dẫn quá dài (giới hạn sun_path) thì chuyển sang chỗ ngắn hơn.
	socketPath := defaultAdminSocket(spoolDir, opts.Name, s.cfg.StateDir)

	if err := checkOpts(opts); err != nil {
		return nil, BadRequest("%s", err.Error())
	}
	// Mỗi pipeline một cổng /metrics riêng; cổng do người dùng đặt không được trùng pipeline khác.
	opts.Metrics = strings.TrimSpace(opts.Metrics)
	if opts.Metrics == "" {
		addr, err := s.pickMetricsAddr()
		if err != nil {
			return nil, err
		}
		opts.Metrics = addr
	} else {
		port := metricsPort(opts.Metrics)
		if port <= 0 || port > 65535 {
			return nil, BadRequest("invalid metrics address %q (want host:port)", opts.Metrics)
		}
		if other, taken := s.usedMetricsPorts()[port]; taken {
			return nil, Conflict("metrics port %d is already used by pipeline %q", port, other)
		}
	}

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
	for _, kv := range [][2]string{{srcEnv, opts.SourcePassword}, {dstEnv, opts.TargetPassword}} {
		line, err := formatEnvLine(kv[0], kv[1])
		if err != nil {
			return nil, err
		}
		envBuf.WriteString(line)
	}

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
	// Nếu configDir không khớp bất kỳ mẫu scan nào, ghi vào <state_dir>/pipelines.json
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
		// Nguồn sự thật duy nhất: <state_dir>/pipelines.json (luôn ghi được, kể cả khi /etc/strand read-only).
		if err := s.reg.AddCreated(entry); err != nil {
			return nil, fmt.Errorf("pipeline files written but registering in %s failed: %w", s.reg.createdPath(), err)
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
	vOut, vCode, vErr := s.runStrand(ctx, p, "validate", "-c", yamlPath)
	valExit := 0
	if vCode != nil {
		valExit = *vCode
	} else if vErr != nil {
		valExit = -1
	}
	valOK := vErr == nil && valExit == 0

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
		Result:   map[bool]string{true: "ok", false: "validate_failed"}[valOK],
		ExitCode: vCode,
		Detail: map[string]any{
			"config":   yamlPath,
			"env":      envPath,
			"val_exit": valExit,
		},
	})

	res := &CreatePipelineResult{
		OK:         valOK,
		Pipeline:   opts.Name,
		ConfigPath: yamlPath,
		EnvPath:    envPath,
		Message:    "Pipeline created successfully",
	}
	if !valOK {
		// File vẫn được giữ lại để sửa ở tab Config; ok=false để UI/API không báo thành công giả.
		res.Message = fmt.Sprintf("Pipeline files were created but validation failed (exit %d): fix the pipeline in the Config tab and re-validate before Start", valExit)
		if strings.Contains(vOut, "Unknown database") || strings.Contains(vOut, "1049") {
			res.Message += fmt.Sprintf(". Hint: the target metadata database must exist (CREATE DATABASE %s;)", metaDBOf(opts))
		}
		if vErr != nil && vOut == "" {
			vOut = vErr.Error()
		}
	}
	res.Validate.ExitCode = valExit
	res.Validate.Output = vOut
	res.Plan.ExitCode = planExit
	res.Plan.Output = pOut

	return res, nil
}

// metaDBOf trả tên database metadata của target MySQL (mặc định strand).
func metaDBOf(opts NewPipelineOpts) string {
	if d := strings.TrimSpace(opts.TargetDatabase); d != "" {
		return d
	}
	return "strand"
}

// atoiField đọc một trường số nguyên không âm của form; rỗng = 0.
func atoiField(r *http.Request, name string) (int, error) {
	v := strings.TrimSpace(r.PostForm.Get(name))
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid %s %q: want a non-negative integer", name, v)
	}
	return n, nil
}

// parseFormOpts đọc mọi trường mà form HTML gửi (application/x-www-form-urlencoded).
func parseFormOpts(r *http.Request) (NewPipelineOpts, error) {
	var opts NewPipelineOpts
	if err := r.ParseForm(); err != nil {
		return opts, fmt.Errorf("invalid form: %v", err)
	}
	f := r.PostForm
	opts.Name = f.Get("name")
	opts.From = f.Get("from")
	opts.To = f.Get("to")
	opts.SourceHost = f.Get("source_host")
	opts.TargetHost = f.Get("target_host")
	opts.SourceUser = f.Get("source_user")
	opts.TargetUser = f.Get("target_user")
	opts.SourcePassword = f.Get("source_password")
	opts.TargetPassword = f.Get("target_password")
	opts.SourcePasswordEnv = f.Get("source_password_env")
	opts.TargetPasswordEnv = f.Get("target_password_env")
	opts.SourceDSN = f.Get("source_dsn")
	opts.TargetDSN = f.Get("target_dsn")
	opts.Database = f.Get("database")
	opts.TargetDatabase = f.Get("target_database")
	opts.TargetPGDatabase = f.Get("target_pg_database")
	opts.PGSchema = f.Get("schema")
	opts.CommitOrder = f.Get("commit_order")
	opts.SpoolDir = f.Get("spool_dir")
	opts.ReplicaLagMax = f.Get("replica_lag_max")
	opts.GovAlgorithm = f.Get("governor_algorithm")
	opts.LagSLO = f.Get("lag_slo")
	opts.Metrics = f.Get("metrics")
	opts.LogFormat = f.Get("log_format")
	opts.Databases = splitList(f.Get("databases"))
	opts.Includes = splitList(f.Get("include"))
	opts.Excludes = splitList(f.Get("exclude"))
	opts.Windows = splitList(f.Get("windows"))
	for field, dst := range map[string]*int{
		"source_port": &opts.SourcePort, "target_port": &opts.TargetPort, "workers": &opts.Workers,
		"server_id": &opts.ServerID, "iops_soft": &opts.IOPSSoft, "iops_hard": &opts.IOPSHard,
		"chunk_rows": &opts.ChunkRows,
	} {
		n, err := atoiField(r, field)
		if err != nil {
			return opts, err
		}
		*dst = n
	}
	if v := strings.TrimSpace(f.Get("breaker_max_ratio")); v != "" {
		x, err := strconv.ParseFloat(v, 64)
		if err != nil || x < 0 {
			return opts, fmt.Errorf("invalid breaker_max_ratio %q: want a non-negative number", v)
		}
		opts.BreakerMaxRatio = x
	}
	return opts, nil
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
		var err error
		if opts, err = parseFormOpts(r); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
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
