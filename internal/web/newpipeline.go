package web

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	texttmpl "text/template"
	"time"

	"strand/internal/backup/conncheck"
	"strand/internal/config"
	"strand/internal/web/store"

	mysqldriver "github.com/go-sql-driver/mysql"

	"gopkg.in/yaml.v3"
)

const (
	pipelineSourcePassword = "STRAND_PIPELINE_SOURCE_PASSWORD"
	pipelineTargetPassword = "STRAND_PIPELINE_TARGET_PASSWORD"
	pipelineSourceDSN      = "STRAND_PIPELINE_SOURCE_DSN"
	pipelineTargetDSN      = "STRAND_PIPELINE_TARGET_DSN"
)

// NewPipelineOpts là tham số tạo pipeline mới.
type NewPipelineOpts struct {
	Name      string `json:"name"`
	From      string `json:"from"` // mysql | mariadb | postgres
	To        string `json:"to"`   // mysql | postgres
	Overwrite bool   `json:"overwrite,omitempty"`

	SourceHost string `json:"source_host"`
	SourcePort int    `json:"source_port"`
	TargetHost string `json:"target_host"`
	TargetPort int    `json:"target_port"`

	SourceSSHTunnel *config.SSHTunnelConfig `json:"source_ssh_tunnel,omitempty"`
	TargetSSHTunnel *config.SSHTunnelConfig `json:"target_ssh_tunnel,omitempty"`

	SourceUser string `json:"source_user"`
	TargetUser string `json:"target_user"`

	SourcePassword string `json:"source_password"`
	TargetPassword string `json:"target_password"`

	SourcePasswordEnv string `json:"source_password_env"` // optional legacy API override
	TargetPasswordEnv string `json:"target_password_env"` // optional legacy API override

	SourceDSN string `json:"source_dsn,omitempty"`
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
	Keyless  string   `json:"keyless,omitempty"`

	ServerID    int    `json:"server_id,omitempty"`
	CommitOrder string `json:"commit_order,omitempty"`

	SpoolDir string `json:"spool_dir,omitempty"`
	Workers  int    `json:"workers,omitempty"`
	IOPSSoft int    `json:"iops_soft,omitempty"`
	IOPSHard int    `json:"iops_hard,omitempty"`

	ChunkRows       int      `json:"chunk_rows,omitempty"`
	MaxParallel     int      `json:"max_parallel,omitempty"` // bootstrap.logical.max_parallel: số bảng snapshot cùng lúc
	StartAt         string   `json:"start_at,omitempty"`
	Windows         []string `json:"windows,omitempty"`
	BreakerMaxRatio float64  `json:"breaker_max_ratio,omitempty"`
	ReplicaLagMax   string   `json:"replica_lag_max,omitempty"`
	GovAlgorithm    string   `json:"governor_algorithm,omitempty"`
	LagSLO          string   `json:"lag_slo,omitempty"`

	Metrics   string `json:"metrics,omitempty"`
	LogFormat string `json:"log_format,omitempty"`

	// MongoDB -> MongoDB (from = to = mongodb). The console sends host(s)/port/user/password (the source_* /
	// target_* fields above) plus the options below and the server builds the URI; source_uri / target_uri are
	// still accepted from the API. Either way the URI is a secret: only in the encrypted credential record (SRC_URI / DST_URI).
	SourceURI            string `json:"source_uri,omitempty"`
	TargetURI            string `json:"target_uri,omitempty"`
	SourceAuthDB         string `json:"source_auth_db,omitempty"`
	TargetAuthDB         string `json:"target_auth_db,omitempty"`
	SourceReplicaSet     string `json:"source_replica_set,omitempty"`
	TargetReplicaSet     string `json:"target_replica_set,omitempty"`
	SourceTLS            bool   `json:"source_tls,omitempty"`
	TargetTLS            bool   `json:"target_tls,omitempty"`
	MongoSourceDB        string `json:"source_database,omitempty"`
	MongoTargetDB        string `json:"target_mongo_database,omitempty"`
	MongoSnapshotBatch   int    `json:"snapshot_batch,omitempty"`
	MongoStreamBatch     int    `json:"stream_batch,omitempty"`
	MongoLeaseTTL        string `json:"lease_ttl,omitempty"`
	MongoOverwriteTarget bool   `json:"overwrite_target,omitempty"`
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
	if d := s.settingStr("config_dir"); d != "" {
		return d
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
  dsn_env: STRAND_PIPELINE_SOURCE_DSN
  create_slot: true
  publication: strand_{{.Name | slot}}
{{- end}}
{{- with .SourceSSHTunnel}}
  ssh_tunnel:
{{- if .Profile}}
    profile: {{printf "%q" .Profile}}
{{- else}}
    host: {{quote .Host}}
    user: {{quote .User}}
    {{- if .Port}}
    port: {{.Port}}
    {{- end}}
    {{- if .IdentityFile}}
    identity_file: {{quote .IdentityFile}}
    {{- end}}
{{- end}}
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
  dsn_env: STRAND_PIPELINE_TARGET_DSN
  commit_order: {{.CommitOrder}}
{{- end}}
{{- with .TargetSSHTunnel}}
  ssh_tunnel:
{{- if .Profile}}
    profile: {{printf "%q" .Profile}}
{{- else}}
    host: {{quote .Host}}
    user: {{quote .User}}
    {{- if .Port}}
    port: {{.Port}}
    {{- end}}
    {{- if .IdentityFile}}
    identity_file: {{quote .IdentityFile}}
    {{- end}}
{{- end}}
{{- end}}

tables:
  include: [{{range $i, $e := .Includes}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
{{- if .Excludes}}
  exclude: [{{range $i, $e := .Excludes}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
{{- end}}
{{- if .FullRowKeyless}}
  keyless: full_row
{{- end}}

bootstrap:
{{- if .StartAt}}
  default: start_at
  start_at: {{printf "%q" .StartAt}}
{{- else}}
  default: logical
{{- if or .ChunkRows .Windows .MaxParallel}}
  logical: { {{logical .}} }
{{- end}}
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
	Name            string
	From, To        string
	SourceSSHTunnel *config.SSHTunnelConfig
	TargetSSHTunnel *config.SSHTunnelConfig

	SourceHost        string
	SourcePort        int
	TargetHost        string
	TargetPort        int
	SourceUser        string
	TargetUser        string
	SourcePasswordEnv string
	TargetPasswordEnv string
	Database          string
	TargetDatabase    string
	Includes          []string
	Excludes          []string
	FullRowKeyless    bool
	ServerID          int
	CommitOrder       string
	SpoolDir          string
	Socket            string
	Workers           int
	IOPSSoft          int
	IOPSHard          int
	ChunkRows         int
	MaxParallel       int
	StartAt           string
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
	"slot":    func(s string) string { return strings.ReplaceAll(s, "-", "_") },
	"logical": logicalYAML,
	"quote":   strconv.Quote,
}).Parse(pipelineTmplText))

// logicalYAML: nội dung khối bootstrap.logical (chunk_rows, max_parallel, windows) của pipeline mới.
func logicalYAML(td tmplData) string {
	var parts []string
	if td.ChunkRows > 0 {
		parts = append(parts, fmt.Sprintf("chunk_rows: %d", td.ChunkRows))
	}
	if td.MaxParallel > 0 {
		parts = append(parts, fmt.Sprintf("max_parallel: %d", td.MaxParallel))
	}
	if len(td.Windows) > 0 {
		ws := make([]string, len(td.Windows))
		for i, w := range td.Windows {
			ws[i] = `"` + w + `"`
		}
		parts = append(parts, "windows: ["+strings.Join(ws, ", ")+"]")
	}
	return strings.Join(parts, ", ")
}

// buildNewPipelineYAML sinh chuỗi YAML từ NewPipelineOpts.
func buildNewPipelineYAML(opts NewPipelineOpts, spoolDir, socketPath string) (string, error) {
	if isMongo(opts.From) || isMongo(opts.To) {
		return buildMongoPipelineYAML(opts, spoolDir, socketPath)
	}
	from := strings.ToLower(strings.TrimSpace(opts.From))
	to := strings.ToLower(strings.TrimSpace(opts.To))
	td := tmplData{
		Name:              opts.Name,
		From:              from,
		SourceSSHTunnel:   opts.SourceSSHTunnel,
		TargetSSHTunnel:   opts.TargetSSHTunnel,
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
		FullRowKeyless:    strings.EqualFold(strings.TrimSpace(opts.Keyless), "full_row"),
		ServerID:          opts.ServerID,
		CommitOrder:       opts.CommitOrder,
		SpoolDir:          spoolDir,
		Socket:            socketPath,
		Workers:           opts.Workers,
		IOPSSoft:          opts.IOPSSoft,
		IOPSHard:          opts.IOPSHard,
		ChunkRows:         opts.ChunkRows,
		MaxParallel:       opts.MaxParallel,
		StartAt:           opts.StartAt,
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
		td.SourcePasswordEnv = pipelineSourcePassword
	}
	if td.TargetPasswordEnv == "" {
		td.TargetPasswordEnv = pipelineTargetPassword
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
	mongo := isMongo(opts.From) || isMongo(opts.To)
	switch strings.ToLower(strings.TrimSpace(opts.Keyless)) {
	case "", "off":
	case "full_row":
		if mongo {
			return errors.New("keyless: full-row matching is only available for SQL pipelines")
		}
	default:
		return errors.New("keyless: choose off or full_row")
	}
	for _, endpoint := range []struct {
		name, kind string
		ssh        *config.SSHTunnelConfig
	}{
		{"source", opts.From, opts.SourceSSHTunnel},
		{"target", opts.To, opts.TargetSSHTunnel},
	} {
		if err := endpoint.ssh.Validate(endpoint.kind); err != nil {
			return fmt.Errorf("%s_ssh_host: %w", endpoint.name, err)
		}
	}
	if mongo {
		if err := checkMongoOpts(opts); err != nil {
			return err
		}
	}
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
	if opts.MaxParallel < 0 || opts.MaxParallel > config.MaxSnapshotParallel {
		return fmt.Errorf("max_parallel: %d is not an integer between 1 and %d", opts.MaxParallel, config.MaxSnapshotParallel)
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
	return s.pickMetricsAddrFor("")
}

func (s *Server) pickMetricsAddrFor(pipelineName string) (string, error) {
	used := s.usedMetricsPorts()
	for port := 9464; port < 9464+1000; port++ {
		if other, taken := used[port]; taken && other != pipelineName {
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

func pipelinePGDSN(dsn, host string, port int, user, password, database string) string {
	if dsn != "" {
		return dsn
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if port == 0 {
		port = 5432
	}
	if user == "" {
		user = "strand"
	}
	address := net.JoinHostPort(strings.Trim(host, "[]"), strconv.Itoa(port))
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: address, Path: "/" + database, RawQuery: "sslmode=prefer"}
	return u.String()
}

// createPipeline thực hiện tạo pipeline mới:
// 1. Kiểm tra whitelist tên và trùng lặp
// 2. Lưu YAML và thông tin đăng nhập đã mã hóa vào metadata DB.
// 3. Thêm pipeline vào web.yaml / scan
// 4. Registry.Refresh
// 5. Chạy validate và plan
// 6. Ghi audit log
func (s *Server) createPipeline(ctx context.Context, opts NewPipelineOpts, ip string) (*CreatePipelineResult, error) {
	opts.Name = strings.TrimSpace(opts.Name)
	opts.From, opts.To = strings.ToLower(strings.TrimSpace(opts.From)), strings.ToLower(strings.TrimSpace(opts.To))
	for _, engine := range []*string{&opts.From, &opts.To} {
		if *engine == "pg" || *engine == "postgresql" {
			*engine = "postgres"
		}
	}
	if !ValidName(opts.Name) {
		return nil, BadRequest("invalid pipeline name %q (allowed: letters, digits, _ . - ; up to 63 chars)", opts.Name)
	}

	existing := s.reg.Get(opts.Name)
	if existing != nil {
		if opts.Overwrite && existing.Mode() == ModeSystemd {
			return nil, Conflict("edit the existing systemd pipeline to keep its service credential configuration")
		}
		if s.isRunning(existing) {
			return nil, Conflict("pipeline %q is currently running; stop it first before overwriting", opts.Name)
		}
		if !opts.Overwrite {
			return nil, Conflict("pipeline %q already exists in registry (check 'Overwrite' to update it)", opts.Name)
		}
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

	if !opts.Overwrite {
		if fileExists(yamlPath) {
			return nil, Conflict("config file %s already exists (check 'Overwrite' to update it)", yamlPath)
		}
		if fileExists(envPath) {
			return nil, Conflict("env file %s already exists (check 'Overwrite' to update it)", envPath)
		}
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
	for _, ssh := range []*config.SSHTunnelConfig{opts.SourceSSHTunnel, opts.TargetSSHTunnel} {
		if ssh != nil && ssh.Profile != "" {
			if _, err := s.savedTunnel(ctx, ssh.Profile); err != nil {
				return nil, BadRequest("%s", err.Error())
			}
		}
	}
	// Mỗi pipeline một cổng /metrics riêng; cổng do người dùng đặt không được trùng pipeline khác.
	opts.Metrics = strings.TrimSpace(opts.Metrics)
	if opts.Metrics == "" {
		addr, err := s.pickMetricsAddrFor(opts.Name)
		if err != nil {
			return nil, err
		}
		opts.Metrics = addr
	} else {
		port := metricsPort(opts.Metrics)
		if port <= 0 || port > 65535 {
			return nil, BadRequest("invalid metrics address %q (want host:port)", opts.Metrics)
		}
		if other, taken := s.usedMetricsPorts()[port]; taken && other != opts.Name {
			return nil, Conflict("metrics port %d is already used by pipeline %q", port, other)
		}
	}

	yamlContent, err := buildNewPipelineYAML(opts, spoolDir, socketPath)
	if err != nil {
		return nil, BadRequest("%s", err.Error())
	}

	srcEnv := opts.SourcePasswordEnv
	if srcEnv == "" {
		srcEnv = pipelineSourcePassword
	}
	dstEnv := opts.TargetPasswordEnv
	if dstEnv == "" {
		dstEnv = pipelineTargetPassword
	}
	var envBuf bytes.Buffer
	envBuf.WriteString(fmt.Sprintf("# Environment variables for pipeline %s\n", opts.Name))
	secrets := [][2]string{{srcEnv, opts.SourcePassword}, {dstEnv, opts.TargetPassword}}
	sourceDB := "app"
	if dbs := dataDatabases(opts); len(dbs) > 0 {
		sourceDB = dbs[0]
	}
	if opts.From == "postgres" {
		secrets[0] = [2]string{pipelineSourceDSN, pipelinePGDSN(opts.SourceDSN, opts.SourceHost, opts.SourcePort, opts.SourceUser, opts.SourcePassword, sourceDB)}
	}
	if opts.To == "postgres" {
		database := strings.TrimSpace(opts.TargetPGDatabase)
		if database == "" {
			database = sourceDB
		}
		secrets[1] = [2]string{pipelineTargetDSN, pipelinePGDSN(opts.TargetDSN, opts.TargetHost, opts.TargetPort, opts.TargetUser, opts.TargetPassword, database)}
	}
	if isMongo(opts.From) {
		srcURI, err := mongoURIOf(opts, "source")
		if err != nil {
			return nil, err
		}
		dstURI, err := mongoURIOf(opts, "target")
		if err != nil {
			return nil, err
		}
		secrets = [][2]string{{mongoSrcURIEnv, srcURI}, {mongoDstURIEnv, dstURI}}
	}
	if secrets[0][0] == secrets[1][0] {
		return nil, BadRequest("source and target credentials must use distinct environment variable names")
	}
	for _, kv := range secrets {
		line, err := formatEnvLine(kv[0], kv[1])
		if err != nil {
			return nil, err
		}
		envBuf.WriteString(line)
	}

	sealed, err := s.db.box.Seal(envBuf.Bytes(), opts.Name)
	if err != nil {
		return nil, fmt.Errorf("encrypt pipeline credentials: %w", err)
	}
	row, found, err := s.db.st.GetPipeline(ctx, opts.Name)
	if err != nil {
		return nil, err
	}
	row.Name, row.ConfigPath, row.ConfigYAML = opts.Name, yamlPath, yamlContent
	row.HasConfig, row.EnvEnc, row.EnvPath = true, sealed, ""
	row.MissingSince = time.Time{}
	if !found {
		row.Origin = store.OriginCreated
	}
	if opts.Overwrite {
		if err := s.db.st.UpsertPipeline(ctx, row); err != nil {
			return nil, fmt.Errorf("store pipeline configuration: %w", err)
		}
	} else if created, err := s.db.st.CreatePipeline(ctx, row); err != nil {
		return nil, fmt.Errorf("store pipeline configuration: %w", err)
	} else if !created {
		return nil, Conflict("pipeline %q already exists", opts.Name)
	}
	s.materialize(opts.Name)
	s.reg.Refresh(ctx)

	p := s.reg.Get(opts.Name)
	if p == nil {
		// Tạo đối tượng tạm nếu refresh chưa kịp nhận
		p = &Pipeline{
			Name:         opts.Name,
			ConfigPath:   yamlPath,
			dbEnv:        s.db,
			Origin:       "created",
			profileDBDSN: s.cfg.Database.DSN,
			Cfg: &config.Config{Source: config.SourceConfig{SSHTunnel: opts.SourceSSHTunnel},
				Target: config.TargetConfig{SSHTunnel: opts.TargetSSHTunnel}},
		}
	}

	// Chạy validate (và plan), mỗi lệnh có hạn giờ: nguồn/đích không trả lời (tường lửa nuốt gói) làm lệnh
	// treo nhiều phút, vượt WriteTimeout của server và client nhận phản hồi rỗng dù pipeline đã được tạo.
	vctx, vcancel := context.WithTimeout(ctx, newPipelineCheckTimeout)
	vOut, vCode, vErr := s.runStrand(vctx, p, "validate", "-c", yamlPath)
	if vctx.Err() == context.DeadlineExceeded {
		vOut += fmt.Sprintf("\n[strand web: validate stopped after %s: the source or target did not answer (check host, port and firewall)]", newPipelineCheckTimeout)
	}
	vcancel()
	valExit := 0
	if vCode != nil {
		valExit = *vCode
	} else if vErr != nil {
		valExit = -1
	}
	valOK := vErr == nil && valExit == 0

	// Chạy plan
	pctx, pcancel := context.WithTimeout(ctx, newPipelineCheckTimeout)
	pOut, pCode, pErr := s.runStrand(pctx, p, "plan", "-c", yamlPath)
	if pctx.Err() == context.DeadlineExceeded {
		pOut += fmt.Sprintf("\n[strand web: plan stopped after %s: the source or target did not answer]", newPipelineCheckTimeout)
	}
	pcancel()
	planExit := 0
	if pCode != nil {
		planExit = *pCode
	} else if pErr != nil {
		planExit = -1
	}
	if pErr != nil && pOut == "" {
		pOut = pErr.Error()
	}

	_ = s.audit.Log(AuditEntry{
		IP:       ip,
		Action:   "pipeline.create",
		Pipeline: opts.Name,
		Result:   map[bool]string{true: "ok", false: "validate_failed"}[valOK],
		ExitCode: vCode,
		Detail: map[string]any{
			"config":      yamlPath,
			"credentials": "encrypted metadata database",
			"val_exit":    valExit,
		},
	})

	res := &CreatePipelineResult{
		OK:         valOK,
		Pipeline:   opts.Name,
		ConfigPath: yamlPath,
		Message:    "Pipeline created successfully",
	}
	if !valOK {
		// File vẫn được giữ lại để sửa ở tab Config; ok=false để UI/API không báo thành công giả.
		res.Message = fmt.Sprintf("Pipeline files were created but validation failed (exit %d): fix the pipeline in the Config tab and re-validate before Start", valExit)
		if strings.Contains(vOut, "a logical snapshot of a keyless table") {
			res.Message += ". To keep tables without a key, restore a consistent backup of all included tables on the target, then set bootstrap.default to start_at and bootstrap.start_at to the position recorded in that backup"
		}
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
	opts.Overwrite = f.Get("overwrite") == "true" || f.Get("overwrite") == "1" || f.Get("overwrite") == "on"
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
	opts.StartAt = strings.TrimSpace(f.Get("start_at"))
	opts.SourceURI = f.Get("source_uri")
	opts.TargetURI = f.Get("target_uri")
	opts.SourceAuthDB = strings.TrimSpace(f.Get("source_auth_db"))
	opts.TargetAuthDB = strings.TrimSpace(f.Get("target_auth_db"))
	opts.SourceReplicaSet = strings.TrimSpace(f.Get("source_replica_set"))
	opts.TargetReplicaSet = strings.TrimSpace(f.Get("target_replica_set"))
	opts.SourceTLS = f.Get("source_tls") == "true" || f.Get("source_tls") == "on"
	opts.TargetTLS = f.Get("target_tls") == "true" || f.Get("target_tls") == "on"
	opts.MongoSourceDB = strings.TrimSpace(f.Get("source_database"))
	opts.MongoTargetDB = strings.TrimSpace(f.Get("target_mongo_database"))
	opts.MongoLeaseTTL = strings.TrimSpace(f.Get("lease_ttl"))
	opts.MongoOverwriteTarget = f.Get("overwrite_target") == "true" || f.Get("overwrite_target") == "on"
	opts.Databases = splitList(f.Get("databases"))
	opts.Includes = splitList(f.Get("include"))
	opts.Excludes = splitList(f.Get("exclude"))
	opts.Keyless = f.Get("keyless")
	opts.Windows = splitList(f.Get("windows"))
	for field, dst := range map[string]*int{
		"source_port": &opts.SourcePort, "target_port": &opts.TargetPort, "workers": &opts.Workers,
		"server_id": &opts.ServerID, "iops_soft": &opts.IOPSSoft, "iops_hard": &opts.IOPSHard,
		"chunk_rows": &opts.ChunkRows, "max_parallel": &opts.MaxParallel,
		"snapshot_batch": &opts.MongoSnapshotBatch, "stream_batch": &opts.MongoStreamBatch,
	} {
		n, err := atoiField(r, field)
		if err != nil {
			return opts, err
		}
		*dst = n
	}
	for _, side := range []struct {
		name   string
		tunnel **config.SSHTunnelConfig
	}{
		{"source", &opts.SourceSSHTunnel},
		{"target", &opts.TargetSSHTunnel},
	} {
		profile := strings.TrimSpace(f.Get(side.name + "_ssh_profile"))
		if profile != "" {
			*side.tunnel = &config.SSHTunnelConfig{Profile: profile}
			continue
		}
		host := strings.TrimSpace(f.Get(side.name + "_ssh_host"))
		user := strings.TrimSpace(f.Get(side.name + "_ssh_user"))
		key := strings.TrimSpace(f.Get(side.name + "_ssh_identity_file"))
		port, err := atoiField(r, side.name+"_ssh_port")
		if err != nil {
			return opts, err
		}
		if host != "" || user != "" || key != "" || port != 0 {
			*side.tunnel = &config.SSHTunnelConfig{Host: host, Port: port, User: user, IdentityFile: key}
		}
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
			"ConfigDir":   s.ConfigDir(),
			"StateDir":    s.cfg.StateDir,
			"SSHProfiles": s.sshProfileViews(r.Context()),
		},
	}
	s.render(w, "new_pipeline", sess, data)
}

// newPipelineCheckTimeout: hạn giờ của validate và của plan khi tạo pipeline (biến để test rút ngắn).
var newPipelineCheckTimeout = 60 * time.Second

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

	// validate + plan có thể mất tới 2 x newPipelineCheckTimeout: nới hạn ghi của riêng request này.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(2*newPipelineCheckTimeout + 30*time.Second))
	res, err := s.createPipeline(r.Context(), opts, s.clientIP(r))
	if err != nil {
		var ae *ActionError
		if errors.As(err, &ae) {
			writeCreateErr(w, ae.Status, ae.Msg)
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type sourceDatabaseRequest struct {
	From      string                  `json:"from"`
	Host      string                  `json:"host"`
	Port      int                     `json:"port"`
	User      string                  `json:"user"`
	Password  string                  `json:"password"`
	SSHTunnel *config.SSHTunnelConfig `json:"ssh_tunnel,omitempty"`
	// MongoDB only (Host may list several host:port).
	AuthDB     string `json:"auth_db"`
	ReplicaSet string `json:"replica_set"`
	TLS        bool   `json:"tls"`
}

// apiSourceDatabases lists databases reachable by the credentials currently entered in the new-pipeline form.
// MySQL/MariaDB run SHOW DATABASES; PostgreSQL and MongoDB use the connection checker (system databases left out).
func (s *Server) apiSourceDatabases(w http.ResponseWriter, r *http.Request, sess *Session) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	var req sourceDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.SSHTunnel != nil {
		if sess == nil || sess.Role != store.RoleAdmin {
			writeErr(w, http.StatusForbidden, "SSH tunnel database discovery requires an administrator")
			return
		}
		if err := req.SSHTunnel.Validate(req.From); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "pipeline.list_source_databases", Result: "started", Detail: map[string]any{
			"engine": req.From, "host": req.Host, "port": req.Port, "ssh_host": req.SSHTunnel.Host,
		}}); err != nil {
			writeErr(w, http.StatusInternalServerError, "audit log unavailable; refusing database discovery")
			return
		}
		host, port, closeTunnel, err := s.openPipelineProbe(r.Context(), req.From, req.Host, req.Port, req.SSHTunnel)
		if err != nil {
			writeErr(w, http.StatusBadGateway, err.Error())
			return
		}
		defer closeTunnel()
		req.Host, req.Port = host, port
	}

	switch req.From {
	case "mysql", "mariadb":
	case "postgres", "mongodb":
		s.sourceDatabasesByCheck(w, r, req)
		return
	default:
		writeErr(w, http.StatusBadRequest, "unsupported source engine")
		return
	}
	if strings.TrimSpace(req.Host) == "" || strings.TrimSpace(req.User) == "" || req.Port < 1 || req.Port > 65535 {
		writeErr(w, http.StatusBadRequest, "source host, user, and valid port are required")
		return
	}
	cfg := mysqldriver.NewConfig()
	cfg.User, cfg.Passwd = req.User, req.Password
	cfg.Net, cfg.Addr = "tcp", net.JoinHostPort(strings.TrimSpace(req.Host), strconv.Itoa(req.Port))
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		writeErr(w, http.StatusBadRequest, "could not configure source connection")
		return
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		writeErr(w, http.StatusBadGateway, "source connection failed or user cannot list databases")
		return
	}
	defer rows.Close()
	databases := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			writeErr(w, http.StatusBadGateway, "failed to read source database list")
			return
		}
		switch strings.ToLower(name) {
		case "information_schema", "performance_schema", "mysql", "sys":
			continue
		}
		databases = append(databases, name)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusBadGateway, "failed while reading source database list")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"databases": databases})
}

// sourceDatabasesByCheck lists the databases of a PostgreSQL or MongoDB source with the connection checker.
func (s *Server) sourceDatabasesByCheck(w http.ResponseWriter, r *http.Request, req sourceDatabaseRequest) {
	t := conncheck.Target{Engine: req.From, Host: strings.TrimSpace(req.Host), Port: req.Port, User: strings.TrimSpace(req.User), Password: req.Password}
	if req.From == "mongodb" {
		uri, err := buildMongoURI("source", mongoEndpoint{Hosts: req.Host, Port: req.Port, User: req.User, Password: req.Password,
			AuthDB: req.AuthDB, ReplicaSet: req.ReplicaSet, TLS: req.TLS})
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		t.URI = uri
	} else if t.Host == "" || t.User == "" || req.Port < 1 || req.Port > 65535 {
		writeErr(w, http.StatusBadRequest, "source host, user, and valid port are required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	res := s.connChecker().Check(ctx, t)
	if !res.OK {
		msg := "source connection failed"
		if res.Error != "" {
			msg += ": " + res.Error
		}
		writeErr(w, http.StatusBadGateway, msg)
		return
	}
	dbs := make([]string, 0, len(res.Databases))
	for _, d := range res.Databases {
		if req.From == "postgres" && d == "postgres" {
			continue
		}
		dbs = append(dbs, d)
	}
	writeJSON(w, http.StatusOK, map[string]any{"databases": dbs})
}
