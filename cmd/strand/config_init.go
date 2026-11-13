package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"
	"text/template"

	"github.com/spf13/cobra"

	"strand/internal/exitcode"
)

// initOpts là tham số sinh file cấu hình mẫu (strand config init).
type initOpts struct {
	Name, From, To         string
	SourceHost, TargetHost string
	SourcePort, TargetPort int
	Databases              []string // MySQL/MariaDB: các database cần chuyển ("*" = tất cả); PG: [database]
	PGSchema               string   // PG: schema cần chuyển
	Database               string   // database đầu tiên (ví dụ exclude/rename; tên database PG)
	Includes               []string
	// Tùy chọn vận hành/hiệu năng (0 / "" = dùng mặc định của Strand, ghi dạng comment).
	SourceUser, TargetUser   string
	ServerID                 int
	Excludes                 []string
	IOPSSoft, IOPSHard       int
	Workers, ChunkRows       int
	CommitOrder              string
	BreakerMaxRatio          float64
	ReplicaLagMax            string
	Windows                  []string
	Metrics, LogFormat       string
	GrantOn                  string
	Systemd                  bool
	StateDir, Socket         string
	SourceMySQL, TargetMySQL bool
	MariaDB                  bool
}

var initNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// initTemplate: file cấu hình mẫu có chú thích. Mật khẩu luôn qua biến môi trường (không ghi vào file).
const initTemplateText = `# Cấu hình Strand: {{.From}} -> {{.To}} (sinh bởi: strand config init)
# Các bước: export SRC_PW=... DST_PW=...; strand validate -c {{.Name}}.yaml; strand plan -c ...; strand sync -c ...
version: 1
name: {{.Name}}               # tên pipeline: duy nhất trên mỗi đích, dùng cho checkpoint

source:
{{- if .SourceMySQL}}
  kind: mysql                 # {{if .MariaDB}}MariaDB cũng khai báo là mysql; Strand tự nhận ra{{else}}MySQL 5.5 - 8.x{{end}}
  host: {{.SourceHost}}
  port: {{.SourcePort}}
  user: {{.SourceUser}}                # quyền: REPLICATION SLAVE, REPLICATION CLIENT ON *.*; SELECT, LOCK TABLES ON {{.GrantOn}}
  password_env: SRC_PW        # mật khẩu lấy từ biến môi trường SRC_PW
  server_id: {{.ServerID}}            # số duy nhất: khác server_id của nguồn và mọi replica (SHOW SLAVE HOSTS)
{{- else}}
  kind: postgres
  dsn: "postgres://{{.SourceUser}}:${SRC_PW}@{{.SourceHost}}:{{.SourcePort}}/{{.Database}}?sslmode=prefer"
  create_slot: true           # tạo replication slot strand_<name> nếu chưa có
  publication: strand_{{.Name | slot}}   # DBA tạo trước: CREATE PUBLICATION ... FOR TABLE ...
{{- end}}

target:
{{- if .TargetMySQL}}
  kind: mysql                 # MySQL 8.x
  host: {{.TargetHost}}
  port: {{.TargetPort}}
  user: {{.TargetUser}}
  password_env: DST_PW
  database: strand            # database chứa checkpoint (_strand_*): CREATE DATABASE strand; trước
  commit_order: {{.CommitOrder}}       # relaxed: song song theo khóa (nhanh); strict: đúng thứ tự commit của nguồn
{{- else}}
  kind: postgres              # PostgreSQL 16 - 18
  dsn: "postgres://{{.TargetUser}}:${DST_PW}@{{.TargetHost}}:{{.TargetPort}}/{{.Database}}?sslmode=prefer"
  commit_order: {{.CommitOrder}}
{{- end}}

tables:
  # mẫu glob "schema.table"; nhiều database trong cùng một pipeline (một luồng binlog); "*.*" = mọi database
  # người dùng (mysql, sys, information_schema, performance_schema luôn bị bỏ qua)
  include: [{{range $i, $e := .Includes}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
{{- if .Excludes}}
  exclude: [{{range $i, $e := .Excludes}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
{{- else}}
  # exclude: ["{{.Database}}.log_*"]
{{- end}}
  # rename: { "{{.Database}}.orders": "{{.Database}}_new.orders" }

bootstrap:
  default: logical            # snapshot dữ liệu ban đầu trực tiếp từ nguồn (không cần backup)
{{- if or .ChunkRows .Windows}}
  logical: { {{if .ChunkRows}}chunk_rows: {{.ChunkRows}}{{if .Windows}}, {{end}}{{end}}{{if .Windows}}windows: [{{range $i, $e := .Windows}}{{if $i}}, {{end}}"{{$e}}"{{end}}]{{end}} }   # chunk_rows cố định tắt tự chỉnh cỡ chunk
{{- else}}
  # logical: { chunk_rows: 20000, windows: ["22:00-06:00"] }   # cỡ chunk cố định; chỉ snapshot trong khung giờ
{{- end}}

resources:
  spool: { path: {{.StateDir}} }   # thư mục làm việc (state, spill); cần vài GB trống
{{- if .IOPSSoft}}
  source: { iops: { soft: {{.IOPSSoft}}, hard: {{.IOPSHard}} } }   # giới hạn tải đọc snapshot lên nguồn
{{- else}}
  # source: { iops: { soft: 600, hard: 1200 } }   # mặc định 600 IOPS (~9 MiB/s): tăng nếu nguồn còn dư tải
{{- end}}
{{- if .Workers}}
  workers: { max: {{.Workers}} }   # số luồng ghi song song lên đích
{{- else}}
  # workers: { max: 8 }   # số luồng ghi song song lên đích
{{- end}}

governor:
  # breaker bảo vệ nguồn: mở thì snapshot tạm dừng (strand top hiện lý do)
{{- if or .BreakerMaxRatio .ReplicaLagMax}}
  breaker: { {{if .BreakerMaxRatio}}strand_query_p99: { max_ratio: {{.BreakerMaxRatio}} }{{if .ReplicaLagMax}}, {{end}}{{end}}{{if .ReplicaLagMax}}replica_lag: { max: {{.ReplicaLagMax}} }{{end}} }
{{- else}}
  # breaker: { strand_query_p99: { max_ratio: 3 }, replica_lag: { max: 10s } }   # max_ratio so với baseline tự học
{{- end}}

observability:
  admin_socket: {{.Socket}}   # cho strand top / pause / resume
  metrics: "{{.Metrics}}"   # Prometheus /metrics, /healthz
  log: { level: info, format: {{.LogFormat}} }
`

var initTemplate = template.Must(template.New("cfg").Funcs(template.FuncMap{
	"slot": func(s string) string { return strings.ReplaceAll(s, "-", "_") },
}).Parse(initTemplateText))

// renderInitConfig sinh nội dung file cấu hình mẫu.
func renderInitConfig(o initOpts) (string, error) {
	o.From, o.To = strings.ToLower(strings.TrimSpace(o.From)), strings.ToLower(strings.TrimSpace(o.To))
	switch o.From {
	case "mysql", "mariadb":
		o.SourceMySQL, o.MariaDB = true, o.From == "mariadb"
	case "postgres", "postgresql", "pg":
		o.From = "postgres"
	default:
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("--from must be mysql, mariadb or postgres, got %q", o.From), nil)
	}
	switch o.To {
	case "mysql":
		o.TargetMySQL = true
	case "postgres", "postgresql", "pg":
		o.To = "postgres"
	default:
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("--to must be mysql or postgres, got %q", o.To), nil)
	}
	if o.MariaDB && !o.TargetMySQL {
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, "a MariaDB source is supported only with a MySQL 8.x target (--to mysql)", nil)
	}
	if !o.SourceMySQL && o.TargetMySQL {
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, "a PostgreSQL source is supported only with a PostgreSQL target (--to postgres)", nil)
	}
	if !initNameRe.MatchString(o.Name) {
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("--name %q: use lowercase letters, digits, '-' or '_'", o.Name), nil)
	}
	var dbs []string
	for _, d := range o.Databases {
		if d = strings.TrimSpace(d); d != "" {
			dbs = append(dbs, d)
		}
	}
	all := false
	for _, d := range dbs {
		if d == "*" || strings.EqualFold(d, "all") {
			all = true
		} else if strings.ContainsAny(d, "\"'` *?.") {
			return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("--database %q is not a plain database name", d), nil)
		}
	}
	switch {
	case len(dbs) == 0:
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, "--database is required (one or more names, or '*' for all)", nil)
	case !o.SourceMySQL:
		// PG: --database là tên database PostgreSQL (một), bảng chọn theo schema.
		if all || len(dbs) != 1 {
			return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, "PostgreSQL source: --database takes exactly one database name; choose schemas with --schema", nil)
		}
		if o.PGSchema == "" || strings.ContainsAny(o.PGSchema, "\"'` *?.") {
			return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("--schema %q is not a plain schema name", o.PGSchema), nil)
		}
		o.Database, o.Includes = dbs[0], []string{o.PGSchema + ".*"}
	case all:
		o.Database, o.Includes, o.GrantOn = "app", []string{"*.*"}, "*.*"
	default:
		o.Database = dbs[0]
		var grants []string
		for _, d := range dbs {
			o.Includes = append(o.Includes, d+".*")
			grants = append(grants, d+".*")
		}
		o.GrantOn = strings.Join(grants, ", ")
	}
	if o.SourceUser == "" {
		o.SourceUser = "strand"
	}
	if o.TargetUser == "" {
		o.TargetUser = "strand"
	}
	if o.ServerID == 0 {
		o.ServerID = 47001
	}
	if o.CommitOrder == "" {
		o.CommitOrder = "relaxed"
	}
	if o.CommitOrder != "relaxed" && o.CommitOrder != "strict" {
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("--commit-order must be relaxed or strict, got %q", o.CommitOrder), nil)
	}
	if o.Metrics == "" {
		o.Metrics = "127.0.0.1:9464"
	}
	if o.LogFormat == "" {
		o.LogFormat = "json"
	}
	if o.LogFormat != "json" && o.LogFormat != "text" {
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("--log-format must be json or text, got %q", o.LogFormat), nil)
	}
	if o.IOPSSoft < 0 || o.IOPSHard < 0 || o.Workers < 0 || o.ChunkRows < 0 || o.BreakerMaxRatio < 0 {
		return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, "numeric options must not be negative", nil)
	}
	if o.IOPSHard > 0 && o.IOPSSoft == 0 {
		o.IOPSSoft = o.IOPSHard / 2
	}
	if o.IOPSSoft > 0 && o.IOPSHard < o.IOPSSoft {
		o.IOPSHard = 2 * o.IOPSSoft
	}
	for _, v := range append(append([]string{}, o.Excludes...), o.Windows...) {
		if strings.ContainsAny(v, "\"\n") {
			return "", exitcode.New("invalid_flag", exitcode.InvalidSyntax, fmt.Sprintf("invalid value %q", v), nil)
		}
	}
	defPort := func(mysql bool) int {
		if mysql {
			return 3306
		}
		return 5432
	}
	if o.SourcePort == 0 {
		o.SourcePort = defPort(o.SourceMySQL)
	}
	if o.TargetPort == 0 {
		o.TargetPort = defPort(o.TargetMySQL)
	}
	if o.Systemd {
		o.StateDir, o.Socket = "/var/lib/strand/"+o.Name, "/run/strand/"+o.Name+"/admin.sock"
	} else {
		o.StateDir, o.Socket = "./state", "./run/admin.sock"
	}
	var b bytes.Buffer
	if err := initTemplate.Execute(&b, o); err != nil {
		return "", err
	}
	return b.String(), nil
}

func newConfigInitCmd() *cobra.Command {
	var o initOpts
	var out string
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Sinh file cấu hình mẫu có chú thích (vd: strand config init --from mariadb --to mysql -o app.yaml)",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := renderInitConfig(o)
			if err != nil {
				return err
			}
			if out == "" || out == "-" {
				fmt.Fprint(cmd.OutOrStdout(), s)
				return nil
			}
			flag := os.O_WRONLY | os.O_CREATE | os.O_EXCL
			if force {
				flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
			}
			f, err := os.OpenFile(out, flag, 0o600)
			if err != nil {
				if os.IsExist(err) {
					return exitcode.New("file_exists", exitcode.InvalidSyntax, fmt.Sprintf("%s already exists (use --force to overwrite)", out), err)
				}
				return err
			}
			if _, err := f.WriteString(s); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "wrote %s; edit hosts/tables, then: export SRC_PW=... DST_PW=...; strand validate -c %s\n", out, out)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.Name, "name", "app-migration", "Tên pipeline")
	f.StringVar(&o.From, "from", "mysql", "Nguồn: mysql | mariadb | postgres")
	f.StringVar(&o.To, "to", "mysql", "Đích: mysql | postgres")
	f.StringVar(&o.SourceHost, "source-host", "10.0.0.11", "Host nguồn")
	f.IntVar(&o.SourcePort, "source-port", 0, "Cổng nguồn (mặc định 3306 / 5432)")
	f.StringVar(&o.TargetHost, "target-host", "10.0.0.21", "Host đích")
	f.IntVar(&o.TargetPort, "target-port", 0, "Cổng đích (mặc định 3306 / 5432)")
	f.StringSliceVar(&o.Databases, "database", []string{"app"}, "Database cần chuyển: lặp lại hoặc phân cách bằng dấu phẩy (app,shop,crm); '*' = mọi database người dùng. Nguồn PostgreSQL: tên database")
	f.StringVar(&o.PGSchema, "schema", "public", "Nguồn PostgreSQL: schema cần chuyển")
	f.StringVar(&o.SourceUser, "source-user", "strand", "User Strand trên nguồn")
	f.StringVar(&o.TargetUser, "target-user", "strand", "User Strand trên đích")
	f.IntVar(&o.ServerID, "server-id", 47001, "Nguồn MySQL/MariaDB: server_id của Strand (khác mọi server/replica)")
	f.StringSliceVar(&o.Excludes, "exclude", nil, "Bảng loại ra (mẫu glob schema.table, lặp lại hoặc phân cách bằng dấu phẩy)")
	f.IntVar(&o.IOPSSoft, "iops-soft", 0, "Giới hạn mềm tải đọc snapshot lên nguồn (IOPS; mặc định Strand 600)")
	f.IntVar(&o.IOPSHard, "iops-hard", 0, "Giới hạn cứng tải đọc (mặc định 2 × --iops-soft)")
	f.IntVar(&o.Workers, "workers", 0, "Số luồng ghi song song lên đích (mặc định Strand 8)")
	f.IntVar(&o.ChunkRows, "chunk-rows", 0, "Cỡ chunk snapshot cố định (mặc định: tự chỉnh)")
	f.StringSliceVar(&o.Windows, "snapshot-window", nil, "Khung giờ được snapshot, vd 22:00-06:00 (lặp lại được)")
	f.StringVar(&o.CommitOrder, "commit-order", "relaxed", "relaxed | strict")
	f.Float64Var(&o.BreakerMaxRatio, "breaker-max-ratio", 0, "Breaker: p99 câu kiểm tra nguồn / baseline tự học (mặc định 3)")
	f.StringVar(&o.ReplicaLagMax, "replica-lag-max", "", "Breaker: độ trễ replica tối đa, vd 30s (mặc định 10s)")
	f.StringVar(&o.Metrics, "metrics", "127.0.0.1:9464", "Địa chỉ /metrics, /healthz")
	f.StringVar(&o.LogFormat, "log-format", "json", "json | text")
	f.BoolVar(&o.Systemd, "systemd", false, "Đường dẫn cho chạy bằng systemd (/var/lib/strand/<name>, /run/strand/<name>/admin.sock)")
	f.StringVarP(&out, "output", "o", "", "Ghi ra file (mặc định in ra màn hình)")
	f.BoolVar(&force, "force", false, "Ghi đè file đã có")
	return cmd
}
