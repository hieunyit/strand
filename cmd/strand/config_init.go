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
	Name, From, To           string
	SourceHost, TargetHost   string
	SourcePort, TargetPort   int
	Databases                []string // MySQL/MariaDB: các database cần chuyển ("*" = tất cả); PG: [database]
	PGSchema                 string   // PG: schema cần chuyển
	Database                 string   // database đầu tiên (ví dụ exclude/rename; tên database PG)
	Includes                 []string
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
  user: strand                # quyền: REPLICATION SLAVE, REPLICATION CLIENT ON *.*; SELECT, LOCK TABLES ON {{.GrantOn}}
  password_env: SRC_PW        # mật khẩu lấy từ biến môi trường SRC_PW
  server_id: 47001            # số duy nhất: khác server_id của nguồn và mọi replica (SHOW SLAVE HOSTS)
{{- else}}
  kind: postgres
  dsn: "postgres://strand:${SRC_PW}@{{.SourceHost}}:{{.SourcePort}}/{{.Database}}?sslmode=prefer"
  create_slot: true           # tạo replication slot strand_<name> nếu chưa có
  publication: strand_{{.Name | slot}}   # DBA tạo trước: CREATE PUBLICATION ... FOR TABLE ...
{{- end}}

target:
{{- if .TargetMySQL}}
  kind: mysql                 # MySQL 8.x
  host: {{.TargetHost}}
  port: {{.TargetPort}}
  user: strand
  password_env: DST_PW
  database: strand            # database chứa checkpoint (_strand_*): CREATE DATABASE strand; trước
  commit_order: relaxed       # relaxed: song song theo khóa (nhanh); strict: đúng thứ tự commit của nguồn
{{- else}}
  kind: postgres              # PostgreSQL 16 - 18
  dsn: "postgres://strand:${DST_PW}@{{.TargetHost}}:{{.TargetPort}}/{{.Database}}?sslmode=prefer"
  commit_order: relaxed
{{- end}}

tables:
  # mẫu glob "schema.table"; nhiều database trong cùng một pipeline (một luồng binlog); "*.*" = mọi database
  # người dùng (mysql, sys, information_schema, performance_schema luôn bị bỏ qua)
  include: [{{range $i, $e := .Includes}}{{if $i}}, {{end}}"{{$e}}"{{end}}]
  # exclude: ["{{.Database}}.log_*"]
  # rename: { "{{.Database}}.orders": "{{.Database}}_new.orders" }

bootstrap:
  default: logical            # snapshot dữ liệu ban đầu trực tiếp từ nguồn (không cần backup)
  # logical: { chunk_rows: 10000, windows: ["22:00-06:00"] }   # chỉ snapshot trong khung giờ

resources:
  spool: { path: {{.StateDir}} }   # thư mục làm việc (state, spill); cần vài GB trống
  # source: { iops: { soft: 600, hard: 1200 } }               # giới hạn tải đọc lên nguồn
  # workers: { max: 8 }

observability:
  admin_socket: {{.Socket}}   # cho strand top / pause / resume
  metrics: "127.0.0.1:9464"   # Prometheus /metrics, /healthz
  log: { level: info, format: json }
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
	f.BoolVar(&o.Systemd, "systemd", false, "Đường dẫn cho chạy bằng systemd (/var/lib/strand/<name>, /run/strand/<name>/admin.sock)")
	f.StringVarP(&out, "output", "o", "", "Ghi ra file (mặc định in ra màn hình)")
	f.BoolVar(&force, "force", false, "Ghi đè file đã có")
	return cmd
}
