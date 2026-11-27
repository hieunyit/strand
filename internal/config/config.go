package config

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"strand/internal/exitcode"
)

// Config mô tả đầy đủ tệp cấu hình pipeline Strand theo spec/02-specs.md §10
type Config struct {
	Version       int                 `yaml:"version" json:"version"`
	Name          string              `yaml:"name" json:"name"`
	Source        SourceConfig        `yaml:"source" json:"source"`
	Target        TargetConfig        `yaml:"target" json:"target"`
	Tables        TablesConfig        `yaml:"tables" json:"tables"`
	Bootstrap     BootstrapConfig     `yaml:"bootstrap" json:"bootstrap"`
	Schema        SchemaConfig        `yaml:"schema" json:"schema"`
	Checkpoint    CheckpointConfig    `yaml:"checkpoint" json:"checkpoint"`
	Resources     ResourcesConfig     `yaml:"resources" json:"resources"`
	Governor      GovernorConfig      `yaml:"governor" json:"governor"`
	Verify        VerifyConfig        `yaml:"verify" json:"verify"`
	Observability ObservabilityConfig `yaml:"observability" json:"observability"`
	// MongoDB: tinh chỉnh của pipeline kind mongodb -> mongodb (bỏ qua với kind khác).
	MongoDB MongoDBConfig `yaml:"mongodb,omitempty" json:"mongodb,omitempty"`
}

type SourceConfig struct {
	Kind      string           `yaml:"kind" json:"kind"` // mysql | postgres | mongodb
	DSN       string           `yaml:"dsn" json:"dsn"`
	DSNEnv    string           `yaml:"dsn_env,omitempty" json:"dsn_env,omitempty"`
	SSHTunnel *SSHTunnelConfig `yaml:"ssh_tunnel,omitempty" json:"ssh_tunnel,omitempty"`
	// URI: chuỗi kết nối MongoDB (kind mongodb), thường "${SRC_URI}" từ .env; thay cho dsn.
	URI      string `yaml:"uri,omitempty" json:"uri,omitempty"`
	ServerID uint32 `yaml:"server_id" json:"server_id"`
	// URIEnv (kind mongodb): tên biến môi trường chứa URI mongodb:// (thay cho dsn).
	URIEnv string `yaml:"uri_env,omitempty" json:"uri_env,omitempty"`
	// Kết nối dạng tách trường (thay cho dsn): host, port (mặc định 3306), user, password hoặc
	// password_env (tên biến môi trường chứa mật khẩu), database. dsn còn nhận dạng URL mysql://.
	Host        string `yaml:"host,omitempty" json:"host,omitempty"`
	Port        int    `yaml:"port,omitempty" json:"port,omitempty"`
	User        string `yaml:"user,omitempty" json:"user,omitempty"`
	Password    string `yaml:"password,omitempty" json:"password,omitempty"`
	PasswordEnv string `yaml:"password_env,omitempty" json:"password_env,omitempty"`
	Database    string `yaml:"database,omitempty" json:"database,omitempty"`
	// Databases (chỉ kind mongodb): nhiều database nguồn trong một pipeline, thay cho database.
	Databases []string `yaml:"databases,omitempty" json:"databases,omitempty"`

	ReadFrom string `yaml:"read_from" json:"read_from"` // primary | replica | standby (PG16+, chưa triển khai)

	// Chỉ dùng khi kind = postgres (logical replication pgoutput, 02 §3, §8):
	//   - slot: tên replication slot logic (a-z, 0-9, _), mặc định "strand_<name>" đã chuẩn hóa;
	//   - publication: tên publication do DBA tạo trước, mặc định "strand_<name>";
	//   - create_slot: tạo slot khi chưa có (mặc định false: slot thiếu → log_gap, exit 5);
	//   - failover_slot: tạo slot FAILOVER (PG17+) khi create_slot;
	//   - sequence_sync_interval: Δt_sync — chu kỳ đồng bộ last_value của sequence độc lập vào
	//     _strand_promote khi sync (02 §8), mặc định 5s như `promote --sync-interval`.
	Slot                 string `yaml:"slot,omitempty" json:"slot,omitempty"`
	Publication          string `yaml:"publication,omitempty" json:"publication,omitempty"`
	CreateSlot           bool   `yaml:"create_slot,omitempty" json:"create_slot,omitempty"`
	FailoverSlot         bool   `yaml:"failover_slot,omitempty" json:"failover_slot,omitempty"`
	SequenceSyncInterval string `yaml:"sequence_sync_interval,omitempty" json:"sequence_sync_interval,omitempty"`
	// HeartbeatInterval (chỉ source PG): chu kỳ ghi một logical message vào WAL của database nguồn để slot
	// tiến cả khi các bảng của pipeline không có ghi mà cluster vẫn sinh WAL (database khác, bảng ngoài
	// publication). Mặc định 10s; "off" tắt.
	HeartbeatInterval string `yaml:"heartbeat_interval,omitempty" json:"heartbeat_interval,omitempty"`
}

// HeartbeatOff: source.heartbeat_interval = off (hoặc 0).
func (s SourceConfig) HeartbeatOff() bool {
	v := strings.ToLower(strings.TrimSpace(s.HeartbeatInterval))
	return v == "off" || v == "0" || v == "0s"
}

type TargetConfig struct {
	Kind      string           `yaml:"kind" json:"kind"` // mysql | postgres | mongodb
	DSN       string           `yaml:"dsn" json:"dsn"`
	DSNEnv    string           `yaml:"dsn_env,omitempty" json:"dsn_env,omitempty"`
	SSHTunnel *SSHTunnelConfig `yaml:"ssh_tunnel,omitempty" json:"ssh_tunnel,omitempty"`
	// URI: chuỗi kết nối MongoDB (kind mongodb), thường "${DST_URI}" từ .env; thay cho dsn.
	URI string `yaml:"uri,omitempty" json:"uri,omitempty"`
	// URIEnv (kind mongodb): tên biến môi trường chứa URI mongodb:// (thay cho dsn).
	URIEnv string `yaml:"uri_env,omitempty" json:"uri_env,omitempty"`
	// Kết nối dạng tách trường (thay cho dsn): host, port (mặc định 3306), user, password hoặc
	// password_env (tên biến môi trường chứa mật khẩu), database. dsn còn nhận dạng URL mysql://.
	Host        string `yaml:"host,omitempty" json:"host,omitempty"`
	Port        int    `yaml:"port,omitempty" json:"port,omitempty"`
	User        string `yaml:"user,omitempty" json:"user,omitempty"`
	Password    string `yaml:"password,omitempty" json:"password,omitempty"`
	PasswordEnv string `yaml:"password_env,omitempty" json:"password_env,omitempty"`
	Database    string `yaml:"database,omitempty" json:"database,omitempty"`
	// Databases (chỉ kind mongodb, cùng source.databases): database đích theo từng database nguồn
	// (nguồn -> đích); database không có trong map giữ nguyên tên.
	Databases   map[string]string `yaml:"databases,omitempty" json:"databases,omitempty"`
	WriteMode   string            `yaml:"write_mode" json:"write_mode"`     // upsert | insert
	CommitOrder string            `yaml:"commit_order" json:"commit_order"` // strict | relaxed
	Batch       BatchConfig       `yaml:"batch" json:"batch"`
	// StrictParallel (commit_order strict): auto (mặc định) — source MySQL ghi các batch không chung khóa
	// trên nhiều writer (resources.workers.max) và commit đúng thứ tự; off — một writer.
	StrictParallel string `yaml:"strict_parallel,omitempty" json:"strict_parallel,omitempty"`
}

type BatchConfig struct {
	MaxRows  int    `yaml:"max_rows" json:"max_rows"`
	MaxBytes string `yaml:"max_bytes" json:"max_bytes"`
	MaxWait  string `yaml:"max_wait" json:"max_wait"`
}

type TablesConfig struct {
	Include []string          `yaml:"include" json:"include"`
	Exclude []string          `yaml:"exclude,omitempty" json:"exclude,omitempty"`
	Rename  map[string]string `yaml:"rename,omitempty" json:"rename,omitempty"`
	// Mask: cột -> hàm mask, ví dụ "app.users.phone": sha256 (design §4). Chỉ áp được cho snapshot
	// logic và seed logic (03 §8). Chưa triển khai: nhận key và cảnh báo ở validate.
	Mask map[string]string `yaml:"mask,omitempty" json:"mask,omitempty"`
	// Keyless: "full_row" cho phép bảng KHÔNG có PRIMARY KEY / unique key NOT NULL chạy ở chế độ keyless
	// (C-lite): dữ liệu đầu phải đến từ seed/start_at (không snapshot logic), UPDATE/DELETE khớp ảnh trước theo
	// MỌI cột (NULL-safe) với LIMIT 1. Mặc định tắt: bảng thiếu khóa là missing_pk (exit 3).
	Keyless string `yaml:"keyless,omitempty" json:"keyless,omitempty"`
	// KeylessAllowFloat: cho phép bảng keyless có cột FLOAT/DOUBLE (so bằng có thể không khớp); mặc định từ chối.
	KeylessAllowFloat bool `yaml:"keyless_allow_float,omitempty" json:"keyless_allow_float,omitempty"`
	// ZeroDate (chỉ MySQL → PostgreSQL): cách xử lý ngày zero của MySQL ('0000-00-00', tháng/ngày = 00) mà
	// PostgreSQL không biểu diễn được. reject (mặc định): dừng pipeline, exit 3. set_null: ghi NULL (chỉ
	// cột nullable; cột NOT NULL vẫn bị từ chối). epoch: ghi 1970-01-01 00:00:00. Cột thuộc khóa hàng
	// luôn từ chối. Không dùng chữ "null" để YAML không đọc nhầm thành giá trị rỗng.
	ZeroDate string `yaml:"zero_date,omitempty" json:"zero_date,omitempty"`
	// ZeroDateTables: chính sách riêng cho từng bảng nguồn "schema.table", ghi đè ZeroDate.
	ZeroDateTables map[string]string `yaml:"zero_date_tables,omitempty" json:"zero_date_tables,omitempty"`
}

// Giá trị của tables.zero_date.
const (
	ZeroDateReject  = "reject"
	ZeroDateSetNull = "set_null"
	ZeroDateEpoch   = "epoch"
)

// ZeroDateFor trả chính sách zero date của bảng nguồn schema.table (đã chuẩn hóa; mặc định reject).
func (t TablesConfig) ZeroDateFor(schema, table string) string {
	key := schema + "." + table
	for k, v := range t.ZeroDateTables {
		if strings.EqualFold(strings.TrimSpace(k), key) {
			return normalizeZeroDate(v)
		}
	}
	return normalizeZeroDate(t.ZeroDate)
}

// ZeroDateUsed: có bảng nào không dùng reject.
func (t TablesConfig) ZeroDateUsed() bool {
	if normalizeZeroDate(t.ZeroDate) != ZeroDateReject {
		return true
	}
	for _, v := range t.ZeroDateTables {
		if normalizeZeroDate(v) != ZeroDateReject {
			return true
		}
	}
	return false
}

func normalizeZeroDate(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return ZeroDateReject
	}
	return v
}

func validateZeroDate(cfg *Config) error {
	bad := func(field, v string) error {
		return exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("invalid %s %q (want reject | set_null | epoch)", field, v), nil)
	}
	check := func(field, v string) error {
		switch normalizeZeroDate(v) {
		case ZeroDateReject, ZeroDateSetNull, ZeroDateEpoch:
			return nil
		}
		return bad(field, v)
	}
	if err := check("tables.zero_date", cfg.Tables.ZeroDate); err != nil {
		return err
	}
	for k, v := range cfg.Tables.ZeroDateTables {
		if parts := strings.Split(strings.TrimSpace(k), "."); len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
				fmt.Sprintf("tables.zero_date_tables key %q must be schema.table", k), nil)
		}
		if err := check("tables.zero_date_tables["+k+"]", v); err != nil {
			return err
		}
	}
	if cfg.Tables.ZeroDateUsed() && !(strings.EqualFold(cfg.Source.Kind, "mysql") && strings.EqualFold(cfg.Target.Kind, "postgres")) {
		return exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			"tables.zero_date / zero_date_tables only apply to MySQL → PostgreSQL pipelines (MySQL → MySQL keeps zero dates as they are)", nil)
	}
	return nil
}

// KeylessOn: tables.keyless = full_row.
func (t TablesConfig) KeylessOn() bool {
	return strings.EqualFold(strings.TrimSpace(t.Keyless), "full_row")
}

type BootstrapConfig struct {
	AutoSeed bool                          `yaml:"auto_seed,omitempty" json:"auto_seed,omitempty"`
	Default  string                        `yaml:"default" json:"default"` // logical | seed | start_at
	StartAt  string                        `yaml:"start_at,omitempty" json:"start_at,omitempty"`
	PerTable map[string]BootstrapTableMode `yaml:"per_table,omitempty" json:"per_table,omitempty"`
	CatchUp  CatchUpConfig                 `yaml:"catch_up,omitempty" json:"catch_up,omitempty"`
	Logical  LogicalBootstrapConfig        `yaml:"logical,omitempty" json:"logical,omitempty"`
	// CreateSchema: auto (mặc định) — `strand sync` tạo schema/database và bảng còn thiếu ở target
	// cho các bảng sẽ snapshot logic (và bảng mới CREATE TABLE trong lúc stream); off — không tạo,
	// bảng thiếu là lỗi như trước. Quyết định 17 (questions/DECISIONS-2026-11-02.md).
	CreateSchema string `yaml:"create_schema,omitempty" json:"create_schema,omitempty"`
}

// CreateSchemaAuto: bootstrap.create_schema khác "off" (mặc định auto).
func (b BootstrapConfig) CreateSchemaAuto() bool {
	return !strings.EqualFold(strings.TrimSpace(b.CreateSchema), "off")
}

type BootstrapTableMode struct {
	Mode string      `yaml:"mode" json:"mode"` // seed | logical
	Seed *SeedConfig `yaml:"seed,omitempty" json:"seed,omitempty"`
}

type SeedConfig struct {
	Type string `yaml:"type" json:"type"` // xtrabackup | clone | mydumper | mysqldump | pgbackrest | walg | strand-export | volume-snapshot
	URI  string `yaml:"uri" json:"uri"`
	// Các key dưới đây theo ví dụ đầy đủ ở design §4; chưa triển khai, chỉ nhận và cảnh báo ở validate.
	Credentials   string            `yaml:"credentials,omitempty" json:"credentials,omitempty"`
	EncryptionKey string            `yaml:"encryption_key,omitempty" json:"encryption_key,omitempty"`
	Load          *SeedLoadConfig   `yaml:"load,omitempty" json:"load,omitempty"`
	VerifyAfter   *SeedVerifyConfig `yaml:"verify_after,omitempty" json:"verify_after,omitempty"`
}

// SeedLoadConfig: tham số nạp backup logic (design §4). Chưa triển khai.
type SeedLoadConfig struct {
	Threads      int    `yaml:"threads,omitempty" json:"threads,omitempty"`
	DeferIndexes bool   `yaml:"defer_indexes,omitempty" json:"defer_indexes,omitempty"`
	Bandwidth    string `yaml:"bandwidth,omitempty" json:"bandwidth,omitempty"` // kích thước/giây, ví dụ 200MiB
}

// SeedVerifyConfig: verify sau seed (design §4). Chưa triển khai.
type SeedVerifyConfig struct {
	SampleRanges string `yaml:"sample_ranges,omitempty" json:"sample_ranges,omitempty"` // ví dụ 1%
	DeltaWindow  string `yaml:"delta_window,omitempty" json:"delta_window,omitempty"`   // ví dụ 30m
}

type CatchUpConfig struct {
	BinlogArchive string `yaml:"binlog_archive,omitempty" json:"binlog_archive,omitempty"`
}

// MaxSnapshotParallel: giới hạn trên của bootstrap.logical.max_parallel.
const MaxSnapshotParallel = 32

type LogicalBootstrapConfig struct {
	ChunkRows string `yaml:"chunk_rows,omitempty" json:"chunk_rows,omitempty"` // auto | số dòng
	// MaxParallel: số bảng snapshot logic cùng lúc (1..32; vắng = 1). Chunk của một bảng vẫn tuần tự; ngân
	// sách đọc nguồn (token bucket) dùng chung. Source PostgreSQL: bỏ qua (luôn 1).
	MaxParallel int      `yaml:"max_parallel,omitempty" json:"max_parallel,omitempty"`
	Windows     []string `yaml:"windows,omitempty" json:"windows,omitempty"`
}

type SchemaConfig struct {
	Additive    string `yaml:"additive" json:"additive"`       // auto | pause
	Destructive string `yaml:"destructive" json:"destructive"` // pause | approve
	// OnlineDDL (P3-02): nhận diện gh-ost / pt-osc. Tools nil (không khai báo) = mặc định [gh-ost, pt-osc];
	// `tools: []` tắt nhận diện, giữ hành vi dừng/approve cũ.
	OnlineDDL OnlineDDLConfig `yaml:"online_ddl" json:"online_ddl"`
}

// OnlineDDLConfig: spec design §7 (online_ddl).
type OnlineDDLConfig struct {
	Tools         []string `yaml:"tools" json:"tools"`                   // gh-ost | pt-osc
	ExtraPatterns []string `yaml:"extra_patterns" json:"extra_patterns"` // tên bảng tạm tự đặt, {table} = bảng gốc, vd "_{table}_tmp"
}

type CheckpointConfig struct {
	Store      string `yaml:"store" json:"store"`             // target
	FlushEvery string `yaml:"flush_every" json:"flush_every"` // mặc định 500ms
}

type ResourcesConfig struct {
	Memory  MemoryLimits  `yaml:"memory" json:"memory"`
	Spool   SpoolLimits   `yaml:"spool" json:"spool"`
	Source  IOPSLimits    `yaml:"source" json:"source"`
	Target  IOPSLimits    `yaml:"target" json:"target"`
	Workers WorkersLimits `yaml:"workers" json:"workers"`
	// Network: giới hạn băng thông mạng, ví dụ backup_read khi đọc backup từ S3 (design §4). Chưa triển khai.
	Network NetworkLimits `yaml:"network,omitempty" json:"network,omitempty"`
	// Schedule: đổi limit theo lịch cron (design §4). Chưa triển khai.
	Schedule []ScheduleEntry `yaml:"schedule,omitempty" json:"schedule,omitempty"`
}

// ByteRateLimits: cặp soft/hard theo kích thước/giây, ví dụ { soft: 40MiB, hard: 60MiB }.
type ByteRateLimits struct {
	Soft string `yaml:"soft,omitempty" json:"soft,omitempty"`
	Hard string `yaml:"hard,omitempty" json:"hard,omitempty"`
}

func (b ByteRateLimits) isSet() bool { return b.Soft != "" || b.Hard != "" }

type NetworkLimits struct {
	BackupRead ByteRateLimits `yaml:"backup_read,omitempty" json:"backup_read,omitempty"`
}

// ScheduleEntry: một mốc lịch; ngoài cron là các key ghi đè dạng đường dẫn chấm,
// ví dụ { cron: "0 8 * * 1-5", source.iops.soft: 300 }.
type ScheduleEntry struct {
	Cron      string         `yaml:"cron" json:"cron"`
	Overrides map[string]any `yaml:",inline" json:"overrides,omitempty"`
}

type MemoryLimits struct {
	Soft string `yaml:"soft" json:"soft"`
	Hard string `yaml:"hard" json:"hard"`
}

type SpoolLimits struct {
	Path     string `yaml:"path" json:"path"`
	Soft     string `yaml:"soft" json:"soft"`
	Hard     string `yaml:"hard" json:"hard"`
	Overflow string `yaml:"overflow,omitempty" json:"overflow,omitempty"`
}

type IOPSLimits struct {
	IOPS struct {
		Soft int `yaml:"soft" json:"soft"`
		Hard int `yaml:"hard" json:"hard"`
	} `yaml:"iops" json:"iops"`
	// Bandwidth: giới hạn băng thông đọc/ghi (design §4). Chưa triển khai.
	Bandwidth ByteRateLimits `yaml:"bandwidth,omitempty" json:"bandwidth,omitempty"`
}

type WorkersLimits struct {
	Min int `yaml:"min" json:"min"`
	Max int `yaml:"max" json:"max"`
}

type GovernorConfig struct {
	Algorithm     string               `yaml:"algorithm" json:"algorithm"` // grace-v2 | onoff | static
	Tick          string               `yaml:"tick" json:"tick"`           // 1s
	LagSLO        string               `yaml:"lag_slo" json:"lag_slo"`     // 5s
	Probe         string               `yaml:"probe" json:"probe"`         // on | off
	Breaker       BreakerConfig        `yaml:"breaker" json:"breaker"`
	MetricsSource *MetricsSourceConfig `yaml:"metrics_source,omitempty" json:"metrics_source,omitempty"`
}

type BreakerConfig struct {
	StrandQueryP99 struct {
		MaxRatio float64 `yaml:"max_ratio" json:"max_ratio"`
		// MinLatency: the ratio only counts when the p99 itself reaches this (duration, default 20ms;
		// "0s" = no floor).
		MinLatency string `yaml:"min_latency,omitempty" json:"min_latency,omitempty"`
	} `yaml:"strand_query_p99" json:"strand_query_p99"`
	ReplicaLag struct {
		Max string `yaml:"max" json:"max"`
	} `yaml:"replica_lag" json:"replica_lag"`
}

type MetricsSourceConfig struct {
	Kind   string `yaml:"kind" json:"kind"`
	URL    string `yaml:"url" json:"url"`
	AppP99 struct {
		Query string `yaml:"query" json:"query"`
		Max   string `yaml:"max" json:"max"`
	} `yaml:"app_p99" json:"app_p99"`
}

type VerifyConfig struct {
	Delta string           `yaml:"delta" json:"delta"` // on | off
	Full  FullVerifyConfig `yaml:"full" json:"full"`
	// PostSeedWindow: cửa sổ CDC đầu tiên sau seed/--start-at mà mọi range xuất hiện trong đó
	// phải verify 0 lệch trước khi chuyển sang synced (02 §9). Mặc định 30m.
	PostSeedWindow string `yaml:"post_seed_window,omitempty" json:"post_seed_window,omitempty"`
}

type FullVerifyConfig struct {
	Cron string `yaml:"cron" json:"cron"`
	Rate string `yaml:"rate" json:"rate"`
}

type ObservabilityConfig struct {
	Metrics     string    `yaml:"metrics" json:"metrics"`
	AdminSocket string    `yaml:"admin_socket" json:"admin_socket"`
	Log         LogConfig `yaml:"log" json:"log"`
}

type LogConfig struct {
	Level  string `yaml:"level" json:"level"`   // debug | info | warn | error
	Format string `yaml:"format" json:"format"` // json | text
}

// MaxUnixSocketPath là độ dài tối đa (byte) của đường dẫn Unix socket: sun_path dài 108 byte trên Linux
// (104 trên macOS/BSD), gồm cả byte NUL kết thúc.
func MaxUnixSocketPath() int {
	switch runtime.GOOS {
	case "linux":
		return 107
	default:
		return 103
	}
}

// CheckAdminSocketPath từ chối observability.admin_socket quá dài: nếu không, `sync` chỉ báo lỗi khó hiểu
// "listen unix ...: bind: invalid argument" lúc khởi động.
func CheckAdminSocketPath(path string) error {
	if max := MaxUnixSocketPath(); len(path) > max {
		return exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("observability.admin_socket is too long: %d bytes (Unix socket path limit is %d); use a shorter path, e.g. /run/strand/<name>.sock", len(path), max), nil)
	}
	return nil
}

// LoadFile đọc và giải mã file YAML strict theo quy định (từ chối key lạ với exit 2)
func LoadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, exitcode.New("config_read_error", exitcode.InvalidSyntax, fmt.Sprintf("cannot read config file: %s", path), err)
	}

	// Thay thế biến môi trường dạng ${VAR} hoặc $VAR
	expanded := os.ExpandEnv(string(data))

	return LoadYAMLStrict([]byte(expanded))
}

// LoadYAMLStrict giải mã YAML nghiêm ngặt, từ chối mọi trường không xác định (exit code 2)
func LoadYAMLStrict(data []byte) (*Config, error) {
	return loadYAMLStrict(data, nil)
}

// LoadYAMLStrictWithEnv expands environment references using per-pipeline values
// first and the process environment as fallback. It lets database-managed pipelines
// resolve encrypted secrets without mutating process-global environment variables.
func LoadYAMLStrictWithEnv(data []byte, env map[string]string) (*Config, error) {
	expanded := os.Expand(string(data), func(key string) string {
		if value, ok := env[key]; ok {
			return value
		}
		return os.Getenv(key)
	})
	return loadYAMLStrict([]byte(expanded), env)
}

func loadYAMLStrict(data []byte, secretEnv map[string]string) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // Bắt buộc: từ chối key lạ

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax, fmt.Sprintf("invalid config syntax: %v", err), err)
	}

	// 02 §10: YAML, rồi biến môi trường STRAND_*, rồi flag. Ghi đè env trước khi kiểm tra.
	if err := applyProcessEnv(&cfg); err != nil {
		return nil, err
	}

	// DSN dạng URL (mysql://) hoặc tách trường (host/port/user/...) → DSN của driver.
	if err := resolveConnectionsWithEnv(&cfg, secretEnv); err != nil {
		return nil, err
	}

	// Kiểm tra các trường bắt buộc cơ bản
	if cfg.Name == "" {
		return nil, exitcode.New("missing_name", exitcode.InvalidSyntax, "missing required field: name", nil)
	}
	if err := CheckAdminSocketPath(cfg.Observability.AdminSocket); err != nil {
		return nil, err
	}
	if cfg.Source.Kind == "" || cfg.Source.DSN == "" {
		return nil, exitcode.New("missing_source", exitcode.InvalidSyntax, "missing required source configuration", nil)
	}
	if cfg.Target.Kind == "" || cfg.Target.DSN == "" {
		return nil, exitcode.New("missing_target", exitcode.InvalidSyntax, "missing required target configuration", nil)
	}
	if err := cfg.Source.SSHTunnel.Validate(cfg.Source.Kind); err != nil {
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "source.ssh_tunnel: "+err.Error(), err)
	}
	if err := cfg.Target.SSHTunnel.Validate(cfg.Target.Kind); err != nil {
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "target.ssh_tunnel: "+err.Error(), err)
	}
	if cfg.IsMongoDB() {
		if err := validateMongoDB(&cfg); err != nil {
			return nil, err
		}
	} else if len(cfg.Source.Databases) > 0 || len(cfg.Target.Databases) > 0 {
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax, "source.databases / target.databases are only used by MongoDB pipelines; select MySQL or PostgreSQL tables with tables.include", nil)
	}

	switch strings.ToLower(strings.TrimSpace(cfg.Tables.Keyless)) {
	case "", "off", "full_row":
	default:
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("invalid tables.keyless %q (want off | full_row)", cfg.Tables.Keyless), nil)
	}
	if err := validateZeroDate(&cfg); err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(cfg.Target.StrictParallel)) {
	case "", "auto", "off":
	default:
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("invalid target.strict_parallel %q (want auto | off)", cfg.Target.StrictParallel), nil)
	}
	if v := strings.TrimSpace(cfg.Source.HeartbeatInterval); v != "" && !cfg.Source.HeartbeatOff() {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
				fmt.Sprintf("invalid source.heartbeat_interval %q (want a duration of at least 1s, or off)", v), nil)
		}
		if !strings.EqualFold(cfg.Source.Kind, "postgres") {
			return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
				"source.heartbeat_interval only applies to a PostgreSQL source", nil)
		}
	}
	switch strings.ToLower(strings.TrimSpace(cfg.Bootstrap.CreateSchema)) {
	case "":
		cfg.Bootstrap.CreateSchema = "auto"
	case "auto", "off":
		cfg.Bootstrap.CreateSchema = strings.ToLower(strings.TrimSpace(cfg.Bootstrap.CreateSchema))
	default:
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("invalid bootstrap.create_schema %q (want auto | off)", cfg.Bootstrap.CreateSchema), nil)
	}

	// P3-01: schema.additive mặc định auto (spec 02 §6), schema.destructive mặc định pause.
	switch v := strings.ToLower(strings.TrimSpace(cfg.Schema.Additive)); v {
	case "":
		cfg.Schema.Additive = "auto"
	case "auto", "pause":
		cfg.Schema.Additive = v
	default:
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("invalid schema.additive %q (want auto | pause)", cfg.Schema.Additive), nil)
	}
	if cfg.Schema.OnlineDDL.Tools == nil {
		cfg.Schema.OnlineDDL.Tools = []string{"gh-ost", "pt-osc"}
	}
	for i, tool := range cfg.Schema.OnlineDDL.Tools {
		tool = strings.ToLower(strings.TrimSpace(tool))
		if tool != "gh-ost" && tool != "pt-osc" {
			return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
				fmt.Sprintf("invalid schema.online_ddl.tools entry %q (want gh-ost | pt-osc)", tool), nil)
		}
		cfg.Schema.OnlineDDL.Tools[i] = tool
	}
	for _, pat := range cfg.Schema.OnlineDDL.ExtraPatterns {
		if !strings.Contains(pat, "{table}") || strings.Count(pat, "{table}") != 1 {
			return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
				fmt.Sprintf("invalid schema.online_ddl.extra_patterns entry %q (must contain {table} exactly once)", pat), nil)
		}
	}
	switch v := strings.ToLower(strings.TrimSpace(cfg.Schema.Destructive)); v {
	case "":
		cfg.Schema.Destructive = "pause"
	case "pause", "approve":
		cfg.Schema.Destructive = v
	default:
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("invalid schema.destructive %q (want pause | approve)", cfg.Schema.Destructive), nil)
	}

	for i, inc := range cfg.Tables.Include {
		cfg.Tables.Include[i] = strings.TrimSpace(inc)
	}
	for i, exc := range cfg.Tables.Exclude {
		cfg.Tables.Exclude[i] = strings.TrimSpace(exc)
	}
	if len(cfg.Tables.Rename) > 0 {
		cleaned := make(map[string]string, len(cfg.Tables.Rename))
		for k, v := range cfg.Tables.Rename {
			cleaned[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
		cfg.Tables.Rename = cleaned
	}

	if err := ValidateRename(cfg.Tables.Rename, TargetFoldsTableCase(cfg.Target.Kind)); err != nil {
		return nil, err
	}

	if cfg.Bootstrap.AutoSeed {
		b := cfg.Bootstrap
		if !strings.EqualFold(cfg.Source.Kind, "mysql") || !strings.EqualFold(cfg.Target.Kind, "mysql") ||
			(b.Default != "" && !strings.EqualFold(b.Default, "logical")) || b.StartAt != "" || len(b.PerTable) != 0 ||
			!cfg.Tables.KeylessOn() || len(cfg.Tables.Mask) != 0 {
			return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
				"bootstrap.auto_seed requires a MySQL-protocol source and MySQL target, tables.keyless=full_row and logical bootstrap without start_at, per_table or column masks", nil)
		}
	}

	// bootstrap.logical.max_parallel: số bảng snapshot đồng thời (0/vắng = 1, hành vi tuần tự cũ).
	if n := cfg.Bootstrap.Logical.MaxParallel; n < 0 || n > MaxSnapshotParallel {
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax,
			fmt.Sprintf("invalid bootstrap.logical.max_parallel %d (want 1..%d)", n, MaxSnapshotParallel), nil)
	}

	// Thiết lập giá trị mặc định theo spec nếu chưa có
	if cfg.Checkpoint.FlushEvery == "" {
		cfg.Checkpoint.FlushEvery = "500ms"
	}
	if cfg.Governor.Tick == "" {
		cfg.Governor.Tick = "1s"
	}
	if cfg.Governor.LagSLO == "" {
		cfg.Governor.LagSLO = "5s"
	}

	return &cfg, nil
}

// ValidateRename kiểm tables.rename là đơn ánh (review R10): hai bảng nguồn đổi tên về cùng một bảng
// đích sẽ ghi chung một không gian PK ở target, row của bảng này đè row của bảng kia. Từ chối sớm khi
// nạp cấu hình (exit 2). Va chạm với bảng nguồn KHÔNG rename chỉ phát hiện được khi đã có danh sách
// bảng thật (pipeline kiểm lại sau khi đọc source). foldCase: so tên đích không phân biệt hoa thường
// (target MySQL, review G3 — xem TargetFoldsTableCase).
func ValidateRename(rename map[string]string, foldCase bool) error {
	froms := make([]string, 0, len(rename))
	for from := range rename {
		froms = append(froms, from)
	}
	sort.Strings(froms) // thông báo lỗi ổn định
	seen := make(map[string]string, len(rename))
	for _, from := range froms {
		to := rename[from]
		key := to
		if foldCase {
			key = strings.ToLower(to)
		}
		if prev, ok := seen[key]; ok {
			return exitcode.New("invalid_rename", exitcode.InvalidSyntax,
				fmt.Sprintf("tables.rename maps both %q and %q to %q: each target table must come from exactly one source table", prev, from, to), nil)
		}
		seen[key] = from
	}
	return nil
}

// TargetFoldsTableCase: target kind có thể coi tên bảng khác hoa thường là cùng một bảng (review G3).
// MySQL với lower_case_table_names=1/2 (mặc định trên Windows/macOS) thì a.T và a.t trùng nhau; không
// biết giá trị biến lúc nạp cấu hình nên thận trọng coi mọi target không phải PG là gộp hoa thường.
// PG: strand luôn quote định danh nên tên phân biệt hoa thường.
func TargetFoldsTableCase(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "postgres", "postgresql", "pg":
		return false
	}
	return true
}

// LoadFileForPaths đọc file cấu hình chỉ để lấy tên pipeline và đường dẫn (spool, socket): kiểm cú pháp
// strict nhưng không dựng DSN, nên không cần biến môi trường mật khẩu (strand stop / status).
func LoadFileForPaths(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, exitcode.New("config_read_error", exitcode.InvalidSyntax, fmt.Sprintf("cannot read config file: %s", path), err)
	}
	return LoadBytesForPaths(data)
}

// LoadBytesForPaths như LoadFileForPaths nhưng từ nội dung đã có (bản cấu hình lưu trong kho metadata của web).
func LoadBytesForPaths(data []byte) (*Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader([]byte(os.ExpandEnv(string(data)))))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, exitcode.New("invalid_syntax", exitcode.InvalidSyntax, fmt.Sprintf("invalid config syntax: %v", err), err)
	}
	if cfg.Name == "" {
		return nil, exitcode.New("missing_name", exitcode.InvalidSyntax, "missing required field: name", nil)
	}
	return &cfg, nil
}

// KindMongoDB là source.kind/target.kind của pipeline MongoDB -> MongoDB.
const KindMongoDB = "mongodb"
