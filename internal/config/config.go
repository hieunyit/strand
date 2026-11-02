package config

import (
	"bytes"
	"fmt"
	"os"

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
}

type SourceConfig struct {
	Kind     string `yaml:"kind" json:"kind"` // mysql | postgres
	DSN      string `yaml:"dsn" json:"dsn"`
	ServerID uint32 `yaml:"server_id" json:"server_id"`
	ReadFrom string `yaml:"read_from" json:"read_from"` // primary | replica | standby (PG16+, chưa triển khai)
}

type TargetConfig struct {
	Kind        string      `yaml:"kind" json:"kind"` // mysql | postgres
	DSN         string      `yaml:"dsn" json:"dsn"`
	WriteMode   string      `yaml:"write_mode" json:"write_mode"`     // upsert | insert
	CommitOrder string      `yaml:"commit_order" json:"commit_order"` // strict | relaxed
	Batch       BatchConfig `yaml:"batch" json:"batch"`
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
}

type BootstrapConfig struct {
	Default  string                        `yaml:"default" json:"default"` // logical | seed | start_at
	PerTable map[string]BootstrapTableMode `yaml:"per_table,omitempty" json:"per_table,omitempty"`
	CatchUp  CatchUpConfig                 `yaml:"catch_up,omitempty" json:"catch_up,omitempty"`
	Logical  LogicalBootstrapConfig        `yaml:"logical,omitempty" json:"logical,omitempty"`
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

type LogicalBootstrapConfig struct {
	ChunkRows   string   `yaml:"chunk_rows,omitempty" json:"chunk_rows,omitempty"` // auto | số dòng
	MaxParallel int      `yaml:"max_parallel,omitempty" json:"max_parallel,omitempty"`
	Windows     []string `yaml:"windows,omitempty" json:"windows,omitempty"`
}

type SchemaConfig struct {
	Additive    string `yaml:"additive" json:"additive"`       // auto | pause
	Destructive string `yaml:"destructive" json:"destructive"` // pause | approve
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

	// Kiểm tra các trường bắt buộc cơ bản
	if cfg.Name == "" {
		return nil, exitcode.New("missing_name", exitcode.InvalidSyntax, "missing required field: name", nil)
	}
	if cfg.Source.Kind == "" || cfg.Source.DSN == "" {
		return nil, exitcode.New("missing_source", exitcode.InvalidSyntax, "missing required source configuration", nil)
	}
	if cfg.Target.Kind == "" || cfg.Target.DSN == "" {
		return nil, exitcode.New("missing_target", exitcode.InvalidSyntax, "missing required target configuration", nil)
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
