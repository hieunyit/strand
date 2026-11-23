package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"strand/internal/exitcode"
)

// MongoDBConfig is the `mongodb:` section of a MongoDB → MongoDB pipeline
// (source.kind = target.kind = mongodb); zero values mean engine defaults.
type MongoDBConfig struct {
	// SnapshotBatch: documents per snapshot write transaction (default 1000; falls back
	// to bootstrap.logical.chunk_rows).
	SnapshotBatch int `yaml:"snapshot_batch,omitempty" json:"snapshot_batch,omitempty"`
	// StreamBatch: change events per apply transaction (default 2000; falls back to
	// target.batch.max_rows).
	StreamBatch int `yaml:"stream_batch,omitempty" json:"stream_batch,omitempty"`
	// OverwriteTarget: the first snapshot may drop and recreate existing selected target
	// collections (default false: refuse).
	OverwriteTarget bool `yaml:"overwrite_target,omitempty" json:"overwrite_target,omitempty"`
	// Updates: auto (default) | delta | lookup — how update events are applied.
	Updates string `yaml:"updates,omitempty" json:"updates,omitempty"`
	// Indexes: off (default) | replicate — create the source's indexes and collection
	// options (validators, collation, TTL, ...) on the target and keep them in sync.
	Indexes string `yaml:"indexes,omitempty" json:"indexes,omitempty"`
	// LeaseTTL: single-writer lease TTL on the target clock (default 45s).
	LeaseTTL string `yaml:"lease_ttl,omitempty" json:"lease_ttl,omitempty"`
}

// IsMongoKind: kind is MongoDB.
func IsMongoKind(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "mongodb", "mongo":
		return true
	}
	return false
}

// IsMongoDB: the pipeline replicates MongoDB to MongoDB.
func (c *Config) IsMongoDB() bool { return IsMongoKind(c.Source.Kind) || IsMongoKind(c.Target.Kind) }

// resolveMongoConn turns uri | dsn | uri_env (+ user, password | password_env) into the
// URI in dsn. env (per-pipeline secrets) is consulted before the process environment.
func resolveMongoConn(name string, kind, dsn, uri, uriEnv, host, user, password, passwordEnv *string, port *int, env map[string]string) error {
	*kind = KindMongoDB
	lookup := func(k string) (string, bool) {
		if v, ok := env[k]; ok {
			return v, true
		}
		return os.LookupEnv(k)
	}
	if *host != "" || *port != 0 {
		return fmt.Errorf("%s: host/port are not used for kind mongodb; put every replica set member in the URI (uri, dsn or uri_env)", name)
	}
	set := 0
	for _, v := range []string{*dsn, *uri, *uriEnv} {
		if v != "" {
			set++
		}
	}
	if set != 1 {
		return fmt.Errorf("%s: set exactly one of uri (mongodb:// URI, e.g. \"${SRC_URI}\"), dsn, or uri_env (environment variable holding it)", name)
	}
	if *uri != "" {
		*dsn = *uri
	}
	if *uriEnv != "" {
		v, ok := lookup(*uriEnv)
		if !ok || strings.TrimSpace(v) == "" {
			return fmt.Errorf("%s: uri_env %s is not set in the environment", name, *uriEnv)
		}
		*dsn = strings.TrimSpace(v)
	}
	u, err := url.Parse(*dsn)
	if err != nil || (u.Scheme != "mongodb" && u.Scheme != "mongodb+srv") || u.Host == "" {
		return fmt.Errorf("%s: dsn must be a mongodb:// or mongodb+srv:// URI", name)
	}
	pass := *password
	if *passwordEnv != "" {
		if pass != "" {
			return fmt.Errorf("%s: set either password or password_env, not both", name)
		}
		v, ok := lookup(*passwordEnv)
		if !ok {
			return fmt.Errorf("%s: password_env %s is not set in the environment", name, *passwordEnv)
		}
		pass = v
	}
	if *user != "" || pass != "" {
		if u.User != nil {
			return fmt.Errorf("%s: credentials are both in the URI and in user/password fields", name)
		}
		if *user == "" {
			return fmt.Errorf("%s: password given without user", name)
		}
		u.User = url.UserPassword(*user, pass)
		*dsn = u.String()
	}
	return nil
}

// validateMongoDB checks a MongoDB pipeline configuration (after connections are resolved).
func validateMongoDB(cfg *Config) error {
	bad := func(msg string) error {
		return exitcode.New("invalid_syntax", exitcode.InvalidSyntax, msg, nil)
	}
	if !IsMongoKind(cfg.Source.Kind) || !IsMongoKind(cfg.Target.Kind) {
		return bad(fmt.Sprintf("MongoDB replicates only to MongoDB: source.kind %q and target.kind %q must both be mongodb", cfg.Source.Kind, cfg.Target.Kind))
	}
	if strings.TrimSpace(cfg.Source.Database) == "" {
		return bad("source.database is required for kind mongodb")
	}
	if cfg.Target.Database == "" {
		cfg.Target.Database = cfg.Source.Database
	}
	if len(cfg.Tables.Include) == 0 {
		return bad(`tables.include is required for kind mongodb ("database.collection" globs, e.g. "app.*")`)
	}
	switch {
	case len(cfg.Tables.Rename) > 0:
		return bad("tables.rename is not supported for MongoDB pipelines (collections keep their names; map databases with source.database / target.database)")
	case len(cfg.Tables.Mask) > 0:
		return bad("tables.mask is not supported for MongoDB pipelines")
	case cfg.Tables.Keyless != "" && !strings.EqualFold(cfg.Tables.Keyless, "off"):
		return bad("tables.keyless does not apply to MongoDB (every document has _id)")
	case cfg.Bootstrap.Default != "" && cfg.Bootstrap.Default != "logical":
		return bad("bootstrap.default must be logical for MongoDB pipelines (snapshot + change stream)")
	case cfg.Bootstrap.AutoSeed || cfg.Bootstrap.StartAt != "" || len(cfg.Bootstrap.PerTable) > 0:
		return bad("bootstrap.auto_seed / start_at / per_table are not supported for MongoDB pipelines")
	}
	switch strings.ToLower(cfg.MongoDB.Updates) {
	case "", "auto", "delta", "lookup":
		cfg.MongoDB.Updates = strings.ToLower(cfg.MongoDB.Updates)
	default:
		return bad(fmt.Sprintf("invalid mongodb.updates %q (want auto | delta | lookup)", cfg.MongoDB.Updates))
	}
	switch strings.ToLower(cfg.MongoDB.Indexes) {
	case "", "off":
		cfg.MongoDB.Indexes = "off"
	case "replicate":
		cfg.MongoDB.Indexes = "replicate"
	default:
		return bad(fmt.Sprintf("invalid mongodb.indexes %q (want off | replicate)", cfg.MongoDB.Indexes))
	}
	if cfg.MongoDB.LeaseTTL != "" {
		d, err := time.ParseDuration(cfg.MongoDB.LeaseTTL)
		if err != nil || d < 3*time.Second {
			return bad(fmt.Sprintf("invalid mongodb.lease_ttl %q (duration >= 3s)", cfg.MongoDB.LeaseTTL))
		}
	}
	if cs := cfg.Bootstrap.Logical.ChunkRows; cs != "" && cs != "auto" {
		var n int
		if _, err := fmt.Sscanf(cs, "%d", &n); err != nil || n <= 0 || n > 100000 {
			return bad(fmt.Sprintf("invalid bootstrap.logical.chunk_rows %q for MongoDB (auto | 1..100000 documents per snapshot batch)", cs))
		}
	}
	return nil
}

// MongoDBIgnoredKeys lists configured keys that have no effect on a MongoDB pipeline.
func (c *Config) MongoDBIgnoredKeys() []string {
	var out []string
	add := func(set bool, key string) {
		if set {
			out = append(out, key)
		}
	}
	add(c.Target.WriteMode != "", "target.write_mode")
	add(c.Target.CommitOrder != "", "target.commit_order")
	add(c.Target.Batch.MaxBytes != "" || c.Target.Batch.MaxWait != "", "target.batch.max_bytes/max_wait")
	add(c.Resources.Spool.Soft != "" || c.Resources.Spool.Hard != "", "resources.spool limits")
	add(c.Resources.Source.IOPS.Soft != 0 || c.Resources.Target.IOPS.Soft != 0 || c.Resources.Source.IOPS.Hard != 0 || c.Resources.Target.IOPS.Hard != 0, "resources.source/target.iops")
	add(c.Governor.Algorithm != "" || c.Governor.MetricsSource != nil, "governor")
	add(c.Verify.Delta != "" || c.Verify.Full.Cron != "", "verify (use strand verify -c)")
	add(c.Source.ReadFrom != "", "source.read_from (use readPreference in the URI)")
	return out
}
