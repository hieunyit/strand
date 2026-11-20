package web

import (
	"fmt"
	"strings"
	"time"

	"strand/internal/config"
)

// Pipeline kind MongoDB -> MongoDB (form tạo pipeline). Khóa cấu hình: source/target.kind mongodb, uri (bí
// mật, "${SRC_URI}"/"${DST_URI}" từ .env mã hóa), database; tables.include/exclude dạng "db.collection";
// mục mongodb: snapshot_batch, stream_batch, lease_ttl, overwrite_target (internal/config.MongoDBConfig).

const (
	mongoSrcURIEnv = "SRC_URI"
	mongoDstURIEnv = "DST_URI"
)

func isMongo(kind string) bool { return strings.EqualFold(strings.TrimSpace(kind), config.KindMongoDB) }

// checkMongoOpts kiểm đầu vào của cặp MongoDB (gọi từ checkOpts khi một phía là mongodb).
func checkMongoOpts(opts NewPipelineOpts) error {
	if !isMongo(opts.From) || !isMongo(opts.To) {
		return fmt.Errorf("MongoDB can only be copied to MongoDB (source and target must both be mongodb)")
	}
	for field, uri := range map[string]string{"source_uri": opts.SourceURI, "target_uri": opts.TargetURI} {
		uri = strings.TrimSpace(uri)
		if uri == "" {
			return fmt.Errorf("%s: required (mongodb://... or mongodb+srv://...)", field)
		}
		if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
			return fmt.Errorf("%s: must start with mongodb:// or mongodb+srv://", field)
		}
	}
	for field, v := range map[string]string{"source_database": opts.MongoSourceDB, "target_database": opts.MongoTargetDB} {
		v = strings.TrimSpace(v)
		if v == "" {
			return fmt.Errorf("%s: required for a MongoDB pipeline", field)
		}
		if strings.ContainsAny(v, "/\\. \"$\x00") {
			return fmt.Errorf("%s %q is not a valid MongoDB database name", field, v)
		}
	}
	for field, n := range map[string]int{"snapshot_batch": opts.MongoSnapshotBatch, "stream_batch": opts.MongoStreamBatch} {
		if n < 0 || n > 100000 {
			return fmt.Errorf("%s: %d is not an integer between 1 and 100000", field, n)
		}
	}
	if v := strings.TrimSpace(opts.MongoLeaseTTL); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 5*time.Second || d > time.Hour {
			return fmt.Errorf("lease_ttl: %q is not a duration between 5s and 1h", v)
		}
	}
	for _, p := range append(append([]string{}, opts.Includes...), opts.Excludes...) {
		if !strings.Contains(p, ".") {
			return fmt.Errorf("include/exclude pattern %q: use database.collection (for example %s.orders or %s.*)", p, opts.MongoSourceDB, opts.MongoSourceDB)
		}
	}
	return nil
}

// buildMongoPipelineYAML sinh cấu hình pipeline MongoDB. URI không bao giờ nằm trong YAML.
func buildMongoPipelineYAML(opts NewPipelineOpts, spoolDir, socketPath string) (string, error) {
	if err := checkMongoOpts(opts); err != nil {
		return "", err
	}
	q := yamlQuote
	var b strings.Builder
	fmt.Fprintf(&b, "# Strand pipeline %s: MongoDB -> MongoDB (created by strand web)\n", opts.Name)
	fmt.Fprintf(&b, "version: 1\nname: %s\n", q(opts.Name))
	fmt.Fprintf(&b, "source:\n  kind: mongodb\n  uri: \"${%s}\"\n  database: %s\n", mongoSrcURIEnv, q(strings.TrimSpace(opts.MongoSourceDB)))
	fmt.Fprintf(&b, "target:\n  kind: mongodb\n  uri: \"${%s}\"\n  database: %s\n", mongoDstURIEnv, q(strings.TrimSpace(opts.MongoTargetDB)))
	inc := opts.Includes
	if len(inc) == 0 {
		inc = []string{strings.TrimSpace(opts.MongoSourceDB) + ".*"}
	}
	b.WriteString("tables:\n  include:\n")
	for _, p := range inc {
		fmt.Fprintf(&b, "    - %s\n", q(p))
	}
	if len(opts.Excludes) > 0 {
		b.WriteString("  exclude:\n")
		for _, p := range opts.Excludes {
			fmt.Fprintf(&b, "    - %s\n", q(p))
		}
	}
	var m []string
	if opts.MongoSnapshotBatch > 0 {
		m = append(m, fmt.Sprintf("  snapshot_batch: %d", opts.MongoSnapshotBatch))
	}
	if opts.MongoStreamBatch > 0 {
		m = append(m, fmt.Sprintf("  stream_batch: %d", opts.MongoStreamBatch))
	}
	if v := strings.TrimSpace(opts.MongoLeaseTTL); v != "" {
		m = append(m, "  lease_ttl: "+q(v))
	}
	if opts.MongoOverwriteTarget {
		m = append(m, "  overwrite_target: true")
	}
	if len(m) > 0 {
		b.WriteString("mongodb:\n" + strings.Join(m, "\n") + "\n")
	}
	fmt.Fprintf(&b, "resources:\n  spool:\n    path: %s\n", q(spoolDir))
	b.WriteString("observability:\n")
	fmt.Fprintf(&b, "  admin_socket: %s\n", q(socketPath))
	if opts.Metrics != "" {
		fmt.Fprintf(&b, "  metrics: %s\n", q(opts.Metrics))
	}
	if opts.LogFormat != "" {
		fmt.Fprintf(&b, "  log: { level: info, format: %s }\n", q(opts.LogFormat))
	}
	return b.String(), nil
}
