package web

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
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

// mongoHostRE is one seed host: a DNS name or IPv4 address, or a bracketed IPv6 address, with an optional port.
var mongoHostRE = regexp.MustCompile(`^(?:[A-Za-z0-9](?:[A-Za-z0-9._-]{0,252})|\[[0-9A-Fa-f:.]+\])(?::([0-9]{1,5}))?$`)

// mongoNameRE is a replica set name or an authentication database name.
var mongoNameRE = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}$`)

// mongoURIOf is the connection URI of one side ("source" or "target") of a MongoDB pipeline. An explicit
// source_uri / target_uri (API clients) wins; otherwise it is built from the host(s), port, user, password,
// authentication database, replica set and TLS fields the console sends. The field names in errors are the
// form's, so the wizard shows them under the right input. The URI holds the password: callers only put it in
// the encrypted .env or hand it to the driver, never in YAML, logs or responses.
func mongoURIOf(opts NewPipelineOpts, side string) (string, error) {
	uri, host, port, user, pw := opts.SourceURI, opts.SourceHost, opts.SourcePort, opts.SourceUser, opts.SourcePassword
	authDB, rs, useTLS := opts.SourceAuthDB, opts.SourceReplicaSet, opts.SourceTLS
	if side == "target" {
		uri, host, port, user, pw = opts.TargetURI, opts.TargetHost, opts.TargetPort, opts.TargetUser, opts.TargetPassword
		authDB, rs, useTLS = opts.TargetAuthDB, opts.TargetReplicaSet, opts.TargetTLS
	}
	if uri = strings.TrimSpace(uri); uri != "" {
		if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
			return "", fmt.Errorf("%s_uri: must start with mongodb:// or mongodb+srv://", side)
		}
		return uri, nil
	}
	return buildMongoURI(side, mongoEndpoint{Hosts: host, Port: port, User: user, Password: pw, AuthDB: authDB, ReplicaSet: rs, TLS: useTLS})
}

// mongoEndpoint is what the console asks for instead of a URI.
type mongoEndpoint struct {
	Hosts      string // "db1" or "db1:27017, db2:27018" (replica set members)
	Port       int    // default port of the hosts written without one (27017 when 0)
	User       string
	Password   string
	AuthDB     string // authSource (admin when empty and a user is set)
	ReplicaSet string
	TLS        bool
}

// buildMongoURI builds mongodb://user:password@h1:p1,h2:p2/?authSource=..&replicaSet=..&tls=true with the user
// and password escaped. side ("source"/"target") prefixes the field names in errors.
func buildMongoURI(side string, e mongoEndpoint) (string, error) {
	port := e.Port
	if port == 0 {
		port = 27017
	}
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("%s_port: %d is not a port between 1 and 65535", side, port)
	}
	var hosts []string
	for _, h := range strings.Split(e.Hosts, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		m := mongoHostRE.FindStringSubmatch(h)
		if m == nil {
			return "", fmt.Errorf("%s_host: %q is not a host name, IP address or host:port", side, h)
		}
		if m[1] == "" {
			h += ":" + strconv.Itoa(port)
		} else if p, _ := strconv.Atoi(m[1]); p < 1 || p > 65535 {
			return "", fmt.Errorf("%s_host: %q has an invalid port", side, h)
		}
		hosts = append(hosts, h)
	}
	if len(hosts) == 0 {
		return "", fmt.Errorf("%s_host: required (the MongoDB host, or several host:port separated by commas)", side)
	}
	user := strings.TrimSpace(e.User)
	authDB, rs := strings.TrimSpace(e.AuthDB), strings.TrimSpace(e.ReplicaSet)
	if authDB != "" && !mongoNameRE.MatchString(authDB) {
		return "", fmt.Errorf("%s_auth_db: %q is not a valid database name", side, authDB)
	}
	if rs != "" && !mongoNameRE.MatchString(rs) {
		return "", fmt.Errorf("%s_replica_set: %q is not a valid replica set name", side, rs)
	}
	if e.Password != "" && user == "" {
		return "", fmt.Errorf("%s_user: required when a password is set", side)
	}
	u := url.URL{Scheme: "mongodb", Host: strings.Join(hosts, ","), Path: "/"}
	if user != "" {
		u.User = url.UserPassword(user, e.Password)
		if authDB == "" {
			authDB = "admin"
		}
	}
	q := url.Values{}
	if authDB != "" && user != "" {
		q.Set("authSource", authDB)
	}
	if rs != "" {
		q.Set("replicaSet", rs)
	}
	if e.TLS {
		q.Set("tls", "true")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// mongoTargetDB is the target database: the source database's name when left empty.
func mongoTargetDB(opts NewPipelineOpts) string {
	if v := strings.TrimSpace(opts.MongoTargetDB); v != "" {
		return v
	}
	return strings.TrimSpace(opts.MongoSourceDB)
}

// checkMongoOpts kiểm đầu vào của cặp MongoDB (gọi từ checkOpts khi một phía là mongodb).
func checkMongoOpts(opts NewPipelineOpts) error {
	if !isMongo(opts.From) || !isMongo(opts.To) {
		return fmt.Errorf("MongoDB can only be copied to MongoDB (source and target must both be mongodb)")
	}
	for _, side := range []string{"source", "target"} {
		if _, err := mongoURIOf(opts, side); err != nil {
			return err
		}
	}
	if strings.TrimSpace(opts.MongoSourceDB) == "" {
		return fmt.Errorf("source_database: required: the database to copy (List databases shows the ones on the source)")
	}
	for field, v := range map[string]string{"source_database": opts.MongoSourceDB, "target_mongo_database": mongoTargetDB(opts)} {
		v = strings.TrimSpace(v)
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
	fmt.Fprintf(&b, "target:\n  kind: mongodb\n  uri: \"${%s}\"\n  database: %s\n", mongoDstURIEnv, q(mongoTargetDB(opts)))
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
