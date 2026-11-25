package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"strand/internal/backup"
)

// Tool directories. STRAND_BACKUP_TOOLS_DIR, when set, is the only place native tools are taken from;
// otherwise the Web backup command directory and then PATH are searched. Strand always builds the argv
// itself and never runs a shell.
const (
	EnvToolsDir      = "STRAND_BACKUP_TOOLS_DIR"
	EnvWebCommandDir = "STRAND_WEB_BACKUP_COMMAND_DIR"
)

// ResolveTool finds an executable for a native tool name.
func ResolveTool(getenv Getenv, name string) (string, error) {
	if strings.ContainsAny(name, "/\x00") || name == "" {
		return "", fmt.Errorf("invalid tool name %q", name)
	}
	check := func(p string) (string, error) {
		fi, err := os.Stat(p)
		if err != nil {
			return "", err
		}
		if !fi.Mode().IsRegular() || fi.Mode().Perm()&0o111 == 0 {
			return "", fmt.Errorf("%s is not an executable file", p)
		}
		if fi.Mode().Perm()&0o022 != 0 {
			return "", fmt.Errorf("%s must not be group- or world-writable", p)
		}
		return p, nil
	}
	if dir := strings.TrimSpace(getenv(EnvToolsDir)); dir != "" {
		return check(filepath.Join(dir, name))
	}
	if dir := strings.TrimSpace(getenv(EnvWebCommandDir)); dir != "" {
		if p, err := check(filepath.Join(dir, name)); err == nil {
			return p, nil
		}
	}
	p, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("native tool %s not found (install it or set %s)", name, EnvToolsDir)
	}
	return p, nil
}

// Invocation is one native tool run.
type Invocation struct {
	Tool string   // tool name (resolved with ResolveTool)
	Args []string // argv after the program
	Env  []string // extra environment (secrets are passed here, never in argv)
	// Files are written (0600) to a private temp dir before the run; "{dir}" in Args/Env is replaced
	// with that directory.
	Files map[string]string
	// HideEnv names inherited variables to remove besides Strand's own: the (legacy, freely named)
	// variables holding connection secrets. The tool gets its secret only through Env/Files.
	HideEnv []string
}

// password returns the secret named by conn.PasswordEnv.
func password(getenv Getenv, c backup.ConnSpec) string {
	if c.PasswordEnv == "" {
		return ""
	}
	return getenv(c.PasswordEnv)
}

func mysqlConnArgs(c backup.ConnSpec) []string {
	args := []string{"--host=" + c.Host}
	if c.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(c.Port))
	}
	if c.User != "" {
		args = append(args, "--user="+c.User)
	}
	return args
}

// MySQLDumpFlavor describes the dump tool so the right position flags are used.
type MySQLDumpFlavor struct {
	MariaDB bool
	Major   int
	Minor   int
	Patch   int
}

var verRE = regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)`)

// ParseDumpVersion parses `mysqldump --version` output.
func ParseDumpVersion(out string) MySQLDumpFlavor {
	f := MySQLDumpFlavor{MariaDB: strings.Contains(out, "MariaDB")}
	// "mysqldump  Ver 8.0.40 for Linux" / "mysqldump  Ver 10.19 Distrib 10.11.6-MariaDB" /
	// "mariadb-dump from 11.4.2-MariaDB".
	text := out
	if i := strings.Index(out, "Distrib "); i >= 0 {
		text = out[i:]
	} else if i := strings.Index(out, "from "); i >= 0 {
		text = out[i:]
	}
	if m := verRE.FindStringSubmatch(text); m != nil {
		f.Major, _ = strconv.Atoi(m[1])
		f.Minor, _ = strconv.Atoi(m[2])
		f.Patch, _ = strconv.Atoi(m[3])
	}
	return f
}

// MySQLDumpInvocation builds a consistent, position-recording logical dump of conn's databases.
func MySQLDumpInvocation(getenv Getenv, tool string, c backup.ConnSpec, f MySQLDumpFlavor) Invocation {
	args := append(mysqlConnArgs(c), "--single-transaction", "--quick", "--routines", "--events", "--triggers",
		"--hex-blob", "--default-character-set=utf8mb4")
	switch {
	case f.MariaDB:
		args = append(args, "--master-data=2", "--gtid")
	case f.Major > 8 || (f.Major == 8 && (f.Minor > 0 || f.Patch >= 26)):
		args = append(args, "--source-data=2")
	default:
		args = append(args, "--master-data=2")
	}
	switch len(c.Databases) {
	case 0:
		args = append(args, "--all-databases")
	case 1:
		args = append(args, "--", c.Databases[0]) // no CREATE DATABASE: restorable under any name
	default:
		args = append(append(args, "--databases", "--"), c.Databases...)
	}
	inv := Invocation{Tool: tool, Args: args}
	if pw := password(getenv, c); pw != "" {
		inv.Env = append(inv.Env, "MYSQL_PWD="+pw)
	}
	return inv
}

var (
	changeMasterRE = regexp.MustCompile(`CHANGE (?:MASTER|REPLICATION SOURCE) TO (?:MASTER|SOURCE)_LOG_FILE='([^']+)', (?:MASTER|SOURCE)_LOG_POS=(\d+)`)
	gtidPurgedRE   = regexp.MustCompile(`(?s)SET @@GLOBAL\.GTID_PURGED=(?:/\*!80000 '\+'\*/ )?'([^']*)';`)
	mariaGTIDRE    = regexp.MustCompile(`SET GLOBAL gtid_slave_pos='([^']*)'`)
)

// ParseMySQLDumpHeader extracts the binlog coordinates and GTID set written by --source-data/--master-data
// from the head of a dump.
func ParseMySQLDumpHeader(head []byte) backup.Position {
	var p backup.Position
	if m := changeMasterRE.FindSubmatch(head); m != nil {
		p.BinlogFile = string(m[1])
		p.BinlogPos, _ = strconv.ParseInt(string(m[2]), 10, 64)
	}
	if m := gtidPurgedRE.FindSubmatch(head); m != nil {
		p.GTIDSet = strings.Join(strings.Fields(strings.ReplaceAll(string(m[1]), "\n", "")), "")
	} else if m := mariaGTIDRE.FindSubmatch(head); m != nil {
		p.GTIDSet = string(m[1])
	}
	return p
}

// PGDumpInvocation dumps one database in custom format (pg_restore can restore it under another name).
func PGDumpInvocation(getenv Getenv, c backup.ConnSpec, compressedByStrand bool) (Invocation, error) {
	if len(c.Databases) != 1 {
		return Invocation{}, errors.New("pg_dump backs up exactly one database per job")
	}
	args := []string{"--host=" + c.Host, "--format=custom", "--no-password"}
	if c.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(c.Port))
	}
	if c.User != "" {
		args = append(args, "--username="+c.User)
	}
	if compressedByStrand {
		args = append(args, "--compress=0")
	}
	args = append(args, "--dbname="+c.Databases[0])
	inv := Invocation{Tool: "pg_dump", Args: args}
	if pw := password(getenv, c); pw != "" {
		inv.Env = append(inv.Env, "PGPASSWORD="+pw)
	}
	return inv, nil
}

// PGBaseBackupInvocation streams a physical base backup (tar) to stdout; WAL needed for consistency is
// included (-X fetch). Positions are read from the verbose stderr.
func PGBaseBackupInvocation(getenv Getenv, c backup.ConnSpec) Invocation {
	args := []string{"--host=" + c.Host, "--pgdata=-", "--format=tar", "--wal-method=fetch", "--checkpoint=fast",
		"--verbose", "--no-password", "--label=strand"}
	if c.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(c.Port))
	}
	if c.User != "" {
		args = append(args, "--username="+c.User)
	}
	inv := Invocation{Tool: "pg_basebackup", Args: args}
	if pw := password(getenv, c); pw != "" {
		inv.Env = append(inv.Env, "PGPASSWORD="+pw)
	}
	return inv
}

var (
	walStartRE = regexp.MustCompile(`write-ahead log start point: ([0-9A-F]+/[0-9A-F]+) on timeline (\d+)`)
	walEndRE   = regexp.MustCompile(`write-ahead log end point: ([0-9A-F]+/[0-9A-F]+)`)
)

// ParsePGBaseBackupLog extracts start/end LSN and timeline from pg_basebackup --verbose output.
func ParsePGBaseBackupLog(log string) (start, end backup.Position, timeline string) {
	if m := walStartRE.FindStringSubmatch(log); m != nil {
		start.LSN, timeline = m[1], m[2]
	}
	if m := walEndRE.FindStringSubmatch(log); m != nil {
		end.LSN = m[1]
	}
	return
}

// MongoURI returns the connection URI of c (URIEnv, else built from host/port/user/password).
func MongoURI(getenv Getenv, c backup.ConnSpec) (string, error) {
	var uri string
	if c.URIEnv != "" {
		uri = strings.TrimSpace(getenv(c.URIEnv))
		if uri == "" {
			return "", fmt.Errorf("environment variable %s (MongoDB URI) is empty", c.URIEnv)
		}
	} else {
		host := c.Host
		if c.Port > 0 {
			host = net.JoinHostPort(strings.Trim(host, "[]"), strconv.Itoa(c.Port))
		}
		auth, opts := "", "directConnection=true"
		if c.User != "" {
			auth = urlEscape(c.User)
			if pw := password(getenv, c); pw != "" {
				auth += ":" + urlEscape(pw)
			}
			auth += "@"
			opts += "&authSource=admin"
		}
		uri = "mongodb://" + auth + host + "/?" + opts
	}
	return c.MongoTLSURI(uri)
}

func urlEscape(s string) string {
	var b strings.Builder
	for _, r := range []byte(s) {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' || r == '~' {
			b.WriteByte(r)
		} else {
			fmt.Fprintf(&b, "%%%02X", r)
		}
	}
	return b.String()
}

// MongodumpInvocation streams an archive to stdout. Without a database filter the dump includes the
// oplog (--oplog, replica sets) so it restores to one consistent point. The URI (with credentials) is
// passed in a private --config file, never in argv.
func MongodumpInvocation(uri string, c backup.ConnSpec) (Invocation, error) {
	args := []string{"--config={dir}/mongo.yaml", "--archive"}
	switch len(c.Databases) {
	case 0:
		args = append(args, "--oplog")
	case 1:
		args = append(args, "--db="+c.Databases[0])
	default:
		return Invocation{}, errors.New("mongodump backs up one database or the whole replica set (with --oplog)")
	}
	return Invocation{Tool: "mongodump", Args: args, Files: map[string]string{"mongo.yaml": "uri: " + strconv.Quote(uri) + "\n"}}, nil
}

// MydumperInvocation writes a parallel logical dump into {dir}/out (streamed afterwards as tar).
func MydumperInvocation(getenv Getenv, c backup.ConnSpec) Invocation {
	args := []string{"--defaults-extra-file={dir}/client.cnf", "--outputdir={dir}/out", "--threads=4", "--triggers",
		"--events", "--routines", "--host=" + c.Host}
	if c.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(c.Port))
	}
	if c.User != "" {
		args = append(args, "--user="+c.User)
	}
	for _, d := range c.Databases {
		args = append(args, "--database="+d)
	}
	return Invocation{Tool: "mydumper", Args: args, Files: map[string]string{"client.cnf": "[client]\npassword=" + password(getenv, c) + "\n"}}
}

var mydumperPosRE = regexp.MustCompile(`(?m)^\s*(?:Log|File): ?(\S+)\s*\n\s*Pos: ?(\d+)[ \t]*(?:\n\s*GTID: ?(.*))?`)

// ParseMydumperMetadata extracts the source binlog position from mydumper's metadata file: the
// "SHOW MASTER STATUS: Log/Pos/GTID" text of mydumper < 0.12, or the INI [source] ([master] in
// 0.12-0.15) section of newer releases, whose keys may be commented out ("# SOURCE_LOG_FILE = ...").
func ParseMydumperMetadata(text string) backup.Position {
	var p backup.Position
	if m := mydumperPosRE.FindStringSubmatch(text); m != nil {
		p.BinlogFile = m[1]
		p.BinlogPos, _ = strconv.ParseInt(m[2], 10, 64)
		p.GTIDSet = strings.Join(strings.Fields(m[3]), "")
		return p
	}
	section, gtidOpen := "", false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		// A GTID set with several UUIDs may continue on the next lines (each "uuid:1-N," per line).
		if gtidOpen {
			part := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			gtidOpen = strings.HasSuffix(strings.Trim(part, `"'`), ",")
			p.GTIDSet += strings.Join(strings.Fields(strings.Trim(part, `"'`)), "")
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.Trim(line, "[]"))
			continue
		}
		if section != "source" && section != "master" {
			continue
		}
		key, val, ok := strings.Cut(strings.TrimSpace(strings.TrimPrefix(line, "#")), "=")
		if !ok {
			continue
		}
		val = strings.TrimSpace(val)
		if i := strings.Index(val, " #"); i >= 0 {
			val = strings.TrimSpace(val[:i])
		}
		val = strings.Trim(val, `"'`)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "source_log_file", "master_log_file", "file":
			p.BinlogFile = val
		case "source_log_pos", "master_log_pos", "position":
			p.BinlogPos, _ = strconv.ParseInt(val, 10, 64)
		case "executed_gtid_set":
			p.GTIDSet = strings.Join(strings.Fields(val), "")
			gtidOpen = strings.HasSuffix(val, ",")
		}
	}
	return p
}

// Process is a running native tool whose stdout is the backup stream.
type Process struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr *capped
	dir    string
	waited bool
	err    error
	mu     sync.Mutex

	lifecycleMu sync.Mutex
	done        chan struct{}
	killTimer   *time.Timer
	exited      bool
}

// capped keeps the last part of stderr (for errors and position parsing) and forwards it to log. Secrets
// are redacted from what is logged and returned by String, also when a secret is split across writes:
// the last (longest secret - 1) bytes are held back until the next write or Flush.
type capped struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	max       int
	truncated bool
	log       io.Writer
	hide      []string
	pending   []byte
}

func (c *capped) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Write(p)
	if c.buf.Len() > c.max {
		b := c.buf.Bytes()[c.buf.Len()-c.max:]
		c.buf = *bytes.NewBuffer(append([]byte(nil), b...))
		c.truncated = true
	}
	if c.log != nil {
		c.pending = append(c.pending, p...)
		c.emit(false)
	}
	return len(p), nil
}

// Flush writes what Write held back (the process has exited).
func (c *capped) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.log != nil {
		c.emit(true)
	}
}

// emit logs the redacted pending bytes; unless final it keeps the tail that may begin a secret.
func (c *capped) emit(final bool) {
	data := c.pending
	var out bytes.Buffer
	i := 0
	for {
		at, n := -1, 0
		for _, x := range c.hide {
			if len(x) < minSecretLen {
				continue
			}
			if j := bytes.Index(data[i:], []byte(x)); j >= 0 && (at < 0 || j < at || j == at && len(x) > n) {
				at, n = j, len(x)
			}
		}
		if at < 0 {
			break
		}
		out.Write(data[i : i+at])
		out.WriteString("[redacted]")
		i += at + n
	}
	rest := data[i:]
	keep := 0
	if !final {
		keep = min(len(rest), longestSecret(c.hide)-1)
		keep = max(keep, 0)
	}
	out.Write(rest[:len(rest)-keep])
	c.pending = append(c.pending[:0:0], rest[len(rest)-keep:]...)
	if out.Len() > 0 {
		_, _ = c.log.Write(out.Bytes())
	}
}

// raw returns the unredacted tail (for parsing positions; never shown).
func (c *capped) raw() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

func (c *capped) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.buf.String()
	if c.truncated {
		// The cut may have split a secret: drop the bytes that could hold the rest of one.
		s = s[min(len(s), max(longestSecret(c.hide)-1, 0)):]
	}
	return redactAll(s, c.hide)
}

const minSecretLen = 3

func longestSecret(secrets []string) int {
	n := 0
	for _, x := range secrets {
		if len(x) >= minSecretLen {
			n = max(n, len(x))
		}
	}
	return n
}

func redactAll(s string, secrets []string) string {
	for _, x := range secrets {
		if len(x) >= minSecretLen {
			s = strings.ReplaceAll(s, x, "[redacted]")
		}
	}
	return s
}

// secretEnvName reports whether an invocation variable holds a secret (MYSQL_PWD, PGPASSWORD, ...), as
// opposed to settings such as PGSSLMODE, PGHOSTADDR or a {dir} path, which must stay readable.
func secretEnvName(name string) bool {
	n := strings.ToUpper(name)
	return strings.Contains(n, "PASSWORD") || strings.Contains(n, "PWD") || strings.Contains(n, "SECRET") ||
		strings.Contains(n, "TOKEN") || strings.HasSuffix(n, "URI") || strings.HasSuffix(n, "DSN")
}

// uriSecrets returns a connection URI with credentials and its password (raw and decoded).
func uriSecrets(v string) []string {
	out := []string{v}
	if u, err := url.Parse(v); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok {
			out = append(out, pw, url.QueryEscape(pw), url.PathEscape(pw))
		}
	}
	return out
}

// secretsOf lists the secret values of an invocation for redaction: passwords and credentialed URIs passed
// in its environment or option files. Non-secret settings are not listed: redacting them could blank
// output that is parsed (positions) or needed to diagnose a failure.
func secretsOf(inv Invocation) []string {
	var out []string
	for _, kv := range inv.Env {
		if k, v, ok := strings.Cut(kv, "="); ok && secretEnvName(k) {
			out = append(out, uriSecrets(v)...)
		}
	}
	for _, content := range inv.Files {
		for _, line := range strings.Split(content, "\n") {
			if k, v, ok := strings.Cut(line, "="); ok && secretFileKey(k) {
				out = append(out, uriSecrets(strings.Trim(strings.TrimSpace(v), `"'`))...)
			}
			if k, v, ok := strings.Cut(line, ": "); ok && secretFileKey(k) {
				v = strings.TrimSpace(v)
				if uq, err := strconv.Unquote(v); err == nil {
					v = uq
				}
				out = append(out, uriSecrets(v)...)
			}
		}
	}
	return out
}

// secretFileKey reports whether an option-file key (client.cnf password=, mongo.yaml uri:) holds a secret.
func secretFileKey(k string) bool {
	k = strings.ToLower(strings.TrimSpace(k))
	return k == "password" || k == "pass" || k == "uri" || strings.Contains(k, "password") || strings.Contains(k, "secret")
}

// baseEnv is the environment of native tools. Every STRAND_* variable is removed: they carry Strand's own
// secrets (repository and encryption keys, the Web secret, notification passwords) and the connection
// secrets of the source, scratch and restore target (STRAND_BACKUP_CONN_PASSWORD/URI,
// STRAND_BACKUP_SCRATCH_PASSWORD, STRAND_RESTORE_TARGET_PASSWORD/URI, ...), which a tool must not inherit:
// each tool gets only its own secret, explicitly (Invocation.Env/Files). hide adds further names (legacy
// password_env/uri_env variables).
func baseEnv(hide ...string) []string {
	out := []string{}
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "STRAND_") || name == "AWS_SECRET_ACCESS_KEY" || name == "AWS_SESSION_TOKEN" ||
			name == "MYSQL_PWD" || name == "PGPASSWORD" || slices.Contains(hide, name) {
			continue
		}
		out = append(out, kv)
	}
	return append(out, "TZ=UTC")
}

// connSecretEnv lists the variables named by the connections of job that hold secrets.
func connSecretEnv(job backup.Job) []string {
	var out []string
	for _, n := range []string{job.Conn.PasswordEnv, job.Conn.URIEnv, job.Scratch.PasswordEnv, job.Scratch.URIEnv} {
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

// Start launches inv. The caller reads Stdout to EOF; the final Read reports a non-zero exit instead of
// io.EOF. Close kills the process group and removes the private directory.
func Start(ctx context.Context, getenv Getenv, inv Invocation, stdin io.Reader, log io.Writer) (*Process, error) {
	path, err := ResolveTool(getenv, inv.Tool)
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(getenv("STRAND_BACKUP_TMPDIR"), "strand-tool-*")
	if err != nil {
		return nil, err
	}
	sub := func(s string) string { return strings.ReplaceAll(s, "{dir}", dir) }
	for name, content := range inv.Files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			os.RemoveAll(dir)
			return nil, err
		}
	}
	args := make([]string, len(inv.Args))
	for i, a := range inv.Args {
		args[i] = sub(a)
	}
	cmd := exec.Command(path, args...)
	cmd.Env = baseEnv(inv.HideEnv...)
	for _, e := range inv.Env {
		cmd.Env = append(cmd.Env, sub(e))
	}
	cmd.Dir = dir
	cmd.Stdin = stdin
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p := &Process{cmd: cmd, dir: dir, done: make(chan struct{}), stderr: &capped{max: 256 << 10, log: log, hide: secretsOf(inv)}}
	cmd.Stderr = p.stderr
	if p.stdout, err = cmd.StdoutPipe(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("start %s: %w", inv.Tool, err)
	}
	go func() {
		select {
		case <-ctx.Done():
			p.kill()
		case <-p.done:
		}
	}()
	return p, nil
}

// Dir is the private working directory ({dir}).
func (p *Process) Dir() string { return p.dir }

// Stderr returns the redacted tail of the tool's stderr.
func (p *Process) Stderr() string { return p.stderr.String() }

// rawStderr is the unredacted tail, for parsing positions only (never logged or returned in errors).
func (p *Process) rawStderr() string { return p.stderr.raw() }

func (p *Process) kill() {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()
	if p.exited || p.killTimer != nil {
		return
	}
	group := -p.cmd.Process.Pid
	_ = syscall.Kill(group, syscall.SIGTERM)
	p.killTimer = time.AfterFunc(5*time.Second, func() {
		p.lifecycleMu.Lock()
		defer p.lifecycleMu.Unlock()
		if !p.exited {
			_ = syscall.Kill(group, syscall.SIGKILL)
		}
	})
}

// Wait waits for exit (idempotent) and returns a redacted error with the stderr tail on failure.
func (p *Process) Wait() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		p.waited = true
		err := p.cmd.Wait()
		p.stderr.Flush()
		p.lifecycleMu.Lock()
		p.exited = true
		if p.killTimer != nil {
			p.killTimer.Stop()
		}
		close(p.done)
		p.lifecycleMu.Unlock()
		if err != nil {
			tail := strings.TrimSpace(p.stderr.String())
			if len(tail) > 2000 {
				tail = tail[len(tail)-2000:]
			}
			p.err = fmt.Errorf("%s failed: %v: %s", filepath.Base(p.cmd.Path), err, tail)
		}
	}
	return p.err
}

// Read reads stdout; at EOF it waits for the process and returns its failure instead of io.EOF.
func (p *Process) Read(b []byte) (int, error) {
	n, err := p.stdout.Read(b)
	if errors.Is(err, io.EOF) {
		if werr := p.Wait(); werr != nil {
			return n, werr
		}
	}
	return n, err
}

// Close stops the process (if still running) and removes the private directory.
func (p *Process) Close() error {
	p.kill()
	_ = p.Wait()
	return os.RemoveAll(p.dir)
}

// headTap records the first max bytes passing through (for dump header parsing).
type headTap struct {
	r   io.Reader
	buf bytes.Buffer
	max int
}

func (t *headTap) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if room := t.max - t.buf.Len(); room > 0 && n > 0 {
		if room > n {
			room = n
		}
		t.buf.Write(p[:room])
	}
	return n, err
}

// RunCapture runs a tool to completion and returns its stdout (bounded), for version probes.
func RunCapture(ctx context.Context, getenv Getenv, tool string, args ...string) (string, error) {
	path, err := ResolveTool(getenv, tool)
	if err != nil {
		return "", err
	}
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, path, args...)
	cmd.Env = baseEnv()
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err = cmd.Run()
	sc := bufio.NewScanner(io.LimitReader(&out, 64<<10))
	var b strings.Builder
	for sc.Scan() {
		b.WriteString(sc.Text())
		b.WriteByte('\n')
	}
	return b.String(), err
}
