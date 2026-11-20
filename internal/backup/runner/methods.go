package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	if c.URIEnv != "" {
		u := strings.TrimSpace(getenv(c.URIEnv))
		if u == "" {
			return "", fmt.Errorf("environment variable %s (MongoDB URI) is empty", c.URIEnv)
		}
		return u, nil
	}
	host := c.Host
	if c.Port > 0 {
		host += ":" + strconv.Itoa(c.Port)
	}
	auth := ""
	if c.User != "" {
		auth = urlEscape(c.User)
		if pw := password(getenv, c); pw != "" {
			auth += ":" + urlEscape(pw)
		}
		auth += "@"
	}
	return "mongodb://" + auth + host + "/?directConnection=true", nil
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

// ParseMydumperMetadata extracts SHOW MASTER STATUS from mydumper's metadata file.
func ParseMydumperMetadata(text string) backup.Position {
	var p backup.Position
	if m := mydumperPosRE.FindStringSubmatch(text); m != nil {
		p.BinlogFile = m[1]
		p.BinlogPos, _ = strconv.ParseInt(m[2], 10, 64)
		p.GTIDSet = strings.Join(strings.Fields(m[3]), "")
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
}

// capped keeps the last part of stderr (for errors and position parsing) and forwards it to log.
type capped struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	max  int
	log  io.Writer
	hide []string
}

func (c *capped) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Write(p)
	if c.buf.Len() > c.max {
		b := c.buf.Bytes()[c.buf.Len()-c.max:]
		c.buf = *bytes.NewBuffer(append([]byte(nil), b...))
	}
	if c.log != nil {
		_, _ = io.WriteString(c.log, redactAll(string(p), c.hide))
	}
	return len(p), nil
}

func (c *capped) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return redactAll(c.buf.String(), c.hide)
}

func redactAll(s string, secrets []string) string {
	for _, x := range secrets {
		if len(x) >= 3 {
			s = strings.ReplaceAll(s, x, "[redacted]")
		}
	}
	return s
}

// secretsOf lists the secret values of an invocation for redaction.
func secretsOf(inv Invocation) []string {
	var out []string
	for _, kv := range inv.Env {
		if _, v, ok := strings.Cut(kv, "="); ok {
			out = append(out, v)
		}
	}
	for _, content := range inv.Files {
		for _, line := range strings.Split(content, "\n") {
			if _, v, ok := strings.Cut(line, "="); ok {
				out = append(out, strings.TrimSpace(v))
			}
			if _, v, ok := strings.Cut(line, ": "); ok {
				if uq, err := strconv.Unquote(strings.TrimSpace(v)); err == nil {
					out = append(out, uq)
				}
			}
		}
	}
	return out
}

// baseEnv is the environment of native tools: Strand's own secrets are removed.
func baseEnv() []string {
	out := []string{}
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(name, "STRAND_WEB_") || strings.HasPrefix(name, "STRAND_BACKUP_S3_") || strings.HasPrefix(name, "STRAND_BACKUP_REPO_") ||
			strings.HasPrefix(name, "STRAND_BACKUP_ENCRYPTION") || strings.HasPrefix(name, "STRAND_SECRET") ||
			strings.HasPrefix(name, "STRAND_BACKUP_NOTIFY") || name == "AWS_SECRET_ACCESS_KEY" || name == "AWS_SESSION_TOKEN" ||
			name == "MYSQL_PWD" || name == "PGPASSWORD" {
			continue
		}
		out = append(out, kv)
	}
	return append(out, "TZ=UTC")
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
	cmd.Env = baseEnv()
	for _, e := range inv.Env {
		cmd.Env = append(cmd.Env, sub(e))
	}
	cmd.Dir = dir
	cmd.Stdin = stdin
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p := &Process{cmd: cmd, dir: dir, stderr: &capped{max: 256 << 10, log: log, hide: secretsOf(inv)}}
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
		<-ctx.Done()
		p.kill()
	}()
	return p, nil
}

// Dir is the private working directory ({dir}).
func (p *Process) Dir() string { return p.dir }

// Stderr returns the redacted tail of the tool's stderr.
func (p *Process) Stderr() string { return p.stderr.String() }

func (p *Process) kill() {
	if p.cmd.Process != nil && p.cmd.ProcessState == nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
		time.AfterFunc(5*time.Second, func() { _ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL) })
	}
}

// Wait waits for exit (idempotent) and returns a redacted error with the stderr tail on failure.
func (p *Process) Wait() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.waited {
		p.waited = true
		if err := p.cmd.Wait(); err != nil {
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
	p.mu.Lock()
	waited := p.waited
	p.mu.Unlock()
	if !waited {
		p.kill()
		_ = p.Wait()
	}
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
