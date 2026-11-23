package runner

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
)

// PostgreSQL client/server compatibility.
//
// pg_restore 17 and newer always sends "SET transaction_timeout = 0", which PostgreSQL 16 and older
// reject; Strand restores with --exit-on-error, so a restore into such a server needs a pg_restore of
// major 16 or older. That pg_restore cannot read archives written by pg_dump 17+ (archive format 1.16),
// so dumps of PostgreSQL <= 16 servers are taken with a pg_dump of major 16 or older as well (pg_dump
// supports servers back to 9.2): the archive is then restorable into the same server major and, with the
// default newest pg_restore, into any newer one. The older clients are optional versioned tools named
// "<tool>-<major>" (the Web image ships pg_dump-16 and pg_restore-16), resolved like every tool.
const pgLegacyMaxMajor = 16

// OptionalTool is a versioned native tool Strand uses only for some server versions.
type OptionalTool struct {
	Name    string
	Purpose string
}

// OptionalTools lists the versioned tools Strand looks for (tools status / preflight). pg_dump-N and
// pg_restore-N with another N from 12 to 16 are also used when present (exact server major first).
func OptionalTools() []OptionalTool {
	return []OptionalTool{
		{MySQLDump80Tool, "mysqldump of MySQL 8.0/8.1 servers (mysqldump 8.2+ fails there with --source-data)"},
		{"pg_dump-" + strconv.Itoa(pgLegacyMaxMajor), "pg_dump of PostgreSQL <= 16 servers, so the dump stays restorable into PostgreSQL <= 16"},
		{"pg_restore-" + strconv.Itoa(pgLegacyMaxMajor), "restore into PostgreSQL <= 16 (pg_restore 17+ sends SET transaction_timeout)"},
	}
}

var pgVerRE = regexp.MustCompile(`(\d+)(?:\.(\d+))?`)

// pgMajor returns the major version of a server_version string ("16.4 (Debian 16.4-1)", "9.6.24") or of
// a tool's --version output ("pg_restore (PostgreSQL) 19.0 (Debian ...)"); 0 when unknown.
func pgMajor(v string) int {
	if i := strings.Index(v, "(PostgreSQL)"); i >= 0 {
		v = v[i+len("(PostgreSQL)"):]
	}
	m := pgVerRE.FindStringSubmatch(v)
	if m == nil {
		return 0
	}
	major, _ := strconv.Atoi(m[1])
	if major == 9 && m[2] != "" { // 9.x: the major is 9.x, below every supported legacy tool anyway
		return 9
	}
	return major
}

// pgArchiveMinReader returns the oldest pg_restore major that reads a custom-format archive starting
// with header ("PGDMP", major, minor, rev); 0 when unknown or old enough for any supported pg_restore.
func pgArchiveMinReader(header []byte) int {
	if len(header) < 7 || string(header[:5]) != "PGDMP" || header[5] != 1 {
		return 0
	}
	switch minor := int(header[6]); {
	case minor >= 16:
		return 17
	case minor == 15:
		return 16
	case minor == 14:
		return 12
	}
	return 0
}

// legacyPGTool picks "<tool>-<N>" for a server of major server (<= 16) when the default tool is 17+:
// the first resolvable N from max(server, minN) up to 16. ok is false when the default tool is fine.
func legacyPGTool(ctx context.Context, getenv Getenv, tool string, server, minN int) (name string, ok bool, err error) {
	if server == 0 || server > pgLegacyMaxMajor {
		return tool, false, nil
	}
	ver, _ := RunCapture(ctx, getenv, tool, "--version")
	if d := pgMajor(ver); d != 0 && d <= pgLegacyMaxMajor {
		return tool, false, nil
	}
	from := server
	if minN > from {
		from = minN
	}
	for n := from; n <= pgLegacyMaxMajor; n++ {
		if _, rerr := ResolveTool(getenv, tool+"-"+strconv.Itoa(n)); rerr == nil {
			return tool + "-" + strconv.Itoa(n), true, nil
		}
	}
	return "", true, fmt.Errorf("native tool %s-%d not found (install it or set %s)", tool, pgLegacyMaxMajor, EnvToolsDir)
}

// SelectPGRestore picks the pg_restore for restoring an archive (header: its first bytes, nil if
// unknown) into a server whose server_version is serverVersion. Errors name the missing tool, or say
// that the archive cannot be restored into that server at all.
func SelectPGRestore(ctx context.Context, getenv Getenv, serverVersion string, header []byte) (string, error) {
	server := pgMajor(serverVersion)
	minReader := pgArchiveMinReader(header)
	if server != 0 && server <= pgLegacyMaxMajor && minReader > pgLegacyMaxMajor {
		def, _ := RunCapture(ctx, getenv, "pg_restore", "--version")
		if d := pgMajor(def); d == 0 || d > pgLegacyMaxMajor {
			return "", fmt.Errorf("this backup was written by pg_dump 17 or newer (archive format 1.%d): pg_restore 16 or "+
				"older cannot read it and pg_restore 17+ cannot restore into PostgreSQL %d (SET transaction_timeout); "+
				"restore it into PostgreSQL 17 or newer (new backups of PostgreSQL <= 16 use pg_dump-16 when present)", header[6], server)
		}
	}
	name, _, err := legacyPGTool(ctx, getenv, "pg_restore", server, minReader)
	if err != nil {
		return "", fmt.Errorf("restore into PostgreSQL %d: pg_restore 17+ sends SET transaction_timeout, which it "+
			"rejects; provide a pg_restore of major %d-%d as pg_restore-<major>: %w", server, max(server, minReader), pgLegacyMaxMajor, err)
	}
	return name, nil
}

// SelectPGDump picks the pg_dump for a server whose server_version is serverVersion: "pg_dump", or
// pg_dump-N (N <= 16) for a PostgreSQL <= 16 server so that the archive stays restorable into it. When no
// such tool exists the default is used and note says why the backup will only restore into 17+.
func SelectPGDump(ctx context.Context, getenv Getenv, serverVersion string) (tool, note string) {
	name, _, err := legacyPGTool(ctx, getenv, "pg_dump", pgMajor(serverVersion), 0)
	if err != nil {
		return "pg_dump", fmt.Sprintf("PostgreSQL %d source but no pg_dump-%d: the backup will only be restorable into "+
			"PostgreSQL 17 or newer (%v)", pgMajor(serverVersion), pgLegacyMaxMajor, err)
	}
	return name, ""
}

// pgServerVersion returns the server_version of c's server (database db), "" when it cannot connect.
func (e *Env) pgServerVersion(ctx context.Context, c backup.ConnSpec, db string) string {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := connectBackupPG(cctx, e.Getenv, c, db, false)
	if err != nil {
		return ""
	}
	defer conn.Close(context.Background())
	return conn.ParameterStatus("server_version")
}

// pgDumpTool picks the pg_dump for job's source server (see SelectPGDump) and logs the choice.
func (e *Env) pgDumpTool(ctx context.Context, c backup.ConnSpec) string {
	db := "postgres"
	if len(c.Databases) > 0 && c.Databases[0] != "" {
		db = c.Databases[0]
	}
	server := e.pgServerVersion(ctx, c, db)
	tool, note := SelectPGDump(ctx, e.Getenv, server)
	if note != "" {
		e.logf("warning: pg_dump: %s", note)
	} else if tool != "pg_dump" {
		e.logf("pg_dump: PostgreSQL %s server, using %s", server, tool)
	}
	return tool
}
