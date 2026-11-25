package runner

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

// MySQL point-in-time recovery replays `mysqlbinlog --database=<target> --rewrite-db=<source>-><target>`
// into the target database. --rewrite-db renames only the default database (the `use` of statement
// events and the table map of row events); database names written in statement text are executed as
// written. DDL is always logged as a statement, so `USE shop; ALTER TABLE shop.orders ...` replayed into
// shop_restore next to the source would change the source itself. And --database drops statements whose
// default database is another one, so `USE other; ALTER TABLE shop.t ...` would silently be left out.
//
// binlogGuard reads mysqlbinlog's text output and fails closed on both. It runs twice:
//
//   - a prescan over the unfiltered, unrenamed output (before anything is created): a statement that
//     names the source database but is not replayed is a drift error; a replayed statement that names
//     the source database (when the target name differs) or another database of the target server, or
//     that creates, drops or alters a database, is refused;
//   - a filter between the replay mysqlbinlog and the mysql client: every statement is checked again
//     before it is forwarded, and the positions of the forwarded statements are recorded so that the
//     statements the prescan saw touching the source are proven to have been replayed.
//
// Row events (BINLOG '...') are renamed correctly by --rewrite-db and pass through unchecked.
type binlogGuard struct {
	src, dst  string
	forbidden map[string]bool // lower-cased databases a replayed statement must not name
	files     []string        // display names of the binlog files, in order
	replay    bool            // the filter in front of mysql (true) or the prescan (false)

	r         *bufio.Reader
	out       bytes.Buffer
	delimiter string
	db        string // default database in effect (last `use`)
	fileIdx   int
	at        string // "# at" offset of the current event
	when      string // timestamp of the current event header
	stmt      bytes.Buffer
	inStmt    bool
	inBinlog  bool
	err       error
	readErr   error

	// prescan: statements touching the source that must appear in the replay; replay: statements forwarded.
	positions map[string]string
}

func newBinlogGuard(r io.Reader, src, dst string, forbidden map[string]bool, files []string, replay bool) *binlogGuard {
	return &binlogGuard{src: src, dst: dst, forbidden: forbidden, files: files, replay: replay,
		r: bufio.NewReaderSize(r, 1<<20), delimiter: ";", fileIdx: -1, positions: map[string]string{}}
}

// binlogForbiddenDatabases is the set of databases a replayed statement must not name: the other
// databases of the target server, and the source database when the target has another name.
func binlogForbiddenDatabases(src, dst string, targetDatabases []string) map[string]bool {
	out := map[string]bool{}
	for _, d := range targetDatabases {
		out[strings.ToLower(d)] = true
	}
	if src != dst {
		out[strings.ToLower(src)] = true
	} else {
		delete(out, strings.ToLower(src))
	}
	delete(out, strings.ToLower(dst))
	// Read-only schemas: a replayed statement (a routine or view body) may read them.
	delete(out, "information_schema")
	delete(out, "performance_schema")
	return out
}

// Err is the guard's verdict (nil at a clean EOF).
func (g *binlogGuard) Err() error { return g.err }

var (
	binlogHeaderRE = regexp.MustCompile(`^#(\d{6}\s+\d{1,2}:\d{2}:\d{2})\s+server id`)
	binlogUseRE    = regexp.MustCompile("^use\\s+`((?:[^`]|``)*)`")
)

func (g *binlogGuard) Read(p []byte) (int, error) {
	for g.out.Len() == 0 && g.err == nil && g.readErr == nil {
		line, err := g.r.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			// A very long line: part of a statement or of a BINLOG block.
			if g.inBinlog {
				g.out.Write(line)
			} else {
				g.inStmt = true
				g.stmt.Write(line)
			}
			continue
		}
		if len(line) > 0 {
			g.line(line)
		}
		if err != nil {
			if errors.Is(err, io.EOF) && g.err == nil && (g.inStmt || g.inBinlog) && len(bytes.TrimSpace(g.stmt.Bytes())) > 0 {
				g.err = errors.New("mysqlbinlog output ended inside a statement")
			}
			g.readErr = err
		}
	}
	if g.out.Len() > 0 {
		return g.out.Read(p)
	}
	if g.err != nil {
		return 0, g.err
	}
	return 0, g.readErr
}

func (g *binlogGuard) line(line []byte) {
	trimmed := bytes.TrimRight(line, "\r\n")
	switch {
	case g.inBinlog:
		g.out.Write(line)
		if bytes.HasSuffix(bytes.TrimSpace(trimmed), []byte("'"+g.delimiter)) {
			g.inBinlog = false
		}
		return
	case g.inStmt:
		g.stmt.Write(line)
	default:
		t := string(trimmed)
		switch {
		case strings.HasPrefix(t, "# at "):
			g.at = strings.TrimSpace(t[len("# at "):])
			g.out.Write(line)
			return
		case strings.HasPrefix(t, "#"):
			if m := binlogHeaderRE.FindStringSubmatch(t); m != nil {
				g.when = m[1]
				if strings.Contains(t, "Start: binlog v") {
					g.fileIdx++
				}
			}
			g.out.Write(line)
			return
		case strings.TrimSpace(t) == "":
			g.out.Write(line)
			return
		case strings.HasPrefix(t, "DELIMITER "):
			g.delimiter = strings.TrimSpace(t[len("DELIMITER "):])
			g.out.Write(line)
			return
		case strings.HasPrefix(t, "BINLOG '"):
			g.out.Write(line)
			g.inBinlog = !strings.HasSuffix(strings.TrimSpace(t), "'"+g.delimiter) || strings.TrimSpace(t) == "BINLOG '"+g.delimiter
			return
		}
		g.inStmt = true
		g.stmt.Write(line)
	}
	if !bytes.HasSuffix(bytes.TrimSpace(trimmed), []byte(g.delimiter)) {
		return
	}
	text := g.stmt.Bytes()
	if err := g.statement(strings.TrimSuffix(strings.TrimSpace(string(text)), g.delimiter)); err != nil {
		g.err = err
		return
	}
	g.out.Write(text)
	g.stmt.Reset()
	g.inStmt = false
}

func (g *binlogGuard) position() string {
	file := "binlog"
	if g.fileIdx >= 0 && g.fileIdx < len(g.files) {
		file = g.files[g.fileIdx]
	}
	s := fmt.Sprintf("%s position %s", file, g.at)
	if g.when != "" {
		s += " (" + g.when + " UTC)"
	}
	return s
}

func (g *binlogGuard) key() string { return fmt.Sprintf("%d:%s", g.fileIdx, g.at) }

func (g *binlogGuard) statement(text string) error {
	if m := binlogUseRE.FindStringSubmatch(text); m != nil {
		g.db = strings.ReplaceAll(m[1], "``", "`")
		return nil
	}
	toks := sqlTokens(text)
	verb := firstWords(toks, 2)
	switch strings.ToUpper(firstWords(toks, 1)) {
	case "", "SET", "BEGIN", "COMMIT", "ROLLBACK", "XA", "SAVEPOINT", "RELEASE":
		return nil
	}
	quals := qualifiers(toks)
	ddl, ddlName, ddlNamed := databaseDDL(toks)
	touchesSrc := ddlNamed && strings.EqualFold(ddlName, g.src)
	for _, q := range quals {
		if strings.EqualFold(q, g.src) {
			touchesSrc = true
		}
	}
	replayed := g.replay || g.db == g.src
	if !replayed {
		if touchesSrc {
			return fmt.Errorf("binlog statement at %s (%s) changes database %s while its default database was %s; "+
				"the point-in-time replay of %s only replays statements run with %s as default database, so the restore "+
				"would silently diverge from the source. Restore without a target time, or replay the binlogs manually",
				g.position(), verb, quoteMy(g.src), quoteMyOrNone(g.db), quoteMy(g.src), quoteMy(g.src))
		}
		return nil
	}
	if ddl != "" && (ddl != "ALTER" || ddlNamed) {
		return fmt.Errorf("binlog statement at %s (%s DATABASE) is replayed by point-in-time recovery but changes a database "+
			"rather than tables in it; Strand never replays it into %s. Restore to a point before it, or replay the binlogs manually",
			g.position(), ddl, quoteMy(g.dst))
	}
	for _, q := range quals {
		if g.forbidden[strings.ToLower(q)] {
			advice := "restore to a separate server instead"
			if strings.EqualFold(q, g.src) {
				advice = "restore to a separate server under the same database name instead"
			}
			return fmt.Errorf("binlog statement at %s (%s) names database %s explicitly; replaying it into %s would "+
				"change %s itself, so the restore was stopped: %s",
				g.position(), verb, quoteMy(q), quoteMy(g.dst), quoteMy(q), advice)
		}
	}
	if g.replay || touchesSrc {
		g.positions[g.key()] = g.position()
	}
	return nil
}

// unreplayed returns the positions recorded by prescan that the replay did not forward.
func (g *binlogGuard) unreplayed(replay *binlogGuard) []string {
	var out []string
	for k, pos := range g.positions {
		if _, ok := replay.positions[k]; !ok {
			out = append(out, pos)
		}
	}
	sort.Strings(out)
	return out
}

func quoteMy(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

func quoteMyOrNone(name string) string {
	if name == "" {
		return "(none)"
	}
	return quoteMy(name)
}

// --- a small MySQL lexer: enough to find db-qualified names outside strings and comments ---

type sqlTokKind int

const (
	tokIdent  sqlTokKind = iota // bare word
	tokQuoted                   // `quoted` or "quoted" (an identifier under ANSI_QUOTES)
	tokDot
	tokOther
)

type sqlTok struct {
	kind sqlTokKind
	text string
}

func isIdentByte(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// sqlTokens lexes s: strings, comments and variables are dropped; the content of executable comments
// (/*!...*/) is lexed as code.
func sqlTokens(s string) []sqlTok {
	var out []sqlTok
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '#':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '-' && i+2 < len(s) && s[i+1] == '-' && (s[i+2] == ' ' || s[i+2] == '\t' || s[i+2] == '\n'):
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '-' && i+2 == len(s) && s[i+1] == '-':
			i = len(s)
		case c == '/' && i+2 < len(s) && s[i+1] == '*' && s[i+2] == '!':
			i += 3
			for i < len(s) && s[i] >= '0' && s[i] <= '9' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				i = len(s)
			} else {
				i += 2 + end + 2
			}
		case c == '*' && i+1 < len(s) && s[i+1] == '/':
			i += 2 // end of an executable comment
		case c == '\'':
			i = skipQuoted(s, i, '\'', true)
		case c == '"':
			j := skipQuoted(s, i, '"', true)
			out = append(out, sqlTok{tokQuoted, unquote(s[i:j], '"')})
			i = j
		case c == '`':
			j := skipQuoted(s, i, '`', false)
			out = append(out, sqlTok{tokQuoted, unquote(s[i:j], '`')})
			i = j
		case c == '@':
			i++
			if i < len(s) && s[i] == '@' {
				i++
			}
			for i < len(s) {
				if s[i] == '\'' || s[i] == '"' || s[i] == '`' {
					i = skipQuoted(s, i, s[i], s[i] != '`')
				} else if isIdentByte(s[i]) || s[i] == '.' {
					i++
				} else {
					break
				}
			}
		case c == '.':
			out = append(out, sqlTok{tokDot, "."})
			i++
		case isIdentByte(c):
			j := i
			for j < len(s) && isIdentByte(s[j]) {
				j++
			}
			word := s[i:j]
			if strings.Trim(word, "0123456789") == "" {
				out = append(out, sqlTok{tokOther, word})
			} else {
				out = append(out, sqlTok{tokIdent, word})
			}
			i = j
		default:
			out = append(out, sqlTok{tokOther, string(c)})
			i++
		}
	}
	return out
}

// skipQuoted returns the index after the quoted token starting at s[i].
func skipQuoted(s string, i int, q byte, backslash bool) int {
	for j := i + 1; j < len(s); j++ {
		switch {
		case backslash && s[j] == '\\':
			j++
		case s[j] == q:
			if j+1 < len(s) && s[j+1] == q {
				j++
				continue
			}
			return j + 1
		}
	}
	return len(s)
}

func unquote(tok string, q byte) string {
	if len(tok) >= 2 && tok[len(tok)-1] == q {
		tok = tok[1 : len(tok)-1]
	} else if len(tok) >= 1 {
		tok = tok[1:]
	}
	return strings.ReplaceAll(tok, string([]byte{q, q}), string(q))
}

// qualifiers returns the names used as the first part of a qualified name (x.y, `x`.`y`).
func qualifiers(toks []sqlTok) []string {
	var out []string
	for i, t := range toks {
		if (t.kind == tokIdent || t.kind == tokQuoted) && i+1 < len(toks) && toks[i+1].kind == tokDot && (i == 0 || toks[i-1].kind != tokDot) {
			out = append(out, t.text)
		}
	}
	return out
}

func firstWords(toks []sqlTok, n int) string {
	var w []string
	for _, t := range toks {
		if len(w) == n || t.kind != tokIdent {
			break
		}
		w = append(w, strings.ToUpper(t.text))
	}
	return strings.Join(w, " ")
}

// databaseDDL recognizes CREATE/ALTER/DROP DATABASE|SCHEMA [IF [NOT] EXISTS] [name]. named is false for
// ALTER DATABASE without a name (it applies to the default database).
func databaseDDL(toks []sqlTok) (verb, name string, named bool) {
	if len(toks) < 2 || toks[0].kind != tokIdent || toks[1].kind != tokIdent {
		return "", "", false
	}
	verb = strings.ToUpper(toks[0].text)
	if verb != "CREATE" && verb != "ALTER" && verb != "DROP" {
		return "", "", false
	}
	if kw := strings.ToUpper(toks[1].text); kw != "DATABASE" && kw != "SCHEMA" {
		return "", "", false
	}
	rest := toks[2:]
	if len(rest) > 0 && rest[0].kind == tokIdent && strings.EqualFold(rest[0].text, "IF") {
		for len(rest) > 0 && rest[0].kind == tokIdent && (strings.EqualFold(rest[0].text, "IF") || strings.EqualFold(rest[0].text, "NOT") ||
			strings.EqualFold(rest[0].text, "EXISTS")) {
			rest = rest[1:]
		}
	}
	if len(rest) == 0 {
		return verb, "", false
	}
	if rest[0].kind == tokQuoted {
		return verb, rest[0].text, true
	}
	if rest[0].kind == tokIdent {
		switch strings.ToUpper(rest[0].text) {
		case "CHARACTER", "CHARSET", "DEFAULT", "COLLATE", "ENCRYPTION", "READ", "UPGRADE":
			if verb == "ALTER" {
				return verb, "", false
			}
		}
		return verb, rest[0].text, true
	}
	return verb, "", false
}
