package runner

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// gtidPurgedFilter comments out the `SET @@GLOBAL.GTID_PURGED=...;` statement of a mysqldump stream so the
// dump can be loaded into a server that already has GTIDs (a new database next to the source, or any
// non-empty server). The recorded GTID set stays in the catalog for point-in-time recovery.
type gtidPurgedFilter struct {
	r      *bufio.Reader
	out    bytes.Buffer
	inStmt bool
	err    error
}

// NewGTIDPurgedFilter wraps a mysqldump stream.
func NewGTIDPurgedFilter(r io.Reader) io.Reader {
	return &gtidPurgedFilter{r: bufio.NewReaderSize(r, 1<<20)}
}

var gtidPurgedPrefix = []byte("SET @@GLOBAL.GTID_PURGED=")

func (f *gtidPurgedFilter) Read(p []byte) (int, error) {
	for f.out.Len() == 0 && f.err == nil {
		line, err := f.r.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			// Very long line (bulk INSERT): pass through unchanged.
			f.out.Write(line)
			continue
		}
		if len(line) > 0 {
			trimmed := bytes.TrimLeft(line, " \t")
			switch {
			case f.inStmt:
				f.out.WriteString("-- ")
				f.out.Write(line)
				if bytes.Contains(line, []byte(";")) {
					f.inStmt = false
				}
			case bytes.HasPrefix(trimmed, gtidPurgedPrefix):
				f.out.WriteString("-- [strand] ")
				f.out.Write(line)
				f.inStmt = !bytes.Contains(line, []byte(";"))
			default:
				f.out.Write(line)
			}
		}
		if err != nil {
			f.err = err
		}
	}
	if f.out.Len() > 0 {
		return f.out.Read(p)
	}
	return 0, f.err
}

// StripGTIDPurged applies the filter to a whole dump.
func StripGTIDPurged(dump []byte) []byte {
	b, _ := io.ReadAll(NewGTIDPurgedFilter(bytes.NewReader(dump)))
	return b
}

// --- MySQL Partial Restore Stream Filter ---

var (
	myTableStructureRE = regexp.MustCompile(`^-- Table structure for table [` + "`" + `"'']?([^` + "`" + `"' \t\r\n]+)`)
	myTableDataRE      = regexp.MustCompile(`^-- Dumping data for table [` + "`" + `"'']?([^` + "`" + `"' \t\r\n]+)`)
	myViewStructureRE  = regexp.MustCompile(`^-- (?:Temporary|Final) view structure for view [` + "`" + `"'']?([^` + "`" + `"' \t\r\n]+)`)
	mySectionHeaderRE  = regexp.MustCompile(`^-- (?:Table structure|Dumping data|Temporary view|Final view|Dumping events|Dumping routines|Dumping triggers|Current Database|Position to start)`)
	myTriggerOnTableRE = regexp.MustCompile(`(?i)\bTRIGGER\s+[` + "`" + `"'']?([^` + "`" + `"' \t\r\n]+)[` + "`" + `"'']?\s+.*?\bON\s+[` + "`" + `"'']?([^` + "`" + `"' \t\r\n]+)`)
)

type mysqlPartialFilter struct {
	r              *bufio.Reader
	out            bytes.Buffer
	selected       map[string]bool
	seenTables     map[string]bool
	inDropSec      bool
	inTriggerBlock bool
	trigBuf        bytes.Buffer
	err            error
	eofChecked     bool
}

// NewMySQLPartialFilter filters a mysqldump stream to only include the DDL/DML and triggers
// of the selected tables, preserving header SET statements and footer.
func NewMySQLPartialFilter(r io.Reader, objects []string) *mysqlPartialFilter {
	sel := make(map[string]bool, len(objects))
	for _, o := range objects {
		o = strings.TrimSpace(o)
		if o != "" {
			sel[o] = true
		}
	}
	return &mysqlPartialFilter{
		r:          bufio.NewReaderSize(r, 1<<20),
		selected:   sel,
		seenTables: make(map[string]bool),
	}
}

func (f *mysqlPartialFilter) Err() error {
	if errors.Is(f.err, io.EOF) {
		return nil
	}
	return f.err
}

func (f *mysqlPartialFilter) checkMissing() error {
	if f.eofChecked {
		return f.err
	}
	f.eofChecked = true
	if len(f.selected) > 0 {
		for t := range f.selected {
			if !f.seenTables[t] {
				return fmt.Errorf("table %q not found in backup", t)
			}
		}
	}
	return nil
}

func (f *mysqlPartialFilter) Read(p []byte) (int, error) {
	for f.out.Len() == 0 && f.err == nil {
		line, err := f.r.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			if f.inTriggerBlock {
				f.trigBuf.Write(line)
			} else if !f.inDropSec {
				f.out.Write(line)
			}
			continue
		}
		if len(line) > 0 {
			lineStr := string(line)
			trimmed := strings.TrimSpace(lineStr)

			// Section start detection
			if m := myTableStructureRE.FindStringSubmatch(trimmed); len(m) > 1 {
				f.flushTrigger()
				tbl := m[1]
				f.seenTables[tbl] = true
				f.inDropSec = !f.selected[tbl]
				if !f.inDropSec {
					f.out.Write(line)
				}
			} else if m := myTableDataRE.FindStringSubmatch(trimmed); len(m) > 1 {
				f.flushTrigger()
				tbl := m[1]
				f.inDropSec = !f.selected[tbl]
				if !f.inDropSec {
					f.out.Write(line)
				}
			} else if m := myViewStructureRE.FindStringSubmatch(trimmed); len(m) > 1 {
				f.flushTrigger()
				view := m[1]
				f.seenTables[view] = true
				f.inDropSec = !f.selected[view]
				if !f.inDropSec {
					f.out.Write(line)
				}
			} else if strings.HasPrefix(trimmed, "-- ") && mySectionHeaderRE.MatchString(trimmed) {
				f.flushTrigger()
				f.inDropSec = false
				f.out.Write(line)
			} else if !f.inDropSec {
				if strings.HasPrefix(trimmed, "DELIMITER ;;") {
					f.flushTrigger()
					f.inTriggerBlock = true
					f.trigBuf.Write(line)
				} else if f.inTriggerBlock {
					f.trigBuf.Write(line)
					if strings.HasPrefix(trimmed, "DELIMITER ;") {
						f.flushTrigger()
					}
				} else {
					f.out.Write(line)
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				f.flushTrigger()
				if cerr := f.checkMissing(); cerr != nil {
					f.err = cerr
					return 0, f.err
				}
			}
			f.err = err
		}
	}
	if f.out.Len() > 0 {
		return f.out.Read(p)
	}
	return 0, f.err
}

func (f *mysqlPartialFilter) flushTrigger() {
	if f.trigBuf.Len() == 0 {
		f.inTriggerBlock = false
		return
	}
	bufStr := f.trigBuf.String()
	f.trigBuf.Reset()
	f.inTriggerBlock = false

	m := myTriggerOnTableRE.FindStringSubmatch(bufStr)
	if len(m) > 2 {
		tbl := m[2]
		if f.selected[tbl] {
			f.out.WriteString(bufStr)
		}
	} else {
		// Non-trigger delimiter block or delimiter statement: output as is
		f.out.WriteString(bufStr)
	}
}

// --- PostgreSQL TOC Filter ---

// PGTOCInfo holds foreign key, sequence ownership, and index mappings extracted from a pg_dump archive.
type PGTOCInfo struct {
	FKReferences   map[string]string // schema.table.constraint -> schema.reftable
	SequenceOwners map[string]string // schema.seq -> schema.table
	IndexTables    map[string]string // schema.index -> schema.table
}

// unquotePGIdent removes wrapping quotes and unescapes double quotes ("" -> ").
func unquotePGIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return s
}

// splitPGQualifiedIdent splits a qualified PostgreSQL identifier into its unquoted components.
// e.g. `"my schema"."Customers"` -> ["my schema", "Customers"]
// e.g. `public.orders` -> ["public", "orders"]
func splitPGQualifiedIdent(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var parts []string
	var current strings.Builder
	inQuotes := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuotes {
			if ch == '"' {
				if i+1 < len(s) && s[i+1] == '"' {
					current.WriteByte('"')
					i++
				} else {
					inQuotes = false
				}
			} else {
				current.WriteByte(ch)
			}
		} else {
			if ch == '"' {
				inQuotes = true
			} else if ch == '.' {
				parts = append(parts, current.String())
				current.Reset()
			} else if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '(' || ch == ')' || ch == ',' || ch == ';' {
				break
			} else {
				current.WriteByte(ch)
			}
		}
	}
	if current.Len() > 0 || len(parts) > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

type pgCommentHeader struct {
	Name   string
	Type   string
	Schema string
	Owner  string
}

func parsePGCommentHeader(line string) (pgCommentHeader, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "-- Name:") {
		return pgCommentHeader{}, false
	}
	content := strings.TrimPrefix(line, "-- Name:")
	parts := strings.Split(content, ";")
	var h pgCommentHeader
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if i == 0 {
			h.Name = unquotePGIdent(p)
			continue
		}
		idx := strings.Index(p, ":")
		if idx == -1 {
			continue
		}
		key := strings.TrimSpace(p[:idx])
		val := strings.TrimSpace(p[idx+1:])
		switch strings.ToUpper(key) {
		case "TYPE":
			h.Type = strings.ToUpper(val)
		case "SCHEMA":
			h.Schema = unquotePGIdent(val)
		case "OWNER":
			h.Owner = unquotePGIdent(val)
		}
	}
	if h.Schema == "" || h.Schema == "-" {
		h.Schema = "public"
	}
	return h, true
}

var (
	pgIndexOnRE       = regexp.MustCompile(`(?i)\bON\s+(?:ONLY\s+)?([^\s(]+)`)
	pgSeqOwnedByRE    = regexp.MustCompile(`(?i)\bOWNED\s+BY\s+([^\s;]+)`)
	pgAlterTableRE    = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:ONLY\s+)?([^\s]+)`)
	pgNextvalRE       = regexp.MustCompile(`(?i)nextval\('([^']+)'::regclass\)`)
	pgAddConstraintRE = regexp.MustCompile(`(?i)ADD\s+CONSTRAINT\s+([^\s(]+)`)
	pgReferencesRE    = regexp.MustCompile(`(?i)REFERENCES\s+([^\s(]+)`)
)

// parsePGSchemaMetadata parses the DDL schema stream of pg_restore -s -f - into PGTOCInfo.
func parsePGSchemaMetadata(schemaDDL []byte) (PGTOCInfo, error) {
	info := PGTOCInfo{
		FKReferences:   make(map[string]string),
		SequenceOwners: make(map[string]string),
		IndexTables:    make(map[string]string),
	}

	scanner := bufio.NewScanner(bytes.NewReader(schemaDDL))
	var currentHeader *pgCommentHeader
	var stmtLines []string

	processBlock := func(h pgCommentHeader, block string) {
		switch h.Type {
		case "INDEX":
			if m := pgIndexOnRE.FindStringSubmatch(block); len(m) > 1 {
				tblParts := splitPGQualifiedIdent(m[1])
				tableSchema, tableName := h.Schema, ""
				if len(tblParts) >= 2 {
					tableSchema = tblParts[0]
					tableName = tblParts[1]
				} else if len(tblParts) == 1 {
					tableName = tblParts[0]
				}
				if tableSchema == "" || tableSchema == "-" {
					tableSchema = "public"
				}
				if tableName != "" {
					info.IndexTables[h.Schema+"."+h.Name] = tableSchema + "." + tableName
					info.IndexTables[h.Name] = tableSchema + "." + tableName
				}
			}
		case "SEQUENCE", "SEQUENCE OWNED BY":
			if m := pgSeqOwnedByRE.FindStringSubmatch(block); len(m) > 1 {
				target := m[1]
				if !strings.EqualFold(target, "NONE") {
					parts := splitPGQualifiedIdent(target)
					tableSchema, tableName := h.Schema, ""
					if len(parts) >= 3 {
						tableSchema = parts[len(parts)-3]
						tableName = parts[len(parts)-2]
					} else if len(parts) == 2 {
						tableName = parts[0]
					}
					if tableSchema == "" || tableSchema == "-" {
						tableSchema = "public"
					}
					if tableName != "" {
						info.SequenceOwners[h.Schema+"."+h.Name] = tableSchema + "." + tableName
						info.SequenceOwners[h.Name] = tableSchema + "." + tableName
					}
				}
			}
		case "DEFAULT":
			mAlt := pgAlterTableRE.FindStringSubmatch(block)
			mNext := pgNextvalRE.FindStringSubmatch(block)
			if len(mAlt) > 1 && len(mNext) > 1 {
				tblParts := splitPGQualifiedIdent(mAlt[1])
				tableSchema, tableName := h.Schema, ""
				if len(tblParts) >= 2 {
					tableSchema = tblParts[0]
					tableName = tblParts[1]
				} else if len(tblParts) == 1 {
					tableName = tblParts[0]
				}
				if tableSchema == "" || tableSchema == "-" {
					tableSchema = "public"
				}

				seqParts := splitPGQualifiedIdent(mNext[1])
				seqSchema, seqName := h.Schema, ""
				if len(seqParts) >= 2 {
					seqSchema = seqParts[0]
					seqName = seqParts[1]
				} else if len(seqParts) == 1 {
					seqName = seqParts[0]
				}
				if seqSchema == "" || seqSchema == "-" {
					seqSchema = "public"
				}
				if seqName != "" && tableName != "" {
					key := seqSchema + "." + seqName
					if info.SequenceOwners[key] == "" {
						info.SequenceOwners[key] = tableSchema + "." + tableName
						info.SequenceOwners[seqName] = tableSchema + "." + tableName
					}
				}
			}
		case "FK CONSTRAINT":
			mAlt := pgAlterTableRE.FindStringSubmatch(block)
			mConst := pgAddConstraintRE.FindStringSubmatch(block)
			mRef := pgReferencesRE.FindStringSubmatch(block)
			if len(mAlt) > 1 && len(mRef) > 1 {
				srcParts := splitPGQualifiedIdent(mAlt[1])
				srcSchema, srcTable := h.Schema, ""
				if len(srcParts) >= 2 {
					srcSchema = srcParts[0]
					srcTable = srcParts[1]
				} else if len(srcParts) == 1 {
					srcTable = srcParts[0]
				}
				if srcSchema == "" || srcSchema == "-" {
					srcSchema = "public"
				}

				refParts := splitPGQualifiedIdent(mRef[1])
				refSchema, refTable := h.Schema, ""
				if len(refParts) >= 2 {
					refSchema = refParts[0]
					refTable = refParts[1]
				} else if len(refParts) == 1 {
					refTable = refParts[0]
				}
				if refSchema == "" || refSchema == "-" {
					refSchema = "public"
				}

				constName := ""
				if len(mConst) > 1 {
					constName = unquotePGIdent(mConst[1])
				} else {
					// Fallback to name in header
					fields := strings.Fields(h.Name)
					if len(fields) > 1 {
						constName = fields[len(fields)-1]
					} else {
						constName = h.Name
					}
				}

				if srcTable != "" && constName != "" && refTable != "" {
					fkKey := srcSchema + "." + srcTable + "." + constName
					refKey := refSchema + "." + refTable
					info.FKReferences[fkKey] = refKey
					info.FKReferences[srcTable+"."+constName] = refKey
					info.FKReferences[constName] = refKey
				}
			}
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if h, ok := parsePGCommentHeader(line); ok {
			if currentHeader != nil {
				processBlock(*currentHeader, strings.Join(stmtLines, "\n"))
			}
			hh := h
			currentHeader = &hh
			stmtLines = nil
		} else if currentHeader != nil {
			stmtLines = append(stmtLines, line)
		}
	}
	if currentHeader != nil {
		processBlock(*currentHeader, strings.Join(stmtLines, "\n"))
	}
	return info, scanner.Err()
}

// FilterPGTOC filters the raw output of `pg_restore -l` to keep only the selected tables and their
// dependent objects (TABLE DATA, SEQUENCE, SEQUENCE OWNED BY, SEQUENCE SET, DEFAULT, CONSTRAINT,
// INDEX, TRIGGER, COMMENT, POLICY, ROW SECURITY, and needed SCHEMAs). FK constraints to unselected
// tables are dropped and returned as warnings. If any selected table is not found, an error is returned.
func FilterPGTOC(toc []byte, selectedObjects []string, info PGTOCInfo) ([]byte, []string, error) {
	if info.FKReferences == nil {
		info.FKReferences = make(map[string]string)
	}
	if info.SequenceOwners == nil {
		info.SequenceOwners = make(map[string]string)
	}
	if info.IndexTables == nil {
		info.IndexTables = make(map[string]string)
	}

	normalizeTable := func(schema, table string) string {
		schema = unquotePGIdent(schema)
		table = unquotePGIdent(table)
		if schema == "" || schema == "-" {
			schema = "public"
		}
		return schema + "." + table
	}

	normalizeObj := func(obj string) string {
		obj = strings.TrimSpace(obj)
		if obj == "" {
			return ""
		}
		parts := splitPGQualifiedIdent(obj)
		if len(parts) == 0 {
			return ""
		}
		if len(parts) == 1 {
			return "public." + parts[0]
		}
		return parts[0] + "." + parts[1]
	}

	selectedSet := make(map[string]bool, len(selectedObjects))
	for _, obj := range selectedObjects {
		norm := normalizeObj(obj)
		if norm != "" {
			selectedSet[norm] = true
		}
	}

	isTableSelected := func(schema, table string) bool {
		return selectedSet[normalizeTable(schema, table)]
	}

	neededSchemas := make(map[string]bool)
	seenTables := make(map[string]bool)

	// First pass: find which tables exist and which schemas are needed
	scanner := bufio.NewScanner(bytes.NewReader(toc))
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, ";") || trimmed == "" {
			continue
		}
		parts := strings.SplitN(trimmed, ";", 2)
		if len(parts) < 2 {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 3 {
			continue
		}
		desc := fields[2]
		switch desc {
		case "TABLE", "VIEW":
			if len(fields) >= 5 {
				schema, table := fields[3], fields[4]
				normKey := normalizeTable(schema, table)
				seenTables[normKey] = true
				if isTableSelected(schema, table) {
					normSchema := unquotePGIdent(schema)
					if normSchema == "" || normSchema == "-" {
						normSchema = "public"
					}
					neededSchemas[normSchema] = true
				}
			}
		}
	}

	// Verify all selected objects exist
	for _, obj := range selectedObjects {
		norm := normalizeObj(obj)
		if norm == "" {
			continue
		}
		if !seenTables[norm] {
			return nil, nil, fmt.Errorf("table %q not found in backup", obj)
		}
	}

	// Second pass: filter items
	var out bytes.Buffer
	var warnings []string
	scanner = bufio.NewScanner(bytes.NewReader(toc))

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, ";") || trimmed == "" {
			out.WriteString(line + "\n")
			continue
		}
		parts := strings.SplitN(trimmed, ";", 2)
		if len(parts) < 2 {
			out.WriteString(line + "\n")
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 3 {
			out.WriteString(line + "\n")
			continue
		}
		desc := fields[2]

		keep := false

		switch desc {
		case "SCHEMA":
			// fields: [catId, objId, "SCHEMA", "-", schemaName, owner]
			if len(fields) >= 5 {
				schemaName := unquotePGIdent(fields[4])
				keep = neededSchemas[schemaName]
			}

		case "TABLE", "VIEW":
			// fields: [catId, objId, "TABLE", schema, tableName, owner]
			if len(fields) >= 5 {
				keep = isTableSelected(fields[3], fields[4])
			}
		}

		// Handle multi-word desc
		descCombined := desc
		if desc == "TABLE" && len(fields) >= 4 && fields[3] == "DATA" {
			descCombined = "TABLE DATA"
			// fields: [catId, objId, "TABLE", "DATA", schema, tableName, owner]
			if len(fields) >= 6 {
				keep = isTableSelected(fields[4], fields[5])
			}
		} else if desc == "SEQUENCE" && len(fields) >= 5 && fields[3] == "OWNED" && fields[4] == "BY" {
			descCombined = "SEQUENCE OWNED BY"
			// fields: [catId, objId, "SEQUENCE", "OWNED", "BY", schema, seqName, owner]
			if len(fields) >= 7 {
				schema, seqName := unquotePGIdent(fields[5]), unquotePGIdent(fields[6])
				seqKey := normalizeTable(schema, seqName)
				ownerTable := info.SequenceOwners[seqKey]
				if ownerTable == "" {
					ownerTable = info.SequenceOwners[seqName]
				}
				if ownerTable != "" {
					if !strings.Contains(ownerTable, ".") {
						ownerTable = "public." + ownerTable
					}
					keep = selectedSet[ownerTable]
				} else {
					warnings = append(warnings, fmt.Sprintf("sequence %s.%s dropped: owning table could not be resolved", schema, seqName))
					keep = false
				}
			}
		} else if desc == "SEQUENCE" && len(fields) >= 4 && fields[3] == "SET" {
			descCombined = "SEQUENCE SET"
			// fields: [catId, objId, "SEQUENCE", "SET", schema, seqName, owner]
			if len(fields) >= 6 {
				schema, seqName := unquotePGIdent(fields[4]), unquotePGIdent(fields[5])
				seqKey := normalizeTable(schema, seqName)
				ownerTable := info.SequenceOwners[seqKey]
				if ownerTable == "" {
					ownerTable = info.SequenceOwners[seqName]
				}
				if ownerTable != "" {
					if !strings.Contains(ownerTable, ".") {
						ownerTable = "public." + ownerTable
					}
					keep = selectedSet[ownerTable]
				} else {
					warnings = append(warnings, fmt.Sprintf("sequence %s.%s dropped: owning table could not be resolved", schema, seqName))
					keep = false
				}
			}
		} else if desc == "SEQUENCE" && len(fields) >= 5 {
			// fields: [catId, objId, "SEQUENCE", schema, seqName, owner]
			schema, seqName := unquotePGIdent(fields[3]), unquotePGIdent(fields[4])
			seqKey := normalizeTable(schema, seqName)
			ownerTable := info.SequenceOwners[seqKey]
			if ownerTable == "" {
				ownerTable = info.SequenceOwners[seqName]
			}
			if ownerTable != "" {
				if !strings.Contains(ownerTable, ".") {
					ownerTable = "public." + ownerTable
				}
				keep = selectedSet[ownerTable]
			} else {
				warnings = append(warnings, fmt.Sprintf("sequence %s.%s dropped: owning table could not be resolved", schema, seqName))
				keep = false
			}
		} else if desc == "DEFAULT" {
			// fields: [catId, objId, "DEFAULT", schema, tableName, colName, owner]
			if len(fields) >= 5 {
				keep = isTableSelected(fields[3], fields[4])
			}
		} else if desc == "CONSTRAINT" && len(fields) >= 5 {
			// fields: [catId, objId, "CONSTRAINT", schema, tableName, constraintName, owner]
			keep = isTableSelected(fields[3], fields[4])
		} else if desc == "FK" && len(fields) >= 4 && fields[3] == "CONSTRAINT" {
			descCombined = "FK CONSTRAINT"
			// fields: [catId, objId, "FK", "CONSTRAINT", schema, tableName, constraintName, owner]
			if len(fields) >= 7 {
				schema, table, constraintName := unquotePGIdent(fields[4]), unquotePGIdent(fields[5]), unquotePGIdent(fields[6])
				if isTableSelected(schema, table) {
					fkKey := normalizeTable(schema, table) + "." + constraintName
					refTable := info.FKReferences[fkKey]
					if refTable == "" {
						refTable = info.FKReferences[table+"."+constraintName]
					}
					if refTable == "" {
						refTable = info.FKReferences[constraintName]
					}
					if refTable == "" {
						warnings = append(warnings, fmt.Sprintf("foreign key constraint %s on %s.%s dropped: referenced table could not be resolved", constraintName, schema, table))
						keep = false
					} else {
						if !strings.Contains(refTable, ".") {
							refTable = "public." + refTable
						}
						if selectedSet[refTable] {
							keep = true
						} else {
							warnings = append(warnings, fmt.Sprintf("foreign key constraint %s on %s.%s dropped: referenced table %s is not selected", constraintName, schema, table, refTable))
							keep = false
						}
					}
				}
			}
		} else if desc == "INDEX" && len(fields) >= 5 {
			// fields: [catId, objId, "INDEX", schema, indexName, owner]
			schema, indexName := unquotePGIdent(fields[3]), unquotePGIdent(fields[4])
			idxKey := normalizeTable(schema, indexName)
			table := info.IndexTables[idxKey]
			if table == "" {
				table = info.IndexTables[indexName]
			}
			if table != "" {
				if !strings.Contains(table, ".") {
					table = "public." + table
				}
				keep = selectedSet[table]
			} else {
				// If index cannot be resolved, check if it's a primary key index on a selected table
				for sel := range selectedSet {
					tName := sel
					if idx := strings.Index(sel, "."); idx != -1 {
						tName = sel[idx+1:]
					}
					if indexName == tName+"_pkey" || indexName == sel+"_pkey" {
						return nil, nil, fmt.Errorf("cannot restore table %q: primary key index %q could not be resolved", sel, indexName)
					}
				}
				warnings = append(warnings, fmt.Sprintf("index %s.%s dropped: owning table could not be resolved", schema, indexName))
				keep = false
			}
		} else if desc == "TRIGGER" && len(fields) >= 5 {
			// fields: [catId, objId, "TRIGGER", schema, tableName, triggerName, owner]
			keep = isTableSelected(fields[3], fields[4])
		} else if desc == "POLICY" && len(fields) >= 5 {
			// fields: [catId, objId, "POLICY", schema, tableName, policyName, owner]
			keep = isTableSelected(fields[3], fields[4])
		} else if desc == "ROW" && len(fields) >= 4 && fields[3] == "SECURITY" {
			descCombined = "ROW SECURITY"
			// fields: [catId, objId, "ROW", "SECURITY", schema, tableName, owner]
			if len(fields) >= 6 {
				keep = isTableSelected(fields[4], fields[5])
			}
		} else if desc == "COMMENT" {
			// fields: [catId, objId, "COMMENT", schema, "TABLE", tableName, owner]
			// or:     [catId, objId, "COMMENT", schema, "COLUMN", tableName.col, owner]
			if len(fields) >= 6 {
				schema := unquotePGIdent(fields[3])
				kind := fields[4]
				target := fields[5]
				tableName := unquotePGIdent(target)
				if kind == "COLUMN" {
					parts := splitPGQualifiedIdent(target)
					if len(parts) >= 2 {
						tableName = parts[0]
					}
				}
				keep = isTableSelected(schema, tableName)
			}
		}

		_ = descCombined
		if keep {
			out.WriteString(line + "\n")
		}
	}

	return out.Bytes(), warnings, nil
}
