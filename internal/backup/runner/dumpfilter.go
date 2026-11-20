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
	FKReferences   map[string]string // constraint_name -> referenced table (e.g. "public.users" or "users")
	SequenceOwners map[string]string // sequence_name -> owning table (e.g. "public.orders" or "orders")
	IndexTables    map[string]string // index_name -> table (e.g. "public.orders" or "orders")
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

	selectedMap := make(map[string]bool, len(selectedObjects)*2)
	for _, obj := range selectedObjects {
		obj = strings.TrimSpace(obj)
		if obj == "" {
			continue
		}
		selectedMap[obj] = true
		if !strings.Contains(obj, ".") {
			selectedMap["public."+obj] = true
		} else if strings.HasPrefix(obj, "public.") {
			selectedMap[strings.TrimPrefix(obj, "public.")] = true
		}
	}

	isTableSelected := func(schema, table string) bool {
		if schema == "" || schema == "-" {
			return selectedMap[table] || selectedMap["public."+table]
		}
		return selectedMap[table] || selectedMap[schema+"."+table]
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
				seenTables[table] = true
				if schema != "-" && schema != "" {
					seenTables[schema+"."+table] = true
				}
				if isTableSelected(schema, table) && schema != "-" && schema != "" {
					neededSchemas[schema] = true
				}
			}
		}
	}

	// Verify all selected objects exist
	for _, obj := range selectedObjects {
		obj = strings.TrimSpace(obj)
		if obj == "" {
			continue
		}
		if !seenTables[obj] && !seenTables["public."+obj] && !seenTables[strings.TrimPrefix(obj, "public.")] {
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
				schemaName := fields[4]
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
				schema, seqName := fields[5], fields[6]
				ownerTable := info.SequenceOwners[seqName]
				if ownerTable == "" {
					ownerTable = info.SequenceOwners[schema+"."+seqName]
				}
				if ownerTable == "" {
					// Fallback: infer if seqName has table prefix
					for t := range selectedMap {
						base := t
						if idx := strings.Index(t, "."); idx != -1 {
							base = t[idx+1:]
						}
						if strings.HasPrefix(seqName, base+"_") {
							ownerTable = t
							break
						}
					}
				}
				if ownerTable != "" {
					keep = selectedMap[ownerTable]
				}
			}
		} else if desc == "SEQUENCE" && len(fields) >= 4 && fields[3] == "SET" {
			descCombined = "SEQUENCE SET"
			// fields: [catId, objId, "SEQUENCE", "SET", schema, seqName, owner]
			if len(fields) >= 6 {
				schema, seqName := fields[4], fields[5]
				ownerTable := info.SequenceOwners[seqName]
				if ownerTable == "" {
					ownerTable = info.SequenceOwners[schema+"."+seqName]
				}
				if ownerTable == "" {
					for t := range selectedMap {
						base := t
						if idx := strings.Index(t, "."); idx != -1 {
							base = t[idx+1:]
						}
						if strings.HasPrefix(seqName, base+"_") {
							ownerTable = t
							break
						}
					}
				}
				if ownerTable != "" {
					keep = selectedMap[ownerTable]
				}
			}
		} else if desc == "SEQUENCE" && len(fields) >= 5 {
			// fields: [catId, objId, "SEQUENCE", schema, seqName, owner]
			schema, seqName := fields[3], fields[4]
			ownerTable := info.SequenceOwners[seqName]
			if ownerTable == "" {
				ownerTable = info.SequenceOwners[schema+"."+seqName]
			}
			if ownerTable == "" {
				for t := range selectedMap {
					base := t
					if idx := strings.Index(t, "."); idx != -1 {
						base = t[idx+1:]
					}
					if strings.HasPrefix(seqName, base+"_") {
						ownerTable = t
						break
					}
				}
			}
			if ownerTable != "" {
				keep = selectedMap[ownerTable]
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
				schema, table, constraintName := fields[4], fields[5], fields[6]
				if isTableSelected(schema, table) {
					refTable := info.FKReferences[constraintName]
					if refTable == "" {
						refTable = info.FKReferences[schema+"."+table+"."+constraintName]
					}
					if refTable != "" && isTableSelected("", refTable) {
						keep = true
					} else {
						warnings = append(warnings, fmt.Sprintf("foreign key constraint %s on %s.%s dropped: referenced table %s is not selected", constraintName, schema, table, refTable))
						keep = false
					}
				}
			}
		} else if desc == "INDEX" && len(fields) >= 5 {
			// fields: [catId, objId, "INDEX", schema, indexName, owner]
			schema, indexName := fields[3], fields[4]
			table := info.IndexTables[indexName]
			if table == "" {
				table = info.IndexTables[schema+"."+indexName]
			}
			if table == "" {
				// Fallback: check if index name has table prefix
				for t := range selectedMap {
					base := t
					if idx := strings.Index(t, "."); idx != -1 {
						base = t[idx+1:]
					}
					if strings.Contains(indexName, base) {
						table = t
						break
					}
				}
			}
			if table != "" {
				keep = selectedMap[table]
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
				schema := fields[3]
				kind := fields[4]
				target := fields[5]
				tableName := target
				if kind == "COLUMN" {
					if idx := strings.Index(target, "."); idx != -1 {
						tableName = target[:idx]
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
