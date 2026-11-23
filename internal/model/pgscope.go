package model

import "strings"

var pgRegclassEscape = strings.NewReplacer(`\`, `\\`, "'", "''")

// PGRelationRows limits a scan or mutation to the named relation in traditional
// inheritance. A declarative partition root represents its partitions in CDC.
func PGRelationRows(quotedTable, qualifier string) string {
	name := "E'" + pgRegclassEscape.Replace(quotedTable) + "'::pg_catalog.regclass"
	return "(" + qualifier + "tableoid = " + name +
		" OR (SELECT relkind = 'p' FROM pg_catalog.pg_class WHERE oid = " + name + "))"
}
