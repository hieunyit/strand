package validate

import (
	"strings"
	"testing"

	"strand/internal/mysqlver"
)

func TestBinlogRowMetadataNote57(t *testing.T) {
	v, _ := mysqlver.Parse("5.7.44-log")
	note := binlogRowMetadataNote57(v)
	if !strings.Contains(note, "5.7") || strings.Contains(note, "SET PERSIST") || strings.Contains(note, "FULL is recommended") {
		t.Errorf("note: %s", note)
	}
}
