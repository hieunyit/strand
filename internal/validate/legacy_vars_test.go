package validate

import (
	"errors"
	"testing"
)

func TestCompatM56_SessionBinlogHint(t *testing.T) {
	e := errors.New("Error 1146 (42S02): Table 'performance_schema.variables_by_thread' doesn't exist")
	if sessionBinlogHint(e) == "" {
		t.Error("expected hint for pre-5.7 server")
	}
	if sessionBinlogHint(errors.New("Error 1142: SELECT command denied")) != "" {
		t.Error("permission error must keep the grant advice")
	}
	if sessionBinlogHint(nil) != "" {
		t.Error("nil error")
	}
}
