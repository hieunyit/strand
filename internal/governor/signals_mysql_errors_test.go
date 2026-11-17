package governor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	mysqldrv "github.com/go-sql-driver/mysql"
)

const statusQ = "SHOW GLOBAL STATUS"

func statusRows() fakeResult {
	return fakeResult{
		cols: []string{"Variable_name", "Value"},
		rows: [][]driver.Value{{"Innodb_buffer_pool_wait_free", "0"}, {"Innodb_log_waits", "0"}},
	}
}

func syntaxErr() error { return &mysqldrv.MySQLError{Number: 1064, Message: "syntax"} }

func fakeMySQL(answer func(q string) fakeResult) *MySQLDBSignalReader {
	return NewMySQLDBSignalReader(sql.OpenDB(fakeConnector{answer: answer}), nil)
}

func TestMySQLSignals_AllQueriesFailReturnsError(t *testing.T) {
	r := fakeMySQL(func(string) fakeResult { return fakeResult{err: errors.New("denied")} })
	if _, err := r.ReadSignals(context.Background()); err == nil {
		t.Fatal("expected error when every signal query fails")
	}
}

func TestMySQLSignals_GlobalStatusFailureReturnsError(t *testing.T) {
	r := fakeMySQL(func(q string) fakeResult {
		if strings.HasPrefix(q, statusQ) {
			return fakeResult{err: errors.New("boom")}
		}
		return fakeResult{cols: []string{"Seconds_Behind_Source"}}
	})
	if _, err := r.ReadSignals(context.Background()); err == nil {
		t.Fatal("SHOW GLOBAL STATUS failure must be propagated")
	}
}

func TestMySQLSignals_MissingRequiredGlobalCounterReturnsError(t *testing.T) {
	r := fakeMySQL(func(q string) fakeResult {
		if strings.HasPrefix(q, statusQ) {
			return fakeResult{cols: []string{"Variable_name", "Value"}, rows: [][]driver.Value{{"Innodb_log_waits", "0"}}}
		}
		return fakeResult{cols: []string{"Seconds_Behind_Source"}}
	})
	if _, err := r.ReadSignals(context.Background()); err == nil {
		t.Fatal("missing required hard-signal counter must be reported")
	}
}

func TestMySQLSignals_ReplicaStatusQueryErrorPropagates(t *testing.T) {
	r := fakeMySQL(func(q string) fakeResult {
		if strings.HasPrefix(q, statusQ) {
			return statusRows()
		}
		return fakeResult{err: errors.New("access denied")}
	})
	if _, err := r.ReadSignals(context.Background()); err == nil {
		t.Fatal("non-syntax replica status error must not fall back silently")
	}
}

func TestMySQLSignals_UnsupportedReplicaFallsBackToSlave(t *testing.T) {
	var asked []string
	r := fakeMySQL(func(q string) fakeResult {
		asked = append(asked, q)
		switch {
		case strings.HasPrefix(q, statusQ):
			return statusRows()
		case q == "SHOW REPLICA STATUS":
			return fakeResult{err: syntaxErr()}
		}
		return one("Seconds_Behind_Master")(int64(9))
	})
	sig, err := r.ReadSignals(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sig.ReplicaLagSeconds != 9 {
		t.Fatalf("lag = %v, want 9 (asked %v)", sig.ReplicaLagSeconds, asked)
	}
}

func TestMySQLSignals_BothUnsupportedIsNotAnError(t *testing.T) {
	r := fakeMySQL(func(q string) fakeResult {
		if strings.HasPrefix(q, statusQ) {
			return statusRows()
		}
		return fakeResult{err: syntaxErr()}
	})
	sig, err := r.ReadSignals(context.Background())
	if err != nil || sig.ReplicaLagSeconds != 0 {
		t.Fatalf("sig=%+v err=%v", sig, err)
	}
}

func TestMySQLSignals_PrimaryWithoutReplicaRowsIsNotAnError(t *testing.T) {
	r := fakeMySQL(func(q string) fakeResult {
		if strings.HasPrefix(q, statusQ) {
			return statusRows()
		}
		return fakeResult{cols: []string{"Seconds_Behind_Source"}}
	})
	if _, err := r.ReadSignals(context.Background()); err != nil {
		t.Fatal(err)
	}
}
