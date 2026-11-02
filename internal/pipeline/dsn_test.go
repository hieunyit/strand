package pipeline

import (
	"testing"

	drivermysql "github.com/go-sql-driver/mysql"
)

// SG-1: kết nối target phải mang nhãn pipeline/instance để instance mới ngắt được kết nối của
// instance zombie. go-sql-driver FormatDSN làm rơi ConnectionAttributes.
func TestSG1_TargetDSNCarriesConnectionAttributes(t *testing.T) {
	dsn, err := targetDSN("u:p@tcp(h:3306)/strand", "pipe-1", "host/42/7")
	if err != nil {
		t.Fatal(err)
	}
	c, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if c.ConnectionAttributes != "strand_pipeline:pipe-1,strand_instance:host/42/7" {
		t.Fatalf("connection attributes lost: %q (dsn %s)", c.ConnectionAttributes, dsn)
	}
	if c.Params["foreign_key_checks"] != "0" || c.Params["time_zone"] == "" {
		t.Fatalf("session params lost: %v", c.Params)
	}
}
