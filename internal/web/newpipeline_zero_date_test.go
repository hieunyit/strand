package web

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"strand/internal/config"
)

// Wizard New pipeline: ô "MySQL zero dates" (chỉ MySQL → PostgreSQL) ghi tables.zero_date; YAML sinh ra
// được config chấp nhận; cặp khác hoặc giá trị lạ bị từ chối ở field zero_date.
func TestNewPipelineZeroDate(t *testing.T) {
	e, c, csrf := npEnv(t)
	resp, page := e.do(c, "GET", "/pipelines/new", "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, `name="zero_date"`) ||
		!strings.Contains(page, `data-when="src:mysql,mariadb;dst:postgres"`) || !strings.Contains(page, `value="epoch"`) {
		t.Fatalf("expected the zero date choice for MySQL → PostgreSQL in the wizard, got %d", resp.StatusCode)
	}
	for _, tt := range []struct {
		name, from, to, zeroDate string
		status                   int
		wantYAML                 string // rỗng: không có dòng zero_date
	}{
		{name: "zd-default", from: "mysql", to: "postgres", status: 200},
		{name: "zd-reject", from: "mysql", to: "postgres", zeroDate: "reject", status: 200},
		{name: "zd-epoch", from: "mysql", to: "postgres", zeroDate: "epoch", status: 200, wantYAML: "\n  zero_date: epoch\n"},
		{name: "zd-null", from: "mysql", to: "postgres", zeroDate: "set_null", status: 200, wantYAML: "\n  zero_date: set_null\n"},
		{name: "zd-mysql-target", from: "mysql", to: "mysql", zeroDate: "epoch", status: 400},
		{name: "zd-unknown", from: "mysql", to: "postgres", zeroDate: "null", status: 400},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, _, y, body := createNPForm(t, e, c, csrf, url.Values{
				"name": {tt.name}, "from": {tt.from}, "to": {tt.to}, "database": {"shop"}, "zero_date": {tt.zeroDate},
			})
			if code != tt.status {
				t.Fatalf("create: got %d, want %d: %s", code, tt.status, body)
			}
			if code == http.StatusBadRequest {
				var result struct {
					Fields map[string]string `json:"fields"`
				}
				if err := json.Unmarshal([]byte(body), &result); err != nil || result.Fields["zero_date"] == "" {
					t.Fatalf("expected a zero_date field error, got %s", body)
				}
				return
			}
			if got := strings.Contains(y, "zero_date:"); got != (tt.wantYAML != "") || (tt.wantYAML != "" && !strings.Contains(y, tt.wantYAML)) {
				t.Fatalf("zero_date YAML: want %q:\n%s", tt.wantYAML, y)
			}
			if _, err := config.LoadYAMLStrictWithEnv([]byte(y), map[string]string{"STRAND_PIPELINE_TARGET_DSN": "postgres://u@h/db", "STRAND_PIPELINE_SOURCE_DSN": "root@tcp(h:3306)/shop", "STRAND_PIPELINE_SOURCE_PASSWORD": "x", "STRAND_PIPELINE_TARGET_PASSWORD": "x"}); err != nil {
				t.Fatalf("generated YAML rejected by config: %v\n%s", err, y)
			}
		})
	}
}
