package validate

import (
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// review R26: bảng đích hoặc bảng _strand_* không phải InnoDB phải bị từ chối (exit 3).
func TestR26_TargetEngineDecision(t *testing.T) {
	cases := []struct {
		name string
		rows []TableEngine
		bad  []string // chuỗi phải có trong lỗi; nil = chấp nhận
	}{
		{"all innodb", []TableEngine{{"app", "orders", "InnoDB"}, {"strand", "_strand_checkpoint", "innodb"}}, nil},
		{"empty", nil, nil},
		{"myisam data table", []TableEngine{{"app", "orders", "MyISAM"}, {"app", "users", "InnoDB"}}, []string{"app.orders (ENGINE=MyISAM)"}},
		{"memory checkpoint", []TableEngine{{"strand", "_strand_checkpoint", "MEMORY"}}, []string{"strand._strand_checkpoint (ENGINE=MEMORY)"}},
		{"view / null engine", []TableEngine{{"app", "v", ""}}, []string{"app.v (ENGINE=NULL)"}},
		{"several", []TableEngine{{"app", "b", "ARCHIVE"}, {"app", "a", "CSV"}}, []string{"app.a (ENGINE=CSV), app.b (ENGINE=ARCHIVE)"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := TargetEngineError(c.rows)
			if c.bad == nil {
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				return
			}
			if exitcode.GetExitCode(err) != exitcode.Rejected || !strings.Contains(err.Error(), exitcode.ErrTargetNotTransactional) {
				t.Fatalf("expected target_not_transactional (exit 3), got %v", err)
			}
			for _, b := range c.bad {
				if !strings.Contains(err.Error(), b) {
					t.Fatalf("error %q must mention %q", err, b)
				}
			}
		})
	}
}

// review R26: chỉ kiểm bảng đích (tên sau rename) và bảng _strand_* của schema metadata; bảng khác
// trong cùng schema (không đồng bộ) không bị từ chối oan.
func TestR26_SelectTargetEngines(t *testing.T) {
	want := []model.TableID{{Schema: "app", Table: "orders_v2"}, {Schema: "App", Table: "Users"}}
	rows := []TableEngine{
		{"app", "orders_v2", "MyISAM"},             // bảng đích
		{"app", "orders", "MyISAM"},                // tên nguồn trước rename: không phải đích
		{"app", "users", "InnoDB"},                 // khớp không phân biệt hoa thường
		{"app", "unrelated_log", "MyISAM"},         // không đồng bộ: bỏ qua
		{"meta", "_strand_worker_delta", "MEMORY"}, // metadata Strand
		{"other", "_strand_checkpoint", "MyISAM"},  // _strand_* ngoài schema metadata: không phải của pipeline này
	}
	got := selectTargetEngines(want, "meta", rows)
	var names []string
	for _, r := range got {
		names = append(names, r.Schema+"."+r.Table)
	}
	if strings.Join(names, ",") != "app.orders_v2,app.users,meta._strand_worker_delta" {
		t.Fatalf("selected %v", names)
	}
	err := TargetEngineError(got)
	if err == nil || !strings.Contains(err.Error(), "app.orders_v2") || !strings.Contains(err.Error(), "meta._strand_worker_delta") ||
		strings.Contains(err.Error(), "unrelated_log") {
		t.Fatalf("unexpected decision: %v", err)
	}
}
