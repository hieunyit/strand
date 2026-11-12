package pipeline

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"strand/internal/checkpoint"
	"strand/internal/metrics"
	"strand/internal/model"
)

// Hồi quy (e2e TestG1_E2E_SeedFromXtraBackupZeroTableReads): cửa sổ catch-up sau seed bận làm tập khóa đã
// chạm của một bảng vượt giới hạn; post-seed verify khi đó chỉ báo incomplete và giữ VerifyPending mãi
// (pipeline không bao giờ synced, `verify --mode full` ở process khác không gỡ được). Giờ bảng tràn được
// verify TOÀN BỘ: lệch ở khóa không nằm trong tập đã chạm vẫn bị phát hiện, và bảng khớp thì qua.
func TestPostSeedOverflowVerifiesWholeTable(t *testing.T) {
	src, _ := startTestDB(t, "mysql")
	dst, _ := startTestDB(t, "mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ddl := "CREATE TABLE app.ev (id BIGINT PRIMARY KEY, v INT)"
	var ins strings.Builder
	ins.WriteString("INSERT INTO app.ev VALUES ")
	for i := 1; i <= 2500; i++ {
		if i > 1 {
			ins.WriteString(",")
		}
		ins.WriteString("(" + strconv.Itoa(i) + ",1)")
	}
	if _, err := src.ExecContext(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.ExecContext(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := src.ExecContext(ctx, ins.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.ExecContext(ctx, ins.String()); err != nil {
		t.Fatal(err)
	}
	st := checkpoint.NewMySQLStore(dst, "pst", model.EngineMySQL)
	if err := st.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	epoch, err := st.AdvanceEpoch(ctx, "pst", "test", "h")
	if err != nil {
		t.Fatal(err)
	}
	// Checkpoint giả phủ toàn bộ GTID của source: bước kiểm lại row lệch không phải chờ pipeline apply.
	var executed string
	if err := src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.ExecContext(ctx, "REPLACE INTO _strand_checkpoint (pipeline_id, epoch, applied, updated_at) VALUES (?, ?, ?, NOW(6))",
		"pst", epoch, strings.ReplaceAll(executed, "\n", "")); err != nil {
		t.Fatal(err)
	}
	id := model.TableID{Schema: "app", Table: "ev"}
	p := &Pipeline{
		id: "pst", src: src, dst: dst, srcEngine: model.EngineMySQL, dstEngine: model.EngineMySQL,
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: metrics.NewMetrics(prometheus.NewRegistry()),
		filter:  &tableFilter{include: []string{"app.*"}},
		tables:  map[model.TableID]*tableMeta{id: {ID: id, Columns: []string{"id", "v"}, PK: []string{"id"}, PKNumeric: true}},
	}
	keys := map[model.TableID][]int64{id: {1, 2}} // phần khóa còn giữ được trước khi tràn

	// Bảng khớp: verify toàn bộ qua.
	if _, err := p.verifyTablesKeys(ctx, 0, keys, nil, map[model.TableID]bool{id: true}); err != nil {
		t.Fatalf("identical table, whole-table verify: %v", err)
	}
	// Lệch ở khóa ngoài tập đã chạm (khóa bị bỏ khi tràn).
	if _, err := dst.ExecContext(ctx, "UPDATE app.ev SET v = 2 WHERE id = 2400"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.verifyTablesKeys(ctx, 0, keys, nil, nil); err != nil {
		t.Fatalf("control: only touched keys verified, mismatch at 2400 should be invisible, got %v", err)
	}
	_, err = p.verifyTablesKeys(ctx, 0, keys, nil, map[model.TableID]bool{id: true})
	if err == nil || !strings.Contains(err.Error(), "2400") {
		t.Fatalf("whole-table verify of an overflowed table missed the mismatch at id 2400: %v", err)
	}
}
