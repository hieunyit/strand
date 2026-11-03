package pg_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
	"strand/internal/reader/pg"
)

// AC6: decode trên physical standby (PG16+). Dựng primary + standby (pg_basebackup, streaming
// replication, physical slot, hot_standby_feedback=on) trên cùng một Docker network; reader tạo slot
// logical TRÊN STANDBY và đọc được Txn ghi trên primary.
func TestAC6_DecodeOnStandby(t *testing.T) {
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("tạo docker network: %v", err)
	}
	trackNet(nw)

	primary, err := startContainer(ctx, primaryRequest("postgres:18", pgArgs, nw.Name, "pg_primary"))
	if err != nil {
		t.Fatalf("khởi động primary: %v", err)
	}
	pdb := primary.db

	// Bảng + publication tạo trên primary, tới standby qua WAL.
	pub := setupTables(t, pdb, "sb", "CREATE TABLE sb (id int PRIMARY KEY, msg text)", "sb")

	standbyArgs := strings.Join(append(append([]string{}, pgArgs...),
		"-c", "hot_standby=on", "-c", "hot_standby_feedback=on"), " ")
	script := `set -e
until pg_isready -q -h pg_primary -p 5432 -U strand; do sleep 0.5; done
mkdir -p "$PGDATA" && chown postgres:postgres "$PGDATA" && chmod 700 "$PGDATA"
gosu postgres pg_basebackup -h pg_primary -U strand -D "$PGDATA" -R -X stream -C -S standby_phys
exec gosu postgres postgres ` + standbyArgs + `
`
	standby, err := startContainer(ctx, testcontainers.ContainerRequest{
		Image:        "postgres:18",
		ExposedPorts: []string{"5432/tcp"},
		Networks:     []string{nw.Name},
		Env:          map[string]string{"PGPASSWORD": "dev"},
		Entrypoint:   []string{"bash", "-c", script},
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept read-only connections"),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("khởi động standby: %v", err)
	}
	sdb := standby.db

	var inRecovery bool
	var ver int
	if err := sdb.QueryRow("SELECT pg_is_in_recovery(), current_setting('server_version_num')::int").Scan(&inRecovery, &ver); err != nil {
		t.Fatal(err)
	}
	if !inRecovery || ver < 160000 {
		t.Fatalf("container thứ hai phải là standby PG16+: in_recovery=%v version=%d", inRecovery, ver)
	}
	waitFor(t, time.Minute, "publication tới standby", func() bool {
		var n int
		_ = sdb.QueryRow("SELECT count(*) FROM pg_publication WHERE pubname = $1", pub).Scan(&n)
		return n == 1
	})

	// Tạo slot trên standby phải chờ bản ghi running-xacts từ primary: chủ động phát ra để test nhanh.
	stopSnap := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopSnap:
				return
			case <-time.After(500 * time.Millisecond):
				_, _ = pdb.Exec("SELECT pg_log_standby_snapshot()")
			}
		}
	}()
	cfg := pg.Config{DSN: standby.dsn, Slot: "sb_slot", Publication: pub, CreateSlot: true}
	r, err := pg.New(cfg)
	close(stopSnap)
	if err != nil {
		t.Fatalf("tạo slot trên standby: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	var slotOnStandby int
	if err := sdb.QueryRow("SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'sb_slot' AND slot_type = 'logical'").Scan(&slotOnStandby); err != nil || slotOnStandby != 1 {
		t.Fatalf("slot logical phải nằm trên standby: %d %v", slotOnStandby, err)
	}
	var onPrimary int
	_ = pdb.QueryRow("SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'sb_slot'").Scan(&onPrimary)
	if onPrimary != 0 {
		t.Fatal("slot không được nằm trên primary")
	}

	ch, err := r.Start(ctx, nil)
	if err != nil {
		t.Fatalf("Start trên standby: %v", err)
	}
	for i := 1; i <= 50; i++ {
		mustExec(t, pdb, "INSERT INTO sb VALUES ($1, $2)", i, fmt.Sprintf("tu primary %d", i))
	}
	mustExec(t, pdb, "BEGIN; INSERT INTO sb VALUES (100, 'prepared'); PREPARE TRANSACTION 'sb_g';")
	mustExec(t, pdb, "COMMIT PREPARED 'sb_g'")
	insertMarker(t, pdb, "sb", "end")
	// WAL tới standby rồi mới được decode; đẩy running-xacts để walsender trên standby không phải chờ.
	go func() {
		for i := 0; i < 20; i++ {
			_, _ = pdb.Exec("SELECT pg_log_standby_snapshot()")
			time.Sleep(500 * time.Millisecond)
		}
	}()
	got, _ := newTxnReader(t, ch).untilMarker("end", time.Minute)
	if len(got) != 51 {
		t.Fatalf("standby phải decode đủ 51 Txn từ primary, được %d", len(got))
	}
	for i, txn := range got[:50] {
		rows := txnRows(t, txn)
		if len(rows) != 1 || col(t, rows[0], "id") != int64(i+1) || col(t, rows[0], "msg") != fmt.Sprintf("tu primary %d", i+1) {
			t.Fatalf("Txn %d sai: %+v", i, rows)
		}
	}
	if rows := txnRows(t, got[50]); col(t, rows[0], "id") != int64(100) {
		t.Fatalf("Txn COMMIT PREPARED sai: %+v", rows)
	}
	v, s, tp := r.Protocol()
	t.Logf("standby: proto=%d streaming=%v two_phase=%v", v, s, tp)

	// Failover slot không tạo được trên standby → từ chối rõ ràng (exit 3).
	_, err = pg.New(pg.Config{DSN: standby.dsn, Slot: "sb_fail", Publication: pub, CreateSlot: true, Failover: true})
	var e *exitcode.Error
	if !errors.As(err, &e) || e.ExitCode != exitcode.Rejected {
		t.Fatalf("Failover=true trên standby phải bị từ chối exit 3, được %v", err)
	}
}
