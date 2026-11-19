package breaker_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/tools/diagnostics/breaker"
)

func ownMariaDBFixture(t *testing.T) (*sql.DB, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Spin up isolated MariaDB 10.11 container with small redo log to simulate redo pressure
	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mariadb:10.11",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MARIADB_ROOT_PASSWORD": "lab-test-pass",
				"MARIADB_DATABASE":      "breaker_lab",
			},
			Cmd: []string{
				"--innodb-log-file-size=4M",
				"--innodb-log-buffer-size=1M",
				"--innodb-flush-log-at-trx-commit=1",
			},
			WaitingFor: wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	}

	c, err := testcontainers.GenericContainer(ctx, req)
	if c != nil {
		t.Cleanup(func() {
			termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer termCancel()
			if err := c.Terminate(termCtx); err != nil {
				t.Logf("cleanup own MariaDB container: %v", err)
			}
		})
	}
	if err != nil {
		t.Skipf("Docker or MariaDB not available in current environment: %v", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get host: %v", err)
	}
	port, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatalf("failed to get port: %v", err)
	}

	dsn := fmt.Sprintf("root:lab-test-pass@tcp(%s:%s)/breaker_lab?parseTime=true", host, port.Port())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	var pingErr error
	for attempt := 0; attempt < 30; attempt++ {
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
		pingErr = db.PingContext(pingCtx)
		pingCancel()
		if pingErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if pingErr != nil {
		t.Fatalf("failed to ping MariaDB after retries: %v", pingErr)
	}

	return db, dsn
}

func TestMariaDBRedoPressureReproduction(t *testing.T) {
	db, dsn := ownMariaDBFixture(t)
	ctx := context.Background()

	// 1. Initial diagnostic collection
	collector := breaker.NewDiagnosticCollector(db, dsn)
	snap1, err := collector.Collect(ctx)
	if err != nil {
		t.Fatalf("snap1 collection failed: %v", err)
	}
	t.Logf("[LAB REPRODUCTION] Baseline Server Version: %s", snap1.ServerVersion)
	t.Logf("[LAB REPRODUCTION] Config innodb_log_file_size: %d bytes, log_buffer_size: %d bytes",
		snap1.Variables.InnodbLogFileSize, snap1.Variables.InnodbLogBufferSize)

	// 2. Setup table for large write transaction
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS redo_heavy (
			id INT AUTO_INCREMENT PRIMARY KEY,
			payload LONGBLOB NOT NULL
		) ENGINE=InnoDB;
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// 3. Generate large transaction pressure with 5MB total payload across batches
	blob := make([]byte, 64*1024) // 64 KB per row
	_, _ = rand.Read(blob)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx failed: %v", err)
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO redo_heavy (payload) VALUES (?)")
	if err != nil {
		t.Fatalf("prepare insert failed: %v", err)
	}
	for i := 0; i < 80; i++ { // 80 * 64KB = 5.12 MB, exceeding 4MB log file size
		if _, err := stmt.ExecContext(ctx, blob); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert failed at %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// 4. Post-pressure diagnostic collection
	snap2, err := collector.Collect(ctx)
	if err != nil {
		t.Fatalf("snap2 collection failed: %v", err)
	}

	// 5. Verify counter deltas
	if snap2.StatusDelta == nil {
		t.Fatalf("Expected status delta between snap1 and snap2")
	}
	delta := *snap2.StatusDelta

	t.Logf("[LAB REPRODUCTION] Exact Counter Deltas Recorded:")
	t.Logf("  ΔInnodb_os_log_written: %d bytes", delta.DeltaInnodbOsLogWritten)
	t.Logf("  ΔInnodb_os_log_fsyncs:  %d", delta.DeltaInnodbOsLogFsyncs)
	t.Logf("  ΔInnodb_data_fsyncs:    %d", delta.DeltaInnodbDataFsyncs)
	t.Logf("  ΔInnodb_data_writes:    %d", delta.DeltaInnodbDataWrites)
	t.Logf("  ΔInnodb_log_waits:      %d", delta.DeltaInnodbLogWaits)
	t.Logf("  ΔHandler_commit:        %d", delta.DeltaHandlerCommit)

	if delta.DeltaInnodbOsLogWritten == 0 {
		t.Errorf("Expected DeltaInnodbOsLogWritten > 0 after 5MB payload")
	}
	if delta.DeltaInnodbDataWrites == 0 && delta.DeltaInnodbOsLogFsyncs == 0 && delta.DeltaInnodbDataFsyncs == 0 {
		t.Errorf("Expected writes or fsyncs > 0 on commit")
	}
	if delta.DeltaHandlerCommit == 0 {
		t.Errorf("Expected DeltaHandlerCommit > 0")
	}

	// 6. Explicitly distinguish LAB reproduction vs USER HOST:
	t.Log("[LAB REPRODUCTION] Lab fixture confirms heavy transaction induces measurable redo log writes and fsync pressure.")
	t.Log("[USER HOST STATUS] UNRESOLVED. Lab test demonstrates the diagnostic mechanism, but cannot prove the exact cause on user host without incident logs.")
}
