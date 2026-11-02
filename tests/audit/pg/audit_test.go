package pg_audit

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

// startPostgres khởi tạo container PostgreSQL độc lập phục vụ test.
func startPostgres(t *testing.T, image string, extraCmd ...string) (*sql.DB, string, int, func()) {
	t.Helper()
	ctx := context.Background()

	cmd := []string{
		"postgres",
		"-c", "wal_level=logical",
		"-c", "max_wal_senders=10",
		"-c", "max_replication_slots=10",
	}
	cmd = append(cmd, extraCmd...)

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "strand",
			"POSTGRES_PASSWORD": "dev",
			"POSTGRES_DB":       "strand",
		},
		Cmd:        cmd,
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container %s: %v", image, err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("failed to get container host: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("failed to get mapped port: %v", err)
	}
	port := mappedPort.Int()

	dsn := fmt.Sprintf("postgres://strand:dev@%s:%d/strand?sslmode=disable", host, port)

	var db *sql.DB
	for i := 0; i < 60; i++ {
		db, err = sql.Open("pgx", dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				break
			}
			_ = db.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("failed to ping postgres at %s:%d: %v", host, port, err)
	}

	// Cấu hình pg_hba.conf cho replication connection
	_, _, err = container.Exec(ctx, []string{"bash", "-c", "echo 'host replication all all trust' >> /var/lib/postgresql/data/pg_hba.conf"})
	if err != nil {
		_ = db.Close()
		_ = container.Terminate(ctx)
		t.Fatalf("failed to append replication rule to pg_hba.conf: %v", err)
	}
	if _, err := db.Exec("SELECT pg_reload_conf()"); err != nil {
		_ = db.Close()
		_ = container.Terminate(ctx)
		t.Fatalf("failed to reload postgres configuration: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = container.Terminate(context.Background())
	}

	return db, host, port, cleanup
}

func connectReplication(ctx context.Context, host string, port int) (*pgconn.PgConn, error) {
	connStr := fmt.Sprintf("postgres://strand:dev@%s:%d/strand?sslmode=disable&replication=database", host, port)
	return pgconn.Connect(ctx, connStr)
}

// AC1: pgoutput proto v1–v4 chạy trên PG 16, 17, 18.
func TestAC1_ProtoV1toV4_PG16_17_18(t *testing.T) {
	pgVersions := []string{"postgres:16", "postgres:17", "postgres:18"}

	for _, image := range pgVersions {
		image := image
		t.Run(image, func(t *testing.T) {
			db, host, port, cleanup := startPostgres(t, image)
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			_, err := db.ExecContext(ctx, "CREATE TABLE t_proto (id int PRIMARY KEY, val text);")
			if err != nil {
				t.Fatalf("failed to create table: %v", err)
			}
			_, err = db.ExecContext(ctx, "CREATE PUBLICATION pub_proto FOR TABLE t_proto;")
			if err != nil {
				t.Fatalf("failed to create publication: %v", err)
			}

			// Kiểm tra từng proto version từ 1 đến 4
			for protoVer := 1; protoVer <= 4; protoVer++ {
				slotName := fmt.Sprintf("slot_proto_%d", protoVer)

				replConn, err := connectReplication(ctx, host, port)
				if err != nil {
					t.Fatalf("failed to connect replication for proto %d: %v", protoVer, err)
				}

				_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
					Mode: pglogrepl.LogicalReplication,
				})
				if err != nil {
					_ = replConn.Close(ctx)
					t.Fatalf("failed to create slot %s on %s: %v", slotName, image, err)
				}

				pluginArgs := []string{
					fmt.Sprintf("proto_version '%d'", protoVer),
					"publication_names 'pub_proto'",
				}
				err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
					PluginArgs: pluginArgs,
					Mode:       pglogrepl.LogicalReplication,
				})
				if err != nil {
					_ = replConn.Close(ctx)
					t.Fatalf("failed to start replication proto %d on %s: %v", protoVer, image, err)
				}

				// Insert một bản ghi
				testVal := fmt.Sprintf("val_proto_%d", protoVer)
				_, err = db.ExecContext(ctx, "INSERT INTO t_proto (id, val) VALUES ($1, $2)", protoVer, testVal)
				if err != nil {
					_ = replConn.Close(ctx)
					t.Fatalf("failed to insert data for proto %d: %v", protoVer, err)
				}

				// Đọc message qua replication stream
				receivedInsert := false
				for !receivedInsert {
					select {
					case <-ctx.Done():
						t.Fatalf("timeout waiting for proto %d insert message on %s", protoVer, image)
					default:
					}

					rawMsg, err := replConn.ReceiveMessage(ctx)
					if err != nil {
						t.Fatalf("failed to receive replication message: %v", err)
					}

					if cd, ok := rawMsg.(*pgproto3.CopyData); ok {
						switch cd.Data[0] {
						case pglogrepl.PrimaryKeepaliveMessageByteID:
							pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
							if err == nil && pkm.ReplyRequested {
								_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pkm.ServerWALEnd})
							}
						case pglogrepl.XLogDataByteID:
							xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
							if err != nil {
								t.Fatalf("failed to parse XLogData: %v", err)
							}
							walData := xld.WALData
							_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: xld.WALStart})

							var msg pglogrepl.Message
							if protoVer == 1 {
								msg, err = pglogrepl.Parse(walData)
							} else {
								msg, err = pglogrepl.ParseV2(walData, false)
							}
							if err != nil {
								t.Fatalf("failed to parse message proto %d: %v", protoVer, err)
							}

							switch msg.(type) {
							case *pglogrepl.InsertMessage:
								receivedInsert = true
								t.Logf("[%s proto %d] Nhận InsertMessage thành công", image, protoVer)
							case *pglogrepl.InsertMessageV2:
								receivedInsert = true
								t.Logf("[%s proto %d] Nhận InsertMessageV2 thành công", image, protoVer)
							}
						}
					}
				}

				_ = replConn.Close(ctx)

				// Xóa slot sau khi test xong (retry ngắn nếu backend postgres đang ngắt kết nối)
				var dropErr error
				for retry := 0; retry < 20; retry++ {
					_, dropErr = db.ExecContext(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
					if dropErr == nil {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				if dropErr != nil {
					t.Fatalf("failed to drop slot %s: %v", slotName, dropErr)
				}
			}
		})
	}
}

// AC2: Nhận đúng transaction đang streaming (proto v2+).
func TestAC2_StreamingTxn_ProtoV2(t *testing.T) {
	// Giới hạn logical_decoding_work_mem = 64kB để ép PostgreSQL stream transaction đang dở
	db, host, port, cleanup := startPostgres(t, "postgres:16", "-c", "logical_decoding_work_mem=64kB")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "CREATE TABLE t_stream (id int PRIMARY KEY, content text);")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.ExecContext(ctx, "CREATE PUBLICATION pub_stream FOR TABLE t_stream;")
	if err != nil {
		t.Fatalf("failed to create publication: %v", err)
	}

	replConn, err := connectReplication(ctx, host, port)
	if err != nil {
		t.Fatalf("failed to connect replication: %v", err)
	}
	defer replConn.Close(ctx)

	slotName := "slot_stream"
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil {
		t.Fatalf("failed to create slot: %v", err)
	}
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	}()

	err = pglogrepl.StartReplication(ctx, replConn, slotName, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '2'", "streaming 'on'", "publication_names 'pub_stream'"},
		Mode:       pglogrepl.LogicalReplication,
	})
	if err != nil {
		t.Fatalf("failed to start replication: %v", err)
	}

	// Chạy transaction lớn (>64kB) để trigger streaming
	t.Log("Bắt đầu insert batch vào t_stream...")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	// 500 rows x 2KB = 1MB > 64kB logical_decoding_work_mem
	_, err = tx.ExecContext(ctx, "INSERT INTO t_stream SELECT g, repeat('x', 2048) FROM generate_series(1, 500) g;")
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("failed to insert batch: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}
	t.Log("Commit batch thành công, bắt đầu đọc replication stream...")

	var receivedStreamStart, receivedStreamStop, receivedStreamCommit bool
	var streamedInserts int
	inStream := false

	for !(receivedStreamStart && receivedStreamStop && receivedStreamCommit) {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for streaming messages (start=%v, stop=%v, commit=%v, inserts=%d)",
				receivedStreamStart, receivedStreamStop, receivedStreamCommit, streamedInserts)
		default:
		}

		rawMsg, err := replConn.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("failed to receive message: %v", err)
		}

		if cd, ok := rawMsg.(*pgproto3.CopyData); ok {
			switch cd.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
				if err == nil && pkm.ReplyRequested {
					_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pkm.ServerWALEnd})
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
				if err != nil {
					t.Fatalf("failed to parse XLogData: %v", err)
				}
				walData := xld.WALData
				_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: xld.WALStart})

				msg, err := pglogrepl.ParseV2(walData, inStream)
				if err != nil {
					t.Fatalf("failed to parse V2 message: %v", err)
				}

				switch m := msg.(type) {
				case *pglogrepl.StreamStartMessageV2:
					receivedStreamStart = true
					inStream = true
					t.Logf("Nhận StreamStartMessageV2: Xid=%d, FirstSegment=%d", m.Xid, m.FirstSegment)
				case *pglogrepl.StreamStopMessageV2:
					receivedStreamStop = true
					inStream = false
					t.Log("Nhận StreamStopMessageV2")
				case *pglogrepl.InsertMessageV2:
					if inStream {
						streamedInserts++
					}
				case *pglogrepl.StreamCommitMessageV2:
					receivedStreamCommit = true
					inStream = false
					t.Logf("Nhận StreamCommitMessageV2: Xid=%d, CommitLSN=%s", m.Xid, m.CommitLSN)
				}
			}
		}
	}

	if streamedInserts == 0 {
		t.Fatalf("kỳ vọng nhận các InsertMessageV2 trong stream segment nhưng không có")
	}
	t.Logf("AC2 PASS: Nhận đủ StreamStart, %d streamed InsertMessageV2, StreamStop, StreamCommit", streamedInserts)
}

// AC3: Tạo slot kèm exported snapshot; consistent_point khớp vị trí bắt đầu stream.
func TestAC3_ExportSnapshot_ConsistentPoint(t *testing.T) {
	db, host, port, cleanup := startPostgres(t, "postgres:16")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "CREATE TABLE t_snap (id int PRIMARY KEY, val text);")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.ExecContext(ctx, "CREATE PUBLICATION pub_snap FOR TABLE t_snap;")
	if err != nil {
		t.Fatalf("failed to create publication: %v", err)
	}

	// Ghi 1 row trước khi tạo snapshot
	_, err = db.ExecContext(ctx, "INSERT INTO t_snap VALUES (1, 'before_snap');")
	if err != nil {
		t.Fatalf("failed to insert initial row: %v", err)
	}

	replConn, err := connectReplication(ctx, host, port)
	if err != nil {
		t.Fatalf("failed to connect replication: %v", err)
	}
	defer replConn.Close(ctx)

	slotName := "slot_snap"
	crsr, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		t.Fatalf("failed to create replication slot with EXPORT_SNAPSHOT: %v", err)
	}
	defer func() {
		_, _ = db.Exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	}()

	if crsr.SnapshotName == "" {
		t.Fatalf("SnapshotName trả về rỗng")
	}
	if crsr.ConsistentPoint == "" {
		t.Fatalf("ConsistentPoint trả về rỗng")
	}
	t.Logf("Tạo slot thành công: SnapshotName=%s, ConsistentPoint=%s", crsr.SnapshotName, crsr.ConsistentPoint)

	consistentLSN, err := pglogrepl.ParseLSN(crsr.ConsistentPoint)
	if err != nil {
		t.Fatalf("failed to parse ConsistentPoint %s to LSN: %v", crsr.ConsistentPoint, err)
	}

	// Kiểm tra exported snapshot bằng transaction REPEATABLE READ trên connection thường
	func() {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			t.Fatalf("failed to begin tx: %v", err)
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", crsr.SnapshotName))
		if err != nil {
			t.Fatalf("failed to set transaction snapshot: %v", err)
		}

		var count int
		err = tx.QueryRowContext(ctx, "SELECT count(*) FROM t_snap;").Scan(&count)
		if err != nil {
			t.Fatalf("failed to query count in snapshot: %v", err)
		}
		if count != 1 {
			t.Fatalf("snapshot không nhìn thấy đúng 1 row ban đầu, actual count=%d", count)
		}
	}()

	// Ghi thêm row 2 SAU snapshot
	_, err = db.ExecContext(ctx, "INSERT INTO t_snap VALUES (2, 'after_snap');")
	if err != nil {
		t.Fatalf("failed to insert row 2: %v", err)
	}

	// Bắt đầu stream replication từ đúng vị trí consistent_point
	err = pglogrepl.StartReplication(ctx, replConn, slotName, consistentLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names 'pub_snap'"},
		Mode:       pglogrepl.LogicalReplication,
	})
	if err != nil {
		t.Fatalf("failed to start replication from consistent point %s: %v", crsr.ConsistentPoint, err)
	}

	// Đọc message: stream phải nhận row 2 ('after_snap') và không gửi lại row 1
	receivedRow2 := false
	receivedRow1 := false

readLoop:
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for replication events after consistent_point")
		default:
		}

		rawMsg, err := replConn.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("failed to receive replication message: %v", err)
		}

		if cd, ok := rawMsg.(*pgproto3.CopyData); ok {
			switch cd.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
				if err == nil && pkm.ReplyRequested {
					_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pkm.ServerWALEnd})
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
				if err != nil {
					t.Fatalf("failed to parse XLogData: %v", err)
				}
				walData := xld.WALData
				_ = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: xld.WALStart})

				msg, err := pglogrepl.Parse(walData)
				if err != nil {
					continue
				}

				if ins, ok := msg.(*pglogrepl.InsertMessage); ok {
					// ins.Tuple.Columns: col 0 = id, col 1 = val
					if len(ins.Tuple.Columns) >= 2 {
						val := string(ins.Tuple.Columns[1].Data)
						if val == "before_snap" {
							receivedRow1 = true
						}
						if val == "after_snap" {
							receivedRow2 = true
							break readLoop
						}
					}
				}
			}
		}
	}

	if receivedRow1 {
		t.Fatalf("LỖI: stream gửi lại row 1 trước consistent_point")
	}
	if !receivedRow2 {
		t.Fatalf("LỖI: stream không nhận được row 2 sau consistent_point")
	}
	t.Logf("AC3 PASS: ConsistentPoint=%s khớp vị trí bắt đầu stream; chỉ nhận row ghi sau snapshot", crsr.ConsistentPoint)
}

// AC4: Logical decoding trên standby PG16+.
func TestAC4_StandbyLogicalDecoding_PG16(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Tạo Docker network kết nối Primary và Standby
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("failed to create docker network: %v", err)
	}
	defer nw.Remove(ctx)

	// Khởi tạo Primary
	priReq := testcontainers.ContainerRequest{
		Image:    "postgres:16",
		Networks: []string{nw.Name},
		NetworkAliases: map[string][]string{
			nw.Name: {"primary"},
		},
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "strand",
			"POSTGRES_PASSWORD": "dev",
			"POSTGRES_DB":       "strand",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	primaryContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: priReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start primary: %v", err)
	}
	defer primaryContainer.Terminate(ctx)

	priHost, _ := primaryContainer.Host(ctx)
	priPortRaw, _ := primaryContainer.MappedPort(ctx, "5432/tcp")
	priPort := priPortRaw.Int()

	priDSN := fmt.Sprintf("postgres://strand:dev@%s:%d/strand?sslmode=disable", priHost, priPort)
	var priDB *sql.DB
	for i := 0; i < 30; i++ {
		priDB, err = sql.Open("pgx", priDSN)
		if err == nil {
			if pingErr := priDB.Ping(); pingErr == nil {
				break
			}
			_ = priDB.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to ping primary: %v", err)
	}
	defer priDB.Close()

	// Cấu hình pg_hba.conf và tạo bảng, publication trên Primary
	_, _, _ = primaryContainer.Exec(ctx, []string{"bash", "-c", "echo 'host replication all all trust' >> /var/lib/postgresql/data/pg_hba.conf"})
	_, _ = priDB.ExecContext(ctx, "SELECT pg_reload_conf();")
	_, err = priDB.ExecContext(ctx, "CREATE TABLE t_stb (id int PRIMARY KEY, val text); CREATE PUBLICATION pub_stb FOR TABLE t_stb;")
	if err != nil {
		t.Fatalf("failed to setup table & pub on primary: %v", err)
	}

	// Khởi tạo Standby container
	stbCmd := "mkdir -p /tmp/standby_data && chown -R postgres:postgres /tmp/standby_data && chmod 700 /tmp/standby_data && " +
		"gosu postgres bash -c 'PGPASSWORD=dev pg_basebackup -h primary -p 5432 -U strand -D /tmp/standby_data -Fp -Xs -R' && " +
		"exec gosu postgres postgres -D /tmp/standby_data -c wal_level=logical -c hot_standby_feedback=on"

	stbReq := testcontainers.ContainerRequest{
		Image:    "postgres:16",
		Networks: []string{nw.Name},
		NetworkAliases: map[string][]string{
			nw.Name: {"standby"},
		},
		Entrypoint:   []string{"bash", "-c", stbCmd},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp").WithStartupTimeout(60 * time.Second),
	}

	standbyContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: stbReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start standby: %v", err)
	}
	defer standbyContainer.Terminate(ctx)

	stbHost, _ := standbyContainer.Host(ctx)
	stbPortRaw, _ := standbyContainer.MappedPort(ctx, "5432/tcp")
	stbPort := stbPortRaw.Int()

	stbDSN := fmt.Sprintf("postgres://strand:dev@%s:%d/strand?sslmode=disable", stbHost, stbPort)
	var stbDB *sql.DB
	for i := 0; i < 30; i++ {
		stbDB, err = sql.Open("pgx", stbDSN)
		if err == nil {
			if pingErr := stbDB.Ping(); pingErr == nil {
				break
			}
			_ = stbDB.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to ping standby: %v", err)
	}
	defer stbDB.Close()

	// Đảm bảo standby đang ở recovery mode
	var inRecovery bool
	err = stbDB.QueryRowContext(ctx, "SELECT pg_is_in_recovery();").Scan(&inRecovery)
	if err != nil || !inRecovery {
		t.Fatalf("standby không ở trạng thái recovery: err=%v, inRecovery=%v", err, inRecovery)
	}

	// Kết nối replication tới STANDBY
	stbReplConn, err := connectReplication(ctx, stbHost, stbPort)
	if err != nil {
		t.Fatalf("failed to connect replication to standby: %v", err)
	}
	defer stbReplConn.Close(ctx)

	// Goroutine phát sinh activity trên primary để unblock logical slot creation trên standby
	doneCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-doneCh:
				return
			case <-ticker.C:
				_, _ = priDB.Exec("CHECKPOINT; INSERT INTO t_stb VALUES (1, 'pulse') ON CONFLICT (id) DO UPDATE SET val = 'pulse';")
			}
		}
	}()

	// Tạo logical replication slot trên STANDBY
	slotName := "standby_slot"
	_, err = pglogrepl.CreateReplicationSlot(ctx, stbReplConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	close(doneCh)

	if err != nil {
		t.Fatalf("failed to create logical replication slot on standby: %v", err)
	}
	t.Logf("Tạo logical replication slot %s thành công trên Standby PG16!", slotName)

	// Bắt đầu stream replication từ STANDBY
	err = pglogrepl.StartReplication(ctx, stbReplConn, slotName, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names 'pub_stb'"},
		Mode:       pglogrepl.LogicalReplication,
	})
	if err != nil {
		t.Fatalf("failed to start replication from standby: %v", err)
	}

	// Ghi dữ liệu kiểm tra trên Primary
	_, err = priDB.ExecContext(ctx, "INSERT INTO t_stb (id, val) VALUES (999, 'from_primary_to_standby') ON CONFLICT (id) DO UPDATE SET val = 'from_primary_to_standby';")
	if err != nil {
		t.Fatalf("failed to insert test data on primary: %v", err)
	}

	// Đọc replication event từ STANDBY
	received := false
	for !received {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for replicated event from standby")
		default:
		}

		rawMsg, err := stbReplConn.ReceiveMessage(ctx)
		if err != nil {
			t.Fatalf("failed to receive replication message from standby: %v", err)
		}

		if cd, ok := rawMsg.(*pgproto3.CopyData); ok {
			switch cd.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
				if err == nil && pkm.ReplyRequested {
					_ = pglogrepl.SendStandbyStatusUpdate(ctx, stbReplConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pkm.ServerWALEnd})
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
				if err != nil {
					continue
				}
				_ = pglogrepl.SendStandbyStatusUpdate(ctx, stbReplConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: xld.WALStart})

				msg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					continue
				}

				if ins, ok := msg.(*pglogrepl.InsertMessage); ok {
					if len(ins.Tuple.Columns) >= 2 && string(ins.Tuple.Columns[1].Data) == "from_primary_to_standby" {
						received = true
						t.Log("Nhận được InsertMessage decode trực tiếp từ Standby PG16!")
					}
				}
			}
		}
	}

	t.Log("AC4 PASS: Logical decoding trên standby PG16+ hoạt động hoàn toàn chính xác")
}
