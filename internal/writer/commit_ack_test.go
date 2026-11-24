package writer_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/go-sql-driver/mysql"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/writer"
)

// The driver completes COMMIT on the server, then simulates losing the reply
// before database/sql or the writer can learn the result.
type lostCommitAckConnector struct{ driver.Connector }

func (c lostCommitAckConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return lostCommitAckConn{conn}, nil
}

type lostCommitAckConn struct{ driver.Conn }

func (c lostCommitAckConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return lostCommitAckTx{tx}, nil
}

type lostCommitAckTx struct{ driver.Tx }

func (tx lostCommitAckTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

func TestMySQLResumeAfterLostCommitAcknowledgment(t *testing.T) {
	ctx := context.Background()
	db, dsn := getMySQL(t)
	if db == nil {
		t.Fatal("MySQL unavailable")
	}
	const pipelineID = "lost-commit-ack"
	epoch := ensureMySQLMetadataTables(t, db, pipelineID)
	const tableName = "commit_ack_keyless"
	mustExec(t, db, "CREATE TABLE "+tableName+" (v BIGINT NOT NULL)")
	t.Cleanup(func() { mustExec(t, db, "DROP TABLE "+tableName) })
	table := model.TableID{Schema: "strand", Table: tableName}
	if err := writer.NewMySQLWriter(db, pipelineID, 1, epoch).EnsureWorkerRow(ctx); err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	faultyDB := sql.OpenDB(lostCommitAckConnector{connector})
	defer faultyDB.Close()
	w := writer.NewMySQLWriter(faultyDB, pipelineID, 1, epoch)
	w.SetKeyless(table, true)
	w.SetPrimaryKey(table, []string{"v"})
	const gtid = "2ad04d80-08d9-4a0a-8e91-044193c9859b:1"
	batch := []*model.Txn{{Rows: []model.RowChange{
		{Table: table, Op: model.OpInsert, Key: []model.Value{int64(1)}, Cols: []model.ColValue{{Name: "v", Value: int64(1)}}},
	}}}
	if err := w.ApplyBatch(ctx, batch, writer.DeltaRow{Applied: gtid}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ApplyBatch error = %v, want lost COMMIT reply", err)
	}

	store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
	if _, err := store.AdvanceEpoch(ctx, pipelineID, "restart", "writer-test"); err != nil {
		t.Fatal(err)
	}
	applied, err := store.LoadApplied(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want, err := checkpoint.ParseMySQLGTIDSet(gtid)
	if err != nil || !applied.Equal(want) {
		t.Fatalf("applied after reconnect = %v, want %s (parse error %v)", applied, gtid, err)
	}
	var rows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("committed keyless rows = %d, want one (query error %v)", rows, err)
	}
}
