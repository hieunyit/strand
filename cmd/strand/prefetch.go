package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"strand/internal/assemble"
	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
	archivereader "strand/internal/reader/archive"
	mysqlreader "strand/internal/reader/mysql"
	"strand/internal/spool"
)

// prefetch (02 §9, P1-14): đọc log từ P vào spool, archive trước rồi source, song song với
// restore. Nhờ vậy archive hoặc binlog trên source bị xóa sau đó vẫn không gây log_gap.
// Dừng khi đã phủ tới gtid_executed của source tại lúc bắt đầu đọc source.
func prefetch(ctx context.Context, m *seedManifestLite, spoolDir string, hardLimit int64, archiveDir, sourceDSN string) (int, error) {
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		return 0, err
	}
	var opts []spool.Option
	if hardLimit > 0 {
		opts = append(opts, spool.WithLocalHardLimit(hardLimit), spool.WithTotalHardLimit(hardLimit))
	}
	sp, err := spool.New(spoolDir, opts...)
	if err != nil {
		return 0, err
	}
	defer sp.Close()

	covered, err := checkpoint.ParseMySQLGTIDSet(m.P)
	if err != nil {
		return 0, err
	}
	// Các Txn đã có trong spool (apply chạy lại sau khi bị ngắt).
	if it, err := sp.Iter(ctx, model.Pos{}); err == nil {
		for {
			t, err := it.Next(ctx)
			if err != nil {
				break
			}
			covered.Add(t.SourcePos)
		}
		it.Close()
	}
	spillDir := filepath.Join(spoolDir, "spill")
	_ = os.MkdirAll(spillDir, 0o750)
	n := 0
	consume := func(events <-chan reader.Event, stopAt *checkpoint.MySQLAppliedSet) error {
		asm := assemble.New(assemble.WithSpillDir(spillDir))
		for ev := range events {
			if ev.Type == reader.EventError {
				return ev.Err
			}
			t, err := asm.Push(ev)
			if err != nil {
				return err
			}
			if t == nil {
				continue
			}
			if !covered.Contains(t.SourcePos) {
				if err := sp.Append(ctx, t); err != nil {
					return err // prefetch_exhausted khi mọi tầng spool chạm hard limit
				}
				covered.Add(t.SourcePos)
				n++
			}
			if stopAt != nil && stopAt.IsSubsetOf(covered) {
				return nil
			}
		}
		return nil
	}

	if archiveDir != "" {
		ar, err := archivereader.New(archivereader.Config{Dir: archiveDir})
		if err != nil {
			return n, err
		}
		actx, cancel := context.WithCancel(ctx)
		events, err := ar.Start(actx, nil)
		if err == nil {
			err = consume(events, nil)
		}
		cancel()
		_ = ar.Close()
		if err != nil {
			fmt.Printf("archive prefetch stopped: %v (continuing from source)\n", err)
		}
	}
	if sourceDSN != "" {
		db, err := sql.Open("mysql", sourceDSN)
		if err != nil {
			return n, err
		}
		var executed string
		err = db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed)
		db.Close()
		if err != nil {
			return n, err
		}
		stopAt, err := checkpoint.ParseMySQLGTIDSet(strings.ReplaceAll(executed, "\n", ""))
		if err != nil {
			return n, err
		}
		if !stopAt.IsSubsetOf(covered) {
			rd, err := mysqlreader.New(mysqlreader.Config{DSN: sourceDSN, ServerID: 49000 + uint32(os.Getpid()%1000)})
			if err != nil {
				return n, err
			}
			sctx, cancel := context.WithCancel(ctx)
			events, err := rd.Start(sctx, covered.Clone())
			if err != nil {
				cancel()
				return n, exitcode.New(exitcode.ErrLogGap, exitcode.DataError, fmt.Sprintf("prefetch from source: %v", err), err)
			}
			err = consume(events, stopAt)
			cancel()
			_ = rd.Close()
			if err != nil {
				if exitcode.GetExitCode(err) == exitcode.DataError {
					return n, err
				}
				return n, exitcode.New(exitcode.ErrLogGap, exitcode.DataError, fmt.Sprintf("prefetch from source: %v", err), err)
			}
		}
	}
	return n, nil
}

type seedManifestLite struct{ P string }
