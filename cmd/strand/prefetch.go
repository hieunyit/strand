package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"strand/internal/assemble"
	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/reader"
	archivereader "strand/internal/reader/archive"
	mysqlreader "strand/internal/reader/mysql"
	"strand/internal/seed"
	"strand/internal/spool"
)

func readSourceGTID(ctx context.Context, sourceDSN string) (*checkpoint.MySQLAppliedSet, error) {
	db, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		return nil, err
	}
	var executed string
	err = db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed)
	_ = db.Close()
	if err != nil {
		return nil, err
	}
	return checkpoint.ParseMySQLGTIDSet(strings.ReplaceAll(executed, "\n", ""))
}

type seedManifestLite struct {
	P              string
	Tables         []string
	SchemaSnapshot string
	ArchiveSchema  string
}

// prefetch (02 §9, P1-14): đọc log từ P vào spool, archive trước rồi source, song song với
// restore. Nhờ vậy archive hoặc binlog trên source bị xóa sau đó vẫn không gây log_gap.
// Nếu until == nil: dừng khi đã phủ tới gtid_executed của source tại lúc bắt đầu đọc source.
// Nếu until != nil: đọc source liên tục cho tới khi until đóng, rồi đọc tiếp tới gtid_executed
// của source tại thời điểm until đóng.
func prefetch(ctx context.Context, m *seedManifestLite, spoolDir string, localHard, hardLimit int64, overflow, archiveDir, sourceDSN string, until <-chan struct{}) (int, error) {
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		return 0, err
	}
	var opts []spool.Option
	// Tầng local đầy thì tràn sang bucket staging; mọi tầng chạm hard limit → prefetch_exhausted.
	bucket, prefix, err := spool.BucketFromURI(overflow)
	if err != nil {
		return 0, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, err.Error(), err)
	}
	if bucket != nil {
		opts = append(opts, spool.WithBucket(bucket, prefix))
	}
	if localHard > 0 {
		opts = append(opts, spool.WithLocalHardLimit(localHard))
	} else if hardLimit > 0 && bucket == nil {
		opts = append(opts, spool.WithLocalHardLimit(hardLimit))
	}
	if hardLimit > 0 {
		opts = append(opts, spool.WithTotalHardLimit(hardLimit))
	}
	sp, err := spool.New(spoolDir, opts...)
	defer func() {
		if sp != nil {
			u := sp.Usage()
			fmt.Printf("spool usage: local %d bytes, bucket %d bytes\n", u.LocalBytes, u.BucketBytes)
		}
	}()
	if err != nil {
		return 0, err
	}
	defer sp.Close()

	startGTID := ""
	if m != nil {
		startGTID = m.P
	}
	covered, err := checkpoint.ParseMySQLGTIDSet(startGTID)
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

	var sidecar *archivereader.SchemaSidecar
	if m != nil {
		sidecarPath := m.ArchiveSchema
		if sidecarPath == "" {
			sidecarPath = m.SchemaSnapshot
		}
		if sidecarPath != "" {
			if sc, err := archivereader.LoadSchemaSidecar(sidecarPath); err == nil {
				sidecar = sc
			} else {
				return 0, fmt.Errorf("load archive schema sidecar %s: %w", sidecarPath, err)
			}
		}
	}

	var tableScope func(schema, table string) bool
	if m != nil && len(m.Tables) > 0 {
		hasStar := false
		for _, t := range m.Tables {
			if strings.TrimSpace(t) == "*" {
				hasStar = true
				break
			}
		}
		if !hasStar {
			var patterns []seed.TablePattern
			for _, t := range m.Tables {
				if p, err := seed.ParseTablePattern(t); err == nil {
					patterns = append(patterns, p)
				}
			}
			if len(patterns) > 0 {
				tableScope = func(schema, table string) bool {
					tid := model.TableID{Schema: schema, Table: table}
					for _, p := range patterns {
						if p.Match(tid) {
							return true
						}
					}
					return false
				}
			}
		}
	}

	var keylessFunc func(model.TableID) bool
	if sidecar != nil {
		keylessFunc = func(tid model.TableID) bool {
			if tbl, ok := sidecar.GetTable(tid.Schema, tid.Table); ok {
				return tbl.IdentityMode == archivereader.IdentityModeKeyless
			}
			return false
		}
	}

	consume := func(events <-chan reader.Event, stopAt *checkpoint.MySQLAppliedSet, untilCh <-chan struct{}) error {
		var asmOpts []assemble.Option
		asmOpts = append(asmOpts, assemble.WithSpillDir(spillDir))
		if keylessFunc != nil {
			asmOpts = append(asmOpts, assemble.WithKeylessFunc(keylessFunc))
		}
		if sidecar != nil {
			kmMap := make(map[model.TableID]assemble.KeyMeta)
			for _, tbl := range sidecar.Tables {
				tid := model.TableID{Schema: tbl.Schema, Table: tbl.Table}
				var km assemble.KeyMeta
				if tbl.IdentityMode == archivereader.IdentityModeKeyless {
					km.Unique = append(km.Unique, assemble.UniqueIndex{
						Name:    assemble.KeylessIndexName,
						Columns: tbl.Columns,
						Prefix:  true,
					})
				}
				if len(km.Unique) > 0 {
					kmMap[tid] = km
				}
			}
			if len(kmMap) > 0 {
				asmOpts = append(asmOpts, assemble.WithKeyMetadata(kmMap))
			}
		}
		asm := assemble.New(asmOpts...)
		if sidecar != nil {
			for _, tbl := range sidecar.Tables {
				if tbl.IdentityMode == archivereader.IdentityModeUnique && len(tbl.KeyColumns) > 0 {
					asm.Registry().RegisterTable(tbl.Schema, tbl.Table, tbl.KeyColumns, tbl.Columns)
				}
			}
		}
		inTxn := false
		// Khi untilCh đóng (restore-cmd kết thúc), chốt stopAt bằng gtid_executed hiện tại của source.
		checkUntil := func() error {
			untilCh = nil
			var err error
			stopAt, err = readSourceGTID(ctx, sourceDSN)
			return err
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-untilCh:
				if err := checkUntil(); err != nil {
					return err
				}
				if !inTxn && stopAt.IsSubsetOf(covered) {
					return nil
				}
			case ev, ok := <-events:
				if !ok {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if inTxn {
						return exitcode.New(
							exitcode.ErrLogGap,
							exitcode.DataError,
							"event stream closed unexpectedly while inside transaction (partial transaction not covered)",
							nil,
						)
					}
					if stopAt != nil && !stopAt.IsSubsetOf(covered) {
						return fmt.Errorf("source event stream closed before reaching %s", stopAt)
					}
					return nil
				}
				if ev.Type == reader.EventError {
					return ev.Err
				}
				switch ev.Type {
				case reader.EventGTID, reader.EventQuery, reader.EventTableMap, reader.EventRows:
					inTxn = true
				}
				t, err := asm.Push(ev)
				if err != nil {
					return err
				}
				if t == nil {
					continue
				}
				inTxn = false
				if sidecar != nil {
					for _, rc := range t.Rows {
						if tbl, ok := sidecar.GetTable(rc.Table.Schema, rc.Table.Table); ok {
							if err := archivereader.VerifyRowChangeIdentity(*tbl, rc); err != nil {
								return exitcode.New(
									exitcode.ErrBinlogConfigInvalid,
									exitcode.Rejected,
									fmt.Sprintf("spool prefetch downstream safety violation: %v", err),
									err,
								)
							}
						}
					}
				}
				if !covered.Contains(t.SourcePos) {
					if err := sp.Append(ctx, t); err != nil {
						return err // prefetch_exhausted khi mọi tầng spool chạm hard limit
					}
					covered.Add(t.SourcePos)
					n++
				}
				if untilCh != nil {
					select {
					case <-untilCh:
						if err := checkUntil(); err != nil {
							return err
						}
					default:
					}
				}
				if stopAt != nil && stopAt.IsSubsetOf(covered) {
					return nil
				}
			}
		}
	}

	if archiveDir != "" {
		var charsets map[uint64]string
		if sourceDSN != "" {
			if charsets, err = loadSourceCharsets(ctx, sourceDSN); err != nil {
				return n, err
			}
		} else {
			fmt.Println("archive prefetch: no --source; values of non-UTF-8 string columns are not converted")
		}
		ar, err := archivereader.New(archivereader.Config{
			Dir:               archiveDir,
			CollationCharsets: charsets,
			Sidecar:           sidecar,
			TableScope:        tableScope,
		})
		if err != nil {
			return n, err
		}
		actx, cancel := context.WithCancel(ctx)
		events, err := ar.Start(actx, nil)
		if err == nil {
			err = consume(events, nil, nil)
		}
		cancel()
		_ = ar.Close()
		if err != nil {
			if ctx.Err() != nil {
				return n, ctx.Err()
			}
			var se *exitcode.Error
			if errors.As(err, &se) && se.Code == exitcode.ErrPrefetchExhausted {
				return n, err
			}
			if sourceDSN == "" {
				return n, fmt.Errorf("archive prefetch failed with no source fallback: %w", err)
			}
			if exitcode.GetExitCode(err) == exitcode.Rejected || exitcode.GetExitCode(err) == exitcode.NeedsDecision {
				return n, err
			}
			fmt.Printf("archive prefetch stopped: %v (continuing from source)\n", err)
		}
	}
	if sourceDSN != "" {
		var stopAt *checkpoint.MySQLAppliedSet
		if until != nil {
			select {
			case <-until:
				until = nil
			default:
			}
		}
		if until == nil {
			stopAt, err = readSourceGTID(ctx, sourceDSN)
			if err != nil {
				return n, err
			}
		}
		if until != nil || !stopAt.IsSubsetOf(covered) {
			charsets, err := loadSourceCharsets(ctx, sourceDSN)
			if err != nil {
				return n, err
			}
			// Giá trị chuỗi của cột không phải UTF-8 chuyển sang UTF-8 như reader của pipeline (charset.go).
			// MySQL 5.7 không có metadata cột trong TABLE_MAP: lấy tên cột/unsigned/ENUM từ information_schema.
			infoDB, err := sql.Open("mysql", sourceDSN)
			if err != nil {
				return n, err
			}
			defer infoDB.Close()
			srcVer, err := mysqlver.Detect(ctx, infoDB)
			if err != nil {
				return n, err
			}
			rd, err := mysqlreader.New(mysqlreader.Config{DSN: sourceDSN, ServerID: 49000 + uint32(os.Getpid()%1000), Flavor: srcVer.SyncerFlavor(), CollationCharsets: charsets,
				TableInfo: mysqlreader.NewInfoSchemaProvider(infoDB)})
			if err != nil {
				return n, err
			}
			sctx, cancel := context.WithCancel(ctx)
			events, err := rd.Start(sctx, covered.Clone())
			if err != nil {
				cancel()
				if ctx.Err() != nil {
					return n, ctx.Err()
				}
				return n, exitcode.New(exitcode.ErrLogGap, exitcode.DataError, fmt.Sprintf("prefetch from source: %v", err), err)
			}
			err = consume(events, stopAt, until)
			cancel()
			_ = rd.Close()
			if err != nil {
				if ctx.Err() != nil {
					return n, ctx.Err()
				}
				if exitcode.GetExitCode(err) == exitcode.DataError {
					return n, err
				}
				return n, exitcode.New(exitcode.ErrLogGap, exitcode.DataError, fmt.Sprintf("prefetch from source: %v", err), err)
			}
		}
	}
	if err := sp.Flush(ctx); err != nil {
		return n, err
	}
	return n, nil
}

// loadSourceCharsets đọc bảng collation id -> charset của source.
func loadSourceCharsets(ctx context.Context, dsn string) (map[uint64]string, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return mysqlreader.LoadCollationCharsets(ctx, db)
}
