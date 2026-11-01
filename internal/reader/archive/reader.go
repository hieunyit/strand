// Package archive hiện thực reader đọc binlog từ file lưu trữ theo spec/02 §3 và §9.
package archive

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/reader"
)

// Config cấu hình cho ArchiveReader.
type Config struct {
	Dir        string
	StartPos   model.Pos
	BufferSize int
}

// ArchiveReader đọc sự kiện binlog từ các file archive trên đĩa.
// spec: 02 §3, §9
type ArchiveReader struct {
	cfg        Config
	mu         sync.RWMutex
	currentPos model.Pos
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closed     bool
}

// New khởi tạo ArchiveReader.
func New(cfg Config) (*ArchiveReader, error) {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	return &ArchiveReader{
		cfg:        cfg,
		currentPos: cfg.StartPos,
	}, nil
}

// Start bắt đầu đọc các file binlog archive và đẩy event vào channel trả về.
func (r *ArchiveReader) Start(ctx context.Context, from checkpoint.AppliedSet) (<-chan reader.Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, fmt.Errorf("archive reader is closed")
	}

	// Quét danh sách file archive trong thư mục
	files, err := r.scanFiles()
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, exitcode.New(
			exitcode.ErrLogGap,
			exitcode.DataError,
			fmt.Sprintf("no archive binlog files found in %s", r.cfg.Dir),
			nil,
		)
	}

	ctxRead, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	out := make(chan reader.Event, r.cfg.BufferSize)
	r.wg.Add(1)

	go func() {
		defer r.wg.Done()
		defer close(out)

		tableMap := make(map[uint64]*replication.TableMapEvent)
		parser := replication.NewBinlogParser()
		parser.SetFlavor("mysql")

		for _, filePath := range files {
			if ctxRead.Err() != nil {
				return
			}

			// Kiểm tra nếu file bị xóa giữa chừng
			if _, statErr := os.Stat(filePath); statErr != nil {
				select {
				case out <- reader.Event{
					Type: reader.EventError,
					Err: exitcode.New(
						exitcode.ErrLogGap,
						exitcode.DataError,
						fmt.Sprintf("archive file missing or deleted: %s", filePath),
						statErr,
					),
				}:
				case <-ctxRead.Done():
				}
				return
			}

			onEvent := func(ev *replication.BinlogEvent) error {
				if ctxRead.Err() != nil {
					return ctxRead.Err()
				}
				ts := time.Unix(int64(ev.Header.Timestamp), 0).UTC()

				switch ev.Header.EventType {
				case replication.GTID_EVENT:
					if gtidEv, ok := ev.Event.(*replication.GTIDEvent); ok {
						u, err := uuid.FromBytes(gtidEv.SID)
						if err != nil {
							return nil
						}
						gtid := fmt.Sprintf("%s:%d", u.String(), gtidEv.GNO)
						r.mu.Lock()
						r.currentPos = model.Pos{Engine: model.EngineMySQL, GTID: gtid}
						r.mu.Unlock()

						select {
						case out <- reader.Event{
							Type:      reader.EventGTID,
							Pos:       r.Position(),
							GTID:      gtid,
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.GTID_TAGGED_LOG_EVENT:
					if taggedEv, ok := ev.Event.(*replication.GtidTaggedLogEvent); ok {
						u, err := uuid.FromBytes(taggedEv.SID)
						if err != nil {
							return nil
						}
						gtid := fmt.Sprintf("%s:%s:%d", u.String(), taggedEv.Tag, taggedEv.GNO)
						r.mu.Lock()
						r.currentPos = model.Pos{Engine: model.EngineMySQL, GTID: gtid}
						r.mu.Unlock()

						select {
						case out <- reader.Event{
							Type:      reader.EventGTID,
							Pos:       r.Position(),
							GTID:      gtid,
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.QUERY_EVENT:
					if qEv, ok := ev.Event.(*replication.QueryEvent); ok {
						select {
						case out <- reader.Event{
							Type:      reader.EventQuery,
							Pos:       r.Position(),
							Query:     string(qEv.Query),
							Schema:    string(qEv.Schema),
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.TABLE_MAP_EVENT:
					if tmEv, ok := ev.Event.(*replication.TableMapEvent); ok {
						tableMap[tmEv.TableID] = tmEv
						select {
						case out <- reader.Event{
							Type:      reader.EventTableMap,
							Pos:       r.Position(),
							TableID:   tmEv.TableID,
							Schema:    string(tmEv.Schema),
							Table:     string(tmEv.Table),
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
						schema, table := "", ""
						if tm, found := tableMap[rowsEv.TableID]; found {
							schema, table = string(tm.Schema), string(tm.Table)
						}
						select {
						case out <- reader.Event{
							Type:      reader.EventRows,
							Pos:       r.Position(),
							TableID:   rowsEv.TableID,
							Schema:    schema,
							Table:     table,
							Op:        model.OpInsert,
							Rows:      rowsEv.Rows,
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
						schema, table := "", ""
						if tm, found := tableMap[rowsEv.TableID]; found {
							schema, table = string(tm.Schema), string(tm.Table)
						}
						select {
						case out <- reader.Event{
							Type:      reader.EventRows,
							Pos:       r.Position(),
							TableID:   rowsEv.TableID,
							Schema:    schema,
							Table:     table,
							Op:        model.OpUpdate,
							Rows:      rowsEv.Rows,
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					if rowsEv, ok := ev.Event.(*replication.RowsEvent); ok {
						schema, table := "", ""
						if tm, found := tableMap[rowsEv.TableID]; found {
							schema, table = string(tm.Schema), string(tm.Table)
						}
						select {
						case out <- reader.Event{
							Type:      reader.EventRows,
							Pos:       r.Position(),
							TableID:   rowsEv.TableID,
							Schema:    schema,
							Table:     table,
							Op:        model.OpDelete,
							Rows:      rowsEv.Rows,
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.XID_EVENT:
					if xidEv, ok := ev.Event.(*replication.XIDEvent); ok {
						select {
						case out <- reader.Event{
							Type:      reader.EventXID,
							Pos:       r.Position(),
							XID:       xidEv.XID,
							Timestamp: ts,
						}:
						case <-ctxRead.Done():
							return ctxRead.Err()
						}
					}

				case replication.ROTATE_EVENT:
					select {
					case out <- reader.Event{
						Type:      reader.EventRotate,
						Pos:       r.Position(),
						Timestamp: ts,
					}:
					case <-ctxRead.Done():
						return ctxRead.Err()
					}
				}

				return nil
			}

			if err := parser.ParseFile(filePath, 0, onEvent); err != nil {
				if ctxRead.Err() != nil {
					return
				}
				select {
				case out <- reader.Event{
					Type: reader.EventError,
					Err: exitcode.New(
						exitcode.ErrLogGap,
						exitcode.DataError,
						fmt.Sprintf("error parsing archive binlog file %s: %v", filePath, err),
						err,
					),
				}:
				case <-ctxRead.Done():
				}
				return
			}
		}
	}()

	return out, nil
}

// Position trả về vị trí hiện tại của reader.
func (r *ArchiveReader) Position() model.Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentPos
}

// Close dừng ArchiveReader.
func (r *ArchiveReader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()

	r.wg.Wait()
	return nil
}

func (r *ArchiveReader) scanFiles() ([]string, error) {
	entries, err := os.ReadDir(r.cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read archive dir %s: %w", r.cfg.Dir, err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		// Bỏ qua các file ẩn hoặc file tạm
		name := entry.Name()
		if len(name) > 0 && name[0] == '.' {
			continue
		}
		files = append(files, filepath.Join(r.cfg.Dir, name))
	}
	sort.Strings(files)
	return files, nil
}
