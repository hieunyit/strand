// Package reader định nghĩa interface chung cho các engine đọc dữ liệu (MySQL binlog, PG logical, archive) theo spec/02-specs.md §3.
package reader

import (
	"context"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// EventType phân loại sự kiện nhận được từ stream CDC.
type EventType string

const (
	EventGTID     EventType = "gtid"
	EventQuery    EventType = "query"
	EventTableMap EventType = "table_map"
	EventRows     EventType = "rows"
	EventXID      EventType = "xid"
	EventRotate   EventType = "rotate"
	EventError    EventType = "error"
)

// Event biểu diễn một sự kiện thô nhận được từ binlog trước khi qua Txn Assembler.
type Event struct {
	Type      EventType `json:"type"`
	Pos       model.Pos `json:"pos"`
	Timestamp time.Time `json:"timestamp"`

	GTID    string `json:"gtid,omitempty"`
	Query   string `json:"query,omitempty"`
	Schema  string `json:"schema,omitempty"`
	Table   string `json:"table,omitempty"`
	TableID uint64 `json:"table_id,omitempty"`
	// Columns/PKColumns lấy từ TABLE_MAP khi binlog_row_metadata=FULL.
	Columns   []string `json:"columns,omitempty"`
	PKColumns []string `json:"pk_columns,omitempty"`
	Op        model.Op `json:"op,omitempty"`
	Rows      [][]any  `json:"rows,omitempty"`
	XID       uint64   `json:"xid,omitempty"`
	Err       error    `json:"error,omitempty"`
}

// Reader là hợp đồng giữa tầng Reader và Txn Assembler theo spec/02 §3.
// spec: 02 §3
type Reader interface {
	Start(ctx context.Context, from checkpoint.AppliedSet) (<-chan Event, error)
	Position() model.Pos
	Close() error
}
