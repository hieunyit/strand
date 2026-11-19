// Package mcp implements the read-only MCP stdio adapter for backup management.
// It deliberately depends on a narrow service interface and has no S3 or web dependency.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
)

const (
	ProtocolVersion       = "2026-07-28"
	LegacyProtocolVersion = "2025-11-25"
	maxInputBytes         = 1 << 20
)

var supportedLegacyVersions = map[string]bool{
	"2025-11-25": true,
	"2025-06-18": true,
	"2025-03-26": true,
	"2024-11-05": true,
}

// Record is a JSON-friendly backup catalog record. Implementations must not put
// credentials, signed URLs, or other secrets in returned records.
type Record map[string]any

// BackupService is injected by the future backup catalog/service layer.
// Filters are restricted to the fields documented by list_backups.
type BackupService interface {
	ListBackups(ctx context.Context, filters map[string]any) ([]Record, error)
	GetBackup(ctx context.Context, backupID string) (Record, error)
	BackupStatus(ctx context.Context, backupID string) (Record, error)
}

// ErrNotFound is returned by a BackupService when the requested backup does not exist.
var ErrNotFound = errors.New("backup not found")

// InvalidArgumentError marks a service error caused by the caller's arguments; its message is shown to
// the client (it must not contain secrets). Other service errors are reported generically.
type InvalidArgumentError struct{ Msg string }

func (e *InvalidArgumentError) Error() string { return e.Msg }

// Auditor records every tool call. Serve is fail-closed: when Audit returns an error the call's result is
// withheld and the client receives an error instead.
type Auditor interface {
	Audit(ctx context.Context, tool string, args map[string]any, result string) error
}

// MaxConcurrentCalls bounds in-flight tools/call requests per connection.
const MaxConcurrentCalls = 16

// Option configures Serve.
type Option func(*connection)

// WithAuditor sets the audit sink for tool calls.
func WithAuditor(a Auditor) Option { return func(c *connection) { c.auditor = a } }

// Serve runs a newline-delimited JSON-RPC MCP stdio server. The caller owns in
// and out. Tool calls run concurrently so notifications/cancelled can be read
// and honored while a service call is in progress.
func Serve(ctx context.Context, in io.Reader, out io.Writer, service BackupService, opts ...Option) error {
	if in == nil || out == nil {
		return errors.New("mcp: stdin and stdout are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn := &connection{ctx: ctx, out: out, service: service, active: make(map[string]context.CancelFunc)}
	for _, o := range opts {
		o(conn)
	}
	lines := make(chan []byte)
	readErr := make(chan error, 1)
	go scanInput(ctx, in, lines, readErr)

	for {
		select {
		case <-ctx.Done():
			conn.cancelAll()
			conn.wg.Wait()
			return ctx.Err()
		case line, ok := <-lines:
			if !ok {
				// stdin closed: the client is done sending, but calls already accepted still get their
				// responses (a piped/scripted client closes stdin right after its last request).
				conn.wg.Wait()
				if err := <-readErr; err != nil {
					return fmt.Errorf("mcp: read stdin: %w", err)
				}
				return nil
			}
			conn.handle(line)
		}
	}
}

func scanInput(ctx context.Context, in io.Reader, lines chan<- []byte, result chan<- error) {
	defer close(lines)
	s := bufio.NewScanner(in)
	s.Buffer(make([]byte, 64*1024), maxInputBytes)
	for s.Scan() {
		line := append([]byte(nil), s.Bytes()...)
		select {
		case lines <- line:
		case <-ctx.Done():
			result <- ctx.Err()
			return
		}
	}
	result <- s.Err()
}

type connection struct {
	ctx     context.Context
	out     io.Writer
	service BackupService
	auditor Auditor

	writeMu sync.Mutex
	stateMu sync.Mutex
	ready   bool
	active  map[string]context.CancelFunc
	wg      sync.WaitGroup
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func (c *connection) handle(line []byte) {
	var req rpcMessage
	if err := json.Unmarshal(line, &req); err != nil {
		c.respond(json.RawMessage("null"), nil, &rpcError{Code: -32700, Message: "Parse error"})
		return
	}
	if req.JSONRPC != "2.0" || req.Method == "" || !validID(req.ID) {
		if len(req.ID) != 0 {
			c.respond(req.ID, nil, &rpcError{Code: -32600, Message: "Invalid Request"})
		}
		return
	}

	isNotification := len(req.ID) == 0
	version, hasVersion, versionErr := requestProtocolVersion(req.Params)
	if versionErr != nil {
		if !isNotification {
			c.respond(req.ID, nil, &rpcError{Code: -32602, Message: "Invalid protocol metadata"})
		}
		return
	}
	modern := hasVersion && version == ProtocolVersion
	if hasVersion && version != ProtocolVersion && !supportedLegacyVersions[version] {
		if !isNotification {
			c.respond(req.ID, nil, &rpcError{Code: -32022, Message: "Unsupported protocol version", Data: map[string]any{"supported": supportedVersions()}})
		}
		return
	}
	switch req.Method {
	case "initialize":
		if isNotification {
			return
		}
		c.initialize(req)
	case "notifications/initialized":
		c.stateMu.Lock()
		c.ready = true
		c.stateMu.Unlock()
	case "notifications/cancelled":
		c.cancelRequest(req.Params)
	case "server/discover":
		if isNotification {
			return
		}
		if !hasVersion || !modern {
			c.respond(req.ID, nil, &rpcError{Code: -32602, Message: "server/discover requires the 2026-07-28 protocol envelope"})
			return
		}
		c.respondModern(req.ID, map[string]any{
			"supportedVersions": supportedVersions(),
			"capabilities":      map[string]any{"tools": map[string]any{}},
			"instructions":      "Read-only backup catalog tools. Backup records must not contain credentials or signed URLs.",
			"ttlMs":             0,
			"cacheScope":        "PRIVATE",
		}, nil)
	case "ping":
		if !isNotification {
			if modern {
				c.respond(req.ID, nil, &rpcError{Code: -32601, Message: "Method not found"})
			} else {
				c.respond(req.ID, map[string]any{}, nil)
			}
		}
	case "tools/list":
		if isNotification {
			return
		}
		if !modern && !c.isReady(req.ID) {
			return
		}
		result := map[string]any{"tools": toolDefinitions()}
		if modern {
			result["ttlMs"], result["cacheScope"] = 0, "PRIVATE"
			c.respondModern(req.ID, result, nil)
		} else {
			c.respond(req.ID, result, nil)
		}
	case "tools/call":
		if isNotification {
			return
		}
		if !modern && !c.isReady(req.ID) {
			return
		}
		c.startToolCall(req, modern)
	default:
		if !isNotification {
			c.respond(req.ID, nil, &rpcError{Code: -32601, Message: "Method not found"})
		}
	}
}

func supportedVersions() []string {
	return []string{ProtocolVersion, "2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"}
}

func requestProtocolVersion(raw json.RawMessage) (string, bool, error) {
	var params struct {
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	if len(raw) == 0 {
		return "", false, nil
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return "", false, err
	}
	rawVersion, ok := params.Meta["io.modelcontextprotocol/protocolVersion"]
	if !ok {
		return "", false, nil
	}
	var version string
	if err := json.Unmarshal(rawVersion, &version); err != nil || version == "" {
		return "", false, errors.New("invalid protocol version")
	}
	return version, true, nil
}

func validID(id json.RawMessage) bool {
	if len(id) == 0 {
		return true // notification
	}
	var v any
	if json.Unmarshal(id, &v) != nil {
		return false
	}
	switch v.(type) {
	case nil, string, float64:
		return true
	default:
		return false
	}
}

func (c *connection) initialize(req rpcMessage) {
	var params struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if len(req.Params) == 0 || json.Unmarshal(req.Params, &params) != nil || params.ProtocolVersion == "" {
		c.respond(req.ID, nil, &rpcError{Code: -32602, Message: "Invalid params"})
		return
	}
	c.stateMu.Lock()
	c.ready = false
	c.stateMu.Unlock()
	version := params.ProtocolVersion
	if !supportedLegacyVersions[version] {
		version = LegacyProtocolVersion // counter-offer; modern clients must use server/discover.
	}
	c.respond(req.ID, map[string]any{
		"protocolVersion": version,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "strand-backup", "version": "0.1.0"},
		"instructions":    "Read-only backup catalog tools. Backup records must not contain credentials or signed URLs.",
	}, nil)
}

func (c *connection) isReady(id json.RawMessage) bool {
	c.stateMu.Lock()
	ready := c.ready
	c.stateMu.Unlock()
	if !ready {
		c.respond(id, nil, &rpcError{Code: -32002, Message: "Server not initialized"})
	}
	return ready
}

func (c *connection) startToolCall(req rpcMessage, modern bool) {
	key := idKey(req.ID)
	callCtx, cancel := context.WithCancel(c.ctx)
	c.stateMu.Lock()
	if _, exists := c.active[key]; exists {
		c.stateMu.Unlock()
		cancel()
		c.respond(req.ID, nil, &rpcError{Code: -32600, Message: "Request id is already active"})
		return
	}
	if len(c.active) >= MaxConcurrentCalls {
		c.stateMu.Unlock()
		cancel()
		c.respond(req.ID, nil, &rpcError{Code: -32000, Message: "Too many concurrent requests"})
		return
	}
	c.active[key] = cancel
	c.wg.Add(1)
	c.stateMu.Unlock()

	go func() {
		defer c.wg.Done()
		defer cancel()
		result, err := c.callTool(callCtx, req.Params)
		c.stateMu.Lock()
		delete(c.active, key)
		c.stateMu.Unlock()
		if callCtx.Err() != nil {
			c.respondProtocol(req.ID, nil, &rpcError{Code: -32800, Message: "Request cancelled"}, modern)
			return
		}
		if err != nil {
			c.respondProtocol(req.ID, nil, &rpcError{Code: -32602, Message: err.Error()}, modern)
			return
		}
		c.respondProtocol(req.ID, result, nil, modern)
	}()
}

func (c *connection) callTool(ctx context.Context, raw json.RawMessage) (any, error) {
	var params struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &params) != nil || params.Name == "" {
		return nil, errors.New("tools/call requires name and arguments")
	}
	if params.Arguments == nil {
		params.Arguments = map[string]any{}
	}
	value, err := c.dispatch(ctx, params.Name, params.Arguments)
	if c.auditor == nil {
		return value, err
	}
	result := "ok"
	if err != nil {
		result = "error"
	} else if m, ok := value.(map[string]any); ok && m["isError"] == true {
		result = "denied"
	}
	tool := params.Name
	if len(tool) > 64 {
		tool = tool[:64]
	}
	if aerr := c.auditor.Audit(context.WithoutCancel(ctx), tool, params.Arguments, result); aerr != nil {
		return toolError("audit log unavailable; request refused"), nil
	}
	return value, err
}

func (c *connection) dispatch(ctx context.Context, name string, args map[string]any) (any, error) {
	params := struct {
		Name      string
		Arguments map[string]any
	}{name, args}
	var value any
	var err error
	switch params.Name {
	case "list_backups":
		filters, ferr := validateFilters(params.Arguments)
		if ferr != nil {
			return toolError(ferr.Error()), nil
		}
		if c.service == nil {
			return toolError("backup catalog service is unavailable"), nil
		}
		value, err = c.service.ListBackups(ctx, filters)
	case "get_backup":
		id, ferr := requiredBackupID(params.Arguments)
		if ferr != nil {
			return toolError(ferr.Error()), nil
		}
		if c.service == nil {
			return toolError("backup catalog service is unavailable"), nil
		}
		value, err = c.service.GetBackup(ctx, id)
	case "backup_status":
		id, ferr := requiredBackupID(params.Arguments)
		if ferr != nil {
			return toolError(ferr.Error()), nil
		}
		if c.service == nil {
			return toolError("backup catalog service is unavailable"), nil
		}
		value, err = c.service.BackupStatus(ctx, id)
	default:
		return toolError(fmt.Sprintf("unknown tool %q", params.Name)), nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var inv *InvalidArgumentError
		switch {
		case errors.Is(err, ErrNotFound):
			return toolError("backup not found"), nil
		case errors.As(err, &inv):
			return toolError(inv.Msg), nil
		}
		return toolError("backup service request failed"), nil
	}
	return toolResult(value), nil
}

func validateFilters(args map[string]any) (map[string]any, error) {
	allowed := map[string]bool{"source": true, "engine": true, "status": true, "limit": true}
	filters := make(map[string]any, len(args))
	for key, value := range args {
		if !allowed[key] {
			return nil, fmt.Errorf("unsupported list_backups argument %q", key)
		}
		switch key {
		case "limit":
			n, ok := value.(float64)
			if !ok || n < 1 || n > 500 || n != math.Trunc(n) {
				return nil, errors.New("limit must be an integer from 1 to 500")
			}
			filters[key] = int(n)
		default:
			s, ok := value.(string)
			if !ok || len(s) > 256 {
				return nil, fmt.Errorf("%s must be a string no longer than 256 bytes", key)
			}
			if key == "engine" && s != "mysql" && s != "postgres" {
				return nil, errors.New("engine must be mysql or postgres")
			}
			filters[key] = s
		}
	}
	return filters, nil
}

func requiredBackupID(args map[string]any) (string, error) {
	if len(args) != 1 {
		return "", errors.New("backup_id is required and must be the only argument")
	}
	id, ok := args["backup_id"].(string)
	if !ok || strings.TrimSpace(id) == "" || len(id) > 512 {
		return "", errors.New("backup_id must be a non-empty string no longer than 512 bytes")
	}
	return id, nil
}

type toolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func toolResult(value any) map[string]any {
	b, err := json.Marshal(value)
	if err != nil {
		return toolError("backup service returned a non-JSON value")
	}
	return map[string]any{"content": []toolContent{{Type: "text", Text: string(b)}}}
}

func toolError(message string) map[string]any {
	return map[string]any{"isError": true, "content": []toolContent{{Type: "text", Text: message}}}
}

func (c *connection) cancelRequest(raw json.RawMessage) {
	var params struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if json.Unmarshal(raw, &params) != nil || len(params.RequestID) == 0 {
		return
	}
	c.stateMu.Lock()
	cancel := c.active[idKey(params.RequestID)]
	c.stateMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func idKey(raw json.RawMessage) string {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return "invalid:" + string(raw)
	}
	switch v := value.(type) {
	case string:
		return "s:" + v
	case float64:
		return "n:" + strconv.FormatFloat(v, 'g', -1, 64)
	case nil:
		return "null"
	default:
		return "invalid:" + string(raw)
	}
}

func (c *connection) cancelAll() {
	c.stateMu.Lock()
	for _, cancel := range c.active {
		cancel()
	}
	c.stateMu.Unlock()
}

func (c *connection) respond(id json.RawMessage, result any, rpcErr *rpcError) {
	if len(id) == 0 {
		return
	}
	response := rpcResponse{JSONRPC: "2.0", ID: id, Result: result, Error: rpcErr}
	line, err := json.Marshal(response)
	if err != nil {
		line, _ = json.Marshal(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: -32603, Message: "Internal error"}})
	}
	c.writeMu.Lock()
	_, _ = c.out.Write(append(line, '\n'))
	c.writeMu.Unlock()
}

func (c *connection) respondProtocol(id json.RawMessage, result any, rpcErr *rpcError, modern bool) {
	if modern {
		c.respondModern(id, result, rpcErr)
		return
	}
	c.respond(id, result, rpcErr)
}

func (c *connection) respondModern(id json.RawMessage, result any, rpcErr *rpcError) {
	if rpcErr == nil {
		if body, ok := result.(map[string]any); ok {
			body["resultType"] = "complete"
			body["_meta"] = map[string]any{"io.modelcontextprotocol/serverInfo": map[string]any{"name": "strand-backup", "version": "0.1.0"}}
		}
	}
	c.respond(id, result, rpcErr)
}

type toolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func toolDefinitions() []toolDefinition {
	objectSchema := func(properties map[string]any, required ...string) map[string]any {
		return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
	}
	stringSchema := map[string]any{"type": "string"}
	return []toolDefinition{
		{
			Name:        "list_backups",
			Description: "List cataloged backups using optional filters. Read-only; does not access object storage directly.",
			InputSchema: objectSchema(map[string]any{
				"source": stringSchema,
				"engine": map[string]any{"type": "string", "enum": []string{"mysql", "postgres"}},
				"status": stringSchema,
				"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": 500},
			}),
		},
		{
			Name:        "get_backup",
			Description: "Get catalog metadata and recovery position for one backup by backup_id. Does not expose storage credentials or signed URLs. Read-only.",
			InputSchema: objectSchema(map[string]any{"backup_id": stringSchema}, "backup_id"),
		},
		{
			Name:        "backup_status",
			Description: "Get verification, retention, and availability status for one cataloged backup. Read-only.",
			InputSchema: objectSchema(map[string]any{"backup_id": stringSchema}, "backup_id"),
		},
	}
}
