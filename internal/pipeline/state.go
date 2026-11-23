package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"strand/internal/admin"
	"strand/internal/exitcode"
	"strand/internal/model"
	mysqlreader "strand/internal/reader/mysql"
	"strand/internal/snapshot"
)

// localState lưu cục bộ (thư mục resources.spool.path) những bảng đã snapshot xong.
//
// Trạng thái này chỉ là tối ưu: mất file thì mọi bảng được snapshot lại, và snapshot lại
// luôn đúng vì flush thay thế nguyên range ở target trong một transaction (RS-1).
// Không lưu trên target vì spec 02 §4 chỉ cho phép 3 bảng metadata với số row cố định.
type localState struct {
	verificationRevision uint64
	mu                   sync.Mutex
	path                 string
	data                 stateFile
	// targetOf: bảng nguồn → bảng đích theo tables.rename hiện tại; SetDone(true) ghi lại ánh xạ này
	// (review N14). nil thì không ghi.
	targetOf func(table string) string
}

type stateFile struct {
	PipelineID string                           `json:"pipeline_id"`
	Done       map[string]bool                  `json:"snapshot_done"`
	Plans      map[string]snapshot.SnapshotPlan `json:"snapshot_plans,omitempty"`
	// VerifyPending persists outstanding post-seed work across restarts.
	// A completed attempt can leave incomplete evidence while CDC keeps running.
	VerifyPending  bool                        `json:"post_seed_verify_pending,omitempty"`
	VerifyEvidence *admin.VerificationEvidence `json:"post_seed_verification,omitempty"`
	VerifyConfig   string                      `json:"post_seed_verification_config,omitempty"`
	// PGSlot: slot của source PG lấy từ seed.json (--start-at seed) khi cấu hình không đặt source.slot;
	// các lần chạy sau dùng lại slot này.
	PGSlot string `json:"pg_slot,omitempty"`
	// Targets: bảng nguồn → bảng đích (sau rename) lúc bảng được bootstrap / đánh dấu Done (review N14).
	// Bảng Done mà tables.rename đổi đích thì đích mới không có lịch sử: khởi động bị từ chối.
	Targets map[string]string `json:"table_targets,omitempty"`
	// SourceSchemas: lịch sử cấu trúc bảng nguồn MySQL 5.x (TABLE_MAP không có tên cột) kèm khoảng hiệu lực
	// GTID, xem internal/reader/mysql/schemahist.go. State cũ không có field này: coi như rỗng.
	SourceSchemas map[string][]mysqlreader.SchemaVersion `json:"source_schemas,omitempty"`
	// AddedTables: bảng thêm bằng `snapshot add` khi pipeline đang chạy mà không nằm trong tables.include.
	// Khởi động sau hợp chúng vào bộ lọc (tables.exclude vẫn thắng), nếu không bảng sẽ âm thầm rời pipeline.
	AddedTables []string `json:"added_tables,omitempty"`
}

// VerificationAddedTables reads the runtime additions used on pipeline restart.
// A strict verification cannot safely ignore unreadable or malformed scope.
func VerificationAddedTables(dir, pipelineID string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "pipeline-state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var state struct {
		PipelineID  string   `json:"pipeline_id"`
		AddedTables []string `json:"added_tables"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	if state.PipelineID != pipelineID {
		return nil, fmt.Errorf("local state is for pipeline %q, expected %q", state.PipelineID, pipelineID)
	}
	return state.AddedTables, nil
}

// AddedTables trả các bảng đã thêm qua snapshot add (đã sắp xếp).
func (s *localState) AddedTables() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.data.AddedTables...)
}

// AddTable ghi nhớ bảng thêm qua snapshot add (idempotent).
func (s *localState) AddTable(table string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.data.AddedTables {
		if t == table {
			return nil
		}
	}
	s.clearVerificationLocked()
	s.data.AddedTables = append(s.data.AddedTables, table)
	sort.Strings(s.data.AddedTables)
	return s.flushLocked()
}

// DropAddedTable quên một bảng đã thêm qua snapshot add.
func (s *localState) DropAddedTable(table string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.data.AddedTables[:0]
	for _, t := range s.data.AddedTables {
		if t != table {
			out = append(out, t)
		}
	}
	if len(out) != len(s.data.AddedTables) {
		s.clearVerificationLocked()
	}
	s.data.AddedTables = out
	return s.flushLocked()
}

// mergeAddedTables (review: snapshot add mất sau restart) hợp các bảng đã thêm vào bộ lọc. Bảng bị
// tables.exclude loại thì exclude thắng: trả trong dropped để người gọi cảnh báo và quên chúng.
func mergeAddedTables(f *tableFilter, added []string) (dropped []string) {
	for _, name := range added {
		s, t, ok := strings.Cut(name, ".")
		if !ok {
			dropped = append(dropped, name)
			continue
		}
		id := model.TableID{Schema: s, Table: t}
		if f.excluded(id) {
			dropped = append(dropped, name)
			continue
		}
		f.addIncluded(id)
	}
	return dropped
}

func (s *localState) LoadSchemaHistory() map[string][]mysqlreader.SchemaVersion {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copySchemas(s.data.SourceSchemas)
}

func (s *localState) SaveSchemaHistory(m map[string][]mysqlreader.SchemaVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.SourceSchemas = copySchemas(m) // sao chép: reader tiếp tục sửa map của nó
	return s.flushLocked()
}

func copySchemas(m map[string][]mysqlreader.SchemaVersion) map[string][]mysqlreader.SchemaVersion {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string][]mysqlreader.SchemaVersion, len(m))
	for k, vs := range m {
		out[k] = append([]mysqlreader.SchemaVersion(nil), vs...)
	}
	return out
}

func (s *localState) PGSlot() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.PGSlot
}

func (s *localState) SetPGSlot(slot string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.PGSlot = slot
	return s.flushLocked()
}

func (s *localState) VerifyPending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.VerifyPending
}

func (s *localState) SetVerifyPending(v bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.VerifyPending = v
	if v {
		s.clearVerificationLocked()
	}
	return s.flushLocked()
}

func (s *localState) verification(config string) *admin.VerificationEvidence {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.VerifyConfig != config {
		return nil
	}
	return s.data.VerifyEvidence
}

func (s *localState) clearVerificationLocked() {
	s.verificationRevision++
	s.data.VerifyEvidence, s.data.VerifyConfig = nil, ""
}

func (s *localState) invalidateVerification() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hadEvidence := s.data.VerifyEvidence != nil
	s.clearVerificationLocked()
	if hadEvidence {
		return s.flushLocked()
	}
	return nil
}

func (s *localState) verificationVersion() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.verificationRevision
}

func (s *localState) recordVerification(result *admin.VerificationEvidence, config string, pending bool, version uint64) (*admin.VerificationEvidence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.verificationRevision != version {
		return &admin.VerificationEvidence{
			Status: "incomplete", CheckedAt: result.CheckedAt,
			Reason: "table scope or schema changed during verification",
		}, nil
	}
	s.data.VerifyEvidence, s.data.VerifyConfig, s.data.VerifyPending = result, config, pending
	return result, s.flushLocked()
}

func (s *localState) Plan(table string) (snapshot.SnapshotPlan, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.data.Plans[table]
	return p, ok
}

func (s *localState) SetPlan(table string, p snapshot.SnapshotPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Plans == nil {
		s.data.Plans = map[string]snapshot.SnapshotPlan{}
	}
	s.data.Plans[table] = p
	return s.flushLocked()
}

func openLocalState(dir, pipelineID string) (*localState, error) {
	if dir == "" {
		return nil, fmt.Errorf("resources.spool.path is required (local pipeline state)")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	s := &localState{path: filepath.Join(dir, "pipeline-state.json"), data: stateFile{PipelineID: pipelineID, Done: map[string]bool{}}}
	raw, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return s, nil
	case err != nil:
		return nil, fmt.Errorf("read state: %w", err)
	}
	var f stateFile
	if err := json.Unmarshal(raw, &f); err != nil {
		// File hỏng: coi như mất, snapshot lại toàn bộ (an toàn).
		return s, nil
	}
	if f.PipelineID != pipelineID || f.Done == nil {
		return s, nil
	}
	s.data = f
	return s, nil
}

func (s *localState) IsDone(table string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Done[table]
}

func (s *localState) DoneTables() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for t, d := range s.data.Done {
		if d {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}

func (s *localState) SetDone(table string, done bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if done {
		if !s.data.Done[table] {
			s.clearVerificationLocked()
		}
		s.data.Done[table] = true
		if s.targetOf != nil {
			if s.data.Targets == nil {
				s.data.Targets = map[string]string{}
			}
			s.data.Targets[table] = s.targetOf(table)
		}
	} else {
		s.clearVerificationLocked()
		delete(s.data.Done, table)
		delete(s.data.Targets, table)
	}
	// Bảng xong hoặc phải snapshot lại từ đầu: bỏ plan cũ.
	delete(s.data.Plans, table)
	return s.flushLocked()
}

// targetChange: bảng nguồn mà đích hiện tại khác đích đã ghi trong state.
type targetChange struct {
	Table, Was, Now string
}

// planTargetMapping (thuần, review N14) so ánh xạ nguồn→đích hiện tại (current) với ánh xạ đã ghi
// (persisted): bảng Done đổi đích → conflicts (phải từ chối khởi động); bảng chưa Done đổi đích → ghi
// ánh xạ mới và bỏ plan snapshot dở (các chunk đã chép nằm ở đích cũ); bảng chưa có ánh xạ (state cũ
// hoặc bảng mới) → ghi ánh xạ hiện tại.
func planTargetMapping(done func(string) bool, persisted, current map[string]string) (conflicts []targetChange, record map[string]string, replan []string) {
	record = map[string]string{}
	names := make([]string, 0, len(current))
	for t := range current {
		names = append(names, t)
	}
	sort.Strings(names)
	for _, t := range names {
		now := current[t]
		was, ok := persisted[t]
		switch {
		case ok && was == now:
		case ok && done(t):
			conflicts = append(conflicts, targetChange{Table: t, Was: was, Now: now})
		case ok:
			record[t] = now
			replan = append(replan, t)
		default:
			record[t] = now
		}
	}
	return conflicts, record, replan
}

// ReconcileTargets áp planTargetMapping lên state. Có conflict thì không ghi gì và trả conflicts.
// legacy = state cũ có bảng Done nhưng chưa từng ghi ánh xạ (ghi ánh xạ hiện tại, không từ chối).
func (s *localState) ReconcileTargets(current map[string]string) (conflicts []targetChange, legacy bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	legacy = s.data.Targets == nil && len(s.data.Done) > 0
	conflicts, record, replan := planTargetMapping(func(t string) bool { return s.data.Done[t] }, s.data.Targets, current)
	if len(conflicts) > 0 || (len(record) == 0 && len(replan) == 0) {
		return conflicts, legacy, nil
	}
	if s.data.Targets == nil {
		s.data.Targets = map[string]string{}
	}
	for t, to := range record {
		s.data.Targets[t] = to
	}
	for _, t := range replan {
		delete(s.data.Plans, t)
	}
	return nil, legacy, s.flushLocked()
}

func (s *localState) flushLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// checkTableTargets (review N14): bảng đã Done mà tables.rename đổi bảng đích thì đích mới chỉ nhận
// CDC về sau, không có lịch sử — từ chối khởi động (exit 3). Bảng chưa Done thì chỉ ghi ánh xạ mới.
func (p *Pipeline) checkTableTargets() error {
	current := make(map[string]string, len(p.tables))
	for id := range p.tables {
		current[id.String()] = p.filter.Target(id).String()
	}
	conflicts, legacy, err := p.state.ReconcileTargets(current)
	if err != nil {
		return err
	}
	if legacy {
		p.log.Info("pipeline state has no recorded source→target table mapping (older state file); recording the current tables.rename mapping")
	}
	if len(conflicts) == 0 {
		return nil
	}
	return targetConflictError(conflicts)
}

func targetConflictError(conflicts []targetChange) error {
	parts := make([]string, 0, len(conflicts))
	for _, c := range conflicts {
		parts = append(parts, fmt.Sprintf("%s was bootstrapped into %s but tables.rename now maps it to %s", c.Table, c.Was, c.Now))
	}
	return exitcode.New(exitcode.ErrTableTargetChanged, exitcode.Rejected,
		strings.Join(parts, "; ")+": the new target would only receive future changes, without the existing rows. "+
			"Revert tables.rename, or re-bootstrap the table into the new target: add it to tables.exclude and start once "+
			"(its snapshot state is dropped), then either remove the exclude and restart (it is snapshotted into the new target) "+
			"or run `strand snapshot add --table <t>` against the running pipeline", nil)
}
