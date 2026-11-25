package runner

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
)

// ConsistentTime labels a standalone restore point. Logical dumps use the
// recorded job start, which is only an approximation of their snapshot time.
func ConsistentTime(r backup.Record) time.Time {
	if r.Method == backup.MethodMongodump && r.Details["oplog_boundary"] == "embedded-v1" {
		if ts, err := ParseOplogTS(r.EndPosition.OplogTS); err == nil {
			return time.Unix(int64(ts.T)+1, 0).UTC()
		}
	}
	switch r.Method {
	case backup.MethodMySQLDump, backup.MethodMariaDBDump, backup.MethodPGDump, backup.MethodMydumper:
		if r.StartTime != nil {
			return *r.StartTime
		}
	default:
		if r.EndTime != nil {
			return *r.EndTime
		}
		if r.StartTime != nil {
			return *r.StartTime
		}
	}
	return r.CreatedAt
}

// pitrBase reports whether r can anchor log replay.
func pitrBase(r backup.Record) bool {
	if r.KindOrFull() == backup.KindLog || r.CopyOf != "" || RequireStandaloneBackup(r) != nil {
		return false
	}
	switch r.Engine {
	case backup.EngineMySQL:
		return r.Position.GTIDSet != "" && r.Method == backup.MethodMySQLDump && mysqlDumpHasEndTime(r)
	case backup.EnginePostgres:
		return r.Method == backup.MethodPGBaseBackup && r.EndPosition.LSN != ""
	case backup.EngineMongoDB:
		_, err := ParseOplogTS(r.EndPosition.OplogTS)
		return r.Method == backup.MethodMongodump && r.Details["databases"] == "" &&
			r.Details["oplog_boundary"] == "embedded-v1" && err == nil
	}
	return false
}

func mysqlDumpHasEndTime(r backup.Record) bool {
	return r.EndTime != nil && !r.EndTime.IsZero() && !r.EndTime.Before(ConsistentTime(r))
}

func pitrBaseEarliestTime(r backup.Record) time.Time {
	if r.Method == backup.MethodMySQLDump && mysqlDumpHasEndTime(r) {
		// The dump's snapshot is acquired sometime before the stream ends.
		// mysqlbinlog excludes events from the cutoff second onwards, so use
		// the first whole second strictly after that upper bound.
		return r.EndTime.Truncate(time.Second).Add(time.Second)
	}
	return ConsistentTime(r)
}

func logEnd(r backup.Record) time.Time {
	if r.EndTime != nil {
		return *r.EndTime
	}
	return r.CreatedAt
}

// sortedLogs returns the complete log segments of rs in archive order.
func sortedLogs(rs []backup.Record) []backup.Record {
	var logs []backup.Record
	for _, r := range rs {
		if r.KindOrFull() == backup.KindLog && r.Status == backup.StatusComplete && r.CopyOf == "" {
			logs = append(logs, r)
		}
	}
	sort.SliceStable(logs, func(i, j int) bool { return logSeq(logs[i]) < logSeq(logs[j]) })
	return logs
}

// lastRun returns the contiguous run of log segments that ends with the newest segment.
func lastRun(logs []backup.Record) []backup.Record {
	if len(logs) == 0 {
		return nil
	}
	start := len(logs) - 1
	for start > 0 && Contiguous(logs[start-1], logs[start]) {
		start--
	}
	return logs[start:]
}

// Window is the recoverable range of a source.
type Window struct {
	Start *time.Time `json:"start,omitempty"`
	End   *time.Time `json:"end,omitempty"`
	// Continuous is true when any time in [Start, End] can be restored (base + logs); false means only
	// the discrete restore points exist (Start = oldest, End = newest).
	Continuous bool `json:"continuous"`
	// VerifyOnReplay permits trying PostgreSQL PITR without asserting an end timestamp.
	VerifyOnReplay bool `json:"verify_on_replay,omitempty"`
}

// RecoveryWindow computes the window from a source's complete records.
func RecoveryWindow(rs []backup.Record) Window {
	var w Window
	groups := map[archiveLineage][]backup.Record{}
	for _, log := range sortedLogs(rs) {
		if log.Details["source_identity"] != "" {
			key := archiveLineageOf(log)
			groups[key] = append(groups[key], log)
		}
	}
	var bases []backup.Record
	for _, r := range rs {
		if r.Status == backup.StatusComplete && r.KindOrFull() != backup.KindLog {
			bases = append(bases, r)
		}
	}
	for _, b := range bases {
		t := ConsistentTime(b)
		if w.Start == nil || t.Before(*w.Start) {
			w.Start = &t
		}
		if w.End == nil || t.After(*w.End) {
			w.End = &t
		}
	}
	var continuous Window
	for lineage, logs := range groups {
		if lineage.method == backup.MethodWAL {
			for _, base := range bases {
				if pitrBase(base) && samePITRLineage(logs[0], base) && slices.ContainsFunc(logs, func(log backup.Record) bool {
					return logCoversBase(log, base)
				}) {
					w.VerifyOnReplay = true
					break
				}
			}
			continue
		}
		if lineage.method != backup.MethodBinlog && lineage.method != backup.MethodOplog {
			continue
		}
		run := lastRun(logs)
		end := logEnd(run[len(run)-1])
		var start *time.Time
		for _, base := range bases {
			t := pitrBaseEarliestTime(base)
			if !pitrBase(base) || t.After(end) || (start != nil && !t.Before(*start)) {
				continue
			}
			for _, log := range run {
				if samePITRLineage(log, base) && logCoversBase(log, base) {
					start = &t
					break
				}
			}
		}
		if start != nil && (continuous.End == nil || end.After(*continuous.End) ||
			(end.Equal(*continuous.End) && start.Before(*continuous.Start))) {
			continuous = Window{Start: start, End: &end, Continuous: true}
		}
	}
	if continuous.Continuous {
		return continuous
	}
	return w
}

// RestorePlan contains the base backup and the log segments to replay.
type RestorePlan struct {
	Chain          []backup.Record `json:"chain"` // retained in the plan format, built-in restore accepts one base
	Logs           []backup.Record `json:"logs"`
	Target         *time.Time      `json:"target,omitempty"`
	VerifyOnReplay bool            `json:"verify_target_on_replay,omitempty"`
}

// Base returns the restore point the logs are replayed on.
func (p RestorePlan) Base() backup.Record { return p.Chain[len(p.Chain)-1] }

// RequireStandaloneBackup keeps legacy incremental artifacts available for native tools.
func RequireStandaloneBackup(r backup.Record) error {
	if r.KindOrFull() == backup.KindIncremental || r.ParentID != "" {
		return fmt.Errorf("incremental restore point %s requires `strand backup fetch` and the native restore tool", r.ID)
	}
	return nil
}

func restoreChain(base backup.Record, byID map[string]backup.Record) ([]backup.Record, error) {
	chain := []backup.Record{base}
	seen := map[string]bool{base.ID: true}
	for id := base.ParentID; id != ""; {
		if seen[id] {
			return nil, fmt.Errorf("incremental chain of %s contains a cycle at %s", base.ID, id)
		}
		parent, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("incremental chain of %s is broken: %s is missing", base.ID, id)
		}
		if parent.Status != backup.StatusComplete {
			return nil, fmt.Errorf("incremental chain of %s is broken: %s is %s (only complete restore points can be restored)", base.ID, id, parent.Status)
		}
		if parent.KindOrFull() == backup.KindLog {
			return nil, fmt.Errorf("incremental chain of %s has a log segment as parent: %s", base.ID, id)
		}
		seen[id] = true
		chain = append(chain, parent)
		id = parent.ParentID
	}
	slices.Reverse(chain)
	return chain, nil
}

// PlanRestore chooses what to restore. With no target time, the chosen backup is restored as is.
// With a target time, an explicit base is pinned. Otherwise the newest eligible base with suitable
// logs is chosen. MySQL needs an event time at or after the target. PostgreSQL proves the time
// target during replay.
func PlanRestore(rs []backup.Record, chosen *backup.Record, target *time.Time) (RestorePlan, error) {
	// A failed, expired or unfinished record is never a restore point (its payload is partial or gone).
	if chosen != nil {
		if err := backup.RequireComplete(*chosen, "restored"); err != nil {
			return RestorePlan{}, err
		}
	}
	byID := map[string]backup.Record{}
	for _, r := range rs {
		byID[r.ID] = r
	}
	if target == nil {
		if chosen == nil {
			return RestorePlan{}, errors.New("choose a backup or a target time")
		}
		if chosen.KindOrFull() == backup.KindLog {
			return RestorePlan{}, errors.New("a log segment is not a restore point")
		}
		chain, err := restoreChain(*chosen, byID)
		if err != nil {
			return RestorePlan{}, err
		}
		if err := RequireStandaloneBackup(*chosen); err != nil {
			return RestorePlan{}, err
		}
		return RestorePlan{Chain: chain}, nil
	}
	if chosen != nil && chosen.Engine == backup.EngineMySQL || slices.ContainsFunc(rs, func(r backup.Record) bool { return r.Engine == backup.EngineMySQL }) {
		if err := checkMySQLTargetTime(*target); err != nil {
			return RestorePlan{}, err
		}
	}
	if chosen != nil {
		if err := RequireStandaloneBackup(*chosen); err != nil {
			return RestorePlan{}, err
		}
		if chosen.Method == backup.MethodMySQLDump {
			if err := checkMySQLPITRBaseTime(*chosen, *target); err != nil {
				return RestorePlan{}, err
			}
		}
	}
	var bases []backup.Record
	for _, r := range byID {
		if chosen != nil && r.ID != chosen.ID {
			continue
		}
		if r.Status != backup.StatusComplete || !pitrBase(r) || pitrBaseEarliestTime(r).After(*target) {
			continue
		}
		if r.Engine == backup.EngineMongoDB {
			cutoff, err := mongoRecoveryCutoff(*target)
			at, _ := ParseOplogTS(r.EndPosition.OplogTS)
			if err != nil || at.Compare(cutoff) >= 0 {
				continue
			}
		}
		bases = append(bases, r)
	}

	if len(bases) == 0 {
		return RestorePlan{}, fmt.Errorf("no base backup that supports point-in-time recovery is consistent before %s", target.UTC().Format(time.RFC3339))
	}
	slices.SortFunc(bases, func(a, b backup.Record) int {
		if byTime := ConsistentTime(b).Compare(ConsistentTime(a)); byTime != 0 {
			return byTime
		}
		return cmp.Compare(b.ID, a.ID)
	})
	archives := sortedLogs(rs)
	var newestErr error
	for _, base := range bases {
		plan, err := planPITRFromBase(base, byID, archives, target)
		if err == nil {
			return plan, nil
		}
		if newestErr == nil {
			newestErr = err
		}
	}
	return RestorePlan{}, newestErr
}

// checkMySQLTargetTime refuses a point-in-time target with fractional seconds: mysqlbinlog --stop-datetime
// has one-second resolution, so the replay would silently stop up to a second before the requested time.
func checkMySQLTargetTime(target time.Time) error {
	if target.Nanosecond() != 0 {
		return fmt.Errorf("MySQL point-in-time recovery needs a whole-second target time (got %s); binlog replay stops at whole seconds, use %s or %s",
			target.UTC().Format(time.RFC3339Nano), target.UTC().Truncate(time.Second).Format(time.RFC3339),
			target.UTC().Truncate(time.Second).Add(time.Second).Format(time.RFC3339))
	}
	return nil
}

func checkMySQLPITRBaseTime(base backup.Record, target time.Time) error {
	if !mysqlDumpHasEndTime(base) {
		return fmt.Errorf("base backup %s has no valid dump completion time for point-in-time recovery", base.ID)
	}
	if earliest := pitrBaseEarliestTime(base); target.Before(earliest) {
		return fmt.Errorf("base backup %s can only anchor point-in-time recovery at or after %s, following dump completion",
			base.ID, earliest.UTC().Format(time.RFC3339))
	}
	return nil
}

func planPITRFromBase(base backup.Record, byID map[string]backup.Record, archives []backup.Record, target *time.Time) (RestorePlan, error) {
	chain, err := restoreChain(base, byID)
	if err != nil {
		return RestorePlan{}, err
	}
	var logs []backup.Record
	for _, l := range archives {
		if samePITRLineage(l, base) {
			logs = append(logs, l)
		}
	}
	first := -1
	for i, l := range logs {
		if logCoversBase(l, base) {
			first = i
			break
		}
	}
	if first < 0 {
		return RestorePlan{}, fmt.Errorf("no archived log proves coverage of the recovery position for base backup %s", base.ID)
	}
	var plan []backup.Record
	for i := first; i < len(logs); i++ {
		if i > first && !Contiguous(logs[i-1], logs[i]) {
			if base.Engine == backup.EnginePostgres {
				break // PostgreSQL must prove it reached the target during recovery.
			}
			return RestorePlan{}, fmt.Errorf("log gap between %s and %s before the target time", logs[i-1].ID, logs[i].ID)
		}
		plan = append(plan, logs[i])
		if base.Engine != backup.EnginePostgres && !logEnd(logs[i]).Before(*target) {
			return RestorePlan{Chain: chain, Logs: plan, Target: target}, nil
		}
	}
	if base.Engine == backup.EnginePostgres {
		return RestorePlan{Chain: chain, Logs: plan, Target: target, VerifyOnReplay: true}, nil
	}
	return RestorePlan{}, fmt.Errorf("target time %s is after the newest archived log (%s)", target.UTC().Format(time.RFC3339),
		logEnd(logs[len(logs)-1]).UTC().Format(time.RFC3339))
}

type archiveLineage struct {
	source, engine, method, jobID, identity, timeline string
}

func archiveLineageOf(r backup.Record) archiveLineage {
	return archiveLineage{r.Source, r.Engine, r.Method, r.JobID, r.Details["source_identity"], r.Details["timeline"]}
}

func samePITRLineage(l, base backup.Record) bool {
	id := base.Details["source_identity"]
	if id == "" || l.Details["source_identity"] != id || l.Engine != base.Engine || l.Source != base.Source ||
		base.JobID == "" || l.JobID != base.JobID {
		return false
	}
	switch base.Engine {
	case backup.EngineMySQL:
		return l.Method == backup.MethodBinlog
	case backup.EngineMongoDB:
		return l.Method == backup.MethodOplog && l.Details["oplog_boundary"] == "exclusive-v1"
	case backup.EnginePostgres:
		// Cross-timeline replay requires trusted timeline history.
		return l.Method == backup.MethodWAL && base.Details["timeline"] != "" && base.Details["timeline"] == l.Details["timeline"]
	default:
		return false
	}
}

// logCoversBase reports whether segment l contains (or follows directly) the base's recovery position.
func logCoversBase(l, base backup.Record) bool {
	switch l.Method {
	case backup.MethodWAL:
		start, startErr := ParseLSN(l.Position.LSN)
		end, endErr := ParseLSN(l.EndPosition.LSN)
		baseEnd, baseErr := ParseLSN(base.EndPosition.LSN)
		return startErr == nil && endErr == nil && baseErr == nil && start <= baseEnd && baseEnd < end
	case backup.MethodOplog:
		from, startErr := ParseOplogTS(l.Position.OplogTS)
		to, endErr := ParseOplogTS(l.EndPosition.OplogTS)
		at, baseErr := ParseOplogTS(base.EndPosition.OplogTS)
		return startErr == nil && endErr == nil && baseErr == nil && from.Compare(at) <= 0 && at.Compare(to) < 0
	case backup.MethodBinlog:
		pos := base.Position
		return pos.BinlogFile != "" && pos.BinlogPos >= 4 &&
			l.Position.BinlogFile == pos.BinlogFile && l.EndPosition.BinlogFile == pos.BinlogFile &&
			l.Position.BinlogPos >= 4 && l.Position.BinlogPos <= pos.BinlogPos &&
			l.EndPosition.BinlogPos > pos.BinlogPos
	}
	return false
}

// ParseLSN parses "X/Y".
func ParseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(s, "/")
	a, err1 := strconv.ParseUint(hi, 16, 32)
	b, err2 := strconv.ParseUint(lo, 16, 32)
	if !ok || err1 != nil || err2 != nil {
		return 0, fmt.Errorf("invalid LSN %q", s)
	}
	return a<<32 | b, nil
}

// FormatLSN renders an LSN as "X/Y".
func FormatLSN(v uint64) string { return fmt.Sprintf("%X/%X", v>>32, uint32(v)) }

func lsnLess(a, b string) bool {
	x, err1 := ParseLSN(a)
	y, err2 := ParseLSN(b)
	return err1 == nil && err2 == nil && x < y
}
