package runner

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
)

// ConsistentTime is the time a base backup's data is consistent to: the snapshot start for logical
// single-transaction dumps, the end for physical backups and mongodump --oplog.
func ConsistentTime(r backup.Record) time.Time {
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
	if r.KindOrFull() == backup.KindLog || r.CopyOf != "" {
		return false
	}
	switch r.Engine {
	case backup.EngineMySQL:
		return r.Position.GTIDSet != "" && r.Method == backup.MethodMySQLDump
	case backup.EnginePostgres:
		return r.Method == backup.MethodPGBaseBackup && r.Position.LSN != ""
	case backup.EngineMongoDB:
		return r.Method == backup.MethodMongodump && r.Position.OplogTS != ""
	}
	return false
}

func logEnd(r backup.Record) time.Time {
	if r.EndTime != nil {
		return *r.EndTime
	}
	return r.CreatedAt
}

func logStart(r backup.Record) time.Time {
	if r.StartTime != nil {
		return *r.StartTime
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
}

// RecoveryWindow computes the window from a source's complete records.
func RecoveryWindow(rs []backup.Record) Window {
	var w Window
	run := lastRun(sortedLogs(rs))
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
	if len(run) == 0 {
		return w
	}
	runStart, runEnd := logStart(run[0]), logEnd(run[len(run)-1])
	var earliest *time.Time
	for _, b := range bases {
		if !pitrBase(b) {
			continue
		}
		t := ConsistentTime(b)
		if !t.Before(runStart) && !t.After(runEnd) && (earliest == nil || t.Before(*earliest)) {
			tt := t
			earliest = &tt
		}
	}
	if earliest != nil {
		w.Continuous = true
		if w.Start == nil || earliest.Before(*w.Start) {
			w.Start = earliest
		}
		if w.End == nil || runEnd.After(*w.End) {
			e := runEnd
			w.End = &e
		}
	}
	return w
}

// RestorePlan is a base backup (plus its incremental parents) and the log segments to replay.
type RestorePlan struct {
	Chain  []backup.Record `json:"chain"` // full first, then incrementals, the base last
	Logs   []backup.Record `json:"logs"`
	Target *time.Time      `json:"target,omitempty"`
}

// Base returns the restore point the logs are replayed on.
func (p RestorePlan) Base() backup.Record { return p.Chain[len(p.Chain)-1] }

// PlanRestore chooses what to restore. With target nil, rec (the chosen backup) is restored as is. With a
// target time, the newest PITR base consistent at or before the target is chosen automatically, then the
// contiguous log segments from that base up to the first segment ending at or after the target.
func PlanRestore(rs []backup.Record, chosen *backup.Record, target *time.Time) (RestorePlan, error) {
	// A failed, expired or unfinished record is never a restore point (its payload is partial or gone).
	if chosen != nil {
		if err := backup.RequireComplete(*chosen, "restored"); err != nil {
			return RestorePlan{}, err
		}
	}
	byID := map[string]backup.Record{}
	notComplete := map[string]backup.Status{}
	for _, r := range rs {
		if r.Status == backup.StatusComplete {
			byID[r.ID] = r
		} else {
			notComplete[r.ID] = r.Status
		}
	}
	chainOf := func(b backup.Record) ([]backup.Record, error) {
		chain := []backup.Record{b}
		for p := b.ParentID; p != ""; {
			pr, ok := byID[p]
			if !ok {
				if other, known := notComplete[p]; known {
					return nil, fmt.Errorf("incremental chain of %s is broken: %s is %s (only complete restore points can be restored)", b.ID, p, other)
				}
				return nil, fmt.Errorf("incremental chain of %s is broken: %s is missing", b.ID, p)
			}
			chain = append([]backup.Record{pr}, chain...)
			p = pr.ParentID
		}
		return chain, nil
	}
	if target == nil {
		if chosen == nil {
			return RestorePlan{}, errors.New("choose a backup or a target time")
		}
		if chosen.KindOrFull() == backup.KindLog {
			return RestorePlan{}, errors.New("a log segment is not a restore point")
		}
		chain, err := chainOf(*chosen)
		return RestorePlan{Chain: chain}, err
	}
	var base *backup.Record
	for _, r := range byID {
		if chosen != nil && r.ID != chosen.ID {
			continue
		}
		if !pitrBase(r) || ConsistentTime(r).After(*target) {
			continue
		}
		if base == nil || ConsistentTime(r).After(ConsistentTime(*base)) {
			rr := r
			base = &rr
		}
	}
	if base == nil {
		return RestorePlan{}, fmt.Errorf("no base backup that supports point-in-time recovery is consistent before %s", target.UTC().Format(time.RFC3339))
	}
	chain, err := chainOf(*base)
	if err != nil {
		return RestorePlan{}, err
	}
	logs := sortedLogs(rs)
	first := -1
	for i, l := range logs {
		if logCoversBase(l, *base) {
			first = i
			break
		}
	}
	if first < 0 {
		return RestorePlan{}, fmt.Errorf("no archived logs after base backup %s", base.ID)
	}
	var plan []backup.Record
	for i := first; i < len(logs); i++ {
		if i > first && !Contiguous(logs[i-1], logs[i]) {
			return RestorePlan{}, fmt.Errorf("log gap between %s and %s before the target time", logs[i-1].ID, logs[i].ID)
		}
		plan = append(plan, logs[i])
		if !logEnd(logs[i]).Before(*target) {
			return RestorePlan{Chain: chain, Logs: plan, Target: target}, nil
		}
	}
	return RestorePlan{}, fmt.Errorf("target time %s is after the newest archived log (%s)", target.UTC().Format(time.RFC3339),
		logEnd(logs[len(logs)-1]).UTC().Format(time.RFC3339))
}

// logCoversBase reports whether segment l contains (or follows directly) the base's recovery position.
func logCoversBase(l, base backup.Record) bool {
	switch l.Method {
	case backup.MethodWAL:
		return lsnLess(base.Position.LSN, l.EndPosition.LSN)
	case backup.MethodOplog:
		a, err1 := ParseOplogTS(l.EndPosition.OplogTS)
		b, err2 := ParseOplogTS(base.Position.OplogTS)
		return err1 == nil && err2 == nil && (a.T > b.T || (a.T == b.T && a.I >= b.I))
	case backup.MethodBinlog:
		// Segments are replayed with --exclude-gtids=<base GTID set>, so starting one segment early is
		// harmless; start with the segment of the base's binlog file, else the first ending after the base.
		if f := l.Details["server_file"]; f != "" && base.Position.BinlogFile != "" {
			return f >= base.Position.BinlogFile
		}
		return !logEnd(l).Before(ConsistentTime(base))
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
