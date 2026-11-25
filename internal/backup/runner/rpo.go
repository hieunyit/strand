package runner

import (
	"time"

	"strand/internal/backup"
)

// PITREnd returns the latest time a source can be recovered to by replaying archived logs: the end of an
// unbroken run of log segments (binlog, WAL or oplog) that reaches back over a complete standalone base
// backup of the same lineage (pitrBase: not a replica, not an incremental), the newest such run winning.
// It is nil when there is no such run: no base backup, or segments of another lineage. Segments after a
// gap do not extend the window. RPO credits archived logs only up to this point; a log segment alone
// restores nothing.
//
// For binlog and oplog, when the newest run is anchored this is RecoveryWindow's continuous End.
// PostgreSQL WAL is included as well: RecoveryWindow does not assert a WAL window (the target time is
// verified during replay), but a contiguous WAL run from a covered base is replayable to its last segment.
func PITREnd(rs []backup.Record) *time.Time {
	groups := map[archiveLineage][]backup.Record{}
	for _, log := range sortedLogs(rs) {
		if log.Details["source_identity"] != "" {
			key := archiveLineageOf(log)
			groups[key] = append(groups[key], log)
		}
	}
	var bases []backup.Record
	for _, r := range rs {
		if r.Status == backup.StatusComplete && pitrBase(r) {
			bases = append(bases, r)
		}
	}
	var best *time.Time
	anchored := func(run []backup.Record, end time.Time) bool {
		for _, base := range bases {
			if pitrBaseEarliestTime(base).After(end) {
				continue
			}
			for _, log := range run {
				if samePITRLineage(log, base) && logCoversBase(log, base) {
					return true
				}
			}
		}
		return false
	}
	for lineage, logs := range groups {
		switch lineage.method {
		case backup.MethodBinlog, backup.MethodOplog, backup.MethodWAL:
		default:
			continue
		}
		// Walk the contiguous runs from the newest; the first anchored one is this lineage's window.
		for stop := len(logs); stop > 0; {
			start := stop - 1
			for start > 0 && Contiguous(logs[start-1], logs[start]) {
				start--
			}
			run := logs[start:stop]
			end := logEnd(run[len(run)-1])
			if best != nil && !end.After(*best) {
				break
			}
			if anchored(run, end) {
				best = &end
				break
			}
			stop = start
		}
	}
	return best
}
