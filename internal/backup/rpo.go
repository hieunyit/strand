package backup

import (
	"sort"
	"time"
)

// RPO bases: which restore point the current RPO is measured from.
const (
	RPOBasisLog    = "log"    // last archived log segment (PITR sources)
	RPOBasisBackup = "backup" // last complete backup
	RPOBasisNone   = "none"   // no restore point yet
)

// RPOInput is what the RPO of one source depends on.
type RPOInput struct {
	Source   string
	JobID    string
	JobName  string
	Engine   string
	PolicyID string
	// PITR: the job archives logs, so the newest log segment is the latest restore point.
	PITR bool
	// TargetMinutes is the policy's rpo_minutes (0 = no target, never a breach).
	TargetMinutes int
	RTOMinutes    int
	// LastBackup is the point of the newest complete backup (its consistent point: start time, else
	// creation time); LastLogEnd the end of the newest archived log segment.
	LastBackup *time.Time
	LastLogEnd *time.Time
}

// RPOStatus is the computed state of a source.
type RPOStatus struct {
	Source        string     `json:"source"`
	JobID         string     `json:"job_id,omitempty"`
	JobName       string     `json:"job_name,omitempty"`
	Engine        string     `json:"engine,omitempty"`
	PolicyID      string     `json:"policy_id,omitempty"`
	PITR          bool       `json:"pitr"`
	Basis         string     `json:"basis"`
	LastPoint     *time.Time `json:"last_restore_point,omitempty"`
	CurrentSecs   int64      `json:"current_rpo_seconds"` // -1 when there is no restore point
	TargetMinutes int        `json:"rpo_target_minutes"`
	RTOMinutes    int        `json:"rto_minutes"`
	Breach        bool       `json:"breach"`
}

// ComputeRPO = now - latest restore point: the last log segment for a PITR source, else the last backup. A
// PITR source whose log archive has not produced a segment yet falls back to its last backup. A source
// without any restore point has no RPO (CurrentSecs -1) and is not flagged: a new job would otherwise
// alarm before its first run.
func ComputeRPO(now time.Time, in RPOInput) RPOStatus {
	st := RPOStatus{Source: in.Source, JobID: in.JobID, JobName: in.JobName, Engine: in.Engine, PolicyID: in.PolicyID, PITR: in.PITR,
		Basis: RPOBasisNone, CurrentSecs: -1, TargetMinutes: in.TargetMinutes, RTOMinutes: in.RTOMinutes}
	point := in.LastBackup
	if point != nil {
		st.Basis = RPOBasisBackup
	}
	if in.PITR && in.LastLogEnd != nil && (point == nil || in.LastLogEnd.After(*point)) {
		point, st.Basis = in.LastLogEnd, RPOBasisLog
	}
	if point == nil {
		return st
	}
	p := *point
	st.LastPoint = &p
	secs := int64(now.Sub(p).Seconds())
	if secs < 0 {
		secs = 0
	}
	st.CurrentSecs = secs
	st.Breach = in.TargetMinutes > 0 && secs > int64(in.TargetMinutes)*60
	return st
}

// SortRPO orders statuses by source for stable output.
func SortRPO(s []RPOStatus) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Source != s[j].Source {
			return s[i].Source < s[j].Source
		}
		return s[i].JobID < s[j].JobID
	})
}
