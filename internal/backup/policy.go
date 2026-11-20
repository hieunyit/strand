package backup

import (
	"fmt"
	"reflect"
	"time"
)

// Policy is a reusable backup policy: schedule, retention, storage, protection and verification. A job
// follows its policy for every field not listed in Job.Overrides.
type Policy struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Builtin     bool   `json:"builtin,omitempty"` // seeded Gold/Silver/Bronze (editable)

	Schedule      Schedule        `json:"schedule"`
	WindowMinutes int             `json:"window_minutes,omitempty"`
	JitterSeconds int             `json:"jitter_seconds,omitempty"`
	Retention     RetentionPolicy `json:"retention"`
	// LogArchive turns on continuous log archiving (PITR) for jobs whose method supports it;
	// LogFlushSeconds bounds the RPO. RPOMinutes is the target, RTOMinutes informational.
	LogArchive      bool `json:"log_archive,omitempty"`
	LogFlushSeconds int  `json:"log_flush_seconds,omitempty"`
	RPOMinutes      int  `json:"rpo_minutes,omitempty"`
	RTOMinutes      int  `json:"rto_minutes,omitempty"`
	// Storage and protection. Empty Repository/ReplicateTo leave the job's own value alone.
	Repository    string `json:"repository,omitempty"`
	ReplicateTo   string `json:"replicate_to,omitempty"`
	Compress      bool   `json:"compress,omitempty"`
	CompressLevel int    `json:"compress_level,omitempty"`
	Encrypt       bool   `json:"encrypt,omitempty"`
	// Verification.
	VerifyEverySeconds  int  `json:"verify_every_seconds,omitempty"`
	VerifySamplePercent int  `json:"verify_sample_percent,omitempty"`
	TestRestore         bool `json:"test_restore,omitempty"`
	// Retry.
	RetryMax            int `json:"retry_max,omitempty"`
	RetryBackoffSeconds int `json:"retry_backoff_seconds,omitempty"`

	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Validate checks the policy values.
func (p Policy) Validate() error {
	bad := func(s string) error { return fmt.Errorf("%w: policy %s", ErrInvalid, s) }
	if !validID(p.ID) {
		return bad("id is invalid")
	}
	if !safeText(p.Name, 96) {
		return bad("name is required and must be at most 96 characters")
	}
	if len(p.Description) > 512 || hasControl(p.Description) {
		return bad("description is too long")
	}
	if err := p.Schedule.Validate(); err != nil {
		return err
	}
	if p.WindowMinutes < 0 || p.WindowMinutes > 7*24*60 || p.JitterSeconds < 0 || p.JitterSeconds > 3600 {
		return bad("window_minutes (0-10080) or jitter_seconds (0-3600) is out of range")
	}
	if err := p.Retention.Validate(); err != nil {
		return err
	}
	if p.LogFlushSeconds != 0 && (p.LogFlushSeconds < 10 || p.LogFlushSeconds > 86400) {
		return bad("log_flush_seconds must be between 10 and 86400")
	}
	if p.RPOMinutes < 0 || p.RPOMinutes > 525600 || p.RTOMinutes < 0 || p.RTOMinutes > 525600 {
		return bad("rpo_minutes/rto_minutes are out of range")
	}
	for _, r := range []string{p.Repository, p.ReplicateTo} {
		if r != "" && !validID(r) {
			return bad("invalid repository id")
		}
	}
	if p.ReplicateTo != "" && p.ReplicateTo == p.Repository {
		return bad("replicate_to must name a different repository")
	}
	if p.CompressLevel < 0 || p.CompressLevel > 4 {
		return bad("compress_level must be between 0 and 4")
	}
	if p.VerifyEverySeconds != 0 && (p.VerifyEverySeconds < 300 || p.VerifyEverySeconds > 365*24*3600) {
		return bad("verify_every_seconds must be between 300 and one year")
	}
	if p.VerifySamplePercent < 0 || p.VerifySamplePercent > 100 {
		return bad("verify_sample_percent must be between 0 and 100")
	}
	if p.RetryMax < 0 || p.RetryMax > 20 || p.RetryBackoffSeconds < 0 || p.RetryBackoffSeconds > 86400 ||
		(p.RetryMax > 0 && p.RetryBackoffSeconds < 10) {
		return bad("retry_max (0-20) and retry_backoff_seconds (10-86400 when retrying) are out of range")
	}
	return nil
}

// BuiltinPolicies are the Gold/Silver/Bronze policies seeded once.
func BuiltinPolicies() []Policy {
	const week, month = 7 * 24 * 3600, 30 * 24 * 3600
	return []Policy{
		{ID: "policy-gold", Name: "Gold", Builtin: true, Description: "Daily full at 01:00, continuous log archiving (RPO 5 min), weekly verify with test restore",
			Schedule: Schedule{Kind: ScheduleDaily, At: "01:00", Timezone: "UTC"}, WindowMinutes: 120,
			Retention: RetentionPolicy{KeepDaily: 14, KeepWeekly: 8, KeepMonthly: 12}, LogArchive: true, LogFlushSeconds: 300,
			RPOMinutes: 5, RTOMinutes: 60, Compress: true, VerifyEverySeconds: week, TestRestore: true},
		{ID: "policy-silver", Name: "Silver", Builtin: true, Description: "Daily full at 01:00, weekly verify",
			Schedule: Schedule{Kind: ScheduleDaily, At: "01:00", Timezone: "UTC"}, WindowMinutes: 240,
			Retention: RetentionPolicy{KeepDaily: 7, KeepWeekly: 4}, RPOMinutes: 1440, RTOMinutes: 240, Compress: true,
			VerifyEverySeconds: week},
		{ID: "policy-bronze", Name: "Bronze", Builtin: true, Description: "Weekly full on Sunday 02:00, monthly verify",
			Schedule: Schedule{Kind: ScheduleWeekly, At: "02:00", Days: []int{0}, Timezone: "UTC"}, WindowMinutes: 720,
			Retention: RetentionPolicy{KeepWeekly: 4}, RPOMinutes: 10080, RTOMinutes: 1440, Compress: true,
			VerifyEverySeconds: month},
	}
}

// Overridable job fields (names used in Job.Overrides).
var overridable = []string{"schedule", "window_minutes", "jitter_seconds", "retention", "log_archive", "log_flush_seconds",
	"repository", "replicate_to", "compress", "compress_level", "encrypt", "verify_every_seconds", "verify_sample_percent",
	"test_restore", "retry_max", "retry_backoff_seconds"}

// OverridableFields lists the field names a job can override.
func OverridableFields() []string { return append([]string(nil), overridable...) }

// IsOverridable reports a valid Job.Overrides entry.
func IsOverridable(f string) bool {
	for _, o := range overridable {
		if o == f {
			return true
		}
	}
	return false
}

// EffectiveSchedule is the job's schedule: Schedule, else interval(every_seconds).
func (j Job) EffectiveSchedule() Schedule {
	if j.Schedule != nil {
		s := *j.Schedule
		if s.Kind == ScheduleInterval && s.EverySeconds == 0 {
			s.EverySeconds = j.EverySeconds
		}
		return s
	}
	return Schedule{Kind: ScheduleInterval, EverySeconds: j.EverySeconds}
}

// NominalSeconds is the typical gap between two runs (used for every_seconds, which the retry back-off and
// the legacy columns still read): the distance between the next two runs, clamped to [60 s, 365 d].
func (s Schedule) NominalSeconds() int {
	if s.Kind == ScheduleInterval && s.EverySeconds > 0 {
		return s.EverySeconds
	}
	n, err := s.NextN(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 2)
	gap := 86400
	if err == nil {
		gap = int(n[1].Sub(n[0]) / time.Second)
	}
	if gap < 60 {
		gap = 60
	}
	if gap > 365*86400 {
		gap = 365 * 86400
	}
	return gap
}

// NormalizeSchedule keeps every_seconds consistent with the schedule: interval schedules and plain
// every_seconds jobs are untouched; other kinds get their nominal gap as every_seconds.
func NormalizeSchedule(j *Job) {
	if j.Schedule == nil {
		return
	}
	if j.Schedule.Kind == ScheduleInterval {
		if j.Schedule.EverySeconds == 0 {
			j.Schedule.EverySeconds = j.EverySeconds
		}
		j.EverySeconds = j.Schedule.EverySeconds
		return
	}
	j.EverySeconds = j.Schedule.NominalSeconds()
}

type policyField struct {
	name string
	get  func(Job) any
	pget func(Policy) any
	set  func(*Job, Policy)
	// managed: whether the policy value applies (empty storage ids leave the job alone).
	managed func(Policy) bool
}

func always(Policy) bool { return true }

var policyFields = []policyField{
	{"schedule", func(j Job) any { return j.EffectiveSchedule() }, func(p Policy) any { return p.Schedule },
		func(j *Job, p Policy) { s := p.Schedule; j.Schedule = &s }, always},
	{"window_minutes", func(j Job) any { return j.WindowMinutes }, func(p Policy) any { return p.WindowMinutes },
		func(j *Job, p Policy) { j.WindowMinutes = p.WindowMinutes }, always},
	{"jitter_seconds", func(j Job) any { return j.JitterSeconds }, func(p Policy) any { return p.JitterSeconds },
		func(j *Job, p Policy) { j.JitterSeconds = p.JitterSeconds }, always},
	{"retention", func(j Job) any { return j.Retention }, func(p Policy) any { return p.Retention },
		func(j *Job, p Policy) { j.Retention = p.Retention }, always},
	{"log_archive", func(j Job) any { return j.LogArchive }, func(p Policy) any { return p.LogArchive },
		func(j *Job, p Policy) { j.LogArchive = p.LogArchive }, always},
	{"log_flush_seconds", func(j Job) any { return j.LogFlushSeconds }, func(p Policy) any { return p.LogFlushSeconds },
		func(j *Job, p Policy) { j.LogFlushSeconds = p.LogFlushSeconds }, always},
	{"repository", func(j Job) any { return j.Repository }, func(p Policy) any { return p.Repository },
		func(j *Job, p Policy) { j.Repository = p.Repository }, func(p Policy) bool { return p.Repository != "" }},
	{"replicate_to", func(j Job) any { return j.ReplicateTo }, func(p Policy) any { return p.ReplicateTo },
		func(j *Job, p Policy) { j.ReplicateTo = p.ReplicateTo }, func(p Policy) bool { return p.ReplicateTo != "" }},
	{"compress", func(j Job) any { return j.Compress }, func(p Policy) any { return p.Compress },
		func(j *Job, p Policy) { j.Compress = p.Compress }, always},
	{"compress_level", func(j Job) any { return j.CompressLevel }, func(p Policy) any { return p.CompressLevel },
		func(j *Job, p Policy) { j.CompressLevel = p.CompressLevel }, always},
	{"encrypt", func(j Job) any { return j.Encrypt }, func(p Policy) any { return p.Encrypt },
		func(j *Job, p Policy) { j.Encrypt = p.Encrypt }, always},
	{"verify_every_seconds", func(j Job) any { return j.VerifyEverySeconds }, func(p Policy) any { return p.VerifyEverySeconds },
		func(j *Job, p Policy) { j.VerifyEverySeconds = p.VerifyEverySeconds }, always},
	{"verify_sample_percent", func(j Job) any { return j.VerifySamplePercent }, func(p Policy) any { return p.VerifySamplePercent },
		func(j *Job, p Policy) { j.VerifySamplePercent = p.VerifySamplePercent }, always},
	{"test_restore", func(j Job) any { return j.TestRestore }, func(p Policy) any { return p.TestRestore },
		func(j *Job, p Policy) { j.TestRestore = p.TestRestore }, always},
	{"retry_max", func(j Job) any { return j.RetryMax }, func(p Policy) any { return p.RetryMax },
		func(j *Job, p Policy) { j.RetryMax = p.RetryMax }, always},
	{"retry_backoff_seconds", func(j Job) any { return j.RetryBackoffSeconds }, func(p Policy) any { return p.RetryBackoffSeconds },
		func(j *Job, p Policy) { j.RetryBackoffSeconds = p.RetryBackoffSeconds }, always},
}

// DeriveOverrides names the policy-managed fields whose value in `want` differs from the policy; present
// limits the check to fields the caller actually sent.
func DeriveOverrides(want Job, p Policy, present func(field string) bool) []string {
	var out []string
	for _, f := range policyFields {
		if !present(f.name) || !f.managed(p) {
			continue
		}
		if !reflect.DeepEqual(f.get(want), f.pget(p)) {
			out = append(out, f.name)
		}
	}
	return out
}

// ApplyPolicy materializes the policy into the job: every managed field not in j.Overrides takes the policy
// value. Settings the job's method cannot honour are switched off and reported as warnings, so a policy
// edit never makes a stored job unrunnable: PITR on a method/database set without PITR support, and
// test-restore on a job without a scratch connection or a supporting method. A job that OVERRIDES
// log_archive/test_restore keeps its value (ValidateJobForSave then rejects it if unsupported).
func ApplyPolicy(j Job, p Policy) (Job, []string) {
	skip := map[string]bool{}
	for _, o := range j.Overrides {
		skip[o] = true
	}
	if j.IsCustom() { // wrapper-script jobs upload to their own bucket: no repository, no replication
		skip["repository"], skip["replicate_to"] = true, true
	}
	for _, f := range policyFields {
		if !skip[f.name] && f.managed(p) {
			f.set(&j, p)
		}
	}
	var warn []string
	if !skip["log_archive"] && j.LogArchive && !j.IsCustom() {
		if err := ValidatePITR(j); err != nil {
			j.LogArchive = false
			warn = append(warn, fmt.Sprintf("policy %q requests PITR but it is off for this job: %v", p.Name, err))
		}
	} else if !skip["log_archive"] && j.LogArchive && j.IsCustom() {
		j.LogArchive = false
	}
	if !skip["test_restore"] && j.TestRestore {
		supported := j.Method == MethodMySQLDump || j.Method == MethodMariaDBDump || j.Method == MethodPGDump
		if !supported || j.Scratch.Host == "" {
			j.TestRestore = false
			warn = append(warn, fmt.Sprintf("policy %q requests test-restore but it is off for this job (needs mysqldump, mariadb-dump or pg_dump and a scratch connection)", p.Name))
		}
	}
	j.PolicyID = p.ID
	NormalizeSchedule(&j)
	return j, warn
}
