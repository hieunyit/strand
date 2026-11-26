package web

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"strand/internal/admin"
)

// Luồng sống của Web UI (docs/WEB-API.md "Live stream"): sampler lấy mẫu, Server dựng bản ghi gọn cho từng
// pipeline và phát qua hub; trình duyệt nhận bằng Server-Sent Events (GET /api/v1/stream) và cập nhật số tại
// chỗ. Không handler nào ở đây gọi admin socket.

// LiveTables là tổng hợp snapshot của một pipeline.
type LiveTables struct {
	Done       int     `json:"done"`
	Running    int     `json:"running"`
	Pending    int     `json:"pending"`
	Total      int     `json:"total"`
	Pct        float64 `json:"pct"`
	RowsCopied int64   `json:"rows_copied"`
	RowsEst    int64   `json:"rows_estimate"`
	RowsPct    float64 `json:"rows_pct"`
	ETA        float64 `json:"eta_s,omitempty"`
	// CDC activity: tables changed in the last 5 min / hour, CDC row changes applied, newest change (-1: none).
	Active5m int   `json:"active_5m"`
	Active1h int   `json:"active_1h"`
	Changes  int64 `json:"changes"`
	LastMs   int64 `json:"last_ms"`
}

// LiveTableRow là một bảng (chỉ gửi khi đổi, trừ snapshot đầy đủ).
type LiveTableRow struct {
	Table  string  `json:"table"`
	State  string  `json:"state"`
	Pct    float64 `json:"pct"`
	Copied int64   `json:"rows_copied"`
	Est    int64   `json:"rows_estimate"`
	ETA    float64 `json:"eta_s,omitempty"`
	// LastMs: last CDC change in Unix milliseconds (-1: none recorded); Changes: CDC row changes applied.
	LastMs  int64             `json:"last_ms"`
	Changes int64             `json:"changes"`
	F       map[string]string `json:"f"`
}

// LiveRecord là bản ghi gọn của một pipeline sau mỗi lần lấy mẫu.
type LiveRecord struct {
	Name      string      `json:"name"`
	State     string      `json:"state"`
	RawState  string      `json:"raw_state"`
	Reachable bool        `json:"reachable"`
	Stale     bool        `json:"stale"`
	SampledAt time.Time   `json:"sampled_at"`
	LastOK    *time.Time  `json:"last_ok,omitempty"`
	AgeS      float64     `json:"age_s"` // giây từ lần lấy mẫu tốt gần nhất, tại lúc phát
	Lag       *float64    `json:"lag_s,omitempty"`
	LagLevel  string      `json:"lag_level"`
	Rows      float64     `json:"rows_s"`
	Txn       float64     `json:"txn_s"`
	SnapRows  float64     `json:"snapshot_rows_s"`
	Zone      string      `json:"zone,omitempty"`
	Sev       int         `json:"sev"`
	Attn      bool        `json:"attention"`
	Tables    *LiveTables `json:"tables,omitempty"`
	UptimeS   float64     `json:"uptime_s,omitempty"`
	CkptAt    *time.Time  `json:"checkpoint_at,omitempty"`
	// F: chuỗi hiển thị theo khóa data-bind (định dạng giống hệt trang render ở server).
	F map[string]string `json:"f"`
	// Pt: điểm biểu đồ của mẫu này (giống /api/v1/pipelines/{name}/series).
	Pt *Sample `json:"pt,omitempty"`

	// Chỉ có ở bản đầy đủ (client đang xem pipeline này).
	TableRows  []LiveTableRow `json:"table_rows,omitempty"`
	TablesFull bool           `json:"tables_full,omitempty"` // TableRows là toàn bộ danh sách, không phải phần đổi
	// TablesChanged: danh sách nhiều bảng (> tablesInlineMax) đã đổi (bảng mới/bỏ, hoặc quá nhiều dòng đổi):
	// không gửi toàn bộ, trang dựng lại phần đang xem từ server.
	TablesChanged bool       `json:"tables_changed,omitempty"`
	EventsNew     []LogEvent `json:"events_new,omitempty"`
}

// LiveOverview là tổng hợp của trang Overview.
type LiveOverview struct {
	Total      int               `json:"total"`
	Healthy    int               `json:"healthy"`
	Attn       []string          `json:"attention"` // tên pipeline cần chú ý, xếp như trang
	MaxLag     float64           `json:"max_lag_s"`
	MaxLagName string            `json:"max_lag_pipeline,omitempty"`
	Rows       float64           `json:"rows_s"`
	TblDone    int64             `json:"tables_done"`
	TblTotal   int64             `json:"tables_total"`
	TblPct     float64           `json:"tables_pct"`
	Counts     map[string]int    `json:"counts"`
	Names      []string          `json:"pipelines"`
	F          map[string]string `json:"f"`
}

// LiveSnapshot là trạng thái đầy đủ: sự kiện `snapshot` của SSE và phản hồi của GET /api/v1/live.
type LiveSnapshot struct {
	IntervalS  float64      `json:"interval_s"`
	ServerTime time.Time    `json:"server_time"`
	LastID     uint64       `json:"last_event_id"`
	Pipeline   string       `json:"pipeline,omitempty"` // pipeline có bản đầy đủ ("" = all)
	Overview   LiveOverview `json:"overview"`
	Pipelines  []LiveRecord `json:"pipelines"`
}

// liveDelta nhớ những gì đã phát cho mỗi pipeline để chỉ gửi phần đổi (bảng, sự kiện lỗi).
type liveDelta struct {
	mu     sync.Mutex
	tables map[string]map[string]string // pipeline -> bảng -> chữ ký
	events map[string]map[string]bool   // pipeline -> khóa sự kiện
}

func tableSig(t admin.TableInfo) string {
	return fmt.Sprintf("%s|%d|%d|%.1f|%.0f", t.State, tblCopied(t), tblEst(t), t.ProgressPct, t.ETASeconds)
}

// tableActSig is the CDC-activity part of a row: changes there never force a re-render of a paged list.
func tableActSig(t admin.TableInfo) string {
	return fmt.Sprintf("|%d|%d", t.Changes(), tblLastMs(t))
}

func eventKey(e LogEvent) string { return e.Time + "|" + e.Level + "|" + e.Message }

func liveTableRow(t admin.TableInfo, now time.Time) LiveTableRow {
	return LiveTableRow{Table: t.Table, State: t.State, Pct: t.ProgressPct, Copied: tblCopied(t), Est: tblEst(t), ETA: t.ETASeconds,
		LastMs: tblLastMs(t), Changes: t.Changes(),
		F: map[string]string{"pct": fmt.Sprintf("%.1f%%", t.ProgressPct), "rows": tblRowsText(t), "eta": tblETAText(t),
			"last": tblLastText(t, now), "last_title": tblLastTitle(t), "chg": tblChangesText(t), "chg_title": tblChangesTitle(t)}}
}

// tblRowsText: "250,000 / 1,000,000" khi đang chép; bảng xong: số đã chép (và ước lượng nếu có).
func tblRowsText(t admin.TableInfo) string {
	if t.State == "done" {
		if t.RowsCopied == 0 {
			// SQL pipelines do not keep the copied count of a finished table: show the source statistics.
			if t.RowsEstimate > 0 {
				return "~" + fmtInt(t.RowsEstimate)
			}
			return "0"
		}
		s := fmtInt(t.RowsCopied)
		if t.RowsEstimate > 0 && t.RowsEstimate != t.RowsCopied {
			s += " (~" + fmtInt(t.RowsEstimate) + ")"
		}
		return s
	}
	return fmtInt(t.RowsCopied) + " / " + fmtInt(t.RowsEstimate)
}

// tblLastMs: the last CDC change of a table in Unix milliseconds, -1 when none is recorded (data-last).
func tblLastMs(t admin.TableInfo) int64 {
	if t.LastChangeAt == nil {
		return -1
	}
	return t.LastChangeAt.UnixMilli()
}

// tblLastText: "12 s ago" relative to now (the sample time), "-" without a recorded change.
func tblLastText(t admin.TableInfo, now time.Time) string {
	if t.LastChangeAt == nil {
		return "-"
	}
	if now.IsZero() {
		now = time.Now()
	}
	if now.Before(*t.LastChangeAt) {
		now = *t.LastChangeAt
	}
	return fmtRel(*t.LastChangeAt, now)
}

// tblLastTitle explains the Last sync cell: when the change was applied, its source commit time and the delay.
func tblLastTitle(t admin.TableInfo) string {
	if t.LastChangeAt == nil {
		return "No change applied to this table since the pipeline started"
	}
	s := "Last change applied to the target at " + formatWebTime(*t.LastChangeAt, "2006-01-02 15:04:05 MST")
	if t.LastSourceAt != nil {
		s += "; committed on the source at " + formatWebTime(*t.LastSourceAt, "2006-01-02 15:04:05 MST")
		if d := t.LastChangeAt.Sub(*t.LastSourceAt).Seconds(); d >= 1 {
			s += " (" + fmtSecs(d) + " later on the target)"
		}
	}
	return s
}

// tblLastHTML renders the Last sync cell; the browser keeps the relative text current from data-ago.
func tblLastHTML(t admin.TableInfo, now time.Time) template.HTML {
	if t.LastChangeAt == nil {
		return template.HTML(`<span class="muted" title="` + template.HTMLEscapeString(tblLastTitle(t)) + `">-</span>`)
	}
	return template.HTML(`<time data-ago="` + strconv.FormatInt(tblLastMs(t), 10) + `" datetime="` +
		t.LastChangeAt.UTC().Format(time.RFC3339) + `" title="` + template.HTMLEscapeString(tblLastTitle(t)) + `">` +
		template.HTMLEscapeString(tblLastText(t, now)) + `</time>`)
}

// tblChangesText: CDC row changes applied since the pipeline started ("-" when none).
func tblChangesText(t admin.TableInfo) string {
	if t.Changes() == 0 {
		return "-"
	}
	return fmtInt(t.Changes())
}

func tblChangesTitle(t admin.TableInfo) string {
	return fmtInt(t.Inserts) + " inserts, " + fmtInt(t.Updates) + " updates, " + fmtInt(t.Deletes) +
		" deletes applied by CDC since the pipeline started"
}

func tblETAText(t admin.TableInfo) string {
	if t.ETASeconds > 0 && t.State != "done" {
		return fmtSecs(t.ETASeconds)
	}
	return "-"
}

func liveTablesOf(tv TablesView, top *admin.TopInfo) *LiveTables {
	if tv.Total > 0 {
		lt := &LiveTables{Done: tv.Done, Running: tv.Running, Pending: tv.Pending, Total: tv.Total, Pct: tv.TablesPct,
			RowsCopied: tv.RowsCopied, RowsEst: tv.RowsEstimate, RowsPct: tv.RowsPct,
			Active5m: tv.Active5m, Active1h: tv.Active1h, Changes: tv.Changes, LastMs: -1}
		if tv.LastChange != nil {
			lt.LastMs = tv.LastChange.UnixMilli()
		}
		if top != nil {
			lt.ETA = top.ETASeconds
		}
		return lt
	}
	if top != nil && top.TablesTotal > 0 {
		pct := math.Min(100, float64(top.TablesDone)*100/float64(top.TablesTotal))
		return &LiveTables{Done: int(top.TablesDone), Total: int(top.TablesTotal), Pct: pct, RowsCopied: top.SnapshotRows,
			RowsEst: top.SnapshotRowsTotal, ETA: top.ETASeconds}
	}
	return nil
}

// liveRecord dựng bản ghi của một pipeline từ Snap. full=true: kèm phần đổi của bảng và sự kiện lỗi
// (allTables: toàn bộ bảng, allEvents: toàn bộ sự kiện) và cập nhật bộ nhớ delta.
func (s *Server) liveRecord(p *Pipeline, sn *Snap, pt *Sample, now time.Time) LiveRecord {
	v := s.view(p)
	r := LiveRecord{Name: p.Name, State: v.State, RawState: v.RawState, Reachable: v.Reachable, Stale: v.Stale,
		SampledAt: v.SampledAt, LagLevel: v.LagLevel, Rows: v.Rates.Rows, Txn: v.Rates.Txn, SnapRows: v.Rates.SnapRows,
		Sev: stateSev(v.State), Attn: needsAttention(v), Pt: pt}
	if !v.LastOK.IsZero() {
		t := v.LastOK
		r.LastOK = &t
		r.AgeS = math.Max(0, now.Sub(t).Seconds())
	}
	if v.Top != nil {
		lag := math.Max(0, v.Top.LagSeconds)
		r.Lag, r.Zone = &lag, v.Top.Zone
	}
	h := headFromSnap(sn)
	if h.HasInfo {
		r.UptimeS = h.Uptime
	}
	if !h.CkptAt.IsZero() {
		t := h.CkptAt
		r.CkptAt = &t
	}
	r.Tables = liveTablesOf(tablesFromSnap(sn), v.Top)
	r.F = liveFormat(v, r, h, now)
	return r
}

// liveFormat: chuỗi hiển thị theo khóa data-bind. Cùng hàm định dạng với template để số không "nhảy" khi
// trang được render lại.
func liveFormat(v PipelineView, r LiveRecord, h HeadInfo, now time.Time) map[string]string {
	f := map[string]string{"state": v.State, "reason": stateReason(v), "zone": "-", "lag": "-", "rows": "-", "txn": "-",
		"snap_rows": "-", "tables": "-", "tables_pct": "", "snap_rows_copied": "-", "eta": "-", "uptime": "-", "ckpt": "-", "ckpt_ts": "",
		"sampled": "-", "pid": ""}
	if v.Top != nil {
		f["lag"] = fmtLag(v.Top.LagSeconds)
		f["cdc_state"] = v.Top.CDCState
		if v.Top.Verification != nil {
			f["verification_status"] = v.Top.Verification.Status
			f["verification_at"] = v.Top.Verification.CheckedAt.Format(time.RFC3339Nano)
		}
		if v.Top.Zone != "" {
			f["zone"] = v.Top.Zone
		}
		if v.Top.ETASeconds > 0 {
			f["eta"] = fmtSecs(v.Top.ETASeconds)
		}
	}
	if v.Top != nil || v.Rates.OK {
		f["rows"], f["txn"], f["snap_rows"] = fmtNum(v.Rates.Rows), fmtNum(v.Rates.Txn), fmtNum(v.Rates.SnapRows)
	}
	if t := r.Tables; t != nil && t.Total > 0 {
		f["tables"] = strconv.Itoa(t.Done) + " / " + strconv.Itoa(t.Total)
		f["tables_pct"] = fmt.Sprintf("%.1f%%", t.Pct)
		f["snap_rows_copied"] = fmtInt(t.RowsCopied)
		f["snap_rows_est"] = "~" + fmtInt(t.RowsEst)
		f["rows_pct"] = fmt.Sprintf("%.1f%%", t.RowsPct)
		f["tables_done"] = strconv.Itoa(t.Done)
		f["tables_total"] = strconv.Itoa(t.Total)
		f["tables_running"] = strconv.Itoa(t.Running)
		f["tables_pending"] = strconv.Itoa(t.Pending)
		f["tables_active5m"] = strconv.Itoa(t.Active5m)
		f["tables_active1h"] = strconv.Itoa(t.Active1h)
		f["tables_changes"] = fmtInt(t.Changes)
		f["tables_last"] = "-"
		if t.LastMs >= 0 {
			f["tables_last"] = fmtRel(time.UnixMilli(t.LastMs), now)
		}
	}
	if h.HasInfo {
		f["uptime"] = fmtSecs(h.Uptime)
	}
	if !h.CkptAt.IsZero() {
		f["ckpt"], f["ckpt_ts"] = fmtRel(h.CkptAt, now), formatWebTime(h.CkptAt, "2006-01-02 15:04:05 MST")
	}
	if !v.SampledAt.IsZero() {
		f["sampled"] = formatWebTime(v.SampledAt, "15:04:05 MST")
	}
	if v.Runtime.PID != 0 {
		f["pid"] = strconv.Itoa(v.Runtime.PID)
	}
	return f
}

// fullDelta thêm vào r phần đổi của bảng và sự kiện so với lần phát trước (all=true: toàn bộ, không đổi bộ nhớ).
func (s *Server) fullDelta(p *Pipeline, r *LiveRecord, sn *Snap, all bool) {
	if sn == nil {
		return
	}
	// Chuỗi của sơ đồ Data path, thẻ Run và Checkpoint: chỉ trang chi tiết cần.
	f := make(map[string]string, len(r.F)+16)
	for k, v := range r.F {
		f[k] = v
	}
	view := s.view(p)
	f["flow_phase"] = replicationPhase(view)
	for _, d := range mongoFlow(view, sn.Status) {
		f[d.Bind] = d.Value
	}
	for _, d := range flowDiagnostics(sn.Metrics) {
		f[d.Bind] = d.Value
	}
	for _, n := range s.graphNodes(s.view(p), sn.Status, sn.Metrics) {
		for i, b := range n.Binds {
			if b != "" && i < len(n.Lines) {
				f[b] = n.Lines[i]
			}
		}
	}
	if sn.Status != nil {
		f["governor"] = fmtGovernor(sn.Status.Governor)
	}
	if c := sn.Checkpoint; c != nil {
		f["ckpt_applied"], f["epoch"] = c.Applied, strconv.FormatInt(c.Epoch, 10)
	}
	r.F = f
	tv := tablesFromSnap(sn)
	ev := EventsToLog(sn.Events)
	for i := range ev {
		ev[i].Message = MaskSecrets(ev[i].Message)
		ev[i].Raw = ""
	}
	big := len(tv.Tables) > tablesInlineMax
	if all {
		if big {
			// Nhiều bảng: chỉ các bảng đang chép (trang đang xem lấy phần còn lại từ HTML/API phân trang).
			for _, t := range tv.Tables {
				if t.State == "running" && len(r.TableRows) < tablesInlineMax {
					r.TableRows = append(r.TableRows, liveTableRow(t, sn.At))
				}
			}
		} else {
			r.TablesFull = true
			for _, t := range tv.Tables {
				r.TableRows = append(r.TableRows, liveTableRow(t, sn.At))
			}
		}
		r.EventsNew = ev
		return
	}
	s.delta.mu.Lock()
	defer s.delta.mu.Unlock()
	if sn.Tables != nil {
		prev := s.delta.tables[r.Name]
		cur := make(map[string]string, len(tv.Tables))
		// actRows: rows whose only change is CDC activity (counts, last change). A paged list updates them in
		// place and never re-renders for them; past tablesInlineMax the most recently changed are sent.
		var actRows []admin.TableInfo
		for _, t := range tv.Tables {
			snapSig := tableSig(t)
			sig := snapSig + tableActSig(t)
			cur[t.Table] = sig
			old, seen := prev[t.Table]
			switch {
			case prev == nil || !seen || !strings.HasPrefix(old, snapSig+"|"):
				r.TableRows = append(r.TableRows, liveTableRow(t, sn.At))
			case old != sig:
				actRows = append(actRows, t)
			}
		}
		if big && len(actRows) > tablesInlineMax {
			sort.Slice(actRows, func(i, j int) bool { return tblLastMs(actRows[i]) > tblLastMs(actRows[j]) })
			actRows = actRows[:tablesInlineMax]
		}
		snapChanged := len(r.TableRows)
		for _, t := range actRows {
			r.TableRows = append(r.TableRows, liveTableRow(t, sn.At))
		}
		switch {
		case big && prev != nil && (len(prev) != len(cur) || snapChanged > tablesInlineMax):
			// Nhiều bảng: không bao giờ gửi cả danh sách; client dựng lại trang đang xem.
			r.TableRows, r.TablesChanged = nil, true
		case big && prev == nil:
			// Lần đầu (web vừa khởi động): chỉ gửi bảng đang chép.
			var run []LiveTableRow
			for _, row := range r.TableRows {
				if row.State == "running" && len(run) < tablesInlineMax {
					run = append(run, row)
				}
			}
			r.TableRows = run
		case prev == nil || len(prev) != len(cur):
			// Danh sách bảng đổi (bảng mới, bảng bị bỏ): gửi toàn bộ để client dựng lại.
			r.TableRows, r.TablesFull = nil, true
			for _, t := range tv.Tables {
				r.TableRows = append(r.TableRows, liveTableRow(t, sn.At))
			}
		}
		s.delta.tables[r.Name] = cur
	}
	if sn.Events != nil {
		prev := s.delta.events[r.Name]
		cur := make(map[string]bool, len(ev))
		for _, e := range ev {
			k := eventKey(e)
			cur[k] = true
			if prev != nil && !prev[k] {
				r.EventsNew = append(r.EventsNew, e)
			}
		}
		s.delta.events[r.Name] = cur
	}
}

func (s *Server) liveOverview(now time.Time) LiveOverview {
	ov := s.samp.BuildOverview()
	b := overviewBody{Overview: ov}
	for _, p := range s.reg.List() {
		b.Views = append(b.Views, s.view(p))
	}
	sortViews(b.Views)
	b.summarize()
	lo := LiveOverview{Total: ov.Total, Healthy: b.Healthy, MaxLag: ov.MaxLagSeconds, MaxLagName: ov.MaxLagPipeline, Rows: ov.RowsPerSec,
		TblDone: b.TblDone, TblTotal: b.TblTotal, TblPct: b.TblPct, Counts: ov.Counts, Attn: []string{}, Names: []string{}}
	for _, v := range b.Attn {
		lo.Attn = append(lo.Attn, v.Name)
	}
	for _, v := range b.Views {
		lo.Names = append(lo.Names, v.Name)
	}
	sort.Strings(lo.Names)
	maxLag := "0.00 s"
	if ov.MaxLagPipeline != "" {
		maxLag = fmtLag(ov.MaxLagSeconds)
	}
	sub := strconv.Itoa(b.Healthy) + " healthy"
	if len(b.Attn) > 0 {
		sub += ", " + strconv.Itoa(len(b.Attn)) + " need attention"
	}
	lo.F = map[string]string{"total": strconv.Itoa(ov.Total), "healthy_sub": sub, "attn": strconv.Itoa(len(b.Attn)),
		"max_lag": maxLag, "max_lag_pipeline": "no lag", "rows": fmtNum(ov.RowsPerSec), "tables_done": "-", "tables_total": "",
		"tables_pct": ""}
	if ov.MaxLagPipeline != "" {
		lo.F["max_lag_pipeline"] = ov.MaxLagPipeline
	}
	if b.TblTotal > 0 {
		lo.F["tables_done"] = strconv.FormatInt(b.TblDone, 10)
		lo.F["tables_total"] = "/ " + strconv.FormatInt(b.TblTotal, 10)
		lo.F["tables_pct"] = fmt.Sprintf("%.1f%%", b.TblPct)
	}
	for st, n := range ov.Counts {
		lo.F["count_"+st] = strconv.Itoa(n)
	}
	return lo
}

// snapshotFor dựng trạng thái đầy đủ; pipeline != "" thì bản ghi của pipeline đó kèm toàn bộ bảng và sự kiện.
func (s *Server) snapshotFor(pipeline string, lastID uint64) LiveSnapshot {
	now := s.now()
	out := LiveSnapshot{IntervalS: s.samp.Interval().Seconds(), ServerTime: now, LastID: lastID, Pipeline: pipeline,
		Overview: s.liveOverview(now), Pipelines: []LiveRecord{}}
	for _, p := range s.reg.List() {
		sn := s.samp.Latest(p.Name)
		r := s.liveRecord(p, sn, nil, now)
		if p.Name == pipeline {
			s.fullDelta(p, &r, sn, true)
		}
		out.Pipelines = append(out.Pipelines, r)
	}
	return out
}

// onSample (móc của sampler): phát bản ghi của pipeline vừa lấy mẫu.
func (s *Server) onSample(name string, sn *Snap, smp Sample) {
	if s.isActive() {
		s.db.metrics.add(name, smp) // lịch sử số liệu trong DB (ghi theo lô; chỉ instance active)
	}
	p := s.reg.Get(name)
	if p == nil {
		return
	}
	s.observeStop(p, sn.Runtime) // dừng êm không do web: desired -> stopped (không tự chạy lại)
	now := s.now()
	pt := smp
	lite := s.liveRecord(p, sn, &pt, now)
	full := lite
	s.fullDelta(p, &full, sn, false)
	s.hub.publish("sample", name, full, lite)
}

// onRound (móc của sampler): phát tổng hợp Overview; danh sách pipeline đổi thì phát `structure`.
func (s *Server) onRound(structural bool) {
	s.refreshDesired()
	now := s.now()
	ov := s.liveOverview(now)
	if structural {
		s.hub.publish("structure", "", map[string]any{"pipelines": ov.Names}, nil)
	}
	s.hub.publish("overview", "", ov, nil)
}

// ---- HTTP ----

const (
	sseRetryMS     = 3000
	sseWriteWindow = 30 * time.Second
	sseMaxStreams  = 128
	logPollEvery   = 2 * time.Second
)

func (s *Server) streamPipelineParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := r.URL.Query().Get("pipeline")
	if name == "" || name == "all" {
		return "", true
	}
	if s.reg.Get(name) == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return "", false
	}
	return name, true
}

// apiLive: GET /api/v1/live?pipeline=<name>|all — trạng thái đầy đủ dạng JSON (client không dùng được SSE thì hỏi
// định kỳ endpoint này). Chỉ đọc bộ nhớ của sampler.
func (s *Server) apiLive(w http.ResponseWriter, r *http.Request, _ *Session) {
	name, ok := s.streamPipelineParam(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, s.snapshotFor(name, s.hub.lastID()))
}

// apiStream: GET /api/v1/stream?pipeline=<name>|all[&logs=1] — Server-Sent Events.
func (s *Server) apiStream(w http.ResponseWriter, r *http.Request, sess *Session) {
	name, ok := s.streamPipelineParam(w, r)
	if !ok {
		return
	}
	logs := name != "" && r.URL.Query().Get("logs") == "1"
	if n := s.streams.Add(1); n > sseMaxStreams {
		s.streams.Add(-1)
		w.Header().Set("Retry-After", "10")
		writeErr(w, http.StatusServiceUnavailable, "too many live streams")
		return
	}
	defer s.streams.Add(-1)
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("X-Accel-Buffering", "no") // nginx: không gom đệm
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	var lastID uint64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		lastID, _ = strconv.ParseUint(strings.TrimSpace(v), 10, 64)
	} else if v := r.URL.Query().Get("last_event_id"); v != "" {
		lastID, _ = strconv.ParseUint(v, 10, 64)
	}
	sub, replay, resumed, seq := s.hub.subscribe(name, lastID)
	defer s.hub.unsubscribe(sub)

	write := func(id uint64, typ string, data []byte) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteWindow))
		var b strings.Builder
		if id > 0 {
			fmt.Fprintf(&b, "id: %d\n", id)
		}
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", typ, data)
		if _, err := w.Write([]byte(b.String())); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	comment := func(c string) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteWindow))
		if _, err := fmt.Fprintf(w, ": %s\n\n", c); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	sendSnapshot := func(id uint64) bool {
		b, err := jsonMarshal(s.snapshotFor(name, id))
		return err == nil && write(id, "snapshot", b)
	}
	_ = rc.SetWriteDeadline(time.Now().Add(sseWriteWindow))
	if _, err := fmt.Fprintf(w, "retry: %d\n\n", sseRetryMS); err != nil {
		return
	}
	if resumed {
		for _, ev := range replay {
			if !write(ev.ID, ev.Type, ev.payloadFor(name)) {
				return
			}
		}
		if len(replay) == 0 && !comment("resumed") {
			return
		}
	} else if !sendSnapshot(seq) {
		return
	}

	hb := time.NewTicker(s.sseHeartbeat)
	defer hb.Stop()
	var logTick <-chan time.Time
	var tail logTail
	if logs {
		lt := time.NewTicker(logPollEvery)
		defer lt.Stop()
		logTick = lt.C
		if !s.sendLogs(r.Context(), name, &tail, write) {
			return
		}
	}
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-sub.ch:
			if sub.lagged.Swap(false) {
				// Client chậm làm rơi sự kiện: gửi lại trạng thái đầy đủ (client bỏ bản ghi cũ hơn).
				if !sendSnapshot(ev.ID) {
					return
				}
				continue
			}
			if !write(ev.ID, ev.Type, ev.payloadFor(name)) {
				return
			}
		case <-hb.C:
			if !s.streamAuthValid(r) {
				_ = write(0, "bye", []byte(`{"reason":"session ended"}`))
				return
			}
			if !comment("hb " + strconv.FormatInt(time.Now().Unix(), 10)) {
				return
			}
		case <-logTick:
			if !s.sendLogs(ctx, name, &tail, write) {
				return
			}
		}
	}
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// streamAuthValid kiểm lại phiên của một luồng đang mở (đăng xuất hoặc hết hạn thì đóng luồng).
func (s *Server) streamAuthValid(r *http.Request) bool {
	if tok := bearer(r); tok != "" {
		return s.apiTokenHash() != "" && constEq(hashKey(tok), s.apiTokenHash())
	}
	c, err := r.Cookie(sessionCookie)
	return err == nil && s.sessions.Get(c.Value) != nil
}

// logTail nhớ các dòng log đã gửi cho một kết nối.
type logTail struct {
	prev []string
	sent bool
}

// newLines trả các dòng của cur chưa có trong prev: tìm đoạn đuôi dài nhất của prev trùng đầu cur.
// overlap=false khi không trùng chút nào (log quay vòng hoặc hơn len(cur) dòng mới).
func newLines(prev, cur []string) (out []string, overlap bool) {
	if len(prev) == 0 {
		return cur, false
	}
	maxK := len(prev)
	if len(cur) < maxK {
		maxK = len(cur)
	}
	for k := maxK; k > 0; k-- {
		match := true
		for i := 0; i < k; i++ {
			if prev[len(prev)-k+i] != cur[i] {
				match = false
				break
			}
		}
		if match {
			return cur[k:], true
		}
	}
	return cur, false
}

const logTailLines = 200

func (s *Server) sendLogs(ctx context.Context, name string, t *logTail, write func(uint64, string, []byte) bool) bool {
	p := s.reg.Get(name)
	if p == nil {
		return true
	}
	lctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	lines, err := s.reg.Logs(lctx, p, logTailLines)
	cancel()
	payload := map[string]any{"pipeline": name}
	if err != nil {
		payload["error"] = err.Error()
		b, _ := jsonMarshal(payload)
		return write(0, "log", b)
	}
	for i := range lines {
		lines[i] = MaskSecrets(lines[i])
	}
	add, overlap := newLines(t.prev, lines)
	t.prev = lines
	if t.sent && len(add) == 0 {
		return true
	}
	payload["lines"] = add
	payload["initial"] = !t.sent // lần đầu: client ghép với các dòng trang đã render (bỏ trùng)
	payload["gap"] = t.sent && !overlap
	t.sent = true
	b, err := jsonMarshal(payload)
	if err != nil {
		return true
	}
	return write(0, "log", b)
}
