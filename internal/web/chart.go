package web

import (
	"fmt"
	"html"
	"html/template"
	"math"
	"strconv"
	"strings"
	"time"
)

// Biểu đồ SVG render phía server, không dùng JS hay thư viện ngoài. Màu và font nằm ở CSS (class),
// nên CSP `default-src 'self'` (cấm style nội tuyến) vẫn đúng.

// Point là một điểm (thời gian, giá trị).
type Point struct {
	T time.Time
	V float64
}

// ChartSeries là một đường của biểu đồ.
type ChartSeries struct {
	Name   string
	Class  string // s1, s2, s3 (màu trong CSS)
	Points []Point
}

// FormatValue định dạng số cho nhãn trục: 1.2k, 3.4M, 0.25.
func FormatValue(v float64) string {
	a := math.Abs(v)
	switch {
	case a >= 1e9:
		return fmt.Sprintf("%.1fG", v/1e9)
	case a >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case a >= 1e4:
		return fmt.Sprintf("%.1fk", v/1e3)
	case a >= 100:
		return fmt.Sprintf("%.0f", v)
	case a >= 10:
		return fmt.Sprintf("%.1f", v)
	case a == 0:
		return "0"
	}
	return fmt.Sprintf("%.2f", v)
}

// niceMax làm tròn lên một giá trị đẹp (1, 2, 5 × 10^k) để trục y dễ đọc.
func niceMax(v float64) float64 {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return 1
	}
	exp := math.Floor(math.Log10(v))
	f := v / math.Pow(10, exp)
	var n float64
	switch {
	case f <= 1:
		n = 1
	case f <= 2:
		n = 2
	case f <= 5:
		n = 5
	default:
		n = 10
	}
	return n * math.Pow(10, exp)
}

// LineChart vẽ biểu đồ đường nhiều series theo thời gian. Không có dữ liệu thì trả khung "no data".
func LineChart(title, unit string, series []ChartSeries, w, h int) template.HTML {
	const padL, padR, padT, padB = 52, 12, 10, 24
	var sb strings.Builder
	fmt.Fprintf(&sb, `<figure class="chart"><figcaption><span class="ctitle">%s</span> <span class="unit">%s</span>%s</figcaption>`, html.EscapeString(title), html.EscapeString(unit), lastReadout(series))
	var tmin, tmax time.Time
	vmax := 0.0
	n := 0
	for _, s := range series {
		for _, p := range s.Points {
			if math.IsNaN(p.V) || math.IsInf(p.V, 0) {
				continue
			}
			if n == 0 || p.T.Before(tmin) {
				tmin = p.T
			}
			if n == 0 || p.T.After(tmax) {
				tmax = p.T
			}
			if p.V > vmax {
				vmax = p.V
			}
			n++
		}
	}
	fmt.Fprintf(&sb, `<svg class="linechart" viewBox="0 0 %d %d" role="img" aria-label="%s">`, w, h, html.EscapeString(title))
	if n == 0 {
		fmt.Fprintf(&sb, `<text class="nodata" x="%d" y="%d" text-anchor="middle">no data</text></svg></figure>`, w/2, h/2)
		return template.HTML(sb.String())
	}
	ymax := niceMax(vmax)
	pw, ph := float64(w-padL-padR), float64(h-padT-padB)
	span := tmax.Sub(tmin).Seconds()
	xOf := func(t time.Time) float64 {
		if span <= 0 {
			return float64(padL) + pw
		}
		return float64(padL) + pw*t.Sub(tmin).Seconds()/span
	}
	yOf := func(v float64) float64 { return float64(padT) + ph*(1-v/ymax) }
	// Horizontal grid at 0, 25, 50, 75, 100 % of the (rounded) maximum.
	for _, f := range []float64{0, 0.25, 0.5, 0.75, 1} {
		y := yOf(ymax * f)
		fmt.Fprintf(&sb, `<line class="grid" x1="%d" x2="%d" y1="%.1f" y2="%.1f"/>`, padL, w-padR, y, y)
		fmt.Fprintf(&sb, `<text class="axis" x="%d" y="%.1f" text-anchor="end">%s</text>`, padL-4, y+3, FormatValue(ymax*f))
	}
	// Time axis: start, middle and end, with tick marks.
	for i, f := range []float64{0, 0.5, 1} {
		x := float64(padL) + pw*f
		anchor := []string{"start", "middle", "end"}[i]
		fmt.Fprintf(&sb, `<line class="tick" x1="%.1f" x2="%.1f" y1="%d" y2="%d"/>`, x, x, h-padB, h-padB+3)
		fmt.Fprintf(&sb, `<text class="axis" x="%.1f" y="%d" text-anchor="%s">%s</text>`, x, h-6, anchor, tmin.Add(time.Duration(float64(tmax.Sub(tmin))*f)).Format("15:04:05"))
	}
	for _, s := range series {
		var pts []string
		for _, p := range s.Points {
			if math.IsNaN(p.V) || math.IsInf(p.V, 0) {
				continue
			}
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", xOf(p.T), yOf(p.V)))
		}
		if len(pts) == 0 {
			continue
		}
		cls := s.Class
		if cls == "" {
			cls = "s1"
		}
		if len(pts) == 1 { // một điểm: vẽ chấm
			xy := strings.Split(pts[0], ",")
			fmt.Fprintf(&sb, `<circle class="pt %s" cx="%s" cy="%s" r="2.5"><title>%s</title></circle>`, cls, xy[0], xy[1], html.EscapeString(s.Name))
			continue
		}
		xy0, xyN := strings.Split(pts[0], ","), strings.Split(pts[len(pts)-1], ",")
		fmt.Fprintf(&sb, `<path class="area %s" d="M%s,%d L%s L%s,%d Z"/>`, cls, xy0[0], h-padB, strings.Join(pts, " L"), xyN[0], h-padB)
		fmt.Fprintf(&sb, `<polyline class="line %s" points="%s"><title>%s</title></polyline>`, cls, strings.Join(pts, " "), html.EscapeString(s.Name))
		fmt.Fprintf(&sb, `<circle class="pt %s" cx="%s" cy="%s" r="2.5"/>`, cls, xyN[0], xyN[1])
	}
	sb.WriteString(`</svg>`)
	if len(series) > 0 {
		sb.WriteString(`<div class="legend">`)
		for _, s := range series {
			cls := s.Class
			if cls == "" {
				cls = "s1"
			}
			fmt.Fprintf(&sb, `<span class="key %s">%s</span>`, cls, html.EscapeString(s.Name))
		}
		sb.WriteString(`</div>`)
	}
	sb.WriteString(`</figure>`)
	return template.HTML(sb.String())
}

// lastReadout shows the latest value of the first series next to the chart title.
func lastReadout(series []ChartSeries) string {
	if len(series) == 0 || len(series[0].Points) == 0 {
		return ""
	}
	v := series[0].Points[len(series[0].Points)-1].V
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return ""
	}
	return fmt.Sprintf(` <span class="now">now <b>%s</b></span>`, FormatValue(v))
}

// Sparkline vẽ đường nhỏ không trục cho bảng/danh sách.
func Sparkline(vals []float64, w, h int) template.HTML {
	if len(vals) < 2 {
		return template.HTML(fmt.Sprintf(`<svg class="spark" viewBox="0 0 %d %d" aria-hidden="true" data-spark="%s"></svg>`, w, h, sparkValues(vals)))
	}
	vmax := 0.0
	for _, v := range vals {
		if v > vmax && !math.IsInf(v, 0) {
			vmax = v
		}
	}
	if vmax <= 0 {
		vmax = 1
	}
	pts := make([]string, 0, len(vals))
	for i, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			v = 0
		}
		x := float64(w-2)*float64(i)/float64(len(vals)-1) + 1
		y := float64(h-2)*(1-v/vmax) + 1
		pts = append(pts, fmt.Sprintf("%.1f,%.1f", x, y))
	}
	last := strings.Split(pts[len(pts)-1], ",")
	lastX, lastY := last[0], last[1]
	return template.HTML(fmt.Sprintf(`<svg class="spark" viewBox="0 0 %d %d" aria-hidden="true" data-spark="%s"><polyline class="line s1" points="%s"/><circle class="pt s1" cx="%s" cy="%s" r="1.8"/></svg>`, w, h, sparkValues(vals), strings.Join(pts, " "), lastX, lastY))
}

// sparkValues: các giá trị của sparkline (app.js nối thêm điểm mới và vẽ lại tại chỗ).
func sparkValues(vals []float64) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			v = 0
		}
		parts[i] = strconv.FormatFloat(v, 'g', 6, 64)
	}
	return strings.Join(parts, ",")
}

// ProgressBar vẽ thanh tiến độ bằng SVG (CSP cấm style nội tuyến nên không dùng width: x%).
func ProgressBar(pct float64) template.HTML {
	if pct < 0 || math.IsNaN(pct) {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	cls := "pbar"
	if pct >= 100 {
		cls = "pbar done"
	}
	return template.HTML(fmt.Sprintf(`<svg class="%s" viewBox="0 0 100 8" preserveAspectRatio="none" role="img" aria-label="%.1f%%"><rect class="bg" x="0" y="0" width="100" height="8"/><rect class="fg" x="0" y="0" width="%.2f" height="8"/></svg>`, cls, pct, pct))
}

// GraphNode là một ô của sơ đồ pipeline.
type GraphNode struct {
	Title string
	Lines []string
	Binds []string // khóa data-bind của từng dòng ("" = tĩnh), để app.js cập nhật tại chỗ
	Class string   // ok | warn | bad | idle
}

// PipelineGraph vẽ Source → Assembler → Scheduler → Writer×N → Target (dòng chảy trái sang phải).
func PipelineGraph(nodes []GraphNode) template.HTML {
	const nw, nh, gap = 150, 96, 34
	w := len(nodes)*nw + (len(nodes)-1)*gap + 4
	var sb strings.Builder
	fmt.Fprintf(&sb, `<svg class="graph" viewBox="0 0 %d %d" role="img" aria-label="pipeline graph"><defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path class="arrowhead" d="M0,0 L10,5 L0,10 z"/></marker></defs>`, w, nh+4)
	for i, n := range nodes {
		x := 2 + i*(nw+gap)
		cls := n.Class
		if cls == "" {
			cls = "idle"
		}
		fmt.Fprintf(&sb, `<g class="node %s" data-graph-node><rect x="%d" y="2" width="%d" height="%d" rx="6"/>`, cls, x, nw, nh)
		fmt.Fprintf(&sb, `<text class="ntitle" x="%d" y="22">%s</text>`, x+10, html.EscapeString(n.Title))
		for j, ln := range n.Lines {
			if j >= 4 {
				break
			}
			bind := ""
			if j < len(n.Binds) && n.Binds[j] != "" {
				bind = ` data-bind="` + html.EscapeString(n.Binds[j]) + `"`
			}
			fmt.Fprintf(&sb, `<text class="nline" x="%d" y="%d"%s>%s</text>`, x+10, 42+j*16, bind, html.EscapeString(ln))
		}
		sb.WriteString(`</g>`)
		if i < len(nodes)-1 {
			fmt.Fprintf(&sb, `<line class="edge" x1="%d" y1="%d" x2="%d" y2="%d" marker-end="url(#arr)"/>`, x+nw, nh/2+2, x+nw+gap-2, nh/2+2)
		}
	}
	sb.WriteString(`</svg>`)
	return template.HTML(sb.String())
}

// BigProgressBar is the wide bar of the snapshot summary; label names what it measures (for screen readers).
func BigProgressBar(pct float64, label string) template.HTML {
	if pct < 0 || math.IsNaN(pct) {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	cls := "pbar big"
	if pct >= 100 {
		cls += " done"
	}
	return template.HTML(fmt.Sprintf(`<svg class="%s" viewBox="0 0 100 8" preserveAspectRatio="none" role="img" aria-label="%s: %.1f%%"><rect class="bg" x="0" y="0" width="100" height="8"/><rect class="fg" x="0" y="0" width="%.2f" height="8"/></svg>`, cls, html.EscapeString(label), pct, pct))
}
