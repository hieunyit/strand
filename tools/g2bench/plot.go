package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"time"
)

// PlotRun vẽ SVG ba panel (R, P, p99 cửa sổ 10 s) từ telemetry và log client thô của một lần chạy.
// Không phụ thuộc thư viện ngoài; số liệu lấy từ rows, không có dữ liệu giả.
func PlotRun(path string, rec RunRecord, rows []TimeRow) error {
	if len(rows) == 0 {
		return fmt.Errorf("no rows to plot")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	const W, H, ml, mr, mt, ph, gap = 900.0, 640.0, 60.0, 20.0, 40.0, 150.0, 40.0
	t0 := rows[0].T
	tN := rows[len(rows)-1].T
	span := tN.Sub(t0).Seconds()
	if span <= 0 {
		span = 1
	}
	x := func(t time.Time) float64 { return ml + (W-ml-mr)*t.Sub(t0).Seconds()/span }
	fmt.Fprintf(w, `<svg xmlns="http://www.w3.org/2000/svg" width="%g" height="%g" font-family="sans-serif" font-size="11">`+"\n", W, H)
	fmt.Fprintf(w, `<text x="%g" y="20" font-size="14">%s (governor %s) - telemetry + client raw logs</text>`+"\n", ml, rec.Name, rec.Governor)
	type panel struct {
		title string
		val   func(TimeRow) float64
		hline []float64
		col   string
	}
	panels := []panel{
		{"R (tokens/s, source_iops); dashed = soft / hard", func(r TimeRow) float64 { return r.Sample.R }, []float64{float64(rec.SoftIOPS), float64(rec.HardIOPS)}, "#1f77b4"},
		{"P (pressure total)", func(r TimeRow) float64 { return r.Sample.P }, nil, "#d95f02"},
		{"app p99 over 10 s window, client-side (ms)", func(r TimeRow) float64 { return r.P99ms }, nil, "#2a9d4b"},
	}
	for i, p := range panels {
		top := mt + float64(i)*(ph+gap)
		lo, hi := math.Inf(1), math.Inf(-1)
		for _, r := range rows {
			if v := p.val(r); finite(v) {
				lo, hi = math.Min(lo, v), math.Max(hi, v)
			}
		}
		for _, h := range p.hline {
			hi = math.Max(hi, h)
		}
		if !finite(lo) {
			lo, hi = 0, 1
		}
		lo = math.Min(lo, 0)
		if hi <= lo {
			hi = lo + 1
		}
		y := func(v float64) float64 { return top + ph - ph*(v-lo)/(hi-lo) }
		fmt.Fprintf(w, `<text x="%g" y="%g">%s</text>`+"\n", ml, top-6, p.title)
		fmt.Fprintf(w, `<rect x="%g" y="%g" width="%g" height="%g" fill="none" stroke="#999"/>`+"\n", ml, top, W-ml-mr, ph)
		// vùng snapshot
		if !rec.StrandStart.IsZero() && !rec.BootstrapFinished.IsZero() {
			a := math.Max(x(rec.StrandStart), ml)
			b := math.Min(x(rec.BootstrapFinished), W-mr)
			if b > a {
				fmt.Fprintf(w, `<rect x="%g" y="%g" width="%g" height="%g" fill="#888" fill-opacity="0.12"/>`+"\n", a, top, b-a, ph)
			}
		}
		for _, h := range p.hline {
			fmt.Fprintf(w, `<line x1="%g" x2="%g" y1="%g" y2="%g" stroke="#c00" stroke-dasharray="4 3"/>`+"\n", ml, W-mr, y(h), y(h))
		}
		fmt.Fprintf(w, `<text x="%g" y="%g" text-anchor="end">%.3g</text><text x="%g" y="%g" text-anchor="end">%.3g</text>`+"\n", ml-4, top+10, hi, ml-4, top+ph, lo)
		var pts string
		flush := func() {
			if pts != "" {
				fmt.Fprintf(w, `<polyline fill="none" stroke="%s" stroke-width="1.3" points="%s"/>`+"\n", p.col, pts)
				pts = ""
			}
		}
		for _, r := range rows {
			v := p.val(r)
			if !finite(v) || (i < 2 && !r.Sample.OK) {
				flush() // mẫu lỗi/thiếu: ngắt đường, không nội suy
				continue
			}
			pts += fmt.Sprintf("%.1f,%.1f ", x(r.T), y(v))
		}
		flush()
	}
	fmt.Fprintf(w, `<text x="%g" y="%g">t (s) from first sample; shaded = snapshot window [strand start, bootstrap finished)</text>`+"\n", ml, H-8)
	fmt.Fprintln(w, "</svg>")
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
