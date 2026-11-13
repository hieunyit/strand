package main

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Sampler lấy mẫu /metrics của Strand mỗi giây. Mẫu chỉ hợp lệ (OK) khi HTTP 200 và đủ R, zone,
// pressure, breaker, lag hữu hạn; pressure = 0 là giá trị hợp lệ.
type Sampler struct {
	addr, pipeline, pressureKey string
	mu                          sync.Mutex
	s                           []Sample
}

// pressureKey: chỉ grace-v2 xuất strand_governor_pressure{signal="total"}; static/onoff chỉ xuất
// signal="replica_lag" (internal/pipeline/governor.go). Mỗi governor đọc đúng tín hiệu của nó, không fallback chéo.
func pressureKey(governor string) string {
	if governor == "grace-v2" {
		return `strand_governor_pressure|total`
	}
	return `strand_governor_pressure|replica_lag`
}

func NewSampler(addr, pipeline, governor string) *Sampler {
	return &Sampler{addr: addr, pipeline: pipeline, pressureKey: pressureKey(governor)}
}

func (s *Sampler) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	cl := &http.Client{Timeout: 900 * time.Millisecond}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			sm := Sample{T: now.UTC()}
			if resp, err := cl.Get("http://" + s.addr + "/metrics"); err == nil {
				m := parseMetrics(resp.Body, s.pipeline)
				resp.Body.Close()
				sm = sampleFrom(m, resp.StatusCode, sm.T, s.pressureKey)
			}
			s.mu.Lock()
			s.s = append(s.s, sm)
			s.mu.Unlock()
		}
	}
}

func sampleFrom(m map[string]float64, status int, t time.Time, pKey string) Sample {
	sm := Sample{T: t}
	var rateOK, zoneOK, pOK, brOK, lagOK bool
	sm.R, rateOK = m[`strand_governor_rate_tokens|source_iops`]
	sm.Zone, zoneOK = m["strand_governor_zone|"]
	sm.P, pOK = m[pKey]
	sm.Breaker, brOK = m["strand_breaker_open|"]
	sm.Lag, lagOK = m["strand_lag_seconds|"]
	sm.OK = status == http.StatusOK && rateOK && zoneOK && pOK && brOK && lagOK &&
		finite(sm.R) && sm.R >= 0 && finite(sm.Zone) && sm.Zone >= 0 && sm.Zone <= 2 &&
		finite(sm.P) && sm.P >= 0 && finite(sm.Breaker) && (sm.Breaker == 0 || sm.Breaker == 1) &&
		finite(sm.Lag) && sm.Lag >= 0
	return sm
}

func (s *Sampler) Snapshot() []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Sample(nil), s.s...)
}

// parseMetrics đọc exposition text; khóa = "<tên>|<giá trị nhãn>", chỉ series có pipeline = id.
func parseMetrics(r io.Reader, pipeline string) map[string]float64 {
	out := map[string]float64{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	want := map[string]string{"strand_governor_rate_tokens": "resource", "strand_governor_zone": "",
		"strand_governor_pressure": "signal", "strand_breaker_open": "", "strand_lag_seconds": ""}
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.IndexAny(line, "{ ")
		if i < 0 {
			continue
		}
		name := line[:i]
		lbl, ok := want[name]
		if !ok {
			continue
		}
		labels := map[string]string{}
		rest := line[i:]
		if strings.HasPrefix(rest, "{") {
			j := strings.Index(rest, "}")
			if j < 0 {
				continue
			}
			for _, kv := range strings.Split(rest[1:j], ",") {
				k, v, ok := strings.Cut(kv, "=")
				if ok {
					labels[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"`)
				}
			}
			rest = rest[j+1:]
		}
		if labels["pipeline"] != pipeline {
			continue
		}
		fs := strings.Fields(rest)
		if len(fs) == 0 {
			continue
		}
		f, err := strconv.ParseFloat(fs[0], 64)
		if err != nil {
			continue
		}
		key := name + "|"
		if lbl != "" {
			key += labels[lbl]
		}
		old, dup := out[key]
		if !dup || (name == "strand_lag_seconds" && f > old) { // lag: lớn nhất qua các phase
			out[key] = f
		}
	}
	return out
}
