package pipeline

import (
	"context"
	"testing"
	"time"

	"strand/internal/admin"
	"strand/internal/config"
	"strand/internal/governor"
	"strand/internal/model"
	"strand/internal/sched"
)

// review R17: `throttle set --soft` với governor static phải đổi nhịp snapshot thật (pace đọc soft
// limit hiện hành), và IOPS target (chưa thi hành) bị từ chối thay vì báo đã đặt.
func TestR17_ThrottleChangesStaticPacingAndRejectsUnenforcedLimits(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}, srcEngine: model.EngineMySQL}
	p.cfg.Resources.Source.IOPS.Soft = 600
	p.gov = governor.NewOnoffGovernor(governor.OnoffConfig{SoftRate: 600})
	p.govAlgo = "static"
	p.setSoftRate(600)
	p.running.Store(true) // admin chỉ nhận throttle khi pipeline đã chạy (review R12)
	h := &adminHandler{p: p}
	res, err := h.ThrottleSet(context.Background(), admin.Request{SourceIOPS: 123, Soft: true})
	if err != nil || res.SourceIOPSSoft != 123 {
		t.Fatalf("throttle: %+v %v", res, err)
	}
	if got := p.softRate(); got != 123 {
		t.Fatalf("static pacing rate after throttle = %v, want 123", got)
	}
	if _, rate := p.gov.CurrentRate(); rate != 123 {
		t.Fatalf("governor soft rate = %v, want 123", rate)
	}
	if _, err := h.ThrottleSet(context.Background(), admin.Request{TargetIOPS: 50, Soft: true}); err == nil {
		t.Fatal("target IOPS throttle acknowledged although it is not enforced")
	}
}

// review R18: `strand top` báo tốc độ theo cửa sổ, không phải tổng row từ lúc khởi động.
func TestR18_TopThroughputIsARate(t *testing.T) {
	p := &Pipeline{id: "r18", sched: sched.NewScheduler(sched.DefaultConfig())}
	h := &adminHandler{p: p}
	t0 := time.Unix(1000, 0)
	p.appliedRows.Store(1_000_000)
	p.throughput.sample(p.appliedRows.Load(), t0)
	if top, _ := h.Top(context.Background()); top.Throughput != 0 {
		t.Fatalf("one sample: throughput %v, want 0", top.Throughput)
	}
	p.throughput.sample(p.appliedRows.Load(), t0.Add(time.Second)) // idle
	if top, _ := h.Top(context.Background()); top.Throughput != 0 {
		t.Fatalf("idle pipeline with 1e6 historical rows reports %v rows/s", top.Throughput)
	}
	p.appliedRows.Add(500)
	p.throughput.sample(p.appliedRows.Load(), t0.Add(3*time.Second))
	if top, _ := h.Top(context.Background()); top.Throughput != 250 {
		t.Fatalf("500 rows in 2 s: throughput %v, want 250", top.Throughput)
	}
}
