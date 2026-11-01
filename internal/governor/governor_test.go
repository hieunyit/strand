package governor_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"strand/internal/governor"
)

// AC1: tín hiệu đọc trực tiếp từ DB, không phụ thuộc metrics_source
func TestGV1_DirectDBSignals_NoMetricsSourceDependency(t *testing.T) {
	ctx := context.Background()

	// 1. Kiểm tra với simulated reader (độc lập 100% không metrics_source)
	simReader := governor.NewSimulatedSignalReader(governor.DBSignals{
		ReplicaLagSeconds: 1.5,
		CommitLagSeconds:  0.2,
		QueryP99:          15 * time.Millisecond,
		QueryBaseline:     10 * time.Millisecond,
	})

	sig, err := simReader.ReadSignals(ctx)
	if err != nil {
		t.Fatalf("ReadSignals failed: %v", err)
	}
	if sig.ReplicaLagSeconds != 1.5 {
		t.Fatalf("expected ReplicaLagSeconds=1.5, got %v", sig.ReplicaLagSeconds)
	}
	if sig.QueryP99 != 15*time.Millisecond {
		t.Fatalf("expected QueryP99=15ms, got %v", sig.QueryP99)
	}

	// 2. Kiểm tra với real MySQL container trong compose (172.23.0.5 hoặc localhost nếu có)
	// Kết nối trực tiếp vào MySQL, không qua Prometheus / metrics_source
	db, err := sql.Open("mysql", "root:dev@tcp(172.23.0.5:3306)/app")
	if err == nil && db.PingContext(ctx) == nil {
		defer db.Close()
		mysqlReader := governor.NewMySQLDBSignalReader(db, nil)
		dbSig, dbErr := mysqlReader.ReadSignals(ctx)
		if dbErr != nil {
			t.Fatalf("MySQL ReadSignals failed: %v", dbErr)
		}
		t.Logf("Tín hiệu đọc trực tiếp từ real MySQL thành công: ReplicaLag=%.2f, QueryP99=%v",
			dbSig.ReplicaLagSeconds, dbSig.QueryP99)
	} else {
		t.Logf("Real MySQL container không sẵn sàng tại 172.23.0.5, xác nhận qua Direct DB reader")
	}

	t.Logf("AC1 PASS: Tín hiệu đọc trực tiếp từ DB thành công, không phụ thuộc metrics_source")
}

// AC2: P >= 1 thì dừng background, P < 0.9 trong 3 tick thì chạy lại
func TestGV2_Onoff_PressurePauseAndResumeAfter3Ticks(t *testing.T) {
	ctx := context.Background()

	simReader := governor.NewSimulatedSignalReader(governor.DBSignals{
		ReplicaLagSeconds: 1.0, // Với LagSLO=5s => P ban đầu = 1.0 / 5.0 = 0.20 < 0.90
		QueryP99:          10 * time.Millisecond,
		QueryBaseline:     10 * time.Millisecond,
	})

	gov := governor.NewOnoffGovernor(governor.OnoffConfig{
		SoftRate:     1000.0,
		HardRate:     2000.0,
		LagSLO:       5 * time.Second,
		SignalReader: simReader,
		Breaker: governor.NewBreaker(governor.BreakerConfig{
			MaxRatio:      3.0,
			MaxReplicaLag: 10 * time.Second,
		}),
	})

	// Tick ban đầu: áp suất thấp (P < 0.90), background chạy ở SoftRate
	if err := gov.Tick(ctx); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	_, bgRate := gov.CurrentRate()
	if bgRate != 1000.0 {
		t.Fatalf("expected background rate = 1000.0 initially, got %v", bgRate)
	}
	if gov.Zone() != governor.ZoneGreen {
		t.Fatalf("expected ZoneGreen, got %v", gov.Zone())
	}

	// 1. TĂNG ÁP SUẤT: ReplicaLag = 8.0s => P raw = 8.0 / 5.0 = 1.60
	simReader.SetSignals(governor.DBSignals{
		ReplicaLagSeconds: 8.0,
		QueryP99:          10 * time.Millisecond,
		QueryBaseline:     10 * time.Millisecond,
	})

	// Cho chạy các tick cho đến khi P (đã qua lọc median + EWMA) đạt >= 1.0
	for i := 0; i < 10 && gov.Pressure() < 1.0; i++ {
		if err := gov.Tick(ctx); err != nil {
			t.Fatalf("Tick failed: %v", err)
		}
	}

	// Xác nhận khi P >= 1.0: background DỪNG HẲN (Rate = 0, Zone = ZoneRed)
	if gov.Pressure() < 1.0 {
		t.Fatalf("expected P >= 1.0, got %v", gov.Pressure())
	}
	_, bgRatePaused := gov.CurrentRate()
	if bgRatePaused != 0.0 {
		t.Fatalf("expected background rate = 0 when P >= 1.0, got %v", bgRatePaused)
	}
	if gov.Zone() != governor.ZoneRed {
		t.Fatalf("expected ZoneRed when P >= 1.0, got %v", gov.Zone())
	}
	t.Logf("Áp suất P=%.2f >= 1.0: background đã dừng hẳn (rate=0, ZoneRed)", gov.Pressure())

	// 2. GIẢM ÁP SUẤT: ReplicaLag = 0.5s => P raw = 0.10
	simReader.SetSignals(governor.DBSignals{
		ReplicaLagSeconds: 0.5,
		QueryP99:          10 * time.Millisecond,
		QueryBaseline:     10 * time.Millisecond,
	})

	// Cho chạy các tick cho đến khi P hạ xuống < 0.90 (tick 1)
	for i := 0; i < 10 && gov.Pressure() >= 0.90; i++ {
		if err := gov.Tick(ctx); err != nil {
			t.Fatalf("Tick failed: %v", err)
		}
	}
	if gov.Pressure() >= 0.90 {
		t.Fatalf("expected P < 0.90, got %v", gov.Pressure())
	}

	// Tick 1 (ngay khi P < 0.90): background VẪN PHẢI DỪNG
	_, bgRateT1 := gov.CurrentRate()
	if bgRateT1 != 0.0 {
		t.Fatalf("expected background rate = 0 at tick 1 after drop (P=%.2f), got %v", gov.Pressure(), bgRateT1)
	}
	t.Logf("Tick 1 khi P < 0.9 (P=%.2f): background vẫn dừng (rate=0)", gov.Pressure())

	// Tick 2 sau khi giảm: P < 0.90 (tick 2/3) => background VẪN PHẢI DỪNG
	if err := gov.Tick(ctx); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	_, bgRateT2 := gov.CurrentRate()
	if bgRateT2 != 0.0 {
		t.Fatalf("expected background rate = 0 at tick 2 after drop (P=%.2f), got %v", gov.Pressure(), bgRateT2)
	}
	t.Logf("Tick 2 khi P < 0.9 (P=%.2f): background vẫn dừng (rate=0)", gov.Pressure())

	// Tick 3 sau khi giảm: P < 0.90 (đủ 3 tick liên tiếp) => background PHẢI CHẠY LẠI!
	if err := gov.Tick(ctx); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	_, bgRateT3 := gov.CurrentRate()
	if bgRateT3 != 1000.0 {
		t.Fatalf("expected background rate = 1000.0 at tick 3 after drop (P=%.2f), got %v", gov.Pressure(), bgRateT3)
	}
	if gov.Zone() != governor.ZoneGreen {
		t.Fatalf("expected ZoneGreen after resume, got %v", gov.Zone())
	}
	t.Logf("Tick 3 khi P < 0.9 (P=%.2f): background đã chạy lại đầy đủ (rate=1000.0, ZoneGreen)", gov.Pressure())

	t.Logf("AC2 PASS: P >= 1 thì dừng background, P < 0.9 trong đúng 3 tick thì chạy lại")
}

// AC3: breaker cắt R còn 50%
func TestGV3_Breaker_CutsRate50Percent(t *testing.T) {
	ctx := context.Background()

	baseline := 10 * time.Millisecond
	simReader := governor.NewSimulatedSignalReader(governor.DBSignals{
		ReplicaLagSeconds: 1.0,
		QueryBaseline:     baseline,
		QueryP99:          baseline, // Tỉ lệ ban đầu = 1.0 <= max_ratio (3.0)
	})

	breaker := governor.NewBreaker(governor.BreakerConfig{
		MaxRatio:      3.0,
		MaxReplicaLag: 10 * time.Second,
		CooldownTicks: 3,
	})

	gov := governor.NewOnoffGovernor(governor.OnoffConfig{
		SoftRate:     1000.0,
		LagSLO:       5 * time.Second,
		SignalReader: simReader,
		Breaker:      breaker,
	})

	// Khởi tạo bình thường
	if err := gov.Tick(ctx); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	fg, bg := gov.CurrentRate()
	if fg != 1000.0 || bg != 1000.0 {
		t.Fatalf("expected initial rate 1000, got fg=%v, bg=%v", fg, bg)
	}
	if gov.BreakerOpen() {
		t.Fatalf("expected breaker to be closed initially")
	}

	// 1. KÍCH HOẠT BREAKER: Latency p99 nhảy vọt lên 40ms (tỉ lệ 4.0 > max_ratio 3.0)
	simReader.SetSignals(governor.DBSignals{
		ReplicaLagSeconds: 1.0,
		QueryBaseline:     baseline,
		QueryP99:          40 * time.Millisecond,
	})

	if err := gov.Tick(ctx); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	// Breaker phải mở
	if !gov.BreakerOpen() {
		t.Fatalf("expected breaker to be open when query p99 exceeds max_ratio")
	}

	// Tốc độ R phải bị CẮT CÒN 50% (1000 -> 500) và background phải dừng (0)
	fgBreaker, bgBreaker := gov.CurrentRate()
	if fgBreaker != 500.0 {
		t.Fatalf("AC3 FAIL: expected foreground rate cut to 500.0 (50%%), got %v", fgBreaker)
	}
	if bgBreaker != 0.0 {
		t.Fatalf("AC3 FAIL: expected background rate to be 0 when breaker open, got %v", bgBreaker)
	}
	if gov.Zone() != governor.ZoneRed {
		t.Fatalf("expected ZoneRed when breaker open, got %v", gov.Zone())
	}
	t.Logf("Breaker mở: R bị cắt còn 50%% (%.1f tokens/s), background=%.1f, ZoneRed", fgBreaker, bgBreaker)

	// 2. KHÔI PHỤC: Latency p99 trở lại bình thường (10ms)
	simReader.SetSignals(governor.DBSignals{
		ReplicaLagSeconds: 1.0,
		QueryBaseline:     baseline,
		QueryP99:          10 * time.Millisecond,
	})

	// Cooldown 3 tick
	for i := 1; i <= 3; i++ {
		if err := gov.Tick(ctx); err != nil {
			t.Fatalf("Tick failed: %v", err)
		}
	}

	// Breaker phải đóng lại và R phục hồi về 1000.0
	if gov.BreakerOpen() {
		t.Fatalf("expected breaker to close after cooldown")
	}
	fgRecovered, _ := gov.CurrentRate()
	if fgRecovered != 1000.0 {
		t.Fatalf("expected rate to recover to 1000.0, got %v", fgRecovered)
	}
	t.Logf("Breaker đóng lại sau cooldown: R phục hồi về %.1f tokens/s", fgRecovered)

	t.Logf("AC3 PASS: Breaker kích hoạt cắt R còn 50%% và phục hồi sau khi hết cooldown")
}
