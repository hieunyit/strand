package main

import (
	"math"
	"testing"
	"time"

	"strand/internal/checkpoint"
)

func TestComputeRSSGrowthMBPerHour(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

	t.Run("WarmupFirstHalfThenFlatSecondHalf", func(t *testing.T) {
		// Nửa đầu (0h..3h) tăng mạnh từ 100 MB lên 400 MB do khởi tạo cache/spool,
		// nửa sau (3h..6h) giữ phẳng ở 400 MB -> rss_growth trên nửa sau phải bằng 0 MB/giờ.
		var samples []Sample
		for h := 0; h <= 6; h++ {
			rss := 400.0
			if h < 3 {
				rss = 100.0 + float64(h)*100.0
			}
			samples = append(samples, Sample{
				Timestamp:      t0.Add(time.Duration(h) * time.Hour),
				ElapsedSeconds: float64(h * 3600),
				RSSMB:          rss,
			})
		}

		got := computeRSSGrowthMBPerHour(samples)
		if math.Abs(got-0.0) > 1e-6 {
			t.Fatalf("kỳ vọng rss_growth = 0 MB/giờ trên nửa sau, nhận được %.6f", got)
		}
	})

	t.Run("LinearGrowthInSecondHalf", func(t *testing.T) {
		// 8 mẫu từ giờ 0 đến giờ 7; nửa sau (giờ 4..7) tăng đều 3.5 MB/giờ.
		var samples []Sample
		for h := 0; h < 8; h++ {
			var rss float64
			if h < 4 {
				rss = 200.0
			} else {
				rss = 200.0 + float64(h-4)*3.5
			}
			samples = append(samples, Sample{
				Timestamp: t0.Add(time.Duration(h) * time.Hour),
				RSSBytes:  int64(rss * 1024 * 1024),
			})
		}

		got := computeRSSGrowthMBPerHour(samples)
		if math.Abs(got-3.5) > 1e-4 {
			t.Fatalf("kỳ vọng rss_growth = 3.5 MB/giờ, nhận được %.6f", got)
		}
	})

	t.Run("TooFewSamples", func(t *testing.T) {
		if got := computeRSSGrowthMBPerHour(nil); got != 0 {
			t.Fatalf("kỳ vọng 0 khi không có mẫu, nhận được %v", got)
		}
		if got := computeRSSGrowthMBPerHour([]Sample{{RSSMB: 120}}); got != 0 {
			t.Fatalf("kỳ vọng 0 khi chỉ có 1 mẫu, nhận được %v", got)
		}
	})
}

func TestEvaluatePassAndBuildConclusion(t *testing.T) {
	t.Run("PassWhenAllCriteriaMet", func(t *testing.T) {
		ok := evaluatePass(PassInput{
			FinalStateEqual:       true,
			ForbiddenExit:         false,
			CheckpointMaxBytes:    65536,
			RSSGrowthMBPerHour:    5.0,
			MaxCheckpointBytes:    DefaultMaxCheckpointBytes,
			MaxRSSGrowthMBPerHour: DefaultMaxRSSGrowthMBPerHour,
		})
		if !ok {
			t.Fatal("kỳ vọng PASS khi chạm đúng ngưỡng biên cho phép (65536 B và 5.0 MB/h)")
		}
	})

	t.Run("FailWhenFinalStateMismatch", func(t *testing.T) {
		ok := evaluatePass(PassInput{
			FinalStateEqual:       false,
			ForbiddenExit:         false,
			CheckpointMaxBytes:    1024,
			RSSGrowthMBPerHour:    0.1,
			MaxCheckpointBytes:    DefaultMaxCheckpointBytes,
			MaxRSSGrowthMBPerHour: DefaultMaxRSSGrowthMBPerHour,
		})
		if ok {
			t.Fatal("kỳ vọng FAIL khi final_state_equal = false")
		}
	})

	t.Run("FailWhenForbiddenExit", func(t *testing.T) {
		ok := evaluatePass(PassInput{
			FinalStateEqual:       true,
			ForbiddenExit:         true,
			CheckpointMaxBytes:    1024,
			RSSGrowthMBPerHour:    0.1,
			MaxCheckpointBytes:    DefaultMaxCheckpointBytes,
			MaxRSSGrowthMBPerHour: DefaultMaxRSSGrowthMBPerHour,
		})
		if ok {
			t.Fatal("kỳ vọng FAIL khi gặp mã cấm restart")
		}
	})

	t.Run("FailWhenCheckpointExceedsLimit", func(t *testing.T) {
		ok := evaluatePass(PassInput{
			FinalStateEqual:       true,
			ForbiddenExit:         false,
			CheckpointMaxBytes:    65537,
			RSSGrowthMBPerHour:    0.5,
			MaxCheckpointBytes:    DefaultMaxCheckpointBytes,
			MaxRSSGrowthMBPerHour: DefaultMaxRSSGrowthMBPerHour,
		})
		if ok {
			t.Fatal("kỳ vọng FAIL khi checkpoint_max_bytes > 65536")
		}
	})

	t.Run("FailWhenRSSGrowthExceedsLimit", func(t *testing.T) {
		ok := evaluatePass(PassInput{
			FinalStateEqual:       true,
			ForbiddenExit:         false,
			CheckpointMaxBytes:    2048,
			RSSGrowthMBPerHour:    5.01,
			MaxCheckpointBytes:    DefaultMaxCheckpointBytes,
			MaxRSSGrowthMBPerHour: DefaultMaxRSSGrowthMBPerHour,
		})
		if ok {
			t.Fatal("kỳ vọng FAIL khi rss_growth > 5.0 MB/giờ")
		}
	})

	t.Run("BuildConclusionIntegration", func(t *testing.T) {
		t0 := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
		samples := []Sample{
			{Timestamp: t0, RSSMB: 100, CheckpointBytes: 512},
			{Timestamp: t0.Add(1 * time.Hour), RSSMB: 150, CheckpointBytes: 4096},
			{Timestamp: t0.Add(2 * time.Hour), RSSMB: 152, CheckpointBytes: 2048},
			{Timestamp: t0.Add(3 * time.Hour), RSSMB: 154, CheckpointBytes: 1024},
		}
		c := buildConclusion(samples, true, false, 0, DefaultMaxCheckpointBytes, DefaultMaxRSSGrowthMBPerHour)
		if !c.Pass {
			t.Fatalf("kỳ vọng Conclusion.Pass = true, nhận được %+v", c)
		}
		if c.CheckpointMaxBytes != 4096 {
			t.Fatalf("kỳ vọng CheckpointMaxBytes = 4096, nhận được %d", c.CheckpointMaxBytes)
		}
		if math.Abs(c.RSSGrowth-2.0) > 1e-4 {
			t.Fatalf("kỳ vọng RSSGrowth = 2.0 MB/giờ, nhận được %.6f", c.RSSGrowth)
		}
	})
}

func TestIsForbiddenExitCode(t *testing.T) {
	for _, code := range []int{2, 3, 4, 5, 10} {
		if !isForbiddenExitCode(code) {
			t.Fatalf("mã %d phải là mã cấm restart (03 §4)", code)
		}
	}
	for _, code := range []int{-1, 0, 1, 9, 15, 137, 143} {
		if isForbiddenExitCode(code) {
			t.Fatalf("mã %d không phải là mã cấm restart", code)
		}
	}
}

func TestParseVmRSSBytesAndCountGTIDSet(t *testing.T) {
	status := "Name:\tstrand\nState:\tS (sleeping)\nVmSize:\t  512000 kB\nVmRSS:\t   65536 kB\nThreads:\t12\n"
	rss, err := parseVmRSSBytes(status)
	if err != nil {
		t.Fatalf("parseVmRSSBytes lỗi: %v", err)
	}
	if rss != 65536*1024 {
		t.Fatalf("kỳ vọng %d byte, nhận được %d", 65536*1024, rss)
	}

	set, err := checkpoint.ParseMySQLGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10:15:20-25")
	if err != nil {
		t.Fatalf("ParseMySQLGTIDSet lỗi: %v", err)
	}
	if n := countGTIDSet(set); n != 17 {
		t.Fatalf("kỳ vọng 17 GTID, nhận được %d", n)
	}
}
