package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

const usage = `g2bench - Gate 2 governor benchmark (G2-12), external clients (sysbench / pgbench)

  g2bench fixture-create [flags]   dựng dữ liệu nguồn + đích rỗng, chụp volume hai container (restore point bất biến)
  g2bench run            [flags]   khôi phục fixture, tải sóng vuông + spike, snapshot, đo, tổng hợp (3 governor x 3 lần)
  g2bench analyze        --out D   tính lại results.json + biểu đồ từ dữ liệu thô trong D
  g2bench overhead       [flags]   so overhead sysbench có/không instrumentation (pilot)
  g2bench calibrate      [flags]   đo năng lực bão hoà của client trên fixture (chốt biên độ trước khi đo)
`

func commonFlags(fs *flag.FlagSet, o *Opts) {
	fs.StringVar(&o.Engine, "engine", "mysql", "mysql | postgres")
	fs.StringVar(&o.SourceDSN, "source", "", "DSN source (mysql: user:pw@tcp(h:p)/ không kèm db; postgres: postgres://...)")
	fs.StringVar(&o.TargetDSN, "target", "", "DSN target (cùng dạng)")
	fs.StringVar(&o.SourceContainer, "source-container", "", "container docker của source (volume dữ liệu được chụp/khôi phục)")
	fs.StringVar(&o.TargetContainer, "target-container", "", "container docker của target")
	fs.StringVar(&o.HelperImage, "helper-image", "", "image có sh/tar/find/sha256sum cho chụp/khôi phục volume")
	fs.StringVar(&o.FixtureDir, "fixture-dir", "", "thư mục fixture (fixture.json + source.tar + target.tar)")
	fs.StringVar(&o.ClientContainer, "client-container", "", "container chạy client (docker exec); rỗng = chạy trực tiếp")
	fs.StringVar(&o.ClientBinary, "client-binary", "", "đường dẫn sysbench (bản patch) / pgbench trong container client")
	fs.StringVar(&o.ClientSHA, "client-sha256", "", "sha256 của binary client (bắt buộc cho official)")
	fs.IntVar(&o.BigRows, "big-rows", 2_000_000, "số row bảng snapshot bench.big")
	fs.IntVar(&o.AppRows, "app-rows", 100_000, "sysbench: số row mỗi bảng ứng dụng")
	fs.IntVar(&o.AppTables, "app-tables", 1, "sysbench: số bảng sbtest")
	fs.IntVar(&o.PGScale, "pg-scale", 1, "pgbench: scale factor (accounts = 100000 x scale)")
	fs.Int64Var(&o.Seed, "seed", 20261113, "seed dữ liệu và thứ tự chạy")
	fs.IntVar(&o.Clients, "clients", 16, "số client/thread của tải ứng dụng")
	fs.IntVar(&o.Jobs, "jobs", 8, "pgbench -j")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var o Opts
	ctx := context.Background()
	var err error
	switch cmd {
	case "fixture-create":
		commonFlags(fs, &o)
		fs.Parse(args)
		err = FixtureCreate(ctx, o)
	case "run":
		commonFlags(fs, &o)
		runFlags(fs, &o)
		fs.Parse(args)
		err = validateRun(&o)
		if err == nil {
			err = Benchmark(ctx, o)
		}
	case "analyze":
		fs.StringVar(&o.Out, "out", "", "thư mục kết quả")
		fs.Parse(args)
		err = AnalyzeAll(o)
	case "overhead":
		err = overheadCmd(ctx, fs, args)
	case "calibrate":
		err = calibrateCmd(ctx, fs, args)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func runFlags(fs *flag.FlagSet, o *Opts) {
	fs.StringVar(&o.Strand, "strand", "./strand", "binary strand")
	fs.StringVar(&o.Governors, "governors", "static,onoff,grace-v2", "danh sách governor")
	fs.IntVar(&o.Repeats, "repeats", 3, "số lần mỗi governor")
	fs.Float64Var(&o.Wave.Low, "low", 50, "tải mức thấp (txn/s)")
	fs.Float64Var(&o.Wave.High, "high", 200, "tải mức cao (txn/s)")
	fs.DurationVar(&o.Wave.Period, "period", 10*time.Minute, "chu kỳ sóng vuông")
	fs.DurationVar(&o.Wave.SpikeEvery, "spike-every", 3*time.Minute, "khoảng cách spike")
	fs.DurationVar(&o.Wave.SpikeDur, "spike-dur", 10*time.Second, "độ dài spike")
	fs.Float64Var(&o.Wave.SpikeMult, "spike-mult", 2, "spike = mult x mức cao")
	fs.DurationVar(&o.Warmup, "warmup", 120*time.Second, "tải chạy trước khi khởi động strand")
	fs.DurationVar(&o.StreamAfter, "stream-after", 120*time.Second, "stream thêm sau snapshot")
	fs.DurationVar(&o.CatchupTimeout, "catchup-timeout", 10*time.Minute, "chờ target bắt kịp")
	fs.DurationVar(&o.SnapshotTimeout, "snapshot-timeout", 3*time.Hour, "giới hạn thời gian snapshot")
	fs.DurationVar(&o.MaxChunk, "max-chunk", 30*time.Second, "độ dài tối đa một tiến trình client (dừng tải chờ tối đa chừng này)")
	fs.IntVar(&o.SoftIOPS, "soft-iops", 400, "resources.source.iops.soft")
	fs.IntVar(&o.HardIOPS, "hard-iops", 600, "resources.source.iops.hard")
	fs.StringVar(&o.LagSLO, "lag-slo", "5s", "governor.lag_slo")
	fs.StringVar(&o.MetricsAddr, "metrics-addr", "127.0.0.1:19190", "địa chỉ metrics của strand")
	fs.StringVar(&o.Out, "out", "g2bench-out", "thư mục kết quả")
	fs.StringVar(&o.Only, "only", "", "smoke: chạy một lần với governor này, không quyết định")
	fs.BoolVar(&o.Official, "official", false, "đánh dấu OFFICIAL: từ chối chạy nếu thiếu điều kiện giao thức")
	fs.BoolVar(&o.RelaxBreaker, "smoke-relax-breaker", false, "chỉ smoke dev: nới breaker strand_query_p99 (không dùng cho official)")
	fs.BoolVar(&o.Attest, "attest-dedicated", false, "xác nhận máy không chạy việc khác trong lúc đo")
	fs.BoolVar(&o.RerunFailed, "rerun-failed", true, "chạy bù một lần ở cuối cho lần chạy lỗi (giao thức §4.5)")
}

func validateRun(o *Opts) error {
	if o.SourceDSN == "" || o.TargetDSN == "" || o.Strand == "" || o.ClientBinary == "" {
		return fmt.Errorf("--source, --target, --strand, --client-binary are required")
	}
	if o.Engine != "mysql" && o.Engine != "postgres" {
		return fmt.Errorf("--engine must be mysql or postgres")
	}
	if o.Wave.Period <= 0 || o.Wave.Low <= 0 || o.Wave.High <= 0 || o.Clients < 1 || o.MaxChunk < time.Second {
		return fmt.Errorf("invalid wave/clients/max-chunk")
	}
	if o.Out != "" {
		abs, err := absPath(o.Out)
		if err != nil {
			return err
		}
		o.Out = abs
	}
	return nil
}
