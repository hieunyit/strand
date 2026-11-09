package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Metrics chứa đầy đủ 20 metric theo đúng đặc tả tại spec/02 §11.
type Metrics struct {
	Registry *prometheus.Registry

	// 1. strand_lag_seconds: gauge, label [pipeline, phase] (bootstrap|synced)
	LagSeconds *prometheus.GaugeVec

	// 2. strand_apply_txn_total: counter, label [pipeline]
	ApplyTxnTotal *prometheus.CounterVec

	// 3. strand_apply_rows_total: counter, label [pipeline]
	ApplyRowsTotal *prometheus.CounterVec

	// 4. strand_holes: gauge, label [pipeline]
	Holes *prometheus.GaugeVec

	// 5. strand_checkpoint_compaction_lag_seconds: gauge, label [pipeline]
	CheckpointCompactionLagSeconds *prometheus.GaugeVec

	// 6. strand_writeset_entries: gauge, label [pipeline, table]
	WritesetEntries *prometheus.GaugeVec

	// 7. strand_pending_cross_range: gauge, label [pipeline]
	PendingCrossRange *prometheus.GaugeVec

	// 8. strand_pending_cross_range_oldest_seconds: gauge, label [pipeline]
	PendingCrossRangeOldestSeconds *prometheus.GaugeVec

	// 9. strand_governor_rate_tokens: gauge, label [pipeline, resource]
	GovernorRateTokens *prometheus.GaugeVec

	// 10. strand_governor_zone: gauge, label [pipeline] (0 Green, 1 Yellow, 2 Red)
	GovernorZone *prometheus.GaugeVec

	// 11. strand_governor_pressure: gauge, label [pipeline, signal]
	GovernorPressure *prometheus.GaugeVec

	// 12. strand_sensitivity_unknown: gauge (0/1), label [pipeline]
	SensitivityUnknown *prometheus.GaugeVec

	// 13. strand_breaker_open: gauge (0/1), label [pipeline]
	BreakerOpen *prometheus.GaugeVec

	// 14. strand_spool_bytes: gauge, label [pipeline, tier] (local|bucket)
	SpoolBytes *prometheus.GaugeVec

	// 15. strand_snapshot_ranges: gauge, label [pipeline, table, state]
	SnapshotRanges *prometheus.GaugeVec

	// 16. strand_seed_progress_ratio: gauge, label [pipeline, table]
	SeedProgressRatio *prometheus.GaugeVec

	// 17. strand_retention_headroom_seconds: gauge, label [pipeline]
	RetentionHeadroomSeconds *prometheus.GaugeVec

	// 18. strand_verify_mismatch_total: counter, label [pipeline, table]
	VerifyMismatchTotal *prometheus.CounterVec

	// 19. strand_verify_unverifiable_hot_total: counter, label [pipeline, table]
	VerifyUnverifiableHotTotal *prometheus.CounterVec

	// 20. strand_fenced_total: counter, label [pipeline]
	FencedTotal *prometheus.CounterVec

	// Bổ sung 02 §12: strand_spool_capacity_bytes{tier} (cho alert StrandSpoolNearFull).
	SpoolCapacityBytes *prometheus.GaugeVec

	// F11: strand_rows_not_written_total{pipeline}: batch có row đã định tuyến mà writer không ghi
	// (bộ phát hiện trước commit); pipeline dừng mỗi lần đếm tăng.
	RowsNotWrittenTotal *prometheus.CounterVec
	// VerifyIncompleteTotal: verify sau seed không đủ bằng chứng (tràn giới hạn theo dõi khóa đổi).
	VerifyIncompleteTotal *prometheus.CounterVec
}

// NewMetrics khởi tạo và đăng ký đầy đủ 20 metric theo spec/02 §11.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	m := &Metrics{
		Registry: reg,

		LagSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_lag_seconds",
			Help: "Độ trễ đồng bộ tính bằng giây theo từng giai đoạn (phase).",
		}, []string{"pipeline", "phase"}),

		ApplyTxnTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "strand_apply_txn_total",
			Help: "Tổng số transaction đã apply vào cơ sở dữ liệu đích.",
		}, []string{"pipeline"}),

		ApplyRowsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "strand_apply_rows_total",
			Help: "Tổng số row đã apply vào cơ sở dữ liệu đích.",
		}, []string{"pipeline"}),

		Holes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_holes",
			Help: "Số lượng lỗ hổng (holes) chưa được lấp trong scheduler.",
		}, []string{"pipeline"}),

		CheckpointCompactionLagSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_checkpoint_compaction_lag_seconds",
			Help: "Độ trễ compaction checkpoint tính bằng giây.",
		}, []string{"pipeline"}),

		WritesetEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_writeset_entries",
			Help: "Số phần tử trong writeset theo từng bảng.",
		}, []string{"pipeline", "table"}),

		PendingCrossRange: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_pending_cross_range",
			Help: "Số thao tác đổi PK xuyên range đang chờ xử lý.",
		}, []string{"pipeline"}),

		PendingCrossRangeOldestSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_pending_cross_range_oldest_seconds",
			Help: "Tuổi của thao tác chờ đổi PK xuyên range lâu nhất tính bằng giây.",
		}, []string{"pipeline"}),

		GovernorRateTokens: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_governor_rate_tokens",
			Help: "Tốc độ cấp phát token hiện tại của governor theo từng tài nguyên.",
		}, []string{"pipeline", "resource"}),

		GovernorZone: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_governor_zone",
			Help: "Vùng hoạt động của governor (0 Green, 1 Yellow, 2 Red).",
		}, []string{"pipeline"}),

		GovernorPressure: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_governor_pressure",
			Help: "Áp suất đo được từ các nguồn tín hiệu.",
		}, []string{"pipeline", "signal"}),

		SensitivityUnknown: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_sensitivity_unknown",
			Help: "Trạng thái độ nhạy hệ thống chưa xác định (0 hoặc 1).",
		}, []string{"pipeline"}),

		BreakerOpen: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_breaker_open",
			Help: "Trạng thái Circuit Breaker đang mở ngắt mạch (0 hoặc 1).",
		}, []string{"pipeline"}),

		SpoolBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_spool_bytes",
			Help: "Dung lượng spool đang sử dụng tính bằng byte theo từng tầng (local, bucket).",
		}, []string{"pipeline", "tier"}),

		SnapshotRanges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_snapshot_ranges",
			Help: "Số lượng range snapshot theo từng bảng và trạng thái.",
		}, []string{"pipeline", "table", "state"}),

		SeedProgressRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_seed_progress_ratio",
			Help: "Tiến độ seed theo từng bảng (0.0 đến 1.0).",
		}, []string{"pipeline", "table"}),

		RetentionHeadroomSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_retention_headroom_seconds",
			Help: "Khoảng an toàn retention log của source trước khi bị purge tính bằng giây.",
		}, []string{"pipeline"}),

		VerifyMismatchTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "strand_verify_mismatch_total",
			Help: "Tổng số row hoặc range phát hiện lệch trong quá trình verify.",
		}, []string{"pipeline", "table"}),

		VerifyUnverifiableHotTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "strand_verify_unverifiable_hot_total",
			Help: "Tổng số row quá nóng không thể kiểm chứng trong verify.",
		}, []string{"pipeline", "table"}),

		FencedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "strand_fenced_total",
			Help: "Tổng số lần pipeline bị fenced do instance khác nhận quyền.",
		}, []string{"pipeline"}),
		RowsNotWrittenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "strand_rows_not_written_total",
			Help: "Số batch bị từ chối trước commit vì có row đã định tuyến mà writer không ghi (pipeline dừng).",
		}, []string{"pipeline"}),
		VerifyIncompleteTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "strand_verify_incomplete_total",
			Help: "Post-seed verify runs that could not cover every changed key (tracking limit or key encoding failure)",
		}, []string{"pipeline"}),
		SpoolCapacityBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_spool_capacity_bytes",
			Help: "Dung lượng tối đa (hard limit) của spool theo tầng.",
		}, []string{"pipeline", "tier"}),
	}

	reg.MustRegister(
		m.LagSeconds,
		m.ApplyTxnTotal,
		m.ApplyRowsTotal,
		m.Holes,
		m.CheckpointCompactionLagSeconds,
		m.WritesetEntries,
		m.PendingCrossRange,
		m.PendingCrossRangeOldestSeconds,
		m.GovernorRateTokens,
		m.GovernorZone,
		m.GovernorPressure,
		m.SensitivityUnknown,
		m.BreakerOpen,
		m.SpoolBytes,
		m.SnapshotRanges,
		m.SeedProgressRatio,
		m.RetentionHeadroomSeconds,
		m.VerifyMismatchTotal,
		m.VerifyUnverifiableHotTotal,
		m.FencedTotal,
		m.SpoolCapacityBytes,
		m.RowsNotWrittenTotal,
		m.VerifyIncompleteTotal,
	)

	return m
}

// Handler trả về http.Handler phục vụ endpoint /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
