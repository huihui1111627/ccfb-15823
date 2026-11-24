package lb

import "time"

// Histogram 是对数分桶延迟直方图，用于估计分位数。
type Histogram struct {
	// Buckets[i] 统计 [Bounds[i], Bounds[i+1]) 的样本数，
	// 最后一个桶右边界为 +inf。
	Counts  []int64
	Bounds  []time.Duration
	Total   int64
	Sum     time.Duration
	Min     time.Duration
	Max     time.Duration
	hasData bool
}

// NewHistogram 构造从 min 到 max、按倍数 factor 增长的对数桶。
func NewHistogram(min, max time.Duration, factor float64) *Histogram {
	var bounds []time.Duration
	for v := min; v < max; v = time.Duration(float64(v) * factor) {
		bounds = append(bounds, v)
	}
	bounds = append(bounds, max)
	return &Histogram{
		Counts: make([]int64, len(bounds)),
		Bounds: bounds,
	}
}

func (h *Histogram) Observe(d time.Duration) {
	idx := 0
	for idx < len(h.Bounds)-1 && d >= h.Bounds[idx+1] {
		idx++
	}
	if idx >= len(h.Counts) {
		idx = len(h.Counts) - 1
	}
	h.Counts[idx]++
	h.Total++
	h.Sum += d
	if !h.hasData || d < h.Min {
		h.Min = d
	}
	if d > h.Max {
		h.Max = d
	}
	h.hasData = true
}

// Quantile 返回 p 分位数（0~1），无数据时返回 0。
func (h *Histogram) Quantile(p float64) time.Duration {
	if h.Total == 0 {
		return 0
	}
	target := int64(float64(h.Total)*p + 0.5)
	var acc int64
	for i, c := range h.Counts {
		acc += c
		if acc >= target {
			return h.Bounds[i]
		}
	}
	return h.Bounds[len(h.Bounds)-1]
}

func (h *Histogram) Mean() time.Duration {
	if h.Total == 0 {
		return 0
	}
	return h.Sum / time.Duration(h.Total)
}

// BackendStat 是页面上单个后端一行的数据。
type BackendStat struct {
	ID              string  `json:"id"`
	Weight          float64 `json:"weight"`
	EffectiveWeight float64 `json:"effective_weight"`
	WarmupFactor    float64 `json:"warmup_factor"`

	Picked    int64 `json:"picked"`
	InFlight  int64 `json:"inflight"`
	Completed int64 `json:"completed"`
	TimedOut  int64 `json:"timed_out"`
	Swept     int64 `json:"swept"` // 被兜底回收的在途数（理想情况下恒为 0）

	LatencyEstimateMS float64 `json:"latency_estimate_ms"`
	Utilization       float64 `json:"utilization"`
}

// Snapshot 是 Balancer 的全量指标快照。
type Snapshot struct {
	Strategy       string        `json:"strategy"`
	AvailableCount int           `json:"available_count"`
	TotalPicked    int64         `json:"total_picked"`
	TotalInFlight  int64         `json:"total_inflight"`
	TotalCompleted int64         `json:"total_completed"`
	TotalTimeout   int64         `json:"total_timeout"`
	TotalSwept     int64         `json:"total_swept"`
	QueuedNow      int           `json:"queued_now"`
	QueueTimeouts  int64         `json:"queue_timeouts"`
	QueueWaitMax   time.Duration `json:"queue_wait_max"`

	LatencyMS   LatencyReport `json:"latency_ms"`
	QueueWaitMS LatencyReport `json:"queue_wait_ms"`
	Backends    []BackendStat `json:"backends"`
}

// LatencyReport 是直方图导出的分位数报告（毫秒）。
type LatencyReport struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

func report(h *Histogram) LatencyReport { return ExportReport(h) }

// ExportReport 把直方图导出为分位数报告，供库外（模拟器）使用。
func ExportReport(h *Histogram) LatencyReport {
	ms := func(d time.Duration) float64 {
		return float64(d.Microseconds()) / 1000.0
	}
	return LatencyReport{
		P50:  ms(h.Quantile(0.50)),
		P90:  ms(h.Quantile(0.90)),
		P99:  ms(h.Quantile(0.99)),
		P999: ms(h.Quantile(0.999)),
		Max:  ms(h.Max),
		Mean: ms(h.Mean()),
	}
}
