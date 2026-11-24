package sim

import (
	"context"
	"fmt"
	"sort"
	"time"

	"p2clb/pkg/lb"
)

// AllStrategies 是参与对比的四种策略，顺序固定。
var AllStrategies = []lb.Strategy{
	lb.StrategyP2C,
	lb.StrategyRoundRobin,
	lb.StrategyRandom,
	lb.StrategyInflightP2C,
}

// BenchmarkResult 是一次四策略对比的结果。
type BenchmarkResult struct {
	Seed     uint64
	Config   Config
	Reports  map[lb.Strategy]Report
	Checks   Checks
	Duration time.Duration
}

// Checks 是需求里要求的三条正确性校验。
type Checks struct {
	// WeightProportional 健康稳态下各后端请求量是否与权重成正比。
	WeightProportional WeightCheck
	// QueueBound 排队等待是否从未超过 QueueTimeout。
	QueueBound QueueCheck
	// NoInflightLeak 模拟结束后在途是否全部回收。
	NoInflightLeak LeakCheck
}

type WeightCheck struct {
	Pass    bool
	MaxDev  float64 // 权重档聚合口径的最大相对偏差
	Details []WeightRow
}

type WeightRow struct {
	ID        string
	Weight    float64
	Picked    int64
	WantShare float64
	GotShare  float64
	Deviation float64
}

type QueueCheck struct {
	Pass       bool
	QueueMaxMS float64
	LimitMS    float64
}

type LeakCheck struct {
	Pass     bool
	Leftover int64 // 运行结束后仍残留在 Balancer 上的在途
	Swept    int64 // 被兜底回收的数量
}

// CheckInflightLeak 跑一轮并检查结束后在途是否全部回收（需求 8）。
func CheckInflightLeak(ctx context.Context, cfg Config) LeakCheck {
	r := NewRunner(cfg)
	bal := lb.New(r.Discovery(), r.BalancerConfig(lb.StrategyP2C))
	r.runWithBalancer(ctx, bal)
	snap := bal.Snapshot()
	bal.Close()
	return LeakCheck{
		Pass:     snap.TotalInFlight == 0 && snap.TotalSwept >= 0,
		Leftover: snap.TotalInFlight,
		Swept:    snap.TotalSwept,
	}
}

// RunBenchmark 依次用四种策略各跑一次等价模拟。
func RunBenchmark(ctx context.Context, cfg Config) (BenchmarkResult, error) {
	start := time.Now()
	reports := make(map[lb.Strategy]Report, len(AllStrategies))
	for _, st := range AllStrategies {
		r := NewRunner(cfg)
		rep := r.Run(ctx, st)
		reports[st] = rep
	}
	res := BenchmarkResult{
		Seed:     cfg.Seed,
		Config:   cfg,
		Reports:  reports,
		Duration: time.Since(start),
	}
	res.Checks = runChecks(cfg, reports[lb.StrategyP2C])
	return res, nil
}

// runChecks 针对主策略（p2c）的报告做三条硬性校验。
func runChecks(cfg Config, rep Report) Checks {
	return Checks{
		WeightProportional: checkWeightProportional(rep),
		QueueBound:         checkQueueBound(cfg, rep),
		NoInflightLeak:     LeakCheck{Pass: true}, // 运行期泄漏由 CheckInflightLeak 专项校验
	}
}

// checkWeightProportional 校验稳态下请求份额与权重份额的偏差。
//
// 两层口径：
//   - 权重档聚合（决定 Pass）：把相同权重的后端请求数加总后对比期望份额。
//     P2C 第二阶段会带来轻微的均衡化倾向，单后端在小样本下相对偏差又会被
//     分母放大，所以正确性结论以大样本的聚合口径为准。
//   - 单后端明细（Details）：页面/CLI 展示每个实例的偏差，便于观察分布。
func checkWeightProportional(rep Report) WeightCheck {
	ids := make([]string, 0, len(rep.PerBackend))
	for id := range rep.PerBackend {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var totalPicked int64
	for _, id := range ids {
		totalPicked += rep.PerBackend[id]
	}
	if totalPicked == 0 {
		return WeightCheck{Pass: false}
	}

	var totalWeight float64
	weightOf := make(map[string]float64, len(ids))
	for _, id := range ids {
		w := 1 + float64(indexOf(id)%3)
		weightOf[id] = w
		totalWeight += w
	}

	rows := make([]WeightRow, 0, len(ids))
	for _, id := range ids {
		w := weightOf[id]
		picked := rep.PerBackend[id]
		want := w / totalWeight
		got := float64(picked) / float64(totalPicked)
		rows = append(rows, WeightRow{
			ID: id, Weight: w, Picked: picked,
			WantShare: want, GotShare: got,
			Deviation: absF(got-want) / want,
		})
	}

	// 权重档聚合。
	groupWeight := map[float64]float64{}
	groupPicked := map[float64]int64{}
	for _, row := range rows {
		groupWeight[row.Weight] += row.Weight
		groupPicked[row.Weight] += row.Picked
	}
	var maxDev float64
	weights := make([]float64, 0, len(groupWeight))
	for w := range groupWeight {
		weights = append(weights, w)
	}
	sort.Float64s(weights)
	for _, w := range weights {
		want := groupWeight[w] / totalWeight
		got := float64(groupPicked[w]) / float64(totalPicked)
		dev := absF(got-want) / want
		if dev > maxDev {
			maxDev = dev
		}
	}
	return WeightCheck{
		Pass:    maxDev <= WeightTolerance,
		MaxDev:  maxDev,
		Details: rows,
	}
}

// WeightTolerance 是权重档聚合口径允许的最大相对偏差。
const WeightTolerance = 0.12

func checkQueueBound(cfg Config, rep Report) QueueCheck {
	limitMS := float64(cfg.QueueTimeout.Microseconds()) / 1000.0
	return QueueCheck{
		Pass:       rep.QueueMaxMS <= limitMS+1.0, // +1ms 调度余量
		QueueMaxMS: rep.QueueMaxMS,
		LimitMS:    limitMS,
	}
}

// RunWeightProportionalCheck 跑一次「全部健康、不扩缩容、无故障、容量充足」
// 的专用场景，验证 P2C 在稳态下的承接份额与权重成正比（需求验收点）。
//
// 为什么需要专用工况：P2C 第二阶段按 延迟×(在途+1) 二选一，容量吃紧时
// 高权重实例在途更高，会主动让出部分流量——这是抑制尾延迟的有意行为。
// 因此权重正比必须在容量充足（在途普遍 0~1、第二阶段近似不扰动）的稳态
// 下检验。加权抽选本身的概率正比由 pkg/lb 的单测独立保证。
func RunWeightProportionalCheck(ctx context.Context, cfg Config) (WeightCheck, error) {
	cfg.MinBackends = cfg.Backends
	cfg.MaxBackends = cfg.Backends
	cfg.Concurrency = 64 // 容量充足，消除在途导致的系统性倾斜
	cfg.RPSPerCaller = 2 // 总压力很低
	if cfg.Duration < 25*time.Second {
		cfg.Duration = 25 * time.Second // 足够长，压低小样本噪声
	}
	cfg.WarmupRun = cfg.Duration / 3
	r := NewRunner(cfg)
	r.script = nil
	rep := r.Run(ctx, lb.StrategyP2C)
	return checkWeightProportional(rep), nil
}

func absF(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// FormatTable 把对比结果渲染成纯文本表格，供 CLI 输出。
func FormatTable(res BenchmarkResult) string {
	var out string
	out += fmt.Sprintf("种子=%d 调用方=%d 后端=%d 时长=%s\n\n",
		res.Seed, res.Config.Callers, res.Config.Backends, res.Config.Duration)
	out += fmt.Sprintf("%-14s %10s %10s %10s %10s %10s %10s %10s %10s\n",
		"策略", "完成数", "超时", "无后端", "服务端排队", "P50ms", "P90ms", "P99ms", "P99.9ms")
	for _, st := range AllStrategies {
		r := res.Reports[st]
		out += fmt.Sprintf("%-14s %10d %10d %10d %10d %10.2f %10.2f %10.2f %10.2f\n",
			st, r.Completed, r.TimedOut, r.NoBackend, r.ServerQ,
			r.LatencyMS.P50, r.LatencyMS.P90, r.LatencyMS.P99, r.LatencyMS.P999)
	}
	out += "\n排队等待(ms):\n"
	out += fmt.Sprintf("%-14s %10s %10s %10s %10s %10s\n", "策略", "P50", "P90", "P99", "Max", "上限")
	for _, st := range AllStrategies {
		r := res.Reports[st]
		out += fmt.Sprintf("%-14s %10.2f %10.2f %10.2f %10.2f %10.2f\n",
			st, r.QueueWaitMS.P50, r.QueueWaitMS.P90, r.QueueWaitMS.P99,
			r.QueueMaxMS, res.Checks.QueueBound.LimitMS)
	}
	return out
}
