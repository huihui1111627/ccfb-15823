package lb

import "time"

// Strategy 是后端选择策略。
type Strategy string

const (
	// StrategyP2C 本库的主策略：按权重抽两个，选 延迟估计×(在途+1)×利用率因子 更小的。
	StrategyP2C Strategy = "p2c"
	// StrategyRoundRobin 轮询（基线）。
	StrategyRoundRobin Strategy = "round_robin"
	// StrategyRandom 纯随机（基线）。
	StrategyRandom Strategy = "random"
	// StrategyInflightP2C 两次随机但只看在途数（基线）。
	StrategyInflightP2C Strategy = "inflight_p2c"
)

// Config 是 Balancer 的配置。零值不可直接使用，请用 DefaultConfig 后覆盖。
type Config struct {
	// Strategy 选择策略，默认 p2c。
	Strategy Strategy

	// NeutralLatency 延迟估计的中性值：新后端初始估计、长期无流量后的回归目标。
	NeutralLatency time.Duration
	// FastRaiseRatio 样本比当前估计高出该比例时，估计立即抬到样本值。
	// 取 0 表示只要样本更大就立即抬升（对变慢最敏感）。
	FastRaiseRatio float64
	// DecayAlpha 较小样本的 EWMA 系数，越大向低值收敛越快。
	DecayAlpha float64
	// IdleRevert 多久没有新样本后，估计开始随时间向中性值回归。
	IdleRevert time.Duration
	// IdleRevertTime 估计从当前值线性回归到中性值所用的时间。
	IdleRevertTime time.Duration

	// UtilWeight 利用率在打分中的权重，score = latency×(inflight+1)×(1+UtilWeight×util)。
	UtilWeight float64
	// UtilAlpha 利用率 EWMA 系数。
	UtilAlpha float64

	// Warmup 新后端从最低份额线性升到正常份额的时长。
	Warmup time.Duration
	// WarmupStartWeight 预热开始时的有效权重比例（如 0.05 = 正常份额的 5%）。
	WarmupStartWeight float64

	// QueueTimeout 没有可用后端时请求最多排队等待多久，超时返回 ErrNoBackend。
	QueueTimeout time.Duration
	// FlightTimeout 在途兜底：超过此时长仍未 Done 的在途记录强制回收。
	// 正常路径永远先靠显式 Done 回收，这只是防止泄漏的安全网。
	FlightTimeout time.Duration
	// SweepInterval 兜底扫描间隔。
	SweepInterval time.Duration

	// Clock 用于注入测试时钟；nil 时使用真实时钟。
	Clock Clock
	// Rand 随机源；nil 时使用全局随机源。测试可注入固定种子。
	Rand Rand
}

// DefaultConfig 返回一套可用的默认配置。
func DefaultConfig() Config {
	return Config{
		Strategy:          StrategyP2C,
		NeutralLatency:    10 * time.Millisecond,
		FastRaiseRatio:    0,
		DecayAlpha:        0.2,
		IdleRevert:        5 * time.Second,
		IdleRevertTime:    10 * time.Second,
		UtilWeight:        0.5,
		UtilAlpha:         0.3,
		Warmup:            30 * time.Second,
		WarmupStartWeight: 0.05,
		QueueTimeout:      2 * time.Second,
		FlightTimeout:     30 * time.Second,
		SweepInterval:     500 * time.Millisecond,
	}
}

func (c *Config) normalize() {
	if c.NeutralLatency <= 0 {
		c.NeutralLatency = 10 * time.Millisecond
	}
	if c.DecayAlpha <= 0 || c.DecayAlpha > 1 {
		c.DecayAlpha = 0.2
	}
	if c.UtilAlpha <= 0 || c.UtilAlpha > 1 {
		c.UtilAlpha = 0.3
	}
	if c.IdleRevert <= 0 {
		c.IdleRevert = 5 * time.Second
	}
	if c.IdleRevertTime <= 0 {
		c.IdleRevertTime = 10 * time.Second
	}
	if c.Warmup <= 0 {
		c.Warmup = 30 * time.Second
	}
	if c.WarmupStartWeight <= 0 || c.WarmupStartWeight > 1 {
		c.WarmupStartWeight = 0.05
	}
	if c.QueueTimeout <= 0 {
		c.QueueTimeout = 2 * time.Second
	}
	if c.FlightTimeout <= 0 {
		c.FlightTimeout = 30 * time.Second
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = 500 * time.Millisecond
	}
	if c.Strategy == "" {
		c.Strategy = StrategyP2C
	}
}
