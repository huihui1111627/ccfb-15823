// Package sim 在进程内模拟调用方、后端实例和网络，用于对比负载均衡策略。
package sim

import (
	"math/rand/v2"
	"time"
)

// Config 是一次模拟运行的参数。
type Config struct {
	Seed         uint64
	Callers      int           // 调用方数量
	Backends     int           // 初始后端数量
	Duration     time.Duration // 运行时长
	WarmupRun    time.Duration // 统计前预热（让估计器、预热权重进入稳态）
	RPSPerCaller int           // 每个调用方的目标每秒请求数

	BaseLatency   time.Duration // 健康后端基础处理延迟
	LatencyJitter float64       // 处理延迟抖动比例，如 0.2 表示 ±20%
	NetworkDelay  time.Duration // 固定网络延迟（在处理延迟之外叠加）
	Concurrency   int           // 单个后端最大并发处理能力（信号量）
	QueueTimeout  time.Duration // Balancer 无后端可用时的排队上限

	MinBackends int // 扩缩容下限
	MaxBackends int // 扩缩容上限
}

func (c *Config) normalize() {
	if c.Callers <= 0 {
		c.Callers = 200
	}
	if c.Backends <= 0 {
		c.Backends = 100
	}
	if c.Duration <= 0 {
		c.Duration = 30 * time.Second
	}
	if c.WarmupRun <= 0 {
		c.WarmupRun = 3 * time.Second
	}
	if c.RPSPerCaller <= 0 {
		c.RPSPerCaller = 20
	}
	if c.BaseLatency <= 0 {
		c.BaseLatency = 5 * time.Millisecond
	}
	if c.NetworkDelay <= 0 {
		c.NetworkDelay = time.Millisecond
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.QueueTimeout <= 0 {
		c.QueueTimeout = 2 * time.Second
	}
	if c.MinBackends <= 0 {
		c.MinBackends = c.Backends / 2
	}
	if c.MaxBackends <= 0 {
		c.MaxBackends = c.Backends * 2
	}
}

// streamRand 为每个调用方/后端派生独立的随机流，保证四种策略跑同一份脚本时，
// 除了调度决策不同，其余随机性完全一致，对比才公平。
func streamRand(seed uint64, stream uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, stream))
}

// DefaultConfigForCLI 返回命令行使用的基准参数。
func DefaultConfigForCLI() Config {
	return Config{
		Seed:          20261124,
		Callers:       200,
		Backends:      100,
		Duration:      30 * time.Second,
		WarmupRun:     3 * time.Second,
		RPSPerCaller:  25,
		BaseLatency:   5 * time.Millisecond,
		LatencyJitter: 0.2,
		NetworkDelay:  time.Millisecond,
		Concurrency:   8,
		QueueTimeout:  2 * time.Second,
	}
}
