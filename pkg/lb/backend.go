package lb

import (
	"time"

	"p2clb/pkg/discovery"
)

// backend 是 Balancer 内部对一个后端实例维护的全部状态。
type backend struct {
	id     string
	weight float64

	inflight int64

	// 预热：joinedAt 之后的 Warmup 时间内，有效权重从 start 线性升到 1。
	joinedAt time.Time

	est  *Estimator
	util float64 // 最近一次利用率的 EWMA，取值 0~1

	// 指标计数。
	picked    int64
	completed int64
	timedOut  int64
	swept     int64

	// removed 表示发现源已经摘除该实例；在途清零后从 map 删除。
	removed bool
}

func newBackend(d discovery.Backend, cfg Config, now time.Time) *backend {
	return &backend{
		id:       d.ID,
		weight:   d.Weight,
		joinedAt: now,
		est:      newEstimator(cfg, now),
	}
}

// warmupFactor 返回当前预热系数 [WarmupStartWeight, 1]。
func (b *backend) warmupFactor(cfg Config, now time.Time) float64 {
	elapsed := now.Sub(b.joinedAt)
	if elapsed >= cfg.Warmup {
		return 1
	}
	p := float64(elapsed) / float64(cfg.Warmup)
	return cfg.WarmupStartWeight + (1-cfg.WarmupStartWeight)*p
}

// effectiveWeight 是参与加权随机的实际权重。
func (b *backend) effectiveWeight(cfg Config, now time.Time) float64 {
	return b.weight * b.warmupFactor(cfg, now)
}

// observeUtil 用后端响应里携带的利用率做 EWMA。
func (b *backend) observeUtil(cfg Config, u float64) {
	if u < 0 {
		u = 0
	} else if u > 1 {
		u = 1
	}
	b.util = b.util*(1-cfg.UtilAlpha) + u*cfg.UtilAlpha
}

// scoreAt 是 P2C 的打分（需求 2、7）：
//
//	延迟估计 × (在途+1) × (1 + UtilWeight×利用率)
//
// 越小越优。注意高权重后端按加权抽选本就承接更多在途，因此在容量吃紧时
// 第二阶段会把一部分流量让给低权重实例——这是 P2C 抑制尾延迟的有意行为。
// 「请求量与权重成正比」的验收在健康、容量充足的稳态下进行（见 sim 包的
// RunWeightProportionalCheck），加权抽选本身的概率正比由 lb 单测保证。
func (b *backend) scoreAt(cfg Config, now time.Time) float64 {
	latency := float64(b.est.Estimate(now))
	load := float64(b.inflight + 1)
	utilFactor := 1 + cfg.UtilWeight*b.util
	return latency * load * utilFactor
}

// score 保留给外部（测试/快照）按当前时间打分。
func (b *backend) score(cfg Config, now time.Time) float64 { return b.scoreAt(cfg, now) }
