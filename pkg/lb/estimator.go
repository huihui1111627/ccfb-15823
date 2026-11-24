package lb

import (
	"math/rand/v2"
	"time"
)

// Clock 抽象时间，便于单测注入虚拟时钟。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Rand 是本库使用的随机源抽象，测试可注入固定种子。
type Rand interface {
	IntN(n int) int
	Float64() float64
}

type lockedRand struct{ r *rand.Rand }

func (l *lockedRand) IntN(n int) int {
	if n <= 0 {
		return 0
	}
	return l.r.IntN(n)
}

func (l *lockedRand) Float64() float64 { return l.r.Float64() }

// Estimator 是单个后端的延迟估计器。
//
// 规则（对应需求 3、4）：
//   - 样本 >= 当前估计×(1+FastRaiseRatio)：立即抬到样本值（对变慢敏感）；
//   - 样本更小：EWMA 缓慢下拉；
//   - 超过 IdleRevert 没有新样本：估计随时间线性回归到中性值，
//     不会永远停在一次慢记录上。
type Estimator struct {
	neutral        time.Duration
	fastRaiseRatio float64
	alpha          float64
	idleRevert     time.Duration
	idleRevertTime time.Duration

	estimate    time.Duration
	lastSample  time.Time
	lastUpdated time.Time
}

func newEstimator(cfg Config, now time.Time) *Estimator {
	return &Estimator{
		neutral:        cfg.NeutralLatency,
		fastRaiseRatio: cfg.FastRaiseRatio,
		alpha:          cfg.DecayAlpha,
		idleRevert:     cfg.IdleRevert,
		idleRevertTime: cfg.IdleRevertTime,
		estimate:       cfg.NeutralLatency,
		lastSample:     now,
		lastUpdated:    now,
	}
}

// Observe 收到一次新的请求延迟样本。
func (e *Estimator) Observe(rtt time.Duration, now time.Time) {
	threshold := time.Duration(float64(e.estimate) * (1 + e.fastRaiseRatio))
	switch {
	case rtt >= threshold:
		// 变慢：立即抬升，不等收敛。
		e.estimate = rtt
	case rtt < e.estimate:
		// 变快：EWMA 慢慢下拉，避免抖动。
		e.estimate = time.Duration(float64(e.estimate)*(1-e.alpha) + float64(rtt)*e.alpha)
	}
	e.lastSample = now
	e.lastUpdated = now
}

// Estimate 返回当前估计值，应用空闲回归。
func (e *Estimator) Estimate(now time.Time) time.Duration {
	idleFor := now.Sub(e.lastSample)
	if idleFor <= e.idleRevert {
		return e.estimate
	}
	progress := float64(idleFor-e.idleRevert) / float64(e.idleRevertTime)
	if progress >= 1 {
		return e.neutral
	}
	return time.Duration(float64(e.estimate)*(1-progress) + float64(e.neutral)*progress)
}
