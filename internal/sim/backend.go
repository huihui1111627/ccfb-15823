package sim

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Backend 是一个模拟后端实例：有并发上限、处理延迟、可被故障脚本变慢/卡顿。
type Backend struct {
	ID     string
	Weight float64

	capacity int64
	active   atomic.Int64

	mu       sync.Mutex
	base     time.Duration // 当前基础处理延迟（故障脚本可改）
	stutter  time.Duration // >0 时每个请求额外阻塞这么久（间歇卡顿）
	utilEWMA float64       // 利用率的 EWMA，响应里带回给调用方

	sem chan struct{}
	rng *safeRand
}

type BackendSpec struct {
	ID       string
	Weight   float64
	Capacity int
	Base     time.Duration
	Jitter   float64
}

func newBackend(spec BackendSpec, rng *rand.Rand) *Backend {
	b := &Backend{
		ID:       spec.ID,
		Weight:   spec.Weight,
		capacity: int64(spec.Capacity),
		base:     spec.Base,
		sem:      make(chan struct{}, spec.Capacity),
		rng:      newSafeRand(rng),
	}
	return b
}

// SetSlow 把处理延迟改成 d（d==0 恢复为基础值）。
func (b *Backend) SetSlow(d time.Duration) {
	b.mu.Lock()
	b.base = d
	b.mu.Unlock()
}

// SetStutter 设置/解除间歇卡顿。
func (b *Backend) SetStutter(d time.Duration) {
	b.mu.Lock()
	b.stutter = d
	b.mu.Unlock()
}

func (b *Backend) currentState() (base, stutter time.Duration) {
	b.mu.Lock()
	base, stutter = b.base, b.stutter
	b.mu.Unlock()
	return
}

// Handle 处理一次请求，返回端到端处理时间和当前利用率。
// 并发打满时在信号量上排队——这正是真实后端变慢后队列堆积的来源。
func (b *Backend) Handle(jitter float64, network time.Duration) (rtt time.Duration, util float64, queued bool) {
	started := time.Now()
	select {
	case b.sem <- struct{}{}:
	default:
		queued = true
		b.sem <- struct{}{} // 容量满，排队等空位
	}
	active := b.active.Add(1)
	b.updateUtil(active)

	base, stutter := b.currentState()
	proc := base
	if jitter > 0 {
		proc = time.Duration(float64(proc) * (1 + (b.rng.Float64()*2-1)*jitter))
	}
	if stutter > 0 {
		proc += stutter
	}
	time.Sleep(proc)

	<-b.sem
	active = b.active.Add(-1)
	b.updateUtil(active)

	rtt = time.Since(started) + network
	return rtt, b.utilSnapshot(), queued
}

func (b *Backend) updateUtil(active int64) {
	u := float64(active) / float64(b.capacity)
	b.mu.Lock()
	b.utilEWMA = b.utilEWMA*0.7 + u*0.3
	b.mu.Unlock()
}

func (b *Backend) utilSnapshot() float64 {
	b.mu.Lock()
	u := b.utilEWMA
	b.mu.Unlock()
	return u
}

func (b *Backend) Active() int64 { return b.active.Load() }
