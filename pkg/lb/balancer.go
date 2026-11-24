package lb

import (
	"container/heap"
	"context"
	"errors"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"p2clb/pkg/discovery"
)

var (
	// ErrNoBackend 排队等待 QueueTimeout 后仍没有可用后端。
	ErrNoBackend = errors.New("lb: no available backend within queue timeout")
	// ErrClosed Balancer 已关闭。
	ErrClosed = errors.New("lb: balancer closed")
)

// Result 是一次 RPC 结束后回传给 Balancer 的结果。
type Result struct {
	// RTT 本次请求的端到端延迟；TimedOut/Err 非空时忽略。
	RTT time.Duration
	// Utilization 后端在响应里携带的自身利用率，取值 0~1。
	Utilization float64
	// TimedOut 本次请求是否超时/被取消；为 true 时不产生延迟样本，只回收在途。
	TimedOut bool
	// Err 业务错误。非 nil 且 TimedOut=false 时同样不产生延迟样本。
	Err error
}

// Handle 是一次 Pick 的凭证。必须且只能调用一次 Done（重复调用是空操作）。
type Handle struct {
	balancer *Balancer
	b        *backend
	pickedAt time.Time
	f        *flight
	waitTime time.Duration
	done     atomic.Bool
}

// BackendID 返回被选中的后端 ID。
func (h *Handle) BackendID() string { return h.b.id }

// WaitTime 返回本次 Pick 在等待队列里停留的时间（未排队则为 0）。
func (h *Handle) WaitTime() time.Duration { return h.waitTime }

// Done 归还在途并上报结果。取消、超时、panic 路径都应 defer 调用它。
func (h *Handle) Done(r Result) {
	if !h.done.CompareAndSwap(false, true) {
		return // 兜底回收已处理过，忽略迟到的回调
	}
	h.balancer.finish(h, r)
}

// flight 是一条在途记录，用于超时兜底回收。
type flight struct {
	deadline time.Time
	seq      int64
	b        *backend
	h        *Handle
	index    int
}

type flightHeap []*flight

func (h flightHeap) Len() int           { return len(h) }
func (h flightHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h flightHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *flightHeap) Push(x any) {
	it := x.(*flight)
	it.index = len(*h)
	*h = append(*h, it)
}
func (h *flightHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	it.index = -1
	*h = old[:n-1]
	return it
}

// waiter 是无可用后端时排队等待的请求。
type waiter struct {
	ch         chan struct{}
	enqueuedAt time.Time
	deadline   time.Time
	timedOut   bool // 已由 prune/pump 记为超时，timer 分支不得重复计数
}

// Balancer 是客户端负载均衡器。
type Balancer struct {
	cfg   Config
	disc  discovery.Discovery
	clock Clock
	rng   Rand

	mu        sync.Mutex
	backends  map[string]*backend
	available []*backend // 与 backends 中 !removed 的条目保持一致，每次发现变更重建
	rrCounter uint64

	waiters   []*waiter
	flights   flightHeap
	flightSeq int64

	latHist   *Histogram
	queueHist *Histogram

	queueTimeouts int64
	queueWaitMax  time.Duration

	closeOnce sync.Once
	closed    chan struct{}
	wg        sync.WaitGroup
}

// New 创建 Balancer 并开始消费服务发现事件。
func New(disc discovery.Discovery, cfg Config) *Balancer {
	cfg.normalize()
	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}
	rng := cfg.Rand
	if rng == nil {
		rng = &lockedRand{r: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))}
	}
	bl := &Balancer{
		cfg:       cfg,
		disc:      disc,
		clock:     clock,
		rng:       rng,
		backends:  make(map[string]*backend),
		latHist:   NewHistogram(100*time.Microsecond, 10*time.Second, 1.35),
		queueHist: NewHistogram(50*time.Microsecond, 5*time.Second, 1.35),
		closed:    make(chan struct{}),
	}
	bl.wg.Add(2)
	go bl.discoveryLoop()
	go bl.sweepLoop()
	return bl
}

// Close 停止后台循环。
func (bl *Balancer) Close() {
	bl.closeOnce.Do(func() { close(bl.closed) })
	bl.wg.Wait()
}

func (bl *Balancer) discoveryLoop() {
	defer bl.wg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-bl.closed
		cancel()
	}()
	ch, err := bl.disc.Watch(ctx)
	if err != nil {
		return
	}
	for {
		select {
		case <-bl.closed:
			return
		case u, ok := <-ch:
			if !ok {
				return
			}
			bl.applyUpdate(u)
		}
	}
}

func (bl *Balancer) applyUpdate(u discovery.Update) {
	now := bl.clock.Now()
	wanted := make(map[string]discovery.Backend, len(u.Backends))
	for _, d := range u.Backends {
		wanted[d.ID] = d
	}

	bl.mu.Lock()
	defer bl.mu.Unlock()

	for id, d := range wanted {
		if b, ok := bl.backends[id]; ok {
			// 回归的实例（之前被摘除但在途未排空、结构体仍在 map）必须复位
			// removed，否则它永远进不了 available。估计器/在途状态保留。
			b.removed = false
			b.weight = d.Weight // 权重热更新
			continue
		}
		bl.backends[id] = newBackend(d, bl.cfg, now) // 新实例进入预热
	}
	for id, b := range bl.backends {
		if _, ok := wanted[id]; !ok {
			b.removed = true
		}
	}

	avail := make([]*backend, 0, len(wanted))
	for _, b := range bl.backends {
		if !b.removed {
			avail = append(avail, b)
		}
	}
	sort.Slice(avail, func(i, j int) bool { return avail[i].id < avail[j].id })
	bl.available = avail

	bl.pumpWaitersLocked(now)
}

// Pick 选择一个后端。没有可用后端时排队，最多等 QueueTimeout。
func (bl *Balancer) Pick(ctx context.Context) (*Handle, error) {
	var w *waiter

	for {
		now := bl.clock.Now()
		bl.mu.Lock()
		bl.pruneExpiredWaitersLocked(now)
		if b := bl.selectLocked(now); b != nil {
			h := bl.acquireLocked(b, now)
			if w != nil {
				h.waitTime = now.Sub(w.enqueuedAt)
				bl.queueHist.Observe(h.waitTime)
				if h.waitTime > bl.queueWaitMax {
					bl.queueWaitMax = h.waitTime
				}
			}
			bl.mu.Unlock()
			return h, nil
		}
		if w == nil {
			deadline := now.Add(bl.cfg.QueueTimeout)
			if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
				deadline = d
			}
			w = &waiter{
				ch:         make(chan struct{}, 1),
				enqueuedAt: now,
				deadline:   deadline,
			}
			bl.waiters = append(bl.waiters, w)
		}
		bl.mu.Unlock()

		timer := time.NewTimer(time.Until(w.deadline))
		select {
		case <-w.ch:
			timer.Stop()
			continue // 有实例回来了，重新走一次选择
		case <-ctx.Done():
			timer.Stop()
			bl.removeWaiter(w)
			return nil, ctx.Err()
		case <-timer.C:
			// 超时的权威发生点：移除 waiter 并计数。
			// 若它已被 prune/pump 提前处理（timedOut=true），这里只返回错误。
			bl.mu.Lock()
			alreadyCounted := w.timedOut
			bl.removeWaiterLocked(w)
			if !alreadyCounted {
				bl.queueTimeouts++
			}
			waited := bl.clock.Now().Sub(w.enqueuedAt)
			bl.queueHist.Observe(waited)
			if waited > bl.queueWaitMax {
				bl.queueWaitMax = waited
			}
			bl.mu.Unlock()
			return nil, ErrNoBackend
		}
	}
}

func (bl *Balancer) removeWaiter(target *waiter) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.removeWaiterLocked(target)
}

// removeWaiterLocked 调用方必须已持锁。
func (bl *Balancer) removeWaiterLocked(target *waiter) {
	for i, w := range bl.waiters {
		if w == target {
			bl.waiters = append(bl.waiters[:i], bl.waiters[i+1:]...)
			return
		}
	}
}

// pruneExpiredWaitersLocked 丢弃已过 deadline 但还没被唤醒的 waiter，
// 保证 QueuedNow 只统计真正还在等待的请求。
func (bl *Balancer) pruneExpiredWaitersLocked(now time.Time) {
	kept := bl.waiters[:0]
	for _, w := range bl.waiters {
		if w.deadline.After(now) {
			kept = append(kept, w)
			continue
		}
		w.timedOut = true // timer 分支据此跳过重复计数
		bl.queueTimeouts++
		waited := now.Sub(w.enqueuedAt)
		bl.queueHist.Observe(waited)
		if waited > bl.queueWaitMax {
			bl.queueWaitMax = waited
		}
	}
	bl.waiters = kept
}

// pumpWaitersLocked 在有新可用后端时唤醒队头请求。FIFO。
// 等待期间已超时的 waiter 直接出队并记失败，不能留在队头挡住后续请求。
func (bl *Balancer) pumpWaitersLocked(now time.Time) {
	for len(bl.waiters) > 0 {
		w := bl.waiters[0]
		bl.waiters = bl.waiters[1:]
		if !w.deadline.After(now) {
			w.timedOut = true // timer 分支据此跳过重复计数
			bl.queueTimeouts++
			waited := now.Sub(w.enqueuedAt)
			bl.queueHist.Observe(waited)
			if waited > bl.queueWaitMax {
				bl.queueWaitMax = waited
			}
			continue
		}
		if len(bl.available) == 0 {
			// 没有后端可分配，把它放回队头等下一次唤醒。
			bl.waiters = append([]*waiter{w}, bl.waiters...)
			return
		}
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

func (bl *Balancer) acquireLocked(b *backend, now time.Time) *Handle {
	b.inflight++
	b.picked++
	h := &Handle{balancer: bl, b: b, pickedAt: now}
	bl.flightSeq++
	f := &flight{
		deadline: now.Add(bl.cfg.FlightTimeout),
		seq:      bl.flightSeq,
		b:        b,
		h:        h,
	}
	h.f = f
	heap.Push(&bl.flights, f)
	return h
}

func (bl *Balancer) finish(h *Handle, r Result) {
	now := bl.clock.Now()
	bl.mu.Lock()
	defer bl.mu.Unlock()

	b := h.b
	b.inflight--
	if h.f.index >= 0 {
		heap.Remove(&bl.flights, h.f.index)
	}
	h.f.index = -1

	if r.TimedOut || r.Err != nil {
		b.timedOut++
	} else {
		b.completed++
		if r.RTT > 0 {
			b.est.Observe(r.RTT, now)
			bl.latHist.Observe(r.RTT)
		}
		b.observeUtil(bl.cfg, r.Utilization)
	}

	if b.removed && b.inflight == 0 {
		delete(bl.backends, b.id)
	}
}

// sweepLoop 是在途泄漏的安全网：正常路径靠 Done 立即回收，
// 这里只处理回调丢失（goroutine 卡死、漏写 defer 等）的极端情况。
func (bl *Balancer) sweepLoop() {
	defer bl.wg.Done()
	ticker := time.NewTicker(bl.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-bl.closed:
			return
		case now := <-ticker.C:
			bl.sweep(now)
		}
	}
}

func (bl *Balancer) sweep(now time.Time) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	for bl.flights.Len() > 0 && bl.flights[0].deadline.Before(now) {
		f := heap.Pop(&bl.flights).(*flight)
		if !f.h.done.CompareAndSwap(false, true) {
			continue // 已正常 Done，理论上不会出现
		}
		f.index = -1
		b := f.b
		b.inflight--
		b.swept++
		if b.removed && b.inflight == 0 {
			delete(bl.backends, b.id)
		}
	}
}

// ---------------- 选择策略 ----------------

func (bl *Balancer) selectLocked(now time.Time) *backend {
	list := bl.available
	if len(list) == 0 {
		return nil
	}
	switch bl.cfg.Strategy {
	case StrategyRoundRobin:
		b := list[bl.rrCounter%uint64(len(list))]
		bl.rrCounter++
		return b
	case StrategyRandom:
		return list[bl.rng.IntN(len(list))]
	case StrategyInflightP2C:
		a, c := bl.twoUniform(list)
		if a.inflight <= c.inflight {
			return a
		}
		return c
	default: // StrategyP2C
		a, c := bl.weightedTwoDraw(list, now)
		sa, sc := a.scoreAt(bl.cfg, now), c.scoreAt(bl.cfg, now)
		switch {
		case sa < sc:
			return a
		case sc < sa:
			return c
		case a.inflight != c.inflight:
			if a.inflight < c.inflight {
				return a
			}
			return c
		default:
			if a.id < c.id {
				return a
			}
			return c
		}
	}
}

// twoUniform 等概率抽两个不同实例；只有一台时两次都返回它。
func (bl *Balancer) twoUniform(list []*backend) (a, b *backend) {
	if len(list) == 1 {
		return list[0], list[0]
	}
	i := bl.rng.IntN(len(list))
	j := bl.rng.IntN(len(list) - 1)
	if j >= i {
		j++
	}
	return list[i], list[j]
}

// weightedTwoDraw 按有效权重抽两个不同实例（需求 1、5、6）。
func (bl *Balancer) weightedTwoDraw(list []*backend, now time.Time) (a, b *backend) {
	if len(list) == 1 {
		return list[0], list[0]
	}
	a = bl.weightedPick(list, now)
	for i := 0; i < 8; i++ {
		c := bl.weightedPick(list, now)
		if c != a {
			return a, c
		}
	}
	// 随机重抽连续撞同一个（极端权重下理论可能），退化为取下一个不同实例。
	for _, c := range list {
		if c != a {
			return a, c
		}
	}
	return a, a
}

// weightedPick 按有效权重比例抽一个实例。
// 权重在同一 now 下预算一次，避免抽取过程中时钟推进导致口径不一致。
func (bl *Balancer) weightedPick(list []*backend, now time.Time) *backend {
	weights := make([]float64, len(list))
	var total float64
	for i, b := range list {
		weights[i] = b.effectiveWeight(bl.cfg, now)
		total += weights[i]
	}
	if total <= 0 {
		return list[bl.rng.IntN(len(list))]
	}
	r := bl.rng.Float64() * total
	var acc float64
	for i, b := range list {
		acc += weights[i]
		if r < acc {
			return b
		}
	}
	return list[len(list)-1]
}

// Snapshot 导出全量指标，供页面和测试断言使用。
func (bl *Balancer) Snapshot() Snapshot {
	now := bl.clock.Now()
	bl.mu.Lock()
	defer bl.mu.Unlock()

	snap := Snapshot{
		Strategy:       string(bl.cfg.Strategy),
		AvailableCount: len(bl.available),
		QueuedNow:      len(bl.waiters),
		QueueTimeouts:  bl.queueTimeouts,
		QueueWaitMax:   bl.queueWaitMax,
		LatencyMS:      report(bl.latHist),
		QueueWaitMS:    report(bl.queueHist),
		Backends:       make([]BackendStat, 0, len(bl.backends)),
	}
	for _, b := range bl.backends {
		snap.TotalPicked += b.picked
		snap.TotalInFlight += b.inflight
		snap.TotalCompleted += b.completed
		snap.TotalTimeout += b.timedOut
		snap.TotalSwept += b.swept
		snap.Backends = append(snap.Backends, BackendStat{
			ID:                b.id,
			Weight:            b.weight,
			EffectiveWeight:   b.effectiveWeight(bl.cfg, now),
			WarmupFactor:      b.warmupFactor(bl.cfg, now),
			Picked:            b.picked,
			InFlight:          b.inflight,
			Completed:         b.completed,
			TimedOut:          b.timedOut,
			Swept:             b.swept,
			LatencyEstimateMS: float64(b.est.Estimate(now).Microseconds()) / 1000.0,
			Utilization:       b.util,
		})
	}
	sort.Slice(snap.Backends, func(i, j int) bool { return snap.Backends[i].ID < snap.Backends[j].ID })
	return snap
}
