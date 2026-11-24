package sim

import (
	"context"
	"sync"
	"time"

	"p2clb/pkg/discovery"
	"p2clb/pkg/lb"
)

// Runner 执行一次完整模拟：调用方发压、后端处理、脚本注入故障和扩缩容。
// 每次 Run 都从同一份确定性初始状态出发，因此不同策略的多次运行可直接对比。
type Runner struct {
	cfg    Config
	script []Op

	disc     *discovery.MemoryDiscovery
	backends sync.Map // id -> *Backend
	metrics  *Metrics

	// 所有后端的发现权重（权重本身固定，扩缩容改变集合）。
	weights map[string]float64
	mu      sync.Mutex

	lastSnapshot lb.Snapshot
}

func NewRunner(cfg Config) *Runner {
	cfg.normalize()
	r := &Runner{
		cfg:     cfg,
		script:  BuildScript(cfg),
		disc:    discovery.NewMemoryDiscovery(nil),
		weights: make(map[string]float64),
	}
	// 预生成足够覆盖 MaxBackends 的 ID 和权重，扩容时直接取用。
	for i := range cfg.MaxBackends {
		id := backendID(i)
		r.weights[id] = 1 + float64(i%3) // 1,2,3,1,2,3...
	}
	initIDs := make([]string, cfg.Backends)
	for i := range cfg.Backends {
		initIDs[i] = backendID(i)
	}
	r.metrics = newMetrics(initIDs)
	r.setBackends(initIDs)
	return r
}

func (r *Runner) setBackends(ids []string) {
	list := make([]discovery.Backend, 0, len(ids))
	for _, id := range ids {
		list = append(list, discovery.Backend{ID: id, Weight: r.weights[id]})
		if _, ok := r.backends.Load(id); !ok {
			spec := BackendSpec{
				ID:       id,
				Weight:   r.weights[id],
				Capacity: r.cfg.Concurrency,
				Base:     r.cfg.BaseLatency,
				Jitter:   r.cfg.LatencyJitter,
			}
			r.backends.Store(id, newBackend(spec, streamRand(r.cfg.Seed, hashID(id))))
		}
	}
	r.disc.Set(list)
}

func (r *Runner) balancerConfig(strategy lb.Strategy) lb.Config {
	return r.BalancerConfig(strategy)
}

// BalancerConfig 导出与该模拟匹配的 Balancer 配置，供 serve 模式复用。
func (r *Runner) BalancerConfig(strategy lb.Strategy) lb.Config {
	cfg := lb.DefaultConfig()
	cfg.Strategy = strategy
	cfg.QueueTimeout = r.cfg.QueueTimeout
	cfg.NeutralLatency = r.cfg.BaseLatency * 2
	cfg.Rand = &randStub{r: streamRand(r.cfg.Seed, 991)}
	// 预热时长取模拟时长的 1/4，保证扩缩容加入的实例能被观察到爬升过程。
	cfg.Warmup = r.cfg.Duration / 4
	return cfg
}

// RunWithBalancer 在调用方提供的 Balancer 上跑一轮，不负责关闭它
// （serve 模式需要跨轮复用同一个 Balancer，让页面持续读到指标）。
func (r *Runner) RunWithBalancer(ctx context.Context, bal *lb.Balancer) Report {
	return r.runWithBalancer(ctx, bal)
}

// Run 用指定策略跑完整模拟并返回报告。
func (r *Runner) Run(ctx context.Context, strategy lb.Strategy) Report {
	bal := lb.New(r.disc, r.BalancerConfig(strategy))
	defer bal.Close()
	return r.runWithBalancer(ctx, bal)
}

func (r *Runner) runWithBalancer(ctx context.Context, bal *lb.Balancer) Report {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	// 脚本驱动器。
	wg.Add(1)
	go func() {
		defer wg.Done()
		r.runScript(runCtx)
	}()

	// 调用方。
	startGate := make(chan struct{})
	for c := range r.cfg.Callers {
		wg.Add(1)
		go r.runCaller(runCtx, &wg, startGate, bal, c)
	}
	close(startGate)

	// 到点停止，等所有调用方收尾。
	timer := time.NewTimer(r.cfg.Duration)
	select {
	case <-timer.C:
	case <-runCtx.Done():
	}
	cancel()
	wg.Wait()

	r.lastSnapshot = bal.Snapshot()
	return r.metrics.report(string(bal.Snapshot().Strategy))
}

func (r *Runner) runScript(ctx context.Context) {
	start := time.Now()
	for _, op := range r.script {
		wait := op.At - time.Since(start)
		if wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
		}
		r.applyOp(op)
	}
}

func (r *Runner) applyOp(op Op) {
	if v, ok := r.backends.Load(op.TargetID); ok {
		b := v.(*Backend)
		switch op.Kind {
		case opSlow:
			b.SetSlow(op.SlowTo)
		case opRecover:
			b.SetSlow(r.cfg.BaseLatency)
		case opStutterOn:
			b.SetStutter(op.Stutter)
		case opStutterOff:
			b.SetStutter(0)
		}
	}
	if op.Kind == opScale {
		r.scaleTo(op.Count)
	}
}

func (r *Runner) scaleTo(count int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 现存 ID 按序号排序，扩时按序新建，缩时从尾部摘除。
	existing := make([]string, 0, len(r.weights))
	for id := range r.weights {
		existing = append(existing, id)
	}
	sortByIndex(existing)

	if count > len(existing) {
		next := indexOf(existing[len(existing)-1]) + 1
		for len(existing) < count {
			id := backendID(next)
			if _, ok := r.weights[id]; !ok {
				break // 超出 MaxBackends 预生成范围
			}
			r.metrics.addBackend(id)
			existing = append(existing, id)
			next++
		}
	} else if count < len(existing) {
		existing = existing[:count]
	}
	r.setBackends(existing)
}

func (r *Runner) runCaller(ctx context.Context, wg *sync.WaitGroup, gate chan struct{}, bal *lb.Balancer, idx int) {
	defer wg.Done()
	<-gate

	rng := streamRand(r.cfg.Seed, uint64(1000+idx))
	interval := time.Second / time.Duration(r.cfg.RPSPerCaller)
	// 每个调用方错开相位，避免所有请求在同一瞬间打出；之后严格等间隔。
	phase := time.Duration(rng.Int64N(int64(interval)))
	timer := time.NewTimer(phase)
	defer timer.Stop()

	statsStart := time.Now().Add(r.cfg.WarmupRun)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(interval)
			r.issue(ctx, bal, time.Now().After(statsStart))
		}
	}
}

func (r *Runner) issue(ctx context.Context, bal *lb.Balancer, countIt bool) {
	// 单请求超时设为基础延迟的 30 倍，模拟调用方的超时预算。
	reqCtx, cancel := context.WithTimeout(ctx, r.cfg.BaseLatency*30)
	defer cancel()

	pickStart := time.Now()
	h, err := bal.Pick(reqCtx)
	wait := time.Since(pickStart)
	if err != nil {
		if countIt {
			if reqCtx.Err() != nil && err != lb.ErrNoBackend {
				r.metrics.recordFailure(wait, "canceled")
			} else {
				r.metrics.recordFailure(wait, "no_backend")
			}
		}
		return
	}

	v, ok := r.backends.Load(h.BackendID())
	if !ok {
		// 实例已被摘除的极端竞态：按取消处理，在途仍由 Done 回收。
		h.Done(lb.Result{TimedOut: true})
		if countIt {
			r.metrics.recordFailure(wait, "canceled")
		}
		return
	}
	b := v.(*Backend)

	done := make(chan struct{})
	var rtt time.Duration
	var util float64
	var serverQueued bool
	go func() {
		rtt, util, serverQueued = b.Handle(r.cfg.LatencyJitter, r.cfg.NetworkDelay)
		close(done)
	}()

	select {
	case <-done:
		h.Done(lb.Result{RTT: rtt, Utilization: util})
		if countIt {
			r.metrics.recordSuccess(h.BackendID(), rtt, h.WaitTime(), serverQueued)
		}
	case <-reqCtx.Done():
		// 超时/取消：必须及时减回在途（需求 8）。
		h.Done(lb.Result{TimedOut: true})
		if countIt {
			r.metrics.recordFailure(h.WaitTime(), "timeout")
		}
		<-done // 后端调用最终会结束，避免 goroutine 泄漏
	}
}

// Snapshot 暴露底层 Balancer 视角之外的世界状态，供页面使用。
func (r *Runner) Discovery() *discovery.MemoryDiscovery { return r.disc }

// BackendActive 返回某后端当前在模拟服务端的在途数。
func (r *Runner) BackendActive(id string) int64 {
	if v, ok := r.backends.Load(id); ok {
		return v.(*Backend).Active()
	}
	return 0
}

func hashID(id string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	return h
}

type lockedSource struct{ r *randStub }
