package lb

import (
	"context"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"

	"p2clb/pkg/discovery"
)

// seedRand 固定种子随机源，保证选择概率类测试可复现。
type seedRand struct{ r *rand.Rand }

func (s *seedRand) IntN(n int) int {
	if n <= 0 {
		return 0
	}
	return s.r.IntN(n)
}
func (s *seedRand) Float64() float64 { return s.r.Float64() }

func newTestBalancer(t *testing.T, cfg Config, backends []discovery.Backend) (*Balancer, *discovery.MemoryDiscovery) {
	t.Helper()
	disc := discovery.NewMemoryDiscovery(backends)
	bl := New(disc, cfg)
	t.Cleanup(bl.Close)
	if len(backends) > 0 {
		bl.waitForBackends(len(backends))
	}
	return bl, disc
}

func weightedBackends(weights ...float64) []discovery.Backend {
	out := make([]discovery.Backend, len(weights))
	for i, w := range weights {
		out[i] = discovery.Backend{ID: "b" + string(rune('A'+i)), Weight: w}
	}
	return out
}

func TestWeightedTwoDrawProbabilityProportionalToWeight(t *testing.T) {
	cfg := testConfig()
	cfg.Rand = &seedRand{r: rand.New(rand.NewPCG(42, 7))}
	backends := weightedBackends(1, 3, 6) // 期望候选份额 10% / 30% / 60%
	bl, _ := newTestBalancer(t, cfg, backends)
	bl.mu.Lock()
	now := bl.clock.Now()
	list := append([]*backend(nil), bl.available...)
	bl.mu.Unlock()

	// P2C 的最终选择还受在途/延迟打分影响，所以这里直接验证需求 1：
	// 加权抽取环节，每个后端进入候选池的概率与权重成正比。
	counts := map[string]int{}
	const n = 60000
	for range n {
		b := bl.weightedPick(list, now)
		counts[b.id]++
	}
	want := map[string]float64{"bA": 0.1, "bB": 0.3, "bC": 0.6}
	for id, p := range want {
		got := float64(counts[id]) / n
		if d := abs(got - p); d > 0.02 {
			t.Errorf("backend %s draw share = %.3f, want %.3f (±0.02)", id, got, p)
		}
	}
}

func TestWeightedTwoDrawReturnsDistinctBackends(t *testing.T) {
	cfg := testConfig()
	cfg.Rand = &seedRand{r: rand.New(rand.NewPCG(11, 13))}
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1, 3, 6))
	bl.mu.Lock()
	now := bl.clock.Now()
	list := append([]*backend(nil), bl.available...)
	bl.mu.Unlock()

	for range 5000 {
		a, b := bl.weightedTwoDraw(list, now)
		if a == b { // 需求 6：两个候选恰好同一台时必须重抽
			t.Fatal("weightedTwoDraw returned the same backend twice")
		}
	}

	// 只剩一台时直接用它，不报错。
	one := []*backend{list[0]}
	a, b := bl.weightedTwoDraw(one, now)
	if a != list[0] || b != list[0] {
		t.Fatal("single backend should be used directly")
	}
}

func TestWarmupRampsLinearly(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	cfg := testConfig()
	cfg.Clock = clk
	cfg.Warmup = 100 * time.Millisecond
	cfg.WarmupStartWeight = 0.05
	// 两台老后端在 t=0 建立并先把预热跑完。
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1, 1))
	clk.Advance(cfg.Warmup + time.Millisecond)

	// t=101ms 时加入第三台，它的 joinedAt 必须取这个 fakeClock 时刻。
	clk.Advance(50 * time.Millisecond)
	joinAt := clk.Now()
	bl.applyUpdate(discovery.Update{Backends: weightedBackends(1, 1, 1), Full: true})

	bl.mu.Lock()
	var fresh *backend
	for _, b := range bl.backends {
		if b.id == "bC" {
			fresh = b
		}
	}
	now := bl.clock.Now()
	bl.mu.Unlock()
	if fresh == nil {
		t.Fatal("new backend not registered")
	}
	if !fresh.joinedAt.Equal(joinAt) {
		t.Fatalf("joinedAt = %v, want %v", fresh.joinedAt, joinAt)
	}
	// 距加入时刻 0ms：系数应在起点 0.05。
	if f := fresh.warmupFactor(bl.cfg, now); abs(f-0.05) > 0.001 {
		t.Fatalf("initial warmup factor = %.3f, want 0.05", f)
	}
	// 推进到预热中点：0.05 + 0.95×0.5 = 0.525。
	clk.Advance(50 * time.Millisecond)
	if f := fresh.warmupFactor(bl.cfg, bl.clock.Now()); abs(f-0.525) > 0.01 {
		t.Fatalf("mid-warmup factor = %.3f, want 0.525", f)
	}

	clk.Advance(50 * time.Millisecond)
	if f := fresh.warmupFactor(bl.cfg, bl.clock.Now()); f != 1 {
		t.Fatalf("warmup should finish at factor 1, got %.3f", f)
	}
}

// waitForBackends 轮询直到 discovery 协程消费完最新 Set。
func (bl *Balancer) waitForBackends(want int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bl.mu.Lock()
		n := len(bl.available)
		bl.mu.Unlock()
		if n == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func TestP2CPrefersLowInflightAndLowLatency(t *testing.T) {
	cfg := testConfig()
	cfg.Rand = &seedRand{r: rand.New(rand.NewPCG(1, 2))}
	// 同权重，让打分差异只来自在途和延迟。
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1, 1))

	// bA 堆 5 个在途；bB 空闲。
	for range 5 {
		h, err := bl.Pick(context.Background())
		if err != nil || h.BackendID() != "bA" {
			// 随机种子不一定先命中 bA，手动构造状态。
			h.Done(Result{RTT: time.Millisecond})
		}
	}
	bl.mu.Lock()
	var a *backend
	for _, be := range bl.backends {
		if be.id == "bA" {
			a = be
		}
	}
	now := bl.clock.Now()
	bl.mu.Unlock()
	if a == nil {
		t.Fatal("backend bA missing")
	}
	// 确定性地制造差异：bA 在途高、估计延迟也高。
	a.inflight = 5
	a.est.Observe(200*time.Millisecond, now)

	choseB := 0
	const n = 2000
	for range n {
		h, err := bl.Pick(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if h.BackendID() == "bB" {
			choseB++
		}
		h.Done(Result{RTT: time.Millisecond})
	}
	// 两台都在候选池时 bB 分数必然更小；只有没抽到 bB 的局会选 bA。
	if choseB <= n/2 {
		t.Fatalf("healthy backend chosen %d/%d times, want strong majority", choseB, n)
	}
}

func TestSingleBackendUsedDirectly(t *testing.T) {
	cfg := testConfig()
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1))
	for range 10 {
		h, err := bl.Pick(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if h.BackendID() != "bA" {
			t.Fatalf("got %s, want bA", h.BackendID())
		}
		h.Done(Result{RTT: time.Millisecond})
	}
}

func TestNoBackendQueuesThenFailsWithinTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.QueueTimeout = 80 * time.Millisecond
	disc := discovery.NewMemoryDiscovery(nil)
	bl := New(disc, cfg)
	t.Cleanup(bl.Close)

	start := time.Now()
	_, err := bl.Pick(context.Background())
	waited := time.Since(start)
	if err != ErrNoBackend {
		t.Fatalf("err = %v, want ErrNoBackend", err)
	}
	if waited < cfg.QueueTimeout {
		t.Fatalf("failed after %v, should have queued ~%v", waited, cfg.QueueTimeout)
	}

	snap := bl.Snapshot()
	if snap.QueueTimeouts != 1 {
		t.Fatalf("queue timeouts = %d, want 1", snap.QueueTimeouts)
	}
}

func TestQueuedRequestWokenWhenBackendJoins(t *testing.T) {
	cfg := testConfig()
	cfg.QueueTimeout = 2 * time.Second
	disc := discovery.NewMemoryDiscovery(nil)
	bl := New(disc, cfg)
	t.Cleanup(bl.Close)

	type result struct {
		h   *Handle
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		h, err := bl.Pick(context.Background())
		resCh <- result{h, err}
	}()

	time.Sleep(30 * time.Millisecond) // 确保请求已入队
	disc.Set(weightedBackends(1))

	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("queued pick failed: %v", r.err)
		}
		if r.h.BackendID() != "bA" {
			t.Fatalf("got %s, want bA", r.h.BackendID())
		}
		r.h.Done(Result{RTT: time.Millisecond})
	case <-time.After(2 * time.Second):
		t.Fatal("queued request not woken after backend joined")
	}

	// 队列等待时长被记录且不超过 QueueTimeout。
	snap := bl.Snapshot()
	if snap.QueueWaitMax <= 0 || snap.QueueWaitMax > cfg.QueueTimeout {
		t.Fatalf("queue wait max = %v, want in (0, %v]", snap.QueueWaitMax, cfg.QueueTimeout)
	}
}

func TestInflightReleasedOnTimeoutAndCancel(t *testing.T) {
	cfg := testConfig()
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1))

	ctx, cancel := context.WithCancel(context.Background())
	h, err := bl.Pick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.BackendID() != "bA" {
		t.Fatalf("got %s, want bA", h.BackendID())
	}
	if h.b.timedOut != 0 {
		t.Fatalf("timed out counter before Done = %d, want 0", h.b.timedOut)
	}
	cancel()
	h.Done(Result{TimedOut: true}) // 超时/取消路径也要减在途（需求 8）

	snap := bl.Snapshot()
	if snap.TotalInFlight != 0 {
		t.Fatalf("inflight after timeout = %d, want 0", snap.TotalInFlight)
	}
	if h.b.timedOut != 1 {
		t.Fatalf("timed out counter = %d, want 1", h.b.timedOut)
	}
}

func TestDoubleDoneIsNoop(t *testing.T) {
	cfg := testConfig()
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1))
	h, err := bl.Pick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h.Done(Result{RTT: time.Millisecond})
	h.Done(Result{RTT: time.Millisecond}) // 重复回调不得把在途减成负数

	snap := bl.Snapshot()
	if snap.TotalInFlight != 0 {
		t.Fatalf("inflight = %d, want 0", snap.TotalInFlight)
	}
	if snap.TotalCompleted != 1 {
		t.Fatalf("completed = %d, want 1", snap.TotalCompleted)
	}
}

func TestSweeperReclaimsLeakedFlight(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	cfg := testConfig()
	cfg.Clock = clk
	cfg.FlightTimeout = 10 * time.Millisecond
	cfg.SweepInterval = time.Hour // 手动驱动 sweep，避免真实 ticker 干扰
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1))

	h, err := bl.Pick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// 故意不调用 Done，模拟回调丢失，推进时钟后兜底回收。
	clk.Advance(20 * time.Millisecond)
	bl.sweep(clk.Now())

	snap := bl.Snapshot()
	if snap.TotalInFlight != 0 {
		t.Fatalf("inflight after sweep = %d, want 0", snap.TotalInFlight)
	}
	if snap.TotalSwept != 1 {
		t.Fatalf("swept = %d, want 1", snap.TotalSwept)
	}
	// 兜底回收之后迟到的 Done 必须是空操作。
	h.Done(Result{RTT: time.Millisecond})
	if snap2 := bl.Snapshot(); snap2.TotalCompleted != 0 {
		t.Fatalf("late Done after sweep should be ignored, completed=%d", snap2.TotalCompleted)
	}
}

func TestRemovedBackendDroppedAfterInflightDrains(t *testing.T) {
	cfg := testConfig()
	disc := discovery.NewMemoryDiscovery(weightedBackends(1, 1))
	bl := New(disc, cfg)
	t.Cleanup(bl.Close)

	h, err := bl.Pick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	removed := h.BackendID()
	kept := "bA"
	if removed == "bA" {
		kept = "bB"
	}
	disc.Set([]discovery.Backend{{ID: kept, Weight: 1}}) // 只保留另一台
	bl.waitForBackends(1)

	bl.mu.Lock()
	_, stillThere := bl.backends[removed]
	avail := len(bl.available)
	bl.mu.Unlock()
	if !stillThere {
		t.Fatal("removed backend with inflight should stay in map until drained")
	}
	if avail != 1 {
		t.Fatalf("available = %d, want 1", avail)
	}
	h.Done(Result{RTT: time.Millisecond})
	bl.mu.Lock()
	_, stillThere = bl.backends[removed]
	bl.mu.Unlock()
	if stillThere {
		t.Fatal("removed backend should be deleted after inflight drains")
	}
}

func TestRemovedBackendReturnsWhileInflightDrained(t *testing.T) {
	// 回归的实例若在途未排空，其结构体仍留在 map 里；再次发现它时必须复位
	// removed 并重新可用，而不是被永久排除在 available 外。
	cfg := testConfig()
	disc := discovery.NewMemoryDiscovery(weightedBackends(1, 1))
	bl := New(disc, cfg)
	t.Cleanup(bl.Close)
	bl.waitForBackends(2)

	// 让两台都带上在途，摘除后结构体不会立刻从 map 删除。
	h1, err := bl.Pick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := bl.Pick(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	disc.Set(nil) // 全部摘除
	bl.waitForBackends(0)
	if got := len(bl.available); got != 0 {
		t.Fatalf("available after full removal = %d, want 0", got)
	}

	// 两台都重新回来（在途仍未排空），必须重新可用。
	disc.Set(weightedBackends(1, 1))
	bl.waitForBackends(2)
	bl.mu.Lock()
	allUnremoved := true
	for _, b := range bl.backends {
		if b.removed {
			allUnremoved = false
		}
	}
	bl.mu.Unlock()
	if !allUnremoved {
		t.Fatal("returned backends still carry removed=true")
	}

	h1.Done(Result{RTT: time.Millisecond})
	h2.Done(Result{RTT: time.Millisecond})
}

func TestUtilizationFeedsScore(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	cfg := testConfig()
	cfg.Clock = clk
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1, 1))

	// 两台延迟、在途完全相同，只有利用率不同。
	bl.mu.Lock()
	ids := make([]*backend, 0, 2)
	for _, b := range bl.backends {
		ids = append(ids, b)
	}
	now := bl.clock.Now()
	bl.mu.Unlock()
	if len(ids) != 2 {
		t.Fatalf("backends = %d, want 2", len(ids))
	}
	low, high := ids[0], ids[1]
	high.util = 0.9
	low.util = 0.1
	if low.score(bl.cfg, now) >= high.score(bl.cfg, now) {
		t.Fatal("lower utilization backend should have lower score")
	}
}

func TestStrategyBasics(t *testing.T) {
	for _, st := range []Strategy{StrategyRoundRobin, StrategyRandom, StrategyInflightP2C} {
		cfg := testConfig()
		cfg.Strategy = st
		cfg.Rand = &seedRand{r: rand.New(rand.NewPCG(9, 9))}
		bl, _ := newTestBalancer(t, cfg, weightedBackends(1, 1, 1))
		got := map[string]int{}
		for range 300 {
			h, err := bl.Pick(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got[h.BackendID()]++
			h.Done(Result{RTT: time.Millisecond})
		}
		if len(got) != 3 {
			t.Fatalf("strategy %s did not use all backends: %v", st, got)
		}
		if st == StrategyRoundRobin {
			ids := make([]string, 0, len(got))
			for id := range got {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				if got[id] != 100 {
					t.Fatalf("round robin uneven: %v", got)
				}
			}
		}
	}
}

func TestConcurrentPickDoesNotCorruptState(t *testing.T) {
	cfg := testConfig()
	cfg.Rand = &seedRand{r: rand.New(rand.NewPCG(3, 4))}
	bl, _ := newTestBalancer(t, cfg, weightedBackends(1, 2, 3))

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				h, err := bl.Pick(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				h.Done(Result{RTT: time.Millisecond, Utilization: 0.2})
			}
		}()
	}
	wg.Wait()
	snap := bl.Snapshot()
	if snap.TotalInFlight != 0 {
		t.Fatalf("inflight leaked: %d", snap.TotalInFlight)
	}
	if snap.TotalCompleted != 16*500 {
		t.Fatalf("completed = %d, want %d", snap.TotalCompleted, 16*500)
	}
	if bl.flights.Len() != 0 {
		t.Fatalf("flight heap not empty: %d", bl.flights.Len())
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
