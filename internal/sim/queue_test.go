package sim

import (
	"context"
	"testing"
	"time"

	"p2clb/pkg/discovery"
	"p2clb/pkg/lb"
)

// TestEndToEndQueueWhenNoBackends 验证全部后端摘除时请求排队、
// 后端回来后被唤醒，且等待时长不超过 QueueTimeout。
func TestEndToEndQueueWhenNoBackends(t *testing.T) {
	cfg := DefaultConfigForCLI()
	cfg.Seed = 555
	cfg.Callers = 10
	cfg.Backends = 4
	cfg.Duration = 4 * time.Second
	cfg.WarmupRun = 0
	cfg.QueueTimeout = 500 * time.Millisecond

	r := NewRunner(cfg)
	r.script = nil // 关掉脚本扩缩容，手动控制后端集合
	bal := lb.New(r.Discovery(), r.BalancerConfig(lb.StrategyP2C))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.runWithBalancer(ctx, bal); close(done) }()

	time.Sleep(500 * time.Millisecond)
	// 全部摘除：新请求必须排队而不是立即失败。
	r.Discovery().Set(nil)
	time.Sleep(150 * time.Millisecond) // 短于 500ms 超时，请求应仍在排队
	snap := bal.Snapshot()
	if snap.AvailableCount != 0 {
		t.Fatalf("expected 0 available backends, got %d", snap.AvailableCount)
	}
	if snap.QueuedNow == 0 {
		t.Fatalf("expected queued requests with no backends, got %d", snap.QueuedNow)
	}
	// 后端回来：排队请求应被唤醒。
	r.Discovery().Set([]discovery.Backend{
		{ID: "backend-0", Weight: 1},
		{ID: "backend-1", Weight: 2},
	})
	time.Sleep(400 * time.Millisecond)
	snap = bal.Snapshot()
	if snap.QueuedNow != 0 {
		t.Fatalf("queue should drain after backends return, got %d waiting", snap.QueuedNow)
	}
	if snap.QueueWaitMax > cfg.QueueTimeout {
		t.Fatalf("queue wait %v exceeded timeout %v", snap.QueueWaitMax, cfg.QueueTimeout)
	}

	cancel()
	<-done
	bal.Close()
}

// TestQueueTimeoutProducesNoBackendFailure 验证排队超时计入 NoBackend 指标。
func TestQueueTimeoutProducesNoBackendFailure(t *testing.T) {
	cfg := DefaultConfigForCLI()
	cfg.Seed = 888
	cfg.Callers = 8
	cfg.Backends = 3
	cfg.Duration = 3 * time.Second
	cfg.WarmupRun = 0
	cfg.QueueTimeout = 200 * time.Millisecond

	r := NewRunner(cfg)
	r.script = nil // 关掉脚本扩缩容，保证后端在本轮内不会回来
	bal := lb.New(r.Discovery(), r.BalancerConfig(lb.StrategyP2C))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.runWithBalancer(ctx, bal); close(done) }()

	time.Sleep(400 * time.Millisecond)
	r.Discovery().Set(nil) // 后端全没，且在本轮内不回来
	time.Sleep(1500 * time.Millisecond)

	snap := bal.Snapshot()
	if snap.QueueTimeouts == 0 {
		t.Fatal("expected queue timeouts with no backends")
	}
	cancel()
	<-done
	bal.Close()
}
