package sim

import (
	"context"
	"testing"
	"time"

	"p2clb/pkg/lb"
)

// TestWeightProportionalHealthySteadyState 验证健康、容量充足的稳态下，
// P2C 各权重档的请求量与权重成正比。
func TestWeightProportionalHealthySteadyState(t *testing.T) {
	if testing.Short() {
		t.Skip("稳态权重校验需要约 25s")
	}
	cfg := DefaultConfigForCLI()
	cfg.Seed = 20261124
	cfg.Callers = 100
	cfg.Backends = 30
	cfg.Duration = 25 * time.Second
	wc, err := RunWeightProportionalCheck(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !wc.Pass {
		t.Fatalf("weight proportional check failed, max grouped deviation %.1f%% (tol %.0f%%)",
			wc.MaxDev*100, WeightTolerance*100)
	}
}

// TestQueueNeverExceedsTimeout 验证排队等待从不超过配置上限。
func TestQueueNeverExceedsTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	cfg := DefaultConfigForCLI()
	cfg.Seed = 4242
	cfg.Callers = 80
	cfg.Backends = 16
	cfg.Duration = 8 * time.Second
	cfg.WarmupRun = time.Second
	// 缩容下限很小，强制制造无可用后端/高压排队场景。
	cfg.MinBackends = 2
	cfg.MaxBackends = cfg.Backends * 2
	r := NewRunner(cfg)
	rep := r.Run(context.Background(), lb.StrategyP2C)
	qc := checkQueueBound(cfg, rep)
	if !qc.Pass {
		t.Fatalf("queue wait %.2fms exceeded limit %.2fms", qc.QueueMaxMS, qc.LimitMS)
	}
}

// TestNoInflightLeakAfterRun 验证一轮模拟结束后在途全部回收。
func TestNoInflightLeakAfterRun(t *testing.T) {
	cfg := DefaultConfigForCLI()
	cfg.Seed = 777
	cfg.Callers = 30
	cfg.Backends = 10
	cfg.Duration = 3 * time.Second
	cfg.WarmupRun = 0
	leak := CheckInflightLeak(context.Background(), cfg)
	if !leak.Pass {
		t.Fatalf("inflight leaked: leftover=%d swept=%d", leak.Leftover, leak.Swept)
	}
}

// TestAllStrategiesCompleteWithoutError 四种策略都应跑完整轮并产出报告。
func TestAllStrategiesCompleteWithoutError(t *testing.T) {
	cfg := DefaultConfigForCLI()
	cfg.Seed = 99
	cfg.Callers = 20
	cfg.Backends = 8
	cfg.Duration = 4 * time.Second
	cfg.WarmupRun = 0
	for _, st := range AllStrategies {
		r := NewRunner(cfg)
		rep := r.Run(context.Background(), st)
		if rep.Completed == 0 {
			t.Fatalf("strategy %s completed no requests", st)
		}
	}
}
