package lb

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time          { return f.t }
func (f *fakeClock) Advance(d time.Duration) { f.t = f.t.Add(d) }

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Warmup = time.Nanosecond // 大多数测试不需要预热干扰
	cfg.WarmupStartWeight = 1
	cfg.IdleRevert = time.Second
	cfg.IdleRevertTime = 2 * time.Second
	cfg.QueueTimeout = 50 * time.Millisecond
	return cfg
}

func TestEstimatorFastRaiseOnSlowdown(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	cfg := testConfig()
	e := newEstimator(cfg, clk.t)

	// 初始值为中性值。
	if got := e.Estimate(clk.t); got != cfg.NeutralLatency {
		t.Fatalf("initial estimate = %v, want %v", got, cfg.NeutralLatency)
	}

	// 慢样本立即抬升（需求 3）。
	slow := 200 * time.Millisecond
	e.Observe(slow, clk.t)
	if got := e.Estimate(clk.t); got != slow {
		t.Fatalf("after slow sample estimate = %v, want %v", got, slow)
	}

	// 快样本只缓慢下拉。
	fast := 10 * time.Millisecond
	clk.Advance(time.Millisecond)
	e.Observe(fast, clk.t)
	got := e.Estimate(clk.t)
	want := time.Duration(float64(slow)*(1-cfg.DecayAlpha) + float64(fast)*cfg.DecayAlpha)
	if got != want {
		t.Fatalf("after fast sample estimate = %v, want %v", got, want)
	}
	if got <= fast {
		t.Fatalf("estimate should decay slowly, got %v", got)
	}
}

func TestEstimatorIdleRevertsToNeutral(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	cfg := testConfig()
	e := newEstimator(cfg, clk.t)

	e.Observe(300*time.Millisecond, clk.t)
	// 空闲窗口内保持不变。
	clk.Advance(cfg.IdleRevert)
	if got := e.Estimate(clk.t); got != 300*time.Millisecond {
		t.Fatalf("estimate changed within idle window: %v", got)
	}
	// 回归过程中单调向中性值靠拢（需求 4）。
	clk.Advance(cfg.IdleRevertTime / 2)
	mid := e.Estimate(clk.t)
	if mid <= cfg.NeutralLatency || mid >= 300*time.Millisecond {
		t.Fatalf("mid revert estimate = %v, expected between neutral and slow value", mid)
	}
	// 足够久后回到中性值。
	clk.Advance(cfg.IdleRevertTime)
	if got := e.Estimate(clk.t); got != cfg.NeutralLatency {
		t.Fatalf("after long idle estimate = %v, want neutral %v", got, cfg.NeutralLatency)
	}
}

func TestEstimatorNewSampleResetsIdle(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	cfg := testConfig()
	e := newEstimator(cfg, clk.t)
	e.Observe(300*time.Millisecond, clk.t)
	clk.Advance(cfg.IdleRevert + cfg.IdleRevertTime/2)
	clk.Advance(time.Millisecond)
	e.Observe(50*time.Millisecond, clk.t) // 新样本刷新 lastSample，回归重新计时
	if got := e.Estimate(clk.t); got < 50*time.Millisecond || got > 300*time.Millisecond {
		t.Fatalf("unexpected estimate after fresh sample: %v", got)
	}
}

// TestEstimatorSlowUpFastDown 对照需求 3：变慢一步抬到位，变快多步缓降。
func TestEstimatorSlowUpFastDown(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	cfg := testConfig()
	e := newEstimator(cfg, clk.t)

	// 连续慢样本：每一次都立即抬升，没有滞后。
	e.Observe(100*time.Millisecond, clk.t)
	if got := e.Estimate(clk.t); got != 100*time.Millisecond {
		t.Fatalf("first slow sample = %v, want immediate 100ms", got)
	}
	clk.Advance(time.Millisecond)
	e.Observe(250*time.Millisecond, clk.t)
	if got := e.Estimate(clk.t); got != 250*time.Millisecond {
		t.Fatalf("second slow sample = %v, want immediate 250ms", got)
	}

	// 连续快样本：估计逐步下降，不会一步跌到底。
	prev := e.Estimate(clk.t)
	for i := 0; i < 5; i++ {
		clk.Advance(time.Millisecond)
		e.Observe(5*time.Millisecond, clk.t)
		got := e.Estimate(clk.t)
		if got >= prev {
			t.Fatalf("fast samples should pull estimate down: %v -> %v", prev, got)
		}
		prev = got
	}
	if prev <= 5*time.Millisecond {
		t.Fatalf("estimate should decay gradually, reached floor too fast: %v", prev)
	}
}
