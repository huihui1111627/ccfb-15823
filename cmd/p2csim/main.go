package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"p2clb/internal/sim"
	"p2clb/internal/web"
	"p2clb/pkg/lb"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "bench":
		runBench(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `p2csim — 客户端负载均衡模拟器

用法:
  p2csim serve [flags]   启动持续模拟和流量分布页面
  p2csim bench [flags]   四策略对比基准 + 正确性校验

通用 flags:
  -seed       随机种子（默认 20261124）
  -callers    调用方数量（默认 200）
  -backends   后端数量（默认 100）
  -rps        每调用方每秒请求数（默认 25）
  -duration   模拟时长（默认 serve=60s bench=30s）
  -queue-to   无可用后端时排队上限（默认 2s）
`)
}

func baseFlags(fs *flag.FlagSet) (*sim.Config, *uint64) {
	cfg := sim.DefaultConfigForCLI()
	seed := fs.Uint64("seed", 20261124, "固定随机种子")
	fs.IntVar(&cfg.Callers, "callers", 200, "调用方数量")
	fs.IntVar(&cfg.Backends, "backends", 100, "后端数量")
	fs.IntVar(&cfg.RPSPerCaller, "rps", 25, "每调用方每秒请求数")
	fs.DurationVar(&cfg.QueueTimeout, "queue-to", 2*time.Second, "排队上限")
	fs.IntVar(&cfg.Concurrency, "concurrency", 8, "单后端并发容量")
	return &cfg, seed
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	cfg, seed := baseFlags(fs)
	addr := fs.String("addr", ":8080", "页面监听地址")
	fs.DurationVar(&cfg.Duration, "duration", 60*time.Second, "单轮模拟时长（页面持续刷新）")
	_ = fs.Parse(args)
	cfg.Seed = *seed

	runner := sim.NewRunner(*cfg)
	balCfg := runner.BalancerConfig(lb.StrategyP2C)
	bal := lb.New(runner.Discovery(), balCfg)

	// 常驻模拟在后台跑；到点后重启一轮，Balancer 复用以保持指标连续。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go loopRun(ctx, runner, bal)

	srv := web.NewServer(bal, runner, *cfg)
	log.Printf("流量分布页面: http://localhost%s", *addr)
	if err := http.ListenAndServe(*addr, srv.Routes()); err != nil {
		log.Fatal(err)
	}
}

// loopRun 让常驻 Runner 在同一个 Balancer 上反复执行模拟。
func loopRun(ctx context.Context, runner *sim.Runner, bal *lb.Balancer) {
	for {
		runner.RunWithBalancer(ctx, bal)
		if ctx.Err() != nil {
			return
		}
	}
}

func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	cfg, seed := baseFlags(fs)
	fs.DurationVar(&cfg.Duration, "duration", 30*time.Second, "每策略模拟时长")
	healthyOnly := fs.Bool("healthy-only", false, "只跑健康稳态权重校验（无故障/扩缩容）")
	_ = fs.Parse(args)
	cfg.Seed = *seed
	cfg.WarmupRun = 3 * time.Second

	ctx := context.Background()

	if *healthyOnly {
		c := *cfg
		c.MinBackends = c.Backends
		c.MaxBackends = c.Backends
		wc, err := sim.RunWeightProportionalCheck(ctx, c)
		if err != nil {
			log.Fatal(err)
		}
		printWeightCheck(wc)
		if !wc.Pass {
			os.Exit(1)
		}
		return
	}

	res, err := sim.RunBenchmark(ctx, *cfg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(sim.FormatTable(res))

	// 专项健康稳态校验：专用低压力工况，避免故障/扩缩容和容量压力干扰权重结论。
	hc := *cfg
	wc, err := sim.RunWeightProportionalCheck(ctx, hc)
	if err != nil {
		log.Fatal(err)
	}
	res.Checks.WeightProportional = wc

	fmt.Println("\n正确性校验：")
	fmt.Printf("  权重正比（健康稳态，最大相对偏差 %.1f%%，容忍 %.0f%%）：%s\n",
		wc.MaxDev*100, sim.WeightTolerance*100, pass(wc.Pass))
	fmt.Printf("  排队不超过上限（max %.2fms / 上限 %.2fms）：%s\n",
		res.Checks.QueueBound.QueueMaxMS, res.Checks.QueueBound.LimitMS,
		pass(res.Checks.QueueBound.Pass))

	// 在途泄漏校验：再跑一轮并在结束后检查 Balancer 快照。
	leak := sim.CheckInflightLeak(ctx, *cfg)
	fmt.Printf("  在途无泄漏（结束后残留总数 %d，兜底回收 %d）：%s\n",
		leak.Leftover, leak.Swept, pass(leak.Pass))

	if !wc.Pass || !res.Checks.QueueBound.Pass || !leak.Pass {
		os.Exit(1)
	}
	fmt.Println("\n全部校验通过 ✅")
}

func printWeightCheck(wc sim.WeightCheck) {
	fmt.Printf("健康稳态权重正比校验（最大相对偏差 %.2f%%，容忍 %.0f%%）\n\n",
		wc.MaxDev*100, sim.WeightTolerance*100)
	fmt.Printf("%-14s %8s %12s %12s %12s\n", "后端", "权重", "请求数", "期望份额", "实测份额")
	for _, r := range wc.Details {
		fmt.Printf("%-14s %8.1f %12d %11.2f%% %11.2f%%\n",
			r.ID, r.Weight, r.Picked, r.WantShare*100, r.GotShare*100)
	}
	fmt.Println("\n结果:", pass(wc.Pass))
}

func pass(ok bool) string {
	if ok {
		return "通过 ✅"
	}
	return "失败 ❌"
}
