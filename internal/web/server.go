package web

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"p2clb/internal/sim"
	"p2clb/pkg/lb"
)

// Server 持有一个常驻模拟（serve 模式）并对外提供快照与基准接口。
type Server struct {
	bal    *lb.Balancer
	runner *sim.Runner
	cfg    sim.Config
}

func NewServer(bal *lb.Balancer, runner *sim.Runner, cfg sim.Config) *Server {
	return &Server{bal: bal, runner: runner, cfg: cfg}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", Handler())
	mux.HandleFunc("/api/snapshot", s.handleSnapshot)
	mux.HandleFunc("/api/benchmark", s.handleBenchmark)
	return mux
}

func (s *Server) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	snap := s.bal.Snapshot()
	// 模拟服务端视角的真实在途，补充调用方本地在途之外的对照。
	type row struct {
		lb.BackendStat
		ServerInflight int64 `json:"server_inflight"`
	}
	out := snap
	out.Backends = nil
	resp := struct {
		lb.Snapshot
		ServerBackends []row `json:"server_backends"`
	}{Snapshot: out}
	for _, b := range snap.Backends {
		resp.ServerBackends = append(resp.ServerBackends, row{
			BackendStat:    b,
			ServerInflight: s.runner.BackendActive(b.ID),
		})
	}
	// 前端直接读 Backends，这里把增强行回填成兼容结构。
	resp.Snapshot.Backends = make([]lb.BackendStat, len(resp.ServerBackends))
	for i, r := range resp.ServerBackends {
		resp.Snapshot.Backends[i] = r.BackendStat
	}
	writeJSON(w, resp)
}

func (s *Server) handleBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	cfg := s.cfg
	cfg.Duration = 20 * time.Second // 页面触发的短基准
	cfg.WarmupRun = 3 * time.Second

	res, err := sim.RunBenchmark(r.Context(), cfg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// 同时补一次健康稳态权重校验（专用低压力工况）。
	wc, _ := sim.RunWeightProportionalCheck(context.Background(), cfg)
	res.Checks.WeightProportional = wc

	writeJSON(w, map[string]any{
		"table":  sim.FormatTable(res),
		"checks": res.Checks,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
