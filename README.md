# p2clb — 服务间调用的客户端负载均衡库

模拟蜂场场景：几百个调用方、几百个后端实例，后端会随机扩缩容、变慢或间歇卡顿。
库本身（`pkg/lb`）不依赖模拟器，可独立用于真实服务；`internal/sim` 在进程内模拟
调用方、后端和网络，`internal/web` 提供流量分布页面。

## 快速开始

```bash
# 跑全部单元测试（含 -race）
go test ./... -race

# 四策略对比基准 + 三条正确性校验（固定随机种子，可复现）
go run ./cmd/p2csim bench

# 高压力工况，尾延迟差距更明显
go run ./cmd/p2csim bench -callers 300 -backends 60 -rps 40 -concurrency 6 -duration 20s

# 只跑健康稳态的权重正比专项校验
go run ./cmd/p2csim bench -healthy-only

# 启动流量分布页面（默认 http://localhost:8080，每秒刷新）
go run ./cmd/p2csim serve
```

## 目录结构

```
pkg/discovery/   服务发现接口 + 进程内实现（权重/扩缩容事件）
pkg/lb/          负载均衡库：P2C、延迟估计、预热、排队、在途兜底、指标
internal/sim/    调用方/后端/网络/故障脚本模拟器，四策略对比与正确性校验
internal/web/    流量分布页面（go:embed 单文件，无前端构建依赖）
cmd/p2csim/      serve / bench 两个子命令
```

## 需求与实现对照

| # | 需求 | 实现位置 |
|---|------|----------|
| 1 | 按权重随机抽两个，抽中概率与权重成正比 | `balancer.go: weightedTwoDraw/weightedPick`，单测 `TestWeightedTwoDrawProbabilityProportionalToWeight` |
| 2 | 两候选选 `延迟估计×(在途+1)` 更小者，融入利用率 | `backend.go: scoreAt`，因子 `(1+UtilWeight×util)` |
| 3 | 慢样本立即抬升、快样本 EWMA 缓降 | `estimator.go: Observe`，单测 `TestEstimatorFastRaiseOnSlowdown` / `TestEstimatorSlowUpFastDown` |
| 4 | 长期无流量估计值回归中性值 | `estimator.go: Estimate` 空闲线性回归，单测 `TestEstimatorIdleRevertsToNeutral` |
| 5 | 新后端小份额起步、线性升到正常份额 | `backend.go: warmupFactor/effectiveWeight`，单测 `TestWarmupRampsLinearly` |
| 6 | 两候选相同重抽；只剩一台直接用；零台排队等而非立即失败 | `weightedTwoDraw`、`selectLocked`、`Pick` 队列，单测 `TestWeightedTwoDrawReturnsDistinctBackends` / `TestSingleBackendUsedDirectly` / `TestEndToEndQueueWhenNoBackends` |
| 7 | 后端响应带利用率，调用方纳入打分 | `Result.Utilization` → `backend.observeUtil` → `scoreAt`，单测 `TestUtilizationFeedsScore` |
| 8 | 本地维护在途，取消/超时及时减回，不因漏回调偏高 | `finish` 立即回收 + `sweepLoop` 超时兜底 + 幂等 `Done`，单测 `TestInflightReleasedOnTimeoutAndCancel` / `TestDoubleDoneIsNoop` / `TestSweeperReclaimsLeakedFlight` |

页面四块：① 各后端请求量与在途数；② 延迟估计（含排队等待、无后端失败数）；
③ 选中次数与权重对照（含利用率）；④ 四策略对比基准按钮。

## 核心设计

### 两阶段选择（P2C, power of two choices）

1. **加权抽选**：按 `weight × warmupFactor` 的有效权重抽两个**不同**实例。
   抽选概率严格正比于有效权重——这是「请求量与权重成正比」的保证来源。
2. **打分二选一**：`score = 延迟估计 × (在途+1) × (1+UtilWeight×利用率)`，取小者。

第二阶段是 P2C 抑制尾延迟的关键：变慢的实例延迟估计立刻抬高、堆了请求的实例
在途变大，两者都会让它在候选对决中落败，流量在它恢复前自然绕开。

### 为什么权重正比要在「健康稳态」下验收

P2C 的第二阶段在容量吃紧时会**有意**偏离权重：高权重实例承接的在途更多，
对决中更容易输给在途为 0 的低权重实例，于是主动让出一部分流量。这是降低
尾延迟的正确行为，不是缺陷。因此权重正比的验收放在容量充足（在途普遍 0~1、
第二阶段近似不扰动）的专用工况，按**权重档聚合**统计；加权抽选本身的概率
正比则由 `pkg/lb` 单测在任意时刻独立保证。

### 在途回收的三道防线

1. 正常路径：`Handle.Done` 在请求结束（含取消、超时）时立即减在途、删堆记录。
2. 幂等保护：`done` 用 CAS 保证重复/迟到回调是空操作，不会把在途减成负数。
3. 兜底扫描：`sweepLoop` 周期性清理超过 `FlightTimeout` 仍未 Done 的记录，
   应对 goroutine 卡死、漏写 defer 等极端情况。被兜底回收的数量在页面可见，
   健康系统里应恒为 0。

### 排队语义

没有可用后端时请求进入 FIFO 队列，最长等 `QueueTimeout`：
- 新后端加入 → `pumpWaitersLocked` 唤醒队头，重新走选择；
- 等待超时 → 返回 `ErrNoBackend`，计入「无后端失败」；
- 每次选择前剪枝过期 waiter，保证「排队中」指标只反映真实等待数。

## 模拟器的公平性

四种策略跑同一份确定性故障/扩缩容脚本（`script.go`），随机数按
`(seed, streamID)` 派生独立流，因此除调度决策外的所有随机性完全一致。
对比策略：`p2c` / `round_robin` / `random` / `inflight_p2c`（两次随机只看在途）。

## 典型基准结果（seed=20261124，300 调用方 / 60 后端，高压力）

```
策略            完成数     超时    服务端排队   P99ms    P99.9ms
p2c            201757     195      4744       21.68     42.35
round_robin    180059    1711      6709       42.35    103.40
random         179626    1521      7496       66.17    103.40
inflight_p2c   202528      58         0       27.10     42.35
```

P2C 类策略（p2c、inflight_p2c）的尾延迟和超时显著优于轮询/纯随机；
完整版 p2c 因额外考虑延迟估计与利用率，在故障实例上的避让更果断。
具体数字随参数变化，用固定种子可随时复现。
