package sim

import (
	"sync"
	"time"

	"p2clb/pkg/lb"
)

// Metrics 汇总一次模拟运行的全局指标。
type Metrics struct {
	mu sync.Mutex

	latency   *lb.Histogram
	queueWait *lb.Histogram

	completed int64
	timedOut  int64
	canceled  int64
	noBackend int64 // 排队超时导致的失败
	serverQ   int64 // 在后端信号量上排过队的请求
	queueMax  time.Duration

	// 每个后端最终承接的请求数，用于权重正比校验。
	perBackend map[string]int64
}

func newMetrics(backendIDs []string) *Metrics {
	m := &Metrics{
		latency:    lb.NewHistogram(200*time.Microsecond, 10*time.Second, 1.25),
		queueWait:  lb.NewHistogram(50*time.Microsecond, 5*time.Second, 1.25),
		perBackend: make(map[string]int64, len(backendIDs)),
	}
	for _, id := range backendIDs {
		m.perBackend[id] = 0
	}
	return m
}

func (m *Metrics) recordSuccess(id string, rtt, wait time.Duration, serverQueued bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed++
	m.latency.Observe(rtt)
	m.perBackend[id]++
	if serverQueued {
		m.serverQ++
	}
	m.recordQueueWaitLocked(wait)
}

func (m *Metrics) recordFailure(wait time.Duration, kind string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch kind {
	case "timeout":
		m.timedOut++
	case "canceled":
		m.canceled++
	case "no_backend":
		m.noBackend++
	}
	m.recordQueueWaitLocked(wait)
}

func (m *Metrics) recordQueueWaitLocked(wait time.Duration) {
	if wait <= 0 {
		return
	}
	m.queueWait.Observe(wait)
	if wait > m.queueMax {
		m.queueMax = wait
	}
}

// addBackend 给扩容新出现的后端建计数桶。
func (m *Metrics) addBackend(id string) {
	m.mu.Lock()
	if _, ok := m.perBackend[id]; !ok {
		m.perBackend[id] = 0
	}
	m.mu.Unlock()
}

// Report 是一次运行的最终报告。
type Report struct {
	Strategy string

	Completed int64
	TimedOut  int64
	Canceled  int64
	NoBackend int64
	ServerQ   int64

	LatencyMS   lb.LatencyReport
	QueueWaitMS lb.LatencyReport
	QueueMaxMS  float64
	PerBackend  map[string]int64
}

func (m *Metrics) report(strategy string) Report {
	m.mu.Lock()
	defer m.mu.Unlock()
	pb := make(map[string]int64, len(m.perBackend))
	for k, v := range m.perBackend {
		pb[k] = v
	}
	return Report{
		Strategy:    strategy,
		Completed:   m.completed,
		TimedOut:    m.timedOut,
		Canceled:    m.canceled,
		NoBackend:   m.noBackend,
		ServerQ:     m.serverQ,
		LatencyMS:   lb.ExportReport(m.latency),
		QueueWaitMS: lb.ExportReport(m.queueWait),
		QueueMaxMS:  float64(m.queueMax.Microseconds()) / 1000.0,
		PerBackend:  pb,
	}
}
