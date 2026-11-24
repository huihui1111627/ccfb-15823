package discovery

import (
	"context"
	"sync"
)

// MemoryDiscovery 是进程内的服务发现实现，用于测试和模拟。
// 每次 Set 都会向所有观察者广播一份全量快照。
type MemoryDiscovery struct {
	mu       sync.Mutex
	current  []Backend
	watchers map[chan Update]struct{}
}

func NewMemoryDiscovery(initial []Backend) *MemoryDiscovery {
	cp := append([]Backend(nil), initial...)
	return &MemoryDiscovery{
		current:  cp,
		watchers: make(map[chan Update]struct{}),
	}
}

// Set 替换当前后端集合并广播全量快照。
func (m *MemoryDiscovery) Set(backends []Backend) {
	cp := append([]Backend(nil), backends...)

	m.mu.Lock()
	m.current = cp
	watchers := make([]chan Update, 0, len(m.watchers))
	for ch := range m.watchers {
		watchers = append(watchers, ch)
	}
	m.mu.Unlock()

	u := Update{Backends: append([]Backend(nil), cp...), Full: true}
	for _, ch := range watchers {
		select {
		case ch <- u:
		default:
			// 观察者通道满了就丢弃这次变更；它的下一次读取会通过
			// Snapshot 拿到最新状态（见 Watch 初始化逻辑）。
			go m.deliverLatest(ch)
		}
	}
}

func (m *MemoryDiscovery) deliverLatest(ch chan Update) {
	m.mu.Lock()
	snap := append([]Backend(nil), m.current...)
	m.mu.Unlock()
	select {
	case ch <- Update{Backends: snap, Full: true}:
	default:
	}
}

// Snapshot 返回当前后端集合的副本。
func (m *MemoryDiscovery) Snapshot() []Backend {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Backend(nil), m.current...)
}

func (m *MemoryDiscovery) Watch(ctx context.Context) (<-chan Update, error) {
	ch := make(chan Update, 8)

	m.mu.Lock()
	m.watchers[ch] = struct{}{}
	snap := append([]Backend(nil), m.current...)
	m.mu.Unlock()

	// 先投递当前全量快照，保证 Balancer 启动即有数据。
	ch <- Update{Backends: snap, Full: true}

	go func() {
		<-ctx.Done()
		m.mu.Lock()
		delete(m.watchers, ch)
		m.mu.Unlock()
	}()
	return ch, nil
}
