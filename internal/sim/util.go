package sim

import (
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
)

// randStub 把 math/rand/v2 的 *rand.Rand 适配成 lb.Rand 接口（单协程使用）。
type randStub struct{ r *rand.Rand }

func (s *randStub) IntN(n int) int {
	if n <= 0 {
		return 0
	}
	return s.r.IntN(n)
}
func (s *randStub) Float64() float64 { return s.r.Float64() }

// safeRand 是并发安全的随机源。math/rand/v2 的 *Rand 不能被多 goroutine
// 并发使用，而后端的 Handle 会被多个请求 goroutine 同时进入，因此加锁保护。
type safeRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func newSafeRand(r *rand.Rand) *safeRand { return &safeRand{r: r} }

func (s *safeRand) Float64() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.r.Float64()
}

// sortByIndex 按 backend-N 的序号排序，保证扩缩容顺序确定。
func sortByIndex(ids []string) {
	sort.Slice(ids, func(i, j int) bool { return indexOf(ids[i]) < indexOf(ids[j]) })
}

func indexOf(id string) int {
	n, err := strconv.Atoi(id[len("backend-"):])
	if err != nil {
		return 1 << 30
	}
	return n
}
