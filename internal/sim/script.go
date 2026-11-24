package sim

import (
	"sort"
	"time"
)

// Op 是脚本对模拟世界施加的一个操作。
type Op struct {
	At       time.Duration // 相对模拟开始的时间
	Kind     opKind
	TargetID string // 针对单个后端的操作
	SlowTo   time.Duration
	Stutter  time.Duration
	// 扩缩容：把后端总数调整到 Count（按 ID 顺序增删）。
	Count int
}

type opKind int

const (
	opSlow opKind = iota
	opRecover
	opStutterOn
	opStutterOff
	opScale
)

// BuildScript 生成一次模拟的确定性事件脚本。
// 同一份脚本会喂给所有策略，保证对比的公平性。
func BuildScript(cfg Config) []Op {
	var ops []Op
	d := cfg.Duration

	// 1) 一小批后端在 15% 处变慢 5 倍，60% 处恢复——考验尾延迟避让。
	slowCount := cfg.Backends / 10
	if slowCount < 1 {
		slowCount = 1
	}
	for i := range slowCount {
		id := backendID(i)
		ops = append(ops,
			Op{At: d / 7, Kind: opSlow, TargetID: id, SlowTo: cfg.BaseLatency * 5},
			Op{At: d * 6 / 10, Kind: opRecover, TargetID: id},
		)
	}

	// 2) 另一小批后端间歇卡顿：每 2s 开 800ms 关 1.2s。
	stutterCount := cfg.Backends / 20
	if stutterCount < 1 {
		stutterCount = 1
	}
	for i := slowCount; i < slowCount+stutterCount; i++ {
		id := backendID(i)
		for at := d / 5; at < d*9/10; at += 2 * time.Second {
			ops = append(ops,
				Op{At: at, Kind: opStutterOn, TargetID: id, Stutter: cfg.BaseLatency * 8},
				Op{At: at + 800*time.Millisecond, Kind: opStutterOff, TargetID: id},
			)
		}
	}

	// 3) 周期性扩缩容，触发预热和排队路径。
	scalePoints := []struct {
		at    time.Duration
		count int
	}{
		{d / 6, cfg.Backends * 3 / 2},
		{d / 3, cfg.Backends * 3 / 4},
		{d / 2, cfg.Backends},
		{d * 2 / 3, cfg.MinBackends}, // 缩到最小，制造压力
		{d * 5 / 6, cfg.Backends},
	}
	for _, p := range scalePoints {
		count := p.count
		if count < cfg.MinBackends {
			count = cfg.MinBackends
		}
		if count > cfg.MaxBackends {
			count = cfg.MaxBackends
		}
		ops = append(ops, Op{At: p.at, Kind: opScale, Count: count})
	}

	sort.SliceStable(ops, func(i, j int) bool { return ops[i].At < ops[j].At })
	return ops
}

func backendID(i int) string {
	return "backend-" + itoa(i)
}

// itoa 避免引入 strconv 造成的小分歧；后端数量有限，简单实现即可。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [12]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
