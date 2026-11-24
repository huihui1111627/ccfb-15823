// Package discovery 定义服务发现抽象。
//
// 库本身不绑定任何注册中心：调用方只要提供一个实现了 Discovery 接口的
// 数据源即可。模拟器里使用 MemoryDiscovery 产生权重变更和扩缩容事件。
package discovery

import "context"

// Backend 是一个后端实例的发现信息。
type Backend struct {
	ID     string
	Weight float64
	Meta   map[string]string
}

// Update 是一次发现变更。Full=true 时 Backends 是当前全量快照。
type Update struct {
	Backends []Backend
	Full     bool
}

// Discovery 是服务发现源。Watch 返回的通道首先投递一次当前全量快照，
// 之后每次变更投递一个新的全量快照。
type Discovery interface {
	Watch(ctx context.Context) (<-chan Update, error)
}
