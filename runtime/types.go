package runtime

import (
	"context"
	"time"
)

// Version 是函数版本的不可变标识。
type Version string

// CurrentVersion 是路由用的特殊值：始终路由到函数当前指向的版本。
const CurrentVersion Version = "CURRENT"

// Handler 是进程内执行器。实现方必须遵守 ctx 的取消约定：
// 当 ctx.Done() 关闭时（超时/取消），应尽快停止工作并返回，
// 平台不会强行抢占 goroutine，只会把迟到的结果丢弃。
type Handler interface {
	Invoke(ctx context.Context, payload []byte) ([]byte, error)
}

// HandlerFunc 让普通函数满足 Handler。
type HandlerFunc func(ctx context.Context, payload []byte) ([]byte, error)

func (f HandlerFunc) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	return f(ctx, payload)
}

// VersionSpec 描述一次发布的内容：版本号、执行器、该版本配置。
type VersionSpec struct {
	Version Version
	Handler Handler
	// NewInstance 可选：每次创建实例时调用。为空时直接复用 Handler，
	// 即把执行器视为无状态单例。提供工厂时，每个池实例彼此隔离。
	NewInstance func() (Handler, error)
	// Concurrency 该版本允许同时执行的调用数，必须 >= 1。
	Concurrency int
	// MaxQueueDepth 并发占满时允许排队等待的调用数，0 表示不排队。
	MaxQueueDepth int
	// IdleTimeout 实例空闲多久后回收；0 表示立即回收（每次调用新建）。
	IdleTimeout time.Duration
	// MaxIdleInstances 最多保留的空闲实例数。
	//   不填（nil 语义，即 <0）：保持历史行为——高并发回落后保留 1 个暖实例；
	//   0：用完即回收，不保留任何空闲实例；
	//   N(>=1)：高并发回落后最多留 N 个空闲实例，多余的立即销毁。
	// 注意 IdleTimeout=0 时无论该值为何都立即回收；版本退场（停用/不再是当前路由版本）
	// 或运行时排空时，空闲实例也会被立即回收。
	MaxIdleInstances *int
}

// Revision 是注册表中一个不可变版本。发布后永不原地修改，
// 更新函数只会新增 revision 或调整路由/启停状态，
// 因此已经开始的调用始终持有它开始那一刻的 revision 指针。
type Revision struct {
	spec        VersionSpec
	publishedAt time.Time
	disabled    bool
}

func (r *Revision) Spec() VersionSpec { return r.spec }

// InvokeRequest 是一次调用的输入。CallID 用于幂等去重，
// 为空表示不去重（每次都真正执行）。
type InvokeRequest struct {
	Function string
	// Version 为具体版本号，或留空 / CurrentVersion 表示走当前版本。
	Version Version
	Payload []byte
	CallID  string
	// Timeout 限制整个调用（排队 + 执行）的时长；0 表示无超时。
	Timeout time.Duration
}

// InvokeResult 是一次调用的稳定结论。
type InvokeResult struct {
	Function string
	Version  Version
	Payload  []byte
	// Attempted 为 true 表示执行器被真正派发执行过（哪怕最终失败）。
	Attempted bool
	Duration  time.Duration
}
