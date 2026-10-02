package runtime

import (
	"context"
	"sync"
	"time"
)

// Instance 是某个函数版本的一个可复用执行实例（进程内对象）。
// Execute 的实现应当尊重 ctx 的取消信号；否则平台超时后只能等待其自然返回，
// 该实例也会被判定为不健康并在返回后立即回收。
type Instance interface {
	Execute(ctx context.Context, in any) (any, error)
	Close() error
}

// Factory 按调用时传入的版本快照创建实例。工厂 panic 与 error 都会被
// 归一化成 INSTANCE_CREATE_FAILED 平台错误，且不会影响其他函数/版本。
type Factory func(version VersionSpec) (Instance, error)

// idleEntry 记录空闲实例及其进入空闲的时刻，用于空闲超时回收。
type idleEntry struct {
	inst  Instance
	since time.Time
}

// pool 管理单个函数版本的实例集合。所有实例状态变更都在 mu 保护下完成，
// 实例的 Execute 调用本身在锁外执行。
type pool struct {
	factory Factory
	spec    VersionSpec

	mu       sync.Mutex
	idle     []*idleEntry
	busy     int
	max      int
	shutdown bool

	// returned 上的非阻塞发送表示“有实例归还/新建失败/实例脱离”，等待者自行重试。
	returned chan struct{}

	idleTimeout time.Duration

	rt *Runtime
}

func newPool(rt *Runtime, spec VersionSpec, maxInstances int, idleTimeout time.Duration) *pool {
	return &pool{
		factory:     rt.factory,
		rt:          rt,
		spec:        spec,
		max:         maxInstances,
		idleTimeout: idleTimeout,
		returned:    make(chan struct{}, 1),
	}
}

func (p *pool) signal() {
	select {
	case p.returned <- struct{}{}:
	default:
	}
}

// acquire 获取一个健康实例。调用方必须已经持有函数级并发名额。
func (p *pool) acquire(ctx context.Context, clock Clock, waitTimeout time.Duration) (Instance, error) {
	p.mu.Lock()
	if p.shutdown {
		p.mu.Unlock()
		return nil, platformError(ErrVersionDeactivated, "version %q is shutting down", p.spec.Version)
	}
	if inst, taken := p.takeIdleLocked(); taken {
		p.mu.Unlock()
		return inst, nil
	}
	if p.busy < p.max {
		p.busy++
		p.mu.Unlock()
		return p.createReserved()
	}
	p.mu.Unlock()

	// 实例数已达上限：等待归还、超时脱离或回收腾出名额。
	timer := clock.After(waitTimeout)
	for {
		select {
		case <-p.returned:
		case <-timer:
			return nil, platformError(ErrQueuedTimeout,
				"timed out waiting for an instance of version %q", p.spec.Version)
		case <-ctx.Done():
			return nil, platformError(ErrCallerCancelled,
				"cancelled while waiting for an instance of version %q", p.spec.Version)
		}
		p.mu.Lock()
		if p.shutdown {
			p.mu.Unlock()
			return nil, platformError(ErrVersionDeactivated, "version %q is shutting down", p.spec.Version)
		}
		if inst, taken := p.takeIdleLocked(); taken {
			p.mu.Unlock()
			return inst, nil
		}
		if p.busy < p.max {
			p.busy++
			p.mu.Unlock()
			return p.createReserved()
		}
		p.mu.Unlock()
	}
}

// takeIdleLocked 从空闲队列取出一个实例并计入忙碌；要求持锁调用。
func (p *pool) takeIdleLocked() (Instance, bool) {
	if len(p.idle) == 0 {
		return nil, false
	}
	e := p.idle[len(p.idle)-1]
	p.idle = p.idle[:len(p.idle)-1]
	p.busy++
	return e.inst, true
}

// createReserved 在锁外创建实例（调用方已让 busy++ 占好名额）；
// 失败时回滚名额并唤醒等待者。
func (p *pool) createReserved() (Instance, error) {
	inst, err := p.create()
	if err != nil {
		p.mu.Lock()
		p.busy--
		p.signal()
		p.mu.Unlock()
		return nil, err
	}
	return inst, nil
}

// create 在锁外调用工厂，并把工厂的 error/panic 归一化成平台错误。
func (p *pool) create() (inst Instance, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PlatformError{
				Code:    ErrInstanceCreate,
				Message: "instance factory panicked",
				Cause:   normalizePanic(r),
			}
		}
	}()
	inst, err = p.factory(p.spec)
	if err != nil {
		return nil, &PlatformError{Code: ErrInstanceCreate, Message: "instance factory returned error", Cause: err}
	}
	if inst == nil {
		return nil, platformError(ErrInstanceCreate, "instance factory returned nil instance")
	}
	return inst, nil
}

// release 归还已正常结束的调用持有的实例。healthy=false 时实例直接关闭销毁
// 而不是回到空闲队列；池已停用时归还的实例同样关闭。
func (p *pool) release(inst Instance, healthy bool) {
	closeNow := false
	p.mu.Lock()
	p.busy--
	switch {
	case !healthy, p.shutdown:
		closeNow = true
	default:
		p.idle = append(p.idle, &idleEntry{inst: inst, since: p.rt.clock.Now()})
	}
	p.signal()
	p.mu.Unlock()

	if closeNow {
		closeQuiet(inst)
	}
}

// releaseBusySlot 在平台按超时/取消定案时立即释放一个忙碌实例名额，
// 但不动实例本身（它可能仍在执行）。这样后续调用不会被不配合取消的
// 执行器阻塞。实例的关闭由 closeDetached / closeIdleInstance 完成。
func (p *pool) releaseBusySlot() {
	p.mu.Lock()
	p.busy--
	p.signal()
	p.mu.Unlock()
}

// closeDetached 关闭一个名额已被 releaseBusySlot 释放、且已确认从
// Execute 返回（或判定为不可信）的实例。它不会进入空闲队列。
func (p *pool) closeDetached(inst Instance) {
	closeQuiet(inst)
}

// closeIdleInstance 处理“平台刚定案，但 goroutine 同时正常完成并已把
// 实例归还空闲队列”的竞争：把该实例从空闲队列移除并关闭，保证后续调用
// 绝不会被派发到这个已经定案超时的实例。
func (p *pool) closeIdleInstance(inst Instance) {
	p.mu.Lock()
	for i, e := range p.idle {
		if e.inst == inst {
			p.idle = append(p.idle[:i], p.idle[i+1:]...)
			p.mu.Unlock()
			closeQuiet(inst)
			return
		}
	}
	p.mu.Unlock()
	// 不在空闲队列：可能已被回收者抢先关闭，幂等返回即可。
}

// reapIdle 回收空闲时间达到 idleTimeout 的实例。
func (p *pool) reapIdle(now time.Time) (closed int) {
	p.mu.Lock()
	if len(p.idle) == 0 {
		p.mu.Unlock()
		return 0
	}
	kept := make([]*idleEntry, 0, len(p.idle))
	var stale []Instance
	for _, e := range p.idle {
		if now.Sub(e.since) >= p.idleTimeout {
			stale = append(stale, e.inst)
		} else {
			kept = append(kept, e)
		}
	}
	p.idle = kept
	p.mu.Unlock()

	for _, inst := range stale {
		closeQuiet(inst)
	}
	return len(stale)
}

// shutdownAll 停用版本池：标记 shutdown 并回收所有空闲实例。
// 在途调用继续持有自己的实例，执行完成后实例被立即关闭（见 release/detach）。
func (p *pool) shutdownAll() {
	p.mu.Lock()
	p.shutdown = true
	stale := make([]Instance, len(p.idle))
	for i, e := range p.idle {
		stale[i] = e.inst
	}
	p.idle = nil
	p.signal()
	p.mu.Unlock()

	for _, inst := range stale {
		closeQuiet(inst)
	}
}

func (p *pool) stats() (idle, busy, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle), p.busy, p.max
}

// closeQuiet 关闭实例并吞掉 panic，保证回收动作本身不会影响其他函数。
func closeQuiet(inst Instance) {
	defer func() { _ = recover() }()
	_ = inst.Close()
}
