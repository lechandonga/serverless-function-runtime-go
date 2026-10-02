package runtime

import (
	"context"
	"sync"
	"time"
)

// instance 是池里一个可复用的执行载体。handler 可能来自版本单例，
// 也可能来自 NewInstance 工厂。closed 一旦置位，绝不允许再派发，
// 从根上杜绝“调用被派发给已回收实例”。
type instance struct {
	id      string
	handler Handler
	closed  bool
}

// instancePool 管理单个版本的并发槽、排队与空闲回收。
//
// 规则：
//   - active 数永远不超过 spec.Concurrency；
//   - 满载时按 FIFO 排队，队列长度上限 MaxQueueDepth，超出立即过载拒绝；
//   - 空闲实例（当前实现保留一个暖实例）空闲 IdleTimeout 后回收，
//     下一次调用自动重新创建；IdleTimeout=0 表示用完即回收；
//   - 回收、派发、版本更新由 revision 不可变性 + mu 互斥共同保证无竞争。
type instancePool struct {
	rev *Revision

	mu      sync.Mutex
	active  int
	idle    *instance
	idleAt  time.Time // idle 入池时刻，用于 reaper 判定
	timer   *time.Timer
	waiters []*waiter
}

type waiter struct {
	ch   chan struct{}
	ctx  context.Context
	dead bool
}

type poolToken struct {
	pool *instancePool
	inst *instance
}

func newInstancePool(rev *Revision) *instancePool {
	return &instancePool{rev: rev}
}

// acquire 在 ctx 截止/取消前取得一个实例。
// 返回的 token 必须且只能 release 一次；crashed=true 时实例销毁。
func (p *instancePool) acquire(ctx context.Context) (*poolToken, error) {
	spec := p.rev.spec

	p.mu.Lock()
	// 有空闲容量即可派发；空闲实例若已被 reaper 标记回收则新建。
	if tok, ok := p.tryDispatchLocked(); ok {
		p.mu.Unlock()
		return tok, nil
	}
	// 并发已满：尝试排队。
	if spec.MaxQueueDepth == 0 || len(p.waiters) >= spec.MaxQueueDepth {
		err := platformError(KindOverload, "", p.rev.spec.Version,
			"concurrency=%d full and queue depth=%d exhausted", spec.Concurrency, spec.MaxQueueDepth)
		p.mu.Unlock()
		return nil, err
	}
	w := &waiter{ch: make(chan struct{}), ctx: ctx}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	select {
	case <-w.ch:
		// release 方在派发前会再次检查并持锁创建实例。
		p.mu.Lock()
		if w.dead {
			// 极端竞态：等待者已超时退出，槽位没有真正交给它。
			p.mu.Unlock()
			return nil, ctx.Err()
		}
		tok, ok := p.tryDispatchLocked()
		if !ok {
			// 理论不可达：被唤醒必然有空闲槽；保守处理为过载。
			p.mu.Unlock()
			return nil, platformError(KindOverload, "", spec.Version, "woken but no slot available")
		}
		p.mu.Unlock()
		return tok, nil
	case <-ctx.Done():
		p.mu.Lock()
		w.dead = true
		p.removeWaiterLocked(w)
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

// tryDispatchLocked 要求持有 mu；在有容量时复用/新建实例。
func (p *instancePool) tryDispatchLocked() (*poolToken, bool) {
	spec := p.rev.spec
	if p.active >= spec.Concurrency {
		return nil, false
	}
	p.stopIdleTimerLocked()
	inst := p.idle
	p.idle = nil
	p.idleAt = time.Time{}
	if inst != nil {
		if inst.closed {
			inst = nil
		}
	}
	if inst == nil {
		h, err := p.createHandler()
		if err != nil {
			return nil, false
		}
		inst = &instance{id: newInstanceID(), handler: h}
	}
	p.active++
	return &poolToken{pool: p, inst: inst}, true
}

func (p *instancePool) createHandler() (Handler, error) {
	spec := p.rev.spec
	if spec.NewInstance != nil {
		return spec.NewInstance()
	}
	return spec.Handler, nil
}

func (p *instancePool) removeWaiterLocked(target *waiter) {
	for i, w := range p.waiters {
		if w == target {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return
		}
	}
}

// release 归还槽位。crashed=true 表示实例执行中崩溃，必须销毁，
// 不可再复用；正常结束时按 IdleTimeout 决定暖实例保留或立即回收。
func (p *poolToken) release(crashed bool) {
	pool := p.pool
	pool.mu.Lock()
	pool.active--

	reuse := !crashed
	spec := pool.rev.spec
	if reuse && spec.IdleTimeout == 0 {
		reuse = false
	}
	if reuse && pool.idle != nil {
		// 当前池策略只保留一个暖实例，多余实例直接回收。
		reuse = false
	}

	if reuse {
		pool.idle = p.inst
		pool.idleAt = time.Now()
		pool.scheduleReaperLocked(spec.IdleTimeout)
	} else {
		p.inst.closed = true
	}

	pool.wakeOneLocked()
	pool.mu.Unlock()
}

// wakeOneLocked 把空出的槽位移交给队首仍存活的等待者。
func (p *instancePool) wakeOneLocked() {
	for len(p.waiters) > 0 {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		if w.dead {
			continue
		}
		close(w.ch)
		return
	}
}

func (p *instancePool) scheduleReaperLocked(d time.Duration) {
	p.stopIdleTimerLocked()
	p.timer = time.AfterFunc(d, p.reap)
}

func (p *instancePool) stopIdleTimerLocked() {
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
}

// reap 回收空闲超时实例。只有在“仍处于空闲且确实超时”时才销毁，
// 因此与派发并发发生时，绝不可能回收一个正在忙的实例。
func (p *instancePool) reap() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.idle == nil || p.idle.closed {
		return
	}
	if time.Since(p.idleAt) < p.rev.spec.IdleTimeout {
		p.scheduleReaperLocked(p.rev.spec.IdleTimeout - time.Since(p.idleAt))
		return
	}
	p.idle.closed = true
	p.idle = nil
	p.idleAt = time.Time{}
}

// Stats 供测试与可观测性使用。
type Stats struct {
	Active       int
	Idle         bool
	QueueLength  int
}

func (p *instancePool) stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Active: p.active, Idle: p.idle != nil && !p.idle.closed, QueueLength: len(p.waiters)}
}
