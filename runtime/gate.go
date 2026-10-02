package runtime

import (
	"context"
	"sync"
	"time"
)

// gateWaiter 是排队中的一次调用。token 容量为 1，拿到 token 即表示
// 占用了一个并发名额。
type gateWaiter struct {
	token chan struct{}
}

// gate 是单个函数级别的并发闸：所有版本共享 maxConcurrency 个名额，
// 超出部分按 FIFO 排队，队列满时立即过载拒绝。
type gate struct {
	mu       sync.Mutex
	active   int
	max      int
	waiters  []*gateWaiter
	queueMax int
}

func newGate(maxConcurrency, maxQueue int) *gate {
	return &gate{max: maxConcurrency, queueMax: maxQueue}
}

// resize 动态修改并发上限与排队上限，立即派发因此可以放行的排队者。
// 调用方必须保证参数已经过校验（>=1 / >=0）。
func (g *gate) resize(maxConcurrency, maxQueue int) {
	g.mu.Lock()
	g.max = maxConcurrency
	g.queueMax = maxQueue
	g.pumpLocked()
	g.mu.Unlock()
}

// pumpLocked 在有名额且有排队者时逐个放行。
func (g *gate) pumpLocked() {
	for g.active < g.max && len(g.waiters) > 0 {
		w := g.waiters[0]
		g.waiters = g.waiters[1:]
		g.active++
		w.token <- struct{}{}
	}
}

// stats 返回 (活跃数, 排队数, 上限)，供观测与测试断言。
func (g *gate) stats() (active, queued, max int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active, len(g.waiters), g.max
}

// acquire 尝试获取一个并发名额。
//   - 立即拿到：返回 release，nil；
//   - 队列已满：返回 ErrOverloaded（调用从未进入执行器）；
//   - queueTimeout 内未等到：返回 ErrQueuedTimeout（调用从未进入执行器）。
//
// 返回的 release 必须且只能调用一次，它会归还名额并派发下一个排队者。
func (g *gate) acquire(ctx context.Context, clock Clock, queueTimeout time.Duration) (release func(), err error) {
	g.mu.Lock()
	if g.active < g.max {
		g.active++
		g.mu.Unlock()
		return g.makeRelease(), nil
	}
	if len(g.waiters) >= g.queueMax {
		g.mu.Unlock()
		return nil, platformError(ErrOverloaded,
			"function concurrency limit reached: active=%d max=%d queue=%d/%d",
			g.active, g.max, len(g.waiters), g.queueMax)
	}
	w := &gateWaiter{token: make(chan struct{}, 1)}
	g.waiters = append(g.waiters, w)
	g.mu.Unlock()

	timer := clock.After(queueTimeout)
	ctxDone := ctx.Done()
	select {
	case <-w.token:
		return g.makeRelease(), nil
	case <-timer:
		// 超时与放行可能同时发生：先尝试从队列摘除。摘除成功说明名额从未
		// 记在本次调用头上；摘除失败说明放行已经发生（token 已写入缓冲），
		// 需要把记在本调用头上的名额归还，由下一个排队者接续。
		g.mu.Lock()
		removed := g.removeWaiterLocked(w)
		g.mu.Unlock()
		if removed {
			return nil, platformError(ErrQueuedTimeout,
				"timed out waiting for concurrency slot after %s", queueTimeout)
		}
		<-w.token
		g.releaseOnce()
		return nil, platformError(ErrQueuedTimeout,
			"timed out waiting for concurrency slot after %s", queueTimeout)
	case <-ctxDone:
		// 调用方在排队期间取消：摘除排队者；若与放行竞争失败，说明已拿到
		// 名额，立即归还并由下一个排队者接续。
		g.mu.Lock()
		removed := g.removeWaiterLocked(w)
		g.mu.Unlock()
		if !removed {
			<-w.token
			g.releaseOnce()
		}
		return nil, platformError(ErrCallerCancelled, "cancelled while queued for concurrency slot")
	}
}

// removeWaiterLocked 从排队队列中删除 w；返回是否真的删除（false 表示已被放行）。
func (g *gate) removeWaiterLocked(w *gateWaiter) bool {
	for i, q := range g.waiters {
		if q == w {
			g.waiters = append(g.waiters[:i], g.waiters[i+1:]...)
			return true
		}
	}
	return false
}

// makeRelease 返回幂等的名额归还函数。
func (g *gate) makeRelease() func() {
	var released bool
	return func() {
		if released {
			return
		}
		released = true
		g.mu.Lock()
		g.active--
		g.pumpLocked()
		g.mu.Unlock()
	}
}

// releaseOnce 用于超时竞争路径中“已被放行但调用要拒绝”时直接归还名额。
func (g *gate) releaseOnce() {
	g.mu.Lock()
	g.active--
	g.pumpLocked()
	g.mu.Unlock()
}
