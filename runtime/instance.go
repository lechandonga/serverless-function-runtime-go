package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// drainingQueuedSentinel 表示“排空开始时仍在队列中、未开始执行”的拒绝。
// Kind 仍归一化为 KindDraining；计数由 beginDrain 权威完成，调用方
// 见到哨兵时不再重复累加排队拒绝数。
var drainingQueuedSentinel = errors.New("draining: queued invocation rejected when drain began")

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
//   - active 数永远不超过 spec.Concurrency；detached 是“调用已离开、
//     但执行器仍在后台跑”的脱离载体，不占并发槽、永不回池；
//   - 满载时按 FIFO 排队，队列长度上限 MaxQueueDepth，超出立即过载拒绝；
//   - 空闲实例最多保留 maxIdle 个（默认 1，可配置，0 表示用完即回收），
//     每个空闲实例在 IdleTimeout 后回收，下一次调用自动重新创建；
//   - 回收、派发、版本更新由 revision 不可变性 + mu 互斥共同保证无竞争。
type instancePool struct {
	rev *Revision

	mu       sync.Mutex
	active   int
	detached int
	idle     []*idleEntry
	waiters  []*waiter
	flights  map[*flight]struct{} // 已真正派发、仍在途的调用
	retired  bool                 // 版本退场（停用/不再是当前版本）：跑完即回收
	draining bool
	// drainGate 在排空状态正式发布前安装（持函数锁），此后该池看到的
	// “排空已经开始”不依赖 beginDrain 是否已经轮到本池：
	// 闸门安装后到 beginDrain 快照之间自然结束的调用也会被计入排空小结，
	// 其载体不会回到空闲池。
	drainGate atomic.Pointer[drainState]
	// drainSnapshotted 为 true 表示 beginDrain 已经完成在途快照。
	drainSnapshotted bool
	// queuedRejectedEarly：闸门安装之后、在途快照之前，已在队列中
	// 等待、被闸门拒绝派发的调用数。与快照时仍在队列的等待者共同
	// 构成 QueuedRejected，权威计数、不依赖各调用方自行累加。
	queuedRejectedEarly int
	// drainCompleted 在池锁内维护：闸门安装之后、在途快照之前自然
	// 结束的在途调用数。这些调用已不在快照 flights 里，单列以免漏计；
	// 快照之后结束的调用直接从快照 flight 集合上判定，不再累加。
	drainCompleted int

	created   atomic.Int64
	destroyed atomic.Int64
}

type idleEntry struct {
	inst     *instance
	idleAt   time.Time // 入池时刻，用于判定/观测
	deadline time.Time
	timer    *time.Timer
}

type waiter struct {
	ch      chan struct{}
	ctx     context.Context
	dead    bool
	drained bool // 被排空清队：拒绝数由池方权威统计，调用方不再累加
	// settle 在排队结论确定（派发/拒绝/取消）时调用一次：
	// 让调用离开排空准入账本。幂等。
	settle      func()
	settledOnce bool
}

// markSettled 要求持有池锁：幂等地标记“结论已确定”，返回是否
// 需要由调用方在锁外真正执行 settle（避免在池锁内拿准入账本锁）。
func (w *waiter) markSettled() bool {
	if w.settle == nil || w.settledOnce {
		return false
	}
	w.settledOnce = true
	return true
}

type poolToken struct {
	pool *instancePool
	inst *instance
}

func newInstancePool(rev *Revision) *instancePool {
	return &instancePool{rev: rev, flights: map[*flight]struct{}{}}
}

// maxIdle 返回该版本生效的空闲实例保留数。
// 配置留空时保持历史默认：空闲未超时的实例最多保留 1 个暖实例。
func (p *instancePool) maxIdle() int {
	if p.rev.spec.MaxIdleInstances != nil {
		return *p.rev.spec.MaxIdleInstances
	}
	return 1
}

// armDrainGate 在排空状态发布前安装闸门。要求持有函数锁，
// 与 resolve 的懒建池互斥，杜绝“排空枚举之后才诞生的新池”。
func (p *instancePool) armDrainGate(d *drainState) {
	p.drainGate.Store(d)
}

// beginDrain 进入排空态：不再派发，并立即清空等待队列。
// 被清除的等待者由 acquire 侧按排空原因结束。可重复、可并发调用。
// d 必须是已经 arm 到本池闸门上的同一个 drainState。
func (p *instancePool) beginDrain(d *drainState) (queued int, flights []*flight) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.drainGate.Load() == nil {
		p.drainGate.Store(d)
	}
	p.draining = true
	for _, w := range p.waiters {
		// 已经自行超时/取消退出的等待者不计入排空拒绝：
		// 它拿到的是 QueueTimeout/Canceled，不是排空结论。
		if w.dead {
			continue
		}
		queued++
		w.dead = true
		w.drained = true
		if d := p.drainGate.Load(); d != nil {
			// 直接在排空账本上离账：等待者此后只负责接收唤醒，
			// seal 不必等它；计数已权威计入 queued。
			d.leaveAdmission()
		}
		w.markSettled()
		close(w.ch) // 未取消的等待者的 ch 尚未关闭
	}
	p.waiters = nil
	for f := range p.flights {
		flights = append(flights, f)
	}
	p.drainSnapshotted = true
	return queued, flights
}

// drainCompletedEarlyLocked 要求持有 mu：返回“快照前自然结束”的调用数。
func (p *instancePool) drainCompletedEarlyLocked() int { return p.drainCompleted }

// markQueuedRejectedEarly 要求持有 mu：登记一笔“闸门后、快照前
// 在队列中被拒绝派发”的调用，统一计入排空小结 QueuedRejected。
func (p *instancePool) markQueuedRejectedEarly() {
	if p.drainGate.Load() != nil && !p.draining {
		p.queuedRejectedEarly++
	}
}

// retire 标记版本退场：空闲实例立即回收，在途调用跑完后不再保留。
func (p *instancePool) retire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retired = true
	p.dropAllIdleLocked()
}

// acquire 在 ctx 截止/取消前取得一个实例。
// 返回的 token 必须且只能终结一次（release 或 detach）。
func (p *instancePool) acquire(ctx context.Context, onQueued func()) (*flight, error) {
	spec := p.rev.spec

	p.mu.Lock()
	drainingNow := p.draining || p.drainGate.Load() != nil
	if drainingNow {
		if d := p.drainGate.Load(); d != nil {
			d.newRejected.Add(1)
		}
		err := platformError(KindDraining, "", spec.Version, "runtime is draining and accepts no new invocations")
		p.mu.Unlock()
		return nil, err
	}
	// 有空闲容量即可派发；空闲实例若已被 reaper 标记回收则新建。
	if f, ok := p.tryDispatchLocked(); ok {
		p.mu.Unlock()
		return f, nil
	}
	// 并发已满：尝试排队。
	if spec.MaxQueueDepth == 0 || len(p.waiters) >= spec.MaxQueueDepth {
		err := platformError(KindOverload, "", p.rev.spec.Version,
			"concurrency=%d full and queue depth=%d exhausted", spec.Concurrency, spec.MaxQueueDepth)
		p.mu.Unlock()
		return nil, err
	}
	w := &waiter{ch: make(chan struct{}), ctx: ctx, settle: onQueued}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	select {
	case <-w.ch:
		// release/drain 方唤醒后，这里再次持锁检查并创建/复用实例。
		p.mu.Lock()
		settled := false
		if w.dead {
			draining := p.draining
			drained := w.drained
			gate := p.drainGate.Load()
			if draining && drained {
				settled = w.markSettled()
				p.mu.Unlock()
				if settled {
					w.settle()
				}
				return nil, drainingQueuedSentinel
			}
			if draining || gate != nil {
				// 闸门已装、快照前被其他事件唤醒判死的排队者：
				// 未获派发，计入 QueuedRejected（快照前那一笔）。
				if gate != nil && !drained {
					p.queuedRejectedEarly++
				}
				settled = w.markSettled()
				p.mu.Unlock()
				if settled {
					w.settle()
				}
				return nil, platformError(KindDraining, "", spec.Version, "runtime is draining and accepts no new invocations")
			}
			// 极端竞态：等待者已超时退出，槽位没有真正交给它。
			settled = w.markSettled()
			p.mu.Unlock()
			if settled {
				w.settle()
			}
			return nil, ctx.Err()
		}
		// 闸门已装（无论 beginDrain 是否已到）：排队者不得派发。
		// 计数由 drain 方在池锁内权威完成（快照时仍在队列的算
		// QueuedRejected；快照前被拒绝的由本池 queuedRejectedEarly 记录），
		// 这里只归一化结论。
		if p.draining || p.drainGate.Load() != nil {
			p.markQueuedRejectedEarly()
			settled = w.markSettled()
			p.mu.Unlock()
			if settled {
				w.settle()
			}
			return nil, drainingQueuedSentinel
		}
		f, ok := p.tryDispatchLocked()
		if !ok {
			// 理论不可达：被唤醒必然有空闲槽；保守处理为过载。
			settled = w.markSettled()
			p.mu.Unlock()
			if settled {
				w.settle()
			}
			return nil, platformError(KindOverload, "", spec.Version, "woken but no slot available")
		}
		settled = w.markSettled()
		p.mu.Unlock()
		if settled {
			w.settle()
		}
		return f, nil
	case <-ctx.Done():
		p.mu.Lock()
		w.dead = true
		settled := w.markSettled()
		p.removeWaiterLocked(w)
		p.mu.Unlock()
		if settled {
			w.settle()
		}
		return nil, ctx.Err()
	}
}

// tryDispatchLocked 要求持有 mu；在有容量时复用/新建实例。
func (p *instancePool) tryDispatchLocked() (*flight, bool) {
	spec := p.rev.spec
	if p.active >= spec.Concurrency {
		return nil, false
	}
	var inst *instance
	for len(p.idle) > 0 {
		entry := p.idle[0]
		p.idle = p.idle[1:]
		p.stopIdleTimerLocked(entry)
		if !entry.inst.closed {
			inst = entry.inst
			break
		}
	}
	if inst == nil {
		h, err := p.createHandler()
		if err != nil {
			return nil, false
		}
		inst = &instance{id: newInstanceID(), handler: h}
		p.created.Add(1)
	}
	p.active++
	tok := &poolToken{pool: p, inst: inst}
	f := newFlight(tok)
	p.flights[f] = struct{}{}
	return f, true
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
// 不可再复用；正常结束时按保留策略决定入空闲池或立即回收。
// 版本退场或排空期间归还的实例一律销毁。
func (p *poolToken) release(f *flight, crashed bool) {
	pool := p.pool
	pool.mu.Lock()
	pool.active--
	delete(pool.flights, f)

	draining := pool.draining || pool.drainGate.Load() != nil
	// 闸门安装之后自然结束的在途调用计入排空小结（无论成功、业务失败
	// 还是 panic，只要拿到的是真实执行结论就算“跑完”）。闸门在
	// beginDrain 之前安装，因此安装到快照之间结束的调用也不会漏计。
	if draining {
		// 快照之后结束的调用在快照集合上直接判定，避免重复计数。
		if !pool.drainSnapshotted {
			pool.drainCompleted++
		}
	}
	reuse := !crashed && !pool.retired && !draining
	spec := pool.rev.spec
	if reuse {
		// IdleTimeout=0 表示立即回收；显式 maxIdle=0 表示不保留；
		// 回落后空闲数超过保留上限的多余额度立即销毁。
		if spec.IdleTimeout <= 0 || pool.maxIdle() <= 0 || len(pool.idle) >= pool.maxIdle() {
			reuse = false
		}
	}
	if reuse {
		entry := &idleEntry{inst: p.inst, idleAt: time.Now(), deadline: time.Now().Add(spec.IdleTimeout)}
		pool.idle = append(pool.idle, entry)
		pool.scheduleReaperLocked(entry, spec.IdleTimeout)
	} else {
		p.inst.closed = true
		pool.destroyed.Add(1)
	}

	pool.wakeOneLocked()
	pool.mu.Unlock()
}

// detach 把一份执行载体与已超时/取消的调用彻底解绑：并发槽立即归还，
// 载体既不回空闲池也不允许再被派发；后台执行器无论之后正常返回、
// 报错还是 panic，都由 finalizeDetached 自动收尾销毁，不影响任何结论。
// 每个 token 只会 detach 或 release 中的一个、且只发生一次。
func (p *poolToken) detach(f *flight) {
	pool := p.pool
	pool.mu.Lock()
	pool.active--
	pool.detached++
	p.inst.closed = true
	delete(pool.flights, f)
	pool.wakeOneLocked()
	pool.mu.Unlock()
}

// finalizeDetached 由后台等待脱离执行器结束的 goroutine 调用：
// 只做脱离账目归零与销毁计数。执行器永不返回时该 goroutine 不占并发槽、
// 不阻塞排空与停机；进程退出时随之消亡，账目长期自洽。
func (p *instancePool) finalizeDetached(inst *instance) {
	p.mu.Lock()
	p.detached--
	inst.closed = true
	p.destroyed.Add(1)
	p.mu.Unlock()
}

// wakeOneLocked 把空出的槽位移交给队首仍存活的等待者。
func (p *instancePool) wakeOneLocked() {
	if p.draining {
		return
	}
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

func (p *instancePool) scheduleReaperLocked(entry *idleEntry, d time.Duration) {
	entry.timer = time.AfterFunc(d, func() { p.reap(entry) })
}

func (p *instancePool) stopIdleTimerLocked(target *idleEntry) {
	if target.timer != nil {
		target.timer.Stop()
		target.timer = nil
	}
}

// reap 回收某个空闲超时实例。只有在“仍处于空闲列表中”时才销毁，
// 因此与派发并发发生时，绝不可能回收一个正在忙的实例。
func (p *instancePool) reap(target *idleEntry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, entry := range p.idle {
		if entry != target {
			continue
		}
		p.idle = append(p.idle[:i], p.idle[i+1:]...)
		p.stopIdleTimerLocked(entry)
		entry.inst.closed = true
		p.destroyed.Add(1)
		return
	}
}

// dropAllIdleLocked 要求持有 mu：立即销毁全部空闲实例（版本退场/停机）。
func (p *instancePool) dropAllIdleLocked() {
	for _, entry := range p.idle {
		p.stopIdleTimerLocked(entry)
		entry.inst.closed = true
		p.destroyed.Add(1)
	}
	p.idle = nil
}

// Stats 供测试与可观测性使用。
type Stats struct {
	Active int
	// Idle 保持历史布尔语义：该版本当前是否存在至少一个空闲实例。
	// 老调用方可以直接 `if st.Idle` 条件判断，升级不破坏源码兼容。
	Idle        bool
	IdleCount   int // 空闲实例的具体数量；需要数量的新代码请使用本字段
	Detached    int
	QueueLength int
	Created     int64
	Destroyed   int64
	Retired     bool
	Draining    bool
}

// 账目不变量：Created - Destroyed == Active + Idle + Detached。
func (p *instancePool) stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	idle := 0
	for _, entry := range p.idle {
		if !entry.inst.closed {
			idle++
		}
	}
	return Stats{
		Active:      p.active,
		Idle:        idle > 0,
		IdleCount:   idle,
		Detached:    p.detached,
		QueueLength: len(p.waiters),
		Created:     p.created.Load(),
		Destroyed:   p.destroyed.Load(),
		Retired:     p.retired,
		Draining:    p.draining,
	}
}
