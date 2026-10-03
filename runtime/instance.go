package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// instance 是池里一个可复用的执行载体。handler 可能来自版本单例，
// 也可能来自 NewInstance 工厂。closed 一旦置位，绝不允许再派发，
// 从根上杜绝“调用被派发给已回收实例”。
type instance struct {
	id       string
	handler  Handler
	closed   bool
	idleAt   time.Time // 进入空闲列表的时刻，用于空闲超时回收
	fromPool bool      // 来自工厂（彼此隔离）；仅用于统计排查
}

// flightState 描述一次“调用—载体”绑定的生命周期状态。
type flightState int32

const (
	flightRunning     flightState = 0 // 仍与调用绑定、正在执行
	flightDetached    flightState = 1 // 调用已超时/取消离开，载体解绑，等待后台收尾
	flightDrainKilled flightState = 2 // 排空宽限期满被强制中断，载体解绑
	flightDone        flightState = 3 // 执行器返回时调用仍在绑定，已正常结账
)

// flight 是一次派发的绑定凭证。调用结论与后台执行通过 state 解耦：
// 一旦状态不再是 running，迟到的执行结果只能销毁载体，不能翻案。
type flight struct {
	state  atomic.Int32
	cancel context.CancelFunc
}

// instancePool 管理单个版本的并发槽、排队、空闲保留与排空。
//
// 规则：
//   - active 数永远不超过 spec.Concurrency；
//   - 满载时按 FIFO 排队，队列长度上限 MaxQueueDepth，超出立即过载拒绝；
//   - 空闲实例最多保留 spec.MaxIdleInstances 个（不填保持历史行为：1 个），
//     空闲 IdleTimeout 后回收；IdleTimeout=0 或保留数=0 表示用完即回收；
//   - 版本退场（停用/不再是当前路由版本）后空闲实例立即回收、跑完不再保留；
//   - 调用超时/取消后载体立即解绑：并发槽释放、载体绝不复用，后台收尾销毁；
//   - 回收、派发、版本更新由 revision 不可变性 + mu 互斥共同保证无竞争。
type instancePool struct {
	fn  string
	rev *Revision

	mu         sync.Mutex
	active     int              // 已派发且仍与调用绑定（占用并发额度）
	idle       []*instance      // 可复用的空闲载体
	detached   int              // 已解绑、执行器尚未收尾销毁的载体数
	created    int64            // 累计创建实例数
	destroyed  int64            // 累计销毁实例数
	bound      map[*flight]bool // 占用并发额度的在执绑定
	unwound    map[*flight]bool // 已解绑等待后台收尾的绑定
	waiters    []*waiter
	woken      []*waiter // 已被空槽唤醒、尚未完成派发的等待者
	retired    bool      // 版本已退场：不再保留空闲实例，在途调用照旧跑完
	timer      *time.Timer
	timerSetAt time.Time

	drainMu       sync.Mutex // 仅串行化 drain 发起时的判断
	draining      bool
	drainCh       chan struct{}
	drainCounters *poolDrainCounters
	drainPulse    chan struct{} // 排空期间每次账目变化时被 ping（容量 1）
}

// waiterOutcome 是等待者被唤醒的唯一结局，由池持锁原子决定。
type waiterOutcome int

const (
	outcomeNone     waiterOutcome = 0 // 仍在等待
	outcomeSlot     waiterOutcome = 1 // 获得槽位，醒来后持锁派发
	outcomeDraining waiterOutcome = 2 // 排空开始，以 KindDraining 结束
	outcomeCanceled waiterOutcome = 3 // 自身 ctx 截止/取消
)

type waiter struct {
	ctx context.Context
	out waiterOutcome // 持锁写入
	ch  chan struct{} // 结局置位后关闭（只关一次）
}

// waiterHook 仅供测试同步：等待者入队并即将阻塞时回调（默认 nil）。
var waiterHook func(*instancePool, *waiter)

// drainBeginHook 仅供测试同步：排空在池内正式开始（停止接单已置位）后回调（默认 nil）。
var drainBeginHook func(Version)

type poolToken struct {
	pool *instancePool
	inst *instance
	fl   *flight
}

func newInstancePool(fn string, rev *Revision) *instancePool {
	return &instancePool{fn: fn, rev: rev, bound: map[*flight]bool{}, unwound: map[*flight]bool{}}
}

// beginAcquire 在 ctx 截止/取消前取得一个实例。
// 排空期间（含排队时排空开始）一律返回 KindDraining。
func (p *instancePool) beginAcquire(ctx context.Context) (*poolToken, error) {
	spec := p.rev.spec

	p.mu.Lock()
	if p.draining {
		p.mu.Unlock()
		return nil, drainingRejected(spec.Version)
	}
	// 有空闲容量即可派发；空闲实例若已被 reaper 标记回收则跳过。
	if tok, ok := p.tryDispatchLocked(); ok {
		p.mu.Unlock()
		return tok, nil
	}
	// 并发已满：尝试排队。
	if spec.MaxQueueDepth == 0 || len(p.waiters) >= spec.MaxQueueDepth {
		err := platformError(KindOverload, "", spec.Version,
			"concurrency=%d full and queue depth=%d exhausted", spec.Concurrency, spec.MaxQueueDepth)
		p.mu.Unlock()
		return nil, err
	}
	w := &waiter{ch: make(chan struct{}), ctx: ctx}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()
	if waiterHook != nil {
		waiterHook(p, w)
	}

	select {
	case <-ctx.Done():
		p.mu.Lock()
		if w.out == outcomeNone {
			w.out = outcomeCanceled
			p.removeWaiterLocked(w)
			close(w.ch)
		}
		p.mu.Unlock()
		if w.out == outcomeCanceled {
			return nil, ctx.Err()
		}
		// 与唤醒/排空竞争失败：以池持锁写入的最终结局为准，落到下方统一处理。
	case <-w.ch:
	}

	// 结局由池持锁原子决定，这里只需照结局执行，不存在两个信号竞争。
	p.mu.Lock()
	switch w.out {
	case outcomeDraining:
		p.removeWokenLocked(w)
		p.mu.Unlock()
		return nil, drainingRejected(spec.Version)
	case outcomeCanceled:
		p.mu.Unlock()
		return nil, ctx.Err()
	case outcomeSlot:
		// 空槽已为本等待者预留（wake 时 active 已交还，此处必须派发成功）。
		tok, ok := p.tryDispatchLocked()
		p.removeWokenLocked(w)
		p.mu.Unlock()
		if !ok {
			return nil, platformError(KindOverload, "", spec.Version, "woken but no slot available")
		}
		return tok, nil
	default:
		p.mu.Unlock()
		return nil, drainingRejected(spec.Version)
	}
}

func drainingRejected(v Version) error {
	return platformError(KindDraining, "", v, "runtime is draining and accepts no new invocations")
}

func (p *instancePool) version() Version { return p.rev.spec.Version }

// tryDispatchLocked 要求持有 mu；在有容量时复用/新建实例。
func (p *instancePool) tryDispatchLocked() (*poolToken, bool) {
	spec := p.rev.spec
	if p.active >= spec.Concurrency {
		return nil, false
	}
	p.stopIdleTimerLocked()
	var inst *instance
	for len(p.idle) > 0 {
		cand := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		if cand.closed {
			continue
		}
		inst = cand
		break
	}
	if inst == nil {
		h, err := p.createHandler()
		if err != nil {
			return nil, false
		}
		inst = &instance{id: newInstanceID(), handler: h, fromPool: spec.NewInstance != nil}
		p.created++
	}
	p.active++
	fl := &flight{}
	p.bound[fl] = true
	return &poolToken{pool: p, inst: inst, fl: fl}, true
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

// finish 处理执行器真正返回（或 panic）后的收尾。
// 调用仍在绑定时：释放并发槽，按保留策略入空闲或销毁；
// 调用此前已解绑（超时/取消/排空中断）：只做后台销毁结账，永不再复用、不翻案。
func (p *poolToken) finish(o invokeOutcome) {
	pool := p.pool
	pool.mu.Lock()
	if p.fl.state.CompareAndSwap(int32(flightRunning), int32(flightDone)) {
		pool.active--
		delete(pool.bound, p.fl)
		if pool.draining {
			pool.drainCounters.Completed.Add(1)
			pool.destroyInstanceLocked(p.inst)
		} else {
			pool.retainOrDestroyLocked(p.inst, o.crashed)
			pool.wakeOneLocked()
		}
		pool.pingDrainLocked()
		pool.mu.Unlock()
		return
	}

	// 已解绑：这份载体与任何调用无关，销毁结账，不影响任何结论。
	delete(pool.unwound, p.fl)
	pool.detached--
	pool.destroyInstanceLocked(p.inst)
	pool.pingDrainLocked()
	pool.mu.Unlock()
}

// detach 调用超时/取消离开时，把载体与本次调用彻底解绑：
// 立即归还并发额度，载体绝不再回到空闲列表，由后台 goroutine 收尾。
// 仅当解绑由“普通超时/取消”完成时返回 true；排空强制中断的解绑返回 false。
func (p *poolToken) detach() bool {
	pool := p.pool
	pool.mu.Lock()
	defer pool.mu.Unlock()
	switch p.fl.state.Load() {
	case int32(flightRunning):
		if !p.fl.state.CompareAndSwap(int32(flightRunning), int32(flightDetached)) {
			return false // 与排空中断竞争失败：账目由中断方结算
		}
		pool.active--
		delete(pool.bound, p.fl)
		pool.unwound[p.fl] = true
		pool.detached++
		if !pool.draining {
			pool.wakeOneLocked() // 并发额度立即交给排队者
		}
		pool.pingDrainLocked()
		return true
	case int32(flightDrainKilled):
		// 排空已强制中断并结算账目（active 已减、已转入 unwound），
		// 这里只确认结论，不能再动账。
		return false
	default:
		return false
	}
}

// markDrainKilled 排空宽限期满时强制中断该绑定。
func (fl *flight) markDrainKilled() bool {
	return fl.state.CompareAndSwap(int32(flightRunning), int32(flightDrainKilled))
}

// stateSnapshot 返回当前绑定状态。
func (fl *flight) stateSnapshot() flightState {
	return flightState(fl.state.Load())
}

// retainOrDestroyLocked 按保留策略决定正常结束的载体去向。
func (p *instancePool) retainOrDestroyLocked(inst *instance, crashed bool) {
	spec := p.rev.spec
	if crashed || p.retired || spec.IdleTimeout == 0 {
		p.destroyInstanceLocked(inst)
		return
	}
	cap := spec.maxIdleInstances()
	if len(p.idle) >= cap {
		p.destroyInstanceLocked(inst)
		return
	}
	inst.idleAt = time.Now()
	p.idle = append(p.idle, inst)
	p.scheduleReaperLocked(inst, spec.IdleTimeout)
}

func (p *instancePool) destroyInstanceLocked(inst *instance) {
	inst.closed = true
	p.destroyed++
}

// retire 标记版本退场：空闲实例立即回收，在途调用照旧跑完但跑完不再保留。
func (p *instancePool) retire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retired = true
	for _, inst := range p.idle {
		p.destroyInstanceLocked(inst)
	}
	p.idle = nil
	p.stopIdleTimerLocked()
}

// wakeOneLocked 把空出的槽位移交给队首仍存活的等待者。
func (p *instancePool) wakeOneLocked() {
	for len(p.waiters) > 0 {
		w := p.waiters[0]
		p.waiters = p.waiters[1:]
		w.out = outcomeSlot
		p.woken = append(p.woken, w)
		close(w.ch)
		return
	}
}

func (p *instancePool) removeWokenLocked(target *waiter) {
	for i, w := range p.woken {
		if w == target {
			p.woken = append(p.woken[:i], p.woken[i+1:]...)
			return
		}
	}
}

// scheduleReaperLocked 确保回收定时器不早于最早到期的空闲实例。
func (p *instancePool) scheduleReaperLocked(inst *instance, d time.Duration) {
	deadline := inst.idleAt.Add(d)
	if p.timer != nil && !p.timerSetAt.After(deadline) {
		return
	}
	p.stopIdleTimerLocked()
	p.timerSetAt = deadline
	p.timer = time.AfterFunc(time.Until(deadline), p.reap)
}

func (p *instancePool) stopIdleTimerLocked() {
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
		p.timerSetAt = time.Time{}
	}
}

// reap 回收已空闲超时的实例。只有在“仍处于空闲且确实超时”时才销毁，
// 因此与派发并发发生时，绝不可能回收一个正在忙的实例。
func (p *instancePool) reap() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) == 0 {
		return
	}
	now := time.Now()
	spec := p.rev.spec
	kept := p.idle[:0]
	for _, inst := range p.idle {
		if now.Sub(inst.idleAt) >= spec.IdleTimeout {
			p.destroyInstanceLocked(inst)
			continue
		}
		kept = append(kept, inst)
	}
	p.idle = kept
	p.timer = nil
	p.timerSetAt = time.Time{}
	if len(kept) > 0 {
		earliest := kept[0]
		for _, inst := range kept[1:] {
			if inst.idleAt.Before(earliest.idleAt) {
				earliest = inst
			}
		}
		p.scheduleReaperLocked(earliest, spec.IdleTimeout)
	}
}

// poolDrainCounters 是单个池一次排空的小结计数（并发发起排空共享同一份）。
type poolDrainCounters struct {
	Completed      atomic.Int64 // 宽限期内正常跑完的在途调用
	Interrupted    atomic.Int64 // 宽限期满被强制中断的在途调用
	QueuedRejected atomic.Int64 // 排空开始时仍在排队、直接结束的等待者
	IdleDestroyed  atomic.Int64 // 排空开始时回收的空闲实例
}

func (p *instancePool) pingDrainLocked() {
	if p.drainPulse != nil {
		select {
		case p.drainPulse <- struct{}{}:
		default:
		}
	}
}

// drain 发起本池排空：拒绝排队者、回收空闲实例，在途调用最多再等宽限 grace；
// 宽限期满仍未结束的强制中断。即使个别执行器无视取消，本方法也按时返回，
// 绝不等待任何已解绑载体。重复/并发发起安全，并发发起共享同一次排空与小结。
func (p *instancePool) drain(grace time.Duration) *poolDrainCounters {
	p.drainMu.Lock()
	if p.draining {
		ch := p.drainCh
		counters := p.drainCounters
		p.drainMu.Unlock()
		<-ch
		return counters
	}
	p.mu.Lock()
	p.draining = true
	p.drainCh = make(chan struct{})
	p.drainPulse = make(chan struct{}, 1)
	p.drainCounters = &poolDrainCounters{}
	counters := p.drainCounters
	// 所有等待者都按排空拒绝：包括已被空槽唤醒、但还没完成派发的那些
	// （结局由本锁原子置位，它们醒来后只会得到 outcomeDraining）。
	for _, w := range p.woken {
		if w.out == outcomeSlot {
			w.out = outcomeDraining
			counters.QueuedRejected.Add(1)
		}
	}
	p.woken = nil
	for _, w := range p.waiters {
		if w.out == outcomeNone {
			w.out = outcomeDraining
			counters.QueuedRejected.Add(1)
			close(w.ch)
		}
	}
	p.waiters = nil
	for _, inst := range p.idle {
		p.destroyInstanceLocked(inst)
		counters.IdleDestroyed.Add(1)
	}
	p.idle = nil
	p.stopIdleTimerLocked()

	deadline := time.NewTimer(grace)
	pulse := p.drainPulse
	p.mu.Unlock()
	p.drainMu.Unlock()
	if drainBeginHook != nil {
		drainBeginHook(p.version())
	}

	interrupt := func() int64 {
		p.mu.Lock()
		var n int64
		for fl := range p.bound {
			if fl.markDrainKilled() {
				n++
				// 立即结算账目：并发占用归零、载体转入解绑集合等待后台销毁，
				// 之后 doInvoke 的 detach 只确认结论、不再动账。
				p.active--
				delete(p.bound, fl)
				p.unwound[fl] = true
				p.detached++
				fl.cancel()
			}
		}
		p.mu.Unlock()
		return n
	}

	for {
		p.mu.Lock()
		remaining := p.active
		p.mu.Unlock()
		if remaining == 0 {
			break
		}
		select {
		case <-deadline.C:
			counters.Interrupted.Add(interrupt())
			// 不再等待：即使执行器无视取消，排空也按时收尾。
			deadline.Stop()
			goto done
		case <-pulse:
		}
	}
	deadline.Stop()

done:
	p.drainMu.Lock()
	close(p.drainCh)
	p.drainPulse = nil
	p.drainMu.Unlock()
	return counters
}

// Stats 供测试与可观测性使用。账目恒等式：
// 当前存活载体 = Active + Idle 实例数 + Detached；Created == Destroyed + 当前存活载体。
type Stats struct {
	Active      int
	Idle        bool // 兼容旧观测：是否存在至少一个空闲实例
	IdleCount   int
	Detached    int
	Created     int64
	Destroyed   int64
	QueueLength int
	Retired     bool
	Draining    bool
}

func (p *instancePool) stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	idleCount := 0
	for _, inst := range p.idle {
		if !inst.closed {
			idleCount++
		}
	}
	return Stats{
		Active:      p.active,
		Idle:        idleCount > 0,
		IdleCount:   idleCount,
		Detached:    p.detached,
		Created:     p.created,
		Destroyed:   p.destroyed,
		QueueLength: len(p.waiters),
		Retired:     p.retired,
		Draining:    p.draining,
	}
}

// maxIdleInstances 返回保留上限：未配置时保持历史行为（1 个）。
func (s VersionSpec) maxIdleInstances() int {
	if s.MaxIdleInstances == nil {
		return 1
	}
	return *s.MaxIdleInstances
}
