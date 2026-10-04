package runtime

import (
	"time"
)

// Drain 主动发起一次优雅排空/停机，并阻塞到排空完成（最多 gracePeriod）。
//
// 语义：
//   - 排空开始后，新提交的调用一律不接，立即返回 KindDraining，
//     与过载、超时、版本停用明确区分；排空结束后再调用也按同一原因稳定拒绝；
//   - 排空开始时仍在队列、未开始执行的调用，立即以 KindDraining 结束；
//   - 已经真正在执行的调用可以把宽限期用满；宽限期到点仍未结束的，
//     被强制给出 KindDrainInterrupted 结论（区别于普通执行超时/取消），
//     执行载体立即解绑、并发槽立即归还，哪怕执行器赖着不响应取消，
//     排空也按时收尾；这些调用之后迟到的结果不能翻案；
//   - 排空完成后资源清零：并发占用归零、空闲实例清空、等待队列清空；
//   - 允许完成后重复发起（返回同一份小结），也允许在排空中被并发发起
//     （并发发起者共享同一次排空、等待同一个结果），都安全且不卡死。
func (rt *Runtime) Drain(gracePeriod time.Duration) DrainSummary {
	if gracePeriod < 0 {
		gracePeriod = 0
	}

	rt.drainClaim.Lock()
	if existing := rt.drain.Load(); existing != nil {
		rt.drainClaim.Unlock()
		<-existing.finish
		return existing.summary
	}

	d := newDrainState()

	// 阶段一：在 admitMu 写锁内完成“认领账本 → 装闸门 → 发布 →
	// 逐池 beginDrain → 密封准入”。入口的排空判定用同一把锁的
	// 读临界区完成，因此：
	//   - 写锁获取时仍在入口读临界区的提交，看到的是旧状态，已登记
	//     准入，随后被池闸门/清队权威处理，不会漏；
	//   - 写锁释放后进入入口的提交，必看到 drain 已发布，立即拒绝。
	rt.admitMu.Lock()
	rt.mu.Lock()
	for _, fn := range rt.functions {
		fn.mu.Lock()
		for _, pool := range fn.pools {
			pool.armDrainGate(d)
		}
		fn.mu.Unlock()
	}
	rt.drain.Store(d)
	rt.mu.Unlock()
	rt.drainClaim.Unlock()

	var queued int64
	var earlyCompleted int64
	var inflight []*flight
	for _, fn := range rt.snapshotFunctions() {
		for _, pool := range fn.snapshotPools() {
			n, flights := pool.beginDrain(d)
			inflight = append(inflight, flights...)
			queued += int64(n)
		}
	}
	d.sealAndWaitAdmitted()
	for _, fn := range rt.snapshotFunctions() {
		for _, pool := range fn.snapshotPools() {
			pool.mu.Lock()
			earlyCompleted += int64(pool.drainCompletedEarlyLocked())
			queued += int64(pool.queuedRejectedEarly)
			pool.mu.Unlock()
		}
	}
	d.queuedRejected.Store(queued)

	// 宽限期放开写锁：新提交在读临界区里立即拒绝并计数。
	rt.admitMu.Unlock()

	// 阶段二：宽限期。
	deadline := time.Now().Add(gracePeriod)
	allDone := make(chan struct{})
	go func() {
		for _, f := range inflight {
			<-f.done
		}
		close(allDone)
	}()
	wait := time.Until(deadline)
	timedOut := wait <= 0
	if !timedOut {
		timer := time.NewTimer(wait)
		select {
		case <-allDone:
			timer.Stop()
		case <-timer.C:
			timedOut = true
		}
	}

	// 到点仍未跑完的：同步强制结论并解绑，槽位当场归还。
	var interrupted int64
	if timedOut {
		for _, f := range inflight {
			if f.interrupt() {
				interrupted++
			}
		}
	}
	// 快照集合是权威划分：每个 flight 最终要么自然结束、要么被中断；
	// interrupt() 返回 false 即临界点自然结束。跑完 = 快照数 -
	// 中断数，再补上快照前（闸门安装后）已自然结束的调用。
	completed := int64(len(inflight)) - interrupted + earlyCompleted

	// 阶段三：收口。重新取写锁——此刻仍在入口读临界区的拒绝调用
	// 与入口判定串行：Lock 成功后才进入入口的提交严格晚于排空，
	// 不属于本小结（拒绝原因仍是同一种 KindDraining）。
	rt.admitMu.Lock()

	var activeLeft, detachedLeft int64
	for _, fn := range rt.snapshotFunctions() {
		for _, pool := range fn.snapshotPools() {
			pool.mu.Lock()
			pool.dropAllIdleLocked()
			activeLeft += int64(pool.active)
			detachedLeft += int64(pool.detached)
			pool.mu.Unlock()
		}
	}

	d.summary = DrainSummary{
		Completed:         completed,
		Interrupted:       interrupted,
		QueuedRejected:    d.queuedRejected.Load(),
		NewRejected:       d.newRejected.Load(),
		RemainingActive:   activeLeft,
		RemainingDetached: detachedLeft,
		GracePeriod:       gracePeriod,
	}
	close(d.finish)
	rt.admitMu.Unlock()
	return d.summary
}

func (rt *Runtime) snapshotFunctions() []*functionEntry {
	rt.mu.RLock()
	fns := make([]*functionEntry, 0, len(rt.functions))
	for _, fn := range rt.functions {
		fns = append(fns, fn)
	}
	rt.mu.RUnlock()
	return fns
}

func (fn *functionEntry) snapshotPools() []*instancePool {
	fn.mu.RLock()
	pools := make([]*instancePool, 0, len(fn.pools))
	for _, pool := range fn.pools {
		pools = append(pools, pool)
	}
	fn.mu.RUnlock()
	return pools
}
