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
	if d := rt.drain.Load(); d != nil {
		rt.drainClaim.Unlock()
		<-d.finish
		return d.summary
	}
	d := newDrainState()
	rt.drain.Store(d) // 先发布：此后任何新提交一律 KindDraining（读路径无锁，不被并发 Drain 饿死）
	rt.drainClaim.Unlock()

	rt.runDrain(d, gracePeriod)
	return d.summary
}

func (rt *Runtime) runDrain(d *drainState, gracePeriod time.Duration) {
	// 1) 所有池进入排空态并在池锁内确定性快照：排队者与在途执行。
	//    派发与 beginDrain 互斥，因此 flights 快照精确等于
	//    “排空开始那一刻已真正在执行”的调用集合，不依赖时序。
	var queued int64
	var inflight []*flight
	for _, fn := range rt.snapshotFunctions() {
		for _, pool := range fn.snapshotPools() {
			n, flights := pool.beginDrain(d)
			queued += int64(n)
			inflight = append(inflight, flights...)
		}
	}
	d.queuedRejected.Store(queued)

	// 2) 宽限期：在途调用全部有了结论（自然跑完 / 自身超时取消）则提前
	//    收尾；否则到点统一强制中断。计数不在这里临时清点：自然终结由
	//    pool.release 在池锁内权威累加 completed，强制中断由
	//    detachForDrain 权威累加 interrupted，两条路径互斥、恰好一次，
	//    因此“收尾瞬间刚好结束”“排空生效后很快结束”都不会两边漏计。
	if len(inflight) > 0 {
		allDone := make(chan struct{})
		go func() {
			for _, f := range inflight {
				<-f.done
			}
			close(allDone)
		}()
		if gracePeriod <= 0 {
			for _, f := range inflight {
				f.forceDrainInterrupt(d)
			}
		} else {
			timer := time.NewTimer(gracePeriod)
			select {
			case <-allDone:
				timer.Stop()
			case <-timer.C:
				// 3) 到点仍未跑完的：强制结论并解绑。解绑同步完成，槽位当场
				//    归还，不等待执行器响应取消，排空绝不被赖着的执行器拖住。
				//    forceDrainInterrupt 与自然终结互斥，恰好在一个计数里落账。
				for _, f := range inflight {
					f.forceDrainInterrupt(d)
				}
			}
		}
	}

	// 4) 资源清零：空闲实例与定时器全部销毁；active 此刻恒为 0。
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
		Completed:         d.completed.Load(),
		Interrupted:       d.interrupted.Load(),
		QueuedRejected:    d.queuedRejected.Load(),
		NewRejected:       d.newRejected.Load(),
		RemainingActive:   activeLeft,
		RemainingDetached: detachedLeft,
		GracePeriod:       gracePeriod,
	}
	close(d.finish)
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
