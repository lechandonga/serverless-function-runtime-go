package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// idempotentEntry 记录某个 CallID 的唯一一次执行结论。
// 并发重复提交时，后来者在 done 上等待并拿到与首次提交完全一致的结果；
// 首次执行结束后再次提交，返回缓存的同一结论。
type idempotentEntry struct {
	done   chan struct{}
	result *InvokeResult
	err    error
}

// Invoke 同步调用一个函数。语义见包文档：
// 超时/排队失败/崩溃等全部归一化为 *PlatformError。
func (rt *Runtime) Invoke(ctx context.Context, req InvokeRequest) (*InvokeResult, error) {
	if rt.draining.Load() {
		return nil, platformError(KindDraining, req.Function, req.Version, "runtime is draining and accepts no new invocations")
	}
	if req.CallID != "" {
		entry, first := rt.joinOrCreate(req.CallID)
		if !first {
			return rt.waitForEntry(ctx, entry)
		}
		res, err := rt.doInvoke(ctx, req)
		rt.finishEntry(req.CallID, entry, res, err)
		return res, err
	}
	return rt.doInvoke(ctx, req)
}

func (rt *Runtime) joinOrCreate(callID string) (*idempotentEntry, bool) {
	rt.callsMu.Lock()
	defer rt.callsMu.Unlock()
	if e, ok := rt.calls[callID]; ok {
		return e, false
	}
	e := &idempotentEntry{done: make(chan struct{})}
	rt.calls[callID] = e
	return e, true
}

func (rt *Runtime) waitForEntry(ctx context.Context, e *idempotentEntry) (*InvokeResult, error) {
	select {
	case <-e.done:
		return e.result, e.err
	case <-ctx.Done():
		// 等待者自己的请求被取消，不影响首次执行与其他重复提交者。
		return nil, normalizeCtxError(ctx, "", "")
	}
}

func (rt *Runtime) finishEntry(callID string, e *idempotentEntry, res *InvokeResult, err error) {
	e.result = res
	e.err = err
	close(e.done)
}

func (rt *Runtime) doInvoke(parent context.Context, req InvokeRequest) (*InvokeResult, error) {
	started := time.Now()

	if rt.draining.Load() {
		return nil, platformError(KindDraining, req.Function, req.Version, "runtime is draining and accepts no new invocations")
	}

	rev, pool, err := rt.resolve(req.Function, req.Version)
	if err != nil {
		return nil, err
	}
	if rt.draining.Load() {
		return nil, platformError(KindDraining, req.Function, rev.spec.Version, "runtime is draining and accepts no new invocations")
	}

	ctx, cancel := context.WithCancel(parent)
	if req.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, req.Timeout)
	}
	defer cancel()

	tok, err := pool.beginAcquire(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, classifyDeadline(ctx, req.Function, rev.spec.Version, started)
		}
		if errors.Is(err, context.Canceled) {
			return nil, platformError(KindCanceled, req.Function, rev.spec.Version, "invocation canceled before dispatch")
		}
		return nil, err // 过载等已归一化错误
	}
	tok.fl.cancel = cancel

	out := make(chan invokeOutcome, 1)
	go func() {
		// 执行器崩溃在此兜底：归一化为平台错误，不影响同函数或其他函数。
		o := runHandler(tok.inst.handler, ctx, req.Payload)
		// 先结账（可能本次调用早已超时/取消/被排空中断，这里只销毁载体），
		// 再送结论；缓冲 channel 保证无人接收时也不会泄漏 goroutine。
		tok.finish(o)
		out <- o
	}()

	select {
	case <-ctx.Done():
		// 结论立即确定。载体与本次调用彻底解绑：并发槽立刻可用，
		// 载体绝不再复用；执行器迟到返回时只能自我销毁，不能翻案。
		state := tok.fl.stateSnapshot()
		if state == flightDrainKilled {
			tok.detach() // 排空已结账，这里只确认结论
			return nil, platformError(KindDrainInterrupted, req.Function, rev.spec.Version,
				"still running when the drain grace period expired; forcefully interrupted")
		}
		tok.detach()
		if errors.Is(ctx.Err(), context.Canceled) || parent.Err() == context.Canceled {
			return nil, platformError(KindCanceled, req.Function, rev.spec.Version, "invocation canceled by caller while running")
		}
		return nil, platformError(KindInvokeTimeout, req.Function, rev.spec.Version, "execution exceeded timeout of %s", req.Timeout)
	case o := <-out:
		if o.crashed {
			return nil, platformError(KindExecutorCrashed, req.Function, rev.spec.Version, "%s", o.errText)
		}
		if o.err != nil {
			return nil, &PlatformError{
				Kind:     KindExecutorFailed,
				Function: req.Function,
				Version:  rev.spec.Version,
				Message:  "handler returned error",
				Cause:    o.err,
			}
		}
		return &InvokeResult{
			Function:  req.Function,
			Version:   rev.spec.Version,
			Payload:   o.payload,
			Attempted: true,
			Duration:  time.Since(started),
		}, nil
	}
}

type invokeOutcome struct {
	payload []byte
	err     error
	crashed bool
	errText string
}

func runHandler(h Handler, ctx context.Context, payload []byte) (o invokeOutcome) {
	defer func() {
		if r := recover(); r != nil {
			o = invokeOutcome{crashed: true, errText: panicText(r)}
		}
	}()
	o.payload, o.err = h.Invoke(ctx, payload)
	return o
}

// classifyDeadline 区分“排队等死”和“父上下文超时”：
// acquire 未拿到槽位时超时，归类为队列超时。
func classifyDeadline(ctx context.Context, fn string, v Version, started time.Time) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return platformError(KindCanceled, fn, v, "invocation canceled while queued")
	}
	return platformError(KindQueueTimeout, fn, v, "timed out waiting for an instance slot")
}

func normalizeCtxError(ctx context.Context, fn string, v Version) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return platformError(KindInvokeTimeout, fn, v, "waiting for in-flight duplicate call exceeded deadline")
	}
	return platformError(KindCanceled, fn, v, "canceled while waiting for in-flight duplicate call")
}

func panicText(r any) string {
	switch x := r.(type) {
	case error:
		return "handler panic: " + x.Error()
	default:
		return "handler panic"
	}
}

func newInstanceID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// DrainSummary 是一次运行时级排空的小结，便于验证与排查。
type DrainSummary struct {
	// Functions 涉及排空的版本池数量。
	Functions int
	// Completed 宽限期内正常跑完的在途调用数。
	Completed int64
	// Interrupted 宽限期用尽仍未结束、被强制中断的在途调用数。
	Interrupted int64
	// QueuedRejected 排空开始时仍在排队、直接按排空原因结束的等待者数。
	QueuedRejected int64
	// IdleDestroyed 排空开始时主动回收的空闲实例数。
	IdleDestroyed int64
	// ActiveLeft 排空结束时仍占用并发额度的绑定数（恒为 0）。
	ActiveLeft int
	// DetachedLeft 排空结束时已解绑、执行器仍未收尾（赖着不响应取消）的载体数。
	// 这些载体不拖排空、不占并发额度，之后自行收尾销毁。
	DetachedLeft int64
}

// Drain 发起运行时级优雅排空，并等待所有版本池排空结束后返回小结。
//
// 语义：
//   - 排空开始后，新提交的调用（以及仍在排队的等待者）一律立即以
//     KindDraining 拒绝，与过载/超时/停用明确区分；排空结束后同样稳定拒绝；
//   - 已经在执行的调用可继续使用整个宽限期；到点未结束的强制取消，
//     结论为 KindDrainInterrupted，与普通执行超时/调用方取消区分；
//   - 迟到返回的结果不能翻案，也不能拖住排空：即使执行器无视取消，
//     Drain 也在宽限期满时返回，遗留载体计入 DetachedLeft 自行销毁；
//   - 排空完成后并发占用归零、空闲实例清零、等待队列清空；
//   - 排空完成后可重复发起，排空中并发发起也安全：共享同一次排空，
//     所有调用方拿到同一份小结，不会卡死。
func (rt *Runtime) Drain(gracePeriod time.Duration) DrainSummary {
	rt.drainMu.Lock()
	if rt.draining.Load() {
		ch := rt.drainDone
		rt.drainMu.Unlock()
		<-ch
		return *rt.drainSummary
	}
	rt.draining.Store(true)
	rt.drainDone = make(chan struct{})
	done := rt.drainDone
	rt.drainMu.Unlock()

	rt.mu.Lock()
	type poolRef struct {
		fnName string
		v      Version
		pool   *instancePool
	}
	pools := make([]poolRef, 0)
	for name, fn := range rt.functions {
		fn.mu.Lock()
		for v, pool := range fn.pools {
			pools = append(pools, poolRef{name, v, pool})
		}
		fn.mu.Unlock()
	}
	rt.mu.Unlock()

	var sum DrainSummary
	sum.Functions = len(pools)
	var wg sync.WaitGroup
	var sumMu sync.Mutex
	for _, pr := range pools {
		wg.Add(1)
		go func(pr poolRef) {
			c := pr.pool.drain(gracePeriod)
			sumMu.Lock()
			sum.Completed += c.Completed.Load()
			sum.Interrupted += c.Interrupted.Load()
			sum.QueuedRejected += c.QueuedRejected.Load()
			sum.IdleDestroyed += c.IdleDestroyed.Load()
			// 排空刚结束时快照该池占用/解绑数（此时没有新调用能再占槽）。
			pst := pr.pool.stats()
			sum.ActiveLeft += pst.Active
			sum.DetachedLeft += int64(pst.Detached)
			sumMu.Unlock()
			wg.Done()
		}(pr)
	}
	wg.Wait()

	rt.drainMu.Lock()
	rt.drainSummary = &sum
	close(done)
	rt.drainMu.Unlock()
	return sum
}

// Stats 返回指定函数指定版本（留空取当前版本）的池统计，便于测试与观测。
type FunctionStats struct {
	Version Version
	Pool    Stats
}

func (rt *Runtime) Stats(name string, v Version) (FunctionStats, error) {
	fn, err := rt.lookup(name, v)
	if err != nil {
		return FunctionStats{}, err
	}
	fn.mu.RLock()
	defer fn.mu.RUnlock()
	if v == "" || v == CurrentVersion {
		v = fn.current
	}
	pool, ok := fn.pools[v]
	if !ok {
		return FunctionStats{Version: v}, nil
	}
	return FunctionStats{Version: v, Pool: pool.stats()}, nil
}

// Close 停止全部空闲回收定时器。已在途的调用不受影响、按原版本跑完。
// 需要“停止接单 + 宽限排空”的常驻服务语义请使用 Drain。
func (rt *Runtime) Close() {
	rt.closeOnce.Do(func() {
		rt.mu.Lock()
		rt.closed = true
		fns := make([]*functionEntry, 0, len(rt.functions))
		for _, fn := range rt.functions {
			fns = append(fns, fn)
		}
		rt.mu.Unlock()

		for _, fn := range fns {
			fn.mu.Lock()
			for _, pool := range fn.pools {
				pool.mu.Lock()
				pool.stopIdleTimerLocked()
				for _, inst := range pool.idle {
					pool.destroyInstanceLocked(inst)
				}
				pool.idle = nil
				pool.mu.Unlock()
			}
			fn.mu.Unlock()
		}
	})
}
