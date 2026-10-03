package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
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

// flight 表示一个已真正派发执行、仍在途的调用。排空用它做两件事：
// 宽限期内等待调用自然结束；到点由 drain 方统一 interrupt，强制给出
// 排空中断结论并把执行载体与调用解绑（detach），保证排空按宽限期
// 准时收尾——即使执行器完全不响应取消，也不会拖住停机。
// flight 由池在派发瞬间（持池锁）登记进 pool.flights，排空开始时
// 在同一把锁里快照，因此“在途集合”的确定不依赖时序。
type flight struct {
	tok         *poolToken
	mu          sync.Mutex
	finished    bool
	done        chan struct{}
	interruptCh chan struct{}
}

func newFlight(tok *poolToken) *flight {
	return &flight{tok: tok, done: make(chan struct{}), interruptCh: make(chan struct{})}
}

// complete 由自然结束路径调用：终结 token 并标记结束，幂等。
func (f *flight) complete(crashed bool) bool {
	f.mu.Lock()
	if f.finished {
		f.mu.Unlock()
		return false
	}
	f.finished = true
	close(f.interruptCh)
	f.mu.Unlock()
	f.tok.release(f, crashed)
	close(f.done)
	return true
}

// interrupt 由排空调度：载体与调用解绑，槽位立即归还；
// 执行 goroutine 的迟到结果无人接收，不能改变任何结论。
func (f *flight) interrupt() bool {
	f.mu.Lock()
	if f.finished {
		f.mu.Unlock()
		return false
	}
	f.finished = true
	close(f.interruptCh)
	f.mu.Unlock()
	f.tok.detach(f)
	close(f.done)
	return true
}

// DrainSummary 是一次排空的小结，便于验证与排查。
type DrainSummary struct {
	// Completed 排空开始时已在执行、并在宽限期内自然跑完的调用数。
	Completed int64
	// Interrupted 排空开始时已在执行、宽限期到点仍未结束、被强制结论的调用数。
	Interrupted int64
	// QueuedRejected 排空开始时仍在队列中、未开始执行、被直接结束的调用数。
	QueuedRejected int64
	// NewRejected 排空开始（含排空结束）后尝试新提交而被拒绝的调用数（尽力统计）。
	NewRejected int64
	// RemainingActive 排空收尾时仍占着并发槽的执行数（强制解绑后恒为 0）。
	RemainingActive int64
	// RemainingDetached 排空收尾时已解绑、但后台执行器尚未返回的载体数。
	// 它们不占并发槽、不在空闲池，之后返回会被自动销毁，不影响排空完成。
	RemainingDetached int64
	// GracePeriod 本次排空使用的宽限期。
	GracePeriod time.Duration
}

type drainState struct {
	newRejected    atomic.Int64
	queuedRejected atomic.Int64

	finish  chan struct{} // 排空完成，summary 可读
	summary DrainSummary
}

func newDrainState() *drainState {
	return &drainState{finish: make(chan struct{})}
}

// Invoke 同步调用一个函数。语义见包文档：
// 超时/排队失败/崩溃等全部归一化为 *PlatformError。
func (rt *Runtime) Invoke(ctx context.Context, req InvokeRequest) (*InvokeResult, error) {
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

func (rt *Runtime) currentDrain() *drainState {
	return rt.drain.Load()
}

// rejectIfDraining 在排空进行中或已完成时，以统一原因拒绝新提交。
func (rt *Runtime) rejectIfDraining() bool {
	if d := rt.currentDrain(); d != nil {
		d.newRejected.Add(1)
		return true
	}
	return false
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

	if rt.rejectIfDraining() {
		return nil, platformError(KindDraining, req.Function, req.Version,
			"runtime is draining and accepts no new invocations")
	}

	rev, pool, err := rt.resolve(req.Function, req.Version)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(parent)
	if req.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, req.Timeout)
	}
	defer cancel()

	f, err := pool.acquire(ctx)
	if err != nil {
		if errors.Is(err, drainingQueuedSentinel) {
			// 排空清队：排队拒绝数已由 beginDrain 权威统计，这里只归一化结论。
			return nil, platformError(KindDraining, req.Function, rev.spec.Version,
				"invocation was still queued when drain began")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, classifyDeadline(ctx, req.Function, rev.spec.Version, started)
		}
		if errors.Is(err, context.Canceled) {
			return nil, platformError(KindCanceled, req.Function, rev.spec.Version, "invocation canceled before dispatch")
		}
		return nil, err // 过载等已归一化错误
	}

	tok := f.tok
	out := make(chan invokeOutcome, 1)
	go func() {
		// 执行器崩溃在此兜底：归一化为平台错误，不影响同函数或其他函数。
		out <- runHandler(tok.inst.handler, ctx, req.Payload)
	}()

	// 排空宽限截止时刻：drain 在另一个 goroutine 里到点写入并唤醒。
	// drain 尚未发生时 deadlineCh 为 nil，select 不会选中它；
	// drain 到点时若本调用仍在跑，由 drain 方直接 interrupt 本 flight。
	select {
	case <-f.interruptCh:
		// 排空宽限期到点（drain 方同步完成解绑）：结论固定为排空中断，
		// 与普通执行超时、调用方取消可区分；迟到结果不能翻案。
		go monitorDetached(tok, out)
		return nil, platformError(KindDrainInterrupted, req.Function, rev.spec.Version,
			"execution was interrupted when the drain grace period expired")
	case <-ctx.Done():
		// 调用已经超时/取消。结论立即确定；执行 goroutine 收到 ctx.Done()
		// 后应自行退出，但即使它赖着不退出，载体也已与本次调用彻底解绑：
		// 并发槽立即归还、载体永不回空闲池，之后任何调用都不可能被安排到它；
		// 它最终正常返回/报错/panic 都只触发自动销毁，迟到结果无人接收。
		f.interrupt()
		go monitorDetached(tok, out)
		if errors.Is(ctx.Err(), context.Canceled) || parent.Err() == context.Canceled {
			return nil, platformError(KindCanceled, req.Function, rev.spec.Version, "invocation canceled by caller while running")
		}
		return nil, platformError(KindInvokeTimeout, req.Function, rev.spec.Version, "execution exceeded timeout of %s", req.Timeout)
	case o := <-out:
		f.complete(o.crashed)
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

// monitorDetached 等待脱离中的执行器结束并自动收尾销毁（正常返回/报错/panic
// 结果一律丢弃，因为调用结论早已固定）。执行器永不返回时，此 goroutine
// 随进程存活，不占用并发额度，也不阻塞排空完成。
func monitorDetached(tok *poolToken, out <-chan invokeOutcome) {
	<-out
	tok.pool.finalizeDetached(tok.inst)
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

// Close 停止接单并回收全部空闲实例与回收定时器。
// 不发起宽限排空：已在途的调用不受影响、按原版本跑完，保持历史语义；
// 需要“先排空再退出”的常驻服务场景请使用 Drain。
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
				pool.dropAllIdleLocked()
				pool.mu.Unlock()
			}
			fn.mu.Unlock()
		}
	})
}
