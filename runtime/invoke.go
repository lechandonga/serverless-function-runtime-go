package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
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

	rev, pool, err := rt.resolve(req.Function, req.Version)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(parent)
	if req.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, req.Timeout)
	}
	defer cancel()

	tok, err := pool.acquire(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, classifyDeadline(ctx, req.Function, rev.spec.Version, started)
		}
		if errors.Is(err, context.Canceled) {
			return nil, platformError(KindCanceled, req.Function, rev.spec.Version, "invocation canceled before dispatch")
		}
		return nil, err // 过载等已归一化错误
	}

	out := make(chan invokeOutcome, 1)
	go func() {
		// 执行器崩溃在此兜底：归一化为平台错误，不影响同函数或其他函数。
		out <- runHandler(tok.inst.handler, ctx, req.Payload)
	}()

	select {
	case <-ctx.Done():
		// 调用已经超时/取消。结论立即确定；执行 goroutine 收到 ctx.Done()
		// 后应自行退出，其迟到结果无人接收，不能改变本次结论。
		tok.release(false)
		if errors.Is(ctx.Err(), context.Canceled) || parent.Err() == context.Canceled {
			return nil, platformError(KindCanceled, req.Function, rev.spec.Version, "invocation canceled by caller while running")
		}
		return nil, platformError(KindInvokeTimeout, req.Function, rev.spec.Version, "execution exceeded timeout of %s", req.Timeout)
	case o := <-out:
		tok.release(o.crashed)
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
				if pool.idle != nil {
					pool.idle.closed = true
					pool.idle = nil
				}
				pool.mu.Unlock()
			}
			fn.mu.Unlock()
		}
	})
}
