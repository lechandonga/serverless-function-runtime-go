package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type versionState struct {
	pool   *pool
	active bool
}

type funcState struct {
	config  FunctionConfig
	gate    *gate
	current string
	// versions 保留所有发布过的版本条目（包括已停用），用于区分
	// VERSION_UNKNOWN 与 VERSION_DEACTIVATED。
	versions map[string]*versionState
}

type dedupEntry struct {
	result *Result
	done   chan struct{}
	at     time.Time
}

// ErrCallerCancelled 表示调用方在排队或等待幂等结果时主动取消 ctx。
const ErrCallerCancelled ErrorCode = "CALLER_CANCELLED"

// ErrExecutorPanic 表示执行器在调用过程中 panic（已被平台恢复，不影响其他函数）。
const ErrExecutorPanic ErrorCode = "EXECUTOR_PANIC"

// Runtime 是本机进程内的 Serverless 函数运行时：不依赖容器或任何远程资源。
type Runtime struct {
	factory Factory

	mu    sync.RWMutex
	funcs map[string]*funcState

	dedupMu sync.Mutex
	dedup   map[string]*dedupEntry

	clock        Clock
	reapInterval time.Duration
	dedupTTL     time.Duration

	stopCh chan struct{}
	wg     sync.WaitGroup
	closed bool
}

// after 使用时钟注册超时；FakeClock 且用于内部后台循环时走严格假时钟，
// 避免真实时间兜底干扰确定性的空闲回收。
func (rt *Runtime) after(d time.Duration) <-chan time.Time {
	if fc, ok := rt.clock.(*FakeClock); ok {
		return fc.fakeAfter(d)
	}
	return rt.clock.After(d)
}

// New 创建运行时并启动后台空闲回收循环。
func New(factory Factory, opts Options) *Runtime {
	if factory == nil {
		panic("runtime: factory must not be nil")
	}
	clock := opts.Clock
	if clock == nil {
		clock = realClock{}
	}
	reapInterval := opts.ReapInterval
	if reapInterval == 0 {
		reapInterval = defaultReapInterval
	}
	dedupTTL := opts.DedupTTL
	if dedupTTL == 0 {
		dedupTTL = defaultDedupTTL
	}
	rt := &Runtime{
		factory:      factory,
		funcs:        make(map[string]*funcState),
		dedup:        make(map[string]*dedupEntry),
		clock:        clock,
		reapInterval: reapInterval,
		dedupTTL:     dedupTTL,
		stopCh:       make(chan struct{}),
	}
	rt.wg.Add(1)
	go rt.reaperLoop()
	return rt
}

// Close 停止后台回收并停用所有版本池（回收空闲实例）；在途调用不受影响，
// 执行完成后其实例同样会被关闭。Close 之后不再接受注册与调用。
func (rt *Runtime) Close() error {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return nil
	}
	rt.closed = true
	states := make([]*funcState, 0, len(rt.funcs))
	for _, fs := range rt.funcs {
		states = append(states, fs)
	}
	rt.mu.Unlock()

	close(rt.stopCh)
	for _, fs := range states {
		for _, vs := range fs.versions {
			vs.pool.shutdownAll()
		}
	}
	rt.wg.Wait()
	return nil
}

// RegisterFunction 注册一个全新函数，发布 initialVersion 并设为当前版本。
// 任何校验失败都整体拒绝，运行时中不会留下半成品状态。
func (rt *Runtime) RegisterFunction(name, initialVersion string, config FunctionConfig) error {
	if name == "" {
		return platformError(ErrInvalidConfig, "function name must not be empty")
	}
	if initialVersion == "" {
		return platformError(ErrInvalidConfig, "initial version must not be empty")
	}
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return err
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return platformError(ErrInvalidConfig, "runtime is closed")
	}
	if _, exists := rt.funcs[name]; exists {
		return platformError(ErrInvalidConfig, "function %q already registered", name)
	}

	fs := &funcState{
		config:   config,
		gate:     newGate(config.MaxConcurrency, config.MaxQueue),
		current:  initialVersion,
		versions: make(map[string]*versionState),
	}
	p := newPool(rt, VersionSpec{Function: name, Version: initialVersion},
		config.MaxInstancesPerVersion, config.InstanceIdleTimeout)
	fs.versions[initialVersion] = &versionState{pool: p, active: true}
	rt.funcs[name] = fs
	return nil
}

// PublishVersion 发布新版本。实例通过共享工厂在首次调用时按版本快照创建。
// setCurrent=true 时把当前路由原子切到新版本；任何失败都不改变现有状态。
func (rt *Runtime) PublishVersion(name, version string, setCurrent bool) error {
	if version == "" {
		return platformError(ErrInvalidConfig, "version must not be empty")
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return platformError(ErrInvalidConfig, "runtime is closed")
	}
	fs, ok := rt.funcs[name]
	if !ok {
		return platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	if _, exists := fs.versions[version]; exists {
		return platformError(ErrInvalidConfig, "version %q of function %q already exists", version, name)
	}
	p := newPool(rt, VersionSpec{Function: name, Version: version},
		fs.config.MaxInstancesPerVersion, fs.config.InstanceIdleTimeout)
	fs.versions[version] = &versionState{pool: p, active: true}
	if setCurrent {
		fs.current = version
	}
	return nil
}

// SetCurrent 原子切换当前版本；目标版本必须存在且处于启用状态。
func (rt *Runtime) SetCurrent(name, version string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return platformError(ErrInvalidConfig, "runtime is closed")
	}
	fs, ok := rt.funcs[name]
	if !ok {
		return platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	vs, ok := fs.versions[version]
	if !ok {
		return platformError(ErrVersionUnknown, "version %q of function %q never published", version, name)
	}
	if !vs.active {
		return platformError(ErrVersionDeactivated, "version %q of function %q is deactivated", version, name)
	}
	fs.current = version
	return nil
}

// UpdateConfig 原子更新函数配置。先完整校验新配置，再一次性替换并调整并发闸
// 与实例池上限；校验失败时旧配置、旧闸、旧路由全部继续可用。
func (rt *Runtime) UpdateConfig(name string, config FunctionConfig) error {
	config = config.withDefaults()
	if err := config.validate(); err != nil {
		return err
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return platformError(ErrInvalidConfig, "runtime is closed")
	}
	fs, ok := rt.funcs[name]
	if !ok {
		return platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	fs.config = config
	fs.gate.resize(config.MaxConcurrency, config.MaxQueue)
	for _, vs := range fs.versions {
		vs.pool.mu.Lock()
		vs.pool.max = config.MaxInstancesPerVersion
		vs.pool.idleTimeout = config.InstanceIdleTimeout
		vs.pool.mu.Unlock()
	}
	return nil
}

// DeactivateVersion 停用版本：不允许停用当前路由指向的版本（请先切换当前版本）。
// 停用后：空闲实例立即回收；在途调用按其开始时的版本跑完；新调用收到
// VERSION_DEACTIVATED；从未发布过的版本仍是 VERSION_UNKNOWN。
func (rt *Runtime) DeactivateVersion(name, version string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return platformError(ErrInvalidConfig, "runtime is closed")
	}
	fs, ok := rt.funcs[name]
	if !ok {
		return platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	vs, ok := fs.versions[version]
	if !ok {
		return platformError(ErrVersionUnknown, "version %q of function %q never published", version, name)
	}
	if fs.current == version {
		return platformError(ErrInvalidConfig,
			"cannot deactivate current version %q of function %q; switch current version first", version, name)
	}
	if vs.active {
		vs.active = false
		vs.pool.shutdownAll()
	}
	return nil
}

// CurrentVersion 返回当前路由版本，主要供观测与文档示例使用。
func (rt *Runtime) CurrentVersion(name string) (string, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	fs, ok := rt.funcs[name]
	if !ok {
		return "", platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	return fs.current, nil
}

// Stats 供测试与观测使用：返回 (活跃调用数, 排队数, 并发上限)。
func (rt *Runtime) GateStats(name string) (active, queued, max int, err error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	fs, ok := rt.funcs[name]
	if !ok {
		return 0, 0, 0, platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	a, q, m := fs.gate.stats()
	return a, q, m, nil
}

// VersionPoolStats 供测试与观测使用：返回某版本实例池的 (空闲, 忙碌, 上限)。
func (rt *Runtime) VersionPoolStats(name, version string) (idle, busy, max int, err error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	fs, ok := rt.funcs[name]
	if !ok {
		return 0, 0, 0, platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	vs, exists := fs.versions[version]
	if !exists {
		return 0, 0, 0, platformError(ErrVersionUnknown, "version %q never published", version)
	}
	i, b, m := vs.pool.stats()
	return i, b, m, nil
}

// resolve 按调用开始时的快照解析目标版本池。返回的 pool 在本次调用期间一直有效：
// 版本随后被停用只影响新调用，不影响已拿到快照的在途调用。
func (rt *Runtime) resolve(name, requestedVersion string) (*funcState, *versionState, FunctionConfig, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	fs, ok := rt.funcs[name]
	if !ok {
		return nil, nil, FunctionConfig{}, platformError(ErrFunctionNotFound, "function %q not registered", name)
	}
	version := requestedVersion
	if version == "" {
		version = fs.current
	}
	vs, exists := fs.versions[version]
	if !exists {
		return nil, nil, FunctionConfig{}, platformError(ErrVersionUnknown,
			"version %q of function %q never published", version, name)
	}
	if !vs.active {
		return nil, nil, FunctionConfig{}, platformError(ErrVersionDeactivated,
			"version %q of function %q is deactivated", version, name)
	}
	return fs, vs, fs.config, nil
}

// Invoke 调用函数。重复的 opts.CallID（含并发重复提交）全局只真正执行一次，
// 所有提交方拿到内容一致的 Result。ctx 可用于调用方主动取消排队/等待。
func (rt *Runtime) Invoke(ctx context.Context, name string, input any, opts InvokeOptions) *Result {
	if opts.CallID != "" {
		return rt.invokeWithDedup(ctx, name, input, opts)
	}
	return rt.invokeOnce(ctx, name, input, opts)
}

func (rt *Runtime) invokeWithDedup(ctx context.Context, name string, input any, opts InvokeOptions) *Result {
	id := opts.CallID

	rt.dedupMu.Lock()
	// 惰性清理过期记录，避免长期运行时表无界增长。
	if e := rt.dedup[id]; e != nil {
		select {
		case <-e.done:
			if rt.clock.Now().Sub(e.at) > rt.dedupTTL {
				delete(rt.dedup, id)
			}
		default:
		}
	}
	if e := rt.dedup[id]; e != nil {
		entry := e
		rt.dedupMu.Unlock()
		// 首个提交仍在执行：并发重复提交等待同一个结论；调用方 ctx 取消
		// 只影响自己的等待，不影响真正的执行，也不会改变最终结论。
		select {
		case <-entry.done:
			return cloneResult(entry.result)
		case <-ctx.Done():
			return &Result{
				Status: StatusPlatformError,
				Err:    platformError(ErrCallerCancelled, "cancelled while waiting for dedup result of call %q", id),
			}
		}
	}
	entry := &dedupEntry{done: make(chan struct{})}
	rt.dedup[id] = entry
	rt.dedupMu.Unlock()

	result := rt.invokeOnce(ctx, name, input, opts)

	rt.dedupMu.Lock()
	entry.result = result
	entry.at = rt.clock.Now()
	rt.dedupMu.Unlock()
	close(entry.done)
	return cloneResult(result)
}

func cloneResult(r *Result) *Result {
	cp := *r
	return &cp
}

type execOutcome struct {
	out any
	err error
}

// invokeOnce 完成一次真实调用：路由快照 -> 并发闸 -> 实例 -> 执行 -> 归一化。
func (rt *Runtime) invokeOnce(ctx context.Context, name string, input any, opts InvokeOptions) *Result {
	fs, vs, config, err := rt.resolve(name, opts.Version)
	if err != nil {
		return &Result{Status: StatusPlatformError, Err: err}
	}
	version := vs.pool.spec.Version

	queueTimeout := opts.QueueTimeout
	if queueTimeout <= 0 {
		queueTimeout = config.QueueTimeout
	}
	invokeTimeout := opts.InvokeTimeout
	if invokeTimeout <= 0 {
		invokeTimeout = config.InvokeTimeout
	}

	// 1) 函数级并发闸：超额排队，队列满立即过载。
	release, err := fs.gate.acquire(ctx, rt.clock, queueTimeout)
	if err != nil {
		return &Result{Status: StatusPlatformError, Err: err, Version: version}
	}
	defer release()

	// 2) 取得（必要时创建）该版本实例；排队期间版本被停用时给出明确拒绝。
	inst, err := vs.pool.acquire(ctx, rt.clock, queueTimeout)
	if err != nil {
		return &Result{Status: StatusPlatformError, Err: err, Version: version}
	}

	// 3) 在独立 goroutine 中执行，主流程用定时器决定调用结论。
	// arbiter 是“谁先定案”的唯一裁决（CAS）：
	//   - 执行 goroutine 先完成：正常归还实例（忙碌名额在 release 内释放一次）；
	//   - 平台先超时/取消：releaseBusySlot 释放名额一次，goroutine 返回后关实例。
	// 因此任何路径下忙碌名额都恰好释放一次，实例也恰好关闭/入队一次。
	execCtx, cancel := context.WithTimeout(ctx, invokeTimeout)
	done := make(chan execOutcome, 1)
	var arbiter atomic.Bool // false=未定案；执行先完成置 true

	finishGoroutine := func(oc execOutcome, healthy bool) {
		if arbiter.CompareAndSwap(false, true) {
			vs.pool.release(inst, healthy)
			done <- oc
			return
		}
		// 平台已先定案：迟到结果丢弃；goroutine 此刻已从 Execute 返回，
		// 名额已由平台释放，这里只关闭实例。
		vs.pool.closeDetached(inst)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				finishGoroutine(execOutcome{err: &PlatformError{
					Code:    ErrExecutorPanic,
					Message: "executor panicked during invocation",
					Cause:   normalizePanic(r),
				}}, false)
			}
		}()
		out, execErr := inst.Execute(execCtx, input)
		finishGoroutine(execOutcome{out: out, err: execErr}, true)
	}()

	select {
	case oc := <-done:
		cancel()
		if oc.err != nil {
			if pe, ok := oc.err.(*PlatformError); ok {
				return &Result{Status: StatusPlatformError, Err: pe, Version: version}
			}
			return &Result{Status: StatusFailed, Err: oc.err, Version: version}
		}
		return &Result{Status: StatusOK, Output: oc.out, Version: version}

	case <-execCtx.Done():
		// 结论此刻由平台确定：超时或调用方取消，迟到结果不得改变结论。
		cancel()
		if !arbiter.CompareAndSwap(false, true) {
			// 极小窗口：执行器同时完成并已归还空闲队列。平台结论仍优先：
			// 排空结果并把实例移出空闲队列关闭，保证不再被派发。
			<-done
			vs.pool.closeIdleInstance(inst)
		} else {
			// 执行器尚未报告完成：立即释放实例忙碌名额；不等待执行器。
			// 不配合 ctx 的执行器只会延迟自身实例的 Close，不再占用任何
			// 并发/实例名额，该实例也不在任何池队列中，绝不会收到新派发。
			vs.pool.releaseBusySlot()
		}
		switch ctx.Err() {
		case context.Canceled:
			return &Result{
				Status:  StatusPlatformError,
				Err:     platformError(ErrCallerCancelled, "caller cancelled invocation of %q@%s", name, version),
				Version: version,
			}
		default:
			return &Result{
				Status:  StatusPlatformError,
				Err:     platformError(ErrInvocationTimeout, "invocation of %q@%s exceeded %s", name, version, invokeTimeout),
				Version: version,
			}
		}
	}
}

// reaperLoop 周期性回收所有版本池中空闲超时的实例。FakeClock 下同样依赖
// After 触发，因此测试可以确定性推进时间。
func (rt *Runtime) reaperLoop() {
	defer rt.wg.Done()
	ch := rt.after(rt.reapInterval)
	for {
		select {
		case <-rt.stopCh:
			return
		case <-ch:
			rt.reapOnce()
			ch = rt.after(rt.reapInterval)
		}
	}
}

func (rt *Runtime) reapOnce() {
	now := rt.clock.Now()
	rt.mu.RLock()
	states := make([]*funcState, 0, len(rt.funcs))
	for _, fs := range rt.funcs {
		states = append(states, fs)
	}
	rt.mu.RUnlock()
	for _, fs := range states {
		for _, vs := range fs.versions {
			vs.pool.reapIdle(now)
		}
	}
}

// ReapNow 触发一次立即回收，主要供测试与示例确定性地验证回收行为。
func (rt *Runtime) ReapNow() int {
	now := rt.clock.Now()
	closed := 0
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	for _, fs := range rt.funcs {
		for _, vs := range fs.versions {
			closed += vs.pool.reapIdle(now)
		}
	}
	return closed
}
