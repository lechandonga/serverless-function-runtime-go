package runtime

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	fn   = "svc"
	ver1 = "v1"
	ver2 = "v2"
	ver3 = "v3-off"
)

// newTestRuntime 构造一个基于 FakeClock 的运行时；build 决定实例行为。
func newTestRuntime(t *testing.T, build func(spec VersionSpec) *scriptInstance, cfg FunctionConfig) (*Runtime, *FakeClock, *scriptFactory) {
	t.Helper()
	fac := newScriptFactory(build)
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	rt := New(fac.Factory(), Options{Clock: clock})
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.RegisterFunction(fn, ver1, cfg); err != nil {
		t.Fatalf("注册初始版本失败: %v", err)
	}
	if err := rt.PublishVersion(fn, ver2, false); err != nil {
		t.Fatalf("发布 v2 失败: %v", err)
	}
	if err := rt.PublishVersion(fn, ver3, false); err != nil {
		t.Fatalf("发布 v3 失败: %v", err)
	}
	if err := rt.DeactivateVersion(fn, ver3); err != nil {
		t.Fatalf("停用 v3 失败: %v", err)
	}
	return rt, clock, fac
}

func simpleBuild(spec VersionSpec) *scriptInstance {
	s := &scriptInstance{startCh: make(chan struct{}, 8)}
	if spec.Function == fn && spec.Version == ver1 {
		s.blockCh = make(chan struct{})
	}
	return s
}

// 业务一：版本与路由语义 —— 指定版本、当前版本、切换、未知/停用可区分拒绝；
// 在途调用在版本停用/切换后仍按开始时的版本跑完。
func TestVersionRoutingAndInFlightPin(t *testing.T) {
	t.Log("[业务] 版本注册/路由/更新 | [输入] 显式版本、当前版本、未知版本、停用版本、在途切换")
	rt, _, fac := newTestRuntime(t, simpleBuild, FunctionConfig{
		MaxConcurrency: 4, MaxQueue: 2,
		InvokeTimeout: time.Second, QueueTimeout: time.Second, InstanceIdleTimeout: time.Minute,
	})

	// v1 调用先阻塞，随后切换当前版本并停用 v1；v1 调用必须仍由 v1 实例跑完。
	v1done := make(chan *Result, 1)
	go func() {
		v1done <- rt.Invoke(context.Background(), fn, "a", InvokeOptions{Version: ver1})
	}()
	var v1inst *scriptInstance
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if all := fac.all(); len(all) > 0 {
			v1inst = all[0]
			break
		}
		time.Sleep(time.Millisecond)
	}
	if v1inst == nil {
		t.Fatal("[判定] v1 实例未被创建")
	}
	<-v1inst.startCh
	t.Log("[输入] 在途调用 call=inflight 已在 v1 开始执行，此时发布/切换路由并停用 v1")

	// 配置不合法的更新必须被整体拒绝，旧配置与路由不受影响。
	bad := FunctionConfig{MaxConcurrency: -1, MaxQueue: 1,
		InvokeTimeout: time.Second, QueueTimeout: time.Second, InstanceIdleTimeout: time.Minute}
	if err := rt.UpdateConfig(fn, bad); err == nil {
		t.Fatal("[判定] 非法配置更新应被拒绝")
	} else if pe, ok := err.(*PlatformError); !ok || pe.Code != ErrInvalidConfig {
		t.Fatalf("[判定] 期望 INVALID_CONFIG，实际 %v", err)
	}
	t.Log("[判定] 非法配置被整体拒绝，运行状态不变")

	if err := rt.SetCurrent(fn, ver2); err != nil {
		t.Fatal(err)
	}
	// 当前版本是 v2，不能停用；停用 v1 需要先确认它不是当前版本（已是 v2）。
	if err := rt.DeactivateVersion(fn, ver2); err == nil {
		t.Fatal("[判定] 不允许停用当前路由版本")
	}
	if err := rt.DeactivateVersion(fn, ver1); err != nil {
		t.Fatalf("停用 v1 失败: %v", err)
	}

	close(v1inst.blockCh)
	r := <-v1done
	expectOK(t, r, "svc@v1:a", ver1)
	t.Logf("[判定] 在途调用完成版本=%s，未被版本更新影响", r.Version)

	// 新调用：默认走 v2；显式 v1 得到 DEACTIVATED；不存在版本得到 UNKNOWN。
	expectOK(t, rt.Invoke(context.Background(), fn, "b", InvokeOptions{}), "svc@v2:b", ver2)
	expectCode(t, rt.Invoke(context.Background(), fn, "c", InvokeOptions{Version: ver1}), ErrVersionDeactivated)
	expectCode(t, rt.Invoke(context.Background(), fn, "c", InvokeOptions{Version: "v9"}), ErrVersionUnknown)
	expectCode(t, rt.Invoke(context.Background(), fn, "c", InvokeOptions{Version: ver3}), ErrVersionDeactivated)
	expectCode(t, rt.Invoke(context.Background(), "missing", "c", InvokeOptions{}), ErrFunctionNotFound)

	// 重新发布 v1（历史版本已停用）必须拒绝版本号冲突，证明条目仍然保留。
	if err := rt.PublishVersion(fn, ver1, false); err == nil {
		t.Fatal("[判定] 已存在的版本号不得重复发布")
	}
}

// 业务二：并发上限与排队/过载，含动态调整上限。
func TestConcurrencyLimitQueueAndOverload(t *testing.T) {
	t.Log("[业务] 并发闸 | [输入] maxConcurrency=2,maxQueue=1，连续 4 个调用 + 动态扩容")

	// 共享 block 通道的工厂：前 2 个实例会同时处于执行中，用于观测真实并发度。
	block := make(chan struct{})
	fac := newScriptFactory(func(spec VersionSpec) *scriptInstance {
		s := &scriptInstance{startCh: make(chan struct{}, 8)}
		s.blockCh = block
		return s
	})
	clock := NewFakeClock(time.Unix(1_700_000_000, 0)).WithRealFallback()
	rt := New(fac.Factory(), Options{Clock: clock})
	defer rt.Close()
	cfg := FunctionConfig{MaxConcurrency: 2, MaxQueue: 1, MaxInstancesPerVersion: 2,
		InvokeTimeout: 5 * time.Second, QueueTimeout: 5 * time.Second, InstanceIdleTimeout: time.Minute}
	if err := rt.RegisterFunction(fn, ver1, cfg); err != nil {
		t.Fatal(err)
	}

	results := make(chan *Result, 4)
	start := func(id string) {
		go func() { results <- rt.Invoke(context.Background(), fn, id, InvokeOptions{}) }()
	}
	start("c1")
	s1 := waitInstance(t, fac, 0)
	<-s1.startCh
	start("c2")
	s2 := waitInstance(t, fac, 1)
	<-s2.startCh // 第 2 个名额触发新建，两个实例并行执行
	t.Log("[输入] c1,c2 已占用 2 个并发名额")

	start("c3") // 排队
	time.Sleep(30 * time.Millisecond)
	active, queued, mx, _ := rt.GateStats(fn)
	t.Logf("[观测] active=%d queued=%d max=%d", active, queued, mx)
	if active != 2 || queued != 1 || mx != 2 {
		t.Fatalf("[判定] 期望 active=2 queued=1 max=2，实际 %d/%d/%d", active, queued, mx)
	}

	overloaded := rt.Invoke(context.Background(), fn, "c4", InvokeOptions{})
	expectCode(t, overloaded, ErrOverloaded)

	// 动态扩容到 3：排队者 c3 应立即拿到名额。
	if err := rt.UpdateConfig(fn, FunctionConfig{MaxConcurrency: 3, MaxQueue: 1, MaxInstancesPerVersion: 3,
		InvokeTimeout: 5 * time.Second, QueueTimeout: 5 * time.Second, InstanceIdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if a, q, _, _ := rt.GateStats(fn); a != 3 || q != 0 {
		t.Fatalf("[判定] 扩容后期望 active=3 queued=0，实际 %d/%d", a, q)
	}
	t.Log("[判定] 扩容后排队者 c3 被放行，active=3")

	close(block)
	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		r := <-results
		if r.Status != StatusOK {
			t.Fatalf("[判定] c1-c3 应成功，实际 %v", r.Err)
		}
		got[r.Output.(string)] = true
	}
	if !got["svc@v1:c1"] || !got["svc@v1:c2"] || !got["svc@v1:c3"] {
		t.Fatalf("[判定] 缺少成功结果: %v", got)
	}
	t.Log("[判定] c1,c2,c3 全部成功；c4 明确过载拒绝")

	// 排队超时：容量 1、队列 1、队列超时很短，第二个调用最终 QUEUED_TIMEOUT。
	block2 := make(chan struct{})
	fac2 := newScriptFactory(func(spec VersionSpec) *scriptInstance {
		return &scriptInstance{startCh: make(chan struct{}, 4), blockCh: block2}
	})
	rt2 := New(fac2.Factory(), Options{Clock: clock})
	defer rt2.Close()
	if err := rt2.RegisterFunction(fn, ver1, FunctionConfig{MaxConcurrency: 1, MaxQueue: 1,
		MaxInstancesPerVersion: 1, InvokeTimeout: time.Hour, QueueTimeout: 50 * time.Millisecond,
		InstanceIdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = rt2.Invoke(context.Background(), fn, "hold", InvokeOptions{}) }()
	<-waitInstance(t, fac2, 0).startCh
	qRes := rt2.Invoke(context.Background(), fn, "late", InvokeOptions{})
	expectCode(t, qRes, ErrQueuedTimeout)
	close(block2)
}

// 业务三：超时取消 —— 超时结论立即确定，迟到结果不得改变结论；
// 不配合取消的执行器返回后其实例被回收，不影响后续调用。
func TestTimeoutCancelAndLateResult(t *testing.T) {
	t.Log("[业务] 超时与迟到结果 | [输入] call=slow（忽略 ctx 永不返回）后紧跟 call=next")
	block := make(chan struct{})
	fac := newScriptFactory(func(spec VersionSpec) *scriptInstance {
		return &scriptInstance{startCh: make(chan struct{}, 4), blockCh: block, ignoreCtx: true}
	})
	clock := NewFakeClock(time.Unix(1_700_000_000, 0)).WithRealFallback()
	rt := New(fac.Factory(), Options{Clock: clock})
	defer rt.Close()
	if err := rt.RegisterFunction(fn, ver1, FunctionConfig{MaxConcurrency: 2, MaxQueue: 0,
		MaxInstancesPerVersion: 2, InvokeTimeout: 80 * time.Millisecond, QueueTimeout: time.Second,
		InstanceIdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}

	slowRes := make(chan *Result, 1)
	go func() {
		slowRes <- rt.Invoke(context.Background(), fn, "slow", InvokeOptions{})
	}()
	<-waitInstance(t, fac, 0).startCh

	var r *Result
	select {
	case r = <-slowRes:
	case <-time.After(2 * time.Second):
		t.Fatal("[判定] 超时后调用未在预期时间内返回结论")
	}
	expectCode(t, r, ErrInvocationTimeout)
	if r.Version != ver1 {
		t.Fatalf("[判定] 超时结果也应带版本快照，实际 %q", r.Version)
	}

	// 超时后执行器仍被 block 卡住（忽略 ctx）：名额与实例名额必须已经释放，
	// next 调用复用池（新建实例）成功，证明没有把调用派发给那个半死实例。
	nextRes := make(chan *Result, 1)
	go func() { nextRes <- rt.Invoke(context.Background(), fn, "next", InvokeOptions{}) }()
	select {
	case <-waitInstance(t, fac, 1).startCh:
	case <-time.After(2 * time.Second):
		t.Fatal("[判定] 超时后实例名额未释放，next 无法开始")
	}
	// next 已经在新实例上开始执行。此时再放行 block：slow 的迟到结果必须
	// 被丢弃，且 next 也得以正常完成。
	close(block)
	select {
	case nr := <-nextRes:
		expectOK(t, nr, "svc@v1:next", ver1)
	case <-time.After(2 * time.Second):
		t.Fatal("[判定] next 调用未完成")
	}
	t.Logf("[判定] slow 的结论保持为首次确定的 %s，迟到结果被丢弃；%v", r.Status, r.Err)
	time.Sleep(30 * time.Millisecond)
	if fac.all()[0].closeCount() != 1 {
		t.Fatalf("[判定] 半死实例应被关闭 1 次，实际 %d", fac.all()[0].closeCount())
	}

	// 调用方主动取消（排队等待中）应归一化为 CALLER_CANCELLED。
	hold := make(chan struct{})
	fac2 := newScriptFactory(func(spec VersionSpec) *scriptInstance {
		return &scriptInstance{startCh: make(chan struct{}, 4), blockCh: hold}
	})
	rt2 := New(fac2.Factory(), Options{Clock: clock})
	defer rt2.Close()
	if err := rt2.RegisterFunction(fn, ver1, FunctionConfig{MaxConcurrency: 1, MaxQueue: 2,
		MaxInstancesPerVersion: 1, InvokeTimeout: time.Hour, QueueTimeout: time.Hour,
		InstanceIdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = rt2.Invoke(context.Background(), fn, "hold", InvokeOptions{}) }()
	<-waitInstance(t, fac2, 0).startCh
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	cr := rt2.Invoke(cctx, fn, "cancel-me", InvokeOptions{})
	expectCode(t, cr, ErrCallerCancelled)
	close(hold)
}

// 业务四：实例生命周期 —— 复用、空闲超时回收、回收后重建；
// 回收与派发并发时不得把调用派发给已回收实例。
func TestInstanceReuseReclaimAndRebuild(t *testing.T) {
	t.Log("[业务] 实例复用/空闲回收 | [输入] 两次连续调用复用，推进时钟回收，再调用重建")
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	fac := newScriptFactory(func(spec VersionSpec) *scriptInstance { return &scriptInstance{} })
	rt := New(fac.Factory(), Options{Clock: clock})
	defer rt.Close()
	if err := rt.RegisterFunction(fn, ver1, FunctionConfig{MaxConcurrency: 2, MaxQueue: 0,
		MaxInstancesPerVersion: 2, InvokeTimeout: time.Second, QueueTimeout: time.Second,
		InstanceIdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}

	expectOK(t, rt.Invoke(context.Background(), fn, "first", InvokeOptions{}), "svc@v1:first", ver1)
	expectOK(t, rt.Invoke(context.Background(), fn, "second", InvokeOptions{}), "svc@v1:second", ver1)
	if len(fac.all()) != 1 {
		t.Fatalf("[判定] 连续调用应复用同一实例，实际创建 %d 个", len(fac.all()))
	}
	if fac.all()[0].execCount() != 2 {
		t.Fatalf("[判定] 复用实例应执行 2 次，实际 %d", fac.all()[0].execCount())
	}
	idle, busy, _, _ := rt.VersionPoolStats(fn, ver1)
	if idle != 1 || busy != 0 {
		t.Fatalf("[判定] 调用后应 1 空闲 0 忙碌，实际 idle=%d busy=%d", idle, busy)
	}
	t.Log("[判定] 实例复用：1 个实例执行 2 次")

	// 推进 30s，不足空闲超时，不应回收。
	clock.Advance(30 * time.Second)
	if n := rt.ReapNow(); n != 0 {
		t.Fatalf("[判定] 未到空闲超时不应回收，实际回收 %d", n)
	}
	// 推进到 >=1m，应回收 1 个。
	clock.Advance(40 * time.Second)
	if n := rt.ReapNow(); n != 1 {
		t.Fatalf("[判定] 空闲超时应回收 1 个实例，实际 %d", n)
	}
	if fac.all()[0].closeCount() != 1 {
		t.Fatalf("[判定] 被回收实例应 Close 1 次，实际 %d", fac.all()[0].closeCount())
	}
	t.Log("[判定] 空闲实例在 idleTimeout 后被回收并 Close")

	// 回收后再次调用：必须新建实例，旧实例不再收到派发。
	expectOK(t, rt.Invoke(context.Background(), fn, "third", InvokeOptions{}), "svc@v1:third", ver1)
	if len(fac.all()) != 2 {
		t.Fatalf("[判定] 回收后应重建，实例总数应为 2，实际 %d", len(fac.all()))
	}
	if fac.all()[0].execCount() != 2 || fac.all()[1].execCount() != 1 {
		t.Fatalf("[判定] 旧实例不应再被派发，执行次数 old=%d new=%d",
			fac.all()[0].execCount(), fac.all()[1].execCount())
	}
	t.Log("[判定] 回收后调用成功重建，派发没有落到已回收实例")
}

// 业务五：重复提交幂等 —— 相同 CallID 串行/并发都只执行一次，结论一致。
func TestIdempotentSubmission(t *testing.T) {
	t.Log("[业务] 幂等 | [输入] CallID=dup-1 串行重放 + 8 个并发重复提交")
	fac := newScriptFactory(func(spec VersionSpec) *scriptInstance {
		return &scriptInstance{startCh: make(chan struct{}, 16)}
	})
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	rt := New(fac.Factory(), Options{Clock: clock})
	defer rt.Close()
	if err := rt.RegisterFunction(fn, ver1, FunctionConfig{MaxConcurrency: 8, MaxQueue: 0,
		MaxInstancesPerVersion: 8, InvokeTimeout: time.Second, QueueTimeout: time.Second,
		InstanceIdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}

	opts := InvokeOptions{CallID: "dup-1"}
	r1 := rt.Invoke(context.Background(), fn, "payload", opts)
	r2 := rt.Invoke(context.Background(), fn, "payload-changed-but-ignored", opts)
	expectOK(t, r1, "svc@v1:payload", ver1)
	if r2.Output != r1.Output || r2.Status != r1.Status || r2.Version != r1.Version {
		t.Fatalf("[判定] 串行重放结论不一致: %+v vs %+v", r1, r2)
	}

	// 8 个并发重复提交，全部等到同一结论；执行器总计只执行 1 次。
	var wg sync.WaitGroup
	rs := make([]*Result, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rs[idx] = rt.Invoke(context.Background(), fn, "payload", opts)
		}(i)
	}
	wg.Wait()
	for i, rr := range rs {
		if rr.Output != r1.Output || rr.Status != r1.Status {
			t.Fatalf("[判定] 并发重放 #%d 结论不一致: %+v", i, rr)
		}
	}
	totalExec := 0
	for _, s := range fac.all() {
		totalExec += s.execCount()
	}
	if totalExec != 1 {
		t.Fatalf("[判定] 相同 CallID 必须只真正执行一次，实际执行 %d 次", totalExec)
	}
	t.Logf("[判定] 串行+并发共 10 次提交，执行器仅执行 %d 次，结论全部一致", totalExec)
}

// 业务六：执行器 panic 与工厂失败的隔离与归一化，不影响其他函数。
func TestPanicAndFactoryFailureIsolation(t *testing.T) {
	t.Log("[业务] 异常归一化/故障隔离 | [输入] panic 调用、工厂失败调用、健康函数调用")
	block := make(chan struct{})
	_ = block
	fac := New(func(spec VersionSpec) (Instance, error) {
		switch {
		case spec.Function == "boom" && spec.Version == "panic":
			return NewFuncInstance(func(ctx context.Context, in any) (any, error) {
				panic("kaboom")
			}, nil), nil
		case spec.Function == "boom" && spec.Version == "broken-factory":
			return nil, errors.New("disk on fire")
		case spec.Function == "healthy":
			return NewFuncInstance(func(ctx context.Context, in any) (any, error) {
				return "alive:" + in.(string), nil
			}, nil), nil
		default:
			return nil, platformError(ErrInstanceCreate, "missing handler")
		}
	}, Options{})
	defer fac.Close()

	if err := fac.RegisterFunction("boom", "panic", FunctionConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := fac.PublishVersion("boom", "broken-factory", true); err != nil {
		t.Fatal(err)
	}
	if err := fac.RegisterFunction("healthy", "v1", FunctionConfig{}); err != nil {
		t.Fatal(err)
	}

	// 当前版本切到 broken-factory：实例创建失败被归一化。
	expectCode(t, fac.Invoke(context.Background(), "boom", "x", InvokeOptions{}), ErrInstanceCreate)
	// 指定 panic 版本：panic 被恢复并归一化，调用进程不崩溃。
	pr := fac.Invoke(context.Background(), "boom", "x", InvokeOptions{Version: "panic"})
	expectCode(t, pr, ErrExecutorPanic)
	if pe := pr.Err.(*PlatformError); !strings.Contains(pe.Cause.Error(), "kaboom") {
		t.Fatalf("[判定] panic 原因应保留，实际 %v", pe.Cause)
	}
	// panic 后同函数/版本可继续调用（实例被销毁，下一次重建）。
	pr2 := fac.Invoke(context.Background(), "boom", "x", InvokeOptions{Version: "panic"})
	expectCode(t, pr2, ErrExecutorPanic)
	// 其他函数完全不受影响。
	hr := fac.Invoke(context.Background(), "healthy", "ping", InvokeOptions{})
	expectOK(t, hr, "alive:ping", "v1")
	t.Log("[判定] panic/工厂失败均被归一化且隔离，健康函数与后续调用正常")
}

// 业务七：回收、派发、版本更新并发交织 —— race 下多轮稳定；
// 任何时刻都不能把调用派发给已回收/已停用版本的实例。
func TestConcurrentReclaimDispatchAndVersionUpdates(t *testing.T) {
	t.Log("[业务] 并发交织 | [输入] 60 个调用派发 + 持续回收 + 版本切换/停用/重发布，worker=8")
	clock := NewFakeClock(time.Unix(1_700_000_000, 0)).WithRealFallback()
	fac := newScriptFactory(func(spec VersionSpec) *scriptInstance {
		return &scriptInstance{startCh: make(chan struct{}, 64)}
	})
	rt := New(fac.Factory(), Options{Clock: clock, ReapInterval: 20 * time.Millisecond})
	defer rt.Close()
	if err := rt.RegisterFunction(fn, ver1, FunctionConfig{MaxConcurrency: 4, MaxQueue: 64,
		MaxInstancesPerVersion: 3, InvokeTimeout: 2 * time.Second, QueueTimeout: 2 * time.Second,
		InstanceIdleTimeout: 30 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{ver2, "v4", "v5"} {
		if err := rt.PublishVersion(fn, v, false); err != nil {
			t.Fatal(err)
		}
	}
	// retired 版本：反复停用 -> 以新版本号重建 -> 切换 -> 再停用，
	// 制造“停用/发布/回收/派发”同时发生的场景。
	if err := rt.PublishVersion(fn, "retired-0", false); err != nil {
		t.Fatal(err)
	}
	var retiredSeq atomic.Int64

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var workerWg sync.WaitGroup

	// 回收推进：真实时间 + FakeClock 双驱动，制造高回收频率。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				clock.Advance(25 * time.Millisecond)
				rt.ReapNow()
			}
		}
	}()

	// 版本切换：在 v1/v2/v4/v5 之间反复切当前路由，并停用/发布非当前版本。
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 在一组始终保持启用的版本之间切换当前路由；
		// 停用/发布 retired-N 版本的动作由另一个角色负责。
		alive := []string{ver1, ver2, "v4", "v5"}
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = rt.SetCurrent(fn, alive[i%len(alive)])
			// 并发校验非法更新被原子拒绝（旧状态继续可用）。
			_ = rt.UpdateConfig(fn, FunctionConfig{MaxConcurrency: -1, MaxQueue: 64,
				MaxInstancesPerVersion: 3, InvokeTimeout: 2 * time.Second,
				QueueTimeout: 2 * time.Second, InstanceIdleTimeout: 30 * time.Millisecond})
			i++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 版本停用/发布者：把上一个 retired-N 停用，再发布 retired-(N+1)，
	// 并偶尔把当前路由短暂切到新版本；与调用派发、回收持续竞争。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(3 * time.Millisecond):
			}
			n := retiredSeq.Load()
			oldName := "retired-0"
			if n > 0 {
				oldName = "retired-" + strconv.FormatInt(n, 10)
			}
			_ = rt.SetCurrent(fn, ver1) // 确保 oldName 不是当前版本
			_ = rt.DeactivateVersion(fn, oldName)
			newName := "retired-" + strconv.FormatInt(n+1, 10)
			if err := rt.PublishVersion(fn, newName, false); err == nil {
				retiredSeq.Add(1)
			}
		}
	}()

	// 调用派发：只调用当前版本；每次结果都必须可解释。
	codes := map[ErrorCode]int{}
	var okCount int
	var mu sync.Mutex
	for w := 0; w < 8; w++ {
		workerWg.Add(1)
		go func(worker int) {
			defer workerWg.Done()
			for i := 0; i < 60; i++ {
				r := rt.Invoke(context.Background(), fn, "load", InvokeOptions{})
				mu.Lock()
				switch r.Status {
				case StatusOK:
					okCount++
					if r.Version == "" {
						t.Errorf("[判定] 成功结果必须带实际版本")
					}
				case StatusFailed:
					t.Errorf("[判定] 简单执行器不应返回业务错误: %v", r.Err)
				default:
					if pe, ok := r.Err.(*PlatformError); ok {
						switch pe.Code {
						case ErrOverloaded, ErrQueuedTimeout, ErrVersionDeactivated,
							ErrInvocationTimeout, ErrInstanceCreate:
							codes[pe.Code]++
						default:
							t.Errorf("[判定] 未预期的平台错误码: %s", pe.Code)
						}
					}
				}
				mu.Unlock()
			}
		}(w)
	}
	workerWg.Wait()
	close(stop)
	wg.Wait()

	t.Logf("[判定] 交织结束 OK=%d 平台拒绝分布=%v（拒绝均属可解释码）", okCount, codes)
	if okCount == 0 {
		t.Fatal("[判定] 并发交织期间至少应有调用成功")
	}
	// 最终把当前路由停回稳定状态，验证在途收尾不 panic：再做一次串行成功调用。
	if err := rt.SetCurrent(fn, ver1); err != nil {
		t.Fatal(err)
	}
	final := rt.Invoke(context.Background(), fn, "final", InvokeOptions{})
	if final.Status != StatusOK {
		t.Fatalf("[判定] 收敛后串行调用应成功，实际 %v", final.Err)
	}
}
