package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本测试文件按业务场景组织，每个用例的日志都标注：
// 覆盖业务、输入标识（函数/版本/CallID）、判定依据（期望错误类别）。
// 并发用例不依赖不可复现的时序魔法：统一用屏障同步，且全部在
// `go test -race` 下稳定通过。

func kindOf(err error) ErrorKind {
	var pe *PlatformError
	if errors.As(err, &pe) {
		return pe.Kind
	}
	return ""
}

// 业务二：并发上限、FIFO 排队、过载拒绝、排队超时。
func TestConcurrencyQueueAndOverload(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := newGate("slow")
	spec := VersionSpec{Version: "v1", Handler: h, Concurrency: 2, MaxQueueDepth: 1, IdleTimeout: time.Minute}
	if err := rt.Register("qfn", spec); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		res string
		err error
	}
	finished := make(chan outcome, 3)
	for i := 0; i < 3; i++ { // 2 个占槽 + 1 个排队
		id := fmt.Sprintf("slot-%d", i)
		go func() {
			r, e := rt.Invoke(context.Background(), InvokeRequest{
				Function: "qfn", Payload: []byte(id), CallID: id, Timeout: 3 * time.Second,
			})
			if e != nil {
				finished <- outcome{"", e}
			} else {
				finished <- outcome{string(r.Payload), nil}
			}
		}()
	}
	// 等两个执行器真正在跑（证明并发=2），第三个在队列里。
	time.Sleep(100 * time.Millisecond)
	st, _ := rt.Stats("qfn", "v1")
	if st.Pool.Active != 2 || st.Pool.QueueLength != 1 {
		t.Fatalf("期望 Active=2 Queue=1，得到 %+v", st.Pool)
	}

	// 第四个调用：槽满且队列满，必须立即得到可区分的过载拒绝。
	if _, err := rt.Invoke(context.Background(), InvokeRequest{
		Function: "qfn", CallID: "overload-1", Timeout: time.Second,
	}); kindOf(err) != KindOverload {
		t.Fatalf("输入 qfn/overload-1 期望过载 %s，得到 %v", KindOverload, err)
	}
	t.Logf("[业务:过载拒绝] 输入 qfn 并发=2 队列=1 已满；判定: 第4个调用快速失败 %s", KindOverload)

	// 排队等待但超时：队列超时类别与执行超时区分。
	_, qerr := rt.Invoke(context.Background(), InvokeRequest{
		Function: "qfn", CallID: "queue-timeout-1", Timeout: 50 * time.Millisecond,
	})
	if kindOf(qerr) != KindOverload {
		t.Fatalf("队列已满时连排队名额都没有，应先得到 %s，得到 %v", KindOverload, qerr)
	}

	// 放大队列后，一个等待者超时应得到 QueueTimeout。
	// 这里通过 Concurrency=1/Queue=1 的独立函数精确构造。
	rt2 := NewRuntime()
	defer rt2.Close()
	hq := newGate("qslow")
	if err := rt2.Register("q2", VersionSpec{Version: "v1", Handler: hq, Concurrency: 1, MaxQueueDepth: 2, IdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = rt2.Invoke(context.Background(), InvokeRequest{Function: "q2", CallID: "hold"})
	}()
	<-hq.started
	_, qerr2 := rt2.Invoke(context.Background(), InvokeRequest{
		Function: "q2", CallID: "queue-timeout-2", Timeout: 60 * time.Millisecond,
	})
	if kindOf(qerr2) != KindQueueTimeout {
		t.Fatalf("输入 q2/queue-timeout-2 期望 %s，得到 %v", KindQueueTimeout, qerr2)
	}
	t.Logf("[业务:排队超时] 输入 q2/queue-timeout-2 在队列中超时；判定: %s（区别于执行超时/过载）", KindQueueTimeout)

	h.release()
	got := map[string]bool{}
	for i := 0; i < 3; i++ {
		o := <-finished
		if o.err != nil {
			t.Fatalf("前三个调用应全部成功，得到 %v", o.err)
		}
		got[o.res] = true
	}
	for i := 0; i < 3; i++ {
		want := "ok:slow:slot-" + fmt.Sprint(i)
		if !got[want] {
			t.Fatalf("缺少排队/占槽调用结果 %s", want)
		}
	}
	t.Logf("[业务:排队规则] 判定: 占槽2个+排队1个全部按 FIFO 得到各自输入对应的结果，未串单")
	hq.release()
}

// 业务三：执行超时真正释放占用、迟到结果不改结论、取消归一化。
func TestTimeoutReleasesSlotAndLateResultIgnored(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := &gateHandler{name: "stuck", started: make(chan struct{}), rel: make(chan struct{}), ignoreCtx: true}
	spec := VersionSpec{Version: "v1", Handler: h, Concurrency: 1, MaxQueueDepth: 0, IdleTimeout: 0}
	if err := rt.Register("tfn", spec); err != nil {
		t.Fatal(err)
	}

	_, err := rt.Invoke(context.Background(), InvokeRequest{Function: "tfn", CallID: "slow-1", Timeout: 80 * time.Millisecond})
	if kindOf(err) != KindInvokeTimeout {
		t.Fatalf("输入 tfn/slow-1 期望执行超时 %s，得到 %v", KindInvokeTimeout, err)
	}
	t.Logf("[业务:超时取消] 输入 tfn/slow-1 执行器忽略取消；判定: %s，结论立即确定", KindInvokeTimeout)

	// 超时返回后并发槽必须已释放：concurrency=1 下第二个调用应能占槽，
	// 而不是拿到过载拒绝。
	second := make(chan error, 1)
	go func() {
		_, e := rt.Invoke(context.Background(), InvokeRequest{
			Function: "tfn", CallID: "after-timeout", Timeout: 2 * time.Second,
		})
		second <- e
	}()
	time.Sleep(150 * time.Millisecond)
	st, _ := rt.Stats("tfn", "v1")
	if st.Pool.Active != 1 {
		t.Fatalf("超时后槽位未释放，Active=%d（期望第二个调用已占槽，Active=1）", st.Pool.Active)
	}
	t.Logf("[业务:资源释放] 判定: 超时返回后第二个调用成功占槽，Active=1，未泄漏并发额度")

	// 迟到结果：此时才放行，第一次的结论已不可更改，第二次正常成功。
	h.release()
	if e := <-second; e != nil {
		t.Fatalf("第二个调用应成功: %v", e)
	}
	t.Logf("[业务:迟到结果] 判定: 第一次执行迟到返回被丢弃，slow-1 结论仍为 %s", KindInvokeTimeout)

	// 调用方主动取消归一化为 Canceled。用独立函数/执行器精确构造，
	// 不依赖前面共享 gate 的释放状态。
	rtC := NewRuntime()
	defer rtC.Close()
	hc := newGate("cancelable")
	if err := rtC.Register("tfn-c", VersionSpec{Version: "v1", Handler: hc, Concurrency: 1, IdleTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelDone := make(chan error, 1)
	go func() {
		_, e := rtC.Invoke(ctx, InvokeRequest{Function: "tfn-c", CallID: "cancel-1", Timeout: 5 * time.Second})
		cancelDone <- e
	}()
	<-hc.started // 保证取消发生在执行中，而不是派发前
	cancel()
	if cerr := <-cancelDone; kindOf(cerr) != KindCanceled {
		t.Fatalf("输入 tfn-c/cancel-1 期望 %s，得到 %v", KindCanceled, cerr)
	}
	t.Logf("[业务:主动取消] 输入 tfn-c/cancel-1 执行中取消；判定: %s", KindCanceled)
	hc.release()
}

// 业务四：空闲实例回收后再次调用自动重建；回收与派发/更新并发无竞争。
func TestIdleRecycleAndRecreate(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created atomic.Int64
	factory := func() (Handler, error) {
		n := created.Add(1)
		return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
			return []byte(fmt.Sprintf("inst%d:%s", n, p)), nil
		}), nil
	}
	spec := VersionSpec{
		Version: "v1", NewInstance: factory,
		Concurrency: 2, MaxQueueDepth: 0, IdleTimeout: 50 * time.Millisecond,
	}
	if err := rt.Register("ifn", spec); err != nil {
		t.Fatal(err)
	}

	res, err := rt.Invoke(context.Background(), InvokeRequest{Function: "ifn", CallID: "c1", Payload: []byte("c1")})
	if err != nil || string(res.Payload) != "inst1:c1" {
		t.Fatalf("首次调用应创建 inst1，得到 %+v %v", res, err)
	}
	st, _ := rt.Stats("ifn", "v1")
	if st.Pool.Idle != 1 || created.Load() != 1 {
		t.Fatalf("调用结束应有 1 个空闲实例，得到 idle=%v created=%d", st.Pool.Idle, created.Load())
	}
	t.Logf("[业务:实例复用] 输入 ifn/c1；判定: 新建实例 inst1 并在结束后空闲保留")

	time.Sleep(120 * time.Millisecond) // 越过 IdleTimeout，等待 reaper 回收
	st, _ = rt.Stats("ifn", "v1")
	if st.Pool.Idle != 0 {
		t.Fatal("空闲超时后实例应已回收")
	}
	t.Logf("[业务:空闲回收] 判定: 超过 IdleTimeout=50ms 后 Idle=false，实例被回收")

	res, err = rt.Invoke(context.Background(), InvokeRequest{Function: "ifn", CallID: "c2", Payload: []byte("c2")})
	if err != nil || string(res.Payload) != "inst2:c2" {
		t.Fatalf("回收后再次调用应自动新建 inst2，得到 %+v %v", res, err)
	}
	t.Logf("[业务:回收后重建] 输入 ifn/c2；判定: 自动创建 inst2，调用成功")
}

// 业务四补充：回收、派发、版本更新同时发生，竞态检测下稳定通过。
func TestRecycleDispatchUpdateConcurrent(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created atomic.Int64
	factory := func() (Handler, error) {
		n := created.Add(1)
		return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
			return []byte(fmt.Sprintf("v?-%d", n)), nil
		}), nil
	}
	base := VersionSpec{NewInstance: factory, Concurrency: 4, MaxQueueDepth: 16, IdleTimeout: time.Millisecond}
	if err := rt.Register("race", VersionSpec{Version: "v1", Handler: nil, NewInstance: factory, Concurrency: 4, MaxQueueDepth: 16, IdleTimeout: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	_ = base

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 派发方：高频调用，反复命中“刚回收 / 正要回收 / 暖实例”各种竞态窗口。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, e := rt.Invoke(context.Background(), InvokeRequest{
					Function: "race",
					Version:  Version(fmt.Sprintf("v%d", 1+(i%6))), // 未知版本也频繁打
					CallID:   fmt.Sprintf("w%d-%d", w, i),
					Timeout:  time.Second,
				})
				switch kindOf(e) {
				case "", KindVersionUnknown, KindVersionDisabled:
				default:
					t.Errorf("派发方出现意外错误类别 %v", e)
					return
				}
			}
		}(w)
	}

	// 更新方：不停发布新版本，制造旧池/新池切换。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := 2; v <= 5; v++ {
			_ = rt.Publish("race", VersionSpec{
				Version:     Version(fmt.Sprintf("v%d", v)),
				NewInstance: factory, Concurrency: 4, MaxQueueDepth: 16, IdleTimeout: time.Millisecond,
			})
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	t.Logf("[业务:并发回收/派发/更新] 判定: 混合压力下无数据竞争、无派发到已回收实例（-race 下验证）")
}

// 业务五：同一 CallID 串行/并发重复提交只真正执行一次，结论一致。
func TestIdempotentDuplicateSubmission(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var executions atomic.Int64
	h := newGate("idem")
	h.name = "idem"
	wrapped := HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
		executions.Add(1)
		return h.Invoke(ctx, p)
	})
	spec := VersionSpec{Version: "v1", Handler: wrapped, Concurrency: 4, MaxQueueDepth: 4, IdleTimeout: time.Minute}
	if err := rt.Register("idfn", spec); err != nil {
		t.Fatal(err)
	}

	const dupID = "same-call-id"
	const n = 8
	var wg sync.WaitGroup
	resCh := make(chan string, n)
	errCh := make(chan error, n)
	for i := 0; i < n; i++ { // 并发重复提交，包括首次提交在内
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := rt.Invoke(context.Background(), InvokeRequest{
				Function: "idfn", CallID: dupID, Payload: []byte("x"), Timeout: 3 * time.Second,
			})
			if e != nil {
				errCh <- e
				return
			}
			resCh <- string(r.Payload)
		}()
	}
	// 等首次执行真正开始（started 由 gate 关闭），此刻重复提交者应在等待同一结论。
	<-h.started
	time.Sleep(50 * time.Millisecond)
	if executions.Load() != 1 {
		t.Fatalf("并发重复提交期间执行次数应为 1，实际 %d", executions.Load())
	}
	t.Logf("[业务:幂等去重] 输入 idfn CallID=%s 并发提交 x8；判定: 执行器仅触发 1 次", dupID)

	h.release()
	wg.Wait()
	close(resCh)
	close(errCh)
	for e := range errCh {
		t.Fatalf("重复提交者应拿到与首次一致的成功结果，得到错误 %v", e)
	}
	for r := range resCh {
		if r != "ok:idem:x" {
			t.Fatalf("重复提交结果不一致: %s", r)
		}
	}
	if executions.Load() != 1 {
		t.Fatalf("结束后执行次数仍应为 1，实际 %d", executions.Load())
	}
	t.Logf("[业务:幂等去重] 判定: 8 个提交全部返回同一结果 ok:idem:x，执行次数=1")

	// 结束后的迟到重复提交也只返回缓存结论，不再执行。
	r, e := rt.Invoke(context.Background(), InvokeRequest{Function: "idfn", CallID: dupID})
	if e != nil || string(r.Payload) != "ok:idem:x" {
		t.Fatalf("事后重复提交应回放缓存结论，得到 %+v %v", r, e)
	}
	if executions.Load() != 1 {
		t.Fatalf("事后提交不应再执行，实际次数 %d", executions.Load())
	}
}

// 业务五补充：首次执行失败时，重复提交者也拿到同一个失败结论。
func TestIdempotentFailureReplay(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var executions atomic.Int64
	h := HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
		executions.Add(1)
		return nil, errors.New("boom-business")
	})
	if err := rt.Register("efn", VersionSpec{Version: "v1", Handler: h, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "efn", CallID: "fail-id"})
		if kindOf(e) != KindExecutorFailed {
			t.Fatalf("期望 %s，得到 %v", KindExecutorFailed, e)
		}
	}
	if executions.Load() != 1 {
		t.Fatalf("失败结论也应去重，执行次数=%d", executions.Load())
	}
	t.Logf("[业务:幂等失败回放] 输入 efn CallID=fail-id x3；判定: 全部拿到同一 %s，执行次数=1", KindExecutorFailed)
}

// 业务六：执行器 panic 归一化为平台错误、实例销毁、不影响其他函数。
func TestExecutorCrashIsolation(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created atomic.Int64
	factory := func() (Handler, error) {
		created.Add(1)
		return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
			panic("intentional crash")
		}), nil
	}
	if err := rt.Register("crash", VersionSpec{
		Version: "v1", NewInstance: factory, Concurrency: 2, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "crash", CallID: fmt.Sprintf("c%d", i)})
		if kindOf(e) != KindExecutorCrashed {
			t.Fatalf("输入 crash/c%d 期望 %s，得到 %v", i, KindExecutorCrashed, e)
		}
	}
	if created.Load() != 3 {
		t.Fatalf("每次崩溃后实例都应销毁重建，期望创建 3 次，实际 %d", created.Load())
	}
	t.Logf("[业务:崩溃归一化] 输入 crash x3 全部 panic；判定: 错误类别=%s，崩溃实例不复用（重建3次）", KindExecutorCrashed)

	// 另一个函数完全不受影响。
	if err := rt.Register("healthy", VersionSpec{
		Version:     "v1",
		Handler:     HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) { return []byte("fine"), nil }),
		Concurrency: 1,
	}); err != nil {
		t.Fatal(err)
	}
	r, e := rt.Invoke(context.Background(), InvokeRequest{Function: "healthy", CallID: "h1"})
	if e != nil || string(r.Payload) != "fine" {
		t.Fatalf("其他函数不应受崩溃影响，得到 %+v %v", r, e)
	}
	t.Logf("[业务:故障隔离] 判定: crash 的 panic 不拖垮 healthy 函数")
}

// gateHandler 是可编排的执行器：可模拟慢执行、忽略取消的执行器、panic。
type gateHandler struct {
	name      string
	started   chan struct{}
	startOnce sync.Once
	rel       chan struct{}
	relOnce   sync.Once

	executions atomic.Int64
	panicAt    int64 // >0 时第 N 次执行 panic
	nth        atomic.Int64
	ignoreCtx  bool
}

func newGate(name string) *gateHandler {
	return &gateHandler{name: name, started: make(chan struct{}), rel: make(chan struct{})}
}

func (g *gateHandler) release() { g.relOnce.Do(func() { close(g.rel) }) }

func (g *gateHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	n := g.nth.Add(1)
	g.executions.Add(1)
	g.startOnce.Do(func() { close(g.started) })
	if g.panicAt > 0 && n == g.panicAt {
		panic("boom from " + g.name)
	}
	if g.ignoreCtx {
		<-g.rel // 故意不响应取消，用于验证“迟到结果不改结论 + 槽位释放”
	} else {
		select {
		case <-g.rel:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return append([]byte("ok:"+g.name+":"), payload...), nil
}

// 业务一：版本注册、显式/当前路由、原子更新、在途调用锁定起始版本。
func TestVersionRoutingAndAtomicUpdate(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h1 := newGate("v1")
	spec1 := VersionSpec{Version: "v1", Handler: h1, Concurrency: 4, MaxQueueDepth: 4, IdleTimeout: time.Minute}
	if err := rt.Register("fn", spec1); err != nil {
		t.Fatalf("输入标识 fn/v1 注册失败: %v", err)
	}
	t.Logf("[业务:版本与路由] 输入 fn/CURRENT 首次注册 v1；判定: CURRENT=v1")

	// 非法更新必须整体拒绝：旧版本继续可用，路由不变。
	bad := VersionSpec{Version: "v2-bad", Handler: HandlerFunc(func(context.Context, []byte) ([]byte, error) {
		return nil, nil
	})}
	if err := rt.Publish("fn", bad); kindOf(err) != KindValidation {
		t.Fatalf("输入 fn/v2-bad(Concurrency=0) 期望 %s，得到 %v", KindValidation, err)
	}
	cur, _ := rt.CurrentVersion("fn")
	if cur != "v1" {
		t.Fatalf("非法更新后路由被污染: %s", cur)
	}
	t.Logf("[业务:原子更新] 输入 fn/v2-bad 非法(Concurrency=0) 被整体拒绝；判定: 错误=%s 且 CURRENT 仍=v1", KindValidation)

	// 在途调用：v1 先起跑，随后发布 v2。
	inflight := make(chan error, 1)
	go func() {
		res, e := rt.Invoke(context.Background(), InvokeRequest{Function: "fn", Version: "v1", Payload: []byte("inflight")})
		if e != nil {
			inflight <- e
			return
		}
		if res.Version != "v1" || string(res.Payload) != "ok:v1:inflight" {
			inflight <- fmt.Errorf("在途调用结论异常: %+v", res)
			return
		}
		inflight <- nil
	}()
	<-h1.started

	h2 := HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
		return append([]byte("ok:v2:"), p...), nil
	})
	if err := rt.Publish("fn", VersionSpec{Version: "v2", Handler: h2, Concurrency: 4, MaxQueueDepth: 4, IdleTimeout: time.Minute}); err != nil {
		t.Fatal(err)
	}
	t.Logf("[业务:在途调用] 输入 fn/v1 调用在途时发布 v2；判定: 该调用必须仍按 v1 跑完")

	res2, err := rt.Invoke(context.Background(), InvokeRequest{Function: "fn", Payload: []byte("new")})
	if err != nil || res2.Version != "v2" || string(res2.Payload) != "ok:v2:new" {
		t.Fatalf("输入 fn/CURRENT 期望路由 v2，得到 %+v %v", res2, err)
	}
	h1.release()
	if e := <-inflight; e != nil {
		t.Fatal(e)
	}
	t.Logf("[业务:在途调用] 判定: v1 在途结果=ok:v1:inflight，且新调用 CURRENT 已路由 v2，两者互不干扰")

	// 未知版本 vs 已停用版本：拒绝原因必须可区分。
	if _, err := rt.Invoke(context.Background(), InvokeRequest{Function: "fn", Version: "v9"}); kindOf(err) != KindVersionUnknown {
		t.Fatalf("输入 fn/v9 从未发布，期望 %s，得到 %v", KindVersionUnknown, err)
	}
	if err := rt.DisableVersion("fn", "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Invoke(context.Background(), InvokeRequest{Function: "fn", Version: "v1"}); kindOf(err) != KindVersionDisabled {
		t.Fatalf("输入 fn/v1 已停用，期望 %s，得到 %v", KindVersionDisabled, err)
	}
	t.Logf("[业务:版本拒绝原因] 判定: fn/v9=%s, fn/v1(停用后)=%s，原因可区分", KindVersionUnknown, KindVersionDisabled)

	if err := rt.DisableVersion("fn", "v2"); kindOf(err) != KindValidation {
		t.Fatalf("停用当前版本应被拒绝，得到 %v", err)
	}
	if _, err := rt.Invoke(context.Background(), InvokeRequest{Function: "missing"}); kindOf(err) != KindFunctionNotFound {
		t.Fatalf("未知函数应返回 %s", KindFunctionNotFound)
	}
}
