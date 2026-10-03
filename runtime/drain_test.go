package runtime

import (
	"context"
	"sync"
	"testing"
	"time"
)

// blockHandler 是完全可控的执行器：Invoke 开始时在 enter 登记并同步，
// 返回由 release 广播；ignoreCtx 时模拟无视取消的赖账执行器。
// reset() 进入下一轮，避免一次性广播后无法再次阻塞。
type blockHandler struct {
	mu        sync.Mutex
	active    int
	enter     chan invocationSignal
	release   chan struct{}
	gate      chan struct{} // 当前轮的放行门；reset 后换一个新的，与旧 release 广播隔离
	relOnce   sync.Once
	ignoreCtx bool
}

type invocationSignal struct {
	id  string
	ctx context.Context
}

func newBlockHandler(ignoreCtx bool, buf int) *blockHandler {
	return &blockHandler{
		enter:     make(chan invocationSignal, buf),
		release:   make(chan struct{}),
		gate:      make(chan struct{}),
		ignoreCtx: ignoreCtx,
	}
}

func (b *blockHandler) reset() {
	b.relOnce = sync.Once{}
	b.release = make(chan struct{})
	b.enter = make(chan invocationSignal, 16)
	b.gate = make(chan struct{})
}

func (b *blockHandler) Invoke(ctx context.Context, p []byte) ([]byte, error) {
	id := string(p)
	b.mu.Lock()
	b.active++
	b.mu.Unlock()
	b.enter <- invocationSignal{id, ctx}
	if b.ignoreCtx {
		<-b.gate // 无视 ctx，模拟赖着不响应取消的执行器
	} else {
		if !b.waitGate(ctx) {
			b.decActive()
			return nil, ctx.Err()
		}
	}
	b.decActive()
	return []byte("ok:" + id), nil
}

func (b *blockHandler) waitGate(ctx context.Context) bool {
	select {
	case <-b.gate:
		return true
	case <-b.release:
		return true
	case <-ctx.Done():
		return false
	}
}

func (b *blockHandler) decActive() {
	b.mu.Lock()
	b.active--
	b.mu.Unlock()
}

// releaseAll 只放行当前轮（关闭本轮 gate），不影响 reset 后的新一轮。
func (b *blockHandler) releaseAll() {
	b.relOnce.Do(func() {
		close(b.gate)
	})
}

func intPtr(n int) *int { return &n }

func invokeAsync(rt *Runtime, req InvokeRequest) <-chan error {
	ch := make(chan error, 1)
	go func() {
		_, e := rt.Invoke(context.Background(), req)
		ch <- e
	}()
	return ch
}

func noopHandler(context.Context, []byte) ([]byte, error) { return []byte("ok"), nil }

func waitDrainTestStats(rt *Runtime, t *testing.T, fn string, v Version, pred func(Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := rt.Stats(fn, v)
		if pred(st.Pool) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待 %s/%s 统计条件超时", fn, v)
}

// 业务七：优雅排空——停止接单、排队者即拒、在途跑完、超期中断、
// 重复与并发发起共享排空、结果与资源归零、小结正确、迟到结果不翻案。
func TestDrainGracefulShutdown(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	v1h := newBlockHandler(false, 16) // 响应取消
	v2h := newBlockHandler(true, 8)   // 无视取消：宽限期满被强制中断
	if err := rt.Register("dfn", VersionSpec{
		Version: "v1", Handler: v1h, Concurrency: 2, MaxQueueDepth: 2,
		IdleTimeout: 0, MaxIdleInstances: intPtr(0),
	}); err != nil {
		t.Fatal(err)
	}

	// 预热：跑 2 个调用，回落后期望保留 2 个空闲实例。
	// 在独立函数 keep 上验证保留，dfn/v1 用 0 保留以便后面精确构造“占满+排队”。
	kh := newBlockHandler(false, 8)
	if err := rt.Register("keep", VersionSpec{
		Version: "v1", Handler: kh, Concurrency: 2, MaxQueueDepth: 0,
		IdleTimeout: time.Hour, MaxIdleInstances: intPtr(2),
	}); err != nil {
		t.Fatal(err)
	}
	warm := make(chan error, 2)
	for _, id := range []string{"warm-1", "warm-2"} {
		id := id
		go func() {
			_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "keep", Version: "v1", CallID: id, Payload: []byte(id)})
			warm <- e
		}()
	}
	<-kh.enter
	<-kh.enter
	kh.releaseAll()
	for i := 0; i < 2; i++ {
		if e := <-warm; e != nil {
			t.Fatal(e)
		}
	}
	st, _ := rt.Stats("keep", "v1")
	if st.Pool.IdleCount != 2 {
		t.Fatalf("预热后期望保留 2 个空闲实例，得到 %+v", st.Pool)
	}
	t.Logf("[业务:保留预热] 输入 keep/warm-1,warm-2 MaxIdleInstances=2；判定: IdleCount=2")

	if err := rt.Publish("dfn", VersionSpec{
		Version: "v2", Handler: v2h, Concurrency: 1, MaxQueueDepth: 0,
		IdleTimeout: time.Hour, MaxIdleInstances: intPtr(1),
	}); err != nil {
		t.Fatal(err)
	}

	// 新一轮 v1 gate：2 在途 + 1 排队。
	v1h.reset()
	d1 := invokeAsync(rt, InvokeRequest{Function: "dfn", Version: "v1", CallID: "drain-run-1", Payload: []byte("run-1"), Timeout: time.Hour})
	d2 := invokeAsync(rt, InvokeRequest{Function: "dfn", Version: "v1", CallID: "drain-run-2", Payload: []byte("run-2"), Timeout: time.Hour})
	<-v1h.enter
	<-v1h.enter
	// 先确定性地占满 2 个并发槽，再发起第三个调用，它必然排队（顺序无关）。
	waitDrainTestStats(rt, t, "dfn", "v1", func(s Stats) bool { return s.Active == 2 })

	// 挂好入队同步钩子后再发起排队调用，确保第三个调用确实阻塞在队列。
	queued := make(chan struct{})
	prevHook := waiterHook
	waiterHook = func(pp *instancePool, w *waiter) {
		if pp.fn == "dfn" && pp.version() == "v1" {
			close(queued)
		}
	}
	dq := invokeAsync(rt, InvokeRequest{Function: "dfn", Version: "v1", CallID: "drain-queue-1", Payload: []byte("drain-queue-1"), Timeout: time.Hour})
	select {
	case <-queued:
	case <-time.After(2 * time.Second):
		t.Fatal("排队等待者未进入队列等待")
	}
	waiterHook = prevHook
	pre, _ := rt.Stats("dfn", "v1")
	if pre.Pool.Active != 2 || pre.Pool.QueueLength != 1 {
		t.Fatalf("排空前应 Active=2 Queue=1，得到 %+v", pre.Pool)
	}

	cs := invokeAsync(rt, InvokeRequest{Function: "dfn", CallID: "stuck-1", Payload: []byte("stuck-1"), Timeout: time.Hour})
	<-v2h.enter

	drainDone := make(chan DrainSummary, 1)
	drainStart := time.Now()
	go func() { drainDone <- rt.Drain(200 * time.Millisecond) }()

	select {
	case e := <-dq:
		if kindOf(e) != KindDraining {
			t.Fatalf("输入 dfn/drain-queue-1 排空中仍在队列，期望 %s，得到 %v", KindDraining, e)
		}
		t.Logf("[业务:排空拒单] 输入 dfn/drain-queue-1；判定: %s（区别于过载/超时/停用）", KindDraining)
	case <-time.After(2 * time.Second):
		t.Fatal("排队的调用未在排空时结束")
	}

	if _, err := rt.Invoke(context.Background(), InvokeRequest{Function: "dfn", CallID: "new-after-drain"}); kindOf(err) != KindDraining {
		t.Fatalf("输入 dfn/new-after-drain 期望 %s，得到 %v", KindDraining, err)
	}
	t.Logf("[业务:停止接单] 输入 dfn/new-after-drain；判定: %s", KindDraining)

	v1h.releaseAll()

	summary := <-drainDone
	elapsed := time.Since(drainStart)
	t.Logf("[业务:排空小结] 耗时=%s Completed=%d Interrupted=%d QueuedRejected=%d IdleDestroyed=%d ActiveLeft=%d DetachedLeft=%d",
		elapsed, summary.Completed, summary.Interrupted, summary.QueuedRejected, summary.IdleDestroyed, summary.ActiveLeft, summary.DetachedLeft)
	if summary.ActiveLeft != 0 {
		t.Fatalf("排空后占用必须归零，得到 %+v", summary)
	}
	if summary.QueuedRejected != 1 {
		t.Fatalf("排队拒绝应为 1，得到 %+v", summary)
	}
	if summary.DetachedLeft != 1 {
		t.Fatalf("v2 赖账载体应计入 DetachedLeft=1，得到 %+v", summary)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("赖账执行器拖住了排空，耗时 %s", elapsed)
	}

	if e := <-cs; kindOf(e) != KindDrainInterrupted {
		t.Fatalf("输入 dfn/stuck-1 期望 %s，得到 %v", KindDrainInterrupted, e)
	}
	t.Logf("[业务:超期中断] 输入 dfn/stuck-1 无视取消；判定: %s（区别于 InvokeTimeout/Canceled），排空按时返回", KindDrainInterrupted)

	for _, ch := range []<-chan error{d1, d2} {
		if e := <-ch; e != nil && kindOf(e) != KindDrainInterrupted {
			t.Fatalf("v1 在途只允许成功或 %s，得到 %v", KindDrainInterrupted, e)
		}
	}
	t.Logf("[业务:在途用完宽限] 判定: run-1/run-2 结论为成功或 %s，无其他类别", KindDrainInterrupted)

	for _, ref := range []struct {
		fn string
		v  Version
	}{{"dfn", "v1"}, {"dfn", "v2"}, {"keep", "v1"}} {
		st, _ := rt.Stats(ref.fn, ref.v)
		if st.Pool.Active != 0 || st.Pool.QueueLength != 0 || st.Pool.IdleCount != 0 {
			t.Fatalf("%s/%s 排空后资源应清零，得到 %+v", ref.fn, ref.v, st.Pool)
		}
	}
	if summary.IdleDestroyed != 2 {
		t.Fatalf("keep 的 2 个空闲实例应在排空开始时回收，得到 %+v", summary)
	}
	t.Logf("[业务:资源归零] 判定: dfn v1/v2、keep 全部 Active=0 Queue=0 Idle=0；DetachedLeft 不占并发额度")

	if _, err := rt.Invoke(context.Background(), InvokeRequest{Function: "dfn", CallID: "after-complete"}); kindOf(err) != KindDraining {
		t.Fatalf("排空结束后调用应稳定 %s，得到 %v", KindDraining, err)
	}
	var wg sync.WaitGroup
	sums := make(chan DrainSummary, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sums <- rt.Drain(time.Millisecond)
		}()
	}
	wg.Wait()
	close(sums)
	for s := range sums {
		if s.ActiveLeft != 0 {
			t.Fatal("重复排空后占用必须为 0")
		}
	}
	t.Logf("[业务:排空幂等] 判定: 排空后并发发起 4 次 Drain 全部安全返回，接单仍为 %s", KindDraining)

	if err := rt.Register("newfn", VersionSpec{Version: "v1", Handler: HandlerFunc(noopHandler), Concurrency: 1}); kindOf(err) != KindDraining {
		t.Fatalf("排空中注册应得到 %s，得到 %v", KindDraining, err)
	}

	st, _ = rt.Stats("dfn", "v2")
	before := st.Pool.Destroyed
	v2h.releaseAll()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ = rt.Stats("dfn", "v2")
		if st.Pool.Detached == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	st, _ = rt.Stats("dfn", "v2")
	if st.Pool.Detached != 0 || st.Pool.Destroyed != before+1 {
		t.Fatalf("赖账载体迟到后必须收尾销毁，Detached=%d Destroyed %d->%d", st.Pool.Detached, before, st.Pool.Destroyed)
	}
	t.Logf("[业务:迟到结果治理] 判定: v2 赖账载体放行后自动销毁（Destroyed=%d），stuck-1 结论不翻案", st.Pool.Destroyed)
}

// 业务七补充：精确的“一个在宽限期内跑完、一个到点被中断”，并验证小结计数。
func TestDrainOneCompletesOneInterrupted(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	fast := newBlockHandler(false, 8)
	slow := newBlockHandler(false, 8)
	if err := rt.Register("mix", VersionSpec{
		Version: "v1",
		Handler: HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
			if string(p) == "fast" {
				return fast.Invoke(ctx, p)
			}
			return slow.Invoke(ctx, p)
		}),
		Concurrency: 2, MaxQueueDepth: 0, IdleTimeout: time.Hour, MaxIdleInstances: intPtr(2),
	}); err != nil {
		t.Fatal(err)
	}

	cf := invokeAsync(rt, InvokeRequest{Function: "mix", CallID: "fast", Payload: []byte("fast"), Timeout: time.Hour})
	cslow := invokeAsync(rt, InvokeRequest{Function: "mix", CallID: "slow", Payload: []byte("slow"), Timeout: time.Hour})
	<-fast.enter
	<-slow.enter

	// 在 mix 池排空真正开始后再放行 fast，保证它算作排空期间完成。
	started := make(chan struct{})
	prevDrainHook := drainBeginHook
	drainBeginHook = func(v Version) {
		if v == "v1" {
			close(started)
		}
	}
	defer func() { drainBeginHook = prevDrainHook }()
	done := make(chan DrainSummary, 1)
	go func() { done <- rt.Drain(300 * time.Millisecond) }()
	<-started
	fast.releaseAll()
	if e := <-cf; e != nil {
		t.Fatalf("输入 mix/fast 宽限期内放行应成功，得到 %v", e)
	}

	s := <-done
	// slow 响应取消：收到中断后可能尚未在快照前走完收尾，DetachedLeft 允许 0；
	// 其调用结论必须是 DrainInterrupted。
	if s.Completed != 1 || s.Interrupted != 1 || s.ActiveLeft != 0 {
		t.Fatalf("期望 Completed=1 Interrupted=1 ActiveLeft=0，得到 %+v", s)
	}
	if e := <-cslow; kindOf(e) != KindDrainInterrupted {
		t.Fatalf("输入 mix/slow 期望 %s，得到 %v", KindDrainInterrupted, e)
	}
	t.Logf("[业务:排空小结精确计数] 输入 mix/fast,mix/slow；判定: Completed=1 Interrupted=1，%s 与普通超时区分", KindDrainInterrupted)
}
