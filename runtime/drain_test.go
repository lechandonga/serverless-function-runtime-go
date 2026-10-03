package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// barrier 是可编排执行器的同步原语：started 表示真正开始执行，
// release 放行。所有并发用例都靠它确定性同步，不赌时序。
type barrier struct {
	name      string
	started   chan struct{}
	startOnce sync.Once
	rel       chan struct{}
	relOnce   sync.Once
	ignoreCtx bool
	runs      atomic.Int64
}

func newBarrier(name string) *barrier {
	return &barrier{name: name, started: make(chan struct{}), rel: make(chan struct{})}
}

func (b *barrier) release() { b.relOnce.Do(func() { close(b.rel) }) }

func (b *barrier) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	b.runs.Add(1)
	b.startOnce.Do(func() { close(b.started) })
	if b.ignoreCtx {
		<-b.rel // 故意不响应取消
	} else {
		select {
		case <-b.rel:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return append([]byte("ok:"+b.name+":"), payload...), nil
}

func expectKind(t *testing.T, business, input string, err error, want ErrorKind) {
	t.Helper()
	if kindOf(err) != want {
		t.Fatalf("[业务:%s] 输入 %s 期望判定 %s，得到 %v", business, input, want, err)
	}
	t.Logf("[业务:%s] 输入 %s；判定: %s", business, input, want)
}

func assertLedger(t *testing.T, business, input string, rt *Runtime, fn, v string) Stats {
	t.Helper()
	st, err := rt.Stats(fn, Version(v))
	if err != nil {
		t.Fatalf("[业务:%s] 输入 %s 读取账目失败: %v", business, input, err)
	}
	live := int64(st.Pool.Active + st.Pool.Idle + st.Pool.Detached)
	if st.Pool.Created-st.Pool.Destroyed != live {
		t.Fatalf("[业务:%s] 输入 %s 账目不平: created=%d destroyed=%d live(active=%d idle=%d detached=%d)=%d",
			business, input, st.Pool.Created, st.Pool.Destroyed,
			st.Pool.Active, st.Pool.Idle, st.Pool.Detached, live)
	}
	return st.Pool
}

// 业务：优雅排空 —— 停止接单、在途跑完、队列直拒、资源归零与小结。
func TestDrainRejectsNewCompletesInflightAndClearsResources(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := newBarrier("drain-fast")
	if err := rt.Register("dfn", VersionSpec{
		Version: "v1", Handler: h, Concurrency: 1, MaxQueueDepth: 8, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	finished := make(chan error, 1)
	go func() {
		r, e := rt.Invoke(context.Background(), InvokeRequest{Function: "dfn", CallID: "inflight-1", Payload: []byte("x")})
		if e != nil {
			finished <- e
			return
		}
		if string(r.Payload) != "ok:drain-fast:x" {
			finished <- fmt.Errorf("在途结果异常: %s", r.Payload)
			return
		}
		finished <- nil
	}()
	<-h.started

	// 再排一个调用进队列，验证排空时“没开始的直接按排空原因结束”。
	queuedErr := make(chan error, 1)
	go func() {
		_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "dfn", CallID: "queued-1", Timeout: 5 * time.Second})
		queuedErr <- e
	}()
	waitFor(t, func() bool {
		st, _ := rt.Stats("dfn", "v1")
		return st.Pool.QueueLength == 1
	}, "queued-1 应进入等待队列")

	// 宽限期 500ms：在途调用很快会被放行并跑完；排空可提前收尾。
	drainDone := make(chan DrainSummary, 1)
	go func() { drainDone <- rt.Drain(500 * time.Millisecond) }()
	// 只观察状态发布，不发起会占排队名额的探测调用。
	waitFor(t, func() bool { return rt.currentDrain() != nil }, "排空状态应已发布")
	expectKind(t, "排空停止接单", "dfn/new-after-drain",
		mustInvokeErr(rt, "dfn", "new-after-drain"), KindDraining)

	h.release()
	if e := <-queuedErr; kindOf(e) != KindDraining {
		t.Fatalf("[业务:排空清队列] 输入 dfn/queued-1 期望 KindDraining，得到 %v", e)
	}
	t.Logf("[业务:排空清队列] 输入 dfn/queued-1 排空开始时仍排队；判定: KindDraining（区别于 QueueTimeout/Overload）")
	if e := <-finished; e != nil {
		t.Fatalf("[业务:排空宽限] 在途调用应在宽限期内自然跑完，得到 %v", e)
	}
	t.Logf("[业务:排空宽限] 输入 dfn/inflight-1；判定: 宽限期内自然跑完，结果不被改写")

	sum := <-drainDone
	if sum.Completed != 1 || sum.Interrupted != 0 || sum.QueuedRejected != 1 {
		t.Fatalf("排空小结不符合预期: %+v", sum)
	}
	if sum.RemainingActive != 0 {
		t.Fatalf("排空后并发占用必须归零，得到 %d", sum.RemainingActive)
	}
	t.Logf("[业务:排空小结] 判定: %+v", sum)

	st := assertLedger(t, "排空资源清零", "dfn/v1", rt, "dfn", "v1")
	if st.Active != 0 || st.Idle != 0 || st.QueueLength != 0 || st.Detached != 0 {
		t.Fatalf("排空后资源必须清零，得到 %+v", st)
	}
	// 排空结束后再调用，必须以同一原因稳定拒绝。
	expectKind(t, "排空后永久拒绝", "dfn/post-close",
		mustInvokeErr(rt, "dfn", "post-close"), KindDraining)
}

// 业务：宽限期到点强制中断，结论区别于超时/取消；赖着不走的执行器不拖住排空。
func TestDrainInterruptsAfterGracePeriod(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := &barrier{name: "stuck", started: make(chan struct{}), rel: make(chan struct{}), ignoreCtx: true}
	if err := rt.Register("sfn", VersionSpec{
		Version: "v1", Handler: h, Concurrency: 1, MaxQueueDepth: 0, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	gotErr := make(chan error, 1)
	go func() {
		_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "sfn", CallID: "long-1", Timeout: time.Hour})
		gotErr <- e
	}()
	<-h.started

	start := time.Now()
	sum := rt.Drain(150 * time.Millisecond)
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Fatalf("排空被赖着不取消的执行器拖住，耗时 %s", elapsed)
	}
	if err := <-gotErr; kindOf(err) != KindDrainInterrupted {
		t.Fatalf("[业务:超期中断] 输入 sfn/long-1 期望 KindDrainInterrupted，得到 %v", err)
	}
	t.Logf("[业务:超期中断] 输入 sfn/long-1 忽略 ctx；判定: KindDrainInterrupted（区别于 InvokeTimeout/Canceled），%s 内收尾", elapsed.Round(time.Millisecond*10))

	if sum.Completed != 0 || sum.Interrupted != 1 {
		t.Fatalf("期望中断 1 跑完 0，得到 %+v", sum)
	}
	st := assertLedger(t, "中断后账目", "sfn/v1", rt, "sfn", "v1")
	if st.Active != 0 || st.Idle != 0 {
		t.Fatalf("强制中断后并发/空闲必须归零，得到 %+v", st)
	}
	if st.Detached != 1 {
		t.Fatalf("执行器尚未返回，应处于已解绑未收尾 detached=1，得到 %d", st.Detached)
	}
	t.Logf("[业务:解绑不停机] 判定: 排空按时收尾，active=0 detached=1（赖着的执行器不占额度）")

	// 执行器之后才返回：结果不能翻案，只自动收尾销毁，账目继续自洽。
	h.release()
	waitFor(t, func() bool {
		s, _ := rt.Stats("sfn", "v1")
		return s.Pool.Detached == 0
	}, "迟到返回后 detached 应归零")
	assertLedger(t, "迟到返回收尾", "sfn/v1", rt, "sfn", "v1")
	t.Logf("[业务:迟到结果不翻案] 判定: long-1 结论仍为 KindDrainInterrupted，载体自动销毁，账目持平")
}

// 业务：排空允许重复发起与并发发起；新提交在整个窗口期稳定拒绝。
func TestDrainIdempotentAndConcurrent(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := newBarrier("idem-drain")
	if err := rt.Register("ifn2", VersionSpec{
		Version: "v1", Handler: h, Concurrency: 1, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = rt.Invoke(context.Background(), InvokeRequest{Function: "ifn2", CallID: "run-1", Payload: []byte("y")})
	}()
	<-h.started

	const n = 6
	var wg sync.WaitGroup
	sums := make([]DrainSummary, n)
	start := make(chan struct{})
	drainEntered := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i == 0 {
				close(drainEntered)
			}
			sums[i] = rt.Drain(300 * time.Millisecond)
		}(i)
	}
	close(start)
	<-drainEntered
	// 排空状态先于池切换发布，自旋等待拒绝生效（不与 Drain 抢调度时序）。
	waitFor(t, func() bool { return rt.currentDrain() != nil }, "drain 状态应已发布")
	h.release()
	wg.Wait()
	for i, s := range sums {
		if s.Completed+s.Interrupted != 1 || s.Interrupted > 1 {
			t.Fatalf("并发发起 #%d 应共享同一次排空结论，得到 %+v", i, s)
		}
	}
	t.Logf("[业务:并发排空] 判定: 6 个并发 Drain 共享同一次排空，小结一致（跑完/中断合计=1）")

	// 完成后重复发起：安全、不卡死、返回同一份小结。
	again := rt.Drain(time.Second)
	first := sums[0]
	if again != first {
		t.Fatalf("重复排空小结应稳定，得到 %+v", again)
	}
	t.Logf("[业务:重复排空] 判定: 排空完成后再次 Drain 立即返回稳定小结，不卡死")
	expectKind(t, "排空后拒绝稳定", "ifn2/again",
		mustInvokeErr(rt, "ifn2", "again"), KindDraining)
}

func mustInvokeErr(rt *Runtime, fn, callID string) error {
	_, e := rt.Invoke(context.Background(), InvokeRequest{Function: fn, CallID: callID})
	return e
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待条件超时: %s", what)
}
