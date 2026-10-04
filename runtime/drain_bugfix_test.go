package runtime

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func invokeAsync(rt *Runtime, callID string) <-chan invokeConclusion {
	return invokeAsyncPayload(rt, callID, nil)
}

func invokeAsyncPayload(rt *Runtime, callID string, payload []byte) <-chan invokeConclusion {
	ch := make(chan invokeConclusion, 1)
	go func() {
		r, err := rt.Invoke(context.Background(), InvokeRequest{
			Function: "bfn", CallID: callID, Payload: payload, Timeout: time.Hour,
		})
		ch <- invokeConclusion{result: r, err: err}
	}()
	return ch
}

type invokeConclusion struct {
	result *InvokeResult
	err    error
}

// 业务：宽限期混跑（一个自然跑完、一个到点被强制中断）时小结计数准确，
// 跑完/中断/剩余活跃与实际发生的调用一一对上。
func TestDrainGraceMixedCompletionSummary(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	// 两个调用的放行完全独立：rel[0] 放行会自然跑完的调用，
	// rel[1] 全程不放行（到点被强制中断）。started 只用于确认两个都已派发。
	var started [2]chan struct{}
	var rel [2]chan struct{}
	for i := range started {
		started[i] = make(chan struct{})
		rel[i] = make(chan struct{})
	}
	var startOnce [2]sync.Once
	h := HandlerFunc(func(ctx context.Context, payload []byte) ([]byte, error) {
		n := int(payload[0] - '0') // 编号来自调用输入，与 goroutine 派发顺序无关
		startOnce[n].Do(func() { close(started[n]) })
		<-rel[n]
		return append(append([]byte("ok:mix#"), strconv.AppendInt(nil, int64(n), 10)...), payload...), nil
	})
	if err := rt.Register("bfn", VersionSpec{
		Version: "v1", Handler: h, Concurrency: 2, MaxQueueDepth: 0, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	first := invokeAsyncPayload(rt, "mix-1", []byte("0"))
	second := invokeAsyncPayload(rt, "mix-2", []byte("1"))
	<-started[0]
	<-started[1]

	// 排空先发布：此后两个调用都已在排空快照中。第一个立即放行、
	// 在宽限期内自然跑完；第二个全程不放行，留到到点被强制中断。
	drainDone := make(chan DrainSummary, 1)
	go func() { drainDone <- rt.Drain(2 * time.Second) }()
	waitFor(t, func() bool { return rt.currentDrain() != nil }, "排空状态应已发布")

	close(rel[0])
	if c := <-first; c.err != nil {
		t.Fatalf("[业务:宽限混跑] 输入 bfn/mix-1 应自然成功，得到 %v", c.err)
	}
	t.Logf("[业务:宽限混跑] 输入 bfn/mix-1 排空生效后放行；判定: 宽限期内自然跑完，计入 Completed")
	if st := assertLedger(t, "宽限混跑排空后观察", "bfn/v1", rt, "bfn", "v1"); st.Active != 1 {
		t.Fatalf("[业务:宽限混跑] 排空发布后应只剩 mix-2 占用 1 槽，得到 active=%d", st.Active)
	}

	sum := <-drainDone
	c := <-second
	expectKind(t, "宽限混跑", "bfn/mix-2", c.err, KindDrainInterrupted)

	if sum.Completed != 1 || sum.Interrupted != 1 {
		t.Fatalf("[业务:宽限混跑] 期望跑完=1 中断=1，得到 Completed=%d Interrupted=%d（完整小结 %+v）",
			sum.Completed, sum.Interrupted, sum)
	}
	if sum.RemainingActive != 0 || sum.QueuedRejected != 0 {
		t.Fatalf("[业务:宽限混跑] 剩余活跃应为 0、排队拒绝应为 0，得到 %+v", sum)
	}
	t.Logf("[业务:宽限混跑] 判定: Completed=1 Interrupted=1 RemainingActive=0，与两个实际调用一一对应: %+v", sum)

	st := assertLedger(t, "宽限混跑资源清零", "bfn/v1", rt, "bfn", "v1")
	if st.Active != 0 || st.IdleInstances != 0 || st.QueueLength != 0 {
		t.Fatalf("[业务:宽限混跑] 排空后资源必须清零，得到 %+v", st)
	}
}

// 业务：排空生效后先前在执行、很快自然结束的调用（含收尾瞬间结束），
// 稳定计入跑完，不允许出现两边都不计。重复 50 次覆盖调度交错。
func TestDrainInflightFinishingEarlyIsCounted(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		rt := NewRuntime()

		h := newBarrier("early")
		if err := rt.Register("bfn", VersionSpec{
			Version: "v1", Handler: h, Concurrency: 1, MaxQueueDepth: 0, IdleTimeout: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}

		done := invokeAsync(rt, "early-1")
		<-h.started

		drainDone := make(chan DrainSummary, 1)
		go func() { drainDone <- rt.Drain(50 * time.Millisecond) }()
		waitFor(t, func() bool { return rt.currentDrain() != nil }, "排空状态应已发布")

		// 排空生效后才放行：在途调用立即自然结束，压测里“很快跑完”的场景。
		h.release()
		sum := <-drainDone
		if c := <-done; c.err != nil {
			t.Fatalf("[业务:提前完成不漏计 iter=%d] 输入 bfn/early-1 应自然成功，得到 %v", iter, c.err)
		}
		if sum.Completed != 1 || sum.Interrupted != 0 || sum.RemainingActive != 0 {
			t.Fatalf("[业务:提前完成不漏计 iter=%d] 提前自然结束必须恰好计入 Completed=1，得到 %+v", iter, sum)
		}
		rt.Close()
	}
	t.Logf("[业务:提前完成不漏计] 输入 bfn/early-1 排空生效后放行；判定: 50 次重复 Completed=1 Interrupted=0 RemainingActive=0，无两边漏计")
}

// 业务：排空后用相同幂等标识重复提交，必须稳定得到排空拒绝，
// 不得回放该标识此前缓存的成功结论。
func TestDrainRejectsDuplicateCallID(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := newBarrier("dup")
	if err := rt.Register("bfn", VersionSpec{
		Version: "v1", Handler: h, Concurrency: 1, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	go func() {
		<-h.started
		h.release()
	}()
	r, err := rt.Invoke(context.Background(), InvokeRequest{
		Function: "bfn", CallID: "same-id", Payload: []byte("p"),
	})
	if err != nil || string(r.Payload) != "ok:dup:p" {
		t.Fatalf("[业务:排空后重复标识] 准备：首次调用应成功，得到 %v %v", r, err)
	}
	t.Logf("[业务:排空后重复标识] 输入 bfn/same-id 首次提交；判定: 执行成功并缓存结论")

	rt.Drain(time.Second)

	for i := 0; i < 20; i++ {
		_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "bfn", CallID: "same-id"})
		expectKind(t, "排空后重复标识", "bfn/same-id#顺序重复", e, KindDraining)
	}

	const workers = 32
	var wg sync.WaitGroup
	var bad atomic.Int64
	start := make(chan struct{})
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, e := rt.Invoke(context.Background(), InvokeRequest{Function: "bfn", CallID: "same-id"}); kindOf(e) != KindDraining {
				bad.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("[业务:排空后重复标识] %d 个并发重复提交中有 %d 个未被 KindDraining 拒绝（疑似回放旧结论）", workers, bad.Load())
	}
	t.Logf("[业务:排空后重复标识] 输入 bfn/same-id 排空后顺序20次+并发32次；判定: 全部 KindDraining，不回放旧成功结论")
}

// 业务：某版本的第一批调用与排空并发时一个不漏；排空返回后
// active/idle/queue/detached 全部归零，没有调用还在跑、没有空闲实例残留。
func TestDrainRacesWithFirstCallsAndDrainsAllResources(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created, runs atomic.Int64
	factory := func() (Handler, error) {
		created.Add(1)
		return HandlerFunc(func(ctx context.Context, payload []byte) ([]byte, error) {
			runs.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		}), nil
	}
	mkSpec := func(v Version) VersionSpec {
		return VersionSpec{
			Version: v, NewInstance: factory,
			Concurrency: 4, MaxQueueDepth: 16, IdleTimeout: time.Hour,
		}
	}
	if err := rt.Register("bfn", mkSpec("v1")); err != nil {
		t.Fatal(err)
	}
	if err := rt.Publish("bfn", mkSpec("v2")); err != nil {
		t.Fatal(err)
	}

	const callers = 64
	var wg sync.WaitGroup
	var kinds [16]atomic.Int64
	observe := func(k ErrorKind) {
		for i, known := range []ErrorKind{KindDraining, KindDrainInterrupted} {
			if k == known {
				kinds[i].Add(1)
				return
			}
		}
		kinds[15].Add(1) // 未预期类别
	}
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, e := rt.Invoke(context.Background(), InvokeRequest{
				Function: "bfn", Version: "v2",
				CallID:  "race-" + strconv.Itoa(i),
				Timeout: time.Hour,
			})
			observe(kindOf(e))
		}(i)
	}
	close(start)
	// 不等待任何“开始执行”信号：第一批调用是否已穿过 resolve 完全交给真实竞态，
	// 反复跑覆盖两种交错，结论集合都必须恰好封闭。
	sum := rt.Drain(200 * time.Millisecond)
	wg.Wait()

	if kinds[15].Load() != 0 {
		t.Fatalf("[业务:首批撞排空] 输入 bfn/v2 race-* 出现了 Draining/DrainInterrupted 之外的结论 %d 个", kinds[15].Load())
	}
	draining, interrupted := kinds[0].Load(), kinds[1].Load()
	if draining+interrupted != callers {
		t.Fatalf("[业务:首批撞排空] 共 %d 个提交，结论只覆盖 %d（Draining=%d Interrupted=%d）",
			callers, draining+interrupted, draining, interrupted)
	}
	if sum.Interrupted != interrupted {
		t.Fatalf("[业务:首批撞排空] 小结 Interrupted=%d 与调用方实际观测 %d 不一致: %+v",
			sum.Interrupted, interrupted, sum)
	}
	if sum.Completed+sum.Interrupted > int64(callers) {
		t.Fatalf("[业务:首批撞排空] 小结跑完/中断合计超过提交总数: %+v", sum)
	}
	if sum.RemainingActive != 0 {
		t.Fatalf("[业务:首批撞排空] RemainingActive 必须为 0，得到 %+v", sum)
	}
	t.Logf("[业务:首批撞排空] 输入 bfn/v2（此前从未执行）64 并发与 Drain 同时发起；判定: Draining=%d DrainInterrupted=%d，无漏拦，执行器实际派发=%d，小结 %+v",
		draining, interrupted, runs.Load(), sum)

	// 排空返回后：不允许还有调用在跑、不允许留下空闲实例/等待队列/脱离载体。
	waitFor(t, func() bool {
		st := assertLedger(t, "首批撞排空资源归零", "bfn/v2", rt, "bfn", "v2")
		return st.Active == 0 && st.IdleInstances == 0 && st.QueueLength == 0 && st.Detached == 0
	}, "bfn/v2 排空后 active/idle/queue/detached 全部归零")
	st1 := assertLedger(t, "首批撞排空旧版本", "bfn/v1", rt, "bfn", "v1")
	if st1.Active != 0 || st1.IdleInstances != 0 || st1.QueueLength != 0 || st1.Detached != 0 {
		t.Fatalf("[业务:首批撞排空] v1 资源必须清零，得到 %+v", st1)
	}
	t.Logf("[业务:首批撞排空资源归零] 判定: v2 与 v1 均 active=0 idle=0 queue=0 detached=0；曾创建实例 %d 个全部闭环", created.Load())
}

// 业务：首批调用中已有 4 个真正派发（该版本此前从未执行），排空与其撞在一起：
// 已派发者全部得到 DrainInterrupted，其余全部 Draining，排空返回后资源归零。
// 用 factoryReady 屏障精确卡住“实例已派发、排空尚未发布”的交错，不赌时序。
func TestDrainRacesWithFirstCallsPartialDispatch(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	releaseHandler := make(chan struct{})
	var started atomic.Int64
	factory := func() (Handler, error) {
		return HandlerFunc(func(ctx context.Context, payload []byte) ([]byte, error) {
			started.Add(1)
			<-ctx.Done()
			<-releaseHandler // 模拟到点时赖着不响应取消的执行器
			return nil, ctx.Err()
		}), nil
	}
	if err := rt.Register("bfn", VersionSpec{
		Version: "v9", NewInstance: factory,
		Concurrency: 4, MaxQueueDepth: 64, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	const callers = 24
	var wg sync.WaitGroup
	var draining, interrupted atomic.Int64
	var otherKinds sync.Map
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, e := rt.Invoke(context.Background(), InvokeRequest{
				Function: "bfn", Version: "v9",
				CallID:  "p-" + strconv.Itoa(i),
				Timeout: time.Hour,
			})
			switch kindOf(e) {
			case KindDraining:
				draining.Add(1)
			case KindDrainInterrupted:
				interrupted.Add(1)
			default:
				v, _ := otherKinds.LoadOrStore(kindOf(e), new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
			}
		}(i)
	}
	close(start)
	// 以池账目中的 Active==4 作为“4 个首批调用已完成派发、flight 已入册”
	// 的权威信号（handler 入口早于入册，不能用来卡这个交错）。
	waitFor(t, func() bool {
		st, _ := rt.Stats("bfn", "v9")
		return st.Pool.Active == 4
	}, "bfn/v9 恰好 4 个首批调用已真正派发")

	sum := rt.Drain(150 * time.Millisecond)
	wg.Wait()

	if interrupted.Load() != 4 || draining.Load() != callers-4 {
		var others string
		otherKinds.Range(func(k, v any) bool {
			others += fmt.Sprintf("%s=%d ", k, v.(*atomic.Int64).Load())
			return true
		})
		t.Fatalf("[业务:首批部分派发] 输入 bfn/v9 p-* 期望中断=4 排空拒绝=%d，得到中断=%d 拒绝=%d 其他[%s]",
			callers-4, interrupted.Load(), draining.Load(), others)
	}
	if sum.Interrupted != 4 || sum.Completed != 0 || sum.RemainingActive != 0 {
		t.Fatalf("[业务:首批部分派发] 小结应 Interrupted=4 Completed=0 RemainingActive=0，得到 %+v", sum)
	}
	t.Logf("[业务:首批部分派发] 输入 bfn/v9（首次执行）24 提交、4 个已派发时发起排空；判定: DrainInterrupted=4 Draining=%d，小结 %+v",
		draining.Load(), sum)

	close(releaseHandler) // 赖着的执行器此刻才肯返回
	waitFor(t, func() bool {
		st := assertLedger(t, "首批部分派发归零", "bfn/v9", rt, "bfn", "v9")
		return st.Active == 0 && st.IdleInstances == 0 && st.QueueLength == 0 && st.Detached == 0
	}, "赖着的执行器迟到返回后，bfn/v9 全部账目归零")
	t.Logf("[业务:首批部分派发归零] 判定: 排空未被赖着的执行器拖住；迟到返回后 active=0 idle=0 queue=0 detached=0")
}

// 业务：统计字段向后兼容——Idle 恢复为“该版本当前有没有空闲实例”的
// 布尔语义，老运维脚本的条件判断可直接编译使用；具体数量走 IdleInstances。
func TestStatsIdleBooleanBackwardCompatible(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	two := 2
	// 3 个独立同步点确保 3 个调用确实同时在执行，再一起放行：
	// 回落时确定经历了 3 个实例、按 MaxIdleInstances=2 留下 2 个空闲，
	// 不依赖“三个 goroutine 恰好并发”的调度运气。
	entered := make([]chan struct{}, 3)
	rel := make(chan struct{})
	for i := range entered {
		entered[i] = make(chan struct{}, 1)
	}
	var entMu sync.Mutex
	enteredSet := make(map[int]chan struct{})
	for i, c := range entered {
		enteredSet[i] = c
	}
	bh := HandlerFunc(func(ctx context.Context, payload []byte) ([]byte, error) {
		idx := int(payload[0] - '0')
		entMu.Lock()
		c := enteredSet[idx]
		entMu.Unlock()
		c <- struct{}{}
		<-rel
		return payload, nil
	})
	if err := rt.Register("bfn", VersionSpec{
		Version:     "v1",
		Handler:     bh,
		Concurrency: 4, MaxIdleInstances: &two, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	initial, _ := rt.Stats("bfn", "v1")
	// 老调用方的用法：直接当布尔条件，必须能编译且语义正确。
	if initial.Pool.Idle {
		t.Fatalf("[业务:Idle布尔兼容] 未调用时 Idle 应为 false，得到 %+v", initial.Pool)
	}
	t.Logf("[业务:Idle布尔兼容] 输入 bfn/v1 未发生调用；判定: Idle=false（老布尔语义可用），IdleInstances=%d",
		initial.Pool.IdleInstances)

	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := rt.Invoke(context.Background(), InvokeRequest{
				Function: "bfn", CallID: "warm-" + strconv.Itoa(i), Payload: []byte{byte('0' + i)},
			})
			if e != nil {
				t.Errorf("[业务:Idle布尔兼容] 预热调用失败: %v", e)
			}
		}(i)
	}
	for i := range 3 {
		<-entered[i]
	}
	close(rel)
	wg.Wait()

	after, _ := rt.Stats("bfn", "v1")
	if !after.Pool.Idle {
		t.Fatalf("[业务:Idle布尔兼容] 并发回落后 Idle 应为 true，得到 %+v", after.Pool)
	}
	if after.Pool.IdleInstances != 2 {
		t.Fatalf("[业务:Idle布尔兼容] 具体数量应在 IdleInstances=2，得到 %d", after.Pool.IdleInstances)
	}
	t.Logf("[业务:Idle布尔兼容] 输入 bfn/v1 3 次并发回落（MaxIdleInstances=2）；判定: Idle=true 且 IdleInstances=2，布尔与数量各司其职")
}
