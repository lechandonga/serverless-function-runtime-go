package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件覆盖排空停机修复的五类业务场景：
//  1. 宽限期混跑：有的自然跑完、有的到点中断，小结计数精确对上；
//  2. 排空闸门安装后、在途快照之前就提前完成的调用不被漏计；
//  3. 排空后用重复 CallID 提交（含成功过的旧标识）稳定以排空原因拒绝；
//  4. 某版本首批调用与排空并发：不漏拦，排空返回后资源全部归零；
//  5. Stats.Idle 历史布尔语义兼容，数量用新字段 IdleCount。
//
// 并发用例都用屏障（barrier）与确定性同步，不 sleep 赌时序，
// 在 go test -race -count=N 下稳定。

// switchingHandler 按派发顺序依次使用 dispatch 通道给出的执行器，
// 便于在一个池里精确编排“多个不同行为的在途调用”。
type switchingHandler struct {
	mu       sync.Mutex
	dispatch chan Handler
	runs     int
}

func (h *switchingHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	next := <-h.dispatch
	h.mu.Lock()
	h.runs++
	h.mu.Unlock()
	return next.Invoke(ctx, payload)
}

func idForRound(round, i int) string {
	return fmt.Sprintf("r%d-c%d", round, i)
}

// 业务：宽限期混跑 —— 宽限期内 2 个调用自然跑完、1 个到点被中断。
// Completed=2、Interrupted=1、RemainingActive=0 与实际调用一一对应；
// 被中断调用拿到 DrainInterrupted，跑完的拿到真实成功结果。
func TestDrainGraceMixedCompletionAndInterruption(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	fastA := newBarrier("mix-a")
	fastB := newBarrier("mix-b")
	stuck := &barrier{name: "mix-stuck", started: make(chan struct{}), rel: make(chan struct{}), ignoreCtx: true}
	dispatcher := &switchingHandler{dispatch: make(chan Handler, 3)}
	if err := rt.Register("mfn", VersionSpec{
		Version: "v1", Handler: dispatcher, Concurrency: 3, MaxQueueDepth: 0, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	type callResult struct {
		id  string
		err error
	}
	resCh := make(chan callResult, 3)
	startCall := func(id string, h Handler) {
		dispatcher.dispatch <- h
		go func() {
			_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "mfn", CallID: id, Timeout: time.Hour})
			resCh <- callResult{id, e}
		}()
	}

	startCall("mix-fast-a", fastA)
	<-fastA.started
	startCall("mix-fast-b", fastB)
	<-fastB.started
	startCall("mix-stuck-1", stuck)
	<-stuck.started

	// 宽限期 300ms：排空发布后立即放行两个会响应结束的调用，
	// 第三个故意忽略 ctx，必然用满宽限期被中断。
	drainDone := make(chan DrainSummary, 1)
	go func() { drainDone <- rt.Drain(300 * time.Millisecond) }()
	waitFor(t, func() bool { return rt.currentDrain() != nil }, "mfn 排空状态应已发布")
	fastA.release()
	fastB.release()

	completedIDs := map[string]bool{}
	for i := 0; i < 2; i++ {
		r := <-resCh
		if r.err != nil {
			t.Fatalf("[业务:宽限跑完] 输入 mfn/%s 应自然成功，得到 %v", r.id, r.err)
		}
		completedIDs[r.id] = true
	}
	if !completedIDs["mix-fast-a"] || !completedIDs["mix-fast-b"] {
		t.Fatalf("[业务:宽限跑完] 自然结束集合不符: %v", completedIDs)
	}
	t.Logf("[业务:宽限跑完] 输入 mfn/mix-fast-a,mix-fast-b；判定: 宽限期内自然结束，结论不被改写")

	sum := <-drainDone
	r := <-resCh
	if r.id != "mix-stuck-1" || kindOf(r.err) != KindDrainInterrupted {
		t.Fatalf("[业务:到点中断] 期望 mix-stuck-1 得到 %s，得到 id=%s err=%v", KindDrainInterrupted, r.id, r.err)
	}
	t.Logf("[业务:到点中断] 输入 mfn/mix-stuck-1 忽略 ctx；判定: %s", KindDrainInterrupted)

	if sum.Completed != 2 || sum.Interrupted != 1 {
		t.Fatalf("[业务:混跑小结] 期望 Completed=2 Interrupted=1，得到 %+v", sum)
	}
	if sum.RemainingActive != 0 {
		t.Fatalf("[业务:混跑小结] RemainingActive 必须为 0，得到 %d", sum.RemainingActive)
	}
	t.Logf("[业务:混跑小结] 判定: Completed=2 Interrupted=1 RemainingActive=0，与 3 次实际调用一一对应: %+v", sum)

	st := assertLedger(t, "混跑资源清零", "mfn/v1", rt, "mfn", "v1")
	if st.Active != 0 || st.IdleCount != 0 || st.QueueLength != 0 {
		t.Fatalf("排空后 active/idle/queue 必须归零，得到 %+v", st)
	}
	if st.Detached != 1 {
		t.Fatalf("被中断的执行器尚未返回，应 detached=1，得到 %d", st.Detached)
	}
	t.Logf("[业务:混跑资源清零] 判定: active=0 idle=0 queue=0 detached=1（赖着的执行器不占额度）")
	stuck.release()
	waitFor(t, func() bool {
		s, _ := rt.Stats("mfn", "v1")
		return s.Pool.Detached == 0
	}, "mix-stuck 放行后 detached 应归零")
}

// earlyFinisher 是“派发后等待外部放行、放行后立刻结束”的执行器：
// 用于确定性复现“排空闸门已安装、beginDrain 在途快照尚未拍到它，
// 调用就自然结束”的竞态窗口。
type earlyFinisher struct {
	name    string
	ran     atomic.Int64
	inside  chan struct{}
	proceed chan struct{}
}

func (e *earlyFinisher) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	e.ran.Add(1)
	close(e.inside)
	<-e.proceed // 测试方在排空闸门装好后放行它
	return []byte("early-ok:" + e.name), nil
}

// 业务：排空闸门安装后、在途快照之前自然结束的调用必须计入 Completed，
// 载体销毁、不留空闲实例；不能“跑完、中断两边都不计”。
func TestDrainCountsFlightFinishingBeforeSnapshot(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	early := &earlyFinisher{name: "early-1", inside: make(chan struct{}), proceed: make(chan struct{})}
	if err := rt.Register("efn", VersionSpec{
		Version: "v1", Handler: early, Concurrency: 1, IdleTimeout: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "efn", CallID: "early-call"})
		done <- e
	}()
	<-early.inside // 调用已经真正在执行

	drainDone := make(chan DrainSummary, 1)
	go func() { drainDone <- rt.Drain(time.Second) }()
	// 排空状态发布与闸门安装在同一把 rt 写锁内完成，因此观察到状态
	// 发布即等价于“闸门已装好”，而此时逐池 beginDrain 尚未开始。
	waitFor(t, func() bool { return rt.currentDrain() != nil }, "efn 闸门应已安装、排空状态已发布")

	// 在“闸门后、快照前”的确定性窗口里让调用自然结束。
	close(early.proceed)
	if e := <-done; e != nil {
		t.Fatalf("[业务:快照前跑完] 输入 efn/early-call 应自然成功，得到 %v", e)
	}
	t.Logf("[业务:快照前跑完] 输入 efn/early-call 在闸门后快照前自然结束；判定: 计入 Completed，不留空闲实例")

	sum := <-drainDone
	if sum.Completed != 1 || sum.Interrupted != 0 {
		t.Fatalf("[业务:快照前小结] 期望 Completed=1 Interrupted=0，得到 %+v", sum)
	}
	t.Logf("[业务:快照前小结] 判定: Completed=1（提前完成不漏计）Interrupted=0: %+v", sum)
	st := assertLedger(t, "快照前资源清零", "efn/v1", rt, "efn", "v1")
	if st.Active != 0 || st.IdleCount != 0 || st.Detached != 0 || st.QueueLength != 0 {
		t.Fatalf("提前完成后资源应全部清零，得到 %+v", st)
	}
}

// 业务：排空后任何提交都立即得到同一种 Draining 拒绝 ——
// 包括“同一个 CallID 此前已经成功过”的重复提交，绝不回放旧成功结论。
func TestDrainRejectsDuplicateCallIDWithoutReplay(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := newBarrier("dup")
	if err := rt.Register("pfn2", VersionSpec{
		Version: "v1", Handler: h, Concurrency: 1, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	h.release() // 首次调用正常跑完，留下成功的幂等结论
	res, err := rt.Invoke(context.Background(), InvokeRequest{
		Function: "pfn2", CallID: "dup-id-1", Payload: []byte("p"),
	})
	if err != nil || string(res.Payload) != "ok:dup:p" {
		t.Fatalf("[业务:排空前成功] 准备条件失败: res=%v err=%v", res, err)
	}
	t.Logf("[业务:排空前成功] 输入 pfn2/dup-id-1；判定: 首次执行成功，结论已缓存")

	// 让排空停在宽限期内（没有在途调用也会立刻收尾，因此直接对
	// “排空已完成”的永久拒绝态断言即可，覆盖等价且更直接）。
	drainDone := make(chan DrainSummary, 1)
	go func() { drainDone <- rt.Drain(time.Second) }()
	<-drainDone

	// 排空已生效后再提交同一标识：必须是 Draining，而不是回放旧结果。
	if r, e := rt.Invoke(context.Background(), InvokeRequest{Function: "pfn2", CallID: "dup-id-1"}); e == nil {
		t.Fatalf("[业务:排空拒重复] 输入 pfn2/dup-id-1 排空后重复提交期望拒绝，得到结果 %v", r)
	} else {
		expectKind(t, "排空拒重复", "pfn2/dup-id-1", e, KindDraining)
	}
	expectKind(t, "排空拒新标识", "pfn2/fresh-id-2",
		mustInvokeErr(rt, "pfn2", "fresh-id-2"), KindDraining)

	// 再次 Drain 返回同一份小结，旧标识重复提交仍是同一种拒绝。
	if again := rt.Drain(time.Second); again.Completed != 0 || again.Interrupted != 0 {
		t.Fatalf("重复排空应返回同一份空小结，得到 %+v", again)
	}
	expectKind(t, "排空后拒重复", "pfn2/dup-id-1",
		mustInvokeErr(rt, "pfn2", "dup-id-1"), KindDraining)
	t.Logf("[业务:排空后拒重复] 判定: 旧成功标识不回放、新标识不执行，原因统一为 Draining")
}

// drainGateHandler 在“已进入入口、已获得执行载体”的第一时间通知 entered，
// 然后阻塞到 release，用于确定性地编排首批调用与排空的交错。
type drainGateHandler struct {
	name    string
	entered chan struct{}
	release chan struct{}
}

func (g *drainGateHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.release
	return []byte("gated:" + g.name), nil
}

// 业务：某版本首批调用与排空并发（版本此前从未执行、没有池）。
// 确定性编排，不赌调度：
//   - 先让并发额度数量的首批调用真正派发并在执行器内阻塞；
//   - 再让其余调用并发发起，同时发起排空（排空状态先行发布）；
//   - 排空返回后：这些调用全部已有确定结论（Draining / 中断），
//     无调用仍在执行，active/idle/queue 全部归零；
//   - 小结计数与每个调用的观测结论严格一一对上。
//
// 整个用例重复多轮并在 -race 下运行，覆盖各类交错。
func TestDrainRacesWithFirstDispatchOfFreshVersion(t *testing.T) {
	const rounds = 60
	const concurrency = 2
	const queued = 3
	const extra = 5 // 排空发布同时涌入的额外提交（部分会过载/排空直拒）
	for round := 0; round < rounds; round++ {
		rt := NewRuntime()
		h := &drainGateHandler{
			name:    fmt.Sprintf("fresh-r%d", round),
			entered: make(chan struct{}, concurrency),
			release: make(chan struct{}),
		}
		if err := rt.Register("rfresh", VersionSpec{
			Version: "v1", Handler: h,
			Concurrency: concurrency, MaxQueueDepth: queued, IdleTimeout: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}

		// 第一批：占满并发槽，真实派发（该版本的首个池此刻才诞生）。
		var wg sync.WaitGroup
		errs := make(chan error, concurrency+queued+extra)
		start := func(i int) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, e := rt.Invoke(context.Background(), InvokeRequest{
					Function: "rfresh", CallID: idForRound(round, i), Timeout: 30 * time.Second,
				})
				errs <- e
			}()
		}
		for i := 0; i < concurrency; i++ {
			start(i)
		}
		for i := 0; i < concurrency; i++ {
			<-h.entered
		}
		// 第二批：填满队列。
		for i := concurrency; i < concurrency+queued; i++ {
			start(i)
		}
		waitFor(t, func() bool {
			s, _ := rt.Stats("rfresh", "v1")
			return s.Pool.QueueLength == queued
		}, fmt.Sprintf("round=%d 队列应填满", round))

		// 第三批与排空同时起跑：它们要么排空直拒、要么过载，
		// 任何一个都不允许在排空返回后仍处于执行/排队。
		drainDone := make(chan DrainSummary, 1)
		go func() { drainDone <- rt.Drain(80 * time.Millisecond) }()
		waitFor(t, func() bool { return rt.currentDrain() != nil }, "排空状态应已发布")
		for i := concurrency + queued; i < concurrency+queued+extra; i++ {
			start(i)
		}

		total := concurrency + queued + extra
		sum := <-drainDone

		// 被强制中断的在途调用放行后才返回；统一放行，全部收口。
		close(h.release)
		wg.Wait()
		close(errs)

		var succeeded, interruptedN, draining, overloaded, other int
		for e := range errs {
			switch kindOf(e) {
			case "":
				succeeded++
			case KindDraining:
				draining++
			case KindDrainInterrupted:
				interruptedN++
			case KindOverload:
				overloaded++
			default:
				other++
				t.Fatalf("[业务:首批并发排空] round=%d 出现非预期判定 %v", round, e)
			}
		}
		if other != 0 {
			t.Fatalf("[业务:首批并发排空] round=%d 存在非预期错误类别", round)
		}
		if int64(succeeded) != sum.Completed {
			t.Fatalf("[业务:首批并发排空] round=%d 成功数=%d 与 Completed=%d 不符: %+v",
				round, succeeded, sum.Completed, sum)
		}
		if int64(interruptedN) != sum.Interrupted {
			t.Fatalf("[业务:首批并发排空] round=%d 中断数=%d 与 Interrupted=%d 不符: %+v",
				round, interruptedN, sum.Interrupted, sum)
		}
		rejections := sum.NewRejected + sum.QueuedRejected
		if rejections != int64(draining) {
			t.Fatalf("[业务:首批并发排空] round=%d 观测 Draining=%d 与小结拒绝=%d 不符: %+v",
				round, draining, rejections, sum)
		}
		if int64(succeeded)+sum.Interrupted+rejections+int64(overloaded) != int64(total) {
			t.Fatalf("[业务:首批并发排空] round=%d 总数对不上: total=%d summary=%+v overloaded=%d",
				round, total, sum, overloaded)
		}

		// 排空返回后资源必须全部归零：无在跑、无空闲实例、无排队。
		st := assertLedger(t, "首批并发资源清零", fmt.Sprintf("rfresh round=%d", round), rt, "rfresh", "v1")
		if st.Active != 0 || st.IdleCount != 0 || st.QueueLength != 0 {
			t.Fatalf("[业务:首批并发资源清零] round=%d 排空返回后仍有资源占用: %+v", round, st)
		}
		expectKind(t, "首批并发排空后拒绝", fmt.Sprintf("rfresh/after-%d", round),
			mustInvokeErr(rt, "rfresh", fmt.Sprintf("after-%d", round)), KindDraining)

		waitFor(t, func() bool {
			s, _ := rt.Stats("rfresh", "v1")
			return s.Pool.Detached == 0
		}, "round 间 detached 应收尾")
		rt.Close()
	}
	t.Logf("[业务:首批并发排空] 判定: 60 轮重复编排首批派发+排队+并发提交对排空；无漏拦，排空返回后 active=idle=queue=0，拒绝原因统一 Draining")
}

// 业务：Stats.Idle 保持历史布尔语义（有没有空闲实例），老运维脚本的
// `if st.Idle` 条件判断升级后仍可编译、语义不变；具体数量走新字段
// IdleCount。
func TestStatsIdleBoolBackwardCompatible(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	h := newBarrier("compat-idle")
	if err := rt.Register("cfn", VersionSpec{
		Version: "v1", Handler: h, Concurrency: 2, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	before, err := rt.Stats("cfn", "v1")
	if err != nil {
		t.Fatal(err)
	}
	// 历史语义：从未跑过时没有空闲实例，布尔为 false。
	if before.Pool.Idle {
		t.Fatalf("[业务:布尔兼容] 输入 cfn/v1 未调用过期望 Idle=false，得到 true")
	}

	h.release() // 调用同步跑完并归还实例
	if _, err := rt.Invoke(context.Background(), InvokeRequest{Function: "cfn", CallID: "c-1", Payload: []byte("z")}); err != nil {
		t.Fatalf("[业务:布尔兼容] 准备调用失败: %v", err)
	}

	after, err := rt.Stats("cfn", "v1")
	if err != nil {
		t.Fatal(err)
	}
	// 老调用方姿势：直接拿布尔字段做条件判断（必须可编译、行为与旧版一致）。
	if !after.Pool.Idle {
		t.Fatalf("[业务:布尔兼容] 输入 cfn/v1 调用结束后期望 Idle=true，得到 false；stats=%+v", after.Pool)
	}
	// 新调用方拿数量。
	if after.Pool.IdleCount != 1 {
		t.Fatalf("[业务:布尔兼容] IdleCount 应为 1，得到 %d", after.Pool.IdleCount)
	}
	t.Logf("[业务:布尔兼容] 输入 cfn/v1；判定: Idle=true 可直接做条件判断（旧语义不变），数量用 IdleCount=1")
}
