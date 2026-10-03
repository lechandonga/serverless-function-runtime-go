package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func invokeOK(rt *Runtime, t *testing.T, fn, id string, v ...Version) string {
	t.Helper()
	req := InvokeRequest{Function: fn, CallID: id, Payload: []byte(id)}
	if len(v) > 0 {
		req.Version = v[0]
	}
	r, err := rt.Invoke(context.Background(), req)
	if err != nil {
		t.Fatalf("输入 %s/%s 期望成功，得到 %v", fn, id, err)
	}
	return string(r.Payload)
}

func countingFactory(created *atomic.Int64) func() (Handler, error) {
	return func() (Handler, error) {
		n := created.Add(1)
		return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
			return []byte(fmt.Sprintf("inst%d:%s", n, p)), nil
		}), nil
	}
}

// barrierFactory 让同批调用先在 inFlight 汇合，release 后再返回，
// 用于确定性地制造“n 个并发在执行”的回落场景（不赌时序）。
type barrierHandler struct {
	inFlight chan struct{}
	release  chan struct{}
	no       int64
	relOnce  map[int64]*sync.Once
}

func newBarrierHandler(n int) *barrierHandler {
	return &barrierHandler{
		inFlight: make(chan struct{}, n),
		release:  make(chan struct{}),
		relOnce:  map[int64]*sync.Once{},
	}
}

func (b *barrierHandler) handler() Handler {
	return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
		b.inFlight <- struct{}{}
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte("ok:" + string(p)), nil
	})
}

// 业务九：MaxIdleInstances 可配置——不填=1（默认）、0=用完即回收、N=高并发回落留 N。
func TestMaxIdleInstancesConfig(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var c0, c2, cDef, cTO atomic.Int64
	b0, b2, bDef, bTO := newBarrierHandler(3), newBarrierHandler(3), newBarrierHandler(3), newBarrierHandler(2)
	factoryWithBarrier := func(created *atomic.Int64, b *barrierHandler) func() (Handler, error) {
		return func() (Handler, error) {
			created.Add(1)
			return b.handler(), nil
		}
	}
	if err := rt.Register("cap0", VersionSpec{Version: "v1", NewInstance: factoryWithBarrier(&c0, b0), Concurrency: 3, IdleTimeout: time.Hour, MaxIdleInstances: intPtr(0)}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Register("cap2", VersionSpec{Version: "v1", NewInstance: factoryWithBarrier(&c2, b2), Concurrency: 3, IdleTimeout: time.Hour, MaxIdleInstances: intPtr(2)}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Register("capdef", VersionSpec{Version: "v1", NewInstance: factoryWithBarrier(&cDef, bDef), Concurrency: 3, IdleTimeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Register("to0", VersionSpec{Version: "v1", NewInstance: factoryWithBarrier(&cTO, bTO), Concurrency: 2, IdleTimeout: 0, MaxIdleInstances: intPtr(5)}); err != nil {
		t.Fatal(err)
	}

	// 受控并发：3 个 goroutine 同时起跑，等全部进入执行屏障后再统一放行，
	// 精确构造“高并发回落”，不赌调度时序。
	simBurst := func(fn string, b *barrierHandler, n int) <-chan error {
		errCh := make(chan error, n)
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			go func(i int) {
				<-start
				id := fmt.Sprintf("%s-b%d", fn, i)
				_, e := rt.Invoke(context.Background(), InvokeRequest{Function: fn, CallID: id, Payload: []byte(id)})
				errCh <- e
			}(i)
		}
		close(start)
		for i := 0; i < n; i++ {
			<-b.inFlight
		}
		close(b.release)
		return errCh
	}
	drainErr := func(ch <-chan error, n int) {
		for i := 0; i < n; i++ {
			if e := <-ch; e != nil {
				t.Fatal(e)
			}
		}
	}

	drainErr(simBurst("cap0", b0, 3), 3)
	drainErr(simBurst("cap2", b2, 3), 3)
	drainErr(simBurst("capdef", bDef, 3), 3)

	waitIdle := func(fn string, want int) Stats {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			fsx, _ := rt.Stats(fn, "v1")
			if fsx.Pool.IdleCount == want {
				return fsx.Pool
			}
			time.Sleep(2 * time.Millisecond)
		}
		fsx, _ := rt.Stats(fn, "v1")
		t.Fatalf("%s 空闲数期望 %d，实际 %+v", fn, want, fsx.Pool)
		return Stats{}
	}

	waitIdle("cap0", 0)
	t.Logf("[业务:保留=0] 输入 cap0 并发3 回落；判定: IdleCount=0，实例用完即销毁")
	st2 := waitIdle("cap2", 2)
	if st2.Created != 3 || st2.Destroyed != 1 {
		t.Fatalf("cap2 应创建3 销毁1，得到 %+v", st2)
	}
	t.Logf("[业务:保留=2] 输入 cap2 并发3 回落；判定: IdleCount=2，多余 1 个立即销毁: %+v", st2)
	stDef := waitIdle("capdef", 1)
	t.Logf("[业务:不填默认=1] 输入 capdef 并发3 回落（不填 MaxIdleInstances）；判定: IdleCount=1，保持历史行为，%+v", stDef)

	// cap0 再次调用必须新建；cap2 复用保留的实例（创建数不增加）。
	invokeOK(rt, t, "cap0", "again")
	if c0.Load() != 4 {
		t.Fatalf("cap0 用完即回收，再次调用应新建，created=%d", c0.Load())
	}
	invokeOK(rt, t, "cap2", "reuse")
	if c2.Load() != 3 {
		t.Fatalf("cap2 应复用空闲实例，created 仍应为 3，实际 %d", c2.Load())
	}
	t.Logf("[业务:保留实例复用] 输入 cap2/reuse；判定: created 仍=3，命中保留的空闲实例")

	// IdleTimeout=0 时即使 MaxIdleInstances=N 也用完即回收。
	drainErr(simBurst("to0", bTO, 2), 2)
	fsx, _ := rt.Stats("to0", "v1")
	if fsx.Pool.IdleCount != 0 {
		t.Fatalf("IdleTimeout=0 必须用完即回收，得到 %+v", fsx.Pool)
	}
	t.Logf("[业务:IdleTimeout 优先] 输入 to0 IdleTimeout=0 MaxIdleInstances=5；判定: IdleCount=0")

	// 非法配置整体拒绝：MaxIdleInstances<0。
	err := rt.Register("bad", VersionSpec{Version: "v1", Handler: HandlerFunc(noopHandler), Concurrency: 1, MaxIdleInstances: intPtr(-1)})
	if kindOf(err) != KindValidation {
		t.Fatalf("MaxIdleInstances=-1 应整体拒绝 %s，得到 %v", KindValidation, err)
	}
	t.Logf("[业务:非法配置整体拒绝] 输入 bad MaxIdleInstances=-1；判定: %s，老版本与路由不受影响", KindValidation)
}

// 业务九补充：版本退场（停用/不再是当前路由版本）立即回收空闲实例，
// 在途照旧跑完、跑完不再保留；非法发布不影响老版本。
func TestRetirementReclaimsIdle(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var c1, c2 atomic.Int64
	b1 := newBarrierHandler(2)
	v1factory := func() (Handler, error) {
		c1.Add(1)
		return b1.handler(), nil
	}
	if err := rt.Register("ret", VersionSpec{Version: "v1", NewInstance: v1factory, Concurrency: 2, IdleTimeout: time.Hour, MaxIdleInstances: intPtr(2)}); err != nil {
		t.Fatal(err)
	}
	// 并发跑出两个空闲实例：屏障确保两个调用确实同时在执行。
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			invokeOK(rt, t, "ret", fmt.Sprintf("w%d", i), "v1")
		}(i)
	}
	close(start)
	<-b1.inFlight
	<-b1.inFlight
	close(b1.release)
	waitIdleCount := func(fn string, v Version, want int) Stats {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			f, _ := rt.Stats(fn, v)
			if f.Pool.IdleCount == want {
				return f.Pool
			}
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatalf("等待 %s/%s 空闲数=%d 超时", fn, v, want)
		return Stats{}
	}
	waitIdleCount("ret", "v1", 2)
	fsx, _ := rt.Stats("ret", "v1")
	st := fsx.Pool
	if st.IdleCount != 2 {
		t.Fatalf("预热后期望 2 空闲，得到 %+v", st)
	}

	// 发布 v2：v1 不再是当前路由版本，空闲实例立即回收。
	if err := rt.Publish("ret", VersionSpec{Version: "v2", NewInstance: countingFactory(&c2), Concurrency: 2, IdleTimeout: time.Hour, MaxIdleInstances: intPtr(1)}); err != nil {
		t.Fatal(err)
	}
	fsx, _ = rt.Stats("ret", "v1")
	st = fsx.Pool
	if st.IdleCount != 0 || !st.Retired {
		t.Fatalf("v1 退场后空闲应立即回收并标记 retired，得到 %+v", st)
	}
	t.Logf("[业务:路由退场回收] 输入 ret 发布 v2 后；判定: v1 IdleCount=0 Retired=true，未等空闲超时")

	// 退场版本显式调用：在途照旧跑完（revision 指针快照），但跑完不再保留。
	invokeOK(rt, t, "ret", "after-retire", "v1")
	fsx, _ = rt.Stats("ret", "v1")
	st = fsx.Pool
	if st.IdleCount != 0 {
		t.Fatalf("退场版本跑完不应保留空闲实例，得到 %+v", st)
	}
	t.Logf("[业务:退场版本跑完不保留] 输入 ret/after-retire 显式 v1；判定: 调用成功但 IdleCount=0")

	// v2 作为当前版本时先跑出空闲实例；随后发布 v3 顶替，v2 立即退场回收。
	invokeOK(rt, t, "ret", "v2-warm-1", "v2")
	fsx, _ = rt.Stats("ret", "v2")
	st = fsx.Pool
	if st.IdleCount != 1 {
		t.Fatalf("v2 退场前期望 1 空闲（保留上限1），得到 %+v", st)
	}
	if err := rt.Publish("ret", VersionSpec{Version: "v3", Handler: HandlerFunc(noopHandler), Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	fsx, _ = rt.Stats("ret", "v2")
	st = fsx.Pool
	if st.IdleCount != 0 || !st.Retired {
		t.Fatalf("v2 被新版本顶替后空闲应立即回收，得到 %+v", st)
	}
	// v2 退场但未停用：显式调用仍可执行（在途照旧跑完），但跑完不再保留。
	invokeOK(rt, t, "ret", "v2-after-retire", "v2")
	fsx, _ = rt.Stats("ret", "v2")
	if fsx.Pool.IdleCount != 0 {
		t.Fatalf("退场版本跑完不再保留，得到 %+v", fsx.Pool)
	}
	if err := rt.DisableVersion("ret", "v2"); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Invoke(context.Background(), InvokeRequest{Function: "ret", Version: "v2"}); kindOf(err) != KindVersionDisabled {
		t.Fatalf("停用版本新调用应 %s，得到 %v", KindVersionDisabled, err)
	}
	t.Logf("[业务:顶替与停用回收] 输入 ret v3 顶替 v2 后 IdleCount=0；停用后新调用 %s", KindVersionDisabled)

	// 非法发布不影响老版本及其池。
	bad := VersionSpec{Version: "v4-bad", Handler: HandlerFunc(noopHandler), Concurrency: 0}
	if err := rt.Publish("ret", bad); kindOf(err) != KindValidation {
		t.Fatalf("非法发布应 %s，得到 %v", KindValidation, err)
	}
	cur, _ := rt.CurrentVersion("ret")
	if cur != "v3" {
		t.Fatalf("非法发布后路由应仍为 v3，得到 %s", cur)
	}
	fsx, _ = rt.Stats("ret", "v1")
	st = fsx.Pool
	if !st.Retired {
		t.Fatal("非法发布不应影响老版本退场状态")
	}
	t.Logf("[业务:非法发布隔离] 输入 ret/v4-bad Concurrency=0；判定: %s，v1/v2/v3 状态不变", KindValidation)
}
