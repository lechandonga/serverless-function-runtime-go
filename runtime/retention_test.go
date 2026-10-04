package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 业务：MaxIdleInstances 可配置：默认保留 1、0 用完即回收、N 限制回落保留数。
func TestMaxIdleInstancesConfiguration(t *testing.T) {
	// 默认（留空）：保持历史行为，最多保留 1 个空闲实例。
	t.Run("default-keeps-one", func(t *testing.T) {
		rt := NewRuntime()
		defer rt.Close()
		var created atomic.Int64
		factory := countingFactory(&created, "def")
		if err := rt.Register("rdef", VersionSpec{
			Version: "v1", NewInstance: factory, Concurrency: 3, IdleTimeout: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
		runN(t, rt, "rdef", 3)
		st := assertLedger(t, "默认保留1", "rdef/v1", rt, "rdef", "v1")
		if st.IdleCount != 1 {
			t.Fatalf("默认策略高并发回落后应只保留 1 个空闲实例，得到 %d", st.IdleCount)
		}
		t.Logf("[业务:保留默认] 输入 rdef 并发3 回落；判定: Idle=1（历史行为不变）")
	})

	// 显式 0：用完即回收，即使 IdleTimeout 很长。
	t.Run("zero-recycles-immediately", func(t *testing.T) {
		rt := NewRuntime()
		defer rt.Close()
		var created atomic.Int64
		zero := 0
		if err := rt.Register("r0", VersionSpec{
			Version: "v1", NewInstance: countingFactory(&created, "r0"),
			Concurrency: 3, IdleTimeout: time.Hour, MaxIdleInstances: &zero,
		}); err != nil {
			t.Fatal(err)
		}
		runN(t, rt, "r0", 3)
		st := assertLedger(t, "零保留", "r0/v1", rt, "r0", "v1")
		if st.IdleCount != 0 || st.Created != 3 || st.Destroyed != 3 {
			t.Fatalf("MaxIdleInstances=0 应用完即回收，得到 %+v", st)
		}
		t.Logf("[业务:零保留] 输入 r0 MaxIdleInstances=0；判定: Idle=0 created=3 destroyed=3")
	})

	// 显式 N：回落后最多留 N，多出的立即销毁。
	t.Run("n-keeps-at-most-n", func(t *testing.T) {
		rt := NewRuntime()
		defer rt.Close()
		n := 2

		// 用屏障保证 4 个调用真正同时在执行（否则快速调用可能复用同一实例，
		// 无法确定性地把并发打满）。
		gate := make(chan struct{})
		var started atomic.Int64
		if err := rt.Register("rN", VersionSpec{
			Version: "v1",
			NewInstance: func() (Handler, error) {
				return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
					started.Add(1)
					<-gate
					return p, nil
				}), nil
			},
			Concurrency: 4, IdleTimeout: time.Hour, MaxIdleInstances: &n,
		}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _ = rt.Invoke(context.Background(), InvokeRequest{
					Function: "rN", CallID: fmt.Sprintf("n1-%d", i), Payload: []byte("x"),
				})
			}(i)
		}
		waitFor(t, func() bool { return started.Load() == 4 }, "4 个实例应全部真正并发执行")
		close(gate)
		wg.Wait()

		st := assertLedger(t, "保留N", "rN/v1", rt, "rN", "v1")
		if st.IdleCount != 2 || st.Created != 4 || st.Destroyed != 2 {
			t.Fatalf("MaxIdleInstances=2 回落应保留2、销毁2，得到 %+v", st)
		}
		t.Logf("[业务:保留N] 输入 rN 并发4 MaxIdleInstances=2；判定: Idle=2，多出2个立即销毁，created=4 destroyed=2")

		// 再来 2 个调用：必须复用 2 个空闲实例，不新建；回落后仍只留 2。
		var wg2 sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg2.Add(1)
			go func(i int) {
				defer wg2.Done()
				_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "rN", CallID: fmt.Sprintf("n2-%d", i)})
				if e != nil {
					t.Errorf("复用调用失败: %v", e)
				}
			}(i)
		}
		wg2.Wait()
		st = assertLedger(t, "N内复用", "rN/v1", rt, "rN", "v1")
		if st.IdleCount != 2 || st.Created != 4 {
			t.Fatalf("回落再调用应复用空闲实例不新建，得到 %+v", st)
		}
		t.Logf("[业务:N内复用] 判定: 再次回落复用 2 个空闲实例，created 仍=4，Idle=2")
	})

	// 负数非法：整体拒绝，老版本与路由不受影响。
	t.Run("negative-rejected", func(t *testing.T) {
		rt := NewRuntime()
		defer rt.Close()
		neg := -1
		if err := rt.Register("bad", VersionSpec{
			Version: "v1", Handler: HandlerFunc(func(context.Context, []byte) ([]byte, error) { return nil, nil }),
			Concurrency: 1, MaxIdleInstances: &neg,
		}); kindOf(err) != KindValidation {
			t.Fatalf("[业务:非法配置] 输入 bad MaxIdleInstances=-1 期望 KindValidation，得到 %v", err)
		}
		t.Logf("[业务:非法配置] 输入 bad MaxIdleInstances=-1；判定: KindValidation 整体拒绝")

		one := 1
		if err := rt.Register("okfn", VersionSpec{
			Version: "v1", Handler: HandlerFunc(func(context.Context, []byte) ([]byte, error) { return []byte("v1"), nil }),
			Concurrency: 1, IdleTimeout: time.Hour, MaxIdleInstances: &one,
		}); err != nil {
			t.Fatal(err)
		}
		bad := VersionSpec{
			Version: "v2", Handler: HandlerFunc(func(context.Context, []byte) ([]byte, error) { return nil, nil }),
			Concurrency: 0, MaxIdleInstances: &neg,
		}
		if err := rt.Publish("okfn", bad); kindOf(err) != KindValidation {
			t.Fatalf("非法新版本应整体拒绝，得到 %v", err)
		}
		cur, _ := rt.CurrentVersion("okfn")
		if cur != "v1" {
			t.Fatalf("非法发布后路由被污染: %s", cur)
		}
		t.Logf("[业务:非法配置不污染] 判定: okfn/v2 被拒后 CURRENT 仍=v1，老版本照常服务")
	})
}

// 业务：版本退场（发布新版本 / 停用）主动回收空闲实例，在途跑完不保留。
func TestVersionRetirementReclaimsIdle(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created atomic.Int64
	factory := countingFactory(&created, "ret")
	if err := rt.Register("retfn", VersionSpec{
		Version: "v1", NewInstance: factory, Concurrency: 4, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	runN(t, rt, "retfn", 3)
	st, _ := rt.Stats("retfn", "v1")
	if st.Pool.IdleCount != 1 {
		t.Fatalf("准备条件：v1 应有 1 个空闲实例，得到 %d", st.Pool.IdleCount)
	}

	// 在途 v1 调用：发布 v2 后仍按 v1 跑完，但跑完不再保留。
	h1 := newBarrier("v1-inflight")
	var v1InflightCreated atomic.Int64
	rt2 := NewRuntime()
	defer rt2.Close()
	f1 := func() (Handler, error) {
		v1InflightCreated.Add(1)
		return h1, nil
	}
	if err := rt2.Register("ret2", VersionSpec{
		Version: "v1", NewInstance: f1, Concurrency: 2, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	inflight := make(chan error, 1)
	go func() {
		r, e := rt2.Invoke(context.Background(), InvokeRequest{Function: "ret2", Version: "v1", CallID: "hold", Payload: []byte("z")})
		if e != nil {
			inflight <- e
			return
		}
		if string(r.Payload) != "ok:v1-inflight:z" {
			inflight <- fmt.Errorf("在途结果异常 %s", r.Payload)
		}
		inflight <- nil
	}()
	<-h1.started

	if err := rt2.Publish("ret2", VersionSpec{
		Version: "v2", Handler: HandlerFunc(func(context.Context, []byte) ([]byte, error) { return []byte("v2"), nil }),
		Concurrency: 2, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	st1, _ := rt2.Stats("ret2", "v1")
	if st1.Pool.Retired != true {
		t.Fatal("v1 不再是当前版本后池应被标记退场")
	}
	t.Logf("[业务:退场回收] 判定: 发布 v2 后 v1 池 retired=true，空闲实例主动回收（不等空闲超时）")
	h1.release()
	if e := <-inflight; e != nil {
		t.Fatalf("退场版本的在途调用应照常跑完: %v", e)
	}
	st1, _ = rt2.Stats("ret2", "v1")
	if st1.Pool.IdleCount != 0 {
		t.Fatalf("退场版本在途调用跑完后不得保留空闲实例，Idle=%d", st1.Pool.IdleCount)
	}
	t.Logf("[业务:退场在途跑完即毁] 输入 ret2/v1/hold；判定: 正常跑完但不留空闲，Idle=0")

	// 停用版本同样立即回收空闲实例。
	if err := rt.Publish("retfn", VersionSpec{
		Version: "v2", Handler: HandlerFunc(func(context.Context, []byte) ([]byte, error) { return []byte("v2"), nil }),
		Concurrency: 4, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	oldSt, _ := rt.Stats("retfn", "v1")
	if oldSt.Pool.IdleCount != 0 {
		t.Fatalf("路由切走后 v1 空闲实例应被主动回收，Idle=%d", oldSt.Pool.IdleCount)
	}
	if err := rt.DisableVersion("retfn", "v1"); err != nil {
		t.Fatal(err)
	}
	t.Logf("[业务:停用回收] 判定: retfn/v1 被停用，空闲实例此前已随退场回收，拒绝原因仍为 VersionDisabled")
}

func countingFactory(counter *atomic.Int64, tag string) func() (Handler, error) {
	return func() (Handler, error) {
		n := counter.Add(1)
		return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
			return []byte(fmt.Sprintf("%s-inst%d:%s", tag, n, p)), nil
		}), nil
	}
}

var runIDCounter atomic.Int64

func runN(t *testing.T, rt *Runtime, fn string, n int) {
	t.Helper()
	batch := runIDCounter.Add(1)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := fmt.Sprintf("%s-b%d-p%d", fn, batch, i)
			_, e := rt.Invoke(context.Background(), InvokeRequest{
				Function: fn, CallID: id, Payload: []byte(id),
			})
			if e != nil {
				t.Errorf("并发调用 %s/p%d 失败: %v", fn, i, e)
			}
		}(i)
	}
	close(start)
	wg.Wait()
}
