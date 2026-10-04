package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// 业务：超时/取消后执行载体彻底解绑，不污染后续调用，账目长期自洽。
func TestDetachedExecutionIsolationAndLedger(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created atomic.Int64
	stuck := &barrier{name: "leak", started: make(chan struct{}), rel: make(chan struct{}), ignoreCtx: true}
	factory := func() (Handler, error) {
		n := created.Add(1)
		// 每次工厂产出独立载体：第一个故意赖着不走，后续载体立即返回。
		if n == 1 {
			return stuck, nil
		}
		return HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
			return []byte(fmt.Sprintf("inst%d:%s", n, p)), nil
		}), nil
	}
	if err := rt.Register("leakfn", VersionSpec{
		Version: "v1", NewInstance: factory, Concurrency: 1, MaxQueueDepth: 0, IdleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	// 第一次调用超时，执行器完全不响应取消。
	_, err := rt.Invoke(context.Background(), InvokeRequest{Function: "leakfn", CallID: "slow", Timeout: 80 * time.Millisecond})
	expectKind(t, "超时解绑", "leakfn/slow", err, KindInvokeTimeout)
	waitFor(t, func() bool {
		s, _ := rt.Stats("leakfn", "v1")
		return s.Pool.Detached == 1
	}, "超时后载体应转为 detached")
	st := assertLedger(t, "超时后账目", "leakfn/v1", rt, "leakfn", "v1")
	if st.Active != 0 || st.Created != 1 {
		t.Fatalf("超时后 active 应归零且旧载体已解绑，得到 %+v", st)
	}
	t.Logf("[业务:超时解绑] 输入 leakfn/slow 执行器忽略取消；判定: active=0 detached=1，额度立即释放")

	// 后续调用必须落到干净的新载体上，绝不与还在跑旧调用的载体复用。
	r, err := rt.Invoke(context.Background(), InvokeRequest{Function: "leakfn", CallID: "clean-1", Payload: []byte("p")})
	if err != nil || string(r.Payload) != "inst2:p" {
		t.Fatalf("[业务:载体隔离] 后续调用应使用全新 inst2，得到 %+v %v", r, err)
	}
	t.Logf("[业务:载体隔离] 输入 leakfn/clean-1；判定: 派发到新建 inst2，未触碰仍在跑旧调用的 inst1")

	// 旧执行器此刻才返回：不能影响 clean-1 的结论，只自动销毁，账目持平。
	stuck.release()
	waitFor(t, func() bool {
		s, _ := rt.Stats("leakfn", "v1")
		return s.Pool.Detached == 0
	}, "旧执行器迟到返回后应自动收尾")
	st = assertLedger(t, "迟到返回收尾", "leakfn/v1", rt, "leakfn", "v1")
	if st.Created != 2 || st.IdleInstances != 1 {
		t.Fatalf("迟到返回后只应多销毁解绑的 inst1，clean-1 的 inst2 应空闲保留，得到 %+v", st)
	}
	t.Logf("[业务:自动销毁] 判定: inst1 迟到返回后自动销毁，inst2 空闲保留，clean-1 结论不变，账目自洽")

	// 长期不返回的执行也不能把并发额度卡歪：并发=1 下连续调用都能成功。
	rt2 := NewRuntime()
	defer rt2.Close()
	var created2 atomic.Int64
	mkSlow := func() (Handler, error) {
		created2.Add(1)
		h := &barrier{name: "forever", started: make(chan struct{}), rel: make(chan struct{}), ignoreCtx: true}
		return h, nil
	}
	if err := rt2.Register("foreverfn", VersionSpec{
		Version: "v1", NewInstance: mkSlow, Concurrency: 1, IdleTimeout: 0,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_, e := rt2.Invoke(context.Background(), InvokeRequest{
			Function: "foreverfn", CallID: fmt.Sprintf("z%d", i), Timeout: 30 * time.Millisecond,
		})
		expectKind(t, "额度不卡死", fmt.Sprintf("foreverfn/z%d", i), e, KindInvokeTimeout)
		assertLedger(t, "连串超时账目", fmt.Sprintf("foreverfn/z%d", i), rt2, "foreverfn", "v1")
	}
	fs, _ := rt2.Stats("foreverfn", "v1")
	if fs.Pool.Active != 0 || fs.Pool.Detached != 3 {
		t.Fatalf("3 个赖着的执行应全部 detached 且不占额度，得到 %+v", fs.Pool)
	}
	t.Logf("[业务:额度不卡死] 输入 foreverfn/z0..z2；判定: active=0 detached=3，后续仍可派发，账目不歪")
}

// 业务：调用方取消后的载体同样解绑，panic/报错的脱离执行也安全收尾。
func TestDetachedPanicAndErrorAreCollected(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	started := make(chan struct{})
	var release = make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, p []byte) ([]byte, error) {
		close(started)
		<-release
		panic("late panic after cancel")
	})
	if err := rt.Register("pfn", VersionSpec{Version: "v1", Handler: h, Concurrency: 1, IdleTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := rt.Invoke(ctx, InvokeRequest{Function: "pfn", CallID: "c1"})
		done <- e
	}()
	<-started
	cancel()
	expectKind(t, "取消解绑", "pfn/c1", <-done, KindCanceled)
	close(release) // 脱离后的执行器 panic
	waitFor(t, func() bool {
		s, _ := rt.Stats("pfn", "v1")
		return s.Pool.Detached == 0
	}, "panic 的脱离执行应被兜底收尾")
	assertLedger(t, "panic 收尾账目", "pfn/v1", rt, "pfn", "v1")
	t.Logf("[业务:脱离 panic 兜底] 判定: 取消后执行器 panic 不影响任何结论，载体销毁、账目自洽")
}
