package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// trackedInst 记录自己执行过哪些调用标识；stuck 调用无视取消直到 release。
type trackedInst struct {
	no      int64
	mu      sync.Mutex
	calls   []string
	release chan struct{}
}

func (in *trackedInst) Invoke(ctx context.Context, p []byte) ([]byte, error) {
	id := string(p)
	in.mu.Lock()
	in.calls = append(in.calls, id)
	in.mu.Unlock()
	if id == "stuck" {
		<-in.release // 赖账：ctx 取消也不返回
	}
	return []byte(fmt.Sprintf("inst%d:%s", in.no, id)), nil
}

// 业务八：超时/取消后载体与调用彻底解绑——之后的调用永不落到旧载体，
// 旧载体无论正常返回、报错还是 panic 都后台销毁，账目始终自洽。
func TestDetachedInstanceIsolationAndAccounts(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created atomic.Int64
	var mu sync.Mutex
	instances := map[int64]*trackedInst{}
	var stuckInst *trackedInst
	factory := func() (Handler, error) {
		n := created.Add(1)
		in := &trackedInst{no: n, release: make(chan struct{})}
		mu.Lock()
		instances[n] = in
		if n == 1 {
			stuckInst = in
		}
		mu.Unlock()
		return in, nil
	}
	if err := rt.Register("iso", VersionSpec{
		Version: "v1", NewInstance: factory, Concurrency: 2, MaxQueueDepth: 0,
		IdleTimeout: time.Hour, MaxIdleInstances: intPtr(4),
	}); err != nil {
		t.Fatal(err)
	}

	waitStats := func(pred func(Stats) bool) Stats {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			st, _ := rt.Stats("iso", "v1")
			if pred(st.Pool) {
				return st.Pool
			}
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatal("等待账目条件超时")
		return Stats{}
	}

	_, err := rt.Invoke(context.Background(), InvokeRequest{Function: "iso", CallID: "stuck", Payload: []byte("stuck"), Timeout: 60 * time.Millisecond})
	if kindOf(err) != KindInvokeTimeout {
		t.Fatalf("输入 iso/stuck 期望 %s，得到 %v", KindInvokeTimeout, err)
	}

	st := waitStats(func(s Stats) bool { return s.Detached == 1 })
	if st.Active != 0 || st.Created != 1 {
		t.Fatalf("解绑后期望 Active=0 Created=1 Detached=1，得到 %+v", st)
	}
	t.Logf("[业务:载体解绑] 输入 iso/stuck 超时离开、执行器赖账；判定: %s 结论即定，Detached=1 Active=0，并发额度立刻可用", KindInvokeTimeout)

	// 之后多次调用：必须新建干净实例，永不安排给旧载体。
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("clean-%d", i)
		r, err := rt.Invoke(context.Background(), InvokeRequest{Function: "iso", CallID: id, Payload: []byte(id)})
		if err != nil {
			t.Fatalf("输入 iso/%s 不应失败：%v", id, err)
		}
		payload := string(r.Payload)
		if payload == "inst1:"+id {
			t.Fatalf("输入 iso/%s 落到了还在跑旧调用的载体 inst1：%s", id, payload)
		}
	}
	stuckInst.mu.Lock()
	calls := append([]string(nil), stuckInst.calls...)
	stuckInst.mu.Unlock()
	if len(calls) != 1 || calls[0] != "stuck" {
		t.Fatalf("旧载体只执行过 stuck，实际执行记录=%v", calls)
	}
	t.Logf("[业务:载体隔离] 输入 iso/clean-0..2；判定: 全部落到新建干净载体，实例#1 执行记录始终只有 [stuck]")

	// 账目自洽：Created == Destroyed + Active + Idle + Detached
	fs1, _ := rt.Stats("iso", "v1")
	st = fs1.Pool
	live := int64(st.Active + st.IdleCount + st.Detached)
	if st.Created != st.Destroyed+live {
		t.Fatalf("账目不自洽：created=%d destroyed=%d active+idle+detached=%d", st.Created, st.Destroyed, live)
	}
	t.Logf("[业务:账目自洽] 判定: Created=%d == Destroyed=%d + (Active%d + Idle%d + Detached%d)，长期不返回不占并发额度",
		st.Created, st.Destroyed, st.Active, st.IdleCount, st.Detached)

	// 旧载体终于返回：自动销毁，Detached 清零，Created 不变，Destroyed+1。
	before := st.Destroyed
	close(stuckInst.release)
	st = waitStats(func(s Stats) bool { return s.Detached == 0 })
	if st.Created != 2 || st.Destroyed != before+1 {
		t.Fatalf("迟到载体应销毁且不新建：Created=%d Destroyed %d->%d", st.Created, before, st.Destroyed)
	}
	if st.Created != st.Destroyed+int64(st.Active+st.IdleCount+st.Detached) {
		t.Fatalf("收尾后账目仍须自洽：%+v", st)
	}
	t.Logf("[业务:脱离执行治理] 判定: 赖账载体迟到返回后自动销毁（Destroyed %d->%d），不回空闲池、不影响任何结论", before, st.Destroyed)
}

// latePanicInst：第一次调用被解绑后才 panic，验证 panic 不翻案、只销毁载体。
type latePanicInst struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	block   bool
}

func (in *latePanicInst) Invoke(ctx context.Context, p []byte) ([]byte, error) {
	in.once.Do(func() { close(in.started) })
	if in.block {
		<-in.release
		panic("panic after detach")
	}
	return []byte("ok:" + string(p)), nil
}

// 业务八补充：解绑后迟到的 panic/错误只销毁载体，结论不翻案、不影响别的调用。
func TestDetachedLatePanicDoesNotOverride(t *testing.T) {
	rt := NewRuntime()
	defer rt.Close()

	var created atomic.Int64
	var first *latePanicInst
	factory := func() (Handler, error) {
		n := created.Add(1)
		inst := &latePanicInst{started: make(chan struct{}), release: make(chan struct{}), block: n == 1}
		if n == 1 {
			first = inst
		}
		return inst, nil
	}
	if err := rt.Register("pan", VersionSpec{
		Version: "v1", NewInstance: factory, Concurrency: 2, MaxQueueDepth: 0,
		IdleTimeout: time.Hour, MaxIdleInstances: intPtr(2),
	}); err != nil {
		t.Fatal(err)
	}

	_, err := rt.Invoke(context.Background(), InvokeRequest{Function: "pan", CallID: "p1", Payload: []byte("p1"), Timeout: 50 * time.Millisecond})
	if kindOf(err) != KindInvokeTimeout {
		t.Fatalf("输入 pan/p1 期望超时结论 %s，得到 %v", KindInvokeTimeout, err)
	}
	t.Logf("[业务:超时即结论] 输入 pan/p1；判定: %s，载体解绑等待后台 panic", KindInvokeTimeout)

	// 新调用立刻能拿到全新实例（旧实例已解绑，不复用）。
	r2, err := rt.Invoke(context.Background(), InvokeRequest{Function: "pan", CallID: "p2", Payload: []byte("p2"), Timeout: time.Second})
	if err != nil || r2 == nil {
		t.Fatalf("解绑后新调用应拿到全新实例正常执行，得到 %+v %v", r2, err)
	}
	if created.Load() != 2 {
		t.Fatalf("解绑载体不可复用，应新建实例，created=%d", created.Load())
	}
	t.Logf("[业务:解绑不复用] 输入 pan/p2；判定: 新建第 2 个实例并成功，未触碰解绑载体")

	// 放行解绑实例，让它迟到 panic；等待其后台收尾销毁。
	close(first.release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := rt.Stats("pan", "v1")
		if st.Pool.Detached == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// p1 结论仍是超时，迟到 panic 不翻案；后续调用继续正常。
	_, e := rt.Invoke(context.Background(), InvokeRequest{Function: "pan", CallID: "p1"})
	if kindOf(e) != KindInvokeTimeout {
		t.Fatalf("迟到 panic 不能翻案，p1 仍应为 %s，得到 %v", KindInvokeTimeout, e)
	}
	r3, err := rt.Invoke(context.Background(), InvokeRequest{Function: "pan", CallID: "p3", Payload: []byte("p3")})
	if err != nil || r3 == nil {
		t.Fatalf("迟到 panic 不应影响后续调用：%v", err)
	}
	t.Logf("[业务:迟到panic治理] 判定: 解绑载体迟到 panic 后自动销毁，p1 结论仍为 %s，p3 正常", KindInvokeTimeout)
}
