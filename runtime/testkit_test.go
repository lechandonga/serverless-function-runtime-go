package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scriptInstance 是一个完全可控的执行实例，用于确定性地制造
// “执行开始/阻塞/完成/panic/忽略 ctx”等场景。
type scriptInstance struct {
	spec VersionSpec

	mu       sync.Mutex
	executed int64 // Execute 被调用次数
	closed   int32 // Close 被调用次数

	startCh   chan struct{} // 每次 Execute 开始时发送（缓冲由构造方决定容量）
	relCh     chan struct{} // nil 表示立即返回；否则阻塞到该通道关闭
	blockCh   chan struct{} // 与 relCh 二选一：按调用控制放行
	panicV    any
	ignoreCtx bool
}

func (s *scriptInstance) Execute(ctx context.Context, in any) (any, error) {
	atomic.AddInt64(&s.executed, 1)
	if s.startCh != nil {
		select {
		case s.startCh <- struct{}{}:
		default:
		}
	}
	if s.panicV != nil {
		panic(s.panicV)
	}
	if s.blockCh != nil {
		select {
		case <-s.blockCh:
		case <-ctx.Done():
			if !s.ignoreCtx {
				return nil, ctx.Err()
			}
			<-s.blockCh
		}
	} else if s.relCh != nil {
		select {
		case <-s.relCh:
		case <-ctx.Done():
			if !s.ignoreCtx {
				return nil, ctx.Err()
			}
			<-s.relCh
		}
	}
	return outFor(s.spec, in), nil
}

func outFor(spec VersionSpec, in any) string {
	return spec.Function + "@" + spec.Version + ":" + in.(string)
}

func (s *scriptInstance) Close() error {
	atomic.AddInt32(&s.closed, 1)
	return nil
}

func (s *scriptInstance) execCount() int  { return int(atomic.LoadInt64(&s.executed)) }
func (s *scriptInstance) closeCount() int { return int(atomic.LoadInt32(&s.closed)) }

// scriptFactory 按版本名前缀构造不同行为的实例，并用 instances 收集创建结果。
type scriptFactory struct {
	mu        sync.Mutex
	instances []*scriptInstance
	build     func(spec VersionSpec) *scriptInstance
}

func newScriptFactory(build func(spec VersionSpec) *scriptInstance) *scriptFactory {
	return &scriptFactory{build: build}
}

func (f *scriptFactory) Factory() Factory {
	return func(spec VersionSpec) (Instance, error) {
		s := f.build(spec)
		s.spec = spec
		f.mu.Lock()
		f.instances = append(f.instances, s)
		f.mu.Unlock()
		return s, nil
	}
}

func (f *scriptFactory) all() []*scriptInstance {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*scriptInstance, len(f.instances))
	copy(out, f.instances)
	return out
}

// waitInstance 等待工厂创建出第 idx 个实例，消除测试调度竞态。
func waitInstance(t *testing.T, f *scriptFactory, idx int) *scriptInstance {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if all := f.all(); len(all) > idx {
			return all[idx]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("[判定] 等待第 %d 个实例创建超时", idx)
	return nil
}

// expectCode 断言平台错误码；返回错误信息供日志使用。
func expectCode(t *testing.T, r *Result, code ErrorCode) {
	t.Helper()
	if r.Status != StatusPlatformError {
		t.Fatalf("[判定] 期望平台错误 %s，实际状态=%s err=%v", code, r.Status, r.Err)
	}
	pe, ok := r.Err.(*PlatformError)
	if !ok {
		t.Fatalf("[判定] 期望 *PlatformError(%s)，实际 %T: %v", code, r.Err, r.Err)
	}
	if pe.Code != code {
		t.Fatalf("[判定] 期望错误码 %s，实际 %s (%v)", code, pe.Code, pe)
	}
	t.Logf("[判定] 错误码符合预期: %s — %s", pe.Code, pe.Message)
}

func expectOK(t *testing.T, r *Result, wantOut string, wantVersion string) {
	t.Helper()
	if r.Status != StatusOK {
		t.Fatalf("[判定] 期望 OK，实际 %s err=%v", r.Status, r.Err)
	}
	if r.Output != wantOut {
		t.Fatalf("[判定] 期望输出 %q，实际 %v", wantOut, r.Output)
	}
	if r.Version != wantVersion {
		t.Fatalf("[判定] 期望执行版本 %q，实际 %q", wantVersion, r.Version)
	}
	t.Logf("[判定] OK: 输出=%q 版本=%q", wantOut, wantVersion)
}
