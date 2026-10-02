// 本机可运行的 Serverless 函数运行时示例：
//
//	go run ./example
//
// 全程不使用容器、网络或任何远程资源。
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lechandonga/serverless-function-runtime-go/runtime"
)

// counterInstance 是一个有状态的进程内实例：证明实例会被复用，
// 空闲超时后被回收，再次调用会重新创建。
type counterInstance struct {
	version string
	created time.Time
	hits    int
}

func (c *counterInstance) Execute(ctx context.Context, in any) (any, error) {
	c.hits++
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return map[string]any{
		"version":  c.version,
		"instance": fmt.Sprintf("counter@%s#%d", c.created.Format("15:04:05.000"), c.hits),
		"echo":     in,
	}, nil
}

func (c *counterInstance) Close() error {
	fmt.Printf("    [实例回收] version=%s hits=%d\n", c.version, c.hits)
	return nil
}

func main() {
	factory := func(spec runtime.VersionSpec) (runtime.Instance, error) {
		switch spec.Version {
		case "v1":
			return &counterInstance{version: "v1", created: time.Now()}, nil
		case "v2":
			return &counterInstance{version: "v2", created: time.Now()}, nil
		}
		return nil, errors.New("unknown version: " + spec.Version)
	}

	rt := runtime.New(factory, runtime.Options{
		ReapInterval: 200 * time.Millisecond,
	})
	defer rt.Close()

	// 注册函数：2 并发、1 排队、实例空闲 800ms 回收。
	if err := rt.RegisterFunction("counter", "v1", runtime.FunctionConfig{
		MaxConcurrency:         2,
		MaxQueue:               1,
		MaxInstancesPerVersion: 2,
		InvokeTimeout:          300 * time.Millisecond,
		QueueTimeout:           200 * time.Millisecond,
		InstanceIdleTimeout:    800 * time.Millisecond,
	}); err != nil {
		panic(err)
	}
	rt.PublishVersion("counter", "v2", true) // 发布并切换当前版本到 v2

	ctx := context.Background()

	fmt.Println("== 1. 当前版本路由（应走 v2）与显式版本调用（v1）==")
	show(rt.Invoke(ctx, "counter", "hello", runtime.InvokeOptions{}))
	show(rt.Invoke(ctx, "counter", "world", runtime.InvokeOptions{Version: "v1"}))

	fmt.Println("== 2. 幂等：相同 CallID 只执行一次 ==")
	show(rt.Invoke(ctx, "counter", "first", runtime.InvokeOptions{CallID: "call-001"}))
	show(rt.Invoke(ctx, "counter", "IGNORED", runtime.InvokeOptions{CallID: "call-001"}))

	fmt.Println("== 3. 未知版本 vs 已停用版本 ==")
	rt.DeactivateVersion("counter", "v1")
	show(rt.Invoke(ctx, "counter", "x", runtime.InvokeOptions{Version: "v1"}))
	show(rt.Invoke(ctx, "counter", "x", runtime.InvokeOptions{Version: "v9"}))

	fmt.Println("== 4. 等待空闲超时，观察实例被回收；再调用自动重建 ==")
	time.Sleep(1200 * time.Millisecond)
	show(rt.Invoke(ctx, "counter", "reborn", runtime.InvokeOptions{}))

	fmt.Println("== 5. 非法配置更新被整体拒绝（旧配置继续可用）==")
	err := rt.UpdateConfig("counter", runtime.FunctionConfig{MaxConcurrency: -3})
	fmt.Printf("    更新结果: %v\n", err)
	show(rt.Invoke(ctx, "counter", "still-works", runtime.InvokeOptions{}))
}

func show(r *runtime.Result) {
	switch r.Status {
	case runtime.StatusOK:
		fmt.Printf("    OK version=%s output=%v\n", r.Version, r.Output)
	case runtime.StatusFailed:
		fmt.Printf("    FAILED version=%s err=%v\n", r.Version, r.Err)
	default:
		if pe, ok := r.Err.(*runtime.PlatformError); ok {
			fmt.Printf("    PLATFORM_ERROR code=%s msg=%s\n", pe.Code, pe.Message)
		} else {
			fmt.Printf("    PLATFORM_ERROR err=%v\n", r.Err)
		}
	}
}
