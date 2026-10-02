# serverless-function-runtime-go

一套完全运行在**本机进程内**的 Serverless 函数运行时：不依赖容器、外部服务或
任何远程资源。支持函数多版本与原子更新、函数级并发上限与 FIFO 排队、
实例复用与空闲回收、调用超时/取消归一化，以及基于 `CallID` 的提交幂等。

## 快速开始

```bash
go run ./example
CGO_ENABLED=1 go test ./runtime/ -race -count=10 -v
```

## 最小用法

```go
rt := runtime.New(func(spec runtime.VersionSpec) (runtime.Instance, error) {
    return runtime.NewFuncInstance(func(ctx context.Context, in any) (any, error) {
        return "echo:" + in.(string), nil
    }, nil), nil
}, runtime.Options{})
defer rt.Close()

rt.RegisterFunction("hello", "v1", runtime.FunctionConfig{
    MaxConcurrency: 2, MaxQueue: 4,
    InvokeTimeout: time.Second, QueueTimeout: time.Second,
    InstanceIdleTimeout: time.Minute,
})

r := rt.Invoke(context.Background(), "hello", "world",
    runtime.InvokeOptions{CallID: "unique-call-id"}) // 幂等
// r.Status == runtime.StatusOK, r.Output == "echo:world"
```

## 文档

- 设计与语义、错误码、复现步骤：[`docs/DESIGN.md`](docs/DESIGN.md)

## 平台错误码速览

`INVALID_CONFIG` · `FUNCTION_NOT_FOUND` · `VERSION_UNKNOWN` ·
`VERSION_DEACTIVATED` · `OVERLOADED` · `QUEUED_TIMEOUT` ·
`INVOCATION_TIMEOUT` · `CALLER_CANCELLED` · `EXECUTOR_PANIC` ·
`INSTANCE_CREATE_FAILED`
