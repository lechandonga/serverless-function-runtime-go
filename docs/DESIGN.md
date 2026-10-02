# 本机 Serverless 函数运行时 — 设计与语义说明

本工程实现了一套**完全运行在本机进程内**的 Serverless 函数运行时：函数以
Go 对象（`runtime.Instance`）的形式注册，由一个共享的 `Factory` 按版本快照
惰性创建。全程**不使用容器、不监听网络端口、不依赖任何外部服务或远程资源**。

- 运行时代码：`runtime/`
- 可运行示例：`example/`（`go run ./example`）
- 测试：`runtime/*_test.go`

## 1. 核心抽象

```go
type Instance interface {
    Execute(ctx context.Context, in any) (any, error)
    Close() error
}
type Factory func(version VersionSpec) (Instance, error)
```

- 一个 `Runtime` 管理多个**函数**（`RegisterFunction(name, initialVersion, cfg)`）。
- 每个函数有多个**版本**（`PublishVersion`），版本用字符串标识。
- 每个版本有独立的**实例池**；实例首次调用时由工厂创建，调用结束后回到
  空闲队列复用，空闲超过 `InstanceIdleTimeout` 被后台回收。
- 所有版本共享同一个**函数级并发闸**（名额数 = `MaxConcurrency`）。

调用返回统一的 `Result{Status, Output, Err, Version}`：

| Status | 含义 |
| --- | --- |
| `OK` | 执行器正常返回 |
| `FAILED` | 执行器返回业务错误（`Err` 即原始错误，不是平台故障） |
| `PLATFORM_ERROR` | 平台侧问题，`Err` 一定是可按码区分的 `*PlatformError` |

## 2. 版本与路由语义

- `InvokeOptions.Version` 为空时路由到函数的**当前版本**；非空时调用指定版本。
- 当前版本通过 `PublishVersion(..., setCurrent=true)` 或 `SetCurrent`
  **原子切换**。
- 调用开始时会先解析并固定**版本快照**（`VersionSpec` + 对应实例池）。
  之后即便当前路由切换、该版本被停用，本次调用也**始终按开始时的版本跑完**。
- 版本拒绝原因严格区分：
  - `VERSION_UNKNOWN`：该版本号从未发布过；
  - `VERSION_DEACTIVATED`：版本发布过但已被停用（新版本调用被拒，在途调用
    不受影响，空闲实例立即回收）。
  - 函数不存在则是 `FUNCTION_NOT_FOUND`。
- `DeactivateVersion` 拒绝停用“当前路由指向的版本”，避免线上版本被误停；
  请先用 `SetCurrent` 切走再停用。已停用版本的条目仍保留（因此重发同名版本
  会得到 `INVALID_CONFIG`，且它与 unknown 始终可区分）。

### 原子更新（没有“改了一半”的状态）

- `RegisterFunction` / `PublishVersion` / `SetCurrent` / `UpdateConfig` /
  `DeactivateVersion` 都在运行时写锁内完成，失败时**什么都不改变**。
- `UpdateConfig` 先做完整校验（填充默认值后校验），通过后才一次性替换配置、
  调整并发闸大小和各实例池上限；校验失败时旧配置、旧路由、旧实例全部继续可用。
- 非法值示例：`MaxConcurrency < 1`、`InvokeTimeout <= 0` 等，一律
  `INVALID_CONFIG` 整体拒绝。

## 3. 实例与并发控制规则

### 并发闸（函数级，跨版本）

- `MaxConcurrency`：同时进入执行器的调用上限。
- `MaxQueue`：满载时允许的排队数，严格 **FIFO**；队列也满时立即返回
  `OVERLOADED`（快速失败，可重试）。
- 排队超过 `QueueTimeout` 返回 `QUEUED_TIMEOUT`（调用从未进入执行器）。
- 排队期间调用方 `ctx` 取消，返回 `CALLER_CANCELLED`，排队者被摘除；
  若取消与放行同时发生，名额会被立即归还并派发给下一个排队者，
  不会出现名额泄漏或重复占用。
- `UpdateConfig` 动态调大/调小上限立即生效：调大时排队者立刻被放行；
  调小时不抢占在途调用，名额按“归还一个、消化一个”收敛。

### 实例池（版本级）

- `MaxInstancesPerVersion` 限制单版本热实例（忙碌+空闲）数量。
- 派发顺序：优先取空闲实例复用，否则新建；已达上限则等待归还/回收腾出名额。
- 空闲实例记录“进入空闲的时刻”，后台回收循环（周期 `ReapInterval`）
  按 `InstanceIdleTimeout` 回收；也可调用 `ReapNow()` 立即扫描。
- 回收只关闭**空闲**实例；忙碌实例绝不会被关闭或再次派发。回收后再次调用
  会由工厂重新创建实例。
- 实例工厂返回错误或 panic 都归一化为 `INSTANCE_CREATE_FAILED`，创建失败
  会回滚已占用的实例名额；故障被限制在该函数/版本内，其他函数不受影响。

### “回收 / 派发 / 版本更新同时发生”如何保证安全

- 实例池的所有状态（空闲队列、忙碌计数、停用标志）都在同一把互斥锁下变更；
  `Execute` 本身在锁外执行，不持有池锁。
- 停用版本（`shutdownAll`）会：置停用标志 → 唤醒等待者 → 回收全部空闲实例。
  等待中的派发被唤醒后要么拿到实例、要么收到 `VERSION_DEACTIVATED`。
- 回收从空闲队列摘除与派发从空闲队列取出是互斥的，因此调用永远拿不到
  “已经关闭/即将关闭”的实例。
- 并发交织场景由
  `TestConcurrentReclaimDispatchAndVersionUpdates` 在 `-race` 下多轮验证。

## 4. 超时、取消与幂等语义

### 执行超时 / 调用方取消

- 每次执行在独立 goroutine 中运行，主流程用
  `context.WithTimeout(ctx, InvokeTimeout)` 决定结论。
- 定时器先到：结论立即定为 `INVOCATION_TIMEOUT`（调用方 ctx 已取消则为
  `CALLER_CANCELLED`），调用方立刻返回，**函数名额与实例名额立即释放**，
  后续调用可马上进行。
- **迟到结果不能改变结论**：执行器之后返回的任何结果都被丢弃。
- 超时实例不再回到空闲队列。实现上用一次 CAS 仲裁“执行 goroutine 先完成”
  还是“平台先定案”，保证忙碌名额恰好释放一次、实例恰好关闭/入队一次：
  - 执行器尊重 `ctx`：很快返回，实例随后关闭；
  - 执行器不配合取消（仍在阻塞）：它不再占用并发/实例名额，也不可能收到新
    派发，只延迟自身实例的 `Close`（等它自然返回），不影响其他调用。
- 执行器 panic 被平台 `recover`，归一化为 `EXECUTOR_PANIC`，进程不崩溃，
  同函数后续调用与其他函数都不受影响。

### 幂等（相同 CallID）

- `InvokeOptions.CallID` 非空时启用，作用域为**整个运行时**。
- 相同 `CallID`（包括多个 goroutine 并发重复提交）只真正执行一次：
  - 第一个提交者执行；后续提交者等待同一个 `done` 信号，拿到内容一致的
    `Result`（同一对象的拷贝）。
  - 重复提交时携带的不同输入/版本参数不会生效——以首次提交为准。
  - 等待者自己的 `ctx` 被取消只中断它自己的等待（`CALLER_CANCELLED`），
    不影响正在进行的真实执行，也不改变最终结论。
- 幂等记录保留 `DedupTTL`（默认 10 分钟），惰性过期清理。

## 5. 本地复现与验证

要求 Go 1.25+（开发环境为 Go 1.26）。无需任何外部依赖。

```bash
# 运行示例（版本路由 / 幂等 / 停用区分 / 空闲回收 / 非法更新）
go run ./example

# 全部测试（含详细业务/输入/判定日志）
go test ./runtime/ -v

# 竞态检测多轮（并发用例必须稳定通过）
CGO_ENABLED=1 go test ./runtime/ -race -count=10

# 覆盖率
go test ./runtime/ -cover
```

### 测试用例与覆盖的业务

| 测试 | 覆盖场景 |
| --- | --- |
| `TestVersionRoutingAndInFlightPin` | 显式/当前版本路由、路由原子切换、在途调用版本钉住、非法更新整体拒绝、停用当前版本被拒、unknown/deactivated/not-found 区分 |
| `TestConcurrencyLimitQueueAndOverload` | 并发上限、FIFO 排队、`OVERLOADED`、`QUEUED_TIMEOUT`、动态扩容放行排队者 |
| `TestTimeoutCancelAndLateResult` | 执行超时立即定案、迟到结果被丢弃、半死实例不占用名额且返回后关闭、排队中 ctx 取消归一化 |
| `TestInstanceReuseReclaimAndRebuild` | 实例复用、未到空闲时间不回收、空闲超时回收并 Close、回收后重建且旧实例不再被派发 |
| `TestIdempotentSubmission` | 串行重放 + 8 个并发重复提交只执行一次、结论全部一致 |
| `TestPanicAndFactoryFailureIsolation` | panic 归一化 `EXECUTOR_PANIC`、工厂失败 `INSTANCE_CREATE_FAILED`、故障隔离、同函数后续调用恢复 |
| `TestConcurrentReclaimDispatchAndVersionUpdates` | 回收 + 派发 + 版本停用/发布/切换高强度并发交织，断言结论全部可解释，`-race` 下多轮稳定 |

测试日志格式统一为 `[业务] ... | [输入] ...`、`[观测] ...`、`[判定] ...`，
可直接看出每个用例覆盖的业务、输入标识（如 `CallID=dup-1`、
`call=slow`、`maxConcurrency=2,maxQueue=1`）以及判定依据。

### 时间确定性

测试使用可注入的 `FakeClock`：默认情况下时间只随 `Advance` 前进
（空闲回收行为完全确定）；需要验证“真实超时等待”的用例通过
`WithRealFallback()` 开启真实时间兜底，避免测试因忘记推进时钟而永久挂起。
