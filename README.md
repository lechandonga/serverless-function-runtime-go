# 本地 Serverless 函数运行时（Go）

一个纯本地、进程内、零外部依赖的函数运行时：版本路由、并发槽 + FIFO 排队、
过载/超时/取消归一化、CallID 幂等去重、实例池与空闲回收，并额外提供
**优雅排空停机、脱离执行治理、可配置实例保留与版本退场回收**。

```
go 1.25+（仅标准库，无外部依赖）
```

## 快速验证

```bash
# 全量单测
go test -count=1 ./...

# 竞态检测 + 反复跑（并发用例全部基于屏障同步，不赌时序）
CGO_ENABLED=1 go test -race -count=10 ./...

# 只看本轮新增能力的用例
go test -race -v -run 'TestDrain|TestDetached|TestMaxIdleInstances|TestVersionRetirement' ./...
```

## 错误类别（稳定可判定，与错误文本无关）

| Kind | 含义 |
| --- | --- |
| `ValidationRejected` | 注册/发布配置非法，整体拒绝，老版本与路由不受影响 |
| `FunctionNotFound` / `VersionUnknown` / `VersionDisabled` | 函数不存在 / 版本从未发布 / 版本已停用 |
| `Overloaded` | 并发槽与排队名额都满，快速失败 |
| `QueueTimeout` | 在队列中等待到截止时刻仍未获得派发 |
| `InvokeTimeout` | 已派发执行，但执行器未在超时内返回 |
| `Canceled` | 调用方在得到结论前主动取消 |
| `Draining` | **排空开始后新提交不接；排空开始时仍在队列、没开始执行的调用** |
| `DrainInterrupted` | **已在执行的调用把宽限期用满仍未结束，被排空强制结论** |
| `ExecutorFailed` / `ExecutorCrashed` | 执行器返回业务错误 / panic 被兜底 |

`Draining` 与过载、超时、停用明确区分；`DrainInterrupted` 与普通执行超时、
调用方取消明确区分，便于调用方做差异化处理。

## 1. 优雅排空/停机：`Runtime.Drain(gracePeriod)`

```go
summary := rt.Drain(5 * time.Second) // 阻塞到排空完成，最多等宽限期
```

语义保证：

- **停止接单**：排空状态先发布，此后任何新提交立即返回 `Draining`；
  排空完成后再调用也以同一原因稳定拒绝（运行时此后不再接单）。
- **队列直拒**：排空开始时仍在队列、尚未真正执行的调用，立即以
  `Draining` 结束，不占用宽限期。
- **在途跑完**：排空开始时已真正在执行的调用可以把宽限期用满，
  宽限期内自然结束的计入 `Completed`，结论就是执行器的真实结果。
- **到点强制中断**：宽限期到点仍未结束的调用，立即得到
  `DrainInterrupted`；其执行载体在同一时刻同步解绑、并发槽当场归还。
  **即使执行器完全不响应 `ctx.Done()`，排空也按时收尾，不会被拖住**；
  这些调用之后迟到的结果一律丢弃，不能翻案。
- **资源清零**：排空完成时并发占用归零、空闲实例清空、等待队列清空。
- **可重复、可并发发起**：排空完成后再次 `Drain` 立即返回同一份小结；
  排空进行中被并发调用时，所有调用共享同一次排空并等待同一结果，
  内部用原子状态发布 + 单次认领实现，安全、不卡死。

`DrainSummary` 字段：

| 字段 | 含义 |
| --- | --- |
| `Completed` | 在宽限期内自然跑完的在途调用数 |
| `Interrupted` | 到点被强制中断的在途调用数 |
| `QueuedRejected` | 排空开始时仍排队、被直接结束的调用数 |
| `NewRejected` | 排空开始后尝试新提交被拒的调用数（尽力统计） |
| `RemainingActive` | 收尾时仍占并发槽的执行数（恒为 0） |
| `RemainingDetached` | 收尾时已解绑、但后台执行器尚未返回的载体数 |
| `GracePeriod` | 本次宽限期 |

`Close()` 保持历史语义：只标记关闭并回收空闲资源，不强制中断在途调用。
需要“先排空再退出”的常驻服务用 `Drain`。

## 2. 超时/取消后的脱离执行治理

调用超时或被取消时，结论立即确定、并发槽立即释放，但执行器 goroutine
可能仍在后台运行。运行时对这份载体做**彻底解绑**：

- 载体被标记为 detached：不计入并发占用、**永不回到空闲池**，之后任何
  调用都不可能被安排到它；池里没有干净载体时直接新建；
- 它之后无论正常返回、报错还是 panic，结果一律丢弃（调用结论早已固定），
  载体由后台守护自动收尾销毁；
- 长期不返回的执行器只留下 detached 记账，不占用并发额度，也不阻塞排空。

账目可通过 `Stats` 观测，恒满足不变量：

```
Created - Destroyed == Active + Idle + Detached
```

`Stats`：`Active`（正在执行）、`Idle`（空闲实例数）、`Detached`
（已解绑未收尾）、`QueueLength`、`Created`、`Destroyed`、`Retired`、`Draining`。

## 3. 空闲实例保留策略与版本退场回收

`VersionSpec` 新增 `MaxIdleInstances *int`：

| 配置 | 含义 |
| --- | --- |
| 留空（`nil`，默认） | 保持历史行为：空闲未超时的实例最多保留 1 个暖实例 |
| 显式 `0` | 用完即回收，即使 `IdleTimeout` 很长也不保留 |
| 显式 `N >= 1` | 高并发回落后最多保留 N 个空闲实例，多出来的在归还时立即销毁；保留的实例各自仍受 `IdleTimeout` 约束 |
| 负数 | 非法配置，发布被整体拒绝（`ValidationRejected`），老版本与路由不受影响 |

版本退场（`Publish` 使旧版本不再是当前路由版本，或 `DisableVersion`
停用版本）时：

- 该版本的空闲实例**立即主动回收**，不等空闲超时、不等停机；
- 在途调用照旧按开始时的版本跑完，但跑完后载体不再保留；
- 已停用版本的新调用仍按原规则返回 `VersionDisabled`。

## 兼容范围

- 不引入任何外部依赖，仍只用标准库；所有语义在当前进程内生效。
- 既有配置（不填 `MaxIdleInstances`）、错误类别、路由/幂等/排队/超时
  语义保持不变；`Close()` 行为保持不变。
