# 本地 Serverless 函数运行时（Go）

纯本地、进程内的 Serverless 函数运行时，不依赖容器、外部服务或远程资源：
版本注册与路由、并发槽与 FIFO 排队、调用超时/取消、幂等去重、实例池与空闲回收、
**优雅排空停机**、**脱离执行治理**、**可配置的空闲实例保留与版本退场回收**
全部发生在当前进程内，仅依赖 Go 标准库。

## 能力一览

| 主题 | 语义 |
| --- | --- |
| 版本与路由 | 发布的版本不可变；`CURRENT` 路由到最新版本；非法发布整体拒绝，旧版本与路由不受影响 |
| 并发与排队 | 每版本 `Concurrency` 个并发槽；占满后按 FIFO 排队，队列上限 `MaxQueueDepth`，超出立即 `Overloaded` |
| 超时/取消 | 排队超时 `QueueTimeout`、执行超时 `InvokeTimeout`、调用方取消 `Canceled`，结论互不相同 |
| 幂等去重 | 相同 `CallID` 并发/重复提交只真正执行一次，所有提交者拿到同一结论 |
| 崩溃隔离 | 执行器 panic 归一化为 `ExecutorCrashed`，崩溃实例销毁，不影响其他调用/函数 |
| 优雅排空 | `Drain(grace)`：停止接单、排队者即拒、在途用完宽限、到点强制中断、资源清零、返回小结 |
| 脱离执行治理 | 超时/取消后载体立即解绑，永不再复用；迟到结果/panic 只销毁载体，账目自洽 |
| 空闲保留 | `MaxIdleInstances` 可配置（默认 1、`0` 用完即回收、`N` 回落留 N）；版本退场主动回收 |

## 错误类别（`PlatformError.Kind`）

- `ValidationRejected`：注册/发布配置非法，整体拒绝，老版本与路由不受影响。
- `FunctionNotFound` / `VersionUnknown` / `VersionDisabled`。
- `Overloaded`：并发槽与队列名额都满，快速失败。
- `QueueTimeout`：排队等待期间超时（未获得派发）。
- `InvokeTimeout`：已派发执行，但执行器未在调用超时内返回。
- `Canceled`：调用方在得到结论前主动取消。
- `Draining`：运行时正在/已经排空，不再接单，或调用在队列中遇到排空。
  与过载、超时、版本停用都不同。
- `DrainInterrupted`：调用已在执行，排空宽限期用尽仍未结束，被强制中断。
  与普通执行超时、调用方取消都不同。
- `ExecutorFailed` / `ExecutorCrashed`：业务错误 / 执行器 panic。

## 优雅排空 / 停机

调用 `rt.Drain(gracePeriod)` 发起一次运行时级排空：

1. **停止接单**：排空一开始，所有新提交（注册/发布/调用）立即返回 `Draining`；
   排空结束后再调用也稳定地得到同一 `Draining` 结论。
2. **排队者即拒**：排空开始时仍在队列里、尚未开始执行的调用，立即以 `Draining`
   结束（计入小结 `QueuedRejected`），不再派发。
3. **在途用完宽限**：已经真正在执行的调用可继续使用整个宽限期；宽限期内正常
   跑完的计入 `Completed`。
4. **到点强制中断**：宽限期满仍未结束的调用被强制取消，调用结论为
   `DrainInterrupted`（计入 `Interrupted`），与普通执行超时、调用方取消可区分。
5. **赖账执行器不拖停机**：即使个别执行器无视 `ctx` 取消永不返回，`Drain` 也在
   宽限期满时按时返回；这些载体与调用解绑、不计并发额度，记入小结
   `DetachedLeft`，之后自行返回时被销毁，**迟到结果不能翻案、也不拖住排空**。
6. **资源清零**：排空结束后各版本池的并发占用为 0、等待队列为空、空闲实例为 0
   （空闲实例在排空开始时主动销毁，计入 `IdleDestroyed`）。
7. **可重复、可并发发起**：排空完成后可以再次 `Drain`；排空中被并发 `Drain`
   也安全——所有发起方共享同一次排空，等待同一个结束信号，拿到同一份小结，
   不会重复收尾或卡死。

`DrainSummary` 字段：`Functions`（参与排空的版本池数）、`Completed`、
`Interrupted`、`QueuedRejected`、`IdleDestroyed`、`ActiveLeft`（恒为 0）、
`DetachedLeft`（排空结束时仍未收尾的赖账载体数，不占并发额度）。

> `Close()` 保持原有轻量语义：停止空闲回收定时器、销毁空闲实例，不强停在途调用。
> 需要“停止接单 + 宽限 + 强制中断 + 小结”的常驻服务停机流程请使用 `Drain`，
> 停机完成后可再调用 `Close` 清理定时器。

## 脱离执行的治理（超时/取消后的载体隔离）

调用超时或被取消时，结论立即确定、并发槽立即释放，但执行器可能还在后台运行。
本运行时把这份“执行载体”与本次调用彻底解绑：

- 载体立即移出可复用集合，之后**任何调用都不会被安排到还在跑旧调用的载体上**；
  池里没有干净可用的载体时才新建。
- 该载体之后无论正常返回、报错还是 panic，都被自动收尾销毁，**不会回到空闲池**，
  也不会改变任何已有调用的结论（迟到结果一律丢弃）。
- 账目恒等且可观测：
  `Created == Destroyed + Active + IdleCount + Detached`。
  - `Active`：正在执行、占用并发额度；
  - `IdleCount`：干净可复用的空闲载体；
  - `Detached`：已解绑、执行器尚未收尾销毁的载体（不占并发额度）；
  - `Created/Destroyed`：累计创建/销毁计数。
  长期不返回的执行只会停留在 `Detached`，不会把并发额度或账目卡歪。

## 空闲实例保留与版本退场

`VersionSpec.MaxIdleInstances *int` 控制每版本最多保留多少空闲实例：

| 取值 | 含义 |
| --- | --- |
| 不填（`nil`） | 保持历史行为：高并发回落后最多保留 1 个暖实例 |
| `0` | 用完即回收，不保留任何空闲实例 |
| `N >= 1` | 高并发回落后最多留 N 个空闲实例，多出来的立即销毁 |

说明：
- `IdleTimeout == 0` 仍然表示“用完即回收”，此时无论 `MaxIdleInstances` 设多少
  都不保留空闲实例（空闲超时策略优先）。
- 非法值（`< 0`）沿用现有规则：**整体拒绝**，老版本与路由不受影响。
- **版本退场主动回收**：版本被停用（`DisableVersion`），或发布新版本使其不再是
  当前路由版本时，它的空闲实例被立即回收，不再傻等空闲超时或停机；在途调用仍按
  起始版本快照跑完，但跑完的那份载体不再保留。

## 兼容范围

- 原有版本路由、并发排队、超时/取消分类、幂等去重、panic 隔离、空闲超时语义保持。
- `VersionSpec` 只新增一个可选指针字段 `MaxIdleInstances`；不填时行为与以前完全
  一致（保留 1 个暖实例）。
- `Close()` 语义不变；`Drain` 与新增错误类别、统计字段均为纯增量，不引入新依赖。

## 本地复现与验证

需要 Go 1.25+（仓库在 Go 1.26 上验证）。

```bash
# 全部用例
go test ./...

# 带竞态检测反复跑（并发用例用屏障同步，不赌时序，可稳定通过）
CGO_ENABLED=1 go test -race -count=10 ./...

# 只看本次新增的三块能力
go test -v -run 'TestDrain|TestDetached|TestMaxIdleInstances|TestRetirement' ./...

# 静态检查与格式化
go vet ./...
gofmt -l .   # 无输出即格式干净
```

关键用例与覆盖点：

- `TestDrainGracefulShutdown`：停止接单、排队者即拒、在途用完宽限、赖账执行器被
  强制中断且不拖停机、重复/并发发起 `Drain`、资源清零、小结计数、迟到结果不翻案。
- `TestDrainOneCompletesOneInterrupted`：精确的“宽限期内跑完 1 个、到点中断 1 个”，
  `Completed=1 Interrupted=1`。
- `TestDetachedInstanceIsolationAndAccounts`：超时解绑后新调用全部落到干净载体，
  旧载体迟到销毁，`Created == Destroyed + Active + Idle + Detached` 始终成立。
- `TestDetachedLatePanicDoesNotOverride`：解绑后迟到的 panic 只销毁载体、不翻案。
- `TestMaxIdleInstancesConfig`：不填=1、`0`=用完即回收、`N`=回落留 N、
  `IdleTimeout=0` 优先、非法值整体拒绝。
- `TestRetirementReclaimsIdle`：版本被新版本顶替/停用时空闲实例立即回收，在途照旧
  跑完但跑完不保留；非法发布不影响老版本。

每个用例的日志都标注了覆盖业务、输入标识（函数/版本/CallID）与判定依据（错误类别
或账目等式），便于直接对照验证。
