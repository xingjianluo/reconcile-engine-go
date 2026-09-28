# Reconcile Engine (Go)

一套**本地可复现**的声明式资源调谐引擎。用一个带版本号的**内存对象存储**
模拟"期望状态（Spec）"与"实际状态（Status）"的收敛过程，目标是让并发变更、
删除与失败重试下的最终结果**一致且可解释**。

- 无外部服务、无真实凭据：存储、队列、时钟全部在进程内，测试可重复运行。
- 变更→调谐任务的**分发与幂等执行**；同一资源的连续变更被**合并并按序**处理。
- 基于**过期版本**的写入被识别为冲突并重试；重试耗尽给出**可区分的失败结论**，
  绝不静默丢弃。
- 删除遵循**终结器（Finalizer）语义**：所有终结器移除后对象才真正消失。
- 失败任务**限流 + 单调退避**重入队；队列**有界不丢**；关闭时在途调谐
  **可观察并优雅收敛**。
- **资源依赖编排**：依赖未就绪时安静等待；依赖变化自动向下传播；删除按
  **依赖逆序**进行；**引用缺失 / 成环**给出可区分、可观察、不卡死的结论；
  高并发与依赖等待下队列与内存**有界**。

## 目录结构

```
internal/object      版本化资源对象、依赖声明、就绪标记、终结器辅助、深拷贝
internal/store       内存版本存储（CAS/乐观锁）、事件 Watch、读穿缓存（陈旧识别）
internal/dependency  依赖图：边维护、就绪阻塞判定、缺失/成环检测、删除传播
internal/queue       去重/按 key 串行/有界/退避工作队列 + 单调退避策略
                     （runnable 与 scheduled 两集合，依赖等待不占工作容量）
internal/reconcile   控制器：事件分发、依赖门控、守护终结器、冲突/退避、优雅关闭
cmd/demo             端到端演示：收敛/冲突/重试/终结器删除 + 依赖等待/传播/逆序删除
```

## 快速验证

要求 Go 1.25+（开发使用 Go 1.26）。

```bash
# 运行全部测试（带竞态检测），无需任何外部服务或凭据
go test -race -count=1 ./...

# 端到端演示，打印"输入"与"判定依据"
go run ./cmd/demo
```

测试输出中每一步都带有 `INPUT ... | JUDGE: ...`，明确给出输入条件与判定依据。
重复运行结果一致（`go test -count=N ./...`）。

## 调谐协议（Reconciliation Protocol）

控制器采用声明式控制循环，与具体资源解耦：

1. **期望状态**由用户写入对象的 `Spec`；调谐器把观察到的**实际状态**写入 `Status`。
2. 存储每次成功写入都会分配一个**全局单调递增的 `ResourceVersion`**。
3. Watch/初始 List 把变更**分发**到工作队列；队列按 `key` 去重合并。
4. Worker 处理某个 key 时**总是先读取最新对象**，再执行一次**幂等**的
   `Reconcile(ctx, obj)`：
   - 成功且已收敛 → `Done`（没有新变更则该 key 退役）；
   - 成功但希望稍后再跑（如等待外部条件）→ 立即重排队（`Result{Requeue:true}`）；
   - 返回错误 → 按退避策略重试；
   - 返回冲突错误 → 重新读取最新版本后重试（见下）。

### 幂等与按序合并

- **同一资源的连续变更被合并**：处理期间再次 `Add` 只置一个"脏"标记，
  `Done` 后至多再跑一次；等待中的重复 `Add` 折叠为一个条目。
  因此 N 次入队不会产生 N 次副作用。
- **每个 key 同时只有一个在途处理**（worker 数只影响不同 key 之间的并发），
  所以同一资源的调谐天然**按序**进行。
- 调谐器以"当前实际状态 vs 期望状态"决定是否动手；已收敛时直接返回，
  重复通知不产生重复副作用（见 `TestIdempotentSideEffects`）。

## 冲突语义（Optimistic Concurrency / Stale Version）

所有写操作都是 **Compare-And-Swap**：调用方必须带上读取时的 `ResourceVersion`。

- 若对象在读取后被替换，写入返回 `*store.ErrConflict`（含期望/实际版本），
  数据**不被**覆盖。
- 调谐器内基于陈旧版本的 CAS 冲突会被控制器识别，**重新读取并重试**，
  绝不会用陈旧数据执行破坏性操作。
- 队列按 `ResourceVersion` 合并事件：携带更旧版本的入队被丢弃，
  不会把待处理条目"降级"到旧版本。
- `store.Cache` 跟踪最近版本，可显式判断 `IsStale`；任何冲突都会使缓存失效，
  破坏性操作前用 `GetFresh` 取最新版本。

典型证据：`TestUpdateCASConflict`、`TestCoalescedBurstProcessesLatestVersion`、
存储层的 `TestConcurrentUpdatesAreSerialized`（50 个并发读改写，49 次冲突、
50 次成功，最终值确定为 50，无丢失更新）。

## 退避与失败语义（Backoff & Terminal Failure）

- 策略可配置（实现 `queue.Backoff`）。内置：
  - `ExponentialBackoff`：`min(Base * 2^(attempt-1), Max)`，**随时间单调不减**，
    在 `Max` 处饱和（避免溢出与惊群）；
  - `ConstantBackoff`：固定延时。
- 失败计数达到 `MaxAttempts` 后**不再重试**，而是产生并记录
  `*queue.ExhaustedError`（含 key、尝试次数、最后一次错误），控制器将其包装为
  带**可区分原因**的 `*reconcile.TerminalFailure`：
  - `ReasonRetryExhausted`：普通调谐错误重试耗尽；
  - `ReasonConflictLoop`：对象持续变化、始终冲突；
  - `ReasonReconcileError`：读取等不可重试错误。
  错误通过 `errors.Is/As` 与原始 cause 可判定、可追踪，**不是静默丢弃**。
- 新一代期望状态（更新版本）到达一个已终结失败的 key 时，旧结论被清除并重新调谐。
- **有界不丢**：队列容量饱和时 `Add` 阻塞等待空位，而不是丢弃任务。

## 删除顺序规则（Finalizer Deletion Ordering）

删除被拆成可观察的多个阶段，严格按序推进：

1. `Delete(key, rv)`：
   - 对象**无终结器** → 立即物理删除并广播 `EventDelete`；
   - 对象**有终结器** → 仅设置 `DeletionTimestamp`（幂等），对象**继续存在**，
     版本递增并触发调谐。
2. 对象处于"删除中"时，各终结器持有者执行幂等清理，完成后移除自己的终结器。
3. 只要还有任意终结器，`Remove` 返回 `ErrFinalizersRemain`，对象不消失。
4. 终结器全部移除后，下一次调谐用**最新版本**调用 `Remove`，对象才真正消失。
5. 任何基于过期版本的 `Remove` 都返回冲突，防止误删被替换后的对象。

证据：`TestFinalizerDeletionSequence`（存储层）与
`TestFinalizerDeletionEndToEnd`（控制器端到端）。

## 依赖编排（Dependency Orchestration）

资源可通过 `Object.Dependencies []DependencyRef` 声明它依赖哪些其他资源；
命名空间留空时在所属命名空间内解析。引擎在声明式控制循环之上维护一张
**依赖图**（`internal/dependency`），边方向为「依赖方 → 被依赖方」，并带一个
反向索引用于变化传播。

### 依赖与就绪如何判定

- 每个对象有一个由其调谐器写入的 **`Ready bool`**：调谐器把实际状态收敛到期望后
  将其置真。
- 默认就绪判定 `dependency.DefaultReady`：对象 **`Ready==true` 且未处于删除中**。
  被删除中的对象**永远不算就绪**，因此它不会再向下游解锁。可通过
  `Config.IsReady` 注入自定义判定。
- 一个存活对象只有在它声明的**每个依赖都存在且就绪**之后，才允许进入应用调谐器：
  - 依赖**尚未就绪**（存在但 `Ready=false` 或删除中）→ **安静等待**：以
    `RequeueWith(DependencyWaitDelay)` 延迟重试，**不消耗退避重试次数**，
    不是错误、不是永久失败，也不会绕过依赖先做动作。
  - 依赖就绪后，依赖方的那次变更会**自动**把等待中的对象重新入队，无需轮询外部系统。
- 门控顺序固定为：成环检测 → 缺失检测 → 未就绪等待 → 守护终结器 → 应用调谐。

### 依赖变化如何传播

- watch 事件在更新图节点的同时，利用反向索引把**每个依赖方**重新入队
  （非版本化触发，与该 key 已有的版本化条目安全合并）。
- 因此被依赖资源**被改动或被删除**时，所有依赖方自动重新调谐，最终结果与
  「依赖从头到尾没变过」一致。
- 同一轮对同一资源的多次触发仍然被队列**合并、按 key 串行、幂等**处理：处理中再
  触发只置脏标记，`Done` 后至多再跑一次；已收敛对象被重复触发时不产生重复副作用。
  证据：`TestDependencyChangePropagatesDownstream`、`TestCoalescedDependencyTriggers`。

### 删除顺序规则（逆序）

引擎用一个**引擎自管的守护终结器** `reconcile.GuardFinalizer` 强制逆序删除，
应用调谐器无需感知它：

1. 一个**存活且当前有依赖方**的对象，在其依赖已就绪后会被自动加上守护终结器
   （存活期单调：至多添加一次，不做增删抖动，因此不会产生重复副作用）。
2. 删除一个仍被依赖的对象时，只要它还有存活的依赖方，它就被**保留为删除中**、
   守护终结器不摘除，物理删除被阻塞——依赖方不清完，它不能真正消失。
3. 依赖方陆续删除、图中不再有依赖方后，引擎摘除守护终结器；随后应用终结器完成
   清理、对象才物理删除。链式依赖因此按 **叶子 → … → 根** 的逆序消失。
4. 极端情况下若对象在守护终结器加上之前就被请求删除（无终结器会立即消失），
   依赖方下一轮会观测到「依赖缺失」的明确结论（见下），同样不会状态错乱或卡死。

证据：`TestReverseOrderDeletion`、`TestChainReverseDeletion`。

### 依赖缺失与成环：可区分、可观察、不卡死

这两种情况都不是「安静无限重试」，而是记录为带**可区分原因**的终结结论
（`Controller.Failures()`，包装可 `errors.As` 的具体错误类型）：

| 情况 | 原因常量 | 包装错误 | 行为 |
| --- | --- | --- | --- |
| 引用了不存在的依赖 | `ReasonDependencyMissing` | `*dependency.MissingError`（含缺失 key 列表） | 记录后不做任何副作用、不忙等 |
| 依赖绕成环 | `ReasonDependencyCycle` | `*dependency.CycleError`（含环路路径） | 环上成员都终结，环外成员不受影响 |

二者都是针对**当前这一代声明**的结论，且能自动恢复：

- 缺失的依赖被**创建**出来 → 创建事件沿反向索引把依赖方重新入队，旧结论被清除，
  依赖方继续收敛（`TestMissingDependencyTerminalThenResolved`）。
- 环被**打断一条边** → 相关对象重新入队，环路消失后正常按序收敛
  （`TestDependencyCycleTerminalThenBroken`）。
- 自成环（A 依赖 A）也被识别为环。

### 有界性（高并发 + 依赖等待）

队列把待处理项分为两个集合：

- **runnable（立即可执行）+ in-flight**：严格受 `Capacity` 约束，饱和时 `Add`
  **阻塞等待空位而不丢弃**。
- **scheduled（延迟等待）**：退避重试与「依赖未就绪」的安静等待都停在这里，
  **不占用 runnable 工作槽**。

这样即使队列容量很小、大量对象在等依赖，**真正就绪可执行的对象（例如被依赖的根）
也永远能拿到槽位推进**，从结构上消除了「满队列全是等待者 → 谁都无法解锁」的
循环饥饿；总内存仍按 key 数有界。watch 订阅者若落后被存储断开，控制器会
**自动重建 watch 并全量 List 做一次 resync**，恢复期间不丢结论。
证据：`TestBoundedUnderDependencyLoad`（cap=8、61 个对象、40 路扇出）。

## 关闭语义（Graceful Shutdown）

取消运行上下文后：停止事件摄入 → 队列标记关闭并**排空在途/就绪任务** →
worker 退出。期间进行中的调谐数量可通过 `Controller.InFlight()` 观察；
排空受 `ShutdownGrace` 限制，超时返回 `*ShutdownIncompleteError`
（仍可观察剩余 Waiting/InFlight 数量）。见 `TestGracefulShutdownObservesInFlight`。

## 测试场景对照

| 需求 | 测试 |
| --- | --- |
| 并发调谐收敛、恰好一次副作用 | `TestConcurrentReconcileConverges` |
| 连续更新合并 + 最新版本 + 冲突重读 | `TestCoalescedBurstProcessesLatestVersion` |
| 重复入队不产生重复副作用 | `TestIdempotentSideEffects`、`TestCoalescing` |
| 乐观锁冲突 / 并发读改写 | `TestUpdateCASConflict`、`TestConcurrentUpdatesAreSerialized` |
| 终结器分阶段删除 | `TestFinalizerDeletionSequence`、`TestFinalizerDeletionEndToEnd` |
| 单调退避 + 耗尽可区分失败 | `TestMonotonicBackoff`、`TestRetryExhaustionIsDistinguishable`、`TestBackoffRetryThenTerminal` |
| 有界不丢、在途可见、优雅关闭 | `TestBoundedCapacityDoesNotDrop`、`TestInFlightVisibility`、`TestGracefulShutdownObservesInFlight` |
| 依赖未就绪安静等待、就绪后自动继续 | `TestDependencyWaitThenProceed` |
| 调谐按依赖顺序、守护终结器就位 | `TestReconcileOrderFollowsDependencies` |
| 依赖变化向下传播、结果一致、幂等 | `TestDependencyChangePropagatesDownstream`、`TestCoalescedDependencyTriggers` |
| 删除按依赖逆序（单级 / 链式） | `TestReverseOrderDeletion`、`TestChainReverseDeletion` |
| 依赖缺失：可区分失败 + 创建后自动恢复 | `TestMissingDependencyTerminalThenResolved` |
| 依赖成环：可区分失败 + 拆边后恢复 | `TestDependencyCycleTerminalThenBroken` |
| 高并发 + 依赖等待下有界不堆积、不饥饿 | `TestBoundedUnderDependencyLoad` |
| 依赖等待不占工作容量、到期提升 | `TestScheduledWaitsDontConsumeCapacity`、`TestScheduledItemPromotesWhenDue`、`TestRunnableCapacityHardBound` |
| 图语义（归一化/阻塞/成环/传播） | `internal/dependency` 包内 `graph_test.go` |

连续多轮竞态稳定验证（无需外部服务/凭据）：

```bash
go test -race -count=10 ./...
```

## 设计取舍

- 单一互斥锁 + 全局递增版本：以最小实现获得全序写与简单可靠的乐观锁，
  足够模拟 API Server 语义；生产可替换为分片/持久化实现（仍满足 `Store` 接口）。
- Watch 订阅带小缓冲；慢消费者会被断开，控制器随后**自动重建 watch 并全量 resync**，
  以保证内存有界、写入不被阻塞，同时不丢结论。
- 队列的退避时钟可注入（`Config.Clock`），使退避测试完全确定、无需真实 sleep。
- 依赖就绪用显式的 `Ready` 标记而非推测：只有调谐器知道自己是否真正可服务下游，
  引擎只负责按图门控、传播与删除排序。
- 守护终结器在存活期单调（只加一次）、仅在删除路径摘除，避免「有/无依赖方」抖动
  引发的反复写入与传播风暴。
- 安静等待（依赖未就绪）与退避重试都放入 scheduled 集合、不计失败次数、不占工作槽，
  使「等待」既不被误判为失败，也不会饿死就绪工作。
