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
- 资源间可声明**依赖关系（DependsOn）**：依赖未就绪时**安静等待**（不判失败、
  不绕过）；依赖变更/删除会**向下传播**自动重新调谐。
- 删除遵循**依赖顺序**：被依赖的资源在依赖方清理完成之前不会被真正删除。
- 依赖**缺失**与**成环**给出**可区分、可观察的终结结论**，不卡死、不无限重试、
  不悄悄消失；修复后由变更传播自动恢复。

## 目录结构

```
internal/object    版本化资源对象、终结器辅助、删除时间戳、深拷贝、DependsOn 声明
internal/store     内存版本存储（CAS/乐观锁）、事件 Watch、读穿缓存（陈旧识别）
internal/queue     去重/按 key 串行/有界/退避工作队列 + 单调退避策略
internal/reconcile 控制器：事件分发、依赖门控/传播/环检测、工作循环、冲突处理、
                   终结器与依赖顺序删除、Watch 断线重连+resync、优雅关闭
cmd/demo           端到端演示：收敛→更新合并→依赖编排（等待/传播/缺失/成环/
                   顺序删除）→失败重试→终结器删除
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

资源通过对象上的 `DependsOn []string`（元素为 `MakeKey(namespace,name)` 形式
的 key）声明依赖。控制器在**调用应用调谐器之前**做依赖门控，因此资源在依赖
未就绪时不可能先把动作做出去。

### 就绪（Ready）怎么判定

判定顺序（见 `evaluateDeps`）：

1. **成环检测优先**：从该对象沿 `DependsOn` 边做 DFS（颜色标记），若能到达
   任意环（含自依赖 `A -> A`），立即终结失败 `ReasonDependencyCycle`，错误为
   `*DependencyCycleError`（携带环路径，如 `a -> b -> a`）。成环检测先于就绪
   判断，否则自依赖会被当成"还没就绪"而永远等下去。
2. **依赖存在性**：逐个 `Get` 依赖 key；不存在 → 终结失败
   `ReasonDependencyMissing`（`*DependencyMissingError`，携带缺失的 key）。
3. **就绪性**：依赖存在且**未处于删除中**，并且满足 `Config.Ready`：
   - 默认规则 `DefaultReady`：`Status` 与 `Spec` 完全一致（即已收敛）；
   - 应用可通过 `Config.Ready func(*object.Object) bool` 自定义就绪条件
     （例如只要求关键字段被观察到）。
4. 全部就绪后才调用一次 `Reconcile`。

依赖存在但还没就绪时，控制器用 `RequeueWith(Config.DepWait)` **安静等待**：
不计失败尝试次数、不产生失败结论、队列中仍只占一个 key 的槽位（有界）。

### 依赖变化如何传播

- 每个 store 事件（Upsert/Delete）除了入队自身，还会触发一次
  `enqueueDependents`：扫描当前 store 中所有 `DependsOn` 包含该 key 的对象并
  入队。依赖集合**直接从 store 现场扫描**而不是维护增量索引，因此传播集合与
  实时状态永远一致，不受事件乱序影响。
- 传播是**逐层自动发生**的：依赖收敛后自身状态变化产生事件 → 唤醒直接依赖方
  → 依赖方收敛后再唤醒下一层，最终叶子也会拿到最新输入。
- 同一轮对同一资源的多次触发仍然在队列里**合并、按序、幂等**处理；处理期间
  到达的新事件只置脏标记，不产生重复副作用，也不会丢掉最新版本。
- 终结失败的 key 不是"死信"：缺失依赖后来被创建、或环被编辑打破，相关变更
  事件会重新入队该 key（队列会复活失败 key），成功收敛后旧结论自动清除。

### 删除的顺序规则

除既有终结器阶段外，控制器对**删除中**的对象额外做反向依赖门控：

- 对象带 `DeletionTimestamp` 时，先扫描是否还有现存对象依赖它
  （`liveDependents`，同样现场扫 store）；
- **只要还有任何依赖方存在（即使依赖方自己也在删除中），就不调用应用的清理
  逻辑**，只安静地按 `DepWait` 重新检查；
- 依赖方全部物理消失后，被依赖对象才进入终结器清理并真正删除。
- 因此多个对象按逆拓扑顺序删除时，物理删除事件的顺序保证是"依赖方先、被依赖
  方后"（`TestOrderedDeletionWaitsForDependents` 用事件顺序断言这一点）。
- 若一个无终结器的对象被直接物理删除而仍有依赖方引用它，依赖方会在传播触发
  后得到 `DependencyMissing` 的终结结论，而不是卡死。

### 异常结论对照

| 情况 | 行为 | 原因 / 错误类型 | 恢复方式 |
| --- | --- | --- | --- |
| 依赖存在但未就绪 | 安静等待，周期重检 | 无（不是失败，不计数） | 依赖就绪事件传播唤醒 |
| 依赖处于删除中 | 安静等待 | 无 | 依赖被删（→缺失结论）或恢复 |
| 依赖 key 不存在 | 立即终结，不重试 | `DependencyMissing` / `*DependencyMissingError` | 依赖被创建后事件传播复活 |
| 依赖图含环（含自环） | 立即终结，不重试 | `DependencyCycle` / `*DependencyCycleError`（带路径） | 编辑打破环后事件传播复活 |
| 删除时仍被依赖 | 阻塞删除、安静等待 | 无 | 依赖方先清理完毕 |

### 有界性与可靠性

- 等待中的对象只占用队列中**每 key 一个**条目；突发传播通过合并折叠，内存
  仍受队列 `Capacity` 约束，饱和时阻塞反压而不是丢弃。
- store 的 Watch 在订阅方跟不上时会断开（store 自身内存有界）；控制器事件泵
  会**重新订阅并全量 relist**（先订阅后 list，中间不丢事件，重复触发会被队列
  合并），因此高并发突发下不会因为一次 Watch 断线永久漏事件。

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
| 依赖未就绪时安静等待、就绪后自动继续 | `TestDependencyWaitsThenProceeds` |
| 依赖变更向下传播（含多级传递）、幂等合并且无陈旧动作 | `TestDependencyChangePropagates`、`TestPropagationIsTransitive` |
| 按依赖顺序删除（依赖方先删、被依赖方后删） | `TestOrderedDeletionWaitsForDependents` |
| 删除等待期间高并发突发仍有界、最终清理完成 | `TestDeletionQueueStaysBoundedWhileWaiting` |
| 依赖缺失：可区分终结失败、不无限重试、出现后自动恢复 | `TestMissingDependencyIsTerminal` |
| 依赖成环（含自环）：可区分终结失败、修复后自动恢复 | `TestDependencyCycleIsTerminal`、`TestSelfDependencyIsCycle` |
| 高并发 + 依赖等待下的整体收敛/回退/有界 | `TestDependencyStressUnderConcurrency` |
| 依赖判定单元测试（环/缺失/就绪/删除中/反向扫描） | `TestFindCycle`、`TestEvaluateDeps`、`TestLiveDependents`、`TestDefaultReady` |

### 本地验证依赖编排

```bash
# 全部用例，带竞态检测；连续多轮验证稳定性（不依赖外部服务/凭据/时序运气）
go test -race -count=50 ./...

# 只跑依赖编排相关用例（-v 查看每步 INPUT/JUDGE）
go test -race -count=10 -v -run 'Dependency|Propagation|OrderedDeletion' ./internal/reconcile/

# 端到端演示：等待 -> 缺失/成环结论 -> 就绪继续 -> 变更传播 -> 顺序删除
go run ./cmd/demo
```

## 设计取舍

- 单一互斥锁 + 全局递增版本：以最小实现获得全序写与简单可靠的乐观锁，
  足够模拟 API Server 语义；生产可替换为分片/持久化实现（仍满足 `Store` 接口）。
- Watch 订阅带小缓冲；慢消费者会被断开（需 resync），以保证内存有界、写入不被阻塞。
- 队列的退避时钟可注入（`Config.Clock`），使退避测试完全确定、无需真实 sleep。
- 依赖边的来源只有对象自身的 `DependsOn`：就绪/成环判定沿 store 实时读取，
  反向依赖（传播集合、删除门控）现场 `List` 扫描，不维护第二份可变索引，从根上
  避免"索引与真相不一致"导致的漏唤醒或误放行；本地内存存储下代价可忽略。
  高并发突发期间的漏事件由"Watch 断线重连 + 全量 relist"兜底。
