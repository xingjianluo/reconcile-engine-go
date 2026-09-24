# 声明式资源调谐引擎（本地可复现）

一套**零外部依赖、零真实凭据**的声明式资源调谐（reconciliation）引擎的参考实现，
用进程内的本地对象存储模拟「期望状态 → 实际状态」的收敛过程，重点覆盖：

- 资源变更 → 调谐任务的**分发、合并、幂等执行**；
- 带版本信息的对象读取与**乐观并发**，过期操作可识别、可重试；
- **终结器（finalizer）语义**的分步删除；
- 失败任务的**限流与指数退避重入队**，重试耗尽给出**可区分的失败结论**；
- 进程关闭时在途调谐**可观察并优雅收敛**。

> 运行环境：Go 1.25+（开发环境为 Go 1.26），无需任何数据库、网络服务或凭据。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/apis/v1` | 资源对象类型：`ResourceVersion`、`Generation`、`Finalizers`、`DeletionTimestamp` |
| `pkg/store` | 带乐观并发控制的本地对象存储（模拟声明式 API Server） |
| `pkg/queue` | 按键去重、同键保序、限流、退避延迟、有界背压的工作队列 |
| `pkg/reconcile` | 控制器/调谐循环、失败分类、重试策略、优雅关闭 |
| `pkg/sideeffects` | 按「资源 + 世代」幂等的副作用记录器 |
| `test/integration` | 仅用公开 API 的黑盒端到端测试 |
| `cmd/demo` | 可运行的端到端演示程序 |

## 1. 调谐协议（Reconciliation Protocol）

资源对象上有两个不同含义的版本号：

- **`ResourceVersion`（RV）**：存储层对**任何一次成功写**单调发放的不透明版本号。
  用于乐观并发——条件更新必须携带读取时的 RV，RV 不匹配则整次写被拒绝（`ErrConflict`）。
- **`Generation`**：**仅在 `Spec`（期望状态）变化时**递增；状态（`Status`）回写不改变它。
  用于标识「期望态的世代」，是外部副作用幂等的自然键。

控制器的调谐循环是**层级触发（level-triggered）**而非边沿触发：

1. 存储的任何成功提交（含删除标记与物理删除）通过钩子把资源名 `Add` 进队列；
2. worker `Get` 到资源名后，从存储**重新读取最新对象**，调用用户实现的
   `Reconciler.Reconcile(ctx, obj)`；
3. 调谐器返回 `Result{Requeue, RequeueAfter}` 或一个**带分类**的错误；
4. 无论成功失败都必须调用 `Done`，控制器据此决定重置退避还是延迟重入队。

调谐器实现必须是**幂等**的：同一世代被反复调用不得重复产生外部副作用。
参考做法是用 `sideeffects.Recorder.OnceExecute(name, generation, fn)` 包裹外部动作。
记录器在锁内**原子预占**键位、锁外执行 `fn`：即使同一键的两次调谐在边界重叠，
也只有一次真正执行，重叠的另一次立即得到 `executed=false`；`fn` 失败则释放预占，
允许同版本下次重试：

```go
rec.OnceExecute(obj.Name, obj.Generation, func() error {
    // 创建/更新外部负载——同一 (资源, 世代) 只真正执行一次
    return nil
})
```

> 关键约定：**外部副作用按 `Generation` 去重，而不是按 `ResourceVersion`**。
> 状态回写会增长 RV 但不改变期望世代；若用 RV 做幂等键，会把状态回写误判为新期望，
> 造成重复副作用甚至自我诱导的调谐空转。引擎自身也避免空转：
> **内容无变化的条件更新是空操作**——不发放新 RV、不触发分发钩子。
>
> 最终一致而非逐版本执行：当多个世代快速连续到来时，较旧世代的调谐可能尚未执行
> 外部动作就被更新世代抢占（`OnceExecute` 对重叠调用返回 `executed=false`），
> 因此副作用可能只作用在部分（必含最终）世代上，但**同一世代绝不会执行两次**。

## 2. 分发、合并与同资源串行

`pkg/queue` 保证每个键（资源名）在任一时刻最多存在于一个位置：
就绪列表、延迟堆，或「已被取走、尚未 Done」。

- **去重**：键已在等待中时，重复 `Add` 直接丢弃，不产生第二个条目；
- **处理中合并**：键正在被调谐时到达的任意多次 `Add` 只记录一个 dirty 标记，
  `Done` 后合并为**一次**重新调谐——连续变更天然合并；
- **同键保序串行**：同一资源的变更严格按入队顺序、且永远不会被两个 worker 同时处理；
  不同资源在多 worker 上并行（见 `concurrency_test.go` 中「同资源最大在途恒为 1、
  不同资源最大并发 = worker 数」的判定）；
- **有界背压**：`New(rate, capacity)` 可设置去重后积压上限，满时 `Add` 返回
  `ErrQueueFull` 而非无限增长；
- **限流**：`rate` 为每秒最多取出条目数，相邻取出之间有最小间隔；
- **不丢任务**：入队成功的键在成功、重入队或终止性失败结论产生之前不会消失。

## 3. 冲突语义（基于过期版本的操作）

所有写都通过 `Store.UpdateFunc(name, rv, mutate)` 条件提交：

- `rv != 0` 且与当前 RV 不一致 → 返回 `store.ErrConflict`，**写不生效**，
  因此不可能基于陈旧快照执行破坏性操作；
- 调谐器把存储冲突包装为 `reconcile.KindConflict` 返回；控制器对冲突
  **立即重读最新对象并重试**（不退避），因为重读后大概率即可成功；
- 对已物理删除对象的迟到更新返回 `ErrNotFound`，删除终态被确认、退避计数清零；
- 冲突与瞬时失败一样计入 `MaxAttempts`（极端持续冲突也会收敛为 `Exhausted`）。

## 4. 失败分类与退避语义

`reconcile.Error` 携带四类**可区分**的失败结论（`FailureKind`）：

| 分类 | 含义 | 控制器动作 |
| --- | --- | --- |
| `Transient` | 瞬时错误（下游暂不可用）；未分类的普通 `error` 也归入此类 | 按指数退避延迟重入队 |
| `Conflict` | 基于过期 RV 的写被拒 | 立即重读重试（计入尝试次数） |
| `Terminal` | 不可恢复（如 Spec 非法），重试无意义 | 不重试，记录 `Terminal` 结论 |
| `Exhausted` | 瞬时/冲突失败在 `MaxAttempts` 后仍未成功 | 停止重入队，记录 `Exhausted` 结论 |

退避由 `queue.ExponentialBackoff(base, max, factor)` 按**每键**独立计算：

```
delay(attempt) = min(base * factor^(attempt-1), max)
```

- 延迟随失败次数**单调不减**（例如 10ms→20ms→40ms→80ms→100ms 封顶）；
- 成功（或 `Terminal` / `Exhausted` 终态）后 `Reset`，该键从 base 重新开始；
- 退避期间到达**新的变更**会提前取消等待（新变更优先，不再等待退避）；
- 重试耗尽**不会静默丢弃**：通过 `Config.OnFailure` 回调与 `Controller.Failures()`
  可查询到 `{Name, Kind, Attempts, Message, At}` 的明确结论，`Exhausted` 与
  `Terminal` 原因可区分。

## 5. 删除顺序规则（Finalizer 语义）

删除不是一次性事件，而是两阶段、逐步推进：

1. `Store.Delete(name)`：
   - 对象有一个或多个终结器 → 设置 `DeletionTimestamp`、发放新 RV，**对象仍然存在**；
     重复 `Delete` 幂等，不产生新版本；
   - 对象没有终结器 → 立即物理移除；
   - 对象不存在 → `ErrNotFound`。
2. 删除标记之后，各控制器在自己的调谐中执行外部清理，并**移除自己负责的终结器**
   （条件更新；过期 RV 同样得到 `ErrConflict`，须重读重试）。
3. **仅当最后一个终结器被移除时**，该次条件更新才把对象物理删除（`removed=true`）；
   此后 `Get` 返回 `ErrNotFound`。

由此得到三条硬保证：终结器未全部移除前对象不会消失；终结器数量单调不增
（每轮调谐至多推进受控的一步）；任何清理写都基于最新 RV，缓存过期不会误删。

## 6. 优雅关闭

`Controller.Run(ctx)` 在 `ctx` 取消后：

1. 队列 `Shutdown()`：拒绝新 `Add`（返回 `ErrQueueStopped`），
   并把所有**退避等待中的条目立即提升**为就绪（关闭不再等待退避）；
2. worker 切换到排空模式：已在途与已排队的任务继续处理，直到队列排空；
   在途任务若此刻失败，其重试也会保留并立即执行最后几轮——重试仍受
   `MaxAttempts` 约束，耗尽即产生 `Exhausted` 结论而终止，保证排空必然结束；
   业务通过 `RequeueAfter` 请求的「等待外部条件」型延迟在关闭时不再等待；
3. `Controller.InFlight()` 可在关闭过程中轮询，观察仍在调谐的资源，
   `Outstanding()`（队列等待 + 在途）可用于判断是否完全收敛；
4. 全部在途调谐收敛后 `Run` 才返回。

正常运行期间传给 `Reconciler` 的 `ctx` 会在关闭时取消，长任务可观察它并尽快收敛。
测试可使用 `WaitForQuiet(timeout, quietFor)` 等待系统进入稳定态后再做终态断言。

## 7. 验证方法（本地、无外部服务、可重复）

```bash
# 全部单元 + 集成测试（竞态检测建议开启）
go test -race ./...

# 反复运行验证可重复性
go test -race -count=10 ./...

# 详细查看每个场景的「输入」与「判定依据」
go test -v ./pkg/store/ ./pkg/queue/ ./pkg/reconcile/ ./test/integration/

# 端到端演示（变更合并 / 退避重试 / 冲突重读 / 终结器删除 / 耗尽结论 / 优雅关闭）
go run ./cmd/demo
```

测试输出中每个用例都打印了**输入**与**判定依据**（中文日志），例如：

- `pkg/store`：版本冲突、Generation 递增、终结器 3→2→1→0 后才 `ErrNotFound`、
  并发读-改-写的成功/冲突计数；
- `pkg/queue`：50 次重复 `Add` 合并为 1、退避序列单调、限流间隔、容量背压、
  关闭排空不丢任务；
- `pkg/reconcile`：7 个场景（合并幂等、冲突重试、退避恢复、Exhausted、Terminal、
  终结器顺序、优雅关闭）+ 同资源串行/异资源并行的并发安全判定；
- `test/integration`：仅通过公开 API 的 4 个端到端场景。

## 8. 最小使用示例

```go
st := store.New()
var created int32
c := reconcile.NewController(reconcile.Config{
    Workers: 2, MaxAttempts: 5,
    Store:      st,
    Reconciler: reconcile.ReconcilerFunc(func(ctx context.Context, obj *v1.Object) (reconcile.Result, error) {
        if obj.Deleted() { /* 清理外部资源并移除本控制器的 finalizer */ }
        // 按世代幂等地执行外部副作用，再条件回写 Status（冲突返回 KindConflict）
        return reconcile.Result{}, nil
    }),
    OnFailure: func(fr reconcile.FailureRecord) { log.Printf("失败结论: %+v", fr) },
})
go c.Run(ctx)

obj, _ := st.Create(&v1.Object{Name: "svc", Spec: map[string]string{"image": "v1"}})
// 存储变更经钩子自动分发；也可手动 c.Enqueue("svc")（重复调用安全）
```
