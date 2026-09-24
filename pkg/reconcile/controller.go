package reconcile

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/pkg/queue"
	"github.com/xingjianluo/reconcile-engine-go/pkg/store"
)

// FailureRecord 是一次可解释的失败结论。
type FailureRecord struct {
	Name     string
	Kind     FailureKind
	Message  string
	Attempts int
	At       time.Time
}

// Config 为控制器配置。
type Config struct {
	// Workers 并发调谐的 worker 数；不同资源可并行，同一资源始终串行。
	Workers int
	// MaxAttempts 是瞬时/冲突错误的最大尝试次数；耗尽后给出 Exhausted 结论。
	// <=0 表示始终重试（关闭时仍会优雅收敛）。
	MaxAttempts int
	// Rate 每秒最多派发的任务数，<=0 不限流。
	Rate float64
	// Capacity 队列去重积压上限，<=0 不限。
	Capacity int
	// Backoff 退避计算器；nil 时使用默认指数退避。
	Backoff    *queue.ExponentialBackoff
	Store      *store.Store
	Reconciler Reconciler
	// OnFailure 在产生终态失败结论（Terminal / Exhausted）时回调。
	OnFailure func(FailureRecord)
}

// Controller 连接存储、队列与 Reconciler，驱动调谐循环。
type Controller struct {
	cfg     Config
	q       *queue.Queue
	backoff *queue.ExponentialBackoff

	mu       sync.Mutex
	inflight map[string]struct{}
	failures []FailureRecord
	stopped  bool

	wg sync.WaitGroup
}

// NewController 构造控制器，并把存储变更钩子接到队列（变更自动分发）。
func NewController(cfg Config) *Controller {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Backoff == nil {
		cfg.Backoff = queue.NewExponentialBackoff(5*time.Millisecond, time.Second, 2)
	}
	c := &Controller{
		cfg:      cfg,
		q:        queue.New(cfg.Rate, cfg.Capacity),
		backoff:  cfg.Backoff,
		inflight: map[string]struct{}{},
	}
	if cfg.Store != nil {
		cfg.Store.SetHook(func(name string, deleted bool) {
			// 存储变更（含彻底删除）触发一次入队合并；
			// 即使对象已删除，仍需一次调谐来确认终态/清理状态。
			_ = c.q.Add(name)
		})
	}
	return c
}

// Enqueue 手动分发一个资源的调谐任务（与存储钩子等价，重复调用安全）。
func (c *Controller) Enqueue(name string) error { return c.q.Add(name) }

// InFlight 返回当前正在调谐的资源名快照（关闭可观察）。
func (c *Controller) InFlight() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.inflight))
	for k := range c.inflight {
		out = append(out, k)
	}
	return out
}

// Failures 返回终态失败结论列表快照（Terminal / Exhausted）。
func (c *Controller) Failures() []FailureRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]FailureRecord, len(c.failures))
	copy(out, c.failures)
	return out
}

// Outstanding 返回尚未处理完的任务数（队列等待 + 在途调谐）。
// 供测试与关闭流程观察「是否已完全收敛」。
func (c *Controller) Outstanding() int {
	return c.q.Len() + c.q.InFlight()
}

// WaitForQuiet 在 timeout 内等待队列与在途调谐全部排空（且持续 quietFor 无新任务），
// 返回 true 表示系统进入稳定态；false 表示超时。
// 用于测试在断言副作用次数前等待所有中间世代的在途调谐完成。
func (c *Controller) WaitForQuiet(ctx context.Context, timeout, quietFor time.Duration) bool {
	deadline := time.Now().Add(timeout)
	lastBusy := time.Now()
	for {
		if c.Outstanding() == 0 {
			if time.Since(lastBusy) >= quietFor {
				return true
			}
		} else {
			lastBusy = time.Now()
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// Run 启动 worker 并阻塞，直到 ctx 取消；
// 取消后停止接收新任务，退避等待立即到期，在途与已排队任务继续处理，
// 全部收敛（或 worker 的 ctx 观察到取消后由业务自行中止）后返回。
//
// 优雅关闭约定：
//   - 传给 Reconciler 的 ctx 在关闭时取消，长任务可观察并尽快收敛；
//   - 队列 Shutdown 后延迟条目被提前提升，已入队任务不丢失，排空后退出；
//   - InFlight 可在关闭过程中被外部轮询以观察在途调谐。
func (c *Controller) Run(ctx context.Context) error {
	for i := 0; i < c.cfg.Workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	<-ctx.Done()
	c.q.Shutdown()
	c.wg.Wait()

	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()
	return ctx.Err()
}

func (c *Controller) worker(ctx context.Context, id int) {
	defer c.wg.Done()
	for {
		// 正常阶段：Get 与 Reconciler 都随 ctx 取消。
		key, shutdown, err := c.q.Get(ctx)
		recCtx := ctx
		if err != nil {
			// ctx 已取消：进入优雅排空。Run 随即 Shutdown，
			// 退避条目被提前提升；用不被取消的上下文把已入队任务处理完。
			key, shutdown, err = c.q.Get(context.Background())
			recCtx = context.Background()
		}
		if err != nil {
			return
		}
		if shutdown {
			return
		}

		c.mu.Lock()
		c.inflight[key] = struct{}{}
		c.mu.Unlock()

		requeueAfter, failureRetry := c.reconcileOne(recCtx, key)

		c.mu.Lock()
		delete(c.inflight, key)
		shutting := c.stopped
		c.mu.Unlock()

		// 优雅排空阶段：在途任务的「失败重试」仍保留（立即执行最后几轮，
		// 由 MaxAttempts 约束，耗尽即终止并产生 Exhausted 结论，保证排体能结束）；
		// 业务主动请求的等待型延迟（RequeueAfter）在关闭时不再等待。
		if shutting && !failureRetry {
			requeueAfter = 0
		}
		c.q.Done(key, requeueAfter)
	}
}

// reconcileOne 执行一次调谐，返回 (下一次延迟重入队时长, 是否为失败重试)。
// 延迟为 0 表示不延迟重入队（成功 / 终态失败 / 立即重试已自行 Add）。
func (c *Controller) reconcileOne(ctx context.Context, key string) (time.Duration, bool) {
	obj, err := c.cfg.Store.Get(key)
	if err != nil {
		// 对象彻底消失：删除已完成，视为调谐成功终态，清退避。
		// （可能是排队期间被无终结器删除；重复的删除通知也汇聚于此。）
		c.backoff.Reset(key)
		return 0, false
	}

	res, rerr := c.cfg.Reconciler.Reconcile(ctx, obj)
	if rerr == nil {
		// 业务请求的重入队不算失败；退避计数归零。
		c.backoff.Reset(key)
		if res.RequeueAfter > 0 {
			return res.RequeueAfter, false
		}
		if res.Requeue {
			// 立即重入队：直接 Add（队列对处理中/等待中的键做合并）。
			_ = c.q.Add(key)
		}
		return 0, false
	}

	kind, msg := classify(rerr)
	switch kind {
	case KindTerminal:
		c.backoff.Reset(key)
		c.recordFailure(key, KindTerminal, msg, c.backoff.Attempts(key))
		return 0, false
	case KindConflict:
		// 基于过期版本：立即重读重试（不退避），但仍计入尝试次数。
		attempt, _ := c.backoff.Fail(key)
		if c.exceeded(attempt) {
			c.recordFailure(key, KindExhausted, "after conflict retries: "+msg, attempt)
			c.backoff.Reset(key)
			return 0, false
		}
		_ = c.q.Add(key)
		return 0, true
	default: // KindTransient 及未分类错误
		attempt, delay := c.backoff.Fail(key)
		if c.exceeded(attempt) {
			c.recordFailure(key, KindExhausted, msg, attempt)
			c.backoff.Reset(key)
			return 0, false
		}
		return delay, true
	}
}

func (c *Controller) exceeded(attempt int) bool {
	return c.cfg.MaxAttempts > 0 && attempt >= c.cfg.MaxAttempts
}

func (c *Controller) recordFailure(name string, kind FailureKind, msg string, attempts int) {
	rec := FailureRecord{Name: name, Kind: kind, Message: msg, Attempts: attempts, At: time.Now()}
	c.mu.Lock()
	c.failures = append(c.failures, rec)
	cb := c.cfg.OnFailure
	c.mu.Unlock()
	if cb != nil {
		cb(rec)
	}
}

// classify 从错误链中取出分类错误；未分类的普通错误按瞬时错误处理。
func classify(err error) (FailureKind, string) {
	var fe *Error
	if errors.As(err, &fe) {
		return fe.Kind, fe.Error()
	}
	return KindTransient, err.Error()
}
