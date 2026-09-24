package queue

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"
)

// ErrQueueStopped 表示队列已关闭，不再接受入队。
var ErrQueueStopped = errors.New("queue stopped")

// ErrQueueFull 表示队列已达容量上限（背压），调用方应稍后重试。
var ErrQueueFull = errors.New("queue full")

// Backoff 决定同一键失败后的重入队延迟，要求随失败次数单调不减。
type Backoff interface {
	// Next 返回第 attempt 次失败（attempt 从 1 开始）后的等待时长。
	Next(attempt int) time.Duration
	// Reset 在键被成功处理后清空其失败计数。
	Reset(key string)
}

// ---- 指数退避（带抖动可选、上限封顶，保证单调增长） ----

// ExponentialBackoff 是线程安全的每键退避计算器。
type ExponentialBackoff struct {
	mu       sync.Mutex
	base     time.Duration
	max      time.Duration
	factor   float64
	attempts map[string]int
}

// NewExponentialBackoff 创建指数退避：base * factor^(attempt-1)，封顶 max。
func NewExponentialBackoff(base, max time.Duration, factor float64) *ExponentialBackoff {
	if base <= 0 {
		base = 10 * time.Millisecond
	}
	if max < base {
		max = base
	}
	if factor <= 1 {
		factor = 2
	}
	return &ExponentialBackoff{base: base, max: max, factor: factor, attempts: map[string]int{}}
}

// Fail 记录一次失败并返回该次数对应的退避时长（单调不减）。
func (b *ExponentialBackoff) Fail(key string) (attempt int, delay time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.attempts[key]++
	n := b.attempts[key]
	d := b.base
	for i := 1; i < n; i++ {
		d = time.Duration(float64(d) * b.factor)
		if d >= b.max {
			d = b.max
			break
		}
	}
	if d > b.max {
		d = b.max
	}
	return n, d
}

// Attempts 返回键当前的失败计数。
func (b *ExponentialBackoff) Attempts(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.attempts[key]
}

// Next 实现 Backoff 接口（不修改内部计数，纯计算）。
func (b *ExponentialBackoff) Next(attempt int) time.Duration {
	b.mu.Lock()
	base, max, factor := b.base, b.max, b.factor
	b.mu.Unlock()
	d := base
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * factor)
		if d >= max {
			return max
		}
	}
	return d
}

// Reset 实现 Backoff 接口。
func (b *ExponentialBackoff) Reset(key string) {
	b.mu.Lock()
	delete(b.attempts, key)
	b.mu.Unlock()
}

// ---- 等待中的延迟条目（小顶堆，readyAt 最早者优先） ----

type delayedItem struct {
	key     string
	readyAt time.Time
	seq     int64
}

type delayHeap []delayedItem

func (h delayHeap) Len() int { return len(h) }
func (h delayHeap) Less(i, j int) bool {
	if h[i].readyAt.Equal(h[j].readyAt) {
		return h[i].seq < h[j].seq
	}
	return h[i].readyAt.Before(h[j].readyAt)
}
func (h delayHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x any)   { *h = append(*h, x.(delayedItem)) }
func (h *delayHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// ---- 工作队列 ----

// Queue 是按键去重、同键保序、失败退避、限流且有界的工作队列。
//
// 每个键在任一时刻最多存在于一个位置：ready 列表 / delay 堆 / 被 Get 取走。
// 处理期间再次 Add 只置 dirty 标记，Done 后合并为一次重新入队，
// 因此连续变更天然合并；重复入队绝不产生重复条目。
type Queue struct {
	mu         sync.Mutex
	cond       *sync.Cond
	ready      []string // FIFO 等待列表
	readySet   map[string]struct{}
	delayed    *delayHeap
	delayedSet map[string]struct{}
	dirty      map[string]struct{} // 处理期间到达的合并标记
	processing map[string]struct{} // 已被 Get 取走、尚未 Done
	stopped    bool
	seq        int64
	capacity   int

	// 限流：相邻两次取出之间的最小间隔
	lastPop time.Time
	minGap  time.Duration
}

// New 创建队列。
//   - rate: 每秒最多取出的条目数，<=0 表示不限流；
//   - capacity: 去重后积压上限，<=0 表示不限（生产中建议设置）。
func New(rate float64, capacity int) *Queue {
	q := &Queue{
		readySet:   map[string]struct{}{},
		delayed:    &delayHeap{},
		delayedSet: map[string]struct{}{},
		dirty:      map[string]struct{}{},
		processing: map[string]struct{}{},
		capacity:   capacity,
	}
	if rate > 0 {
		q.minGap = time.Duration(float64(time.Second) / rate)
	}
	heap.Init(q.delayed)
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) tracked(key string) bool {
	if _, ok := q.readySet[key]; ok {
		return true
	}
	if _, ok := q.delayedSet[key]; ok {
		return true
	}
	if _, ok := q.processing[key]; ok {
		return true
	}
	return false
}

// Add 将键立即入队（变更合并）。
// 若键处于延迟等待中，则提前唤醒（更新的变更优先处理）。
func (q *Queue) Add(key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return ErrQueueStopped
	}
	if _, ok := q.processing[key]; ok {
		// 正在处理：仅打合并标记，不产生第二个条目。
		q.dirty[key] = struct{}{}
		return nil
	}
	if _, ok := q.readySet[key]; ok {
		return nil // 已在就绪列表，去重
	}
	if _, ok := q.delayedSet[key]; ok {
		// 新变更到达：把退避等待提前，立即就绪。
		q.removeDelayedLocked(key)
	}
	if q.capacity > 0 && len(q.readySet)+len(q.delayedSet)+len(q.processing) >= q.capacity {
		return ErrQueueFull
	}
	q.ready = append(q.ready, key)
	q.readySet[key] = struct{}{}
	q.cond.Broadcast()
	return nil
}

// AddAfter 请求延迟后入队（退避重入队）。若期间有新 Add，延迟会被提前取消。
func (q *Queue) AddAfter(key string, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return ErrQueueStopped
	}
	if _, ok := q.readySet[key]; ok {
		return nil
	}
	if _, ok := q.delayedSet[key]; ok {
		return nil
	}
	if q.capacity > 0 && len(q.readySet)+len(q.delayedSet)+len(q.processing) >= q.capacity {
		return ErrQueueFull
	}
	q.seq++
	heap.Push(q.delayed, delayedItem{key: key, readyAt: time.Now().Add(delay), seq: q.seq})
	q.delayedSet[key] = struct{}{}
	q.cond.Broadcast()
	return nil
}

func (q *Queue) removeDelayedLocked(key string) {
	for i, it := range *q.delayed {
		if it.key == key {
			heap.Remove(q.delayed, i)
			delete(q.delayedSet, key)
			return
		}
	}
}

// Get 阻塞取下一个就绪键。
// 返回 shutdown=true 表示队列已停止且全部条目排空，调用方应退出。
// 取走的键进入 processing，直到 Done 才允许再次被调度。
func (q *Queue) Get(ctx context.Context) (key string, shutdown bool, err error) {
	timer := newReusableTimer()
	defer timer.Stop()

	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		q.promoteDelayedLocked(time.Now())
		if len(q.ready) > 0 {
			if !q.rateLimitedLocked(time.Now()) {
				k := q.ready[0]
				q.ready = q.ready[1:]
				delete(q.readySet, k)
				q.processing[k] = struct{}{}
				q.lastPop = time.Now()
				return k, false, nil
			}
		}
		if q.stopped && len(q.ready) == 0 && len(q.delayedSet) == 0 && len(q.processing) == 0 {
			return "", true, nil
		}
		// 计算需要等待的最短时间：限流间隔 或 堆顶到期。
		wait := q.waitDurationLocked(time.Now())
		q.mu.Unlock()
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			q.mu.Lock()
			return "", false, ctx.Err()
		case <-timer.t.C:
		}
		q.mu.Lock()
	}
}

// promoteDelayedLocked 把到期的延迟条目提升到 ready 列表。
func (q *Queue) promoteDelayedLocked(now time.Time) {
	for q.delayed.Len() > 0 {
		top := (*q.delayed)[0]
		if top.readyAt.After(now) {
			return
		}
		heap.Pop(q.delayed)
		delete(q.delayedSet, top.key)
		if _, busy := q.processing[top.key]; busy {
			q.dirty[top.key] = struct{}{}
			continue
		}
		if _, ok := q.readySet[top.key]; ok {
			continue
		}
		q.ready = append(q.ready, top.key)
		q.readySet[top.key] = struct{}{}
	}
}

func (q *Queue) rateLimitedLocked(now time.Time) bool {
	if q.minGap <= 0 {
		return false
	}
	return now.Sub(q.lastPop) < q.minGap
}

func (q *Queue) waitDurationLocked(now time.Time) time.Duration {
	// 上限 50ms：即使错过了 cond.Broadcast，也会周期性重检，
	// 保证 Shutdown/新入队最坏 50ms 内被观察到；
	// 真正的延迟到期仍由精确的堆顶等待时间决定。
	var d time.Duration = 50 * time.Millisecond
	if q.minGap > 0 && len(q.ready) > 0 {
		if w := q.minGap - now.Sub(q.lastPop); w > 0 && w < d {
			d = w
		}
	}
	if q.delayed.Len() > 0 {
		top := (*q.delayed)[0]
		w := time.Until(top.readyAt)
		if w < 0 {
			w = 0
		}
		if w < d {
			d = w
		}
	}
	return d
}

// Done 标记键处理结束。
//   - 处理期间合并到达的变更（dirty）会立刻再入队，保证不丢任务；
//   - requeueWithDelay>0 时按退避延迟重入队；
//   - 队列已 Shutdown 时：新变更与失败重试都立即保留到就绪列表（不再等待
//     退避），由 worker 排空，保证关闭时刻在途任务的最后一轮不被吞掉。
func (q *Queue) Done(key string, requeueWithDelay time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, key)
	_, dirty := q.dirty[key]
	delete(q.dirty, key)

	requeue := dirty || (q.stopped && requeueWithDelay > 0)
	if requeue {
		if _, ok := q.readySet[key]; !ok {
			q.ready = append(q.ready, key)
			q.readySet[key] = struct{}{}
		}
		q.cond.Broadcast()
		return
	}
	if q.stopped {
		q.cond.Broadcast()
		return
	}
	if requeueWithDelay > 0 {
		q.seq++
		heap.Push(q.delayed, delayedItem{key: key, readyAt: time.Now().Add(requeueWithDelay), seq: q.seq})
		q.delayedSet[key] = struct{}{}
	}
	q.cond.Broadcast()
}

// Len 返回等待中（就绪+延迟）的去重键数量。
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.readySet) + len(q.delayedSet)
}

// InFlight 返回已被取走但未 Done 的键数量。
func (q *Queue) InFlight() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.processing)
}

// Shutdown 停止接收新键；Get 会在剩余条目处理完毕后返回 shutdown。
// 延迟条目会被立即提升（关闭时不再等待退避），尽快优雅收敛。
func (q *Queue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	q.stopped = true
	for q.delayed.Len() > 0 {
		it := heap.Pop(q.delayed).(delayedItem)
		delete(q.delayedSet, it.key)
		if _, busy := q.processing[it.key]; busy {
			q.dirty[it.key] = struct{}{}
			continue
		}
		if _, ok := q.readySet[it.key]; !ok {
			q.ready = append(q.ready, it.key)
			q.readySet[it.key] = struct{}{}
		}
	}
	q.cond.Broadcast()
}

// ---- 可复用定时器（避免每次循环新建） ----

type reusableTimer struct {
	t *time.Timer
}

func newReusableTimer() *reusableTimer {
	t := time.NewTimer(time.Hour)
	t.Stop()
	return &reusableTimer{t: t}
}

func (r *reusableTimer) Reset(d time.Duration) {
	if !r.t.Stop() {
		select {
		case <-r.t.C:
		default:
		}
	}
	r.t.Reset(d)
}

func (r *reusableTimer) Stop() { r.t.Stop() }
