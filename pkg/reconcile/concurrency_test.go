package reconcile

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
)

// gatedReconciler 用「在途计数」验证串行性：同一资源在途计数永远 <=1，
// 不同资源允许同时在途。第一个对象的调谐会被 gate 阻塞，
// 直到测试确认 InFlight 可观察后放行，从而验证优雅关闭时的可观察性。
type gatedReconciler struct {
	mu          sync.Mutex
	inflight    map[string]int
	maxInflight map[string]int
	distinctNow int32
	maxDistinct int32
	gate        chan struct{}
	gateUsed    int32
}

func newGated() *gatedReconciler {
	return &gatedReconciler{
		inflight:    map[string]int{},
		maxInflight: map[string]int{},
		gate:        make(chan struct{}),
	}
}

func (g *gatedReconciler) Reconcile(ctx context.Context, obj *v1.Object) (Result, error) {
	g.mu.Lock()
	g.inflight[obj.Name]++
	if g.inflight[obj.Name] > g.maxInflight[obj.Name] {
		g.maxInflight[obj.Name] = g.inflight[obj.Name]
	}
	n := int32(len(g.inflight))
	for {
		// 原子更新最大不同资源并发数。
		old := atomic.LoadInt32(&g.maxDistinct)
		if n <= old || atomic.CompareAndSwapInt32(&g.maxDistinct, old, n) {
			break
		}
	}
	g.mu.Unlock()

	if obj.Name == "slow" && atomic.CompareAndSwapInt32(&g.gateUsed, 0, 1) {
		select {
		case <-g.gate: // 等待测试放行
		case <-ctx.Done():
		}
	}

	g.mu.Lock()
	g.inflight[obj.Name]--
	g.mu.Unlock()
	return Result{}, nil
}

func TestSameResourceSerialDifferentResourceParallel(t *testing.T) {
	st := newStoreForTest()
	g := newGated()
	c := NewController(Config{
		Workers: 6, MaxAttempts: 0,
		Backoff:    newTestBackoff(),
		Store:      st,
		Reconciler: g,
	})
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = c.Run(ctx); close(runDone) }()

	// 6 个资源；slow 会卡在 gate 里。
	for _, name := range []string{"slow", "r2", "r3", "r4", "r5", "r6"} {
		if _, err := st.Create(&v1.Object{Name: name, Spec: map[string]string{"k": "v"}}); err != nil {
			t.Fatal(err)
		}
	}

	// 对 slow 连续注入大量变更，验证处理中全部合并。
	for i := 0; i < 100; i++ {
		_ = c.Enqueue("slow")
	}

	// 等待 slow 进入在途并被 gate 阻塞，此时 InFlight 必须能观察到。
	waitFor(t, time.Second, func() bool {
		for _, n := range c.InFlight() {
			if n == "slow" {
				return true
			}
		}
		return false
	}, "slow 出现在 InFlight")
	t.Logf("输入: slow 调谐进行中（被 gate 阻塞）=> 判定: InFlight()=%v 可观察在途调谐", c.InFlight())

	// 给其他资源一点并行窗口，然后放行 slow。
	time.Sleep(20 * time.Millisecond)
	close(g.gate)

	// 等待其它 5 个资源全部完成。
	waitFor(t, 2*time.Second, func() bool {
		g.mu.Lock()
		done := 0
		for _, n := range []string{"r2", "r3", "r4", "r5", "r6"} {
			if g.maxInflight[n] >= 1 && g.inflight[n] == 0 {
				done++
			}
		}
		g.mu.Unlock()
		return done == 5
	}, "r2..r6 调谐完成")

	// 优雅关闭：Run 应在排空后返回。
	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 在取消后未优雅返回（在途任务未收敛）")
	}

	g.mu.Lock()
	t.Logf("判定: 各资源最大同时在途=%v（每个必须为 1，证明同资源串行）", g.maxInflight)
	t.Logf("判定: 不同资源最大并发数=%d（6 workers，期望 >1，证明不同资源并行）", g.maxDistinct)
	for name, max := range g.maxInflight {
		if max > 1 {
			g.mu.Unlock()
			t.Fatalf("资源 %s 出现 %d 个并发调谐", name, max)
		}
	}
	if g.maxDistinct <= 1 {
		g.mu.Unlock()
		t.Fatalf("不同资源未能并行调谐, maxDistinct=%d", g.maxDistinct)
	}
	g.mu.Unlock()
	t.Log("判定依据: 队列保证同键任一时刻至多一个在处理；多 worker 在不同键上并行；关闭时在途可观察且 Run 优雅返回")
}
