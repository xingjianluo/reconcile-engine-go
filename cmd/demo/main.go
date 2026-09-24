// Command demo 在本地（无外部服务、无凭据）端到端演示声明式调谐引擎：
//
//  1. 创建带两个终结器的资源，连续重复变更被合并、同版本副作用只发生一次；
//  2. 注入瞬时失败，观察指数退避重试后恢复；
//  3. 注入过期版本提交，观察 Conflict 被识别并重读重试；
//  4. 发起删除，观察终结器逐个移除、全部移除后对象物理消失；
//  5. 注入持续失败资源，观察重试耗尽后得到可区分的 Exhausted 失败结论。
//
// 运行: go run ./cmd/demo
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
	"github.com/xingjianluo/reconcile-engine-go/pkg/queue"
	"github.com/xingjianluo/reconcile-engine-go/pkg/reconcile"
	"github.com/xingjianluo/reconcile-engine-go/pkg/sideeffects"
	"github.com/xingjianluo/reconcile-engine-go/pkg/store"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := store.New()
	rec := sideeffects.New()
	var effects int32
	logf := func(format string, args ...any) {
		fmt.Printf("    %s\n", fmt.Sprintf(format, args...))
	}

	r := newDemoReconciler(st, rec, &effects, logf)
	c := reconcile.NewController(reconcile.Config{
		Workers:     3,
		MaxAttempts: 4,
		Rate:        0,
		Backoff:     queue.NewExponentialBackoff(20*time.Millisecond, 500*time.Millisecond, 2),
		Store:       st,
		Reconciler:  r,
		OnFailure: func(fr reconcile.FailureRecord) {
			fmt.Printf("[失败结论] 资源=%s 分类=%s 尝试=%d 原因=%q\n", fr.Name, fr.Kind, fr.Attempts, fr.Message)
		},
	})
	go func() { _ = c.Run(ctx) }()

	fmt.Println("== 1) 创建 web（含终结器 [backup,net]），10 次重复入队 + 2 次 Spec 变更 ==")
	web, err := st.Create(&v1.Object{
		Name: "web", Spec: map[string]string{"image": "v1"}, Finalizers: []string{"backup", "net"},
	})
	must(err)
	for i := 0; i < 10; i++ {
		_ = c.Enqueue("web")
	}
	for _, img := range []string{"v2", "v3"} {
		retryUpdate(st, "web", web.ResourceVersion, func(o *v1.Object) *v1.Object { o.Spec["image"] = img; return o })
	}
	waitReady(st, "web", 2*time.Second)
	got, _ := st.Get("web")
	fmt.Printf("    结果: image=%s status=%v 调谐=%d 次 副作用=%d 次\n\n",
		got.Spec["image"], got.Status, r.calls("web"), atomic.LoadInt32(&effects))

	fmt.Println("== 2) flaky：前 3 次调谐瞬时失败，按指数退避重试后恢复 ==")
	r.setFailUntil("flaky", 3)
	_, err = st.Create(&v1.Object{Name: "flaky", Spec: map[string]string{"x": "1"}})
	must(err)
	waitReady(st, "flaky", 3*time.Second)
	f, _ := st.Get("flaky")
	fmt.Printf("    结果: status=%v 调谐=%d 次（3 次退避重试 + 1 次成功）\n\n", f.Status, r.calls("flaky"))

	fmt.Println("== 3) cached：先读旧版本，再制造并发变更，随后删除触发基于过期 RV 的终结器清理 ==")
	cached, err := st.Create(&v1.Object{
		Name: "cached", Spec: map[string]string{"k": "1"}, Finalizers: []string{"cleanup"},
	})
	must(err)
	_, _, err = st.UpdateFunc("cached", cached.ResourceVersion, func(o *v1.Object) *v1.Object {
		o.Spec["k"] = "2"
		return o
	})
	must(err)
	r.setStale("cached", cached.ResourceVersion)
	_, _, err = st.Delete("cached")
	must(err)
	waitGone(st, "cached", 2*time.Second)
	fmt.Printf("    结果: cached 已物理删除；识别冲突=%d 次\n\n", r.conflicts("cached"))

	fmt.Println("== 4) web 删除：终结器按序移除，全部移除后才消失 ==")
	_, removed, err := st.Delete("web")
	must(err)
	fmt.Printf("    Delete 返回 removed=%v（有终结器，仅标记）\n", removed)
	waitGone(st, "web", 2*time.Second)
	fmt.Printf("    结果: web 已物理删除，删除阶段调谐=%d 次\n\n", r.calls("web"))

	fmt.Println("== 5) doomed：持续瞬时失败，MaxAttempts=4 耗尽 => 可区分的 Exhausted 结论 ==")
	r.setFailUntil("doomed", 1<<30)
	_, err = st.Create(&v1.Object{Name: "doomed", Spec: map[string]string{"x": "1"}})
	must(err)
	time.Sleep(800 * time.Millisecond)

	fmt.Println("\n== 优雅关闭：cancel 后排空在途任务并停止 ==")
	cancel()
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("    Failures=%d 条，InFlight=%v（关闭后应为空）\n", len(c.Failures()), c.InFlight())
	fmt.Println("demo 完成")
}

type demoReconciler struct {
	st      *store.Store
	rec     *sideeffects.Recorder
	effects *int32
	log     func(string, ...any)

	mu        sync.Mutex
	callsMap  map[string]int
	failUntil map[string]int
	staleRV   map[string]uint64
	conflictN map[string]int
}

func newDemoReconciler(st *store.Store, rec *sideeffects.Recorder, fx *int32, log func(string, ...any)) *demoReconciler {
	return &demoReconciler{
		st: st, rec: rec, effects: fx, log: log,
		callsMap: map[string]int{}, failUntil: map[string]int{},
		staleRV: map[string]uint64{}, conflictN: map[string]int{},
	}
}

func (d *demoReconciler) calls(name string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.callsMap[name]
}
func (d *demoReconciler) conflicts(name string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conflictN[name]
}
func (d *demoReconciler) setFailUntil(name string, n int) {
	d.mu.Lock()
	d.failUntil[name] = n
	d.mu.Unlock()
}
func (d *demoReconciler) setStale(name string, rv uint64) {
	d.mu.Lock()
	d.staleRV[name] = rv
	d.mu.Unlock()
}

func (d *demoReconciler) Reconcile(ctx context.Context, obj *v1.Object) (reconcile.Result, error) {
	d.mu.Lock()
	d.callsMap[obj.Name]++
	n := d.callsMap[obj.Name]
	d.mu.Unlock()

	if obj.Deleted() && len(obj.Finalizers) > 0 {
		rv := obj.ResourceVersion
		d.mu.Lock()
		if stale := d.staleRV[obj.Name]; stale != 0 && d.conflictN[obj.Name] == 0 {
			rv = stale
		}
		d.mu.Unlock()
		_, removed, err := d.st.UpdateFunc(obj.Name, rv, func(o *v1.Object) *v1.Object {
			d.log("删除推进: %s 移除终结器 %s（剩余 %d）", o.Name, o.Finalizers[0], len(o.Finalizers)-1)
			o.Finalizers = o.Finalizers[1:]
			return o
		})
		if errors.Is(err, store.ErrConflict) {
			d.mu.Lock()
			d.conflictN[obj.Name]++
			d.mu.Unlock()
			d.log("识别到过期版本: %s 用 RV=%d 提交被拒 => Conflict，立即重读", obj.Name, rv)
			return reconcile.Result{}, reconcile.NewError(reconcile.KindConflict, "stale finalizer removal", err)
		}
		if err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{Requeue: !removed}, nil
	}

	d.mu.Lock()
	failN := d.failUntil[obj.Name]
	d.mu.Unlock()
	if n <= failN {
		d.log("%s 第 %d 次调谐失败（Transient，下游不可用）", obj.Name, n)
		return reconcile.Result{}, reconcile.NewError(reconcile.KindTransient,
			fmt.Sprintf("%s attempt %d downstream unavailable", obj.Name, n), nil)
	}

	exec, err := d.rec.OnceExecute(obj.Name, obj.Generation, func() error {
		atomic.AddInt32(d.effects, 1)
		d.log("执行外部副作用: %s generation=%d（幂等，仅一次）", obj.Name, obj.Generation)
		return nil
	})
	if err != nil {
		return reconcile.Result{}, err
	}
	_, _, err = d.st.UpdateFunc(obj.Name, obj.ResourceVersion, func(o *v1.Object) *v1.Object {
		if o.Status == nil {
			o.Status = map[string]string{}
		}
		if exec {
			o.Status["effect"] = fmt.Sprintf("gen%d", obj.Generation)
		}
		o.Status["ready"] = "true"
		return o
	})
	if errors.Is(err, store.ErrConflict) {
		return reconcile.Result{}, reconcile.NewError(reconcile.KindConflict, "status write conflict", err)
	}
	return reconcile.Result{}, err
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func retryUpdate(st *store.Store, name string, _ uint64, fn func(*v1.Object) *v1.Object) {
	for {
		cur, err := st.Get(name)
		if err != nil {
			panic(err)
		}
		_, _, err = st.UpdateFunc(name, cur.ResourceVersion, fn)
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			panic(err)
		}
		return
	}
}

func waitReady(st *store.Store, name string, within time.Duration) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if o, err := st.Get(name); err == nil && o.Status["ready"] == "true" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("timeout waiting ready: " + name)
}

func waitGone(st *store.Store, name string, within time.Duration) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, err := st.Get(name); errors.Is(err, store.ErrNotFound) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	panic("timeout waiting deletion: " + name)
}
