// Package integration 以黑盒方式（仅使用公开 API）验证引擎在真实使用方式下的
// 端到端行为。运行无需任何外部服务或凭据：
//
//	go test ./test/integration/ -v
package integration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
	"github.com/xingjianluo/reconcile-engine-go/pkg/queue"
	"github.com/xingjianluo/reconcile-engine-go/pkg/reconcile"
	"github.com/xingjianluo/reconcile-engine-go/pkg/sideeffects"
	"github.com/xingjianluo/reconcile-engine-go/pkg/store"
)

// app 组装存储、副作用记录器与一个「部署器」调谐器，模拟上层应用。
type app struct {
	st  *store.Store
	rec *sideeffects.Recorder
	dec *deployer
	c   *reconcile.Controller
}

type deployer struct {
	st      *store.Store
	rec     *sideeffects.Recorder
	created *int32 // 模拟「创建外部负载」这一有副作用操作
	mu      sync.Mutex
	failN   map[string]int
}

func (d *deployer) Reconcile(ctx context.Context, obj *v1.Object) (reconcile.Result, error) {
	// 删除路径：逐个移除本控制器注册的终结器 "deploy"。
	if obj.Deleted() {
		if contains(obj.Finalizers, "deploy") {
			_, removed, err := d.st.UpdateFunc(obj.Name, obj.ResourceVersion, func(o *v1.Object) *v1.Object {
				o.Finalizers = removeOne(o.Finalizers, "deploy")
				return o
			})
			if errors.Is(err, store.ErrConflict) {
				return reconcile.Result{}, reconcile.NewError(reconcile.KindConflict, "stale on delete", err)
			}
			return reconcile.Result{Requeue: !removed}, err
		}
		return reconcile.Result{}, nil
	}

	// 已收敛：Status 已反映当前世代则不做任何写（避免自我诱导的空转）。
	if obj.Status["phase"] == "Running" && obj.Status["effect"] == fmt.Sprintf("true@gen%d", obj.Generation) {
		return reconcile.Result{}, nil
	}

	d.mu.Lock()
	fn := d.failN[obj.Name]
	d.mu.Unlock()
	var attempts int
	fmt.Sscanf(obj.Status["attempts"], "%d", &attempts)
	attempts++
	if attempts <= fn {
		_, _, _ = d.st.UpdateFunc(obj.Name, obj.ResourceVersion, func(o *v1.Object) *v1.Object {
			o.Status = map[string]string{"attempts": fmt.Sprintf("%d", attempts), "phase": "Failing"}
			return o
		})
		return reconcile.Result{}, reconcile.NewError(reconcile.KindTransient,
			fmt.Sprintf("downstream unavailable (attempt %d)", attempts), nil)
	}

	// 外部负载创建：按 Generation 幂等；进入新世代时允许再执行一次。
	exec, err := d.rec.OnceExecute(obj.Name, obj.Generation, func() error {
		atomic.AddInt32(d.created, 1)
		return nil
	})
	if err != nil {
		return reconcile.Result{}, err
	}
	_, _, err = d.st.UpdateFunc(obj.Name, obj.ResourceVersion, func(o *v1.Object) *v1.Object {
		o.Status = map[string]string{
			"attempts": fmt.Sprintf("%d", attempts),
			"phase":    "Running",
			"effect":   fmt.Sprintf("%v@gen%d", exec, o.Generation),
		}
		return o
	})
	if errors.Is(err, store.ErrConflict) {
		return reconcile.Result{}, reconcile.NewError(reconcile.KindConflict, "stale status", err)
	}
	return reconcile.Result{}, err
}

func (d *deployer) setFail(name string, n int) {
	d.mu.Lock()
	d.failN[name] = n
	d.mu.Unlock()
}

func newApp(t *testing.T, maxAttempts int) (*app, context.CancelFunc) {
	t.Helper()
	st := store.New()
	rec := sideeffects.New()
	var created int32
	dec := &deployer{st: st, rec: rec, created: &created, failN: map[string]int{}}
	c := reconcile.NewController(reconcile.Config{
		Workers:     4,
		MaxAttempts: maxAttempts,
		Backoff:     queue.NewExponentialBackoff(2*time.Millisecond, 200*time.Millisecond, 2),
		Store:       st,
		Reconciler:  dec,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Run(ctx) }()
	return &app{st: st, rec: rec, dec: dec, c: c}, cancel
}

func wait(t *testing.T, within time.Duration, f func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timeout: " + msg)
}

// 端到端：期望态多次声明（含重复）→ 实际态收敛；外部负载只创建一次/世代。
func TestE2E_DeclarativeConvergence(t *testing.T) {
	a, cancel := newApp(t, 0)
	defer cancel()

	for i := 0; i < 5; i++ {
		_, err := a.st.Create(&v1.Object{Name: "srv", Spec: map[string]string{"img": "v1"}, Finalizers: []string{"deploy"}})
		if errors.Is(err, store.ErrConflict) {
			break // Create 重复由并发场景触发
		} else if err != nil {
			t.Fatal(err)
		}
	}
	// 重复声明同一期望态（相同 Spec），外加大量重复入队。
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = a.c.Enqueue("srv") }()
	}
	wg.Wait()

	wait(t, 2*time.Second, func() bool {
		o, err := a.st.Get("srv")
		return err == nil && o.Status["phase"] == "Running"
	}, "srv Running")
	o, _ := a.st.Get("srv")
	t.Logf("输入: Create(srv)+30 次重复 Enqueue，Spec 未变 => 判定: phase=%s effect=%s 外部负载创建=%d",
		o.Status["phase"], o.Status["effect"], atomic.LoadInt32(a.dec.created))
	if atomic.LoadInt32(a.dec.created) != 1 {
		t.Fatalf("外部负载应恰好创建 1 次, got %d", atomic.LoadInt32(a.dec.created))
	}

	// 声明新的期望世代（Spec 变化）：允许再创建一次，但旧世代不重复。
	conv := func(cur *v1.Object) *v1.Object { cur.Spec["img"] = "v2"; return cur }
	for {
		cur, _ := a.st.Get("srv")
		_, _, err := a.st.UpdateFunc("srv", cur.ResourceVersion, conv)
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		break
	}
	wait(t, 2*time.Second, func() bool {
		o, err := a.st.Get("srv")
		return err == nil && o.Spec["img"] == "v2" && o.Status["phase"] == "Running" && o.Status["effect"] == "true@gen2"
	}, "srv gen2 Running")
	t.Logf("输入: Spec img=v2（新世代）=> 判定: 外部负载创建=%d（每世代一次，旧世代不重复）",
		atomic.LoadInt32(a.dec.created))
	if atomic.LoadInt32(a.dec.created) != 2 {
		t.Fatalf("新世代应再创建一次, got %d", atomic.LoadInt32(a.dec.created))
	}
}

// 端到端：失败退避 + 耗尽结论可区分。
func TestE2E_BackoffAndExhaustedDistinguishable(t *testing.T) {
	a, cancel := newApp(t, 3)
	defer cancel()

	a.dec.setFail("recover", 2) // 前 2 次失败后恢复
	_, _ = a.st.Create(&v1.Object{Name: "recover", Spec: map[string]string{"img": "v1"}, Finalizers: []string{"deploy"}})
	a.dec.setFail("permanent", 100) // 持续失败
	_, _ = a.st.Create(&v1.Object{Name: "permanent", Spec: map[string]string{"img": "v1"}, Finalizers: []string{"deploy"}})

	wait(t, 2*time.Second, func() bool {
		o, err := a.st.Get("recover")
		return err == nil && o.Status["phase"] == "Running"
	}, "recover 经退避重试后 Running")
	wait(t, 2*time.Second, func() bool {
		for _, f := range a.c.Failures() {
			if f.Name == "permanent" {
				return true
			}
		}
		return false
	}, "permanent 给出 Exhausted 结论")

	o, _ := a.st.Get("recover")
	var fail reconcile.FailureRecord
	for _, f := range a.c.Failures() {
		if f.Name == "permanent" {
			fail = f
		}
	}
	t.Logf("输入: recover 失败 2 次后恢复；permanent 持续失败，MaxAttempts=3 =>")
	t.Logf("判定: recover phase=%s attempts=%s；permanent 失败分类=%s 尝试=%d（与 Transient/Conflict/Terminal 可区分）",
		o.Status["phase"], o.Status["attempts"], fail.Kind, fail.Attempts)
	if fail.Kind != reconcile.KindExhausted || fail.Attempts != 3 {
		t.Fatalf("unexpected failure: %+v", fail)
	}
}

// 端到端：终结器删除顺序与物理消失条件。
func TestE2E_DeletionViaFinalizers(t *testing.T) {
	a, cancel := newApp(t, 0)
	defer cancel()

	_, err := a.st.Create(&v1.Object{
		Name:       "db",
		Spec:       map[string]string{"disk": "ssd"},
		Finalizers: []string{"other", "deploy"}, // other 由其他控制器管理（测试中手动移除）
	})
	if err != nil {
		t.Fatal(err)
	}
	wait(t, time.Second, func() bool {
		o, err := a.st.Get("db")
		return err == nil && o.Status["phase"] == "Running"
	}, "db Running")

	_, removed, err := a.st.Delete("db")
	if err != nil || removed {
		t.Fatalf("带终结器删除不应物理消失: removed=%v err=%v", removed, err)
	}
	// 本控制器移除 "deploy" 后，仍有 "other"，对象必须还在。
	wait(t, 2*time.Second, func() bool {
		o, err := a.st.Get("db")
		return err == nil && !contains(o.Finalizers, "deploy")
	}, "deploy 终结器被本控制器移除")
	o, _ := a.st.Get("db")
	t.Logf("输入: Delete(db)，终结器=[other deploy] => 判定: 本控制器移除 deploy 后剩余=%v 对象仍存在",
		o.Finalizers)
	if len(o.Finalizers) != 1 || o.Finalizers[0] != "other" {
		t.Fatalf("got %v", o.Finalizers)
	}

	// 另一控制器移除最后一个终结器 => 物理消失。
	for {
		cur, _ := a.st.Get("db")
		_, removed, err = a.st.UpdateFunc("db", cur.ResourceVersion, func(x *v1.Object) *v1.Object {
			x.Finalizers = nil
			return x
		})
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		break
	}
	wait(t, 2*time.Second, func() bool {
		_, err := a.st.Get("db")
		return errors.Is(err, store.ErrNotFound)
	}, "db 物理消失")
	t.Log("判定依据: 仅当全部终结器（other 与 deploy）都移除后，对象才 ErrNotFound")
}

// 端到端：关闭时在途任务可观察、排空不丢任务。
func TestE2E_GracefulShutdownObservable(t *testing.T) {
	a, cancel := newApp(t, 0)

	const n = 20
	for i := 0; i < n; i++ {
		_, _ = a.st.Create(&v1.Object{
			Name: fmt.Sprintf("w-%02d", i), Spec: map[string]string{"i": fmt.Sprintf("%d", i)},
			Finalizers: []string{"deploy"},
		})
	}
	// 立即取消，制造「关闭时仍有在途/排队任务」。
	time.Sleep(3 * time.Millisecond)
	inFlightDuring := append([]string(nil), a.c.InFlight()...)
	cancel()

	wait(t, 3*time.Second, func() bool {
		list, _ := a.st.List()
		if len(list) != n {
			return false
		}
		for _, o := range list {
			if o.Status["phase"] != "Running" {
				return false
			}
		}
		return true
	}, "20 个对象在关闭后全部收敛")

	list, _ := a.st.List()
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	notReady := 0
	for _, o := range list {
		if o.Status["phase"] != "Running" {
			notReady++
		}
	}
	t.Logf("输入: 20 个对象调谐途中 cancel（关闭瞬间 InFlight=%v）=> 判定: 收敛=%d/%d 未就绪=%d",
		inFlightDuring, n-notReady, n, notReady)
	if notReady != 0 {
		t.Fatalf("关闭后排空丢失任务: %d 个未就绪", notReady)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func removeOne(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}
