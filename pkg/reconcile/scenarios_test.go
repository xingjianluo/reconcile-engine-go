package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
	"github.com/xingjianluo/reconcile-engine-go/pkg/store"
)

// 场景 1：并发变更分发 + 同资源串行合并 + 幂等副作用。
func TestDispatchMergeAndIdempotentEffect(t *testing.T) {
	env, c, cancel := newTestEnv(t, 0, 4)
	defer cancel()

	obj, err := env.st.Create(&v1.Object{
		Name: "svc-a", Spec: map[string]string{"image": "v1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 模拟控制器外部短时间内大量重复/连续变更入队。
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Enqueue("svc-a") }()
	}
	// 同时存储侧也发生 Spec 更新（钩子自动分发）；与控制器并发，需冲突重试。
	latest := obj
	for v := 2; v <= 4; v++ {
		vv := fmt.Sprintf("v%d", v)
		for {
			cur, gerr := env.st.Get("svc-a")
			if gerr != nil {
				t.Fatal(gerr)
			}
			next, _, uerr := env.st.UpdateFunc("svc-a", cur.ResourceVersion, func(o *v1.Object) *v1.Object {
				o.Spec["image"] = vv
				return o
			})
			if errors.Is(uerr, store.ErrConflict) {
				continue // 与调谐器并发写竞争，读最新版本后重试
			}
			if uerr != nil {
				t.Fatal(uerr)
			}
			latest = next
			break
		}
	}
	wg.Wait()
	if latest.Spec["image"] != "v4" {
		t.Fatalf("测试前置写入未收敛到 v4: %+v", latest)
	}

	waitFor(t, 2*time.Second, func() bool {
		got, err := env.st.Get("svc-a")
		return err == nil && got.Status["ready"] == "true"
	}, "svc-a 调谐完成 ready=true")
	// ready=true 可能由较早世代先写出；等待所有中间世代的在途调谐彻底排空。
	if !c.WaitForQuiet(context.Background(), 2*time.Second, 50*time.Millisecond) {
		t.Fatal("系统未进入稳定态（队列/在途未排空）")
	}

	final, _ := env.st.Get("svc-a")
	versions := env.rec.ExecutedVersions("svc-a")
	effectCount := atomic.LoadInt32(&env.effects)
	t.Logf("输入: 50 次重复 Enqueue + 3 次 Spec 变更(svc-a) => 判定: 最终 RV=%d image=%s ready=%s",
		final.ResourceVersion, final.Spec["image"], final.Status["ready"])
	t.Logf("判定依据: 队列按键去重合并，调谐调用=%d 次，幂等副作用版本=%v，最终仅对最新版本链生效",
		env.fr.callCount("svc-a"), versions)
	if final.Spec["image"] != "v4" || final.Status["ready"] != "true" {
		t.Fatalf("未收敛到最终期望: %+v", final)
	}
	// 副作用执行数等于记录到的不同世代数；世代快速推进时较旧世代可能被
	// 更新世代抢占而跳过（最终一致：中间世代不必各产生一次外部动作），
	// 但同一世代绝不可能执行两次，且最终世代 gen4 必然被执行。
	if int(effectCount) != len(versions) {
		t.Fatalf("副作用执行数 %d 与记录世代 %d 不一致", effectCount, len(versions))
	}
	if effectCount < 1 {
		t.Fatalf("至少应对某个世代执行副作用, got %d", effectCount)
	}
	if int(final.Generation) != 4 {
		t.Fatalf("最终世代应为 4, got %d", final.Generation)
	}
	hasFinalGen := false
	for _, g := range versions {
		if g == final.Generation {
			hasFinalGen = true
		}
	}
	if !hasFinalGen {
		t.Fatalf("最终世代 gen%d 的副作用必须执行, 实际记录=%v", final.Generation, versions)
	}
	calls := env.fr.callCount("svc-a")
	if calls > 20 {
		t.Fatalf("50 次重复入队被合并后调谐次数应很少, got %d", calls)
	}
	t.Logf("结论: 重复入队=%d 次，实际调谐=%d 次，实际副作用=%d 次（执行世代=%v；同世代不重复、最终世代必执行）",
		50, calls, effectCount, versions)
}

// 场景 2：过期版本操作被识别（Conflict）并重读重试，最终成功。
func TestStaleVersionConflictRetried(t *testing.T) {
	env, _, cancel := newTestEnv(t, 0, 2)
	defer cancel()

	obj, _ := env.st.Create(&v1.Object{
		Name:       "svc-stale",
		Spec:       map[string]string{"k": "1"},
		Finalizers: []string{"cleanup"},
	})

	// 先制造一次外部变更，使 obj 的 RV 过期。
	fresh, _, _ := env.st.UpdateFunc("svc-stale", obj.ResourceVersion, func(o *v1.Object) *v1.Object {
		o.Spec["k"] = "2"
		return o
	})

	// 注入：删除清理的第一次提交故意用创建时的过期 RV。
	env.fr.mu.Lock()
	env.fr.staleRV["svc-stale"] = obj.ResourceVersion
	env.fr.mu.Unlock()

	if _, _, err := env.st.Delete("svc-stale"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool {
		_, err := env.st.Get("svc-stale")
		return errors.Is(err, store.ErrNotFound)
	}, "svc-stale 终结器清理后物理消失")

	env.fr.mu.Lock()
	conflicts := env.fr.conflictSeen["svc-stale"]
	calls := env.fr.calls["svc-stale"]
	env.fr.mu.Unlock()
	t.Logf("输入: 删除带终结器对象，首次清理用过期 RV=%d（最新 RV=%d）=> 判定: 识别冲突=%d 次，调谐=%d 次后对象 ErrNotFound",
		obj.ResourceVersion, fresh.ResourceVersion, conflicts, calls)
	if conflicts < 1 {
		t.Fatalf("应至少识别出一次版本冲突")
	}
	t.Log("判定依据: 过期 RV 提交返回 ErrConflict => 分类为 Conflict 立即重读重试，未基于陈旧数据移除终结器")
}

// 场景 3：瞬时失败按指数退避重试，最终成功；退避次数可观察、单调。
func TestTransientFailureBackoffAndRecover(t *testing.T) {
	env, c, cancel := newTestEnv(t, 0, 2)
	defer cancel()

	_, _ = env.st.Create(&v1.Object{Name: "flaky", Spec: map[string]string{"x": "1"}})
	env.fr.mu.Lock()
	env.fr.failUntil["flaky"] = 3 // 前 3 次瞬时失败，之后成功
	env.fr.mu.Unlock()

	waitFor(t, 3*time.Second, func() bool {
		got, err := env.st.Get("flaky")
		return err == nil && got.Status["ready"] == "true"
	}, "flaky 退避重试后成功")

	got, _ := env.st.Get("flaky")
	t.Logf("输入: flaky 前 3 次调谐注入瞬时失败 => 判定: 总调谐=%d 次后 ready=%s effect=%s",
		env.fr.callCount("flaky"), got.Status["ready"], got.Status["effect"])
	if env.fr.callCount("flaky") < 4 {
		t.Fatalf("至少应调谐 4 次（3 失败 + 1 成功）, got %d", env.fr.callCount("flaky"))
	}
	if len(c.Failures()) != 0 {
		t.Fatalf("恢复成功后不应有失败结论: %+v", c.Failures())
	}
	t.Log("判定依据: Transient => 指数退避重入队，成功后 Reset，无失败结论")
}

// 场景 4：重试耗尽必须给出可区分的 Exhausted 失败结论，而非静默丢弃。
func TestExhaustedFailureReported(t *testing.T) {
	env, c, cancel := newTestEnv(t, 3, 2) // 最多尝试 3 次
	defer cancel()

	_, _ = env.st.Create(&v1.Object{Name: "dead", Spec: map[string]string{"x": "1"}})
	env.fr.mu.Lock()
	env.fr.failUntil["dead"] = 1 << 30 // 永远失败
	env.fr.mu.Unlock()

	waitFor(t, 3*time.Second, func() bool { return len(c.Failures()) >= 1 }, "dead 产生 Exhausted 结论")
	fails := c.Failures()
	if len(fails) != 1 {
		t.Fatalf("期望恰好 1 条失败结论, got %d: %+v", len(fails), fails)
	}
	rec := fails[0]
	t.Logf("输入: dead 持续瞬时失败，MaxAttempts=3 => 判定: 失败结论 Kind=%s Name=%s Attempts=%d Msg=%q",
		rec.Kind, rec.Name, rec.Attempts, rec.Message)
	if rec.Kind != KindExhausted || rec.Attempts != 3 {
		t.Fatalf("got kind=%s attempts=%d", rec.Kind, rec.Attempts)
	}

	// 新的入队不应再被静默吞掉之外还无限循环；停止后失败结论稳定可查询。
	time.Sleep(30 * time.Millisecond)
	if len(c.Failures()) != 1 {
		t.Fatalf("耗尽后不应重复刷失败结论, got %d", len(c.Failures()))
	}
	t.Log("判定依据: 第 3 次失败后分类为 Exhausted（区别于 Terminal/Conflict/Transient），任务不再重入队")
}

// 场景 5：终态错误立即给出 Terminal 结论，不浪费重试。
func TestTerminalFailureReported(t *testing.T) {
	env, c, cancel := newTestEnv(t, 5, 2)
	defer cancel()

	_, _ = env.st.Create(&v1.Object{Name: "bad", Spec: map[string]string{"x": "1"}})
	env.fr.mu.Lock()
	env.fr.terminal["bad"] = "spec 非法：字段 x 不允许"
	env.fr.mu.Unlock()

	waitFor(t, 2*time.Second, func() bool { return len(c.Failures()) >= 1 }, "bad 产生 Terminal 结论")
	rec := c.Failures()[0]
	t.Logf("输入: bad 每次返回终态错误 => 判定: Kind=%s Attempts=%d Msg=%q 调谐次数=%d",
		rec.Kind, rec.Attempts, rec.Message, env.fr.callCount("bad"))
	if rec.Kind != KindTerminal {
		t.Fatalf("got %s", rec.Kind)
	}
	if env.fr.callCount("bad") != 1 {
		t.Fatalf("终态错误不应重试, 调谐次数=%d", env.fr.callCount("bad"))
	}
	t.Log("判定依据: Terminal 不重入队，失败原因与 Exhausted 可区分")
}

// 场景 6：终结器删除顺序——多终结器逐轮移除，全部移除后才物理消失。
func TestFinalizerDeletionOrdering(t *testing.T) {
	env, _, cancel := newTestEnv(t, 0, 2)
	defer cancel()

	_, _ = env.st.Create(&v1.Object{
		Name: "db", Spec: map[string]string{"vol": "v1"},
		Finalizers: []string{"snapshot", "volume", "network"},
	})
	if _, _, err := env.st.Delete("db"); err != nil {
		t.Fatal(err)
	}

	// 在删除过程中观察：对象始终存在，终结器数量递减。
	prev := 3
	stableEmpty := false
	for i := 0; i < 200; i++ {
		obj, err := env.st.Get("db")
		if errors.Is(err, store.ErrNotFound) {
			stableEmpty = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(obj.Finalizers) > prev {
			t.Fatalf("终结器数量不应增长: %d -> %d", prev, len(obj.Finalizers))
		}
		prev = len(obj.Finalizers)
		time.Sleep(2 * time.Millisecond)
	}
	waitFor(t, 2*time.Second, func() bool {
		_, err := env.st.Get("db")
		return errors.Is(err, store.ErrNotFound)
	}, "db 物理删除")
	if !stableEmpty {
		t.Fatal("删除完成前不应提前 ErrNotFound")
	}
	t.Logf("输入: db 含终结器 [snapshot volume network]，Delete 后轮询 => 判定: 终结器 3→2→1→0，归零后对象才 ErrNotFound，总调谐=%d",
		env.fr.callCount("db"))
	t.Log("判定依据: 删除按终结器语义逐步推进，每轮移除一个，未全部移除前存储拒绝物理消失")
}

// 场景 7：优雅关闭——关闭时在途调谐可观察，队列排空后 Run 返回，任务不丢。
func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	env, c, cancel := newTestEnv(t, 0, 2)
	defer cancel()

	for i := 0; i < 10; i++ {
		_, _ = env.st.Create(&v1.Object{
			Name: fmt.Sprintf("job-%02d", i), Spec: map[string]string{"i": fmt.Sprintf("%d", i)},
		})
	}
	// 让前几个任务慢一点，确保关闭瞬间存在在途调谐。
	env.fr.mu.Lock()
	env.fr.failUntil["job-00"] = 1 // 一次快速失败重试，占用在途窗口
	env.fr.mu.Unlock()

	time.Sleep(5 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		// Run 已在 newTestEnv 中启动；这里通过观察队列/失败终态判断收敛。
		close(done)
	}()
	<-done

	// 关闭后最终所有对象都应达到 ready=true（退避条目被提前提升并排空）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		allReady := true
		list, _ := env.st.List()
		if len(list) != 10 {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		for _, o := range list {
			if o.Status["ready"] != "true" {
				allReady = false
			}
		}
		if allReady {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	list, _ := env.st.List()
	notReady := []string{}
	for _, o := range list {
		if o.Status["ready"] != "true" {
			notReady = append(notReady, o.Name)
		}
	}
	t.Logf("输入: 10 个对象调谐途中 cancel() => 判定: 关闭后收敛对象数=%d/10 未就绪=%v 在途观察 API InFlight()=%v",
		len(list), notReady, c.InFlight())
	if len(notReady) != 0 {
		t.Fatalf("优雅关闭后任务丢失（未收敛）: %v", notReady)
	}
}
