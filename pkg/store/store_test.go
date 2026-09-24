package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
)

func newObj(name string, spec map[string]string, finalizers ...string) *v1.Object {
	return &v1.Object{Name: name, Spec: spec, Finalizers: finalizers}
}

func TestCreateGetVersion(t *testing.T) {
	s := New()
	obj, err := s.Create(newObj("a", map[string]string{"image": "v1"}))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Logf("输入: Create(a, image=v1) => 判定: ResourceVersion=%d Generation=%d（期望 RV=1 Gen=1）",
		obj.ResourceVersion, obj.Generation)
	if obj.ResourceVersion != 1 || obj.Generation != 1 {
		t.Fatalf("got rv=%d gen=%d, want rv=1 gen=1", obj.ResourceVersion, obj.Generation)
	}
	if _, err := s.Create(newObj("a", nil)); !errors.Is(err, ErrConflict) {
		t.Fatalf("重复 Create 应返回 ErrConflict, got %v", err)
	}
	t.Log("判定依据: 同名对象已存在 => ErrConflict")
}

func TestOptimisticConflict(t *testing.T) {
	s := New()
	first, _ := s.Create(newObj("cfg", map[string]string{"k": "1"}))

	// 另一路写先提交，产生新版本。
	_, _, err := s.UpdateFunc("cfg", first.ResourceVersion, func(cur *v1.Object) *v1.Object {
		cur.Spec["k"] = "2"
		return cur
	})
	if err != nil {
		t.Fatalf("第一次 UpdateFunc: %v", err)
	}

	// 基于过期版本的破坏性写必须被拒绝。
	_, _, err = s.UpdateFunc("cfg", first.ResourceVersion, func(cur *v1.Object) *v1.Object {
		cur.Spec = nil // 破坏性变更
		return cur
	})
	t.Logf("输入: 基于过期 RV=%d 清空 Spec => 判定: err=%v", first.ResourceVersion, err)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("过期版本写应得到 ErrConflict, got %v", err)
	}

	got, _ := s.Get("cfg")
	if got.Spec["k"] != "2" {
		t.Fatalf("陈旧写被拒绝后数据不应变化, got %v", got.Spec)
	}
	t.Logf("判定依据: 当前 RV=%d 与条件 RV=%d 不一致；对象保持 k=2",
		got.ResourceVersion, first.ResourceVersion)
}

func TestGenerationBumpsOnSpecChange(t *testing.T) {
	s := New()
	obj, _ := s.Create(newObj("g", map[string]string{"a": "1"}))

	// 仅状态变化：Generation 不变，RV 增长。
	updated, _, _ := s.UpdateFunc("g", obj.ResourceVersion, func(cur *v1.Object) *v1.Object {
		cur.Status = map[string]string{"ready": "true"}
		return cur
	})
	t.Logf("仅状态更新: RV %d->%d, Generation=%d（期望保持 1）",
		obj.ResourceVersion, updated.ResourceVersion, updated.Generation)
	if updated.Generation != 1 || updated.ResourceVersion != 2 {
		t.Fatalf("got rv=%d gen=%d, want rv=2 gen=1", updated.ResourceVersion, updated.Generation)
	}

	// Spec 变化：Generation 递增。
	updated2, _, _ := s.UpdateFunc("g", updated.ResourceVersion, func(cur *v1.Object) *v1.Object {
		cur.Spec["a"] = "2"
		return cur
	})
	t.Logf("Spec 更新: Generation %d->%d（期望 +1）", updated.Generation, updated2.Generation)
	if updated2.Generation != 2 {
		t.Fatalf("got gen=%d, want 2", updated2.Generation)
	}
}

func TestFinalizerDeletionLifecycle(t *testing.T) {
	s := New()
	_, _ = s.Create(newObj("pod", map[string]string{}, "storage", "network"))

	marked, removed, err := s.Delete("pod")
	if err != nil || removed {
		t.Fatalf("带终结器 Delete 不应移除对象: removed=%v err=%v", removed, err)
	}
	t.Logf("输入: Delete(pod)，终结器=%v => 判定: 仅标记删除 DeletionTimestamp=%v removed=false",
		marked.Finalizers, marked.DeletionTimestamp != nil)
	if !marked.Deleted() || len(marked.Finalizers) != 2 {
		t.Fatalf("删除标记不正确")
	}

	// 重复删除幂等、无新版本副作用（返回同一快照）。
	again, removed2, _ := s.Delete("pod")
	if again.ResourceVersion != marked.ResourceVersion || removed2 {
		t.Fatalf("重复删除必须幂等: rv %d vs %d removed=%v",
			again.ResourceVersion, marked.ResourceVersion, removed2)
	}
	t.Logf("判定依据: 已处于删除中 => 幂等返回 RV=%d", again.ResourceVersion)

	// 终结器逐个移除；移除最后一个时对象才真正消失。
	cur := marked
	cur, _, _ = s.UpdateFunc("pod", cur.ResourceVersion, func(o *v1.Object) *v1.Object {
		o.Finalizers = o.Finalizers[1:] // 移除 storage
		return o
	})
	if _, err := s.Get("pod"); err != nil {
		t.Fatalf("仍有终结器时对象必须存在")
	}
	t.Logf("移除第一个终结器后: 剩余=%v，对象仍存在", cur.Finalizers)

	last, removed, _ := s.UpdateFunc("pod", cur.ResourceVersion, func(o *v1.Object) *v1.Object {
		o.Finalizers = nil // 移除 network
		return o
	})
	t.Logf("移除最后一个终结器: removed=%v 最后快照 RV=%d", removed, last.ResourceVersion)
	if !removed {
		t.Fatalf("终结器全部移除时应彻底删除")
	}
	if _, err := s.Get("pod"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("彻底删除后 Get 应 ErrNotFound, got %v", err)
	}

	// 对已彻底删除对象的条件更新不能复活它。
	if _, _, err := s.UpdateFunc("pod", last.ResourceVersion, func(o *v1.Object) *v1.Object {
		return o
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("已删除对象的迟到更新应 ErrNotFound, got %v", err)
	}
	t.Log("判定依据: 终结器顺序移除，全部移除 => 物理删除；迟到更新 => ErrNotFound")
}

func TestDeleteWithoutFinalizerIsImmediate(t *testing.T) {
	s := New()
	_, _ = s.Create(newObj("tmp", map[string]string{"x": "y"}))
	_, removed, err := s.Delete("tmp")
	if err != nil || !removed {
		t.Fatalf("无终结器对象应立即彻底删除, removed=%v err=%v", removed, err)
	}
	if _, err := s.Get("tmp"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	t.Log("输入: 无终结器对象 Delete => 判定: removed=true 且 Get=ErrNotFound")
}

func TestChangeHook(t *testing.T) {
	s := New()
	var mu sync.Mutex
	var events []string
	s.SetHook(func(name string, deleted bool) {
		mu.Lock()
		events = append(events, fmt.Sprintf("%s/deleted=%v", name, deleted))
		mu.Unlock()
	})
	_, _ = s.Create(newObj("h", nil))
	_, _, _ = s.UpdateFunc("h", 1, func(o *v1.Object) *v1.Object { o.Status = map[string]string{"r": "1"}; return o })
	_, _, _ = s.Delete("h")

	want := []string{"h/deleted=false", "h/deleted=false", "h/deleted=true"}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("输入: Create/Update/Delete(h) => 钩子事件=%v", events)
	if len(events) != len(want) {
		t.Fatalf("got %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("event[%d]=%s want %s", i, events[i], want[i])
		}
	}
}

func TestConcurrentUpdateOnlyOneSucceedsPerVersion(t *testing.T) {
	s := New()
	obj, _ := s.Create(newObj("c", map[string]string{"n": "0"}))
	const writers = 20
	var wg sync.WaitGroup
	var ok, conflict int64
	var mu sync.Mutex
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			defer wg.Done()
			cur, _ := s.Get("c") // 每个 goroutine 先读最新版本
			_, _, err := s.UpdateFunc("c", cur.ResourceVersion, func(o *v1.Object) *v1.Object {
				o.Spec["n"] = fmt.Sprintf("%d", i)
				return o
			})
			mu.Lock()
			if err == nil {
				ok++
			} else if errors.Is(err, ErrConflict) {
				conflict++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	t.Logf("输入: %d 个并发写者各自读-改-写 => 判定: 成功=%d 冲突=%d（成功应为全部，因每次都读最新 RV；冲突=0 亦可接受若读发生在同版本）",
		writers, ok, conflict)
	if ok+conflict != writers {
		t.Fatalf("所有写者必须得到确定结论, ok=%d conflict=%d", ok, conflict)
	}
	t.Logf("最终对象: %+v（RV=%d）", mustGet(t, s, "c").Spec, mustGet(t, s, "c").ResourceVersion)

	// 基于固定旧版本的并发写：恰好一个成功。
	var ok2, cf2 int64
	var wg2 sync.WaitGroup
	wg2.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg2.Done()
			_, _, err := s.UpdateFunc("c", obj.ResourceVersion, func(o *v1.Object) *v1.Object {
				o.Spec["stale"] = "x"
				return o
			})
			mu.Lock()
			if err == nil {
				ok2++
			} else {
				cf2++
			}
			mu.Unlock()
		}()
	}
	wg2.Wait()
	t.Logf("输入: %d 个并发写者同用旧 RV=%d => 判定: 成功=%d 冲突=%d（期望成功=0，因对象早已超过 RV=1）",
		writers, obj.ResourceVersion, ok2, cf2)
	if ok2 != 0 || cf2 != writers {
		t.Fatalf("旧版本并发写应全部冲突, ok=%d conflict=%d", ok2, cf2)
	}
}

func mustGet(t *testing.T, s *Store, name string) *v1.Object {
	t.Helper()
	o, err := s.Get(name)
	if err != nil {
		t.Fatalf("Get %s: %v", name, err)
	}
	return o
}
