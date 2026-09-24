package reconcile

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
	"github.com/xingjianluo/reconcile-engine-go/pkg/queue"
	"github.com/xingjianluo/reconcile-engine-go/pkg/sideeffects"
	"github.com/xingjianluo/reconcile-engine-go/pkg/store"
)

// fakeReconciler 是可注入行为的测试调谐器：
//   - 对带 finalizer 的对象，在删除阶段逐个移除终结器（每轮一个）；
//   - 每次成功调谐通过 Recorder 登记按版本幂等的「外部副作用」；
//   - failSpec 中配置的资源按计划返回瞬时错误，直到 attempts 次后成功；
//   - staleSpec 中配置的资源前 N 次基于陈旧视图提交，触发 Conflict 后恢复。
type fakeReconciler struct {
	mu           sync.Mutex
	calls        map[string]int
	recorder     *sideeffects.Recorder
	effects      *int32
	st           *store.Store
	failUntil    map[string]int // 资源 => 前 N 次返回瞬时错误
	terminal     map[string]string
	staleRV      map[string]uint64 // 资源 => 用该过期 RV 提交直到得到一次 Conflict
	conflictSeen map[string]int
}

func newFake(rec *sideeffects.Recorder, effects *int32) *fakeReconciler {
	return &fakeReconciler{
		calls:        map[string]int{},
		recorder:     rec,
		effects:      effects,
		st:           nil,
		failUntil:    map[string]int{},
		terminal:     map[string]string{},
		staleRV:      map[string]uint64{},
		conflictSeen: map[string]int{},
	}
}

func (f *fakeReconciler) callCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[name]
}

func (f *fakeReconciler) Reconcile(ctx context.Context, obj *v1.Object) (Result, error) {
	f.mu.Lock()
	f.calls[obj.Name]++
	n := f.calls[obj.Name]
	failN := f.failUntil[obj.Name]
	termMsg := f.terminal[obj.Name]
	stale := f.staleRV[obj.Name]
	f.mu.Unlock()

	// 终态错误：永不重试。
	if termMsg != "" {
		return Result{}, NewError(KindTerminal, termMsg, nil)
	}

	// 删除阶段：每轮移除一个终结器，全部移除后对象在提交时被存储物理删除。
	if obj.Deleted() {
		if len(obj.Finalizers) > 0 {
			rv := obj.ResourceVersion
			if stale != 0 && f.conflictSeen[obj.Name] == 0 {
				rv = stale
			}
			_, removed, err := f.st.UpdateFunc(obj.Name, rv, func(o *v1.Object) *v1.Object {
				o.Finalizers = o.Finalizers[1:]
				return o
			})
			if err == store.ErrConflict {
				f.mu.Lock()
				f.conflictSeen[obj.Name]++
				f.mu.Unlock()
				return Result{}, NewError(KindConflict,
					fmt.Sprintf("%s 删除清理基于过期 RV=%d", obj.Name, rv), err)
			}
			if err != nil {
				return Result{}, err
			}
			return Result{Requeue: !removed}, nil
		}
	}

	// 计划内瞬时失败（如外部系统暂时不可用）。
	if n <= failN {
		return Result{}, NewError(KindTransient,
			fmt.Sprintf("%s 第 %d 次调谐：下游暂不可用（注入失败）", obj.Name, n), nil)
	}

	// 幂等副作用：外部副作用按 Generation（Spec 版本）去重——
	// 状态回写只增长 ResourceVersion，不得重复触发外部动作。
	exec, err := f.recorder.OnceExecute(obj.Name, obj.Generation, func() error {
		atomic.AddInt32(f.effects, 1)
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	// 将结果写回 Status（条件更新；内容无变化时存储层判定为空操作）。
	_, _, err = f.st.UpdateFunc(obj.Name, obj.ResourceVersion, func(o *v1.Object) *v1.Object {
		if o.Status == nil {
			o.Status = map[string]string{}
		}
		if exec {
			o.Status["effect"] = fmt.Sprintf("applied@gen%d", obj.Generation)
		}
		o.Status["ready"] = "true"
		return o
	})
	if err == store.ErrConflict {
		return Result{}, NewError(KindConflict,
			fmt.Sprintf("%s 状态回写基于过期 RV=%d，需重读", obj.Name, obj.ResourceVersion), err)
	}
	if err != nil {
		return Result{}, err
	}
	return Result{}, nil
}

type testEnv struct {
	st      *store.Store
	rec     *sideeffects.Recorder
	fr      *fakeReconciler
	effects int32
}

func newTestEnv(t *testing.T, maxAttempts int, workers int) (*testEnv, *Controller, context.CancelFunc) {
	t.Helper()
	st := store.New()
	env := &testEnv{st: st, rec: sideeffects.New()}
	env.fr = newFake(env.rec, &env.effects)
	env.fr.st = st
	bo := newTestBackoff()
	cfg := Config{
		Workers:     workers,
		MaxAttempts: maxAttempts,
		Backoff:     bo,
		Store:       st,
		Reconciler:  env.fr,
	}
	c := NewController(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = c.Run(ctx) }()
	return env, c, cancel
}

// newTestBackoff 使用对测试足够快但仍可观察的退避参数。
func newTestBackoff() *queue.ExponentialBackoff {
	return queue.NewExponentialBackoff(2*time.Millisecond, 100*time.Millisecond, 2)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("等待条件超时: %s", what)
}

func sortedNames(objs []*v1.Object) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Name
	}
	sort.Strings(out)
	return out
}

var _ = v1.Object{}
