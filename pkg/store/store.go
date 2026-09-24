package store

import (
	"errors"
	"sync"
	"time"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
)

// ErrNotFound 表示对象不存在（含已被彻底删除）。
var ErrNotFound = errors.New("object not found")

// ErrConflict 表示基于过期 ResourceVersion 的写操作被拒绝。
var ErrConflict = errors.New("resource version conflict")

// ChangeHook 在每次成功提交后被调用：deleted=true 表示对象被彻底移除。
// 钩子在锁外调用，可安全地向队列分发任务。
type ChangeHook func(name string, deleted bool)

// Store 是带乐观并发控制的本地对象存储（模拟声明式 API Server）。
//
// 并发模型：一把互斥锁串行化所有写操作；ResourceVersion 由全局计数器
// 单调发放，任何写都基于调用方读取时的版本做条件比较，过期写一律
// 返回 ErrConflict，杜绝基于陈旧视图的破坏性操作。
type Store struct {
	mu      sync.Mutex
	objects map[string]*v1.Object
	nextRV  uint64
	hook    ChangeHook
}

// New 创建空存储。
func New() *Store {
	return &Store{objects: make(map[string]*v1.Object), nextRV: 0}
}

// SetHook 注册变更回调（存储→控制器的变更分发），须在任何写之前调用。
func (s *Store) SetHook(h ChangeHook) {
	s.mu.Lock()
	s.hook = h
	s.mu.Unlock()
}

// Create 存入一个新对象；名称已存在返回错误（复用 ErrConflict 表达）。
// 存储层分配首个 ResourceVersion，并在 Generation 为 0 时初始化为 1。
func (s *Store) Create(obj *v1.Object) (*v1.Object, error) {
	if obj == nil || obj.Name == "" {
		return nil, errors.New("object and its Name are required")
	}
	s.mu.Lock()
	if _, ok := s.objects[obj.Name]; ok {
		s.mu.Unlock()
		return nil, ErrConflict
	}
	stored := obj.Clone()
	s.nextRV++
	stored.ResourceVersion = s.nextRV
	if stored.Generation == 0 {
		stored.Generation = 1
	}
	s.objects[stored.Name] = stored
	hook := s.hook
	s.mu.Unlock()

	if hook != nil {
		hook(stored.Name, false)
	}
	return stored.Clone(), nil
}

// Get 按名称读取对象当前快照；不存在返回 ErrNotFound。
func (s *Store) Get(name string) (*v1.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[name]
	if !ok {
		return nil, ErrNotFound
	}
	return obj.Clone(), nil
}

// List 列出全部对象快照（顺序不保证，测试应按名称排序）。
func (s *Store) List() ([]*v1.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*v1.Object, 0, len(s.objects))
	for _, obj := range s.objects {
		out = append(out, obj.Clone())
	}
	return out, nil
}

// UpdateFunc 基于对象最新版本执行条件更新。
//
//   - rv 非零时必须等于当前 ResourceVersion，否则返回 ErrConflict；
//   - mutate 收到的是当前对象的深拷贝，返回 nil 表示放弃本次更新；
//   - 提交时发放新 ResourceVersion；若 Spec 相对旧版本发生变化，
//     Generation 加 1；
//   - 若更新结果处于「已删除且终结器全部移除」状态，对象被彻底删除，
//     返回 removed=true（此时返回的是被移除前的最后快照）。
func (s *Store) UpdateFunc(name string, rv uint64, mutate func(cur *v1.Object) *v1.Object) (out *v1.Object, removed bool, err error) {
	s.mu.Lock()
	cur, ok := s.objects[name]
	if !ok {
		s.mu.Unlock()
		return nil, false, ErrNotFound
	}
	if rv != 0 && rv != cur.ResourceVersion {
		s.mu.Unlock()
		return nil, false, ErrConflict
	}
	next := mutate(cur.Clone())
	if next == nil {
		s.mu.Unlock()
		return cur.Clone(), false, nil
	}
	// Name 不可变，防止 mutate 把对象改名。
	next.Name = name

	// 无实质变化的提交是空操作：不发放新 RV、不触发分发钩子，
	// 避免调谐器写回相同 Status 时形成自我诱导的空转循环。
	if objectsEqual(cur, next) {
		s.mu.Unlock()
		return cur.Clone(), false, nil
	}

	specChanged := !sameMap(next.Spec, cur.Spec)
	s.nextRV++
	next.ResourceVersion = s.nextRV
	if specChanged {
		next.Generation = cur.Generation + 1
	} else if next.Generation == 0 {
		next.Generation = cur.Generation
	}

	willRemove := next.Deleted() && len(next.Finalizers) == 0
	if willRemove {
		delete(s.objects, name)
	} else {
		s.objects[name] = next.Clone()
	}
	hook := s.hook
	s.mu.Unlock()

	if hook != nil {
		hook(name, willRemove)
	}
	return next.Clone(), willRemove, nil
}

// Delete 发起删除：
//   - 对象不存在返回 ErrNotFound；
//   - 已在删除中则幂等返回当前快照（重复删除无副作用）；
//   - 否则设置 DeletionTimestamp、发放新版本；
//   - 若对象没有任何终结器，随即彻底移除（removed=true）。
//
// 终结器存在时对象仅被标记删除，终结器由调谐逻辑逐个移除，
// 最后一个终结器移除（经 UpdateFunc）时对象才真正消失。
func (s *Store) Delete(name string) (out *v1.Object, removed bool, err error) {
	s.mu.Lock()
	cur, ok := s.objects[name]
	if !ok {
		s.mu.Unlock()
		return nil, false, ErrNotFound
	}
	if cur.Deleted() {
		s.mu.Unlock()
		return cur.Clone(), false, nil
	}
	next := cur.Clone()
	now := clock()
	next.DeletionTimestamp = &now
	s.nextRV++
	next.ResourceVersion = s.nextRV
	willRemove := len(next.Finalizers) == 0
	if willRemove {
		delete(s.objects, name)
	} else {
		s.objects[name] = next.Clone()
	}
	hook := s.hook
	s.mu.Unlock()

	if hook != nil {
		hook(name, willRemove)
	}
	return next.Clone(), willRemove, nil
}

// clock 是可替换的时间源（测试需要确定性时间）。
var clock = time.Now

func sameMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// objectsEqual 判断两个对象除 ResourceVersion 外是否实质相同。
// ResourceVersion 由存储层发放，不参与业务等价判断。
func objectsEqual(a, b *v1.Object) bool {
	if a.Name != b.Name || a.Generation != b.Generation {
		return false
	}
	if !sameMap(a.Spec, b.Spec) || !sameMap(a.Status, b.Status) {
		return false
	}
	if len(a.Finalizers) != len(b.Finalizers) {
		return false
	}
	for i := range a.Finalizers {
		if a.Finalizers[i] != b.Finalizers[i] {
			return false
		}
	}
	switch {
	case a.DeletionTimestamp == nil && b.DeletionTimestamp == nil:
		return true
	case a.DeletionTimestamp == nil || b.DeletionTimestamp == nil:
		return false
	default:
		return a.DeletionTimestamp.Equal(*b.DeletionTimestamp)
	}
}
