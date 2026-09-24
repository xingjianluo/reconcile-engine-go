package v1

import "time"

// Object 是调谐引擎管理的声明式资源。
// 字段设计说明见 README「调谐协议」一节。
type Object struct {
	Name              string
	ResourceVersion   uint64 // 存储层每次写操作单调递增，用于乐观并发
	Generation        uint64 // 期望状态（Spec）版本，仅在 Spec 变化时递增
	Spec              map[string]string
	Status            map[string]string
	Finalizers        []string
	DeletionTimestamp *time.Time
}

// Clone 返回深拷贝，避免调用方持有存储内部数据。
func (o *Object) Clone() *Object {
	if o == nil {
		return nil
	}
	cp := *o
	cp.Spec = cloneMap(o.Spec)
	cp.Status = cloneMap(o.Status)
	if o.Finalizers != nil {
		cp.Finalizers = append([]string(nil), o.Finalizers...)
	}
	if o.DeletionTimestamp != nil {
		t := *o.DeletionTimestamp
		cp.DeletionTimestamp = &t
	}
	return &cp
}

// Deleted 报告对象是否已被请求删除（进入终结器阶段）。
func (o *Object) Deleted() bool { return o.DeletionTimestamp != nil }

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
