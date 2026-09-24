package reconcile

import "github.com/xingjianluo/reconcile-engine-go/pkg/store"

// newStoreForTest 返回一个普通存储（不被 Controller 构造函数重复挂钩子；
// NewController 内部会设置钩子）。
func newStoreForTest() *store.Store { return store.New() }
