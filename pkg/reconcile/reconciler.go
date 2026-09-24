package reconcile

import (
	"context"
	"time"

	v1 "github.com/xingjianluo/reconcile-engine-go/pkg/apis/v1"
)

// Result 指示一次调谐后的队列动作。
type Result struct {
	// Requeue 表示需要立刻重新调谐（如同轮尚有剩余工作、等待终结器）。
	Requeue bool
	// RequeueAfter > 0 时延迟重入队（如等待外部条件成熟）。
	RequeueAfter time.Duration
}

// Reconciler 是业务侧实现的调谐逻辑。
// 实现必须幂等：同一 (Name, ResourceVersion) 重复调用不得产生重复副作用。
type Reconciler interface {
	Reconcile(ctx context.Context, obj *v1.Object) (Result, error)
}

// ReconcilerFunc 便于以函数实现 Reconciler。
type ReconcilerFunc func(ctx context.Context, obj *v1.Object) (Result, error)

func (f ReconcilerFunc) Reconcile(ctx context.Context, obj *v1.Object) (Result, error) {
	return f(ctx, obj)
}
