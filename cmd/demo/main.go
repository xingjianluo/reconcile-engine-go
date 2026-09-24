// Command demo drives the reconciliation engine through create, repeated
// update (coalescing), version conflict, failing reconcile (retry/backoff)
// and finalizer-protected deletion, all against an in-memory store with no
// external services or credentials.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/queue"
	"github.com/xingjianluo/reconcile-engine-go/internal/reconcile"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

const finalizerName = "demo.io/cleanup"

// appReconciler is an idempotent reconciler. On active objects it ensures
// its finalizer and mirrors spec into status (the "actual state"). On
// deleting objects it performs cleanup once, removes its finalizer, and
// relies on a follow-up reconcile to physically remove the object. It also
// simulates transient failures and version conflicts.
type appReconciler struct {
	s     store.Store
	flaky int32 // first N active reconciles fail
	calls atomic.Int64
}

func (r *appReconciler) Reconcile(ctx context.Context, obj *object.Object) (reconcile.Result, error) {
	n := r.calls.Add(1)
	fmt.Printf("    [reconcile #%d] input: %s spec=%v status=%v\n",
		n, obj.String(), obj.Spec, obj.Status)

	if obj.BeingDeleted() {
		// Deletion phase: run idempotent cleanup then drop the finalizer.
		if obj.HasFinalizer(finalizerName) {
			fmt.Printf("    [reconcile #%d] judgment: deleting -> run cleanup, remove finalizer (rv=%d)\n", n, obj.ResourceVersion)
			_, err := r.s.Update(ctx, obj.Key(), obj.ResourceVersion, func(o *object.Object) {
				o.RemoveFinalizer(finalizerName)
			})
			if err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{}, nil
		}
		// No finalizer left and still present -> safe to remove physically.
		fmt.Printf("    [reconcile #%d] judgment: no finalizers -> physical remove\n", n)
		if err := r.s.Remove(ctx, obj.Key(), obj.ResourceVersion); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}

	// Simulate a couple of transient failures to show backoff retry.
	if r.flaky > 0 && atomic.AddInt32(&r.flaky, -1) > 0 {
		return reconcile.Result{}, errors.New("simulated transient downstream failure")
	}

	// Active phase: ensure finalizer + converge status to spec idempotently.
	changed := !obj.HasFinalizer(finalizerName) || !mapsEqual(obj.Spec, obj.Status)
	if !changed {
		fmt.Printf("    [reconcile #%d] judgment: already converged, no side effect\n", n)
		return reconcile.Result{}, nil
	}
	_, err := r.s.Update(ctx, obj.Key(), obj.ResourceVersion, func(o *object.Object) {
		o.AddFinalizer(finalizerName)
		o.Status = cloneMap(o.Spec)
	})
	if err != nil {
		// A conflict means we based the write on a stale version; the
		// controller will re-read and retry.
		return reconcile.Result{}, err
	}
	fmt.Printf("    [reconcile #%d] judgment: applied desired state (status=spec, finalizer added)\n", n)
	return reconcile.Result{}, nil
}

func mapsEqual(a, b map[string]string) bool {
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

func cloneMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := store.NewMemoryStore()
	q := queue.New(queue.Config{
		Backoff:     queue.NewExponentialBackoff(5*time.Millisecond, 100*time.Millisecond),
		MaxAttempts: 6,
	})
	rec := &appReconciler{s: s, flaky: 2}
	ctrl := reconcile.New(reconcile.Config{
		Store:         s,
		Queue:         q,
		Reconciler:    rec,
		Workers:       2,
		ShutdownGrace: 2 * time.Second,
		Logger:        func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
	})

	go func() {
		if err := ctrl.Run(ctx); err != nil {
			fmt.Println("controller error:", err)
		}
	}()

	key := object.MakeKey("default", "widget")
	mustCreate(s, &object.Object{Namespace: "default", Name: "widget", Spec: map[string]string{"rev": "v1"}})
	time.Sleep(120 * time.Millisecond)

	// Burst of updates coalesces; each CAS uses the version observed at the
	// start of that reconcile, demonstrating conflict + re-read.
	for i := 2; i <= 5; i++ {
		cur := mustGet(s, key)
		_, _ = s.Update(ctx, key, cur.ResourceVersion, func(o *object.Object) {
			o.Spec["rev"] = fmt.Sprintf("v%d", i)
		})
	}
	time.Sleep(200 * time.Millisecond)

	final := mustGet(s, key)
	fmt.Printf("\n>>> converged status: %v finalizers=%v rv=%d\n\n",
		final.Status, final.Finalizers, final.ResourceVersion)

	// Deletion: timestamp set, finalizer cleanup, then physical removal.
	_, _ = s.Delete(ctx, key, final.ResourceVersion)
	time.Sleep(150 * time.Millisecond)
	if _, err := s.Get(ctx, key); errors.Is(err, object.ErrNotFound) {
		fmt.Println(">>> object physically removed after finalizer cleared")
	} else {
		fmt.Println(">>> ERROR: object still present")
	}

	cancel()
	time.Sleep(300 * time.Millisecond)
	fmt.Println(">>> demo complete; terminal failures:", ctrl.Failures())
}

func mustCreate(s store.Store, o *object.Object) {
	if _, err := s.Create(context.Background(), o); err != nil {
		panic(err)
	}
}

func mustGet(s store.Store, key string) *object.Object {
	o, err := s.Get(context.Background(), key)
	if err != nil {
		panic(err)
	}
	return o
}
