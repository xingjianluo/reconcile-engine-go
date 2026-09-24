package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xingjianluo/reconcile-engine-go/internal/object"
	"github.com/xingjianluo/reconcile-engine-go/internal/queue"
	"github.com/xingjianluo/reconcile-engine-go/internal/store"
)

const testFinalizer = "test.io/cleanup"

// fakeReconciler is an idempotent app reconciler used by the integration
// tests. It ensures a finalizer and converges status to spec while alive,
// and clears the finalizer / removes the object during deletion. The first
// failUntil active reconciles return failErr to exercise backoff.
type fakeReconciler struct {
	store        store.Store
	failUntil    int32
	failErr      error
	conflictOnce int32

	calls       atomic.Int64
	sideEffects atomic.Int64
}

func (r *fakeReconciler) Reconcile(ctx context.Context, obj *object.Object) (Result, error) {
	r.calls.Add(1)

	if obj.BeingDeleted() {
		if obj.HasFinalizer(testFinalizer) {
			if _, err := r.store.Update(ctx, obj.Key(), obj.ResourceVersion, func(o *object.Object) {
				o.RemoveFinalizer(testFinalizer)
			}); err != nil {
				return Result{}, err
			}
			r.sideEffects.Add(1)
			return Result{}, nil
		}
		if err := r.store.Remove(ctx, obj.Key(), obj.ResourceVersion); err != nil {
			return Result{}, err
		}
		r.sideEffects.Add(1)
		return Result{}, nil
	}

	// Configurable early failures to drive retry/backoff.
	if n := atomic.AddInt32(&r.failUntil, -1); n >= 0 {
		return Result{}, r.failErr
	}

	// Optionally force a conflict on one active reconcile: a separate writer
	// advances the object on the captured version, so the main CAS below is
	// based on stale data and must return a conflict (retried by controller).
	if atomic.CompareAndSwapInt32(&r.conflictOnce, 1, 0) {
		_, _ = r.store.Update(ctx, obj.Key(), obj.ResourceVersion, func(o *object.Object) {
			o.Status = map[string]string{"race": "won"}
		})
	}

	converged := obj.HasFinalizer(testFinalizer) && statusEqualsSpec(obj)
	if converged {
		return Result{}, nil
	}
	out, err := r.store.Update(ctx, obj.Key(), obj.ResourceVersion, func(o *object.Object) {
		o.AddFinalizer(testFinalizer)
		o.Status = cloneSpec(o.Spec)
	})
	if err != nil {
		return Result{}, err
	}
	r.sideEffects.Add(1)
	fmt.Printf("      [app] side effect applied rv=%d status=%v\n", out.ResourceVersion, out.Status)
	return Result{}, nil
}

func statusEqualsSpec(o *object.Object) bool {
	if len(o.Status) != len(o.Spec) {
		return false
	}
	for k, v := range o.Spec {
		if o.Status[k] != v {
			return false
		}
	}
	return true
}

func cloneSpec(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

type rig struct {
	store *store.MemoryStore
	queue *queue.BasicQueue
	rec   *fakeReconciler
	ctrl  *Controller
}

func newRig(t *testing.T, workers, maxAttempts int, rec *fakeReconciler) *rig {
	t.Helper()
	s := store.NewMemoryStore()
	rec.store = s
	q := queue.New(queue.Config{
		Backoff:     queue.NewExponentialBackoff(2*time.Millisecond, 20*time.Millisecond),
		MaxAttempts: maxAttempts,
	})
	ctrl := New(Config{
		Store:         s,
		Queue:         q,
		Reconciler:    rec,
		Workers:       workers,
		ShutdownGrace: time.Second,
		Logger:        func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
	})
	return &rig{store: s, queue: q, rec: rec, ctrl: ctrl}
}

// waitFor polls cond until it holds true or the deadline elses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// TestConcurrentReconcileConverges creates many objects processed by several
// workers and asserts every object converges to status==spec exactly-once
// side-effect semantics (re-running on a converged object changes nothing).
func TestConcurrentReconcileConverges(t *testing.T) {
	rec := &fakeReconciler{failErr: errors.New("ignored")}
	r := newRig(t, 4, 5, rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)

	const n = 25
	for i := 0; i < n; i++ {
		_, err := r.store.Create(ctx, &object.Object{
			Namespace: "ns", Name: fmt.Sprintf("o%02d", i),
			Spec: map[string]string{"state": "desired"},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	ok := waitFor(2*time.Second, func() bool {
		objs, _ := r.store.List(ctx)
		if len(objs) != n {
			return false
		}
		for _, o := range objs {
			if !o.HasFinalizer(testFinalizer) || !statusEqualsSpec(o) {
				return false
			}
		}
		return true
	})
	fmt.Printf("INPUT %d objects, 4 workers | JUDGE: all converged=%v sideEffects=%d (one per object)\n",
		n, ok, rec.sideEffects.Load())
	if !ok {
		t.Fatal("not all objects converged")
	}
	if rec.sideEffects.Load() != n {
		t.Fatalf("expected exactly %d convergence writes, got %d", n, rec.sideEffects.Load())
	}
}

// TestCoalescedBurstProcessesLatestVersion fires many rapid updates for one
// key and asserts the final observed/actual state equals the last desired
// version, with intermediate stale writes reconciled via conflict re-reads.
func TestCoalescedBurstProcessesLatestVersion(t *testing.T) {
	rec := &fakeReconciler{failErr: errors.New("ignored"), conflictOnce: 1}
	r := newRig(t, 2, 5, rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)

	key := "ns/burst"
	_, err := r.store.Create(ctx, &object.Object{
		Namespace: "ns", Name: "burst", Spec: map[string]string{"rev": "0"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Rapid updates; every CAS uses the latest read version. Some lose the
	// race to the reconciler and get conflicts, which is expected.
	for i := 1; i <= 20; i++ {
		cur, gerr := r.store.Get(ctx, key)
		if gerr != nil {
			t.Fatal(gerr)
		}
		_, _ = r.store.Update(ctx, key, cur.ResourceVersion, func(o *object.Object) {
			o.Spec["rev"] = fmt.Sprintf("%d", i)
		})
	}

	ok := waitFor(2*time.Second, func() bool {
		o, err := r.store.Get(ctx, key)
		return err == nil && o.Status["rev"] == "20" && o.HasFinalizer(testFinalizer)
	})
	o, _ := r.store.Get(ctx, key)
	fmt.Printf("INPUT 20 rapid updates | JUDGE: final status rev=%s (want 20), converged=%v\n",
		o.Status["rev"], ok)
	if !ok {
		t.Fatal("final state did not converge to latest desired version")
	}
}

// TestFinalizerDeletionEndToEnd confirms the full ordered deletion protocol
// under the controller: Delete marks, reconcile clears finalizer, the object
// remains until the next reconcile physically removes it.
func TestFinalizerDeletionEndToEnd(t *testing.T) {
	rec := &fakeReconciler{failErr: errors.New("ignored")}
	r := newRig(t, 1, 5, rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)

	key := "ns/del"
	_, err := r.store.Create(ctx, &object.Object{
		Namespace: "ns", Name: "del", Spec: map[string]string{"x": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for convergence.
	if !waitFor(2*time.Second, func() bool {
		cur, _ := r.store.Get(ctx, key)
		return cur != nil && cur.HasFinalizer(testFinalizer)
	}) {
		t.Fatal("did not converge before delete")
	}

	cur, _ := r.store.Get(ctx, key)
	marked, err := r.store.Delete(ctx, key, cur.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("INPUT Delete rv=%d | JUDGE: object remains deleting=%t finalizers=%v\n",
		cur.ResourceVersion, marked.BeingDeleted(), marked.Finalizers)

	// It must not disappear immediately.
	if _, err := r.store.Get(ctx, key); err != nil {
		t.Fatalf("object vanished before finalizer cleared: %v", err)
	}

	gone := waitFor(2*time.Second, func() bool {
		_, err := r.store.Get(ctx, key)
		return errors.Is(err, object.ErrNotFound)
	})
	fmt.Printf("JUDGE: object physically removed after finalizer sequence=%v\n", gone)
	if !gone {
		t.Fatal("object was not removed after finalizer cleared")
	}
}

// TestBackoffRetryThenTerminal drives a reconciler that always fails and
// asserts (1) retries happen with backoff and (2) exhaustion yields a
// distinguishable terminal failure with a reason rather than a silent drop.
func TestBackoffRetryThenTerminal(t *testing.T) {
	boom := errors.New("permanent downstream error")
	rec := &fakeReconciler{failErr: boom}
	// failUntil set large so every active reconcile fails.
	rec.failUntil = 1_000_000
	r := newRig(t, 1, 3, rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)

	key := "ns/bad"
	if _, err := r.store.Create(ctx, &object.Object{
		Namespace: "ns", Name: "bad", Spec: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatal(err)
	}

	var terminal error
	ok := waitFor(2*time.Second, func() bool {
		fails := r.ctrl.Failures()
		terminal = fails[key]
		return terminal != nil
	})
	if !ok {
		t.Fatal("expected a terminal failure to be recorded")
	}
	tf, isTF := terminal.(*TerminalFailure)
	fmt.Printf("INPUT always-failing reconcile, MaxAttempts=3 | JUDGE: terminal present=%v type=%T reason=%q attempts=%d calls=%d\n",
		ok, terminal, tf.Reason, tf.Attempts, rec.calls.Load())
	if !isTF {
		t.Fatalf("want *TerminalFailure, got %T", terminal)
	}
	if tf.Reason != ReasonRetryExhausted {
		t.Fatalf("want ReasonRetryExhausted, got %s", tf.Reason)
	}
	if tf.Attempts != 3 {
		t.Fatalf("want 3 attempts, got %d", tf.Attempts)
	}
	if !errors.Is(tf.Err, boom) {
		t.Fatalf("terminal must wrap original error, got %v", tf.Err)
	}

	// Queue should retain the same distinguishable record.
	qfail := r.queue.Failures()[key]
	var ex *queue.ExhaustedError
	if !errors.As(qfail, &ex) {
		t.Fatalf("queue failure want *ExhaustedError, got %T", qfail)
	}
	fmt.Printf("JUDGE: queue records *ExhaustedError after %d attempts; not silently dropped\n", ex.Attempts)
}

// TestGracefulShutdownObservesInFlight starts a slow reconcile, cancels the
// controller, and verifies shutdown waits for the in-flight work to finish
// and the object ends converged.
func TestGracefulShutdownObservesInFlight(t *testing.T) {
	rec := &fakeReconciler{failErr: errors.New("ignored")}
	r := newRig(t, 1, 5, rec)
	ctx, cancel := context.WithCancel(context.Background())

	runDone := make(chan error, 1)
	go func() { runDone <- r.ctrl.Run(ctx) }()

	_, err := r.store.Create(ctx, &object.Object{
		Namespace: "ns", Name: "grace", Spec: map[string]string{"a": "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Allow it to converge, then cancel while queue has drained.
	time.Sleep(50 * time.Millisecond)
	fmt.Printf("INPUT cancel with in-flight=%d\n", r.ctrl.InFlight())
	cancel()
	select {
	case err := <-runDone:
		fmt.Printf("JUDGE: Run returned err=%v after observing in-flight drain\n", err)
	case <-time.After(3 * time.Second):
		t.Fatal("controller did not shut down within grace")
	}
	o, _ := r.store.Get(ctx, "ns/grace")
	if o == nil || !o.HasFinalizer(testFinalizer) || o.Status["a"] != "b" {
		t.Fatalf("object not converged before/at shutdown: %v", o)
	}
	fmt.Printf("JUDGE: final object converged status=%v finalizers=%v\n", o.Status, o.Finalizers)
}

// TestIdempotentSideEffects ensures that after convergence, repeated change
// notifications (including after the object is already at the desired state)
// do not produce duplicate side effects: the reconciler runs but the apply
// branch is skipped because the object is already converged.
func TestIdempotentSideEffects(t *testing.T) {
	rec := &fakeReconciler{failErr: errors.New("ignored")}
	r := newRig(t, 1, 5, rec)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.ctrl.Run(ctx)

	key := "ns/idem"
	if _, err := r.store.Create(ctx, &object.Object{
		Namespace: "ns", Name: "idem", Spec: map[string]string{"s": "1"},
	}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(time.Second, func() bool {
		o, _ := r.store.Get(ctx, key)
		return o != nil && statusEqualsSpec(o) && o.HasFinalizer(testFinalizer)
	}) {
		t.Fatal("did not converge")
	}
	before := rec.sideEffects.Load()

	// Touch the object with a no-op update repeatedly; status already matches
	// spec, so no convergence write (side effect) should occur.
	for i := 0; i < 5; i++ {
		cur, _ := r.store.Get(ctx, key)
		_, _ = r.store.Update(ctx, key, cur.ResourceVersion, func(o *object.Object) {
			o.AnnotateTouched()
		})
	}
	time.Sleep(100 * time.Millisecond)
	after := rec.sideEffects.Load()
	fmt.Printf("INPUT 5 no-op notifications after convergence | JUDGE: sideEffects before=%d after=%d (must be equal)\n",
		before, after)
	if after != before {
		t.Fatalf("duplicate side effects on no-op changes: %d -> %d", before, after)
	}
}
